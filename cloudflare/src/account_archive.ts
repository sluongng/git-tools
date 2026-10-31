/** Same-build, same-account archive. The enclosing account queue must serialize
 * every call through its physical awaits and fence all non-archive entry points. */
import { inspect } from './core';
import { fail, Failure } from './protocol';
import { archivePageHash, canonicalArchive, MAX_ARCHIVE_BYTES, MAX_ARCHIVE_PAGE, MAX_ARCHIVE_PAGES, MAX_ARCHIVE_ROW, MAX_ARCHIVE_ROWS, parseArchiveCall, requireArchiveAuth, type ArchiveAuth, type ArchiveCall, type ArchiveCell, type ArchiveContent, type ArchiveManifest, type ArchivePage, type ArchiveReply, type ArchiveTable } from './archive_protocol';
export interface ArchiveSchemaEntry {kind:'table'|'index';name:string;table:string;sql:string}
export interface AccountArchiveHost {
 accountId:string;sql:SqlStorage;objects:R2Bucket;buildId:string;schema:readonly ArchiveSchemaEntry[];
 transactionSync<T>(f:()=>T):T;nowSeconds():number;
 /** Validate/consume ONLY the known constructor scheduling default; never clear user state. */
 requireFreshTargetInTx():void;
 /** Reject any discovered cross-account custody/dependency for this single-account format. */
 requireSingleAccountInTx():void;
 /** Reconstruct alarms from restored SQL; this is not a mutation of restored application rows. */
 onActivatedInTx():void;
}
interface Job {
 account:string;id:string;build:string;mode:'export'|'restore';credential:string;principal:string;
 phase:'audit'|'audit_final'|'seal'|'rows'|'payloads'|'exported'|'export_complete'|'aborted'|'restoring'|'complete';
 sequence:number;chain:string;rows:number;bytes:number;payloads:number;tables:ArchiveTable[];
 table_index:number;last_rowid:number|null;payload_after:string;audit_cursor:string|null;
 manifest:ArchiveManifest|null;last_hash:string|null;
}
interface Attempt {attempt_id:string;kind:number;id:string;size:number;inspection:string;locator:string;state:'pending'|'written'|'retiring'|'deleted';etag:string|null;version:string|null}
interface Proof {attempt_id:string;presence:string;etag:string|null;version:string|null}
const encoder=new TextEncoder();
const ZERO='0'.repeat(64), MAX_TABLES=512, MAX_COLUMNS=256, MAX_PAYLOADS=131072;
function quoted(name:string):string {return '"'+name.replace(/"/g,'""')+'"';}
function size(value:unknown):number{return encoder.encode(canonicalArchive(value)).byteLength;}
function same(a:unknown,b:unknown):boolean{return canonicalArchive(a)===canonicalArchive(b);}
function hex(bytes:Uint8Array):string{return Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');}
function unhex(s:string):Uint8Array{const b=new Uint8Array(s.length/2);for(let i=0;i<b.length;i++)b[i]=parseInt(s.slice(i*2,i*2+2),16);return b;}
function safe(n:number):boolean{return Number.isSafeInteger(n);}
/** Normalize SQL grammar whitespace only; quoted literals/identifiers remain byte exact. */
function ddl(sql:string):string {
 const tokens:string[]=[];let at=0;
 while(at<sql.length){const c=sql[at];if(/\s/.test(c)){at++;continue;}
  if(sql.slice(at,at+2)==='--'){const end=sql.indexOf('\n',at+2);at=end<0?sql.length:end+1;continue;}
  if(sql.slice(at,at+2)==='/*'){const end=sql.indexOf('*/',at+2);if(end<0)fail(412,'schema_mismatch');at=end+2;continue;}
  if(c==="'"||c==='"'||c==='`'||c==='['){const start=at++;const close=c==='['?']':c;let closed=false;while(at<sql.length){if(sql[at++]===close){if(c!=='['&&sql[at]===close){at++;continue;}closed=true;break;}}if(!closed)fail(412,'schema_mismatch');tokens.push(sql.slice(start,at));continue;}
  const word=/^[A-Za-z_][A-Za-z_0-9]*/.exec(sql.slice(at));if(word){tokens.push(word[0].toUpperCase());at+=word[0].length;}else{tokens.push(c);at++;}
 }
 const position=tokens[0]==='CREATE'&&tokens[1]==='UNIQUE'&&tokens[2]==='INDEX'?3:tokens[0]==='CREATE'&&['TABLE','INDEX'].includes(tokens[1])?2:-1;
 if(position>=0&&tokens.slice(position,position+3).join(' ')==='IF NOT EXISTS')tokens.splice(position,3);
 if(tokens[tokens.length-1]===';')tokens.pop();return JSON.stringify(tokens);
}
function archiveInternal(name:string):boolean{return ['archive_job','archive_checkpoint','archive_payload_proofs','archive_audit_seen'].includes(name);}
/** Reads only archive metadata; safe before application constructors run. */
export function archiveBarrierInTx(sql:SqlStorage):boolean {
 const table=[...sql.exec("SELECT name FROM sqlite_schema WHERE type='table' AND name='archive_job'")];if(!table.length)return false;
 const rows=[...sql.exec('SELECT body FROM archive_job WHERE singleton=1')] as {body:string}[];if(!rows.length)return false;
 let j:Job;try{j=JSON.parse(rows[0].body);}catch{fail(503,'archive_state_invalid');}
 return !['export_complete','aborted','complete'].includes(j.phase);
}
export class AccountArchive {
 constructor(private readonly host:AccountArchiveHost,private readonly secret:()=>string|undefined){
  if(!/^[a-z0-9-]{1,64}$/.test(host.accountId)||!/^[0-9a-f]{64}$/.test(host.buildId))fail(503,'archive_configuration_invalid');
  for(const sql of [
   'CREATE TABLE IF NOT EXISTS archive_job(singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT NOT NULL)',
   'CREATE TABLE IF NOT EXISTS archive_checkpoint(part INTEGER PRIMARY KEY,body TEXT NOT NULL)',
   'CREATE TABLE IF NOT EXISTS archive_payload_proofs(attempt_id TEXT PRIMARY KEY,presence TEXT NOT NULL,etag TEXT,version TEXT)',
   'CREATE TABLE IF NOT EXISTS archive_audit_seen(attempt_id TEXT PRIMARY KEY)',
  ])this.exec(sql);
 }
 private exec(sql:string,...args:(string|number|null|ArrayBuffer)[]){try{return this.host.sql.exec(sql,...args);}catch(e){if(e instanceof Failure)throw e;fail(503,'unavailable');}}
 private rows<T>(sql:string,...args:(string|number|null|ArrayBuffer)[]):T[]{return [...this.exec(sql,...args)] as T[];}
 private one<T>(sql:string,...args:(string|number|null|ArrayBuffer)[]):T|undefined{return this.rows<T>(sql,...args)[0];}
 private tx<T>(f:()=>T):T{try{return this.host.transactionSync(f);}catch(e){if(e instanceof Failure)throw e;fail(503,'unavailable');}}
 private load():Job|undefined{const row=this.one<{body:string}>('SELECT body FROM archive_job WHERE singleton=1');if(!row)return;try{return JSON.parse(row.body);}catch{fail(503,'archive_state_invalid');}}
 private save(j:Job):void{const body=canonicalArchive(j);if(encoder.encode(body).length>1024*1024)fail(413,'resource_exhausted');this.exec('INSERT OR REPLACE INTO archive_job(singleton,body) VALUES(1,?)',body);}
 private checkpoint(page:ArchivePage):void{const body=canonicalArchive(page);this.exec('DELETE FROM archive_checkpoint');for(let start=0,part=0;start<body.length;part++){let end=Math.min(start+32768,body.length);const last=body.charCodeAt(end-1);if(end<body.length&&last>=0xd800&&last<=0xdbff)end--;this.exec('INSERT INTO archive_checkpoint(part,body) VALUES(?,?)',part,body.slice(start,end));start=end;}}
 private lastPage():ArchivePage|undefined{const parts=this.rows<{body:string}>('SELECT body FROM archive_checkpoint ORDER BY part');if(!parts.length)return;const text=parts.map(p=>p.body).join('');if(encoder.encode(text).length>MAX_ARCHIVE_PAGE)fail(503,'archive_state_invalid');try{return JSON.parse(text);}catch{fail(503,'archive_state_invalid');}}
 private reply(j:Job,state:ArchiveReply['state']=j.mode==='export'?'exporting':'restoring'):ArchiveReply{return{schema:1,account_id:j.account,archive_id:j.id,build_id:j.build,state,next_sequence:j.sequence};}
 private bound(call:ArchiveCall,auth:ArchiveAuth):Job{const j=this.load();if(!j||j.id!==call.archive_id||j.account!==call.account_id||j.build!==this.host.buildId)fail(409,'archive_conflict');if(j.credential!==auth.credential_id||j.principal!==auth.principal)fail(403,'permission_denied');return j;}
 private async auth(auth:ArchiveAuth,mode:'export'|'restore'):Promise<void>{await requireArchiveAuth(this.secret(),auth,this.host.accountId,mode);}
 private inventory():ArchiveTable[]{
  const expected=new Map<string,ArchiveSchemaEntry>();for(const e of this.host.schema){if(e.name.startsWith('archive_')||e.name.startsWith('sqlite_')||e.name.startsWith('_cf_')||e.name.startsWith('__cf_')||!['table','index'].includes(e.kind)||!e.sql||JSON.parse(ddl(e.sql))[0]!=='CREATE')fail(412,'schema_mismatch');const old=expected.get(e.name);if(old&&!same(old,e))fail(412,'schema_mismatch');expected.set(e.name,e);}
  const actual=this.rows<{type:string;name:string;tbl_name:string;sql:string|null}>("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name");const found=new Set<string>();
  for(const row of actual){
   // workerd owns this exact table for alarms/bookmarks. Its contents are not
   // application state; restored alarm scheduling is reconstructed from SQL.
   if(row.type==='table'&&row.name==='_cf_METADATA'&&row.tbl_name==='_cf_METADATA')continue;
   if(archiveInternal(row.name))continue;if(row.sql===null) {if(row.type==='index'&&row.name.startsWith('sqlite_autoindex_')&&(expected.get(row.tbl_name)?.kind==='table'||archiveInternal(row.tbl_name)))continue;fail(412,'schema_mismatch');}const e=expected.get(row.name);if(!e||row.type!==e.kind||row.tbl_name!==e.table||ddl(row.sql)!==ddl(e.sql))fail(412,'schema_mismatch');found.add(row.name);}
  if(found.size!==expected.size)fail(412,'schema_mismatch');
  const tables=[...expected.values()].filter(e=>e.kind==='table').sort((a,b)=>a.name<b.name?-1:a.name>b.name?1:0).map(e=>{const info=this.rows<{name:string;type:string}>(`PRAGMA table_info(${quoted(e.name)})`);const columns=info.map(c=>c.name);if(!columns.length||columns.length>MAX_COLUMNS||columns.some(n=>['rowid','_rowid_','oid','__archive_rowid'].includes(n.toLowerCase())))fail(412,'schema_mismatch');return{name:e.name,columns,rows:0};});if(tables.length>MAX_TABLES||size(tables)>512*1024)fail(413,'resource_exhausted');return tables;
 }
 private initialize():ArchiveTable[]{for(const kind of ['table','index'])for(const e of this.host.schema)if(e.kind===kind)this.exec(e.sql);return this.inventory();}
 private schemaMatches(tables:ArchiveTable[]):void{const current=this.inventory();if(!same(current.map(t=>({name:t.name,columns:t.columns})),tables.map(t=>({name:t.name,columns:t.columns}))))fail(412,'schema_mismatch');}
 private manifest(j:Job):ArchiveManifest{return{schema:1,account_id:j.account,archive_id:j.id,build_id:j.build,tables:j.tables,page_count:j.sequence,chain_root:j.chain,rows:j.rows,bytes:j.bytes,payloads:j.payloads};}
 private validateManifest(m:ArchiveManifest,call:ArchiveCall):void{if(m.account_id!==call.account_id||m.archive_id!==call.archive_id)fail(400,'archive_binding_mismatch');if(m.build_id!==this.host.buildId)fail(412,'build_mismatch');if(m.tables.reduce((n,t)=>n+t.rows,0)!==m.rows)fail(400,'archive_count_mismatch');}
 async handle(untrusted:ArchiveCall,auth:ArchiveAuth):Promise<ArchiveReply>{
  const call=parseArchiveCall(untrusted);if(call.account_id!==this.host.accountId)fail(403,'permission_denied');const existing=this.load();const mode=call.action==='status'?(existing?.mode??'export'):call.action.startsWith('restore')?'restore':'export';await this.auth(auth,mode);
  if(call.action==='export_begin'||call.action==='restore_begin'){if(call.action==='restore_begin'&&(!existing||existing.id!==call.archive_id)){for(const version of ['v1','v2']){const listed=await this.host.objects.list({prefix:`native/${version}/${this.host.accountId}/`,limit:1});await this.auth(auth,'restore');if(listed.objects.length||listed.truncated)fail(409,'target_not_empty');}}return this.begin(call,auth);}
  const j=this.bound(call,auth);
  if(call.action==='status'){const result=this.reply(j,j.phase==='complete'||j.phase==='export_complete'?'complete':j.phase==='aborted'?'aborted':j.phase==='exported'?'exported':undefined);if(j.manifest)result.manifest=j.manifest;const page=this.lastPage();if(page)result.last_page=page;return result;}
  if(call.action==='export_abort'){if(j.mode!=='export'||j.phase==='export_complete')fail(409,'archive_conflict');this.tx(()=>{j.phase='aborted';this.save(j);});return this.reply(j,'aborted');}
  if(call.action==='export_finish'){if(j.mode!=='export'||!['exported','export_complete'].includes(j.phase)||!same(j.manifest,call.manifest))fail(409,'archive_conflict');this.tx(()=>{j.phase='export_complete';this.save(j);});return{...this.reply(j,'complete'),manifest:j.manifest!};}
  if(call.action==='export_next'){if(j.mode!=='export')fail(409,'archive_conflict');return this.exportNext(j,call.sequence,auth);}
  if(call.action==='restore_page'){if(j.mode!=='restore'||j.phase!=='restoring')fail(409,'archive_conflict');return this.restorePage(j,call.page,auth);}
  if(call.action==='restore_finish'){if(j.mode!=='restore')fail(409,'archive_conflict');if(j.phase==='complete')return{...this.reply(j,'complete'),manifest:j.manifest!};return this.finishRestore(j,auth);}
  fail(409,'archive_conflict');
 }
 private begin(call:Extract<ArchiveCall,{action:'export_begin'|'restore_begin'}>,auth:ArchiveAuth):ArchiveReply{return this.tx(()=>{
  const old=this.load();if(old&&old.id===call.archive_id){if(old.credential!==auth.credential_id||old.principal!==auth.principal||old.mode!==(call.action==='restore_begin'?'restore':'export'))fail(403,'permission_denied');if(call.action==='restore_begin'&&!same(old.manifest,call.manifest))fail(409,'archive_conflict');return{...this.reply(old,old.phase==='complete'||old.phase==='export_complete'?'complete':old.phase==='exported'?'exported':old.phase==='aborted'?'aborted':undefined),...(old.manifest?{manifest:old.manifest}:{})};}
  if(old&&!['export_complete','aborted','complete'].includes(old.phase))fail(409,'archive_busy');
  if(call.action==='export_begin'&&call.build_id!==undefined&&call.build_id!==this.host.buildId)fail(412,'build_mismatch');
  if(call.action==='restore_begin'){this.validateManifest(call.manifest,call);this.host.requireFreshTargetInTx();}
  const tables=this.initialize();this.host.requireSingleAccountInTx();
  if(call.action==='restore_begin'){if(!same(tables.map(t=>({name:t.name,columns:t.columns})),call.manifest.tables.map(t=>({name:t.name,columns:t.columns}))))fail(412,'schema_mismatch');for(const t of tables){if(this.one<{n:number}>(`SELECT COUNT(*) AS n FROM ${quoted(t.name)}`)!.n!==0)fail(409,'target_not_empty');}}
  else for(const t of tables){t.rows=this.one<{n:number}>(`SELECT COUNT(*) AS n FROM ${quoted(t.name)}`)!.n;if(!safe(t.rows)||t.rows<0)fail(413,'resource_exhausted');}
  const j:Job={account:call.account_id,id:call.archive_id,build:this.host.buildId,mode:call.action==='export_begin'?'export':'restore',credential:auth.credential_id,principal:auth.principal,phase:call.action==='export_begin'?'audit':'restoring',sequence:0,chain:ZERO,rows:0,bytes:0,payloads:0,tables:call.action==='restore_begin'?call.manifest.tables:tables,table_index:0,last_rowid:null,payload_after:'',audit_cursor:null,manifest:call.action==='restore_begin'?call.manifest:null,last_hash:null};
  if(j.tables.reduce((n,t)=>n+t.rows,0)>MAX_ARCHIVE_ROWS)fail(413,'resource_exhausted');this.exec('DELETE FROM archive_checkpoint');this.exec('DELETE FROM archive_payload_proofs');this.exec('DELETE FROM archive_audit_seen');this.save(j);return this.reply(j);
 });}
 private assertAttempt(a:Attempt):void{if(!/^[0-9a-f]{64}$/.test(a.attempt_id)||!Number.isInteger(a.kind)||a.kind<1||a.kind>5||(a.kind===3&&a.id===ZERO)||!/^[0-9a-f]{64}$/.test(a.id)||!safe(a.size)||a.size<0||a.size>(a.kind===1?1024*1024:256*1024)||!['pending','written','retiring','deleted'].includes(a.state)||a.locator!==`native/v2/${this.host.accountId}/${a.kind}/${a.id}/${a.attempt_id}`)fail(503,'archive_storage_invalid');}
 private matchR2(a:Attempt,o:R2Object,source=true):void{const m=o.customMetadata;if(o.key!==a.locator||o.size!==a.size||m?.attempt!==a.attempt_id||m.account!==this.host.accountId||m.kind!==String(a.kind)||m.id!==a.id||m.format!=='1'||Object.keys(m).sort().join(',')!=='account,attempt,format,id,kind'||(source&&((a.etag!==null&&a.etag!==o.etag)||(a.version!==null&&a.version!==o.version))))fail(503,'archive_payload_mismatch');}
 private native(a:Attempt,bytes:Uint8Array):void{try{const d=inspect(a.kind,bytes);if(d.id!==a.id||d.kind!==a.kind||d.size_bytes!==a.size||d.format_version!==1||!same(d,JSON.parse(a.inspection)))fail(503,'archive_payload_mismatch');}catch(e){if(e instanceof Failure)throw e;fail(503,'archive_payload_mismatch');}}
 private async audit(j:Job,auth:ArchiveAuth):Promise<ArchiveReply>{
  const legacy=await this.host.objects.list({prefix:`native/v1/${j.account}/`,limit:1});await this.auth(auth,'export');if(legacy.objects.length||legacy.truncated)fail(412,'archive_legacy_payload');
  const list=await this.host.objects.list({prefix:`native/v2/${j.account}/`,limit:256,...(j.audit_cursor?{cursor:j.audit_cursor}:{}),include:['customMetadata']});await this.auth(auth,'export');
  this.tx(()=>{for(const o of list.objects){const a=this.one<Attempt>('SELECT * FROM forge_storage_attempts WHERE locator=?',o.key);if(!a)fail(503,'archive_unknown_payload');this.assertAttempt(a);if(a.state==='deleted')fail(503,'archive_unknown_payload');this.matchR2(a,o);if(j.phase==='audit_final'){const p=this.one<Proof>('SELECT * FROM archive_payload_proofs WHERE attempt_id=?',a.attempt_id);if(!p||p.presence!=='present'||p.etag!==o.etag||p.version!==o.version)fail(503,'archive_payload_changed');this.exec('INSERT OR REPLACE INTO archive_audit_seen(attempt_id) VALUES(?)',a.attempt_id);}}if(list.truncated)j.audit_cursor=list.cursor;else{j.audit_cursor=null;if(j.phase==='audit_final'){const missing=this.one<{n:number}>("SELECT COUNT(*) AS n FROM archive_payload_proofs p LEFT JOIN archive_audit_seen s ON p.attempt_id=s.attempt_id WHERE p.presence='present' AND s.attempt_id IS NULL")!.n;if(missing)fail(503,'archive_payload_changed');j.phase='seal';this.sealExport(j);}else j.phase='rows';}this.save(j);});return j.phase==='exported'?{...this.reply(j,'exported'),manifest:j.manifest!}:this.reply(j);
 }
 private encodeCell(value:unknown,type:string):ArchiveCell{if(type==='null'&&value===null)return{kind:'null'};if(type==='text'&&typeof value==='string')return{kind:'text',value};if(type==='integer'&&typeof value==='number'&&safe(value))return{kind:'integer',value};if(type==='real'&&typeof value==='number'&&Number.isFinite(value)&&Math.abs(value)<=Number.MAX_SAFE_INTEGER)return{kind:'real',value:String(value)};if(type==='blob'&&(value instanceof ArrayBuffer||ArrayBuffer.isView(value))){const b=value instanceof ArrayBuffer?new Uint8Array(value):new Uint8Array(value.buffer,value.byteOffset,value.byteLength);return{kind:'blob',hex:hex(b)};}fail(413,'archive_value_unsupported');}
 private rowContent(j:Job):ArchiveContent|undefined{
  while(j.table_index<j.tables.length){const t=j.tables[j.table_index];const selected=t.columns.map((c,i)=>`${quoted(c)} AS ${quoted('v'+i)},typeof(${quoted(c)}) AS ${quoted('t'+i)}`).join(',');const rows:{rowid:number;cells:ArchiveCell[]}[]=[];let pageBytes=0;
   for(let n=0;n<128;n++){const row=this.one<Record<string,unknown>>(`SELECT rowid AS __archive_rowid,${selected} FROM ${quoted(t.name)}${j.last_rowid===null?'':' WHERE rowid>?'} ORDER BY rowid LIMIT 1`,...(j.last_rowid===null?[]:[j.last_rowid]));if(!row)break;const id=row.__archive_rowid;if(typeof id!=='number'||!safe(id))fail(413,'archive_value_unsupported');const next={rowid:id,cells:t.columns.map((_,i)=>this.encodeCell(row['v'+i],String(row['t'+i])))};const bytes=size(next);if(bytes>MAX_ARCHIVE_ROW)fail(413,'resource_exhausted');if(rows.length&&pageBytes+bytes>MAX_ARCHIVE_PAGE-65536)break;rows.push(next);pageBytes+=bytes;j.last_rowid=id;}
   if(rows.length)return{kind:'rows',table:t.name,columns:t.columns,rows};j.table_index++;j.last_rowid=null;
  }j.phase='payloads';return;
 }
 private async page(j:Job,content:ArchiveContent,auth:ArchiveAuth,proof?:Proof):Promise<ArchiveReply>{const unhashed={schema:1 as const,account_id:j.account,archive_id:j.id,build_id:j.build,sequence:j.sequence,previous:j.chain,content};const page:ArchivePage={...unhashed,hash:await archivePageHash(unhashed)};await this.auth(auth,'export');if(size(page)>MAX_ARCHIVE_PAGE)fail(413,'resource_exhausted');this.tx(()=>{if(proof)this.exec('INSERT INTO archive_payload_proofs(attempt_id,presence,etag,version) VALUES(?,?,?,?)',proof.attempt_id,proof.presence,proof.etag,proof.version);this.acceptCounts(j,page);this.checkpoint(page);this.save(j);});return{...this.reply(j,'page'),page};}
 private acceptCounts(j:Job,p:ArchivePage):void{j.sequence++;j.chain=p.hash;j.last_hash=p.hash;j.bytes+=size(p.content);if(p.content.kind==='rows')j.rows+=p.content.rows.length;else j.payloads++;if(j.sequence>MAX_ARCHIVE_PAGES||j.rows>MAX_ARCHIVE_ROWS||j.bytes>MAX_ARCHIVE_BYTES||j.payloads>MAX_PAYLOADS)fail(413,'resource_exhausted');}
 private async exportNext(j:Job,sequence:number,auth:ArchiveAuth):Promise<ArchiveReply>{
  if(sequence===j.sequence-1){const page=this.lastPage();if(!page||page.sequence!==sequence)fail(409,'archive_sequence');return{...this.reply(j,'page'),page};}if(sequence!==j.sequence)fail(409,'archive_sequence');if(j.phase==='exported'||j.phase==='export_complete')return{...this.reply(j,j.phase==='exported'?'exported':'complete'),manifest:j.manifest!};if(j.phase==='aborted')fail(409,'archive_aborted');
  if(j.phase==='audit'||j.phase==='audit_final')return this.audit(j,auth);
  if(j.phase==='rows'){const content=this.tx(()=>this.rowContent(j));if(content){await this.auth(auth,'export');return this.page(j,content,auth);}}
  const a=j.phase==='seal'?undefined:this.one<Attempt>("SELECT * FROM forge_storage_attempts WHERE state!='deleted' AND attempt_id>? ORDER BY attempt_id LIMIT 1",j.payload_after);
  if(a){this.assertAttempt(a);const o=await this.host.objects.get(a.locator);await this.auth(auth,'export');let bytes:Uint8Array|null=null;if(o){this.matchR2(a,o);bytes=new Uint8Array(await o.arrayBuffer());await this.auth(auth,'export');this.native(a,bytes);}else if(a.state==='written')fail(503,'archive_payload_missing');const content:ArchiveContent={kind:'payload',attempt_id:a.attempt_id,object_kind:a.kind,object_id:a.id,size:a.size,locator:a.locator,state:a.state as 'written',source_etag:a.etag,source_version:a.version,presence:bytes?'present':'absent',bytes_hex:bytes?hex(bytes):null};j.payload_after=a.attempt_id;
   return this.page(j,content,auth,{attempt_id:a.attempt_id,presence:bytes?'present':'absent',etag:o?.etag??null,version:o?.version??null});
  }
  if(j.phase!=='seal'){j.phase='audit_final';j.audit_cursor=null;this.tx(()=>{this.exec('DELETE FROM archive_audit_seen');this.save(j);});return this.reply(j);}
  return this.tx(()=>{this.sealExport(j);this.save(j);return{...this.reply(j,'exported'),manifest:j.manifest!};});
 }
 private sealExport(j:Job):void{this.validateBindings();if(j.rows!==j.tables.reduce((n,t)=>n+t.rows,0))fail(503,'archive_count_mismatch');this.host.requireSingleAccountInTx();j.phase='exported';j.manifest=this.manifest(j);}
 private validateBindings():void{const bad=this.one<{n:number}>("SELECT COUNT(*) AS n FROM objects o LEFT JOIN forge_storage_bindings b ON o.kind=b.kind AND o.id=b.id LEFT JOIN forge_storage_attempts a ON b.attempt_id=a.attempt_id LEFT JOIN archive_payload_proofs p ON a.attempt_id=p.attempt_id WHERE o.visible=1 AND (a.attempt_id IS NULL OR a.state!='written' OR a.kind!=o.kind OR a.id!=o.id OR a.size!=o.size OR p.presence IS NULL OR p.presence!='present')");if(!bad||bad.n!==0)fail(503,'archive_payload_missing');}
 private tableFor(j:Job,content:Extract<ArchiveContent,{kind:'rows'}>):ArchiveTable{while(j.table_index<j.tables.length&&j.tables[j.table_index].rows===0)j.table_index++;const t=j.tables[j.table_index];if(!t||t.name!==content.table||!same(t.columns,content.columns))fail(400,'archive_table_order');return t;}
 private restoreRows(j:Job,c:Extract<ArchiveContent,{kind:'rows'}>):void{const t=this.tableFor(j,c);const completed=j.tables.slice(0,j.table_index).reduce((n,table)=>n+table.rows,0);const count=j.rows-completed+c.rows.length;if(!safe(count)||count<c.rows.length||count>t.rows)fail(400,'archive_count_mismatch');for(const row of c.rows){if(j.last_rowid!==null&&row.rowid<=j.last_rowid)fail(400,'archive_row_order');const values:(string|number|null|ArrayBuffer)[]=[row.rowid];const placeholders=['?'];for(const v of row.cells){switch(v.kind){case'null':values.push(null);placeholders.push('?');break;case'text':case'integer':values.push(v.value);placeholders.push('?');break;case'real':values.push(v.value);placeholders.push('CAST(? AS REAL)');break;case'blob':values.push(unhex(v.hex).buffer as ArrayBuffer);placeholders.push('?');break;}}this.exec(`INSERT INTO ${quoted(t.name)}(rowid,${t.columns.map(quoted).join(',')}) VALUES(${placeholders.join(',')})`,...values);j.last_rowid=row.rowid;}
  if(count===t.rows){j.table_index++;j.last_rowid=null;}
 }
 private async targetPayload(j:Job,c:Extract<ArchiveContent,{kind:'payload'}>,auth:ArchiveAuth):Promise<Proof>{
  if(j.table_index<j.tables.length){while(j.table_index<j.tables.length&&j.tables[j.table_index].rows===0)j.table_index++;if(j.table_index<j.tables.length)fail(400,'archive_table_order');}
  if(c.attempt_id<=j.payload_after)fail(400,'archive_payload_order');const a=this.one<Attempt>('SELECT * FROM forge_storage_attempts WHERE attempt_id=?',c.attempt_id);if(!a)fail(400,'archive_payload_unknown');this.assertAttempt(a);if(a.state==='deleted'||a.kind!==c.object_kind||a.id!==c.object_id||a.size!==c.size||a.locator!==c.locator||a.state!==c.state||a.etag!==c.source_etag||a.version!==c.source_version)fail(400,'archive_payload_mismatch');
  if(c.presence==='absent'){const exists=await this.host.objects.head(a.locator);await this.auth(auth,'restore');if(exists)fail(409,'target_payload_exists');return{attempt_id:a.attempt_id,presence:'absent',etag:null,version:null};}
  const bytes=unhex(c.bytes_hex!);this.native(a,bytes);
  let existing=await this.host.objects.head(a.locator);await this.auth(auth,'restore');
  if(!existing){try{existing=await this.host.objects.put(a.locator,bytes,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:{attempt:a.attempt_id,account:j.account,kind:String(a.kind),id:a.id,format:'1'}});}catch{existing=null;}await this.auth(auth,'restore');if(!existing){existing=await this.host.objects.head(a.locator);await this.auth(auth,'restore');}}
  if(!existing)fail(503,'archive_payload_unavailable');this.matchR2(a,existing,false);
  const got=await this.host.objects.get(a.locator,{onlyIf:{etagMatches:existing.etag}});await this.auth(auth,'restore');if(!got||!('body'in got))fail(503,'archive_payload_mismatch');this.matchR2(a,got,false);if(got.etag!==existing.etag||got.version!==existing.version)fail(503,'archive_payload_mismatch');const actual=new Uint8Array(await got.arrayBuffer());await this.auth(auth,'restore');this.native(a,actual);if(hex(actual)!==c.bytes_hex)fail(503,'archive_payload_mismatch');return{attempt_id:a.attempt_id,presence:'present',etag:got.etag,version:got.version};
 }
 private async restorePage(j:Job,p:ArchivePage,auth:ArchiveAuth):Promise<ArchiveReply>{
  if(p.account_id!==j.account||p.archive_id!==j.id||p.build_id!==j.build)fail(400,'archive_binding_mismatch');const {hash:claimed,...unhashed}=p;if(await archivePageHash(unhashed)!==claimed)fail(400,'archive_chain_mismatch');await this.auth(auth,'restore');if(p.sequence===j.sequence-1){if(j.last_hash!==p.hash)fail(409,'archive_replay_conflict');return this.reply(j);}if(p.sequence!==j.sequence||p.previous!==j.chain||p.sequence>=j.manifest!.page_count)fail(409,'archive_sequence');
  let proof:Proof|undefined;if(p.content.kind==='payload')proof=await this.targetPayload(j,p.content,auth);
  return this.tx(()=>{if(p.content.kind==='rows')this.restoreRows(j,p.content);else{if(!proof)fail(503,'unavailable');this.exec('INSERT INTO archive_payload_proofs(attempt_id,presence,etag,version) VALUES(?,?,?,?)',proof.attempt_id,proof.presence,proof.etag,proof.version);this.exec('UPDATE forge_storage_attempts SET etag=?,version=? WHERE attempt_id=?',proof.etag,proof.version,proof.attempt_id);j.payload_after=proof.attempt_id;}this.acceptCounts(j,p);this.checkpoint(p);this.save(j);return this.reply(j);});
 }
 private async finishRestore(j:Job,auth:ArchiveAuth):Promise<ArchiveReply>{await this.auth(auth,'restore');return this.tx(()=>{if(j.phase!=='restoring'||!same(this.manifest(j),j.manifest))fail(409,'archive_incomplete');this.schemaMatches(j.tables);for(const t of j.tables)if(this.one<{n:number}>(`SELECT COUNT(*) AS n FROM ${quoted(t.name)}`)!.n!==t.rows)fail(400,'archive_count_mismatch');const missing=this.one<{n:number}>("SELECT COUNT(*) AS n FROM forge_storage_attempts a LEFT JOIN archive_payload_proofs p ON a.attempt_id=p.attempt_id WHERE a.state!='deleted' AND p.attempt_id IS NULL")!.n;if(missing!==0)fail(409,'archive_incomplete');this.validateBindings();this.host.requireSingleAccountInTx();j.phase='complete';this.save(j);this.host.onActivatedInTx();return{...this.reply(j,'complete'),manifest:j.manifest!};});}
}
