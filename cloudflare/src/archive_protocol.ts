/** Dedicated account archive transport. No Forge/checker credential is authority here. */
import { body, digest, fail, Failure, hash, identifier, integer, json, record } from './protocol';
export const MAX_ARCHIVE_PAYLOADS = 131072 + 131072;
export const MAX_ARCHIVE_PAGE = 16 * 1024 * 1024;
export const MAX_ARCHIVE_ROW = 8 * 1024 * 1024;
export const MAX_ARCHIVE_PAGES = 1_000_000;
export const MAX_ARCHIVE_ROWS = 8_000_000;
export const MAX_ARCHIVE_BYTES = 64 * 1024 * 1024 * 1024;
export type ArchiveCell = { kind:'null' } | { kind:'text'; value:string } | { kind:'integer'; value:number } | { kind:'real'; value:string } | { kind:'blob'; hex:string };
export interface ArchiveTable { name:string; columns:string[]; rows:number }
export interface ArchiveManifest { schema:1; account_id:string; archive_id:string; build_id:string; tables:ArchiveTable[]; page_count:number; chain_root:string; rows:number; bytes:number; payloads:number }
export type ArchiveContent = {kind:'rows'; table:string; columns:string[]; rows:{rowid:number; cells:ArchiveCell[]}[]} | {kind:'payload'; attempt_id:string; object_kind:number; object_id:string; size:number; locator:string; state:'pending'|'written'|'retiring'; source_etag:string|null; source_version:string|null; presence:'present'|'absent'; bytes_hex:string|null} | {kind:'private_payload'; capsule_id:string; sequence:number; attempt_id:string; size:number; sha256:string; locator:string; state:'pending'|'written'|'retiring'|'tombstoned'; source_etag:string|null; source_version:string|null; presence:'present'|'absent'; bytes_hex:string|null};
export interface ArchivePage { schema:1; account_id:string; archive_id:string; build_id:string; sequence:number; previous:string; hash:string; content:ArchiveContent }
interface Base { schema:1; account_id:string; archive_id:string }
export type ArchiveCall = Base & ({action:'export_begin'; build_id?:string}|{action:'export_next'; sequence:number}|{action:'export_finish'; manifest:ArchiveManifest}|{action:'export_abort'|'status'|'restore_finish'}|{action:'restore_begin'; manifest:ArchiveManifest}|{action:'restore_page'; page:ArchivePage});
export interface ArchiveReply extends Base { build_id:string; state:'exporting'|'restoring'|'page'|'exported'|'complete'|'aborted'; next_sequence:number; page?:ArchivePage; last_page?:ArchivePage; manifest?:ArchiveManifest }
export interface ArchiveAuth { principal:string; credential_id:string }
interface Grant {principal:string;token_sha256:string;accounts:string[];export:boolean;restore:boolean}
export function canonicalArchive(value:unknown):string {
 if (value === null || typeof value === 'string' || typeof value === 'boolean') return JSON.stringify(value);
 if (typeof value === 'number' && Number.isSafeInteger(value)) return JSON.stringify(value);
 if (Array.isArray(value)) return '['+value.map(canonicalArchive).join(',')+']';
 if (value && typeof value === 'object') return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+canonicalArchive((value as Record<string,unknown>)[k])).join(',')+'}';
 fail(400,'invalid_argument');
}
export async function archivePageHash(page:Omit<ArchivePage,'hash'>):Promise<string> { return hash('tinytree-account-archive-page-v1\0'+page.previous+'\0'+canonicalArchive(page)); }
/** Parse with duplicate-key rejection before any application SQL is touched. */
export function strictArchiveJson(text:string):unknown {
 let at=0, nodes=0;
 const ws=()=>{while (/[ \t\r\n]/.test(text[at]??'') && at<text.length) at++;};
 const string=():string=>{ const start=at++; while(at<text.length) { const c=text[at++]; if(c==='"') {try{return JSON.parse(text.slice(start,at));}catch{fail(400,'invalid_argument');}} if(c==='\\') at++; } fail(400,'invalid_argument'); };
 const value=(depth:number):unknown=>{ if(depth>64 || ++nodes>2_000_000) fail(413,'resource_exhausted'); ws(); const c=text[at];
  if(c==='"') return string();
  if(c==='{') {at++;ws(); const out:Record<string,unknown>=Object.create(null); if(text[at]==='}') {at++;return out;} for(;;) {ws();if(text[at]!=='"') fail(400,'invalid_argument');const k=string();if(Object.hasOwn(out,k))fail(400,'invalid_argument');ws();if(text[at++]!==':')fail(400,'invalid_argument');out[k]=value(depth+1);ws();const end=text[at++];if(end==='}')return out;if(end!==',')fail(400,'invalid_argument');}}
  if(c==='[') {at++;ws();const out:unknown[]=[];if(text[at]===']'){at++;return out;}for(;;){out.push(value(depth+1));ws();const end=text[at++];if(end===']')return out;if(end!==',')fail(400,'invalid_argument');}}
  const match=/^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(text.slice(at));if(!match)fail(400,'invalid_argument');at+=match[0].length;return JSON.parse(match[0]);
 };
 const result=value(0);ws();if(at!==text.length)fail(400,'invalid_argument');return result;
}
function text(value:unknown,max=4096):string {if(typeof value!=='string'||new TextEncoder().encode(value).length>max)fail(400,'invalid_argument');return value;}
export function parseArchiveManifest(raw:unknown):ArchiveManifest {
 const m=record(raw,['schema','account_id','archive_id','build_id','tables','page_count','chain_root','rows','bytes','payloads']);if(m.schema!==1||!Array.isArray(m.tables)||m.tables.length>512)fail(400,'invalid_argument');
 const seen=new Set<string>();const tables=m.tables.map(v=>{const t=record(v,['name','columns','rows']);const n=text(t.name,256);if(seen.has(n)||!Array.isArray(t.columns)||t.columns.length>256)fail(400,'invalid_argument');seen.add(n);const columns=t.columns.map(c=>text(c,256));if(new Set(columns).size!==columns.length)fail(400,'invalid_argument');return{name:n,columns,rows:integer(t.rows,MAX_ARCHIVE_ROWS)};});
 return{schema:1,account_id:identifier(m.account_id),archive_id:digest(m.archive_id),build_id:digest(m.build_id),tables,page_count:integer(m.page_count,MAX_ARCHIVE_PAGES),chain_root:digest(m.chain_root),rows:integer(m.rows,MAX_ARCHIVE_ROWS),bytes:integer(m.bytes,MAX_ARCHIVE_BYTES),payloads:integer(m.payloads,MAX_ARCHIVE_PAYLOADS)};
}
function cell(raw:unknown):ArchiveCell {const c=record(raw,['kind','value','hex'],['kind']);switch(c.kind){case'null':record(c,['kind']);return{kind:'null'};case'text':record(c,['kind','value']);return{kind:'text',value:text(c.value,MAX_ARCHIVE_ROW)};case'integer':record(c,['kind','value']);if(typeof c.value!=='number'||!Number.isSafeInteger(c.value))fail(400,'invalid_argument');return{kind:'integer',value:c.value};case'real':record(c,['kind','value']);if(typeof c.value!=='string'||!Number.isFinite(Number(c.value))||Math.abs(Number(c.value))>Number.MAX_SAFE_INTEGER||String(Number(c.value))!==c.value)fail(400,'invalid_argument');return{kind:'real',value:c.value};case'blob':record(c,['kind','hex']);if(typeof c.hex!=='string'||c.hex.length>MAX_ARCHIVE_ROW*2||!/^(?:[0-9a-f]{2})*$/.test(c.hex))fail(400,'invalid_argument');return{kind:'blob',hex:c.hex};default:fail(400,'invalid_argument');}}
export function parseArchivePage(raw:unknown):ArchivePage {
 const p=record(raw,['schema','account_id','archive_id','build_id','sequence','previous','hash','content']);if(p.schema!==1)fail(400,'invalid_argument');const c=record(p.content,['kind','table','columns','rows','capsule_id','sequence','attempt_id','object_kind','object_id','size','sha256','locator','state','source_etag','source_version','presence','bytes_hex'],['kind']);let content:ArchiveContent;
 if(c.kind==='rows'){record(c,['kind','table','columns','rows']);if(!Array.isArray(c.columns)||c.columns.length>256||!Array.isArray(c.rows)||c.rows.length<1||c.rows.length>128)fail(400,'invalid_argument');content={kind:'rows',table:text(c.table,256),columns:c.columns.map(x=>text(x,256)),rows:c.rows.map(x=>{const r=record(x,['rowid','cells']);if(typeof r.rowid!=='number'||!Number.isSafeInteger(r.rowid)||!Array.isArray(r.cells)||r.cells.length!== (c.columns as unknown[]).length)fail(400,'invalid_argument');const row={rowid:r.rowid,cells:r.cells.map(cell)};if(new TextEncoder().encode(canonicalArchive(row)).length>MAX_ARCHIVE_ROW)fail(413,'resource_exhausted');return row;})};}
 else if(c.kind==='payload'){record(c,['kind','attempt_id','object_kind','object_id','size','locator','state','source_etag','source_version','presence','bytes_hex']);const kind=integer(c.object_kind,5);if(kind<1||!['pending','written','retiring'].includes(String(c.state))||!['present','absent'].includes(String(c.presence)))fail(400,'invalid_argument');if(c.source_etag!==null)text(c.source_etag,1024);if(c.source_version!==null)text(c.source_version,1024);if(c.presence==='present'){if(typeof c.bytes_hex!=='string'||c.bytes_hex.length>2*1024*1024||!/^(?:[0-9a-f]{2})*$/.test(c.bytes_hex))fail(400,'invalid_argument');}else if(c.bytes_hex!==null||c.state==='written')fail(400,'invalid_argument');content={kind:'payload',attempt_id:digest(c.attempt_id),object_kind:kind,object_id:digest(c.object_id),size:integer(c.size,kind===1?1024*1024:256*1024),locator:text(c.locator,512),state:c.state as 'written',source_etag:c.source_etag as string|null,source_version:c.source_version as string|null,presence:c.presence as 'present',bytes_hex:c.bytes_hex as string|null};}
 else if(c.kind==='private_payload'){
  record(c,['kind','capsule_id','sequence','attempt_id','size','sha256','locator','state','source_etag','source_version','presence','bytes_hex']);
  const size=integer(c.size,65536);if(size<1||!['pending','written','retiring','tombstoned'].includes(String(c.state))||!['present','absent'].includes(String(c.presence)))fail(400,'invalid_argument');
  if(c.source_etag!==null)text(c.source_etag,1024);if(c.source_version!==null)text(c.source_version,1024);
  if(c.presence==='present'){const marker=c.bytes_hex===''&&['retiring','tombstoned'].includes(String(c.state));if(!marker&&(c.state==='tombstoned'||typeof c.bytes_hex!=='string'||c.bytes_hex.length!==size*2||!/^(?:[0-9a-f]{2})+$/.test(c.bytes_hex)))fail(400,'invalid_argument');}
  else if(c.bytes_hex!==null||c.state==='written'||c.state==='tombstoned')fail(400,'invalid_argument');
  content={kind:'private_payload',capsule_id:digest(c.capsule_id),sequence:integer(c.sequence,8191),attempt_id:digest(c.attempt_id),size,sha256:digest(c.sha256),locator:text(c.locator,512),state:c.state as 'written',source_etag:c.source_etag as string|null,source_version:c.source_version as string|null,presence:c.presence as 'present',bytes_hex:c.bytes_hex as string|null};
 }
 else fail(400,'invalid_argument');
 const page:ArchivePage={schema:1,account_id:identifier(p.account_id),archive_id:digest(p.archive_id),build_id:digest(p.build_id),sequence:integer(p.sequence,MAX_ARCHIVE_PAGES-1),previous:digest(p.previous),hash:digest(p.hash),content};if(new TextEncoder().encode(canonicalArchive(page)).length>MAX_ARCHIVE_PAGE)fail(413,'resource_exhausted');return page;
}
export function parseArchiveCall(raw:unknown):ArchiveCall {
 const c=record(raw,['schema','account_id','archive_id','action','build_id','sequence','manifest','page'],['schema','account_id','archive_id','action']);const base={schema:1 as const,account_id:identifier(c.account_id),archive_id:digest(c.archive_id)};if(c.schema!==1)fail(400,'invalid_argument');switch(c.action){
 case'export_begin':record(c,['schema','account_id','archive_id','action','build_id'],['schema','account_id','archive_id','action']);return{...base,action:c.action,...(c.build_id===undefined?{}:{build_id:digest(c.build_id)})};
 case'export_next':record(c,['schema','account_id','archive_id','action','sequence']);return{...base,action:c.action,sequence:integer(c.sequence,MAX_ARCHIVE_PAGES)};
 case'export_finish':case'restore_begin':record(c,['schema','account_id','archive_id','action','manifest']);return{...base,action:c.action,manifest:parseArchiveManifest(c.manifest)};
 case'restore_page':record(c,['schema','account_id','archive_id','action','page']);return{...base,action:c.action,page:parseArchivePage(c.page)};
 case'export_abort':case'status':case'restore_finish':record(c,['schema','account_id','archive_id','action']);return{...base,action:c.action};default:fail(400,'invalid_argument');}
}
function grants(secret:string|undefined):Grant[]{try{if(!secret||secret.length>128*1024)throw 0;const raw=strictArchiveJson(secret);if(!Array.isArray(raw)||raw.length<1||raw.length>32)throw 0;const seen=new Set<string>();return raw.map(x=>{const p=record(x,['principal','token_sha256','accounts','export','restore']);if(typeof p.principal!=='string'||!/^[A-Za-z0-9_.-]{1,64}$/.test(p.principal)||typeof p.export!=='boolean'||typeof p.restore!=='boolean'||!Array.isArray(p.accounts)||p.accounts.length>64)throw 0;const token=digest(p.token_sha256);if(seen.has(token))throw 0;seen.add(token);return{principal:p.principal,token_sha256:token,accounts:[...new Set(p.accounts.map(identifier))].sort(),export:p.export,restore:p.restore};});}catch{fail(503,'archive_configuration_invalid');}}
async function credential(p:Grant):Promise<string>{return hash('tinytree-account-archive-credential-v1\0'+canonicalArchive(p));}
export async function authenticateArchive(request:Request,secret:string|undefined):Promise<ArchiveAuth>{const auth=request.headers.get('authorization')??'';if(!auth.startsWith('Bearer ')||auth.length>4120)fail(401,'authentication_required');const actual=await hash(auth.slice(7));for(const p of grants(secret)){let different=0;for(let i=0;i<64;i++)different|=actual.charCodeAt(i)^p.token_sha256.charCodeAt(i);if(!different)return{principal:p.principal,credential_id:await credential(p)};}fail(401,'authentication_required');}
export async function requireArchiveAuth(secret:string|undefined,auth:ArchiveAuth,account:string,permission:'export'|'restore'|'either'):Promise<void>{for(const p of grants(secret))if(p.principal===auth.principal&&await credential(p)===auth.credential_id){if(!p.accounts.includes(account)||(permission==='either'?!p.export&&!p.restore:!p[permission]))fail(403,'permission_denied');return;}fail(401,'authentication_required');}
export async function admitArchive(request:Request,env:{ARCHIVERS_JSON?:string}):Promise<{auth:ArchiveAuth;call:ArchiveCall;bytes:number}>{if(request.method!=='POST')fail(405,'method_not_allowed');const auth=await authenticateArchive(request,env.ARCHIVERS_JSON);if(request.headers.get('content-type')?.split(';')[0]!=='application/json')fail(415,'json_required');const bytes=await body(request,MAX_ARCHIVE_PAGE+65536);let raw:unknown;try{raw=strictArchiveJson(new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(bytes));}catch(e){if(e instanceof Failure)throw e;fail(400,'invalid_argument');}const call=parseArchiveCall(raw);await requireArchiveAuth(env.ARCHIVERS_JSON,auth,call.account_id,call.action==='status'?'either':call.action.startsWith('restore')?'restore':'export');return{auth,call,bytes:bytes.length};}
export function archiveResponse(reply:ArchiveReply):Response{return json(reply);}
export async function protectArchive(action:()=>Promise<Response>):Promise<Response>{try{return await action();}catch(e){return json({schema:1,error:{code:e instanceof Failure?e.code:'unavailable'}},e instanceof Failure?e.status:503);}}
