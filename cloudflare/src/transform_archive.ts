/** Closed local transform custody only. Historical origin data is not a source
 * credential, export permit renewal, publication grant or public read grant. */
import { buildForgeTransform, deriveForgeTransformIdentity, inspect, mapForgeTransformSelection, normalizeForgeTransformCommand, type Descriptor } from './core';
import { canonicalArchive } from './archive_protocol';
import { fail, hash } from './protocol';
import type { SourceExportBinding, SourcePermit, TransformDraftBinding, TransformStart } from './forge_transforms';

type Cell = string | number | null | ArrayBuffer;
type Row = Record<string, Cell>;
type State = Map<string, Row[]>;
interface Draft extends Row { id:string;account:string;principal:string;request_id:string;fingerprint:string;payload:string;binding:string;committer:string;profile:string;phase:string;generation:number;export_id:string|null;source:string|null;lineage:string;plan_hash:string|null;plan_bytes:number;artifact_job:string|null;output_job:string|null;permit:string|null;run_id:string|null;publication_digest:string|null;settled:number }
interface Run extends Row {id:string;account:string;principal:string;draft:string;binding:string;value:string;artifact_proof:string;output_proof:string;generation:number;retired:number}
interface ExportRow extends Row {id:string;account:string;principal:string;draft:string;payload:string;epoch:number;before_revision:string;after_revision:string;phase:string;generation:number;manifest:string|null;selected:number;lineage:string;permit:string|null}
interface Selection {side:'before'|'after';ordinal:number;path:string;canonical_path:string;descriptor:Descriptor;executable:boolean;copy_id:string|null}
interface Mapped {mapped_path:string;destination_path:string;source_relative_path:string;source_canonical_path:string;descriptor:Descriptor;executable:boolean}
interface Lineage {operation:string;source_path:string;target_path:string;source_canonical_path:string;target_canonical_path:string}
interface NativeRun {id:number[];spec:number[];source_account:string;source_before:number[];source_after:number[];source_before_view:number[];source_after_view:number[];destination_account:string;destination_view:number[];destination_base:number[];output_head:number[];lineage:unknown[];destination_paths:string[];pin:{id:number[];artifact:number[];reference:string;generation:number;retired:boolean;protected_roots:string[]}}
interface Generated extends Descriptor {bytes_base64:string}
interface Plan {state:'ready';run:NativeRun;objects:Generated[]}
export interface ClosedTransformArchiveHost {sql:SqlStorage;accountId:string;assertAuthorized():Promise<void>}
export interface PreparedClosedTransformArchive {assertCurrentInTx():void}
const TABLES=['transform_usage','transform_drafts','transform_exports','transform_selection','transform_pack','transform_plan','transform_runs','transform_pin_owners','transform_receipts','transform_recovery','transform_recovery_schedule'] as const;
const MAX_ROWS=262144,MAX_BYTES=32*1024*1024,MAX_PLAN=4*1024*1024,ZERO='0'.repeat(64);
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true,ignoreBOM:false});
function invalid():never {fail(503,'archive_transform_invalid');}
function requireThat(ok:unknown):asserts ok {if(!ok)invalid();}
function safe(n:unknown,max=Number.MAX_SAFE_INTEGER):n is number {return typeof n==='number'&&Number.isSafeInteger(n)&&n>=0&&n<=max;}
function id(s:unknown,n=64):s is string {return typeof s==='string'&&s.length===n&&/^[0-9a-f]+$/.test(s);}
function jobId(value:unknown):boolean {return typeof value==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value);}
function hex(value:unknown,n=32):string {requireThat(Array.isArray(value)&&value.length===n&&value.every(v=>safe(v,255)));return value.map(v=>v.toString(16).padStart(2,'0')).join('');}
function array(value:string):number[] {requireThat(id(value));return Array.from(value.match(/../g)!,v=>parseInt(v,16));}
function json<T>(value:unknown,max=1048576):T {requireThat(typeof value==='string'&&encoder.encode(value).length<=max);try{return JSON.parse(value) as T;}catch{invalid();}}
function same(a:unknown,b:unknown):boolean {return canonicalArchive(a)===canonicalArchive(b);}
function quoted(name:string):string {requireThat(/^[a-z_][a-z0-9_]*$/.test(name));return '"'+name+'"';}
function rows<T>(sql:SqlStorage,query:string,...args:(string|number|null)[]):T[]{try{return [...sql.exec(query,...args)] as T[];}catch{invalid();}}
function one<T>(sql:SqlStorage,query:string,...args:(string|number|null)[]):T|undefined{return rows<T>(sql,query,...args)[0];}
function exists(sql:SqlStorage,table:string):boolean {return !!one(sql,"SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?",table);}
function list<T>(state:State,table:string):T[]{return (state.get(table)??[]) as T[];}
function rawEqual(a:Row,b:Row):boolean {
 const keys=Object.keys(a);if(keys.length!==Object.keys(b).length)return false;
 return keys.every(k=>{const x=a[k],y=b[k];if(x instanceof ArrayBuffer||y instanceof ArrayBuffer){if(!(x instanceof ArrayBuffer)||!(y instanceof ArrayBuffer)||x.byteLength!==y.byteLength)return false;const l=new Uint8Array(x),r=new Uint8Array(y);return l.every((v,i)=>v===r[i]);}return x===y;});
}
function stateEqual(a:State,b:State):boolean {return a.size===b.size&&[...a].every(([name,values])=>{const other=b.get(name);return !!other&&values.length===other.length&&values.every((v,i)=>rawEqual(v,other[i]));});}
/** Admit retained text/blob and collection overhead BEFORE loading rows. The
 * snapshot and its exact final comparison are local bounded validation work. */
function snapshot(sql:SqlStorage):State {
 const found=rows<{name:string}>(sql,"SELECT name FROM sqlite_schema WHERE type='table' AND (name GLOB 'transform_*' OR name GLOB 'export_*') ORDER BY name");
 let count=0,bytes=0;
 for(const {name} of found){
  if(!(TABLES as readonly string[]).includes(name)){if(one(sql,`SELECT 1 FROM ${quoted(name)} LIMIT 1`))fail(400,'failed_precondition');continue;}
  const columns=rows<{name:string}>(sql,`PRAGMA table_info(${quoted(name)})`);requireThat(columns.length>0&&columns.length<=64);
  const charge=columns.map(c=>`CASE typeof(${quoted(c.name)}) WHEN 'text' THEN 2*length(CAST(${quoted(c.name)} AS BLOB)) WHEN 'blob' THEN length(${quoted(c.name)}) ELSE 16 END`).join('+');
  const size=one<{n:number;bytes:number}>(sql,`SELECT COUNT(*) AS n,COALESCE(SUM(${charge}),0) AS bytes FROM ${quoted(name)}`)!;
  requireThat(safe(size.n,MAX_ROWS)&&safe(size.bytes));count+=size.n;bytes+=size.bytes+size.n*(128+columns.length*16);
  if(count>MAX_ROWS||!safe(bytes)||bytes>MAX_BYTES)fail(429,'resource_exhausted');
 }
 const state:State=new Map();for(const {name} of found)if((TABLES as readonly string[]).includes(name))state.set(name,rows<Row>(sql,`SELECT rowid AS snapshot_rowid,* FROM ${quoted(name)} ORDER BY rowid`));return state;
}
function keyRows(state:State):{drafts:Map<string,Draft>;runs:Map<string,Run>;exports:Map<string,ExportRow>} {
 return {drafts:new Map(list<Draft>(state,'transform_drafts').map(d=>[d.id,d])),runs:new Map(list<Run>(state,'transform_runs').map(r=>[r.id,r])),exports:new Map(list<ExportRow>(state,'transform_exports').map(e=>[e.id,e]))};
}
function source(d:Draft):{binding:TransformDraftBinding;payload:TransformStart;source:SourceExportBinding} {
 const binding=json<TransformDraftBinding>(d.binding),payload=json<TransformStart>(d.payload),s=json<SourceExportBinding>(d.source);
 requireThat(binding.draft_id===d.id&&binding.destination_account===d.account&&binding.principal===d.principal&&binding.request_fingerprint===d.fingerprint&&same(s.draft,binding)&&s.export_id===d.export_id&&s.source_account===payload.source_account&&s.source_account!==d.account&&s.source_view===payload.source_view&&s.before_view===payload.source_before_view&&s.after_view===payload.source_after_view&&id(s.before_revision)&&id(s.after_revision)&&id(s.manifest_digest)&&safe(s.source_epoch)&&s.source_epoch>0);return{binding,payload,source:s};
}
function transformIdentity(b:TransformDraftBinding,p:TransformStart,s:SourceExportBinding):unknown {
 requireThat(id(p.change_id,32));
 return{spec:array(b.spec_id),source_account:s.source_account,before:array(s.before_revision),after:array(s.after_revision),before_view:array(s.before_view),after_view:array(s.after_view),before_view_name:s.source_view,after_view_name:s.source_view,destination_account:b.destination_account,destination_view:array(b.destination_view),destination_view_name:b.destination_view_name,destination_base:array(b.destination_base),change_id:Array.from(p.change_id.match(/../g)!,v=>parseInt(v,16)),lineage:p.lineage};
}
function identity(d:Draft):unknown {const {binding,payload,source:s}=source(d);return transformIdentity(binding,payload,s);}
function checkProof(sql:SqlStorage,account:string,d:Draft,root:string,jobId:string,side:'artifact'|'output',physical:boolean):void {
 const proof=one<Record<string,unknown>>(sql,'SELECT * FROM closure_jobs WHERE id=?',jobId);const {binding:b,source:s}=source(d);
 const publication=JSON.stringify([d.id,d.run_id,d.plan_hash,side,root,s.manifest_digest]);
 const binding=JSON.stringify([account,{kind:'principal',principal:d.principal},b.destination_epoch,'transform',publication]);
 requireThat(proof&&proof.account===account&&proof.root_kind===3&&proof.root_id===root&&proof.phase==='published'&&proof.binding===binding&&safe(proof.nodes)&&proof.nodes===proof.checked_nodes&&safe(proof.contexts)&&proof.contexts===proof.checked_contexts&&safe(proof.obligations)&&proof.obligations===proof.checked_obligations);
 for(const [table,total]of [['closure_nodes',proof.nodes],['closure_contexts',proof.contexts],['closure_obligations',proof.obligations]] as const){const actual=one<{n:number;done:number}>(sql,`SELECT COUNT(*) AS n,COALESCE(SUM(done),0) AS done FROM ${table} WHERE job=?`,jobId)!;requireThat(actual.n===total&&actual.done===total);}
 requireThat(one(sql,'SELECT 1 FROM closure_nodes WHERE job=? AND kind=3 AND id=? AND done=1',jobId,root));
 if(!physical)return;
 const missing=one<{n:number}>(sql,"SELECT COUNT(*) AS n FROM closure_nodes n LEFT JOIN objects o ON o.kind=n.kind AND o.id=n.id LEFT JOIN forge_storage_bindings b ON b.kind=n.kind AND b.id=n.id LEFT JOIN forge_storage_attempts a ON a.attempt_id=b.attempt_id LEFT JOIN archive_payload_proofs p ON p.attempt_id='native:'||a.attempt_id WHERE n.job=? AND NOT(n.kind=3 AND n.id=?) AND (n.done<>1 OR o.visible IS NULL OR o.visible<>1 OR a.state IS NULL OR a.state<>'written' OR a.kind<>n.kind OR a.id<>n.id OR a.size<>o.size OR p.presence IS NULL OR p.presence<>'present')",jobId,ZERO)!;
 requireThat(missing.n===0);
}
function checkRetainedReviewOwners(sql:SqlStorage,account:string,runs:Map<string,Run>):void {
 if(!exists(sql,'review_patchsets'))return;
 const retained="(EXISTS(SELECT 1 FROM forge_retained_roots r WHERE r.owner_kind='review-input' AND r.owner_id=p.id AND r.active=1) OR EXISTS(SELECT 1 FROM review_attempts a JOIN forge_retained_roots r ON r.owner_kind='review-candidate' AND r.owner_id=a.id AND r.active=1 WHERE a.patchset=p.id))";
 const count=one<{n:number}>(sql,`SELECT COUNT(*) AS n FROM review_patchsets p WHERE json_type(p.intent,'$.provenance') IS NOT NULL AND json_type(p.intent,'$.provenance')<>'null' AND ${retained}`)!;
 if(!safe(count.n,65536))fail(429,'resource_exhausted');
 const oversized=one(sql,`SELECT 1 FROM review_patchsets p WHERE json_type(p.intent,'$.provenance') IS NOT NULL AND json_type(p.intent,'$.provenance')<>'null' AND ${retained} AND length(CAST(json_extract(p.intent,'$.provenance') AS BLOB))>1024 LIMIT 1`);requireThat(!oversized);
 let after='',seen=0;
 while(seen<count.n){
  const page=rows<{id:string;account:string;source_base:string;source_head:string;provenance:string}>(sql,`SELECT p.id,p.account,p.source_base,p.source_head,json_extract(p.intent,'$.provenance') AS provenance FROM review_patchsets p WHERE p.id>? AND json_type(p.intent,'$.provenance') IS NOT NULL AND json_type(p.intent,'$.provenance')<>'null' AND ${retained} ORDER BY p.id LIMIT 128`,after);
  requireThat(page.length>0);
  for(const patch of page){requireThat(id(patch.id)&&patch.id>after&&patch.account===account);const runId=hex(json<number[]>(patch.provenance,1024)),run=runs.get(runId);requireThat(run&&run.retired===0);const value=json<NativeRun>(run.value);
   requireThat(patch.source_base===hex(value.destination_base)&&patch.source_head===hex(value.output_head)&&one(sql,"SELECT 1 FROM transform_pin_owners WHERE run_id=? AND kind='review' AND owner=?",runId,`review-${patch.id}`));after=patch.id;seen++;}
 }
 requireThat(seen===count.n);
}
function classify(sql:SqlStorage,account:string,state:State,physical=false):void {
 const {drafts,runs,exports}=keyRows(state);
 requireThat(drafts.size<=4096&&runs.size<=1024&&exports.size<=4096);
 for(const key of drafts.keys())requireThat(!exports.has(key));
 const recovery=list<Row>(state,'transform_recovery');requireThat(recovery.length<=1&&recovery.every(r=>r.singleton===1&&(r.cursor===''||id(r.cursor))));
 const schedule=list<Row>(state,'transform_recovery_schedule');requireThat(schedule.length<=1&&schedule.every(r=>r.singleton===1&&safe(r.due)));
 for(const d of drafts.values()) {
  requireThat(id(d.id)&&d.account===account&&safe(d.generation)&&typeof d.principal==='string');
  if(!['committed','aborted'].includes(d.phase)||(d.phase==='committed'&&d.settled!==1))fail(400,'failed_precondition');
  if(d.phase==='committed')requireThat(d.run_id&&runs.has(d.run_id));
  else requireThat(d.permit===null&&d.publication_digest===null);
 }
 for(const e of exports.values()) {requireThat(e.account===account&&id(e.id)&&safe(e.generation));if(!['settled_committed','settled_aborted'].includes(e.phase))fail(400,'failed_precondition');}
 const children=[['transform_selection','owner'],['transform_pack','owner'],['transform_plan','owner']] as const;
 for(const [table,column]of children)for(const row of list<Row>(state,table)){const owner=String(row[column]);requireThat(drafts.has(owner)||exports.has(owner));if(table!=='transform_selection')requireThat(drafts.has(owner));}
 if(exists(sql,'forge_retained_roots')) {
  if(one(sql,"SELECT 1 FROM forge_retained_roots WHERE active=1 AND owner_kind LIKE 'transform-stage-%' LIMIT 1"))fail(400,'failed_precondition');
  const orphan=one(sql,"SELECT 1 FROM forge_retained_roots r LEFT JOIN transform_runs t ON t.id=r.owner_id WHERE r.active=1 AND r.owner_kind LIKE 'transform-%' AND (r.owner_kind NOT IN ('transform-pin','transform-output') OR t.id IS NULL OR t.retired<>0) LIMIT 1");requireThat(!orphan);
 }
 if(exists(sql,'forge_retained_roots'))for(const root of rows<Row>(sql,"SELECT owner_kind,owner_id,kind,id,active FROM forge_retained_roots WHERE owner_kind LIKE 'transform-%' ORDER BY owner_kind,owner_id,kind,id")) {
  if(root.owner_kind==='transform-stage-artifact'||root.owner_kind==='transform-stage-output')requireThat(drafts.has(String(root.owner_id))&&root.active===0&&root.kind===3&&id(root.id));
  else {const run=runs.get(String(root.owner_id));requireThat(run&&root.kind===3);const native=json<NativeRun>(run.value);requireThat((root.owner_kind==='transform-pin'&&root.id===hex(native.pin.artifact))||(root.owner_kind==='transform-output'&&root.id===hex(native.output_head)));}
 }
 for(const row of runs.values()) {
  const d=drafts.get(row.draft);requireThat(d&&d.phase==='committed'&&d.settled===1&&d.run_id===row.id&&row.account===account&&row.principal===d.principal&&row.binding===d.source&&id(row.id)&&safe(row.generation)&&row.generation>=1&&[0,1].includes(row.retired));
  const run=json<NativeRun>(row.value);requireThat(hex(run.id)===row.id&&run.destination_account===account&&run.pin.retired===(row.retired===1)&&run.pin.generation===row.generation);
  if(row.retired){requireThat(!one(sql,'SELECT 1 FROM transform_pin_owners WHERE run_id=? LIMIT 1',row.id)&&!one(sql,"SELECT 1 FROM forge_retained_roots WHERE owner_kind IN ('transform-pin','transform-output') AND owner_id=? AND active=1 LIMIT 1",row.id));continue;}
  for(const [kind,root,job,side]of [['transform-pin',hex(run.pin.artifact),row.artifact_proof,'artifact'],['transform-output',hex(run.output_head),row.output_proof,'output']] as const) {
   requireThat(one(sql,'SELECT 1 FROM forge_retained_roots WHERE owner_kind=? AND owner_id=? AND kind=3 AND id=? AND active=1',kind,row.id,root));
   requireThat(one(sql,'SELECT 1 FROM objects WHERE kind=3 AND id=? AND visible=1',root));checkProof(sql,account,d,root,job,side,physical);
  }
 }
 for(const owner of list<Row>(state,'transform_pin_owners')) {
  const run=runs.get(String(owner.run_id));requireThat(run&&run.retired===0);
  if(owner.kind==='transform'){requireThat(owner.owner===`transform-${run.id}`);continue;}
  requireThat(owner.kind==='review'&&typeof owner.owner==='string'&&/^review-[0-9a-f]{64}$/.test(owner.owner));
  const patch=one<Row>(sql,'SELECT * FROM review_patchsets WHERE id=?',owner.owner.slice(7));requireThat(patch&&patch.account===account);
  const intent=json<{provenance:number[]|null}>(patch.intent);const value=json<NativeRun>(run.value);
  requireThat(intent.provenance!==null&&hex(intent.provenance)===run.id&&patch.source_base===hex(value.destination_base)&&patch.source_head===hex(value.output_head));
 }
 checkRetainedReviewOwners(sql,account,runs);
 if(exists(sql,'forge_storage_reservations'))for(const row of rows<{owner_id:string}>(sql,"SELECT DISTINCT owner_id FROM forge_storage_reservations WHERE owner_kind='transform' ORDER BY owner_id")) {
  const d=drafts.get(row.owner_id);requireThat(d&&['committed','aborted'].includes(d.phase));
  requireThat(one(sql,"SELECT 1 FROM forge_storage_terminal_owners WHERE owner_kind='transform' AND owner_id=?",row.owner_id));
 }
 if(exists(sql,'forge_storage_terminal_owners'))for(const owner of rows<{owner_id:string}>(sql,"SELECT owner_id FROM forge_storage_terminal_owners WHERE owner_kind='transform' ORDER BY owner_id")){const draft=drafts.get(owner.owner_id);requireThat(draft&&['committed','aborted'].includes(draft.phase));}
 for(const row of list<Row>(state,'transform_receipts')) {
  requireThat(row.account===account&&typeof row.principal==='string'&&typeof row.request_id==='string'&&id(row.fingerprint));
  const receipt=json<{id:string;artifact:string;reference:string;generation:number;retired:boolean;protected_roots:string[]}>(row.value);
  const run=[...runs.values()].find(r=>hex(json<NativeRun>(r.value).pin.id)===receipt.id);requireThat(run&&run.retired===1);const pin=json<NativeRun>(run.value).pin;
  requireThat(receipt.artifact===hex(pin.artifact)&&receipt.reference===pin.reference&&receipt.retired===true&&receipt.generation===pin.generation&&Array.isArray(receipt.protected_roots)&&receipt.protected_roots.length===0);
 }

 const usage=list<Row>(state,'transform_usage');requireThat(usage.length<=1);
 if(usage.length){const allocated=[...state].reduce((n,[name,values])=>n+(['transform_usage','transform_recovery','transform_recovery_schedule'].includes(name)?0:values.length),0);requireThat(usage[0].account===account&&safe(usage[0].rows,MAX_ROWS)&&safe(usage[0].bytes,MAX_BYTES)&&allocated<=usage[0].rows);}
 else requireThat([...state].every(([name,values])=>['transform_recovery','transform_recovery_schedule'].includes(name)||values.length===0));
}
/** Cheap fail-closed admission/CAS gate; never substitutes for the asynchronous
 * canonical verification before export sealing and target activation. */
export function requireClosedTransformArchiveInTx(sql:SqlStorage,account:string):void {classify(sql,account,snapshot(sql));}
function selections(state:State,owner:string):Selection[] {
 const rows=list<Row>(state,'transform_selection').filter(r=>r.owner===owner).sort((a,b)=>Number(a.ordinal)-Number(b.ordinal));requireThat(rows.length<=2048);
 return rows.map((r,i)=>{const value=json<Selection>(r.value);requireThat(value.ordinal===i&&r.ordinal===i&&value.side===r.side&&value.path===r.path&&r.ready===1);return value;});
}
function mapped(p:TransformStart,root:string,selection:Selection[]):Mapped[] {
 let state:unknown=null;const result:Mapped[]=[];
 for(let offset=0;offset<selection.length;offset+=128){const batch=mapForgeTransformSelection({spec:p.spec,destination_root:root,state,rows:selection.slice(offset,offset+128).map(r=>({relative_path:r.path,canonical_path:r.canonical_path,descriptor:r.descriptor,executable:r.executable,copy_id:r.copy_id}))}) as{files:Mapped[];state:unknown};requireThat(Array.isArray(batch.files)&&batch.files.length<=128);result.push(...batch.files);state=batch.state;}return result;
}
function planBytes(state:State,d:Draft):Uint8Array {
 requireThat(safe(d.plan_bytes,MAX_PLAN)&&d.plan_bytes>0&&id(d.plan_hash));const chunks=list<Row>(state,'transform_plan').filter(r=>r.owner===d.id).sort((a,b)=>Number(a.ordinal)-Number(b.ordinal));
 const bytes=new Uint8Array(d.plan_bytes);let offset=0;chunks.forEach((row,i)=>{requireThat(row.ordinal===i&&row.data instanceof ArrayBuffer&&row.data.byteLength<=65536&&row.data.byteLength<=bytes.length-offset);bytes.set(new Uint8Array(row.data),offset);offset+=row.data.byteLength;});requireThat(offset===bytes.length);return bytes;
}
function b64(bytes:Uint8Array):string {let s='';for(let n=0;n<bytes.length;n+=8192)s+=String.fromCharCode(...bytes.subarray(n,n+8192));return btoa(s);}
function nativePack(state:State,d:Draft):Generated[] {
 const pack=list<Row>(state,'transform_pack').filter(r=>r.owner===d.id).sort((a,b)=>Number(a.kind)-Number(b.kind)||String(a.id).localeCompare(String(b.id)));requireThat(pack.length<=8192);let total=0;
 return pack.map(row=>{requireThat([2,3].includes(Number(row.kind))&&id(row.id)&&safe(row.size,256*1024)&&row.data instanceof ArrayBuffer&&row.data.byteLength===row.size);total+=row.size;requireThat(total<=MAX_PLAN);const bytes=new Uint8Array(row.data),info=inspect(Number(row.kind),bytes);requireThat(info.id===row.id&&info.size_bytes===row.size&&info.kind===row.kind);return{kind:info.kind,id:info.id,size_bytes:info.size_bytes,format_version:info.format_version,bytes_base64:b64(bytes)};});
}
async function verifyDraft(host:ClosedTransformArchiveHost,state:State,d:Draft):Promise<Plan> {
 const {binding:b,payload:p,source:s}=source(d);
 let fingerprint=false;
 for(const operation of [30,32] as const)for(const view of new Set(['',b.destination_view_name])) {
  const normalized=normalizeForgeTransformCommand({account_id:d.account,operation,view,request_id:d.request_id,payload_json:JSON.stringify(p)}) as{fingerprint:string;payload:TransformStart};
  if(normalized.fingerprint===d.fingerprint&&same(normalized.payload,p))fingerprint=true;
 }
 requireThat(fingerprint);
 const derived=deriveForgeTransformIdentity({kind:'run',identity:identity(d),spec:p.spec}) as{run_id:string;spec_id:string};requireThat(derived.run_id===d.run_id&&derived.spec_id===b.spec_id);
 const data=planBytes(state,d),text=decoder.decode(data);requireThat(await hash(text)===d.plan_hash);await host.assertAuthorized();const plan=json<Plan>(text,MAX_PLAN);requireThat(plan.state==='ready');
 const selection=selections(state,d.id),before=mapped(p,b.destination_root,selection.filter(r=>r.side==='before')),after=mapped(p,b.destination_root,selection.filter(r=>r.side==='after'));
 const lineage:unknown[]=[];const verified=json<Lineage[]>(d.lineage);
 requireThat(verified.length===p.lineage.length);
 for(const [i,hint]of verified.entries()) {requireThat(same({operation:hint.operation,source_path:hint.source_path,target_path:hint.target_path},p.lineage[i]));const target=after.find(f=>f.source_relative_path===hint.target_path);requireThat(target&&target.source_canonical_path===hint.target_canonical_path);lineage.push({operation:hint.operation,source_account:s.source_account,source_revision:array(s.before_revision),source_path:hint.source_canonical_path,destination_path:target.destination_path,destination_revision:Array(32).fill(0)});}
 for(const file of after)lineage.push({operation:'imported_origin',source_account:s.source_account,source_revision:array(s.after_revision),source_path:file.source_canonical_path,destination_path:file.destination_path,destination_revision:Array(32).fill(0)});
 const rebuilt=buildForgeTransform({identity:identity(d),committer:json(d.committer),destination_root:b.destination_root,before:before.map(f=>({path:f.mapped_path,file:{id:f.descriptor.id,size:f.descriptor.size_bytes,executable:f.executable}})),after:after.map(f=>({path:f.mapped_path,file:{id:f.descriptor.id,size:f.descriptor.size_bytes,executable:f.executable}})),lineage,objects:nativePack(state,d)}) as Plan;
 requireThat(rebuilt.state==='ready'&&same(rebuilt,plan)&&hex(plan.run.id)===d.run_id);
 const permit=json<SourcePermit>(d.permit),prepared=permit.prepared;
 requireThat(id(permit.permit_id)&&permit.export_id===d.export_id&&same(prepared.binding,s)&&prepared.run_id===d.run_id&&prepared.plan_digest===d.plan_hash&&prepared.artifact===hex(plan.run.pin.artifact)&&prepared.output_head===hex(plan.run.output_head)&&prepared.artifact_proof===d.artifact_job&&prepared.output_proof===d.output_job&&safe(prepared.prepared_generation)&&prepared.prepared_generation<d.generation);
 requireThat(await hash(JSON.stringify({permit,artifact:hex(plan.run.pin.artifact),output_head:hex(plan.run.output_head)}))===d.publication_digest);await host.assertAuthorized();return plan;
}
async function verifyExport(host:ClosedTransformArchiveHost,state:State,e:ExportRow):Promise<void> {
 const b=json<TransformDraftBinding>(e.draft),p=json<TransformStart>(e.payload);requireThat(p.source_account===host.accountId&&b.destination_account!==host.accountId&&b.principal===e.principal&&id(b.draft_id)&&id(e.before_revision)&&id(e.after_revision)&&safe(e.epoch)&&e.epoch>0);
 if(e.manifest!==null) {
  const files=selections(state,e.id),lineage=json<Lineage[]>(e.lineage);requireThat(files.length===e.selected);
  requireThat(await hash(JSON.stringify({draft:b,source_account:e.account,before:e.before_revision,after:e.after_revision,epoch:e.epoch,files,lineage}))===e.manifest);await host.assertAuthorized();
 }
 if(e.phase==='settled_committed') {
  requireThat(e.manifest!==null);const permit=json<SourcePermit>(e.permit),binding:SourceExportBinding={export_id:e.id,draft:b,source_account:e.account,source_view:p.source_view,before_view:p.source_before_view,after_view:p.source_after_view,before_revision:e.before_revision,after_revision:e.after_revision,source_epoch:e.epoch,manifest_digest:e.manifest};
  const s=permit.prepared.binding;requireThat(permit.export_id===e.id&&id(permit.permit_id)&&same(s,binding));
  const derived=deriveForgeTransformIdentity({kind:'run',identity:transformIdentity(b,p,binding),spec:p.spec}) as{run_id:string;spec_id:string};
  requireThat(derived.run_id===permit.prepared.run_id&&derived.spec_id===b.spec_id&&id(permit.prepared.plan_digest)&&id(permit.prepared.artifact)&&id(permit.prepared.output_head)&&jobId(permit.prepared.artifact_proof)&&jobId(permit.prepared.output_proof)&&safe(permit.prepared.prepared_generation));
 }
}
/** Bounded canonical work is outside SQL; the returned token rechecks the exact
 * transform snapshot plus current local custody inside the final archive TX. */
export async function prepareClosedTransformArchive(host:ClosedTransformArchiveHost):Promise<PreparedClosedTransformArchive> {
 await host.assertAuthorized();const state=snapshot(host.sql);classify(host.sql,host.accountId,state,true);const {drafts,runs,exports}=keyRows(state);
 for(const d of drafts.values())if(d.phase==='committed') {
  let plan:Plan;try{plan=await verifyDraft(host,state,d);}catch(error){if(error instanceof Error&&'status'in error)throw error;invalid();}
  const row=runs.get(d.run_id!);requireThat(row);
  if(row.draft===d.id){const run=json<NativeRun>(row.value),expected={...plan.run,pin:{...plan.run.pin,generation:row.generation,retired:row.retired===1,protected_roots:row.retired?[]:plan.run.pin.protected_roots}};requireThat(same(run,expected));}
 }
 for(const e of exports.values())await verifyExport(host,state,e);
 await host.assertAuthorized();return{assertCurrentInTx:()=>{const current=snapshot(host.sql);requireThat(stateEqual(state,current));classify(host.sql,host.accountId,current,true);}};
}
