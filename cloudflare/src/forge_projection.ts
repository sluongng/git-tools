/** Durable internal sanitized projection preparation. Not an HTTP dispatcher,
 * actor resolver, read grant, or fake ResolveView snapshot. All public callers
 * share exact view/epoch/revision work and reauthorize on every continuation. */
import { buildForgeProjection, inspect, type Descriptor, type Inspection, type ObjectKey } from './core';
import type { ClosureBinding, ClosureJobs } from './closure_jobs';
import type { ForgeViews, ResolvedView, ViewDescription } from './forge_views';
import { fail, hash } from './protocol';

/** Trusted immutable ledger selection; never accepted from public request JSON. */
export interface ReviewProjectionSource {
  account_id: string; view_name: string; view_version: string;
  review_epoch: number; projection_epoch: number; change_id: string;
  patchset_id: string; candidate_id: string; attempt_generation: number;
  canonical_head: string; projected_parent: string;
}
export interface ProjectionHost {
  readonly accountId: string;
  readonly principal: string | null;
  readonly sql: SqlStorage;
  readonly views: ForgeViews;
  transactionSync<T>(callback: () => T): T;
  /** Exact view-owned canonical root + authoritative complete proof, not a hash
   * or a raw visible flag. Called under the same current-policy transaction. */
  requireSourceRootInTx(view: ResolvedView): void;
  requireReviewSourceInTx?(view: ResolvedView, source: Readonly<ReviewProjectionSource>): void;
  describeObjectInTx(key: ObjectKey): Descriptor | undefined;
  readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
  /** Reserve durable Forge quota including failed/uncertain writes BEFORE R2. */
  reserveGeneratedInTx(jobId: string, descriptor: Descriptor): void;
  putVerifiedGenerated(descriptor: Descriptor, bytes: Uint8Array): Promise<void>;
  /** Availability only; this must not mint actor upload or read authority. */
  confirmGeneratedInTx(jobId: string, descriptor: Descriptor): void;
  /** The factory authorizes the actual nullable caller against this view each
   * time. Binding identity is a view tag, not a synthesized member principal.
   * attachInTx is invoked inside ClosureJobs.publish's existing SQL transaction. */
  closureForProjection(view: ResolvedView, binding: ClosureBinding,
    attachInTx: (proofJobId: string) => unknown, authorizeCurrentInTx?: () => void): ClosureJobs;
  retainProjectionRootInTx(projectionId: string, head: string, proofJobId: string): void;
}
export interface ProjectionProgress { job_id: string; expected_generation: number }
type Phase = 'select' | 'build' | 'stage' | 'closure' | 'ready' | 'limit' | 'obsolete';
export interface ProjectionBinding {
  account_id: string; view_version: number[]; policy_epoch: number;
  canonical_revision: number[]; projected_commit: number[]; projected_tree: number[];
}
export interface ProjectionStatus {
  state: 'pending' | 'ready' | 'limit' | 'unavailable';
  job_id: string; generation: number; phase: Phase; selection: ViewDescription;
  counts: { selected_entries: number; confirmed_outputs: number; output_objects: number };
  error: string | null;
  projection?: ProjectionBinding;
}
export interface ReadyProjection {
  catalog_id: string; binding: ProjectionBinding; proof_job: string;
}
/** Private exact-baseline endpoint fact, not a public read/export capability.
 * Canonical copy IDs must never be serialized into destination/public replies. */
export interface CanonicalFileWitness {
  readonly catalog_id: string; readonly account_id: string;
  readonly view_version: string; readonly policy_epoch: number;
  readonly canonical_revision: string; readonly relative_path: string;
  readonly canonical_path: string; readonly file: Descriptor;
  readonly executable: boolean; readonly canonical_copy: string | null;
}
export interface ProjectionPath {
  path: string; canonical_path: string; descriptor: Descriptor;
  executable: boolean; target_bytes: number | null;
}
interface Job {
  id: string; account: string; identity: string; name: string; version: string;
  epoch: number; revision: string; selection_revision: string; purpose: 'baseline' | 'review'; source: string; description: string; phase: Phase; generation: number;
  selected: number; contexts: number; entries_seen: number; path_bytes: number;
  metadata_bytes: number; traversal_path_bytes: number; output_cursor: number; output_count: number;
  plan_hash: string | null; plan_bytes: number; closure_job: string | null;
  error: string | null; result: string | null;
}
interface Frontier { kind: number; id: string; prefix: string; depth: number; position: number; done: number }
interface Selected {
  canonical_path: string; relative_path: string; kind: number; id: string; size: number;
  executable: number; canonical_copy: string | null; target_bytes: number | null;
  symlink_data: ArrayBuffer | null;
}
interface Generated extends Descriptor { bytes_base64: string }
export interface ProjectionCopyOrigin {
  projected_copy: number[]; canonical_revision: number[]; canonical_path: string;
  canonical_copy: number[] | null; file: number[];
}
interface Plan {
  state: 'ready'; record_key: string; binding: ProjectionBinding;
  objects: Generated[]; private_copy_origins: ProjectionCopyOrigin[];
}
interface Catalog { id: string; binding: string; proof_job: string; retired: number }
interface Native extends Inspection {
  commit?: { parents: string[]; predecessors: string[]; root_trees: string[] };
  tree?: { entries: { name: string; kind: number; id: string; size_bytes?: number; executable?: boolean; copy_id?: string | null }[] };
  symlink?: { target_bytes: number };
  copy_history?: { current_path: string; parents: string[] };
}
const MAX_JOBS = 4096;
const MAX_ACTIVE = 8;
const MAX_CONTEXTS = 32768;
const MAX_VISITED_ENTRIES = 100000;
const MAX_SELECTED = 1024;
const MAX_PATH_BYTES = 1024 * 1024;
const MAX_METADATA_TOTAL = 32 * 1024 * 1024;
const MAX_METADATA = 256 * 1024;
const MAX_FILE = 1024 * 1024;
const MAX_PLAN = 4 * 1024 * 1024;
const MAX_PLAN_TOTAL = 32 * 1024 * 1024;
const CHUNK = 64 * 1024;
const MAX_OUTPUTS = 30002;
const MAX_ACCOUNT_ROWS = 262144;
const MAX_ACCOUNT_PATH_BYTES = 64 * 1024 * 1024;
const ROOT = '0'.repeat(64);
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });
class Limit extends Error { constructor(readonly budget: string) { super('projection_limit'); } }
class Invalid extends Error { constructor() { super('projection_object_invalid'); } }
function safe(value: number, max = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(value) && value >= 0 && value <= max; }
function key(value: ObjectKey): boolean { return Number.isInteger(value.kind) && value.kind >= 1 && value.kind <= 5 && /^[0-9a-f]{64}$/.test(value.id); }
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function descriptor(value: Descriptor): Descriptor {
  if (!key(value) || value.format_version !== 1 || !safe(value.size_bytes) || value.size_bytes > (value.kind === 1 ? MAX_FILE : MAX_METADATA)) throw new Invalid();
  return { kind: value.kind, id: value.id, size_bytes: value.size_bytes, format_version: 1 };
}
function hex(bytes: number[]): string {
  if (!Array.isArray(bytes) || bytes.length !== 32 || !bytes.every(b => Number.isInteger(b) && b >= 0 && b <= 255)) throw new Invalid();
  return bytes.map(b => b.toString(16).padStart(2, '0')).join('');
}
function uuid(value: string): boolean { return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value); }
function encode(bytes: Uint8Array): string {
  let result = ''; for (let offset = 0; offset < bytes.length; offset += 0x8000) result += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  return btoa(result);
}
function decode(text: string, size: number): Uint8Array {
  if (!safe(size, MAX_METADATA) || typeof text !== 'string' || text.length !== Math.ceil(size / 3) * 4) throw new Invalid();
  let raw: string; try { raw = atob(text); } catch { throw new Invalid(); }
  if (raw.length !== size) throw new Invalid();
  const bytes = Uint8Array.from(raw, c => c.charCodeAt(0)); if (encode(bytes) !== text) throw new Invalid(); return bytes;
}
function buffer(bytes: Uint8Array): ArrayBuffer { return bytes.slice().buffer as ArrayBuffer; }
function relative(root: string, canonical: string): string {
  if (!root) return canonical;
  if (!canonical.startsWith(`${root}/`)) throw new Invalid();
  return canonical.slice(root.length + 1);
}

export class ForgeProjections {
  constructor(private readonly host: ProjectionHost) {
    for (const statement of [
      'CREATE TABLE IF NOT EXISTS projection_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,frontier_rows INTEGER NOT NULL DEFAULT 0,selected_rows INTEGER NOT NULL DEFAULT 0,path_bytes INTEGER NOT NULL DEFAULT 0,catalog_path_bytes INTEGER NOT NULL DEFAULT 0)',
      'CREATE TABLE IF NOT EXISTS projection_jobs(id TEXT PRIMARY KEY,account TEXT NOT NULL,identity TEXT NOT NULL,name TEXT NOT NULL,version TEXT NOT NULL,epoch INTEGER NOT NULL,revision TEXT NOT NULL,selection_revision TEXT NOT NULL,purpose TEXT NOT NULL,source TEXT NOT NULL,description TEXT NOT NULL,phase TEXT NOT NULL,generation INTEGER NOT NULL DEFAULT 0,selected INTEGER NOT NULL DEFAULT 0,contexts INTEGER NOT NULL DEFAULT 0,entries_seen INTEGER NOT NULL DEFAULT 0,path_bytes INTEGER NOT NULL DEFAULT 0,metadata_bytes INTEGER NOT NULL DEFAULT 0,traversal_path_bytes INTEGER NOT NULL DEFAULT 0,output_cursor INTEGER NOT NULL DEFAULT 0,output_count INTEGER NOT NULL DEFAULT 0,plan_hash TEXT,plan_bytes INTEGER NOT NULL DEFAULT 0,closure_job TEXT,error TEXT,result TEXT,UNIQUE(account,version,epoch,purpose,source))',
      'CREATE TABLE IF NOT EXISTS projection_frontier(job TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,prefix TEXT NOT NULL,depth INTEGER NOT NULL,position INTEGER NOT NULL DEFAULT 0,done INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(job,kind,id,prefix))',
      'CREATE TABLE IF NOT EXISTS projection_selected(job TEXT NOT NULL,canonical_path TEXT NOT NULL,relative_path TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,executable INTEGER NOT NULL,canonical_copy TEXT,target_bytes INTEGER,symlink_data BLOB,PRIMARY KEY(job,canonical_path))',
      'CREATE TABLE IF NOT EXISTS projection_plan_chunks(job TEXT NOT NULL,ordinal INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(job,ordinal))',
      'CREATE TABLE IF NOT EXISTS projection_outputs(job TEXT NOT NULL,ordinal INTEGER NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,confirmed INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(job,ordinal))',
      'CREATE TABLE IF NOT EXISTS projection_catalogs(id TEXT PRIMARY KEY,account TEXT NOT NULL,version TEXT NOT NULL,epoch INTEGER NOT NULL,revision TEXT NOT NULL,binding TEXT NOT NULL,proof_job TEXT NOT NULL,retired INTEGER NOT NULL DEFAULT 0,purpose TEXT NOT NULL,parent TEXT NOT NULL,projected_commit TEXT NOT NULL)',
      'CREATE TABLE IF NOT EXISTS projection_members(projection TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,PRIMARY KEY(projection,kind,id))',
      'CREATE TABLE IF NOT EXISTS projection_paths(projection TEXT NOT NULL,canonical_path TEXT NOT NULL,relative_path TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,executable INTEGER NOT NULL,target_bytes INTEGER,PRIMARY KEY(projection,canonical_path))',
      'CREATE TABLE IF NOT EXISTS projection_private_origins(projection TEXT NOT NULL,canonical_path TEXT NOT NULL,projected_copy TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(projection,canonical_path))',
      'CREATE INDEX IF NOT EXISTS projection_relations ON projection_catalogs(account,version,epoch,projected_commit,retired)',
      'CREATE INDEX IF NOT EXISTS projection_copy_lookup ON projection_private_origins(projected_copy,projection)',
      'CREATE INDEX IF NOT EXISTS projection_member_lookup ON projection_members(kind,id,projection)',
    ]) host.sql.exec(statement);
    host.sql.exec('INSERT OR IGNORE INTO projection_usage(singleton,account) VALUES(1,?)',host.accountId);
  }
  private rows<T>(query: string, ...values: (string | number | null | ArrayBuffer)[]): T[] { return [...this.host.sql.exec(query, ...values)] as T[]; }
  private one<T>(query: string, ...values: (string | number | null | ArrayBuffer)[]): T | undefined { return this.rows<T>(query, ...values)[0]; }
  private usage(): {account:string;frontier_rows:number;selected_rows:number;path_bytes:number;catalog_path_bytes:number} {
    const row=this.one<{account:string;frontier_rows:number;selected_rows:number;path_bytes:number;catalog_path_bytes:number}>('SELECT * FROM projection_usage WHERE singleton=1');
    if(!row||row.account!==this.host.accountId||![row.frontier_rows,row.selected_rows,row.path_bytes,row.catalog_path_bytes].every(n=>safe(n))) fail(503,'failed_precondition');return row;
  }
  private check(view: ResolvedView): ViewDescription {
    this.host.views.checkInTx(view); const description = view.description;
    if (description.account_id !== this.host.accountId || !safe(description.policy_epoch) || description.policy_epoch < 1) fail(503, 'failed_precondition');
    this.host.requireSourceRootInTx(view);
    return description as ViewDescription;
  }
  private load(view: ResolvedView, id: string, generation?: number): Job {
    const d = this.check(view); if (!uuid(id)) fail(400, 'invalid_argument');
    const job = this.one<Job>('SELECT * FROM projection_jobs WHERE id=?', id);
    if (!job || job.account !== this.host.accountId || job.version !== hex(d.view_version) || job.name !== d.view) fail(403, 'permission_denied');
    if (job.phase === 'obsolete' || job.epoch !== d.policy_epoch || job.selection_revision !== hex(d.revision)) fail(409, 'aborted');
    if (job.purpose === 'review') this.reviewSourceInTx(view, job);
    else if (job.purpose !== 'baseline' || job.source !== '' || job.revision !== job.selection_revision) fail(503, 'failed_precondition');
    if (job.identity !== JSON.stringify({ kind: 'view', view_version: job.version }) || !safe(job.generation) || !safe(job.selected, MAX_SELECTED) || !safe(job.output_cursor, MAX_OUTPUTS) || !safe(job.output_count, MAX_OUTPUTS)) fail(503, 'failed_precondition');
    if (generation !== undefined && (!safe(generation) || generation !== job.generation)) fail(409, 'aborted');
    return job;
  }
  private source(view: ResolvedView, input: Readonly<ReviewProjectionSource>): ReviewProjectionSource {
    if (!input || typeof input !== 'object' || Object.keys(input).sort().join(',') !== 'account_id,attempt_generation,candidate_id,canonical_head,change_id,patchset_id,projected_parent,projection_epoch,review_epoch,view_name,view_version') fail(400,'invalid_argument');
    const source: ReviewProjectionSource = {account_id:input.account_id,view_name:input.view_name,view_version:input.view_version,review_epoch:input.review_epoch,projection_epoch:input.projection_epoch,change_id:input.change_id,patchset_id:input.patchset_id,candidate_id:input.candidate_id,attempt_generation:input.attempt_generation,canonical_head:input.canonical_head,projected_parent:input.projected_parent};
    const d=this.check(view);
    if(source.account_id!==this.host.accountId||source.view_name!==d.view||source.view_version!==hex(d.view_version)||source.projection_epoch!==d.policy_epoch||!safe(source.review_epoch)||source.review_epoch<1||!safe(source.attempt_generation)||!/^([0-9a-f]{32})$/.test(source.change_id)||![source.patchset_id,source.candidate_id,source.canonical_head,source.projected_parent].every(id=>typeof id==='string'&&/^[0-9a-f]{64}$/.test(id))||source.projected_parent===ROOT) fail(400,'invalid_argument');
    if(!this.host.requireReviewSourceInTx) fail(503,'failed_precondition');
    this.host.requireReviewSourceInTx(view,Object.freeze(source));
    // A ledger-selected parent must itself have an exact current sanitized
    // relation. Availability of an arbitrary canonical COMMIT is not enough.
    if(!this.catalogForProjectedInTx(view,source.projected_parent)) fail(403,'permission_denied');
    return source;
  }
  private reviewSourceInTx(view:ResolvedView,job:Job):ReviewProjectionSource {
    let value:ReviewProjectionSource;try{value=JSON.parse(job.source);}catch{throw new Invalid();}
    const source=this.source(view,value);
    if(JSON.stringify(source)!==job.source||source.canonical_head!==job.revision) throw new Invalid();
    return source;
  }
  private recordKey(job:Job):string {
    const base=`forge/projections/v1/${job.account}/${job.version}/${job.epoch}`;
    if(job.purpose==='baseline') return base;
    const source=JSON.parse(job.source) as ReviewProjectionSource;
    return `${base}/review/${job.revision}/${source.change_id}/${source.projected_parent}`;
  }
  startReviewResult(view:ResolvedView,input:Readonly<ReviewProjectionSource>):ProjectionStatus {
    return this.host.transactionSync(()=>{
      const source=this.source(view,input),d=this.check(view),encoded=JSON.stringify(source);
      this.obsoleteInTx(d.policy_epoch);
      const old=this.one<Job>("SELECT * FROM projection_jobs WHERE account=? AND version=? AND epoch=? AND purpose='review' AND source=?",this.host.accountId,source.view_version,source.projection_epoch,encoded);
      if(old) return this.report(this.load(view,old.id));
      if(Number(this.one<{n:number}>('SELECT COUNT(*) AS n FROM projection_jobs')!.n)>=MAX_JOBS||Number(this.one<{n:number}>("SELECT COUNT(*) AS n FROM projection_jobs WHERE phase NOT IN('ready','limit','obsolete')")!.n)>=MAX_ACTIVE) fail(429,'resource_exhausted');
      const id=crypto.randomUUID(),identity=JSON.stringify({kind:'view',view_version:source.view_version});
      this.host.sql.exec("INSERT INTO projection_jobs(id,account,identity,name,version,epoch,revision,selection_revision,purpose,source,description,phase) VALUES(?,?,?,?,?,?,?,?,'review',?,?,'select')",id,this.host.accountId,identity,d.view,source.view_version,d.policy_epoch,source.canonical_head,hex(d.revision),encoded,JSON.stringify(d));
      const job=this.load(view,id);this.enqueue(job,{kind:3,id:source.canonical_head},'',0);this.save(job);return this.report(job);
    });
  }
  statusReviewResult(view:ResolvedView,input:Readonly<ReviewProjectionSource>,jobId:string):ProjectionStatus {
    return this.host.transactionSync(()=>{const source=this.source(view,input),job=this.load(view,jobId);if(job.purpose!=='review'||job.source!==JSON.stringify(source)) fail(403,'permission_denied');return this.report(job);});
  }
  async advanceReviewResult(view:ResolvedView,input:Readonly<ReviewProjectionSource>,progress:ProjectionProgress):Promise<ProjectionStatus> {
    this.statusReviewResult(view,input,progress.job_id);
    return this.advanceJob(view,progress);
  }
  async advance(view:ResolvedView,progress:ProjectionProgress):Promise<ProjectionStatus> {
    this.status(view,progress.job_id);return this.advanceJob(view,progress);
  }
  private save(job: Job): void {
    if (!safe(job.generation, Number.MAX_SAFE_INTEGER - 1)) throw new Limit('generation');
    job.generation++;
    this.host.sql.exec('UPDATE projection_jobs SET phase=?,generation=?,selected=?,contexts=?,entries_seen=?,path_bytes=?,metadata_bytes=?,traversal_path_bytes=?,output_cursor=?,output_count=?,plan_hash=?,plan_bytes=?,closure_job=?,error=?,result=? WHERE id=?',
      job.phase, job.generation, job.selected, job.contexts, job.entries_seen, job.path_bytes, job.metadata_bytes, job.traversal_path_bytes, job.output_cursor, job.output_count, job.plan_hash, job.plan_bytes, job.closure_job, job.error, job.result, job.id);
  }
  private report(job: Job): ProjectionStatus {
    const selection = JSON.parse(job.description) as ViewDescription;
    const result: ProjectionStatus = { state: job.phase === 'ready' ? 'ready' : job.phase === 'limit' ? 'limit' : job.error ? 'unavailable' : 'pending',
      job_id: job.id, generation: job.generation, phase: job.phase, selection,
      counts: { selected_entries: job.selected, confirmed_outputs: job.output_cursor, output_objects: job.output_count }, error: job.error };
    if (job.phase === 'ready') {
      if (!job.result) fail(503, 'failed_precondition'); result.projection = JSON.parse(job.result) as ProjectionBinding;
    }
    return result;
  }
  /** Reclaim preparation-only state for epochs no caller can resume. This is
   * bounded account housekeeping inside start's already authorized transaction,
   * not object GC. Ready catalogs and generic closure jobs are never touched. */
  private obsoleteInTx(currentEpoch: number): void {
    const jobs = this.rows<Job>("SELECT * FROM projection_jobs WHERE account=? AND epoch<? AND phase NOT IN('ready','obsolete') ORDER BY CASE WHEN phase='limit' THEN 1 ELSE 0 END,epoch,id LIMIT 16", this.host.accountId, currentEpoch);
    for (const job of jobs) {
      if (!uuid(job.id) || !safe(job.epoch) || job.epoch >= currentEpoch) fail(503, 'failed_precondition');
      const frontier = this.one<{n:number;bytes:number}>('SELECT COUNT(*) AS n,COALESCE(SUM(length(CAST(prefix AS BLOB))),0) AS bytes FROM projection_frontier WHERE job=?', job.id)!;
      const selected = this.one<{n:number;bytes:number}>('SELECT COUNT(*) AS n,COALESCE(SUM(length(CAST(canonical_path AS BLOB))+length(CAST(relative_path AS BLOB))),0) AS bytes FROM projection_selected WHERE job=?', job.id)!;
      const usage = this.usage();
      if (![frontier.n, frontier.bytes, selected.n, selected.bytes].every(value => safe(value))
          || selected.n !== job.selected || selected.bytes !== job.path_bytes
          || usage.frontier_rows < frontier.n || usage.selected_rows < selected.n
          || usage.path_bytes < frontier.bytes + selected.bytes) fail(503, 'failed_precondition');
      this.host.sql.exec('UPDATE projection_usage SET frontier_rows=frontier_rows-?,selected_rows=selected_rows-?,path_bytes=path_bytes-? WHERE singleton=1', frontier.n, selected.n, frontier.bytes + selected.bytes);
      for (const table of ['projection_plan_chunks', 'projection_frontier', 'projection_selected', 'projection_outputs']) {
        this.host.sql.exec(`DELETE FROM ${table} WHERE job=?`, job.id);
      }
      // Keep immutable binding and the closure job/plan fingerprint for future
      // exact lifecycle cleanup. Neither is a capability or a revival cursor.
      job.phase = 'obsolete'; job.error = 'stale_policy'; job.result = null;
      job.selected = 0; job.contexts = 0; job.entries_seen = 0; job.path_bytes = 0;
      job.metadata_bytes = 0; job.traversal_path_bytes = 0; job.output_cursor = 0;
      job.output_count = 0; job.plan_bytes = 0;
      this.save(job);
    }
  }
  /** Preserve order and cardinality while respecting the shared native helper's
   * 512-path / 1-MiB page contract. Empty requests still check current policy. */
  private allowedPathsInTx(view: ResolvedView, paths: string[]): {path:string;allowed:boolean;could_contain:boolean}[] {
    const result: {path:string;allowed:boolean;could_contain:boolean}[] = [];
    let page: string[] = []; let bytes = 0;
    const flush = (): void => {
      const answers = this.host.views.pathsAllowedInTx(view, page).paths;
      if (answers.length !== page.length || answers.some((answer, index) => answer.path !== page[index]
          || typeof answer.allowed !== 'boolean' || typeof answer.could_contain !== 'boolean')) throw new Invalid();
      result.push(...answers); page = []; bytes = 0;
    };
    for (const path of paths) {
      if (typeof path !== 'string') throw new Invalid();
      const length = encoder.encode(path).byteLength;
      if (length > 1024 * 1024) throw new Limit('path_bytes');
      if (page.length === 512 || bytes + length > 1024 * 1024) flush();
      page.push(path); bytes += length;
    }
    if (page.length || paths.length === 0) flush();
    if (result.length !== paths.length) throw new Invalid();
    return result;
  }
  start(view: ResolvedView): ProjectionStatus {
    return this.host.transactionSync(() => {
      const d = this.check(view); const version = hex(d.view_version), revision = hex(d.revision);
      this.obsoleteInTx(d.policy_epoch);
      const old = this.one<Job>("SELECT * FROM projection_jobs WHERE account=? AND version=? AND epoch=? AND purpose='baseline' AND source=''", this.host.accountId, version, d.policy_epoch);
      if (old) return this.report(this.load(view, old.id));
      if (Number(this.one<{n:number}>('SELECT COUNT(*) AS n FROM projection_jobs')!.n) >= MAX_JOBS || Number(this.one<{n:number}>("SELECT COUNT(*) AS n FROM projection_jobs WHERE phase NOT IN('ready','limit','obsolete')")!.n) >= MAX_ACTIVE) fail(429, 'resource_exhausted');
      const id = crypto.randomUUID(); const identity = JSON.stringify({kind:'view',view_version:version});
      this.host.sql.exec("INSERT INTO projection_jobs(id,account,identity,name,version,epoch,revision,selection_revision,purpose,source,description,phase) VALUES(?,?,?,?,?,?,?,?,'baseline','',?,'select')", id, this.host.accountId, identity, d.view, version, d.policy_epoch, revision, revision, JSON.stringify(d));
      const job = this.load(view, id); this.enqueue(job, {kind:3,id:revision}, '', 0); this.save(job); return this.report(job);
    });
  }
  private currentStatus(view: ResolvedView, jobId: string): ProjectionStatus { return this.host.transactionSync(() => this.report(this.load(view, jobId))); }
  status(view: ResolvedView, jobId: string): ProjectionStatus { return this.host.transactionSync(() => {const job=this.load(view,jobId);if(job.purpose!=='baseline') fail(403,'permission_denied');return this.report(job);}); }
  private enqueue(job: Job, object: ObjectKey, prefix: string, depth: number): void {
    if (!key(object) || !safe(depth,64) || encoder.encode(prefix).length > 4096) throw new Limit('tree_depth');
    if (this.one('SELECT 1 FROM projection_frontier WHERE job=? AND kind=? AND id=? AND prefix=?',job.id,object.kind,object.id,prefix)) return;
    if (job.contexts >= MAX_CONTEXTS) throw new Limit('tree_contexts');
    const usage=this.usage(),pathBytes=encoder.encode(prefix).length;if(usage.frontier_rows>=MAX_ACCOUNT_ROWS||usage.path_bytes+pathBytes>MAX_ACCOUNT_PATH_BYTES) fail(429,'resource_exhausted');
    this.host.sql.exec('INSERT INTO projection_frontier(job,kind,id,prefix,depth) VALUES(?,?,?,?,?)',job.id,object.kind,object.id,prefix,depth);this.host.sql.exec('UPDATE projection_usage SET frontier_rows=frontier_rows+1,path_bytes=path_bytes+? WHERE singleton=1',pathBytes);job.contexts++;
  }
  private describe(view: ResolvedView, job: Job, object: ObjectKey): Descriptor {
    return this.host.transactionSync(() => { this.load(view,job.id,job.generation); const d=this.host.describeObjectInTx(object); if(!d) throw new Error('unavailable'); const copy=descriptor(d); if(copy.kind!==object.kind||copy.id!==object.id) throw new Invalid(); return copy; });
  }
  private async read(view: ResolvedView, job: Job, d: Descriptor): Promise<{descriptor:Descriptor;bytes:Uint8Array;info:Native}> {
    this.host.transactionSync(() => this.load(view,job.id,job.generation));
    let value: {descriptor:Descriptor;bytes:Uint8Array};
    try { value=await this.host.readVerifiedObject(d); }
    finally { this.host.transactionSync(() => this.load(view,job.id,job.generation)); }
    if(!same(d,value.descriptor)||value.bytes.length!==d.size_bytes) throw new Invalid();
    let info:Native; try {info=inspect(d.kind,value.bytes) as Native;} catch {throw new Invalid();}
    if(!same(d,info)) throw new Invalid();
    this.host.transactionSync(() => {this.load(view,job.id,job.generation); const fresh=this.host.describeObjectInTx(d); if(!fresh||!same(d,fresh)) throw new Invalid();});
    return {...value,info};
  }
  private blocked(view:ResolvedView, job:Job, error:unknown): ProjectionStatus {
    return this.host.transactionSync(() => {const current=this.load(view,job.id); if(current.phase==='ready'||current.phase==='limit') return this.report(current); current.error=error instanceof Limit ? error.budget : error instanceof Invalid ? 'object_invalid' : 'object_unavailable'; if(error instanceof Limit) current.phase='limit'; this.save(current);return this.report(current);});
  }
  private addSelected(job:Job, view:ResolvedView, canonical:string, entry:NonNullable<Native['tree']>['entries'][number]):void {
    if(job.selected>=MAX_SELECTED) throw new Limit('selected_entries');
    const d=this.host.describeObjectInTx(entry); if(!d) throw new Error('unavailable'); descriptor(d);
    if(d.kind!==entry.kind||d.id!==entry.id||!safe(entry.size_bytes!)) throw new Invalid();
    if(entry.kind===1&&(entry.size_bytes!==d.size_bytes||typeof entry.executable!=='boolean')) throw new Invalid();
    if(entry.copy_id!==undefined&&entry.copy_id!==null&&!/^[0-9a-f]{64}$/.test(entry.copy_id)) throw new Invalid();
    const path=relative(view.description.root_prefix,canonical);
    const bytes=encoder.encode(canonical).length+encoder.encode(path).length;
    if(job.path_bytes+bytes>MAX_PATH_BYTES) throw new Limit('path_bytes');
    const usage=this.usage();if(usage.selected_rows>=MAX_ACCOUNT_ROWS||usage.path_bytes+bytes>MAX_ACCOUNT_PATH_BYTES) fail(429,'resource_exhausted');
    if(this.one('SELECT 1 FROM projection_selected WHERE job=? AND canonical_path=?',job.id,canonical)) throw new Invalid();
    this.host.sql.exec('INSERT INTO projection_selected(job,canonical_path,relative_path,kind,id,size,executable,canonical_copy,target_bytes) VALUES(?,?,?,?,?,?,?,?,?)',
      job.id,canonical,path,d.kind,d.id,d.size_bytes,entry.executable?1:0,entry.kind===1?(entry.copy_id??null):null,entry.kind===4?entry.size_bytes!:null);
    this.host.sql.exec('UPDATE projection_usage SET selected_rows=selected_rows+1,path_bytes=path_bytes+? WHERE singleton=1',bytes);job.path_bytes+=bytes;job.selected++;
  }
  private async select(view:ResolvedView, initial:Job):Promise<ProjectionStatus> {
    let job=initial, reads=0, bytes=0, entries=0; const deadline=Date.now()+5000;
    while(reads<16&&entries<128&&Date.now()<deadline) {
      const work=this.host.transactionSync(() => {
        this.load(view,job.id,job.generation);
        const link=this.one<Selected>('SELECT * FROM projection_selected WHERE job=? AND kind=4 AND symlink_data IS NULL ORDER BY canonical_path LIMIT 1',job.id);
        const frontier=this.one<Frontier>('SELECT * FROM projection_frontier WHERE job=? AND done=0 ORDER BY kind DESC,prefix,id LIMIT 1',job.id);
        return {link,frontier};
      });
      if(!work.link&&!work.frontier) return this.host.transactionSync(() => {job=this.load(view,job.id,job.generation);job.phase='build';job.error=null;this.save(job);return this.report(job);});
      const object=work.link?{kind:4,id:work.link.id}:{kind:work.frontier!.kind,id:work.frontier!.id};
      const d=this.describe(view,job,object); if(bytes+d.size_bytes>MAX_METADATA) break;
      const value=await this.read(view,job,d); reads++;bytes+=d.size_bytes;
      job=this.host.transactionSync(() => {
        const current=this.load(view,job.id,job.generation);
        if(current.metadata_bytes+d.size_bytes>MAX_METADATA_TOTAL) throw new Limit('metadata_bytes');
        current.metadata_bytes+=d.size_bytes;current.error=null;
        if(work.link) {
          if(!value.info.symlink||value.info.symlink.target_bytes!==work.link.target_bytes) throw new Invalid();
          const row=this.one<Selected>('SELECT * FROM projection_selected WHERE job=? AND canonical_path=?',job.id,work.link.canonical_path);
          if(!row||row.symlink_data!==null||row.id!==d.id||row.size!==d.size_bytes) throw new Invalid();
          const retained=Number(this.one<{n:number}>('SELECT COALESCE(SUM(length(symlink_data)),0) AS n FROM projection_selected WHERE job=?',job.id)!.n);
          if(retained+value.bytes.length>1024*1024) throw new Limit('symlink_metadata_bytes');
          this.host.sql.exec('UPDATE projection_selected SET symlink_data=? WHERE job=? AND canonical_path=?',buffer(value.bytes),job.id,row.canonical_path);
        } else {
          const f=work.frontier!;const stored=this.one<Frontier>('SELECT * FROM projection_frontier WHERE job=? AND kind=? AND id=? AND prefix=?',job.id,f.kind,f.id,f.prefix);
          if(!stored||stored.done||stored.position!==f.position) throw new Invalid();
          if(f.kind===3) {
            if(!value.info.commit||value.info.commit.root_trees.length!==1||value.info.commit.parents.length>1) fail(400,'failed_precondition');
            const root=value.info.commit.root_trees[0];if(!/^[0-9a-f]{64}$/.test(root)) throw new Invalid();
            this.enqueue(current,{kind:2,id:root},'',0);
            this.host.sql.exec('UPDATE projection_frontier SET done=1 WHERE job=? AND kind=3 AND id=? AND prefix=?',job.id,f.id,f.prefix);
          } else {
            const tree=value.info.tree;if(!tree||!safe(f.position,tree.entries.length)) throw new Invalid();
            const page=tree.entries.slice(f.position,f.position+(128-entries));
            const paths=page.map(e=>f.prefix?`${f.prefix}/${e.name}`:e.name);
            const decisions=this.allowedPathsInTx(view,paths);
            if(decisions.length!==page.length) throw new Invalid();
            for(let i=0;i<page.length;i++) {
              const entry=page[i],decision=decisions[i];if(decision.path!==paths[i]) throw new Invalid();
              current.entries_seen++;if(current.entries_seen>MAX_VISITED_ENTRIES) throw new Limit('tree_entries');
              current.traversal_path_bytes+=encoder.encode(paths[i]).length;if(current.traversal_path_bytes>4*1024*1024) throw new Limit('traversal_path_bytes');
              if(entry.kind===2) {if(decision.could_contain) this.enqueue(current,entry,paths[i],f.depth+1);}
              else if((entry.kind===1||entry.kind===4)&&decision.allowed) this.addSelected(current,view,paths[i],entry);
              else if(entry.kind!==1&&entry.kind!==4) throw new Invalid();
            }
            entries+=page.length;const next=f.position+page.length;
            this.host.sql.exec('UPDATE projection_frontier SET position=?,done=? WHERE job=? AND kind=? AND id=? AND prefix=?',next,next===tree.entries.length?1:0,job.id,f.kind,f.id,f.prefix);
          }
        }
        this.save(current);return current;
      });
    }
    return this.currentStatus(view,job.id);
  }
  private validatePlan(job:Job, value:unknown):Plan {
    if(!value||typeof value!=='object') throw new Invalid(); const plan=value as Plan;
    if(plan.state!=='ready'||!plan.binding||!Array.isArray(plan.objects)||plan.objects.length<2||plan.objects.length>MAX_OUTPUTS||!Array.isArray(plan.private_copy_origins)||plan.private_copy_origins.length>MAX_SELECTED) throw new Invalid();
    const b=plan.binding;
    if(b.account_id!==job.account||hex(b.view_version)!==job.version||b.policy_epoch!==job.epoch||hex(b.canonical_revision)!==job.revision||plan.record_key!==this.recordKey(job)) throw new Invalid();
    const head=hex(b.projected_commit),root=hex(b.projected_tree); const keys=new Set<string>();let commits=0;
    for(const object of plan.objects) {
      if(!key(object)||![2,3,5].includes(object.kind)||object.format_version!==1||!safe(object.size_bytes)||typeof object.bytes_base64!=='string') throw new Invalid();
      if(object.size_bytes>MAX_METADATA) throw new Limit('metadata_object_bytes');
      if(object.bytes_base64.length!==Math.ceil(object.size_bytes/3)*4) throw new Invalid();
      const k=`${object.kind}/${object.id}`;if(keys.has(k)) throw new Invalid();keys.add(k);if(object.kind===3) commits++;
    }
    if(commits!==1||!keys.has(`2/${root}`)||plan.objects[plan.objects.length-1].kind!==3||plan.objects[plan.objects.length-1].id!==head) throw new Invalid();
    const files=this.rows<Selected>('SELECT * FROM projection_selected WHERE job=? AND kind=1',job.id);const byPath=new Map(files.map(file=>[file.canonical_path,file]));
    if(plan.private_copy_origins.length!==files.length) throw new Invalid();
    const origins=new Set<string>();
    for(const origin of plan.private_copy_origins) {
      const source=byPath.get(origin.canonical_path);
      if(!source||origins.has(origin.canonical_path)||hex(origin.canonical_revision)!==job.revision||hex(origin.file)!==source.id||!keys.has(`5/${hex(origin.projected_copy)}`)||(origin.canonical_copy===null?null:hex(origin.canonical_copy))!==source.canonical_copy) throw new Invalid();
      origins.add(origin.canonical_path);
    }
    return plan;
  }
  private async build(view:ResolvedView, job:Job):Promise<ProjectionStatus> {
    const selected=this.host.transactionSync(() => {
      this.load(view,job.id,job.generation);
      if(this.one('SELECT 1 FROM projection_frontier WHERE job=? AND done=0 LIMIT 1',job.id)) throw new Invalid();
      const rows=this.rows<Selected>('SELECT * FROM projection_selected WHERE job=? ORDER BY canonical_path LIMIT 1025',job.id);
      if(rows.length!==job.selected||rows.length>MAX_SELECTED) throw new Invalid();
      const decisions=this.allowedPathsInTx(view,rows.map(row=>row.canonical_path));
      if(decisions.length!==rows.length||decisions.some((d,i)=>d.path!==rows[i].canonical_path||!d.allowed)) fail(403,'permission_denied');
      return rows.map(row=>{
        const d=descriptor({kind:row.kind,id:row.id,size_bytes:row.size,format_version:1});
        const fresh=this.host.describeObjectInTx(d);if(!fresh||!same(d,fresh)) throw new Invalid();
        const value=row.kind===1?{kind:'file',executable:row.executable===1,canonical_copy:row.canonical_copy}:
          row.kind===4&&row.symlink_data instanceof ArrayBuffer?{kind:'symlink',bytes_base64:encode(new Uint8Array(row.symlink_data))}:null;
        if(!value) throw new Invalid();
        return {canonical_path:row.canonical_path,...d,value};
      });
    });
    let value:unknown;try {value=buildForgeProjection({schema:1,context:{account_id:job.account,view_version:job.version,policy_epoch:job.epoch,canonical_revision:job.revision,review:job.purpose==='review'?{change_id:JSON.parse(job.source).change_id,projected_parent:JSON.parse(job.source).projected_parent}:null},entries:selected});}catch{throw new Invalid();}
    const candidate=value as {state?:string;budget?:string};
    if(candidate?.state==='limit') throw new Limit(typeof candidate.budget==='string'?candidate.budget:'builder');
    const plan=this.host.transactionSync(() => {this.load(view,job.id,job.generation);return this.validatePlan(job,value);});
    const encoded=encoder.encode(JSON.stringify(plan));if(encoded.length>MAX_PLAN) throw new Limit('plan_bytes');
    const digest=await hash(decoder.decode(encoded));
    return this.host.transactionSync(() => {
      const current=this.load(view,job.id,job.generation);
      if(current.phase!=='build'||current.plan_hash!==null) throw new Invalid();
      const total=Number(this.one<{bytes:number}>('SELECT COALESCE(SUM(plan_bytes),0) AS bytes FROM projection_jobs')!.bytes);
      if(total+encoded.length>MAX_PLAN_TOTAL) fail(429,'resource_exhausted');
      for(let offset=0,ordinal=0;offset<encoded.length;offset+=CHUNK,ordinal++) this.host.sql.exec('INSERT INTO projection_plan_chunks(job,ordinal,data) VALUES(?,?,?)',job.id,ordinal,buffer(encoded.subarray(offset,offset+CHUNK)));
      current.plan_hash=digest;current.plan_bytes=encoded.length;current.output_count=plan.objects.length;current.phase='stage';current.error=null;this.save(current);return this.report(current);
    });
  }
  private async plan(view:ResolvedView, job:Job):Promise<Plan> {
    const bytes=this.host.transactionSync(() => {
      const current=this.load(view,job.id,job.generation);
      if(!current.plan_hash||!safe(current.plan_bytes,MAX_PLAN)||current.plan_bytes===0) throw new Invalid();
      const rows=this.rows<{ordinal:number;data:ArrayBuffer}>('SELECT ordinal,data FROM projection_plan_chunks WHERE job=? ORDER BY ordinal LIMIT 65',job.id);
      if(rows.length!==Math.ceil(current.plan_bytes/CHUNK)||rows.length>64) throw new Invalid();
      const result=new Uint8Array(current.plan_bytes);let offset=0;
      rows.forEach((row,index)=>{if(row.ordinal!==index||!(row.data instanceof ArrayBuffer)) throw new Invalid();const data=new Uint8Array(row.data);if(data.length!==Math.min(CHUNK,result.length-offset)) throw new Invalid();result.set(data,offset);offset+=data.length;});return result;
    });
    let text:string;try{text=decoder.decode(bytes);}catch{throw new Invalid();}
    const digest=await hash(text);
    return this.host.transactionSync(() => {const current=this.load(view,job.id,job.generation);if(digest!==current.plan_hash) throw new Invalid();let parsed:unknown;try{parsed=JSON.parse(text);}catch{throw new Invalid();}const plan=this.validatePlan(current,parsed);if(plan.objects.length!==current.output_count) throw new Invalid();return plan;});
  }
  private async stage(view:ResolvedView, initial:Job):Promise<ProjectionStatus> {
    let job=initial;const plan=await this.plan(view,job);let written=0,bytes=0;const deadline=Date.now()+5000;
    while(job.output_cursor<job.output_count&&written<16&&Date.now()<deadline) {
      const object=plan.objects[job.output_cursor];const d=descriptor(object);if(bytes+d.size_bytes>256*1024) break;
      const body=decode(object.bytes_base64,d.size_bytes);let native:Native;try{native=inspect(d.kind,body) as Native;}catch{throw new Invalid();}if(!same(d,native)) throw new Invalid();
      if(d.kind===3&&(!native.commit||native.commit.parents.length!==1||native.commit.parents[0]!==(job.purpose==='review'?JSON.parse(job.source).projected_parent:ROOT)||native.commit.predecessors.length!==0||native.commit.root_trees.length!==1||native.commit.root_trees[0]!==hex(plan.binding.projected_tree))) throw new Invalid();
      if(d.kind===5&&(!native.copy_history||native.copy_history.parents.length!==0)) throw new Invalid();
      this.host.transactionSync(() => {
        const current=this.load(view,job.id,job.generation);if(current.phase!=='stage'||current.output_cursor!==job.output_cursor) fail(409,'aborted');
        const existing=this.one<{kind:number;id:string;size:number;confirmed:number}>('SELECT kind,id,size,confirmed FROM projection_outputs WHERE job=? AND ordinal=?',job.id,job.output_cursor);
        if(existing&&(existing.kind!==d.kind||existing.id!==d.id||existing.size!==d.size_bytes||existing.confirmed!==0)) throw new Invalid();
        this.host.reserveGeneratedInTx(job.id,{...d});
        this.host.sql.exec('INSERT OR IGNORE INTO projection_outputs(job,ordinal,kind,id,size,confirmed) VALUES(?,?,?,?,?,0)',job.id,job.output_cursor,d.kind,d.id,d.size_bytes);
      });
      try {await this.host.putVerifiedGenerated({...d},body);} finally {this.host.transactionSync(()=>this.load(view,job.id,job.generation));}
      job=this.host.transactionSync(() => {
        const current=this.load(view,job.id,job.generation);
        if(current.output_cursor!==job.output_cursor||current.phase!=='stage') fail(409,'aborted');
        this.host.confirmGeneratedInTx(job.id,{...d});
        const actual=this.host.describeObjectInTx(d);if(!actual||!same(d,actual)) throw new Invalid();
        this.host.sql.exec('UPDATE projection_outputs SET confirmed=1 WHERE job=? AND ordinal=?',job.id,job.output_cursor);
        current.output_cursor++;current.error=null;if(current.output_cursor===current.output_count) current.phase='closure';this.save(current);return current;
      });written++;bytes+=d.size_bytes;
    }
    return this.currentStatus(view,job.id);
  }
  private closureBinding(job:Job,plan:Plan):ClosureBinding {
    if(!job.plan_hash) throw new Invalid();
    return {account_id:job.account,identity:{kind:'view',view_version:job.version},policy_epoch:job.epoch,purpose:job.purpose==='review'?'review-projection-prepare':'projection-prepare',
      publication:JSON.stringify([job.version,job.epoch,job.revision,job.plan_hash,plan.record_key,hex(plan.binding.projected_commit),...(job.purpose==='review'?[JSON.parse(job.source)]:[])])};
  }
  /** Called ONLY inside ClosureJobs.publish's existing SQL transaction. */
  private publishInTx(view:ResolvedView,expected:Job,plan:Plan,proofId:string):unknown {
    const job=this.load(view,expected.id,expected.generation);
    if(job.phase!=='closure'||job.closure_job!==proofId||job.output_cursor!==job.output_count||job.output_count!==plan.objects.length) throw new Invalid();
    const binding=this.closureBinding(job,plan);
    const expectedBinding=JSON.stringify([binding.account_id,binding.identity,binding.policy_epoch,binding.purpose,binding.publication]);
    const proof=this.one<{phase:string;account:string;identity:string;binding:string;root_kind:number;root_id:string;nodes:number;checked_nodes:number;contexts:number;checked_contexts:number;obligations:number;checked_obligations:number}>('SELECT * FROM closure_jobs WHERE id=?',proofId);
    if(!proof||proof.phase!=='ready'||proof.account!==job.account||proof.identity!==job.identity||proof.binding!==expectedBinding||proof.root_kind!==3||proof.root_id!==hex(plan.binding.projected_commit)||proof.checked_nodes!==proof.nodes||proof.checked_contexts!==proof.contexts||proof.checked_obligations!==proof.obligations) throw new Invalid();
    const selected=this.rows<Selected>('SELECT * FROM projection_selected WHERE job=? ORDER BY canonical_path LIMIT 1025',job.id);
    if(selected.length!==job.selected||selected.length>MAX_SELECTED) throw new Invalid();
    const decisions=this.allowedPathsInTx(view,selected.map(row=>row.canonical_path));
    if(decisions.length!==selected.length||decisions.some((d,i)=>d.path!==selected[i].canonical_path||!d.allowed)) fail(403,'permission_denied');
    const nodes=this.one<{n:number;bad:number}>('SELECT COUNT(*) AS n,COALESCE(SUM(CASE WHEN done<>1 OR size IS NULL OR size<0 OR kind<1 OR kind>5 THEN 1 ELSE 0 END),0) AS bad FROM closure_nodes WHERE job=?',proofId)!;
    if(nodes.n!==proof.nodes||nodes.bad!==0) throw new Invalid();
    const expectedKeys=new Set([...plan.objects.map(object=>`${object.kind}/${object.id}`),...selected.map(path=>`${path.kind}/${path.id}`)]);
    let expectedCount=expectedKeys.size;
    if(job.purpose==='review') {
      const source=this.reviewSourceInTx(view,job),parent=this.catalogForProjectedInTx(view,source.projected_parent);
      if(!parent) fail(403,'permission_denied');
      const count=this.one<{n:number}>('SELECT COUNT(*) AS n FROM projection_members WHERE projection=?',parent.catalog_id)!.n;
      if(!safe(count,MAX_ACCOUNT_ROWS)||count===0) throw new Invalid();
      expectedCount=count;
      for(const object of expectedKeys) {
        const slash=object.indexOf('/'),kind=Number(object.slice(0,slash)),id=object.slice(slash+1);
        if(!this.one('SELECT 1 FROM projection_members WHERE projection=? AND kind=? AND id=?',parent.catalog_id,kind,id)) expectedCount++;
      }
      if(this.one('SELECT 1 FROM projection_members m WHERE m.projection=? AND NOT EXISTS(SELECT 1 FROM closure_nodes n WHERE n.job=? AND n.kind=m.kind AND n.id=m.id AND n.done=1 AND n.size=m.size) LIMIT 1',parent.catalog_id,proofId)) throw new Invalid();
    }
    if(nodes.n!==expectedCount) throw new Invalid();
    for(const object of plan.objects) if(!this.one('SELECT 1 FROM closure_nodes WHERE job=? AND kind=? AND id=? AND done=1 AND size=?',proofId,object.kind,object.id,object.size_bytes)) throw new Invalid();
    const outputs=this.one<{n:number}>('SELECT COUNT(*) AS n FROM projection_outputs WHERE job=? AND confirmed=1',job.id)!;
    if(outputs.n!==job.output_count) throw new Invalid();
    for(const path of selected) if(!this.one('SELECT 1 FROM closure_nodes WHERE job=? AND kind=? AND id=? AND done=1 AND size=?',proofId,path.kind,path.id,path.size)) throw new Invalid();
    const missing=this.one('SELECT 1 FROM projection_outputs o WHERE o.job=? AND NOT EXISTS(SELECT 1 FROM closure_nodes n WHERE n.job=? AND n.kind=o.kind AND n.id=o.id AND n.done=1 AND n.size=o.size) LIMIT 1',job.id,proofId);if(missing) throw new Invalid();
    const old=this.one<Catalog>('SELECT id,binding,proof_job,retired FROM projection_catalogs WHERE id=?',plan.record_key);
    if(old&&(old.retired!==0||old.binding!==JSON.stringify(plan.binding))) throw new Invalid();
    if(!old) {
      const usage=this.usage();const members=Number(this.one<{n:number}>('SELECT COUNT(*) AS n FROM projection_members')!.n);
      if(members+nodes.n>MAX_ACCOUNT_ROWS||usage.catalog_path_bytes+job.path_bytes>MAX_ACCOUNT_PATH_BYTES) fail(429,'resource_exhausted');
      this.host.sql.exec('UPDATE projection_usage SET catalog_path_bytes=catalog_path_bytes+? WHERE singleton=1',job.path_bytes);
      this.host.sql.exec('INSERT INTO projection_catalogs(id,account,version,epoch,revision,binding,proof_job,retired,purpose,parent,projected_commit) VALUES(?,?,?,?,?,?,?,0,?,?,?)',plan.record_key,job.account,job.version,job.epoch,job.revision,JSON.stringify(plan.binding),proofId,job.purpose,job.purpose==='review'?JSON.parse(job.source).projected_parent:ROOT,hex(plan.binding.projected_commit));
      this.host.sql.exec('INSERT INTO projection_members(projection,kind,id,size) SELECT ?,kind,id,size FROM closure_nodes WHERE job=?',plan.record_key,proofId);
      this.host.sql.exec('INSERT INTO projection_paths(projection,canonical_path,relative_path,kind,id,size,executable,target_bytes) SELECT ?,canonical_path,relative_path,kind,id,size,executable,target_bytes FROM projection_selected WHERE job=?',plan.record_key,job.id);
      for(const origin of plan.private_copy_origins) {
        const encoded=JSON.stringify(origin);if(encoder.encode(encoded).length>8*1024) throw new Invalid();
        this.host.sql.exec('INSERT INTO projection_private_origins(projection,canonical_path,projected_copy,value) VALUES(?,?,?,?)',plan.record_key,origin.canonical_path,hex(origin.projected_copy),encoded);
      }
    }
    this.host.retainProjectionRootInTx(plan.record_key,hex(plan.binding.projected_commit),proofId);
    job.phase='ready';job.error=null;job.result=JSON.stringify(plan.binding);job.plan_bytes=0;this.save(job);
    const frontier=this.one<{n:number;bytes:number}>('SELECT COUNT(*) AS n,COALESCE(SUM(length(CAST(prefix AS BLOB))),0) AS bytes FROM projection_frontier WHERE job=?',job.id)!;
    const usage=this.usage();if(usage.frontier_rows<frontier.n||usage.selected_rows<selected.length||usage.path_bytes<frontier.bytes+job.path_bytes) throw new Invalid();
    this.host.sql.exec('UPDATE projection_usage SET frontier_rows=frontier_rows-?,selected_rows=selected_rows-?,path_bytes=path_bytes-? WHERE singleton=1',frontier.n,selected.length,frontier.bytes+job.path_bytes);
    this.host.sql.exec('DELETE FROM projection_plan_chunks WHERE job=?',job.id);
    this.host.sql.exec('DELETE FROM projection_selected WHERE job=?',job.id);
    this.host.sql.exec('DELETE FROM projection_outputs WHERE job=?',job.id);
    this.host.sql.exec('DELETE FROM projection_frontier WHERE job=?',job.id);
    return {projection_id:plan.record_key,head:hex(plan.binding.projected_commit)};
  }
  private async prove(view:ResolvedView,job:Job):Promise<ProjectionStatus> {
    const plan=await this.plan(view,job);const binding=this.closureBinding(job,plan);
    const closures=this.host.closureForProjection(view,binding,proof=>this.publishInTx(view,job,plan,proof),()=>{this.load(view,job.id);});
    if(!job.closure_job) {
      const status=await closures.begin(`projection-${job.id}`,{kind:3,id:hex(plan.binding.projected_commit)});
      return this.host.transactionSync(() => {const current=this.load(view,job.id,job.generation);if(current.closure_job!==null) fail(409,'aborted');current.closure_job=status.job_id;current.error=null;this.save(current);return this.report(current);});
    }
    const status=closures.status(job.closure_job);
    if(status.phase==='ready') {closures.publish(status.job_id,status.generation);return this.currentStatus(view,job.id);}
    if(status.phase==='published') throw new Invalid();
    if(status.phase==='limit') throw new Limit('closure');
    const next=await closures.advance(status.job_id,status.generation);
    return this.host.transactionSync(() => {const current=this.load(view,job.id,job.generation);current.error=next.error;if(next.phase==='limit') current.phase='limit';this.save(current);return this.report(current);});
  }
  private async advanceJob(view:ResolvedView,progress:ProjectionProgress):Promise<ProjectionStatus> {
    if(!progress||!safe(progress.expected_generation)||!uuid(progress.job_id)) fail(400,'invalid_argument');
    const job=this.host.transactionSync(()=>this.load(view,progress.job_id,progress.expected_generation));
    if(job.phase==='ready'||job.phase==='limit') return this.report(job);
    try {
      if(job.phase==='select') return await this.select(view,job);
      if(job.phase==='build') return await this.build(view,job);
      if(job.phase==='stage') return await this.stage(view,job);
      if(job.phase==='closure') return await this.prove(view,job);
      fail(503,'failed_precondition');
    } catch(error) {
      if(error instanceof Error&&'status' in error) throw error;
      return this.blocked(view,job,error);
    }
  }
  /** These guarded catalog methods are internal. A returned object is data, not
   * a transferable read capability; the read host must recheck the opaque view
   * after each backing await and immediately before returning bytes. */
  readyInTx(view:ResolvedView):ReadyProjection|undefined {
    const d=this.check(view);
    const row=this.one<Catalog>("SELECT id,binding,proof_job,retired FROM projection_catalogs WHERE account=? AND version=? AND epoch=? AND revision=? AND purpose='baseline'",this.host.accountId,hex(d.view_version),d.policy_epoch,hex(d.revision));
    if(!row||row.retired!==0) return undefined;
    let binding:ProjectionBinding;try{binding=JSON.parse(row.binding);}catch{throw new Invalid();}
    if(binding.account_id!==this.host.accountId||hex(binding.view_version)!==hex(d.view_version)||binding.policy_epoch!==d.policy_epoch||hex(binding.canonical_revision)!==hex(d.revision)) throw new Invalid();
    return {catalog_id:row.id,binding,proof_job:row.proof_job};
  }
  private catalog(view:ResolvedView,row:Catalog):ReadyProjection {
    const d=this.check(view);let binding:ProjectionBinding;try{binding=JSON.parse(row.binding);}catch{throw new Invalid();}
    if(row.retired!==0||binding.account_id!==this.host.accountId||hex(binding.view_version)!==hex(d.view_version)||binding.policy_epoch!==d.policy_epoch) throw new Invalid();
    hex(binding.canonical_revision);hex(binding.projected_commit);hex(binding.projected_tree);
    return {catalog_id:row.id,binding,proof_job:row.proof_job};
  }
  private catalogForProjectedInTx(view:ResolvedView,head:string):ReadyProjection|undefined {
    const d=this.check(view);if(!/^[0-9a-f]{64}$/.test(head)) fail(400,'invalid_argument');
    const revisions=this.rows<{revision:string}>('SELECT DISTINCT revision FROM projection_catalogs WHERE account=? AND version=? AND epoch=? AND projected_commit=? AND retired=0 LIMIT 2',this.host.accountId,hex(d.view_version),d.policy_epoch,head);
    if(!revisions.length) return undefined;
    // A projected identity must not silently choose between canonical bases.
    if(revisions.length!==1) fail(409,'aborted');
    const row=this.one<Catalog>('SELECT id,binding,proof_job,retired FROM projection_catalogs WHERE account=? AND version=? AND epoch=? AND projected_commit=? AND retired=0 ORDER BY id LIMIT 1',this.host.accountId,hex(d.view_version),d.policy_epoch,head)!;
    const result=this.catalog(view,row);
    if(hex(result.binding.projected_commit)!==head||hex(result.binding.canonical_revision)!==revisions[0].revision) throw new Invalid();
    return result;
  }
  readyReviewInTx(view:ResolvedView,input:Readonly<ReviewProjectionSource>):ReadyProjection|undefined {
    const source=this.source(view,input),d=this.check(view);
    const id=`forge/projections/v1/${this.host.accountId}/${hex(d.view_version)}/${d.policy_epoch}/review/${source.canonical_head}/${source.change_id}/${source.projected_parent}`;
    const row=this.one<Catalog>("SELECT id,binding,proof_job,retired FROM projection_catalogs WHERE id=? AND purpose='review'",id);
    if(!row||row.retired!==0) return undefined;
    const result=this.catalog(view,row);if(hex(result.binding.canonical_revision)!==source.canonical_head) throw new Invalid();return result;
  }
  /** Select canonical lineage endpoints from this EXACT baseline snapshot.
   * Historical closure membership, copy seeds and object availability alone
   * cannot establish which canonical copy occupies a selected source path. */
  canonicalFileWitnessInTx(view:ResolvedView,relativePath:string):CanonicalFileWitness|undefined {
    const d=this.check(view);
    if(typeof relativePath!=='string'||relativePath.length===0||encoder.encode(relativePath).length>4096) fail(400,'invalid_argument');
    const canonical=d.root_prefix?`${d.root_prefix}/${relativePath}`:relativePath;
    const decisions=this.allowedPathsInTx(view,[canonical]);
    if(decisions.length!==1||decisions[0].path!==canonical||!decisions[0].allowed) fail(403,'permission_denied');
    const ready=this.readyInTx(view);if(!ready) fail(400,'failed_precondition');
    const row=this.one<Selected&{member_size:number|null;origin:string|null}>('SELECT p.canonical_path,p.relative_path,p.kind,p.id,p.size,p.executable,m.size AS member_size,o.value AS origin FROM projection_paths p LEFT JOIN projection_members m ON m.projection=p.projection AND m.kind=p.kind AND m.id=p.id LEFT JOIN projection_private_origins o ON o.projection=p.projection AND o.canonical_path=p.canonical_path WHERE p.projection=? AND p.canonical_path=? AND p.relative_path=?',ready.catalog_id,canonical,relativePath);
    if(!row||row.kind!==1) return undefined;
    if(row.canonical_path!==canonical||row.relative_path!==relative(d.root_prefix,canonical)||row.member_size!==row.size||![0,1].includes(row.executable)||typeof row.origin!=='string'||encoder.encode(row.origin).length>8192) throw new Invalid();
    let origin:ProjectionCopyOrigin;try{origin=JSON.parse(row.origin);}catch{throw new Invalid();}
    if(!origin||typeof origin!=='object'||origin.canonical_path!==canonical||hex(origin.canonical_revision)!==hex(ready.binding.canonical_revision)||hex(origin.file)!==row.id) throw new Invalid();
    // Prove the private origin belongs to this exact generated catalog, rather
    // than accepting an arbitrary origin row or a different snapshot's seed.
    this.memberInTx(view,{kind:5,id:hex(origin.projected_copy)});
    const copy=origin.canonical_copy===null?null:hex(origin.canonical_copy);
    const file=descriptor({kind:1,id:row.id,size_bytes:row.size,format_version:1});
    const actual=this.host.describeObjectInTx(file);if(!actual||!same(file,actual)) fail(503,'unavailable');
    return Object.freeze({catalog_id:ready.catalog_id,account_id:this.host.accountId,view_version:hex(d.view_version),policy_epoch:d.policy_epoch,canonical_revision:hex(ready.binding.canonical_revision),relative_path:relativePath,canonical_path:canonical,file:Object.freeze(file),executable:row.executable===1,canonical_copy:copy});
  }
  canonicalForProjectedBaseInTx(view:ResolvedView,projectedCommit:string):{canonical_revision:string;projection_id:string}|undefined {
    const result=this.catalogForProjectedInTx(view,projectedCommit);
    return result?{canonical_revision:hex(result.binding.canonical_revision),projection_id:result.catalog_id}:undefined;
  }
  authorizedReadMemberInTx(view:ResolvedView,object:ObjectKey):{projection:ReadyProjection;descriptor:Descriptor} {
    const d=this.check(view);if(!key(object)) fail(400,'invalid_argument');
    const row=this.one<Catalog&{size:number}>('SELECT c.id,c.binding,c.proof_job,c.retired,m.size FROM projection_catalogs c JOIN projection_members m ON m.projection=c.id WHERE c.account=? AND c.version=? AND c.epoch=? AND c.retired=0 AND m.kind=? AND m.id=? ORDER BY c.id LIMIT 1',this.host.accountId,hex(d.view_version),d.policy_epoch,object.kind,object.id);
    if(!row) fail(403,'permission_denied');
    const projection=this.catalog(view,row),expected=descriptor({...object,size_bytes:row.size,format_version:1}),actual=this.host.describeObjectInTx(object);
    if(!actual||!same(expected,actual)) fail(503,'unavailable');return {projection,descriptor:expected};
  }
  /** Private normalization witness; never serialize this into product responses. */
  projectedCopyOriginInTx(view:ResolvedView,copyId:string):{projection:ReadyProjection;origin:ProjectionCopyOrigin}|undefined {
    const d=this.check(view);if(!key({kind:5,id:copyId})) fail(400,'invalid_argument');
    const row=this.one<Catalog&{value:string}>('SELECT c.id,c.binding,c.proof_job,c.retired,o.value FROM projection_private_origins o JOIN projection_catalogs c ON c.id=o.projection WHERE c.account=? AND c.version=? AND c.epoch=? AND c.retired=0 AND o.projected_copy=? ORDER BY c.id LIMIT 1',this.host.accountId,hex(d.view_version),d.policy_epoch,copyId);
    if(!row) return undefined;this.authorizedReadMemberInTx(view,{kind:5,id:copyId});const projection=this.catalog(view,row);let origin:ProjectionCopyOrigin;try{origin=JSON.parse(row.value);}catch{throw new Invalid();}
    if(hex(origin.projected_copy)!==copyId||hex(origin.canonical_revision)!==hex(projection.binding.canonical_revision)||!this.allowedPathsInTx(view,[origin.canonical_path])[0]?.allowed) throw new Invalid();
    hex(origin.file);if(origin.canonical_copy!==null) hex(origin.canonical_copy);return {projection,origin};
  }
  memberInTx(view:ResolvedView,object:ObjectKey):{projection:ReadyProjection;descriptor:Descriptor} {
    if(!key(object)) fail(400,'invalid_argument');
    const ready=this.readyInTx(view);if(!ready) fail(400,'failed_precondition');
    const row=this.one<{size:number}>('SELECT size FROM projection_members WHERE projection=? AND kind=? AND id=?',ready.catalog_id,object.kind,object.id);if(!row) fail(403,'permission_denied');
    const expected=descriptor({...object,size_bytes:row.size,format_version:1});const actual=this.host.describeObjectInTx(object);if(!actual||!same(expected,actual)) fail(503,'unavailable');return {projection:ready,descriptor:expected};
  }
  pathsInTx(view:ResolvedView):ProjectionPath[] {
    const ready=this.readyInTx(view);if(!ready) fail(400,'failed_precondition');
    const rows=this.rows<Selected&{member_size:number|null}>('SELECT p.canonical_path,p.relative_path,p.kind,p.id,p.size,p.executable,p.target_bytes,m.size AS member_size FROM projection_paths p LEFT JOIN projection_members m ON m.projection=p.projection AND m.kind=p.kind AND m.id=p.id WHERE p.projection=? ORDER BY p.canonical_path LIMIT 1025',ready.catalog_id);
    if(rows.length>MAX_SELECTED) throw new Invalid();let bytes=0;
    const decisions=this.allowedPathsInTx(view,rows.map(row=>row.canonical_path));
    if(decisions.length!==rows.length) throw new Invalid();
    return rows.map((row,i)=>{bytes+=encoder.encode(row.canonical_path).length+encoder.encode(row.relative_path).length;if(bytes>MAX_PATH_BYTES||decisions[i].path!==row.canonical_path||!decisions[i].allowed||row.relative_path!==relative(view.description.root_prefix,row.canonical_path)||row.member_size!==row.size) throw new Invalid();const expected=descriptor({kind:row.kind,id:row.id,size_bytes:row.size,format_version:1});const actual=this.host.describeObjectInTx(expected);if(!actual||!same(expected,actual)) fail(503,'unavailable');return {path:row.relative_path,canonical_path:row.canonical_path,descriptor:expected,executable:row.executable===1,target_bytes:row.target_bytes};});
  }
}
