/** Durable internal closure validation, not a route, upload API or ACL grant.
 * The trusted host owns current authorization, immutable payloads and final CAS.
 * All frontiers/cursors are SQL-owned; callers can only name a job/generation. */
import { closureStep, inspect, type Descriptor, type ObjectKey } from './core';
import { fail } from './protocol';

export type ClosureIdentity = { kind: 'principal'; principal: string } | { kind: 'view'; view_version: string };
export interface ClosureBinding {
  account_id: string;
  identity: ClosureIdentity;
  policy_epoch: number;
  purpose: string;
  /** Trusted canonical target/expected-CAS binding; never interpreted as a grant. */
  publication: string;
  /** Trusted scoped TREE upload context only; generic roots remain empty. */
  tree_prefix?: string;
}
export interface ClosureJobHost {
  binding: ClosureBinding;
  sql: SqlStorage;
  transactionSync<T>(callback: () => T): T;
  authorizeCurrentInTx(binding: ClosureBinding): void;
  /** Authoritative payload-availability catalog, NOT closure or access proof. */
  describeObjectInTx(key: ObjectKey): Descriptor | undefined;
  /** Fresh full bytes and descriptor, independently verified by the host. The
   * module also invokes native inspection before persisting any FILE check. */
  readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
  /** No awaits: recheck exact target/CAS encoded in binding.publication, then
   * attach the validated root in THIS SQL transaction. Must not perform I/O. */
  attachRootInTx(binding: ClosureBinding, root: ObjectKey, jobId: string): unknown;
}
type Phase = 'graph' | 'checks' | 'paths' | 'ready' | 'published' | 'limit';
interface Job {
  id: string; account: string; identity: string; request_id: string; fingerprint: string;
  binding: string; root_kind: number; root_id: string; phase: Phase; generation: number;
  nodes: number; bytes: number; edges: number; contexts: number; path_bytes: number; obligations: number;
  checked_nodes: number; checked_contexts: number; checked_obligations: number;
  error: string | null; result: string | null;
}
interface Node { kind: number; id: string; position: number; done: number; size: number | null; logical_size: number | null }
interface Context { tree: string; prefix: string; position: number; done: number }
interface Obligation { origin: string; ordinal: number; kind: 'file_size' | 'symlink_size' | 'copy_path'; target_kind: number; target_id: string; expected_size: number | null; path: string | null; done: number }
interface Usage { account: string; jobs: number; frontier_rows: number }
interface ReferencePage {
  descriptor: Descriptor; total: number; next_cursor: number | null;
  references: { ordinal: number; kind: number; id: string; root_tree: boolean; expected_size?: number; size_semantics?: 'stored_file' | 'symlink_target' }[];
}
interface ContextPage {
  descriptor: Descriptor; total: number; processed: number; next_cursor: number | null;
  contexts: { tree_id: string; prefix: string }[];
  copy_obligations: { copy_id: string; current_path: string }[];
}
export interface ClosureJobStatus {
  job_id: string; root: ObjectKey; phase: Phase; generation: number; complete: boolean;
  counts: { objects: number; bytes: number; edges: number; contexts: number; context_path_bytes: number; obligations: number; checked_objects: number; checked_contexts: number; checked_obligations: number };
  error: string | null;
}
const MAX_JOBS = 4096;
const MAX_FRONTIER_ROWS = 262144;
const MAX_NODES = 32768;
const MAX_BYTES = 512 * 1024 * 1024;
const MAX_EDGES = 131072;
const MAX_CONTEXTS = 32768;
const MAX_PATH_BYTES = 4 * 1024 * 1024;
const MAX_OBLIGATIONS = 131072;
const MAX_FILE = 1024 * 1024;
const MAX_METADATA = 256 * 1024;
const MAX_RESULT = 64 * 1024;
const ROOT = '0'.repeat(64);
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });
class Limit extends Error { constructor() { super('closure_limit'); } }
class InvalidObject extends Error { constructor() { super('object_invalid'); } }
interface Work { nodes: number; edges: number; contexts: number; checks: number; files: number; metadata: number; deadline: number }
type WorkItem = { type: 'node'; value: Node } | { type: 'check'; value: Obligation } | { type: 'context'; value: Context };
function safe(value: number, max = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(value) && value >= 0 && value <= max; }
function utf8(value: string, maximum: number): boolean { return encoder.encode(value).byteLength <= maximum && decoder.decode(encoder.encode(value)) === value; }
function validKey(value: ObjectKey): boolean { return Number.isInteger(value.kind) && value.kind >= 1 && value.kind <= 5 && /^[0-9a-f]{64}$/.test(value.id); }
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function root(job: Job): ObjectKey { return { kind: job.root_kind, id: job.root_id }; }
function terminal(job: Job): boolean { return job.phase === 'ready' || job.phase === 'published' || job.phase === 'limit'; }
function wire(object: { descriptor: Descriptor; bytes: Uint8Array }): unknown { return { ...object.descriptor, bytes: Array.from(object.bytes) }; }
/** Canonical tagged identity, not a principal synthesized from a view name.
 * A view-scoped job may be shared by public/authenticated readers; the host
 * MUST still authorize its actual current caller on every operation. */
function identityCopy(value: ClosureIdentity): ClosureIdentity {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Reflect.ownKeys(value).length !== 2) fail(400, 'invalid_argument');
  if (value.kind === 'principal' && Object.hasOwn(value, 'kind') && Object.hasOwn(value, 'principal')
      && typeof value.principal === 'string' && value.principal.length > 0 && utf8(value.principal, 128)
      && !/[\u0000-\u001f\u007f-\u009f]/u.test(value.principal)) return Object.freeze({ kind: 'principal', principal: value.principal });
  if (value.kind === 'view' && Object.hasOwn(value, 'kind') && Object.hasOwn(value, 'view_version')
      && typeof value.view_version === 'string' && /^[0-9a-f]{64}$/.test(value.view_version)) return Object.freeze({ kind: 'view', view_version: value.view_version });
  fail(400, 'invalid_argument');
}
function bindingString(binding: ClosureBinding): string {
  if (!/^[a-z0-9-]{1,64}$/.test(binding.account_id) || !safe(binding.policy_epoch) || binding.policy_epoch < 1
      || !/^[A-Za-z0-9_.-]{1,64}$/.test(binding.purpose) || !utf8(binding.publication, 4096) || binding.publication.length === 0) fail(400, 'invalid_argument');
  if (binding.tree_prefix !== undefined && (typeof binding.tree_prefix !== 'string' || !utf8(binding.tree_prefix, 4096))) fail(400, 'invalid_argument');
  const fields: unknown[] = [binding.account_id, binding.identity, binding.policy_epoch, binding.purpose, binding.publication];
  // Preserve the existing generic empty-root binding exactly. Nonempty scoped
  // TREE prefixes become immutable job identity, not a second empty context.
  if (binding.tree_prefix) fields.push(['tree_prefix', binding.tree_prefix]);
  return JSON.stringify(fields);
}

export class ClosureJobs {
  private readonly binding: ClosureBinding;
  private readonly encodedBinding: string;
  private readonly encodedIdentity: string;
  constructor(private readonly host: ClosureJobHost) {
    // Capture request-scoped trusted identity; do not retain a mutable auth bag.
    this.binding = Object.freeze({ ...host.binding, identity: identityCopy(host.binding.identity) });
    this.encodedIdentity = JSON.stringify(this.binding.identity); this.encodedBinding = bindingString(this.binding);
    for (const statement of [
      'CREATE TABLE IF NOT EXISTS closure_job_usage (singleton INTEGER PRIMARY KEY CHECK(singleton=1), account TEXT NOT NULL, jobs INTEGER NOT NULL, frontier_rows INTEGER NOT NULL)',
      'CREATE TABLE IF NOT EXISTS closure_jobs (id TEXT PRIMARY KEY, account TEXT NOT NULL, identity TEXT NOT NULL, request_id TEXT NOT NULL, fingerprint TEXT NOT NULL, binding TEXT NOT NULL, root_kind INTEGER NOT NULL, root_id TEXT NOT NULL, phase TEXT NOT NULL, generation INTEGER NOT NULL, nodes INTEGER NOT NULL, bytes INTEGER NOT NULL, edges INTEGER NOT NULL, contexts INTEGER NOT NULL, path_bytes INTEGER NOT NULL, obligations INTEGER NOT NULL, checked_nodes INTEGER NOT NULL, checked_contexts INTEGER NOT NULL, checked_obligations INTEGER NOT NULL, error TEXT, result TEXT, UNIQUE(account,identity,request_id))',
      'CREATE TABLE IF NOT EXISTS closure_nodes (job TEXT NOT NULL, kind INTEGER NOT NULL, id TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0, size INTEGER, logical_size INTEGER, PRIMARY KEY(job,kind,id))',
      'CREATE INDEX IF NOT EXISTS closure_nodes_pending ON closure_nodes(job,done,kind,id)',
      'CREATE TABLE IF NOT EXISTS closure_contexts (job TEXT NOT NULL, tree TEXT NOT NULL, prefix TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(job,tree,prefix))',
      'CREATE INDEX IF NOT EXISTS closure_contexts_pending ON closure_contexts(job,done,tree,prefix)',
      'CREATE TABLE IF NOT EXISTS closure_obligations (job TEXT NOT NULL, origin TEXT NOT NULL, ordinal INTEGER NOT NULL, kind TEXT NOT NULL, target_kind INTEGER NOT NULL, target_id TEXT NOT NULL, expected_size INTEGER, path TEXT, done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(job,origin,ordinal))',
      'CREATE INDEX IF NOT EXISTS closure_obligations_pending ON closure_obligations(job,done,origin,ordinal)',
    ]) host.sql.exec(statement);
    host.transactionSync(() => {
      host.sql.exec('INSERT OR IGNORE INTO closure_job_usage(singleton,account,jobs,frontier_rows) VALUES(1,?,0,0)', this.binding.account_id);
      this.usage();
    });
  }
  private one<T>(query: string, ...bindings: (string | number | null)[]): T | undefined { return [...this.host.sql.exec(query, ...bindings)][0] as T | undefined; }
  private usage(): Usage {
    const value = this.one<Usage>('SELECT account,jobs,frontier_rows FROM closure_job_usage WHERE singleton=1')!;
    if (value.account !== this.binding.account_id) fail(403, 'permission_denied'); return value;
  }
  private load(id: string, generation?: number): Job {
    this.host.authorizeCurrentInTx(this.binding);
    if (!/^[0-9a-f-]{36}$/.test(id)) fail(400, 'invalid_argument');
    const job = this.one<Job>('SELECT * FROM closure_jobs WHERE id=?', id);
    if (!job || job.account !== this.binding.account_id || job.identity !== this.encodedIdentity) fail(403, 'permission_denied');
    if (job.binding !== this.encodedBinding) fail(409, 'aborted');
    if (generation !== undefined && (!safe(generation) || job.generation !== generation)) fail(409, 'aborted');
    return job;
  }
  private report(job: Job): ClosureJobStatus {
    return { job_id: job.id, root: root(job), phase: job.phase, generation: job.generation,
      complete: job.phase === 'ready' || job.phase === 'published', counts: { objects: job.nodes, bytes: job.bytes,
        edges: job.edges, contexts: job.contexts, context_path_bytes: job.path_bytes, obligations: job.obligations,
        checked_objects: job.checked_nodes, checked_contexts: job.checked_contexts, checked_obligations: job.checked_obligations }, error: job.error };
  }
  private save(job: Job): void {
    if (!safe(job.generation, Number.MAX_SAFE_INTEGER - 1)) throw new Limit();
    job.generation++;
    this.host.sql.exec('UPDATE closure_jobs SET phase=?,generation=?,nodes=?,bytes=?,edges=?,contexts=?,path_bytes=?,obligations=?,checked_nodes=?,checked_contexts=?,checked_obligations=?,error=?,result=? WHERE id=?',
      job.phase, job.generation, job.nodes, job.bytes, job.edges, job.contexts, job.path_bytes, job.obligations, job.checked_nodes, job.checked_contexts, job.checked_obligations, job.error, job.result, job.id);
  }
  private reserveFrontier(): void {
    const usage = this.usage(); if (usage.frontier_rows >= MAX_FRONTIER_ROWS) throw new Limit();
    this.host.sql.exec('UPDATE closure_job_usage SET frontier_rows=frontier_rows+1 WHERE singleton=1');
  }
  private enqueue(job: Job, key: ObjectKey): void {
    if (!validKey(key)) throw new InvalidObject();
    if (key.kind === 3 && key.id === ROOT) return;
    if (this.one('SELECT 1 FROM closure_nodes WHERE job=? AND kind=? AND id=?', job.id, key.kind, key.id)) return;
    if (job.nodes >= MAX_NODES) throw new Limit(); this.reserveFrontier();
    this.host.sql.exec('INSERT INTO closure_nodes(job,kind,id) VALUES(?,?,?)', job.id, key.kind, key.id); job.nodes++;
  }
  private context(job: Job, tree: string, prefix: string): void {
    if (!/^[0-9a-f]{64}$/.test(tree) || !utf8(prefix, 4096)) throw new InvalidObject();
    if (this.one('SELECT 1 FROM closure_contexts WHERE job=? AND tree=? AND prefix=?', job.id, tree, prefix)) return;
    const bytes = encoder.encode(prefix).byteLength;
    if (job.contexts >= MAX_CONTEXTS || job.path_bytes + bytes > MAX_PATH_BYTES) throw new Limit(); this.reserveFrontier();
    this.host.sql.exec('INSERT INTO closure_contexts(job,tree,prefix) VALUES(?,?,?)', job.id, tree, prefix); job.contexts++; job.path_bytes += bytes;
  }
  private obligation(job: Job, origin: string, ordinal: number, kind: Obligation['kind'], key: ObjectKey, size: number | null, path: string | null): void {
    if (!validKey(key) || !safe(ordinal) || !utf8(origin, 4352) || (size !== null && !safe(size)) || (path !== null && !utf8(path, 4096))) throw new InvalidObject();
    if (this.one('SELECT 1 FROM closure_obligations WHERE job=? AND origin=? AND ordinal=?', job.id, origin, ordinal)) return;
    if (job.obligations >= MAX_OBLIGATIONS) throw new Limit(); this.reserveFrontier();
    this.host.sql.exec('INSERT INTO closure_obligations(job,origin,ordinal,kind,target_kind,target_id,expected_size,path) VALUES(?,?,?,?,?,?,?,?)', job.id, origin, ordinal, kind, key.kind, key.id, size, path); job.obligations++;
  }
  async begin(requestId: string, key: ObjectKey): Promise<ClosureJobStatus> {
    if (!/^[A-Za-z0-9_.-]{1,128}$/.test(requestId) || !validKey(key) || (key.kind === 3 && key.id === ROOT)) fail(400, 'invalid_argument');
    if (this.binding.tree_prefix !== undefined && key.kind !== 2) fail(400, 'invalid_argument');
    key = { kind: key.kind, id: key.id }; // Pin caller-owned input before crypto await.
    this.host.transactionSync(() => this.host.authorizeCurrentInTx(this.binding));
    const payload = encoder.encode(JSON.stringify([this.encodedBinding, key.kind, key.id]));
    const fingerprint = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', payload)), byte => byte.toString(16).padStart(2, '0')).join('');
    return this.host.transactionSync(() => {
      this.host.authorizeCurrentInTx(this.binding);
      const previous = this.one<Job>('SELECT * FROM closure_jobs WHERE account=? AND identity=? AND request_id=?', this.binding.account_id, this.encodedIdentity, requestId);
      if (previous) {
        if (previous.fingerprint !== fingerprint || previous.binding !== this.encodedBinding) fail(400, 'invalid_argument');
        return this.report(previous);
      }
      if (this.usage().jobs >= MAX_JOBS) fail(429, 'resource_exhausted');
      const job: Job = { id: crypto.randomUUID(), account: this.binding.account_id, identity: this.encodedIdentity, request_id: requestId,
        fingerprint, binding: this.encodedBinding, root_kind: key.kind, root_id: key.id, phase: 'graph', generation: 0,
        nodes: 0, bytes: 0, edges: 0, contexts: 0, path_bytes: 0, obligations: 0, checked_nodes: 0, checked_contexts: 0, checked_obligations: 0, error: null, result: null };
      this.host.sql.exec('INSERT INTO closure_jobs(id,account,identity,request_id,fingerprint,binding,root_kind,root_id,phase,generation,nodes,bytes,edges,contexts,path_bytes,obligations,checked_nodes,checked_contexts,checked_obligations) VALUES(?,?,?,?,?,?,?,?,?,0,0,0,0,0,0,0,0,0,0)', job.id, job.account, job.identity, requestId, fingerprint, job.binding, key.kind, key.id, job.phase);
      try { this.enqueue(job, key); if (key.kind === 2) this.context(job, key.id, this.binding.tree_prefix ?? ''); this.save(job); }
      catch (error) { if (error instanceof Limit) fail(429, 'resource_exhausted'); throw error; }
      this.host.sql.exec('UPDATE closure_job_usage SET jobs=jobs+1 WHERE singleton=1');
      return this.report(job);
    });
  }
  status(id: string): ClosureJobStatus { return this.host.transactionSync(() => this.report(this.load(id))); }
  private next(job: Job): WorkItem | undefined {
    const node = this.one<Node>('SELECT kind,id,position,done,size,logical_size FROM closure_nodes WHERE job=? AND done=0 ORDER BY kind,id LIMIT 1', job.id);
    if (node) return { type: 'node', value: node };
    const obligation = this.one<Obligation>('SELECT * FROM closure_obligations WHERE job=? AND done=0 ORDER BY origin,ordinal LIMIT 1', job.id);
    if (obligation) return { type: 'check', value: obligation };
    const context = this.one<Context>('SELECT tree,prefix,position,done FROM closure_contexts WHERE job=? AND done=0 ORDER BY tree,prefix LIMIT 1', job.id);
    return context ? { type: 'context', value: context } : undefined;
  }
  private describe(id: string, generation: number, key: ObjectKey): Descriptor | undefined {
    return this.host.transactionSync(() => {
      this.load(id, generation); const value = this.host.describeObjectInTx(key);
      if (!value) return undefined;
      if (!validKey(value) || value.kind !== key.kind || value.id !== key.id || value.format_version !== 1 || !safe(value.size_bytes)
          || value.size_bytes > (key.kind === 1 ? MAX_FILE : MAX_METADATA)) throw new InvalidObject();
      return { kind: value.kind, id: value.id, format_version: value.format_version, size_bytes: value.size_bytes };
    });
  }
  private async read(key: ObjectKey, expected: Descriptor, work: Work): Promise<{ descriptor: Descriptor; bytes: Uint8Array }> {
    // Reserve admission before the await. The deadline stops NEW work only; an
    // admitted storage await is not force-cancelled or claimed to have timed out.
    if (key.kind === 1) work.files += expected.size_bytes; else work.metadata += expected.size_bytes;
    const value = await this.host.readVerifiedObject(key);
    if (!same(expected, value.descriptor) || value.bytes.byteLength !== expected.size_bytes) throw new InvalidObject();
    let actual: Descriptor;
    try { actual = inspect(key.kind, value.bytes); } catch { throw new InvalidObject(); }
    if (!same(expected, actual)) throw new InvalidObject();
    return { descriptor: expected, bytes: value.bytes };
  }
  private fits(descriptor: Descriptor, work: Work): boolean {
    return descriptor.kind === 1 ? work.files + descriptor.size_bytes <= 4 * 1024 * 1024 : work.metadata + descriptor.size_bytes <= MAX_METADATA;
  }
  private blocked(id: string, generation: number, code: 'object_unavailable' | 'object_invalid' | 'closure_limit'): ClosureJobStatus {
    return this.host.transactionSync(() => {
      const job = this.load(id, generation); job.error = code; if (code === 'closure_limit') job.phase = 'limit'; this.save(job); return this.report(job);
    });
  }
  private ensureCatalog(key: ObjectKey, expected: Descriptor): void {
    const current = this.host.describeObjectInTx(key);
    if (!current || !same(expected, current)) throw new InvalidObject();
  }
  private native(input: unknown): unknown {
    try { return closureStep(input); } catch { throw new InvalidObject(); }
  }
  private commitNode(job: Job, node: Node, value: { descriptor: Descriptor; bytes: Uint8Array }, page: ReferencePage | null): Job {
    return this.host.transactionSync(() => {
      const current = this.load(job.id, job.generation);
      const stored = this.one<Node>('SELECT kind,id,position,done,size,logical_size FROM closure_nodes WHERE job=? AND kind=? AND id=?', job.id, node.kind, node.id);
      if (!stored || stored.done || stored.position !== node.position) fail(409, 'aborted');
      this.ensureCatalog(node, value.descriptor);
      if (stored.size !== null && stored.size !== value.descriptor.size_bytes) throw new InvalidObject();
      if (stored.size === null) {
        if (current.bytes + value.descriptor.size_bytes > MAX_BYTES) throw new Limit();
        current.bytes += value.descriptor.size_bytes;
      }
      let logical = value.descriptor.size_bytes;
      if (node.kind === 4) {
        const info = inspect(4, value.bytes) as Descriptor & { symlink?: { target_bytes: number } };
        if (!info.symlink || !safe(info.symlink.target_bytes)) throw new InvalidObject(); logical = info.symlink.target_bytes;
      }
      let position = 0; let done = true;
      if (page) {
        if (!same(page.descriptor, value.descriptor) || !safe(page.total, 8192) || !Array.isArray(page.references) || page.references.length > 128) throw new InvalidObject();
        position = node.position + page.references.length;
        if (position > page.total || (page.next_cursor === null ? position !== page.total : page.next_cursor !== position || position <= node.position)) throw new InvalidObject();
        done = page.next_cursor === null;
        if (current.edges + page.references.length > MAX_EDGES) throw new Limit(); current.edges += page.references.length;
        for (let i = 0; i < page.references.length; i++) {
          const reference = page.references[i];
          if (reference.ordinal !== node.position + i) throw new InvalidObject();
          this.enqueue(current, reference);
          if (reference.root_tree) { if (reference.kind !== 2) throw new InvalidObject(); this.context(current, reference.id, ''); }
          if (reference.expected_size !== undefined) {
            if (!safe(reference.expected_size)) throw new InvalidObject();
            const semantics = reference.size_semantics;
            if ((semantics !== 'stored_file' || reference.kind !== 1) && (semantics !== 'symlink_target' || reference.kind !== 4)) throw new InvalidObject();
            this.obligation(current, `g/${node.kind}/${node.id}`, reference.ordinal, semantics === 'stored_file' ? 'file_size' : 'symlink_size', reference, reference.expected_size, null);
          }
        }
      }
      this.host.sql.exec('UPDATE closure_nodes SET position=?,done=?,size=?,logical_size=? WHERE job=? AND kind=? AND id=?', position, done ? 1 : 0, value.descriptor.size_bytes, logical, job.id, node.kind, node.id);
      if (done) current.checked_nodes++;
      current.phase = 'graph'; current.error = null; this.save(current); return current;
    });
  }
  private fileCheck(job: Job, obligation: Obligation): Job {
    return this.host.transactionSync(() => {
      const current = this.load(job.id, job.generation);
      const stored = this.one<Obligation>('SELECT * FROM closure_obligations WHERE job=? AND origin=? AND ordinal=?', job.id, obligation.origin, obligation.ordinal);
      if (!stored || stored.done) fail(409, 'aborted');
      const node = this.one<Node>('SELECT kind,id,position,done,size,logical_size FROM closure_nodes WHERE job=? AND kind=? AND id=?', job.id, obligation.target_kind, obligation.target_id);
      if (!node || !node.done || node.kind !== 1 || node.size === null) throw new InvalidObject();
      const descriptor: Descriptor = { kind: 1, id: node.id, size_bytes: node.size, format_version: 1 };
      this.ensureCatalog(descriptor, descriptor);
      // This native item is only a descriptor check. The job node above was
      // completed by fresh host+native FILE-byte verification, not this item.
      const checked = this.native({ operation: 'check', obligations: [{ operation: 'file_size', key: { kind: 1, id: node.id }, expected_size: obligation.expected_size, descriptor }] }) as { valid?: boolean; checked?: number };
      if (checked.valid !== true || checked.checked !== 1) throw new InvalidObject();
      this.host.sql.exec('UPDATE closure_obligations SET done=1 WHERE job=? AND origin=? AND ordinal=?', job.id, obligation.origin, obligation.ordinal);
      current.checked_obligations++; current.phase = 'checks'; current.error = null; this.save(current); return current;
    });
  }
  private commitCheck(job: Job, obligation: Obligation, value: { descriptor: Descriptor; bytes: Uint8Array }): Job {
    const key = { kind: obligation.target_kind, id: obligation.target_id };
    const item = obligation.kind === 'symlink_size'
      ? { operation: 'symlink_size', key, expected_size: obligation.expected_size, object: wire(value) }
      : { operation: 'copy_path', key, expected_path: obligation.path, object: wire(value) };
    const result = this.native({ operation: 'check', obligations: [item] }) as { valid?: boolean; checked?: number };
    if (result.valid !== true || result.checked !== 1) throw new InvalidObject();
    return this.host.transactionSync(() => {
      const current = this.load(job.id, job.generation);
      const stored = this.one<Obligation>('SELECT * FROM closure_obligations WHERE job=? AND origin=? AND ordinal=?', job.id, obligation.origin, obligation.ordinal);
      if (!stored || stored.done) fail(409, 'aborted');
      this.ensureCatalog(key, value.descriptor);
      this.host.sql.exec('UPDATE closure_obligations SET done=1 WHERE job=? AND origin=? AND ordinal=?', job.id, obligation.origin, obligation.ordinal);
      current.checked_obligations++; current.phase = 'checks'; current.error = null; this.save(current); return current;
    });
  }
  private commitContext(job: Job, context: Context, value: { descriptor: Descriptor; bytes: Uint8Array }, page: ContextPage, limit: number): Job {
    return this.host.transactionSync(() => {
      const current = this.load(job.id, job.generation);
      const stored = this.one<Context>('SELECT tree,prefix,position,done FROM closure_contexts WHERE job=? AND tree=? AND prefix=?', job.id, context.tree, context.prefix);
      if (!stored || stored.done || stored.position !== context.position) fail(409, 'aborted');
      this.ensureCatalog({ kind: 2, id: context.tree }, value.descriptor);
      if (!same(page.descriptor, value.descriptor) || !safe(page.total, 4096) || !safe(page.processed, limit)
          || !Array.isArray(page.contexts) || !Array.isArray(page.copy_obligations) || page.contexts.length + page.copy_obligations.length > page.processed) throw new InvalidObject();
      const position = context.position + page.processed;
      if (position > page.total || (page.next_cursor === null ? position !== page.total : page.next_cursor !== position || position <= context.position)) throw new InvalidObject();
      for (const child of page.contexts) this.context(current, child.tree_id, child.prefix);
      for (const copy of page.copy_obligations) {
        // Full native path makes this identity independent of page size and of
        // plain entries omitted from the helper's obligation array.
        this.obligation(current, `c/${context.tree}/${copy.current_path}`, 0, 'copy_path', { kind: 5, id: copy.copy_id }, null, copy.current_path);
      }
      this.host.sql.exec('UPDATE closure_contexts SET position=?,done=? WHERE job=? AND tree=? AND prefix=?', position, page.next_cursor === null ? 1 : 0, job.id, context.tree, context.prefix);
      if (page.next_cursor === null) current.checked_contexts++;
      current.phase = 'paths'; current.error = null; this.save(current); return current;
    });
  }
  /** Per-call counters bound admitted work. A deadline stops new reads/pages;
   * it does not cancel an already admitted storage await or fabricate failure. */
  async advance(id: string, expectedGeneration: number): Promise<ClosureJobStatus> {
    let job = this.host.transactionSync(() => this.load(id, expectedGeneration));
    const work: Work = { nodes: 0, edges: 0, contexts: 0, checks: 0, files: 0, metadata: 0, deadline: Date.now() + 5000 };
    while (!terminal(job) && Date.now() < work.deadline) {
      try {
        const item = this.host.transactionSync(() => this.next(this.load(id, job.generation)));
        if (!item) {
          job = this.host.transactionSync(() => {
            const current = this.load(id, job.generation);
            if (this.next(current) || current.checked_nodes !== current.nodes || current.checked_contexts !== current.contexts || current.checked_obligations !== current.obligations) throw new InvalidObject();
            current.phase = 'ready'; current.error = null; this.save(current); return current;
          });
          break;
        }
        if (item.type === 'check' && item.value.kind === 'file_size') {
          if (work.checks >= 128) break;
          job = this.fileCheck(job, item.value); work.checks++; continue;
        }
        if (work.nodes >= 64 || (item.type === 'node' && work.edges >= 2048) || (item.type === 'context' && work.contexts >= 128) || (item.type === 'check' && work.checks >= 128)) break;
        const key: ObjectKey = item.type === 'node' ? { kind: item.value.kind, id: item.value.id }
          : item.type === 'context' ? { kind: 2, id: item.value.tree } : { kind: item.value.target_kind, id: item.value.target_id };
        const expected = this.describe(id, job.generation, key);
        if (!expected) return this.blocked(id, job.generation, 'object_unavailable');
        if (!this.fits(expected, work)) break;
        if (item.type === 'node' && item.value.size === null && job.bytes + expected.size_bytes > MAX_BYTES) throw new Limit();
        let value: { descriptor: Descriptor; bytes: Uint8Array };
        try { value = await this.read(key, expected, work); }
        catch (error) { return this.blocked(id, job.generation, error instanceof InvalidObject ? 'object_invalid' : 'object_unavailable'); }
        work.nodes++;
        if (item.type === 'node') {
          let page: ReferencePage | null = null;
          if (key.kind !== 1) page = this.native({ operation: 'references', object: wire(value), cursor: item.value.position, limit: Math.min(128, 2048 - work.edges) }) as ReferencePage;
          job = this.commitNode(job, item.value, value, page); work.edges += page?.references.length ?? 0;
        } else if (item.type === 'check') {
          job = this.commitCheck(job, item.value, value); work.checks++;
        } else {
          const limit = Math.min(128, 128 - work.contexts);
          const page = this.native({ operation: 'tree_context', object: wire(value), prefix: item.value.prefix, cursor: item.value.position, limit }) as ContextPage;
          job = this.commitContext(job, item.value, value, page, limit); work.contexts += Math.max(1, page.processed);
        }
      } catch (error) {
        if (error instanceof Limit) return this.blocked(id, job.generation, 'closure_limit');
        if (error instanceof InvalidObject) return this.blocked(id, job.generation, 'object_invalid');
        throw error;
      }
    }
    return this.host.transactionSync(() => this.report(this.load(id, job.generation)));
  }
  publish(id: string, expectedGeneration: number): { status: ClosureJobStatus; result: unknown } {
    if (!safe(expectedGeneration)) fail(400, 'invalid_argument');
    return this.host.transactionSync(() => {
      // Current auth and exact stored binding precede historical receipt replay.
      const job = this.load(id);
      if (job.phase === 'published') {
        if (job.result === null) fail(503, 'failed_precondition');
        return { status: this.report(job), result: JSON.parse(job.result) };
      }
      if (job.generation !== expectedGeneration) fail(409, 'aborted');
      if (job.phase !== 'ready' || this.next(job) || job.checked_nodes !== job.nodes || job.checked_contexts !== job.contexts || job.checked_obligations !== job.obligations) fail(400, 'failed_precondition');
      // The callback, not this generic module, validates exact publication CAS
      // and legal root/purpose semantics. Any failure rolls back the whole tx.
      const result = this.host.attachRootInTx(this.binding, root(job), job.id);
      let encoded: string | undefined;
      try { encoded = JSON.stringify(result); } catch { fail(503, 'failed_precondition'); }
      if (encoded === undefined || encoder.encode(encoded).byteLength > MAX_RESULT) fail(413, 'resource_exhausted');
      job.result = encoded; job.phase = 'published'; job.error = null; this.save(job);
      return { status: this.report(job), result };
    });
  }
}
