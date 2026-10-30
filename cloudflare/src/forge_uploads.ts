/** Actor/path/epoch-bound ordinary uploads. Availability is never ownership.
 * Internal proof cursors are SQL-owned; no main/view/review/check mutation. */
import { inspect, normalizeForgeUploadCommand, uploadScopePage, type Descriptor, type ForgeCall, type ObjectKey } from './core';
import { ClosureJobs, type ClosureBinding } from './closure_jobs';
import type { ForgeViews, ResolvedView } from './forge_views';
import { fail } from './protocol';

export type UploadRequest = { action: 'begin'; version: number[]; path: string; kind: number; id: number[]; size_bytes: number }
  | { action: 'append'; session_id: number[]; offset: number; data: number[] }
  | { action: 'status' | 'finalize'; session_id: number[] };
export interface UploadCommand { operation: 10; session_id: string | null; request: UploadRequest }
type Role = 'root' | 'tree' | 'copy_at' | 'copy_parent';
interface ScopeContext { canonical_path: string; depth: number; role: Role }
export interface UploadScopeInput { descriptor: Descriptor; bytes_base64: string; context: ScopeContext; cursor: number; limit: number }
export interface UploadScopePage {
  next_cursor: number | null; total: number; entries: number;
  children: ({ key: ObjectKey } & ScopeContext)[];
  requirements: ({ kind: 'path'; canonical_path: string } | { kind: 'reference'; canonical_path: string; key: ObjectKey; expected_size: number | null })[];
}
export interface ForgeUploadsHost {
  accountId: string; principal: string | null; sql: SqlStorage; views: ForgeViews;
  transactionSync<T>(callback: () => T): T;
  nowSeconds(): number;
  requireWriterInTx(): void;
  /** Only actual selected-view/current-epoch projection membership; an absent
   * review catalog supplies no witness. Propagate policy/integrity failures. */
  projectionMemberInTx(view: ResolvedView, descriptor: Descriptor): boolean;
  describeObjectInTx(key: ObjectKey): Descriptor | undefined;
  readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
  reserveUploadInTx(sessionId: string, descriptor: Descriptor): void;
  stageUpload(descriptor: Descriptor, bytes: Uint8Array): Promise<unknown>;
  confirmUploadInTx(sessionId: string, descriptor: Descriptor, confirmation: unknown): void;
  retainUploadInTx(sessionId: string, descriptor: Descriptor, expiresAt: number): { staging_lease: string; expires_at: number };
}
interface Session {
  id: string; account: string; principal: string; view_name: string; version: string; revision: string; epoch: number;
  path: string; kind: number; object_id: string; size: number; binding: string; position: number; expires_at: number;
  state: 'receiving' | 'verifying' | 'committed' | 'expired' | 'obsolete' | 'failed'; generation: number; scope_done: number;
  scope_nodes: number; scope_entries: number; scope_path_bytes: number; authority_until: number; integrity_job: string | null; receipt: string | null;
}
interface Node { kind: number; id: string; path: string; depth: number; role: Role; position: number; done: number }
interface Usage { account: string; records: number; active: number; assembly_bytes: number; frontier_rows: number }
interface OwnReceipt { size: number; format: number; expires_at: number }
const MAX_FILE = 1024 * 1024, MAX_META = 256 * 1024, MAX_NODES = 32768, MAX_FRONTIER = 262144;
const MAX_PATH_BYTES = 4 * 1024 * 1024, MAX_ENTRIES = 10000, TTL = 86400;
const encoder = new TextEncoder();
function safe(n: number, max = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(n) && n >= 0 && n <= max; }
function hex(bytes: number[]): string {
  if (!Array.isArray(bytes) || bytes.length !== 32 || !bytes.every(b => safe(b, 255))) fail(400, 'invalid_argument');
  return bytes.map(b => b.toString(16).padStart(2, '0')).join('');
}
function array(id: string): number[] { return Array.from(id.match(/../g)!, byte => parseInt(byte, 16)); }
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function descriptor(s: Session): Descriptor { return { kind: s.kind, id: s.object_id, size_bytes: s.size, format_version: 1 }; }
function base64(bytes: Uint8Array): string { let text = ''; for (let i = 0; i < bytes.length; i += 8192) text += String.fromCharCode(...bytes.subarray(i, i + 8192)); return btoa(text); }
function encode(value: unknown, maximum = 16 * 1024): string { const text = JSON.stringify(value); if (encoder.encode(text).byteLength > maximum) fail(429, 'resource_exhausted'); return text; }
export class ForgeUploads {
  private readonly account: string;
  private readonly principal: string;
  constructor(private readonly host: ForgeUploadsHost) {
    if (host.principal === null) fail(403, 'permission_denied'); this.account = host.accountId; this.principal = host.principal;
    for (const sql of [
      'CREATE TABLE IF NOT EXISTS forge_upload_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,records INTEGER NOT NULL,active INTEGER NOT NULL,assembly_bytes INTEGER NOT NULL,frontier_rows INTEGER NOT NULL)',
      'CREATE TABLE IF NOT EXISTS forge_upload_sessions(id TEXT PRIMARY KEY,account TEXT NOT NULL,principal TEXT NOT NULL,view_name TEXT NOT NULL,version TEXT NOT NULL,revision TEXT NOT NULL,epoch INTEGER NOT NULL,path TEXT NOT NULL,kind INTEGER NOT NULL,object_id TEXT NOT NULL,size INTEGER NOT NULL,binding TEXT NOT NULL,position INTEGER NOT NULL,expires_at INTEGER NOT NULL,state TEXT NOT NULL,generation INTEGER NOT NULL,scope_done INTEGER NOT NULL,scope_nodes INTEGER NOT NULL,scope_entries INTEGER NOT NULL,scope_path_bytes INTEGER NOT NULL,authority_until INTEGER NOT NULL,integrity_job TEXT,receipt TEXT)',
      'CREATE INDEX IF NOT EXISTS forge_upload_expiry ON forge_upload_sessions(state,expires_at)',
      'CREATE TABLE IF NOT EXISTS forge_upload_buffers(session TEXT PRIMARY KEY,data BLOB NOT NULL)',
      'CREATE TABLE IF NOT EXISTS forge_upload_scope(session TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,path TEXT NOT NULL,depth INTEGER NOT NULL,role TEXT NOT NULL,position INTEGER NOT NULL DEFAULT 0,done INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(session,kind,id,path,depth,role))',
      'CREATE INDEX IF NOT EXISTS forge_upload_scope_pending ON forge_upload_scope(session,done,kind,id,path,depth,role)',
      'CREATE TABLE IF NOT EXISTS forge_owned_uploads(account TEXT NOT NULL,principal TEXT NOT NULL,version TEXT NOT NULL,epoch INTEGER NOT NULL,path TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,format INTEGER NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(account,principal,version,epoch,path,kind,id))',
    ]) host.sql.exec(sql);
    host.transactionSync(() => { host.sql.exec('INSERT OR IGNORE INTO forge_upload_usage(singleton,account,records,active,assembly_bytes,frontier_rows) VALUES(1,?,0,0,0,0)', this.account); this.usage(); });
  }
  private one<T>(query: string, ...bindings: (string | number | null | ArrayBuffer)[]): T | undefined { return [...this.host.sql.exec(query, ...bindings)][0] as T | undefined; }
  private now(): number { const now = this.host.nowSeconds(); if (!safe(now, Number.MAX_SAFE_INTEGER - TTL)) fail(503, 'unavailable'); return now; }
  private usage(): Usage { const u = this.one<Usage>('SELECT * FROM forge_upload_usage WHERE singleton=1'); if (!u || u.account !== this.account) fail(403, 'permission_denied'); return u; }
  private status(s: Session): unknown { return { session_id: array(s.id), offset: s.position, state: s.state === 'committed' ? 'committed' : 'pending', receipt: s.receipt === null ? null : JSON.parse(s.receipt) }; }
  private load(id: string, viewName: string): { session: Session; view: ResolvedView } {
    this.host.requireWriterInTx();
    const s = this.one<Session>('SELECT * FROM forge_upload_sessions WHERE id=?', id);
    if (!s || s.account !== this.account || s.principal !== this.principal) fail(403, 'permission_denied');
    // Resolve the caller's alias only at the saved immutable version. An empty
    // default selector must not silently pick a newer view during continuation.
    const view = this.host.views.resolveInTx(viewName, array(s.version)); this.host.views.checkInTx(view);
    if (view.description.view !== s.view_name || hex(view.description.view_version) !== s.version) fail(403, 'permission_denied');
    if (view.description.policy_epoch !== s.epoch || hex(view.description.revision) !== s.revision) fail(409, 'aborted');
    if (s.state === 'obsolete') fail(409, 'aborted');
    if (this.now() >= s.expires_at || s.state === 'expired' || s.state === 'failed') fail(400, 'failed_precondition');
    this.path(view, s.path, s.kind);
    if (s.state === 'committed') { const d = this.host.describeObjectInTx(descriptor(s)); if (!d || !same(d, descriptor(s))) fail(503, 'unavailable'); }
    return { session: s, view };
  }
  private path(view: ResolvedView, path: string, kind: number): void {
    if (path === '' && (kind === 2 || kind === 3)) return;
    const decision = this.host.views.pathsAllowedInTx(view, [path]);
    if (decision.paths.length !== 1 || !decision.paths[0].allowed) fail(403, 'permission_denied');
  }
  private save(s: Session): void {
    s.generation++;
    this.host.sql.exec('UPDATE forge_upload_sessions SET position=?,state=?,generation=?,scope_done=?,scope_nodes=?,scope_entries=?,scope_path_bytes=?,authority_until=?,integrity_job=?,receipt=? WHERE id=?', s.position, s.state, s.generation, s.scope_done, s.scope_nodes, s.scope_entries, s.scope_path_bytes, s.authority_until, s.integrity_job, s.receipt, s.id);
  }
  private current(s: Session): { session: Session; view: ResolvedView } { const value = this.load(s.id, s.view_name); if (value.session.generation !== s.generation || value.session.binding !== s.binding) fail(409, 'aborted'); return value; }
  private reap(epoch: number): void {
    const rows = [...this.host.sql.exec("SELECT * FROM forge_upload_sessions WHERE state IN('receiving','verifying') AND (expires_at<=? OR epoch<>?) LIMIT 8", this.now(), epoch)] as unknown as Session[];
    // Only private assembly is reclaimed. Canonical storage charges, receipts
    // and proof jobs are preserved; stale epochs cannot regain continuation.
    for (const s of rows) { this.host.sql.exec('UPDATE forge_upload_sessions SET state=?,generation=generation+1 WHERE id=?', s.epoch !== epoch ? 'obsolete' : 'expired', s.id); this.host.sql.exec('DELETE FROM forge_upload_buffers WHERE session=?', s.id); this.host.sql.exec('UPDATE forge_upload_usage SET active=active-1,assembly_bytes=assembly_bytes-? WHERE singleton=1', s.size); }
  }
  private begin(call: ForgeCall, id: string, input: Extract<UploadRequest, { action: 'begin' }>): unknown {
    return this.host.transactionSync(() => {
      this.host.requireWriterInTx(); const view = this.host.views.resolveInTx(call.view, input.version); this.host.views.checkInTx(view);
      this.path(view, input.path, input.kind); const d: Descriptor = { kind: input.kind, id: hex(input.id), size_bytes: input.size_bytes, format_version: 1 };
      if (!safe(d.size_bytes, d.kind === 1 ? MAX_FILE : MAX_META)) fail(429, 'resource_exhausted');
      const epoch = view.description.policy_epoch, version = hex(view.description.view_version), revision = hex(view.description.revision);
      const binding = encode([this.account, this.principal, view.description.view, version, epoch, revision, input.path, d]);
      const old = this.one<Session>('SELECT * FROM forge_upload_sessions WHERE id=?', id);
      if (old) { if (old.binding !== binding) fail(409, 'already_exists'); return this.status(this.load(id, call.view).session); }
      this.reap(epoch); const usage = this.usage();
      if (usage.records >= 4096 || usage.active >= 8 || usage.assembly_bytes + d.size_bytes > 8 * MAX_FILE) fail(429, 'resource_exhausted');
      this.host.reserveUploadInTx(id, d); const expiry = this.now() + TTL;
      this.host.sql.exec("INSERT INTO forge_upload_sessions(id,account,principal,view_name,version,revision,epoch,path,kind,object_id,size,binding,position,expires_at,state,generation,scope_done,scope_nodes,scope_entries,scope_path_bytes,authority_until) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,?,'receiving',0,0,0,0,0,?)", id, this.account, this.principal, view.description.view, version, revision, epoch, input.path, d.kind, d.id, d.size_bytes, binding, expiry, expiry);
      this.host.sql.exec('INSERT INTO forge_upload_buffers(session,data) VALUES(?,?)', id, new ArrayBuffer(0));
      this.host.sql.exec('UPDATE forge_upload_usage SET records=records+1,active=active+1,assembly_bytes=assembly_bytes+? WHERE singleton=1', d.size_bytes);
      return this.status(this.load(id, view.description.view).session);
    });
  }
  private buffer(s: Session): Uint8Array {
    const row = this.one<{ data: ArrayBuffer }>('SELECT data FROM forge_upload_buffers WHERE session=?', s.id);
    if (!row || !(row.data instanceof ArrayBuffer)) fail(503, 'unavailable'); const bytes = new Uint8Array(row.data);
    if (bytes.length !== s.position || bytes.length > s.size) fail(503, 'unavailable'); return bytes;
  }
  private async append(viewName: string, id: string, offset: number, values: number[]): Promise<unknown> {
    if (values.length === 0 || values.length > 65536) fail(429, 'resource_exhausted'); const data = Uint8Array.from(values);
    const initial = this.host.transactionSync(() => {
      const { session: s } = this.load(id, viewName);
      if (!safe(offset, s.size) || data.length > s.size - offset || offset > s.position) fail(400, 'invalid_argument');
      if (s.state === 'committed') return { session: s };
      const bytes = this.buffer(s);
      if (offset < s.position || s.state === 'verifying') {
        if (offset + data.length > s.position || !data.every((b, i) => bytes[offset + i] === b)) fail(409, 'already_exists'); return { status: this.status(s) };
      }
      const joined = new Uint8Array(bytes.length + data.length); joined.set(bytes); joined.set(data, bytes.length);
      this.host.sql.exec('UPDATE forge_upload_buffers SET data=? WHERE session=?', joined.buffer, id); s.position += data.length; this.save(s); return { status: this.status(s) };
    });
    if ('status' in initial) return initial.status;
    const s = initial.session!; let loaded: { descriptor: Descriptor; bytes: Uint8Array };
    try { loaded = await this.host.readVerifiedObject(descriptor(s)); } catch (error) { this.host.transactionSync(() => this.current(s)); throw error; }
    return this.host.transactionSync(() => { this.current(s); if (!same(loaded.descriptor, descriptor(s)) || loaded.bytes.length !== s.size) fail(503, 'unavailable'); if (!data.every((b, i) => loaded.bytes[offset + i] === b)) fail(409, 'already_exists'); return this.status(s); });
  }
  private enqueue(s: Session, node: { key: ObjectKey } & ScopeContext): void {
    if (![2, 3, 4, 5].includes(node.key.kind) || !/^[0-9a-f]{64}$/.test(node.key.id) || !safe(node.depth, 64)
        || !['root', 'tree', 'copy_at', 'copy_parent'].includes(node.role) || encoder.encode(node.canonical_path).byteLength > 4096) fail(503, 'unavailable');
    if (this.one('SELECT 1 FROM forge_upload_scope WHERE session=? AND kind=? AND id=? AND path=? AND depth=? AND role=?', s.id, node.key.kind, node.key.id, node.canonical_path, node.depth, node.role)) return;
    const length = encoder.encode(node.canonical_path).byteLength;
    if (s.scope_nodes >= MAX_NODES || s.scope_path_bytes + length > MAX_PATH_BYTES || this.usage().frontier_rows >= MAX_FRONTIER) fail(429, 'resource_exhausted');
    this.host.sql.exec('INSERT INTO forge_upload_scope(session,kind,id,path,depth,role) VALUES(?,?,?,?,?,?)', s.id, node.key.kind, node.key.id, node.canonical_path, node.depth, node.role);
    s.scope_nodes++; s.scope_path_bytes += length;
    this.host.sql.exec('UPDATE forge_upload_usage SET frontier_rows=frontier_rows+1 WHERE singleton=1');
  }
  private next(s: Session): Node | undefined { return this.one<Node>('SELECT kind,id,path,depth,role,position,done FROM forge_upload_scope WHERE session=? AND done=0 ORDER BY kind,id,path,depth,role LIMIT 1', s.id); }
  private reference(s: Session, view: ResolvedView, path: string, key: ObjectKey, expectedSize: number | null): void {
    const d = this.host.describeObjectInTx(key); if (!d) fail(403, 'permission_denied');
    if (expectedSize !== null && d.size_bytes !== expectedSize) fail(400, 'invalid_argument');
    if (this.host.projectionMemberInTx(view, d)) return;
    const receipt = this.one<OwnReceipt>('SELECT size,format,expires_at FROM forge_owned_uploads WHERE account=? AND principal=? AND version=? AND epoch=? AND path=? AND kind=? AND id=?', this.account, this.principal, s.version, s.epoch, path, key.kind, key.id);
    if (!receipt || receipt.size !== d.size_bytes || receipt.format !== d.format_version || receipt.expires_at <= this.now()) fail(403, 'permission_denied');
    s.authority_until = Math.min(s.authority_until, receipt.expires_at);
  }
  private async stage(s: Session): Promise<Session> {
    const bytes = this.host.transactionSync(() => { this.current(s); if (s.position !== s.size) fail(400, 'failed_precondition'); return this.buffer(s); });
    let actual: Descriptor;
    try { actual = inspect(s.kind, bytes); } catch { this.failBytes(s); fail(400, 'invalid_argument'); }
    if (!same(actual, descriptor(s))) { this.failBytes(s); fail(400, 'invalid_argument'); }
    let confirmation: unknown;
    try { confirmation = await this.host.stageUpload(descriptor(s), bytes); }
    catch (error) { this.host.transactionSync(() => this.current(s)); throw error; }
    return this.host.transactionSync(() => {
      const { session: current } = this.current(s); this.host.confirmUploadInTx(s.id, descriptor(s), confirmation);
      current.state = 'verifying';
      if (s.kind === 1) current.scope_done = 1;
      else this.enqueue(current, { key: descriptor(s), canonical_path: s.path, depth: 0, role: 'root' });
      this.save(current); return current;
    });
  }
  private failBytes(s: Session): void {
    this.host.transactionSync(() => {
      const { session: current } = this.current(s); current.state = 'failed'; this.save(current);
      this.host.sql.exec('DELETE FROM forge_upload_buffers WHERE session=?', s.id);
      this.host.sql.exec('UPDATE forge_upload_usage SET active=active-1,assembly_bytes=assembly_bytes-? WHERE singleton=1', s.size);
    });
  }
  private async scope(s: Session): Promise<Session> {
    let current = s; let nodes = 0, bytes = 0, entries = 0; const deadline = Date.now() + 5000;
    while (!current.scope_done && nodes < 64 && entries < 2048 && Date.now() < deadline) {
      const next = this.host.transactionSync(() => { this.current(current); return this.next(current); });
      if (!next) return this.host.transactionSync(() => { const { session: value } = this.current(current); value.scope_done = 1; this.save(value); return value; });
      const d = this.host.transactionSync(() => { this.current(current); const d = this.host.describeObjectInTx(next); if (!d) fail(403, 'permission_denied'); return d; });
      if (!safe(d.size_bytes, MAX_META) || d.kind === 1) fail(429, 'resource_exhausted');
      if (bytes + d.size_bytes > MAX_META) break;
      let loaded: { descriptor: Descriptor; bytes: Uint8Array };
      try { loaded = await this.host.readVerifiedObject(d); } catch (error) { this.host.transactionSync(() => this.current(current)); throw error; }
      this.host.transactionSync(() => this.current(current));
      if (!same(d, loaded.descriptor) || loaded.bytes.byteLength !== d.size_bytes) fail(503, 'unavailable');
      let page: UploadScopePage;
      try { page = uploadScopePage({ descriptor: d, bytes_base64: base64(loaded.bytes), context: { canonical_path: next.path, depth: next.depth, role: next.role }, cursor: next.position, limit: Math.min(128, 2048 - entries) }); }
      catch (error) {
        this.host.transactionSync(() => this.current(current));
        if (error instanceof Error && error.message.startsWith('permission_denied')) fail(403, 'permission_denied');
        if (error instanceof Error && error.message.startsWith('resource_exhausted')) fail(429, 'resource_exhausted');
        fail(400, 'invalid_argument');
      }
      current = this.host.transactionSync(() => {
        const { session: value, view } = this.current(current);
        const stored = this.one<Node>('SELECT kind,id,path,depth,role,position,done FROM forge_upload_scope WHERE session=? AND kind=? AND id=? AND path=? AND depth=? AND role=?', s.id, next.kind, next.id, next.path, next.depth, next.role);
        if (!stored || stored.done || stored.position !== next.position) fail(409, 'aborted');
        const available = this.host.describeObjectInTx(next); if (!available || !same(available, d)) fail(503, 'unavailable');
        if (!safe(page.total, MAX_ENTRIES) || !safe(page.entries, 128) || page.children.length > 128 || page.requirements.length > 256
            || (page.next_cursor !== null && (!safe(page.next_cursor, page.total) || page.next_cursor <= next.position))) fail(503, 'unavailable');
        if (value.scope_entries + page.entries > MAX_ENTRIES) fail(429, 'resource_exhausted');
        // Every requirement is resolved against current authority atomically
        // before this metadata cursor can advance. No client-supplied witnesses.
        for (const requirement of page.requirements) {
          if (requirement.kind === 'path') this.path(view, requirement.canonical_path, 1);
          else this.reference(value, view, requirement.canonical_path, requirement.key, requirement.expected_size);
        }
        for (const child of page.children) this.enqueue(value, child);
        value.scope_entries += page.entries;
        this.host.sql.exec('UPDATE forge_upload_scope SET position=?,done=? WHERE session=? AND kind=? AND id=? AND path=? AND depth=? AND role=?', page.next_cursor ?? page.total, page.next_cursor === null ? 1 : 0, s.id, next.kind, next.id, next.path, next.depth, next.role);
        this.save(value); return value;
      });
      nodes++; bytes += d.size_bytes; entries += page.entries;
    }
    return current;
  }
  private integrity(s: Session): ClosureJobs {
    const binding: ClosureBinding = { account_id: this.account, identity: { kind: 'principal', principal: this.principal }, policy_epoch: s.epoch,
      purpose: 'scoped_upload_integrity', publication: `upload/${s.id}` };
    if (s.kind === 2) binding.tree_prefix = s.path;
    return new ClosureJobs({ binding, sql: this.host.sql, transactionSync: callback => this.host.transactionSync(callback),
      authorizeCurrentInTx: () => {
        const { session: current } = this.load(s.id, s.view_name);
        if (current.binding !== s.binding || !current.scope_done || current.authority_until <= this.now()) fail(403, 'permission_denied');
      }, describeObjectInTx: key => this.host.describeObjectInTx(key), readVerifiedObject: key => this.host.readVerifiedObject(key),
      attachRootInTx: (actual, root, jobId) => {
        const { session: current } = this.load(s.id, s.view_name);
        if (current.integrity_job !== jobId || !current.scope_done || current.authority_until <= this.now() || actual.publication !== binding.publication || root.kind !== s.kind || root.id !== s.object_id) fail(403, 'permission_denied');
        const retained = this.host.retainUploadInTx(s.id, descriptor(s), s.expires_at);
        if (retained.expires_at !== s.expires_at || typeof retained.staging_lease !== 'string' || retained.staging_lease.length < 1 || encoder.encode(retained.staging_lease).byteLength > 1024) fail(503, 'unavailable');
        const receipt = { kind: s.kind, id: array(s.object_id), size_bytes: s.size, format_version: 1, staging_lease: retained.staging_lease, expires_at: retained.expires_at };
        this.host.sql.exec('INSERT INTO forge_owned_uploads(account,principal,version,epoch,path,kind,id,size,format,expires_at) VALUES(?,?,?,?,?,?,?,?,1,?) ON CONFLICT(account,principal,version,epoch,path,kind,id) DO UPDATE SET expires_at=MAX(forge_owned_uploads.expires_at,excluded.expires_at)', this.account, this.principal, s.version, s.epoch, s.path, s.kind, s.object_id, s.size, s.expires_at);
        current.state = 'committed'; current.receipt = encode(receipt); this.save(current);
        this.host.sql.exec('DELETE FROM forge_upload_buffers WHERE session=?', s.id);
        this.host.sql.exec('UPDATE forge_upload_usage SET active=active-1,assembly_bytes=assembly_bytes-? WHERE singleton=1', s.size);
        return this.status(current);
      } });
  }
  private async finalize(viewName: string, id: string): Promise<unknown> {
    let s = this.host.transactionSync(() => this.load(id, viewName).session);
    if (s.state === 'committed') return this.status(s);
    if (s.position !== s.size) fail(400, 'failed_precondition');
    if (s.state === 'receiving') { s = await this.stage(s); return this.host.transactionSync(() => this.status(this.current(s).session)); }
    if (!s.scope_done) { s = await this.scope(s); return this.host.transactionSync(() => this.status(this.current(s).session)); }
    const jobs = this.integrity(s);
    if (s.integrity_job === null) {
      const started = await jobs.begin(`u-${s.id}`, descriptor(s));
      s = this.host.transactionSync(() => { const { session: current } = this.current(s); current.integrity_job = started.job_id; this.save(current); return current; });
      return this.host.transactionSync(() => this.status(this.current(s).session));
    }
    let progress = jobs.status(s.integrity_job);
    if (progress.phase !== 'ready' && progress.phase !== 'published' && progress.phase !== 'limit') {
      try { progress = await jobs.advance(s.integrity_job, progress.generation); }
      catch (error) { this.host.transactionSync(() => this.current(s)); throw error; }
      this.host.transactionSync(() => this.current(s));
    }
    if (progress.phase === 'limit') fail(429, 'resource_exhausted');
    if (progress.error) fail(503, 'unavailable');
    if (progress.phase === 'ready') jobs.publish(s.integrity_job, progress.generation);
    return this.host.transactionSync(() => this.status(this.load(id, viewName).session));
  }
  private response(result: unknown): { state: 'pending' | 'committed'; result: unknown } {
    const state = result !== null && typeof result === 'object' ? (result as { state?: unknown }).state : undefined;
    if (state !== 'pending' && state !== 'committed') fail(503, 'unavailable'); return { state, result };
  }
  async handle(call: ForgeCall): Promise<{ state: 'pending' | 'committed'; result: unknown }> {
    if (call.account_id !== this.account) fail(403, 'permission_denied');
    this.host.transactionSync(() => this.host.requireWriterInTx());
    let command: UploadCommand;
    try { command = normalizeForgeUploadCommand({ account_id: call.account_id, operation: call.operation, view: call.view, request_id: call.request_id, payload_json: call.payload_json, principal: this.principal }); }
    catch (error) { if (error instanceof Error && error.message.startsWith('resource_exhausted')) fail(429, 'resource_exhausted'); fail(400, 'invalid_argument'); }
    if (command.operation !== 10) fail(400, 'invalid_argument'); const request = command.request;
    if (request.action === 'begin') {
      if (command.session_id === null || !/^[0-9a-f]{64}$/.test(command.session_id)) fail(503, 'unavailable');
      return this.response(this.begin(call, command.session_id, request));
    }
    const id = hex(request.session_id);
    if (request.action === 'status') return this.response(this.host.transactionSync(() => this.status(this.load(id, call.view).session)));
    const result = request.action === 'append' ? await this.append(call.view, id, request.offset, request.data) : await this.finalize(call.view, id);
    this.host.transactionSync(() => this.load(id, call.view));
    return this.response(result);
  }
}
