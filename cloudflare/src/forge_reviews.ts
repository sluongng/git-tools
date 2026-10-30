/** Immutable review preparation. Private drafts are not admitted patchsets;
 * candidate construction is not a check verdict. This module never writes main. */
import { inspect, buildForgeCandidate, normalizeForgeReviewCommand, deriveForgeReviewIdentity, formatForgeReview, reviewAdmission, type Descriptor, type ObjectKey, type ForgeCall } from './core';
import { ClosureJobs, type ClosureBinding } from './closure_jobs';
import type { ForgeViews, ResolvedView } from './forge_views';
import type { ForgeProjections, ReviewProjectionSource } from './forge_projection';
import { fail, hash, Failure } from './protocol';

export interface ForgeReviewsHost {
  accountId: string; principal: string; sql: SqlStorage; views: ForgeViews; projections: ForgeProjections;
  transactionSync<T>(callback: () => T): T;
  nowSeconds(): number;
  authorizeInTx(action: 'read' | 'write' | 'review', expectedEpoch?: number, principal?: string): { epoch: number; is_admin: boolean };
  currentMainInTx(): { head: string; generation: number };
  reviewConfiguration(): { profile: Profile; committer: { name: string; email: string } } | null;
  describeObjectInTx(key: ObjectKey): Descriptor | undefined;
  readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
  requireOwnedCommitInTx(view: ResolvedView, descriptor: Descriptor, now: number): number;
  requireScopedReferenceInTx(view: ResolvedView, path: string, descriptor: Descriptor, now: number): number;
  requireRetainedClosureInTx(root: ObjectKey): { proof_job: string };
  retainReviewInputInTx(ownerId: string, root: ObjectKey, proofJob: string): void;
  reserveGeneratedInTx(attemptId: string, descriptor: Descriptor): void;
  stageGenerated(descriptor: Descriptor, bytes: Uint8Array): Promise<unknown>;
  confirmGeneratedInTx(attemptId: string, descriptor: Descriptor, confirmation: unknown): void;
  retainCandidateInTx(attemptId: string, root: ObjectKey, proofJob: string): void;
}
interface Profile { name: string; version: string }
interface Person { name: string; email: string; millis_since_epoch: number; tz_offset_minutes: number }
interface Dependency { change_id: number[]; patchset_id: number[] }
interface Lineage { operation: 'copy' | 'rename'; source_path: string; target_path: string }
interface PushIntent {
  source_base: number[]; source_head: number[]; view_version: number[];
  expected_patchset: number[] | null; predecessor: Dependency | null; provenance: number[] | null; lineage: Lineage[];
}
export type ReviewCommand = { operation: 'push'; intent: PushIntent }
  | { operation: 'get'; intent: { change_id: number[]; patchset_id: number[] | null } }
  | { operation: 'operation' }
  | { operation: 'decide'; intent: { change_id: number[]; patchset_id: number[]; intent:
      { kind: 'comment'; body: string; path: string | null } | { kind: 'vote'; decision: 'approve' | 'request_changes'; candidate_id: number[] } } }
  | { operation: 'land'; intent: { change_id: number[]; patchset_id: number[]; candidate_id: number[] } };
interface Operation {
  id: string; account: string; principal: string; request_id: string; fingerprint: string; kind: string;
  phase: 'admission' | 'admitted' | 'done'; generation: number; epoch: number; view_name: string; version: string; revision: string;
  payload: string; intent: string; profile: string; committer: string; source_mode: 'canonical' | 'projected' | 'predecessor'; mapped_base: string | null;
  facts: string | null; change_id: string | null; patchset_id: string | null; expires_at: number; authority_until: number;
  metadata_bytes: number; scope_entries: number; scope_paths: number; scope_position: number; source_jobs: string;
}
interface Patchset {
  id: string; account: string; change_id: string; ordinal: number; operation: string; principal: string;
  epoch: number; view_name: string; version: string; revision: string; source_base: string; source_head: string;
  source_mode: Operation['source_mode']; mapped_base: string | null; intent: string; profile: string; committer: string;
}
interface Change { id: string; account: string; owner: string; current_patchset: string; version: number }
interface Attempt {
  id: string; account: string; patchset: string; ordinal: number; generation: number; principal: string;
  target: string; target_generation: number; build_target: string; mapped_base: string | null;
  phase: 'collect' | 'build' | 'stage' | 'closure' | 'checks_pending' | 'conflict' | 'unsupported' | 'stale' | 'expired' | 'limit';
  expires_at: number; metadata_bytes: number; plan_hash: string | null; plan_bytes: number; output_cursor: number;
  integrity_job: string | null; candidate: string | null; conflict_path: string | null; publication_generation: number | null;
}
interface Metadata extends Descriptor { bytes_base64: string }
interface Plan { state: 'candidate'; context: unknown; head: string; root_tree: string; rewrites: { old: string; new: string; change_id: string }[]; objects: Metadata[] }
interface Pack { kind: number; id: string; size: number | null; data: ArrayBuffer | null }
const ROOT = '0'.repeat(64), TTL = 86400, MAX_META = 256 * 1024, MAX_PACK = 4 * 1024 * 1024;
const MAX_ROWS = 262144, MAX_PRIVATE_BYTES = 32 * 1024 * 1024, CHUNK = 64 * 1024;
const encoder = new TextEncoder(), decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });
function safe(n: number, max = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(n) && n >= 0 && n <= max; }
function id(value: string, length = 64): boolean { return typeof value === 'string' && value.length === length && /^[0-9a-f]+$/.test(value); }
function hex(value: number[], length = 32): string {
  if (!Array.isArray(value) || value.length !== length || !value.every(b => safe(b, 255))) fail(400, 'invalid_argument');
  return value.map(b => b.toString(16).padStart(2, '0')).join('');
}
function array(value: string): number[] { if (!id(value) && !id(value, 32)) fail(503, 'unavailable'); return Array.from(value.match(/../g)!, b => parseInt(b, 16)); }
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function encoded(value: unknown, limit = 64 * 1024): string { const text = JSON.stringify(value); if (encoder.encode(text).length > limit) fail(429, 'resource_exhausted'); return text; }
function base64(bytes: Uint8Array): string { let result = ''; for (let i = 0; i < bytes.length; i += 8192) result += String.fromCharCode(...bytes.subarray(i, i + 8192)); return btoa(result); }
function decode(value: Metadata): Uint8Array {
  if (!safe(value.size_bytes, MAX_META) || value.bytes_base64.length !== Math.ceil(value.size_bytes / 3) * 4) fail(503, 'unavailable');
  let text: string; try { text = atob(value.bytes_base64); } catch { fail(503, 'unavailable'); }
  const bytes = Uint8Array.from(text, c => c.charCodeAt(0)); if (bytes.length !== value.size_bytes || base64(bytes) !== value.bytes_base64) fail(503, 'unavailable'); return bytes;
}

export class ForgeReviews {
  constructor(private readonly host: ForgeReviewsHost) {
    for (const sql of [
      'CREATE TABLE IF NOT EXISTS review_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,rows INTEGER NOT NULL,bytes INTEGER NOT NULL)',
      'CREATE TABLE IF NOT EXISTS review_operations(id TEXT PRIMARY KEY,account TEXT NOT NULL,principal TEXT NOT NULL,request_id TEXT NOT NULL,fingerprint TEXT NOT NULL,kind TEXT NOT NULL,phase TEXT NOT NULL,generation INTEGER NOT NULL,epoch INTEGER NOT NULL,view_name TEXT NOT NULL,version TEXT NOT NULL,revision TEXT NOT NULL,payload TEXT NOT NULL,intent TEXT NOT NULL,profile TEXT NOT NULL,committer TEXT NOT NULL,source_mode TEXT NOT NULL,mapped_base TEXT,facts TEXT,change_id TEXT,patchset_id TEXT,expires_at INTEGER NOT NULL,authority_until INTEGER NOT NULL,metadata_bytes INTEGER NOT NULL,scope_entries INTEGER NOT NULL,scope_paths INTEGER NOT NULL,scope_position INTEGER NOT NULL DEFAULT 0,source_jobs TEXT NOT NULL DEFAULT \'[]\',UNIQUE(account,principal,request_id))',
      'CREATE TABLE IF NOT EXISTS review_changes(id TEXT PRIMARY KEY,account TEXT NOT NULL,owner TEXT NOT NULL,current_patchset TEXT NOT NULL,version INTEGER NOT NULL)',
      'CREATE TABLE IF NOT EXISTS review_patchsets(id TEXT PRIMARY KEY,account TEXT NOT NULL,change_id TEXT NOT NULL,ordinal INTEGER NOT NULL,operation TEXT NOT NULL,principal TEXT NOT NULL,epoch INTEGER NOT NULL,view_name TEXT NOT NULL,version TEXT NOT NULL,revision TEXT NOT NULL,source_base TEXT NOT NULL,source_head TEXT NOT NULL,source_mode TEXT NOT NULL,mapped_base TEXT,intent TEXT NOT NULL,profile TEXT NOT NULL,committer TEXT NOT NULL,UNIQUE(change_id,ordinal))',
      'CREATE TABLE IF NOT EXISTS review_attempts(id TEXT PRIMARY KEY,account TEXT NOT NULL,patchset TEXT NOT NULL,ordinal INTEGER NOT NULL,generation INTEGER NOT NULL,principal TEXT NOT NULL,target TEXT NOT NULL,target_generation INTEGER NOT NULL,build_target TEXT NOT NULL,mapped_base TEXT,phase TEXT NOT NULL,expires_at INTEGER NOT NULL,metadata_bytes INTEGER NOT NULL,plan_hash TEXT,plan_bytes INTEGER NOT NULL,output_cursor INTEGER NOT NULL,integrity_job TEXT,candidate TEXT,conflict_path TEXT,publication_generation INTEGER,UNIQUE(patchset,ordinal))',
      'CREATE TABLE IF NOT EXISTS review_pack(owner TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER,data BLOB,PRIMARY KEY(owner,kind,id))',
      'CREATE TABLE IF NOT EXISTS review_requirements(owner TEXT NOT NULL,ordinal INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(owner,ordinal))',
      'CREATE TABLE IF NOT EXISTS review_plan(owner TEXT NOT NULL,ordinal INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(owner,ordinal))',
      'CREATE TABLE IF NOT EXISTS review_decisions(account TEXT NOT NULL,change_id TEXT NOT NULL,patchset TEXT NOT NULL,candidate TEXT NOT NULL,principal TEXT NOT NULL,decision TEXT NOT NULL,epoch INTEGER NOT NULL,ordinal INTEGER NOT NULL,PRIMARY KEY(change_id,ordinal))',
      'CREATE TABLE IF NOT EXISTS review_comments(account TEXT NOT NULL,change_id TEXT NOT NULL,patchset TEXT NOT NULL,author TEXT NOT NULL,body TEXT NOT NULL,path TEXT,created_at INTEGER NOT NULL,ordinal INTEGER NOT NULL,PRIMARY KEY(change_id,ordinal))',
    ]) host.sql.exec(sql);
    host.sql.exec('INSERT OR IGNORE INTO review_usage(singleton,account,rows,bytes) VALUES(1,?,0,0)', host.accountId);
  }
  private one<T>(sql: string, ...bindings: (string | number | null | ArrayBuffer)[]): T | undefined { return [...this.host.sql.exec(sql, ...bindings)][0] as T | undefined; }
  private rows<T>(sql: string, ...bindings: (string | number | null | ArrayBuffer)[]): T[] { return [...this.host.sql.exec(sql, ...bindings)] as T[]; }
  private now(): number { const now = this.host.nowSeconds(); if (!safe(now, Math.floor(Number.MAX_SAFE_INTEGER / 1000) - TTL)) fail(503, 'unavailable'); return now; }
  private charge(rows: number, bytes: number): void {
    const usage = this.one<{ account: string; rows: number; bytes: number }>('SELECT * FROM review_usage WHERE singleton=1');
    if (!usage || usage.account !== this.host.accountId || !safe(usage.rows) || !safe(usage.bytes)) fail(503, 'unavailable');
    if (!safe(rows) || !safe(bytes) || usage.rows + rows > MAX_ROWS || usage.bytes + bytes > MAX_PRIVATE_BYTES) fail(429, 'resource_exhausted');
    this.host.sql.exec('UPDATE review_usage SET rows=rows+?,bytes=bytes+? WHERE singleton=1', rows, bytes);
  }
  private operation(principal: string, request: string): Operation | undefined {
    const identity = deriveForgeReviewIdentity({ kind: 'operation', principal, request_id: request }) as { operation_id: string };
    const row = this.one<Operation>('SELECT * FROM review_operations WHERE id=?', identity.operation_id);
    if (row && (row.account !== this.host.accountId || row.principal !== principal || row.request_id !== request)) fail(503, 'unavailable'); return row;
  }
  private view(operation: Operation, action: 'read' | 'write' = 'write'): ResolvedView {
    this.host.authorizeInTx(action, action === 'write' ? operation.epoch : undefined);
    const view = this.host.views.resolveInTx(operation.view_name, array(operation.version)); this.host.views.checkInTx(view);
    if (hex(view.description.revision) !== operation.revision || (action === 'write' && view.description.policy_epoch !== operation.epoch)) fail(409, 'aborted');
    return view;
  }
  private current(operation: Operation): { operation: Operation; view: ResolvedView } {
    const row = this.operation(operation.principal, operation.request_id);
    if (!row || row.id !== operation.id || row.generation !== operation.generation || row.fingerprint !== operation.fingerprint) fail(409, 'aborted');
    if (row.principal !== this.host.principal || (row.phase === 'admission' && this.now() >= row.expires_at)) fail(400, 'failed_precondition');
    const view = this.view(row); if (row.phase === 'admission') this.ownedHead(row, view); return { operation: row, view };
  }
  private ownedHead(operation: Operation, view: ResolvedView): void {
    const intent = JSON.parse(operation.intent) as PushIntent;
    const d = this.host.describeObjectInTx({ kind: 3, id: hex(intent.source_head) }); if (!d) fail(403, 'permission_denied');
    const expiry = this.host.requireOwnedCommitInTx(view, d, this.now()); if (!safe(expiry) || expiry <= this.now()) fail(403, 'permission_denied');
  }
  private save(operation: Operation): void {
    operation.generation++;
    this.host.sql.exec('UPDATE review_operations SET phase=?,generation=?,facts=?,change_id=?,patchset_id=?,authority_until=?,metadata_bytes=?,scope_entries=?,scope_paths=?,scope_position=?,source_jobs=? WHERE id=?', operation.phase, operation.generation, operation.facts, operation.change_id, operation.patchset_id, operation.authority_until, operation.metadata_bytes, operation.scope_entries, operation.scope_paths, operation.scope_position, operation.source_jobs, operation.id);
  }
  private enqueue(owner: string, key: ObjectKey): void {
    if (key.kind === 3 && key.id === ROOT) return;
    if (key.kind === 1) return;
    if (![2, 3, 4, 5].includes(key.kind) || !id(key.id)) fail(503, 'unavailable');
    if (this.one('SELECT 1 FROM review_pack WHERE owner=? AND kind=? AND id=?', owner, key.kind, key.id)) return;
    if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM review_pack WHERE owner=?', owner)!.n >= 8192) fail(429, 'resource_exhausted');
    this.charge(1, 128); this.host.sql.exec('INSERT INTO review_pack(owner,kind,id,size,data) VALUES(?,?,?,NULL,NULL)', owner, key.kind, key.id);
  }
  private begin(call: ForgeCall, intent: PushIntent): Operation {
    return this.host.transactionSync(() => {
      const config = this.host.reviewConfiguration(); if (!config) fail(400, 'failed_precondition');
      const access = this.host.authorizeInTx('write');
      const fp = deriveForgeReviewIdentity({ kind: 'push_fingerprint', request_id: call.request_id, profile: config.profile, payload_json: call.payload_json }) as { fingerprint: string };
      const old = this.operation(this.host.principal, call.request_id);
      if (old) {
        if (old.kind !== 'push' || old.fingerprint !== fp.fingerprint) fail(409, 'already_exists');
        this.host.views.resolveInTx(call.view, array(old.version));
        if (old.phase === 'admission') this.current(old); else this.readable(this.patchset(old.change_id!, old.patchset_id!)); return old;
      }
      if (intent.provenance !== null) fail(501, 'unimplemented');
      const view = this.host.views.resolveInTx(call.view, intent.view_version); this.host.views.checkInTx(view);
      const d = view.description, base = hex(intent.source_base), head = hex(intent.source_head), version = hex(intent.view_version);
      if (d.policy_epoch !== access.epoch || version !== hex(d.view_version)) fail(409, 'aborted');
      const descriptor = this.host.describeObjectInTx({ kind: 3, id: head }); if (!descriptor) fail(403, 'permission_denied');
      const expiry = this.host.requireOwnedCommitInTx(view, descriptor, this.now());
      let mode: Operation['source_mode'] = 'canonical', mapped: string | null = null;
      if (intent.predecessor) {
        const p = this.patchset(hex(intent.predecessor.change_id, 16), hex(intent.predecessor.patchset_id));
        this.readable(p);
        if (this.change(p.change_id).current_patchset !== p.id || p.source_head !== base) fail(400, 'failed_precondition');
        mode = 'predecessor';
      } else if (base !== hex(d.revision)) {
        const relation = this.host.projections.canonicalForProjectedBaseInTx(view, base);
        if (!relation) fail(403, 'permission_denied'); mode = 'projected'; mapped = relation.canonical_revision;
      }
      if (!safe(expiry) || expiry <= this.now()) fail(403, 'permission_denied');
      if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM review_operations')!.n >= 4096
          || this.one<{ n: number }>("SELECT COUNT(*) AS n FROM review_operations WHERE phase='admission' AND expires_at>? AND epoch=?", this.now(), access.epoch)!.n >= 8) fail(429, 'resource_exhausted');
      const operationId = (deriveForgeReviewIdentity({ kind: 'operation', principal: this.host.principal, request_id: call.request_id }) as { operation_id: string }).operation_id;
      const committer = { name: config.committer.name, email: config.committer.email, millis_since_epoch: this.now() * 1000, tz_offset_minutes: 0 };
      const original = encoded(intent), profile = encoded(config.profile), person = encoded(committer);
      this.charge(1, encoder.encode(call.payload_json + original + profile + person).length + 1024);
      this.host.sql.exec("INSERT INTO review_operations(id,account,principal,request_id,fingerprint,kind,phase,generation,epoch,view_name,version,revision,payload,intent,profile,committer,source_mode,mapped_base,expires_at,authority_until,metadata_bytes,scope_entries,scope_paths) VALUES(?,?,?,?,?,'push','admission',0,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)",
        operationId, this.host.accountId, this.host.principal, call.request_id, fp.fingerprint, access.epoch, d.view, version, hex(d.revision), call.payload_json, original, profile, person, mode, mapped, this.now() + TTL, Math.min(expiry, this.now() + TTL));
      const row = this.operation(this.host.principal, call.request_id)!;
      for (const root of [{ kind: 3, id: base }, { kind: 3, id: head }]) {
        const proof = this.host.requireRetainedClosureInTx(root);
        this.host.retainReviewInputInTx(row.id, root, proof.proof_job); this.enqueue(row.id, root);
      }
      return row;
    });
  }
  private async collect(owner: string, check: () => void, savedBytes: number, commit: (bytes: number) => void): Promise<boolean> {
    let reads = 0, total = 0; const deadline = Date.now() + 5000;
    while (reads < 16 && Date.now() < deadline) {
      const next = this.host.transactionSync(() => { check(); return this.one<Pack>('SELECT * FROM review_pack WHERE owner=? AND data IS NULL ORDER BY kind DESC,id LIMIT 1', owner); });
      if (!next) return true;
      const d = this.host.transactionSync(() => { check(); const d = this.host.describeObjectInTx(next); if (!d) fail(503, 'unavailable'); if (!safe(d.size_bytes, MAX_META) || d.kind === 1 || d.format_version !== 1) fail(429, 'resource_exhausted'); return d; });
      if (total + d.size_bytes > 256 * 1024) return false;
      let loaded: { descriptor: Descriptor; bytes: Uint8Array };
      try { loaded = await this.host.readVerifiedObject(d); }
      catch (error) { this.host.transactionSync(check); throw error; }
      this.host.transactionSync(() => {
        check(); if (!same(d, loaded.descriptor) || loaded.bytes.length !== d.size_bytes) fail(503, 'unavailable');
        let info; try { info = inspect(d.kind, loaded.bytes); } catch { fail(503, 'unavailable'); }
        if (!same(info, d)) fail(503, 'unavailable');
        const previous = this.one<Pack>('SELECT * FROM review_pack WHERE owner=? AND kind=? AND id=?', owner, d.kind, d.id);
        if (!previous || previous.data !== null) fail(409, 'aborted');
        if (savedBytes + d.size_bytes > MAX_PACK) fail(429, 'resource_exhausted');
        this.charge(0, d.size_bytes); this.host.sql.exec('UPDATE review_pack SET size=?,data=? WHERE owner=? AND kind=? AND id=?', d.size_bytes, loaded.bytes.slice().buffer, owner, d.kind, d.id);
        for (const reference of info.references) this.enqueue(owner, reference);
        savedBytes += d.size_bytes; commit(savedBytes);
      });
      reads++; total += d.size_bytes;
    }
    return this.host.transactionSync(() => { check(); return !this.one('SELECT 1 FROM review_pack WHERE owner=? AND data IS NULL LIMIT 1', owner); });
  }
  private metadata(owner: string, key: ObjectKey): Metadata {
    const row = this.one<Pack>('SELECT * FROM review_pack WHERE owner=? AND kind=? AND id=?', owner, key.kind, key.id);
    if (!row || row.data === null || row.size === null || !(row.data instanceof ArrayBuffer)) fail(503, 'unavailable');
    const data = new Uint8Array(row.data); if (data.length !== row.size) fail(503, 'unavailable');
    return { kind: row.kind, id: row.id, size_bytes: row.size, format_version: 1, bytes_base64: base64(data) };
  }
  private pack(owner: string): Metadata[] {
    const rows = this.rows<Pack>('SELECT * FROM review_pack WHERE owner=? ORDER BY kind,id LIMIT 8193', owner);
    if (rows.length > 8192 || rows.some(row => row.data === null)) fail(400, 'failed_precondition');
    return rows.map(row => this.metadata(owner, row));
  }
  private pending(operation: Operation): { state: string; result: unknown } {
    const intent = JSON.parse(operation.intent) as PushIntent;
    return { state: 'admission_pending', result: { schema_version: 1, request_id: operation.request_id,
      source_base: hex(intent.source_base), source_head: hex(intent.source_head), view_version: hex(intent.view_version), phase: 'admission' } };
  }
  private change(change: string): Change {
    const row = this.one<Change>('SELECT * FROM review_changes WHERE id=?', change); if (!row) fail(400, 'failed_precondition');
    if (row.account !== this.host.accountId || row.id !== change) fail(503, 'unavailable'); return row;
  }
  private patchset(change: string, patchset?: string): Patchset {
    const current = this.change(change);
    const row = this.one<Patchset>('SELECT * FROM review_patchsets WHERE id=? AND change_id=?', patchset ?? current.current_patchset, change);
    if (!row) fail(400, 'failed_precondition'); if (row.account !== this.host.accountId) fail(503, 'unavailable'); return row;
  }
  private readable(patchset: Patchset, selector = patchset.view_name): ResolvedView {
    this.host.authorizeInTx('read'); const view = this.host.views.resolveInTx(selector, array(patchset.version)); this.host.views.checkInTx(view);
    const paths = this.rows<{ value: string }>('SELECT value FROM review_requirements WHERE owner=? ORDER BY ordinal LIMIT 32769', patchset.operation);
    if (paths.length > 32768) fail(429, 'resource_exhausted');
    for (let i = 0; i < paths.length; i += 128) {
      const names = paths.slice(i, i + 128).flatMap(row => this.requirementPaths(JSON.parse(row.value)));
      if (names.length) {
        const answers = this.host.views.pathsAllowedInTx(view, names).paths;
        if (answers.length !== names.length || answers.some((answer, index) => answer.path !== names[index] || !answer.allowed)) fail(403, 'permission_denied');
      }
    }
    return view;
  }
  private admit(operation: Operation, changeId: string): Patchset {
    return this.host.transactionSync(() => {
      const { operation: row, view } = this.current(operation);
      const intent = JSON.parse(row.intent) as PushIntent;
      if (!id(changeId, 32) || changeId === '0'.repeat(32)) fail(400, 'failed_precondition');
      if (intent.predecessor && hex(intent.predecessor.change_id, 16) === changeId) fail(400, 'failed_precondition');
      this.validateRequirements(row, view);
      if (row.authority_until <= this.now()) fail(403, 'permission_denied');
      const access = this.host.authorizeInTx('write', row.epoch);
      const old = this.one<Change>('SELECT * FROM review_changes WHERE id=?', changeId);
      if (old && (old.account !== this.host.accountId || (old.owner !== row.principal && !access.is_admin))) fail(403, 'permission_denied');
      if ((old?.current_patchset ?? null) !== (intent.expected_patchset === null ? null : hex(intent.expected_patchset))) fail(400, 'failed_precondition');
      const ordinal = old ? old.version + 1 : 1; if (ordinal > 64) fail(429, 'resource_exhausted');
      if (!old && this.one<{ n: number }>('SELECT COUNT(*) AS n FROM review_changes')!.n >= 1024) fail(429, 'resource_exhausted');
      const identity = deriveForgeReviewIdentity({ kind: 'push', account_id: row.account, principal: row.principal, request_id: row.request_id,
        epoch: row.epoch, change_id: changeId, profile: JSON.parse(row.profile), payload_json: row.payload, attempt: 0 }) as { fingerprint: string; patchset_id: string };
      if (identity.fingerprint !== row.fingerprint || !id(identity.patchset_id)) fail(503, 'unavailable');
      this.charge(old ? 1 : 2, encoder.encode(row.intent + row.profile + row.committer).length + 1024);
      this.host.sql.exec('INSERT INTO review_patchsets(id,account,change_id,ordinal,operation,principal,epoch,view_name,version,revision,source_base,source_head,source_mode,mapped_base,intent,profile,committer) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        identity.patchset_id, row.account, changeId, ordinal, row.id, row.principal, row.epoch, row.view_name, row.version, row.revision,
        hex(intent.source_base), hex(intent.source_head), row.source_mode, row.mapped_base, row.intent, row.profile, row.committer);
      if (old) this.host.sql.exec('UPDATE review_changes SET current_patchset=?,version=? WHERE id=?', identity.patchset_id, ordinal, changeId);
      else this.host.sql.exec('INSERT INTO review_changes(id,account,owner,current_patchset,version) VALUES(?,?,?,?,1)', changeId, row.account, row.principal, identity.patchset_id);
      const sourceJobs = this.sourceProofs(row);
      for (const [index, root] of [{ kind: 3, id: hex(intent.source_base) }, { kind: 3, id: hex(intent.source_head) }].entries()) {
        const proof = this.one<{ phase: string; binding: string; root_kind: number; root_id: string }>('SELECT phase,binding,root_kind,root_id FROM closure_jobs WHERE id=?', sourceJobs[index]);
        const binding = this.sourceBinding(row, root, index);
        const expected = JSON.stringify([binding.account_id, binding.identity, binding.policy_epoch, binding.purpose, binding.publication]);
        if (!proof || proof.phase !== 'published' || proof.binding !== expected || proof.root_kind !== 3 || proof.root_id !== root.id) fail(400, 'failed_precondition');
        const retained = this.host.requireRetainedClosureInTx(root);
        this.host.retainReviewInputInTx(identity.patchset_id, root, retained.proof_job);
      }
      row.phase = 'admitted'; row.change_id = changeId; row.patchset_id = identity.patchset_id; this.save(row);
      return this.patchset(changeId, identity.patchset_id);
    });
  }
  private latest(p: Patchset): Attempt | undefined {
    const a = this.one<Attempt>('SELECT * FROM review_attempts WHERE patchset=? ORDER BY ordinal DESC LIMIT 1', p.id);
    if (a && (a.account !== this.host.accountId || a.patchset !== p.id || !id(a.id) || !safe(a.ordinal, 7))) fail(503, 'unavailable'); return a;
  }
  private saveAttempt(a: Attempt): void {
    a.generation++;
    this.host.sql.exec('UPDATE review_attempts SET generation=?,phase=?,metadata_bytes=?,plan_hash=?,plan_bytes=?,output_cursor=?,integrity_job=?,candidate=?,conflict_path=?,publication_generation=? WHERE id=?',
      a.generation, a.phase, a.metadata_bytes, a.plan_hash, a.plan_bytes, a.output_cursor, a.integrity_job, a.candidate, a.conflict_path, a.publication_generation, a.id);
  }
  private attemptCurrent(p: Patchset, a: Attempt): void {
    const row = this.one<Attempt>('SELECT * FROM review_attempts WHERE id=?', a.id);
    if (!row || row.account !== this.host.accountId || row.patchset !== p.id || row.generation !== a.generation || row.principal !== this.host.principal) fail(409, 'aborted');
    if (this.change(p.change_id).current_patchset !== p.id || this.latest(p)?.id !== a.id) fail(409, 'aborted');
    this.host.authorizeInTx('write', p.epoch); const view = this.readable(p);
    if (view.description.policy_epoch !== p.epoch || this.now() >= a.expires_at) fail(400, 'failed_precondition');
    const main = this.host.currentMainInTx(); if (main.head !== a.target || main.generation !== a.target_generation) fail(409, 'aborted');
    const plan = this.buildPlanInTx(p, view, main); if (plan.build !== a.build_target || plan.mapped !== a.mapped_base) fail(409, 'aborted');
  }
  private buildPlanInTx(p: Patchset, view: ResolvedView, main: { head: string; generation: number }): { build: string; mapped: string | null } {
    const intent = JSON.parse(p.intent) as PushIntent;
    let dependency = intent.predecessor;
    const seen = new Set([p.change_id]);
    while (dependency) {
      const change = hex(dependency.change_id, 16); if (seen.has(change) || seen.size >= 64) fail(400, 'failed_precondition'); seen.add(change);
      const parent = this.patchset(change, hex(dependency.patchset_id)); this.readable(parent);
      if (this.change(change).current_patchset !== parent.id) fail(400, 'failed_precondition');
      dependency = (JSON.parse(parent.intent) as PushIntent).predecessor;
    }
    if (intent.predecessor) {
      const parent = this.patchset(hex(intent.predecessor.change_id, 16), hex(intent.predecessor.patchset_id));
      if (parent.source_head !== p.source_base) fail(400, 'failed_precondition');
      const attempt = this.latest(parent); if (!attempt || !attempt.candidate || attempt.publication_generation === null || attempt.target !== main.head || attempt.target_generation !== main.generation || this.now() >= attempt.expires_at) fail(400, 'failed_precondition');
      const candidate = JSON.parse(attempt.candidate) as { head: string };
      this.host.requireRetainedClosureInTx({ kind: 3, id: candidate.head });
      return { build: candidate.head, mapped: candidate.head };
    }
    if (p.source_mode === 'projected') {
      const relation = this.host.projections.canonicalForProjectedBaseInTx(view, p.source_base);
      if (!relation || relation.canonical_revision !== p.mapped_base) fail(403, 'permission_denied');
      return { build: main.head, mapped: relation.canonical_revision };
    }
    if (p.source_base !== p.revision || p.mapped_base !== null) fail(403, 'permission_denied');
    return { build: main.head, mapped: null };
  }
  private startAttempt(p: Patchset): Attempt | undefined {
    return this.host.transactionSync(() => {
      this.host.authorizeInTx('write'); const view = this.readable(p);
      if (this.change(p.change_id).current_patchset !== p.id) return undefined;
      this.host.authorizeInTx('write', p.epoch);
      if (view.description.policy_epoch !== p.epoch) fail(409, 'aborted');
      const main = this.host.currentMainInTx(); if (!id(main.head) || !safe(main.generation) || main.generation < 1) fail(503, 'unavailable');
      const plan = this.buildPlanInTx(p, view, main), old = this.latest(p);
      if (old && old.target === main.head && old.target_generation === main.generation && old.build_target === plan.build && old.mapped_base === plan.mapped
          && this.now() < old.expires_at && old.phase !== 'stale' && old.phase !== 'expired') return old;
      if (old && !['stale', 'expired'].includes(old.phase)) { old.phase = this.now() >= old.expires_at ? 'expired' : 'stale'; this.saveAttempt(old); }
      const ordinal = old ? old.ordinal + 1 : 0; if (ordinal >= 8) fail(429, 'resource_exhausted');
      const identity = deriveForgeReviewIdentity({ kind: 'attempt', account_id: this.host.accountId, change_id: p.change_id, patchset_id: p.id, attempt: ordinal }) as { candidate_id: string };
      if (!id(identity.candidate_id)) fail(503, 'unavailable');
      this.charge(1, 1024);
      this.host.sql.exec("INSERT INTO review_attempts(id,account,patchset,ordinal,generation,principal,target,target_generation,build_target,mapped_base,phase,expires_at,metadata_bytes,plan_bytes,output_cursor) VALUES(?,?,?,?,0,?,?,?,?,?,'collect',?,0,0,0)",
        identity.candidate_id, this.host.accountId, p.id, ordinal, this.host.principal, main.head, main.generation, plan.build, plan.mapped, this.now() + 3600);
      for (const head of new Set([p.source_base, p.source_head, main.head, plan.build, ...(plan.mapped ? [plan.mapped] : [])])) {
        const root = { kind: 3, id: head }, proof = this.host.requireRetainedClosureInTx(root);
        this.host.retainReviewInputInTx(identity.candidate_id, root, proof.proof_job); this.enqueue(identity.candidate_id, root);
      }
      return this.latest(p)!;
    });
  }
  private async prepare(p: Patchset): Promise<void> {
    let a = this.startAttempt(p); if (!a) return;
    if (a.phase === 'collect') {
      const complete = await this.collect(a.id, () => this.attemptCurrent(p, a!), a.metadata_bytes, bytes => { a!.metadata_bytes = bytes; this.saveAttempt(a!); });
      if (complete) this.host.transactionSync(() => { this.attemptCurrent(p, a!); a!.phase = 'build'; this.saveAttempt(a!); });
      return;
    }
    if (a.phase === 'build') {
      const input = this.host.transactionSync(() => {
        this.attemptCurrent(p, a!);
        const mapping = p.source_mode === 'projected' ? { kind: 'projected', canonical_base: a!.mapped_base }
          : p.source_mode === 'predecessor' ? { kind: 'predecessor', mapped_base: a!.mapped_base } : { kind: 'canonical' };
        return { schema: 1, context: { account_id: this.host.accountId, view_version: p.version, policy_epoch: p.epoch,
          target_head: a!.target, target_generation: a!.target_generation, source_base: p.source_base, source_head: p.source_head,
          destination_head: a!.build_target, committer: JSON.parse(p.committer), base_mapping: mapping }, objects: this.pack(a!.id) };
      });
      let output: unknown; try { output = buildForgeCandidate(input); } catch { fail(400, 'failed_precondition'); }
      const value = output as { state: string; path?: string; kind?: number; id?: string };
      if (value.state !== 'candidate') {
        this.host.transactionSync(() => {
          this.attemptCurrent(p, a!);
          if (value.state === 'missing_metadata' && value.kind && value.id) {
            if (value.kind === 1 || (value.kind === 3 && value.id === ROOT)) fail(503, 'unavailable');
            if (this.one('SELECT 1 FROM review_pack WHERE owner=? AND kind=? AND id=?', a!.id, value.kind, value.id)) fail(503, 'unavailable');
            this.enqueue(a!.id, { kind: value.kind, id: value.id }); a!.phase = 'collect';
          }
          else if (value.state === 'conflict') { const view = this.readable(p); this.pathAllowed(view, value.path!); a!.phase = 'conflict'; a!.conflict_path = value.path!; }
          else if (value.state === 'unsupported') a!.phase = 'unsupported';
          else if (value.state === 'limit') a!.phase = 'limit';
          else fail(400, 'failed_precondition');
          this.saveAttempt(a!);
        });
        return;
      }
      const plan = output as Plan; this.validatePlan(plan);
      const bytes = encoder.encode(encoded(plan, 4 * 1024 * 1024)); const digest = await this.planDigest(p, a!, bytes);
      this.host.transactionSync(() => {
        this.attemptCurrent(p, a!); this.charge(Math.ceil(bytes.length / CHUNK), bytes.length);
        for (let offset = 0; offset < bytes.length; offset += CHUNK) this.host.sql.exec('INSERT INTO review_plan(owner,ordinal,data) VALUES(?,?,?)', a!.id, offset / CHUNK, bytes.slice(offset, offset + CHUNK).buffer);
        a!.plan_hash = digest; a!.plan_bytes = bytes.length; a!.phase = 'stage'; this.saveAttempt(a!);
      }); return;
    }
    if (a.phase === 'stage') { await this.stage(p, a); return; }
    if (a.phase === 'closure') { await this.prove(p, a); return; }
    if (a.phase === 'checks_pending') await this.prepareProjection(p, a);
  }
  private validatePlan(plan: Plan): void {
    if (plan.state !== 'candidate' || !id(plan.head) || !id(plan.root_tree) || !Array.isArray(plan.objects) || plan.objects.length > 8192
        || !Array.isArray(plan.rewrites) || plan.rewrites.length > 128) fail(503, 'unavailable');
    let bytes = 0; const keys = new Set<string>();
    for (const object of plan.objects) {
      if (![2, 3, 4, 5].includes(object.kind) || !id(object.id) || object.format_version !== 1 || !safe(object.size_bytes, MAX_META)) fail(429, 'resource_exhausted');
      const key = `${object.kind}/${object.id}`; if (keys.has(key)) fail(503, 'unavailable'); keys.add(key);
      bytes += object.size_bytes; if (bytes > 2 * 1024 * 1024) fail(429, 'resource_exhausted');
      const native = inspect(object.kind, decode(object)); if (!same(native, object)) fail(503, 'unavailable');
    }
    if (!keys.has(`3/${plan.head}`) || plan.rewrites.some(r => !id(r.old) || !id(r.new) || !id(r.change_id, 32))) fail(503, 'unavailable');
  }
  private async planDigest(p: Patchset, a: Attempt, bytes: Uint8Array): Promise<string> {
    try { return await hash(decoder.decode(bytes)); }
    finally { this.host.transactionSync(() => this.attemptCurrent(p, a)); }
  }
  private async plan(p: Patchset, a: Attempt): Promise<Plan> {
    const bytes = this.host.transactionSync(() => {
      this.attemptCurrent(p, a); if (!a.plan_hash || !safe(a.plan_bytes, 4 * 1024 * 1024)) fail(503, 'unavailable');
      const rows = this.rows<{ ordinal: number; data: ArrayBuffer }>('SELECT ordinal,data FROM review_plan WHERE owner=? ORDER BY ordinal', a.id);
      const output = new Uint8Array(a.plan_bytes); let offset = 0;
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i]; if (row.ordinal !== i || !(row.data instanceof ArrayBuffer) || row.data.byteLength > CHUNK || row.data.byteLength > output.length - offset) fail(503, 'unavailable');
        output.set(new Uint8Array(row.data), offset); offset += row.data.byteLength;
      }
      if (offset !== output.length) fail(503, 'unavailable'); return output;
    });
    const digest = await this.planDigest(p, a!, bytes);
    return this.host.transactionSync(() => {
      this.attemptCurrent(p, a); if (digest !== a.plan_hash) fail(503, 'unavailable');
      let plan: Plan; try { plan = JSON.parse(decoder.decode(bytes)); } catch { fail(503, 'unavailable'); }
      this.validatePlan(plan);
      const context = plan.context as { account_id: string; view_version: string; policy_epoch: number; target_head: string; target_generation: number; source_base: string; source_head: string; destination_head: string; committer: unknown; base_mapping: unknown };
      if (!context || typeof context !== 'object' || Array.isArray(context)
          || Object.keys(context).sort().join(',') !== 'account_id,base_mapping,committer,destination_head,policy_epoch,source_base,source_head,target_generation,target_head,view_version') fail(503, 'unavailable');
      // Rust Value maps sort keys; saved JS inputs retain insertion order.
      // Compare the exact known fields, not their JSON object serialization.
      const person = context.committer as Person;
      const frozen = JSON.parse(p.committer) as Person;
      if (!person || typeof person !== 'object' || Array.isArray(person)
          || Object.keys(person).sort().join(',') !== 'email,millis_since_epoch,name,tz_offset_minutes'
          || person.name !== frozen.name || person.email !== frozen.email
          || person.millis_since_epoch !== frozen.millis_since_epoch || person.tz_offset_minutes !== frozen.tz_offset_minutes) fail(503, 'unavailable');
      const mapping = context.base_mapping as { kind: string; canonical_base?: string; mapped_base?: string };
      if (!mapping || typeof mapping !== 'object' || Array.isArray(mapping)) fail(503, 'unavailable');
      const mappingKeys = Object.keys(mapping).sort().join(',');
      if (p.source_mode === 'canonical') {
        if (mappingKeys !== 'kind' || mapping.kind !== 'canonical' || a.mapped_base !== null) fail(503, 'unavailable');
      } else if (p.source_mode === 'projected') {
        if (mappingKeys !== 'canonical_base,kind' || mapping.kind !== 'projected' || typeof mapping.canonical_base !== 'string'
            || !id(mapping.canonical_base) || mapping.canonical_base !== a.mapped_base) fail(503, 'unavailable');
      } else if (p.source_mode === 'predecessor') {
        if (mappingKeys !== 'kind,mapped_base' || mapping.kind !== 'predecessor' || typeof mapping.mapped_base !== 'string'
            || !id(mapping.mapped_base) || mapping.mapped_base !== a.mapped_base) fail(503, 'unavailable');
      } else fail(503, 'unavailable');
      if (context.account_id !== p.account || context.view_version !== p.version || context.policy_epoch !== p.epoch || context.target_head !== a.target
          || context.target_generation !== a.target_generation || context.source_base !== p.source_base || context.source_head !== p.source_head || context.destination_head !== a.build_target) fail(503, 'unavailable');
      return plan;
    });
  }
  private async stage(p: Patchset, initial: Attempt): Promise<void> {
    let a = initial; const plan = await this.plan(p, a); let writes = 0, bytes = 0; const deadline = Date.now() + 5000;
    while (a.output_cursor < plan.objects.length && writes < 16 && Date.now() < deadline) {
      const object = plan.objects[a.output_cursor]; if (bytes + object.size_bytes > 256 * 1024) break;
      const body = decode(object);
      this.host.transactionSync(() => { this.attemptCurrent(p, a); this.host.reserveGeneratedInTx(a.id, object); });
      let confirmation: unknown;
      try { confirmation = await this.host.stageGenerated(object, body); }
      catch (error) { this.host.transactionSync(() => this.attemptCurrent(p, a)); throw error; }
      this.host.transactionSync(() => { this.attemptCurrent(p, a); this.host.confirmGeneratedInTx(a.id, object, confirmation); a.output_cursor++; this.saveAttempt(a); });
      writes++; bytes += body.length;
    }
    if (a.output_cursor === plan.objects.length) this.host.transactionSync(() => { this.attemptCurrent(p, a); a.phase = 'closure'; this.saveAttempt(a); });
  }
  private candidateJobs(p: Patchset, a: Attempt, plan: Plan): ClosureJobs {
    const binding: ClosureBinding = { account_id: this.host.accountId, identity: { kind: 'principal', principal: a.principal }, policy_epoch: p.epoch,
      purpose: 'review-candidate', publication: encoded([p.id, a.id, a.plan_hash, plan.head, a.target, a.target_generation, a.build_target, a.mapped_base]) };
    const publication = encoded(binding);
    return new ClosureJobs({
      binding, sql: this.host.sql, transactionSync: callback => this.host.transactionSync(callback),
      authorizeCurrentInTx: current => { if (encoded(current) !== publication) fail(403, 'permission_denied'); this.attemptCurrent(p, a); },
      describeObjectInTx: key => this.host.describeObjectInTx(key),
      readVerifiedObject: key => this.host.readVerifiedObject(key),
      attachRootInTx: (current, root, proofJob) => {
        if (encoded(current) !== publication || root.kind !== 3 || root.id !== plan.head) fail(403, 'permission_denied');
        this.attemptCurrent(p, a);
        if (a.phase !== 'closure' || a.integrity_job !== proofJob || a.output_cursor !== plan.objects.length) fail(400, 'failed_precondition');
        for (const object of plan.objects) { const actual = this.host.describeObjectInTx(object); if (!actual || !same(actual, object)) fail(503, 'unavailable'); }
        for (const head of [p.source_base, p.source_head]) this.host.requireRetainedClosureInTx({ kind: 3, id: head });
        this.host.retainCandidateInTx(a.id, root, proofJob);
        a.candidate = encoded({ head: plan.head, root_tree: plan.root_tree, rewrites: plan.rewrites }, 1024 * 1024);
        this.charge(0, encoder.encode(a.candidate).length); a.phase = 'checks_pending'; a.publication_generation = a.generation + 1; this.saveAttempt(a);
        return { candidate_id: a.id, head: plan.head };
      },
    });
  }
  private async prove(p: Patchset, a: Attempt): Promise<void> {
    const plan = await this.plan(p, a), jobs = this.candidateJobs(p, a, plan);
    if (a.integrity_job === null) {
      const status = await jobs.begin(`review-${a.id}`, { kind: 3, id: plan.head });
      this.host.transactionSync(() => { this.attemptCurrent(p, a); a.integrity_job = status.job_id; this.saveAttempt(a); }); return;
    }
    const status = jobs.status(a.integrity_job);
    if (status.error === 'closure_limit') fail(429, 'resource_exhausted');
    if (!status.complete) {
      const next = await jobs.advance(status.job_id, status.generation); this.host.transactionSync(() => this.attemptCurrent(p, a));
      if (next.error === 'closure_limit') fail(429, 'resource_exhausted');
      if (next.error === 'object_invalid') fail(400, 'failed_precondition');
      if (next.error === 'object_unavailable') fail(503, 'unavailable');
      return;
    }
    jobs.publish(status.job_id, status.generation);
  }
  /** Native review result identity from a retained ledger candidate. This is
   * trusted host input only; not callable with a JSON candidate/source claim. */
  reviewProjectionSourceInTx(change: string, patchset: string, candidateId: string): ReviewProjectionSource {
    const p = this.patchset(change, patchset), view = this.readable(p);
    const a = this.one<Attempt>('SELECT * FROM review_attempts WHERE id=? AND patchset=?', candidateId, p.id);
    if (!a?.candidate || a.publication_generation === null) fail(400, 'failed_precondition');
    const candidate = JSON.parse(a.candidate) as { head: string };
    this.host.requireRetainedClosureInTx({ kind: 3, id: candidate.head });
    let parent = p.source_base;
    if (!this.host.projections.canonicalForProjectedBaseInTx(view, parent)) {
      const baseline = this.host.projections.readyInTx(view); if (!baseline) fail(400, 'failed_precondition'); parent = hex(baseline.binding.projected_commit);
    }
    return { account_id: p.account, view_name: p.view_name, view_version: p.version, review_epoch: p.epoch,
      projection_epoch: view.description.policy_epoch, change_id: p.change_id, patchset_id: p.id, candidate_id: a.id,
      attempt_generation: a.publication_generation, canonical_head: candidate.head, projected_parent: parent };
  }
  private async prepareProjection(p: Patchset, a: Attempt): Promise<void> {
    const baselineView = this.host.transactionSync(() => {
      this.attemptCurrent(p, a); const view = this.readable(p);
      return !this.host.projections.canonicalForProjectedBaseInTx(view, p.source_base) && !this.host.projections.readyInTx(view) ? view : undefined;
    });
    if (baselineView) {
      const status = this.host.projections.start(baselineView);
      const next = status.state === 'pending' ? await this.host.projections.advance(baselineView, { job_id: status.job_id, expected_generation: status.generation }) : status;
      this.host.transactionSync(() => this.attemptCurrent(p, a));
      if (next.state === 'limit') fail(429, 'resource_exhausted'); if (next.state === 'unavailable') fail(503, 'unavailable'); return;
    }
    const context = this.host.transactionSync(() => { this.attemptCurrent(p, a); return { view: this.readable(p), source: this.reviewProjectionSourceInTx(p.change_id, p.id, a.id) }; });
    const status = this.host.projections.startReviewResult(context.view, context.source);
    if (status.state === 'pending') {
      const next = await this.host.projections.advanceReviewResult(context.view, context.source, { job_id: status.job_id, expected_generation: status.generation });
      this.host.transactionSync(() => this.attemptCurrent(p, a));
      if (next.state === 'limit') fail(429, 'resource_exhausted'); if (next.state === 'unavailable') fail(503, 'unavailable');
    } else if (status.state === 'limit') fail(429, 'resource_exhausted');
    else if (status.state === 'unavailable') fail(503, 'unavailable');
  }
  private approvals(p: Patchset, a: Attempt): number {
    const decisions = this.rows<{ principal: string; decision: string; epoch: number }>('SELECT principal,decision,epoch FROM review_decisions WHERE change_id=? AND patchset=? AND candidate=? ORDER BY ordinal DESC LIMIT 129', p.change_id, p.id, a.id);
    if (decisions.length > 128) fail(503, 'unavailable');
    let count = 0; const seen = new Set<string>();
    for (const decision of decisions) {
      if (seen.has(decision.principal) || decision.epoch !== p.epoch) continue; seen.add(decision.principal);
      try { this.host.authorizeInTx('review', p.epoch, decision.principal); }
      catch (error) { if (error instanceof Failure && error.status === 403) continue; throw error; }
      if (decision.decision === 'request_changes') return 0;
      if (decision.decision !== 'approve') fail(503, 'unavailable'); count++;
    }
    return count;
  }
  private reply(p: Patchset, selector: string): { state: string; result: unknown } {
    return this.host.transactionSync(() => {
      const view = this.readable(p, selector), change = this.change(p.change_id), intent = JSON.parse(p.intent) as PushIntent;
      const a = this.latest(p), main = this.host.currentMainInTx();
      let state = 'rebase_pending', candidate: unknown = null, conflict: string | null = null, approvals = 0;
      if (a) {
        state = a.phase === 'checks_pending' ? 'checks_pending' : ['conflict', 'unsupported', 'stale', 'expired'].includes(a.phase) ? a.phase : 'rebase_pending';
        if (a.phase === 'limit') state = 'unsupported';
        if (this.now() >= a.expires_at) state = 'expired';
        if (main.head !== a.target || main.generation !== a.target_generation || view.description.policy_epoch !== p.epoch) state = 'stale';
        conflict = a.conflict_path; if (conflict !== null) this.pathAllowed(view, conflict);
        if (a.candidate !== null) {
          const raw = JSON.parse(a.candidate) as { head: string; root_tree: string; rewrites: { old: string; new: string; change_id: string }[] };
          this.host.requireRetainedClosureInTx({ kind: 3, id: raw.head });
          candidate = { id: array(a.id), head: array(raw.head), root_tree: array(raw.root_tree), target: array(a.target), target_generation: a.target_generation,
            rewrites: raw.rewrites.map(r => ({ old: array(r.old), new: array(r.new), change_id: array(r.change_id) })) };
          if (view.description.policy_epoch === p.epoch) approvals = this.approvals(p, a);
        }
      }
      if (!a && view.description.policy_epoch !== p.epoch) state = 'stale';
      // Match native confidentiality: comments belong to this exact immutable
      // patchset and policy epoch, and optional paths remain currently allowed.
      const comments = view.description.policy_epoch !== p.epoch ? [] : this.rows<{ author: string; patchset: string; body: string; path: string | null; created_at: number }>('SELECT author,patchset,body,path,created_at FROM review_comments WHERE account=? AND change_id=? AND patchset=? ORDER BY ordinal LIMIT 129', p.account, p.change_id, p.id);
      if (comments.length > 128) fail(503, 'unavailable');
      const commentPaths = comments.flatMap(comment => comment.path === null ? [] : [comment.path]);
      const pathAnswers = commentPaths.length ? this.host.views.pathsAllowedInTx(view, commentPaths).paths : [];
      if (pathAnswers.length !== commentPaths.length || pathAnswers.some((answer, index) => answer.path !== commentPaths[index])) fail(503, 'unavailable');
      let pathIndex = 0;
      const visibleComments = comments.filter(comment => comment.path === null || pathAnswers[pathIndex++].allowed);
      const dto = { change_id: array(p.change_id), owner: change.owner, patchset_id: array(p.id), patchset_number: p.ordinal,
        is_current: change.current_patchset === p.id, source_base: array(p.source_base), source_head: array(p.source_head), view_version: array(p.version),
        predecessor: intent.predecessor, provenance: intent.provenance, lineage: intent.lineage, check_profile: JSON.parse(p.profile), state,
        candidate, conflict_path: conflict, approvals, comments: visibleComments.map(c => ({ author: c.author, patchset_id: array(c.patchset), body: c.body, path: c.path, created_at: c.created_at })) };
      const formatted = formatForgeReview(dto) as { state: string; value: Record<string, unknown> };
      if (a?.candidate !== null && a?.candidate !== undefined && a.publication_generation !== null
          && (this.host.projections.canonicalForProjectedBaseInTx(view, p.source_base) || this.host.projections.readyInTx(view))) {
        const source = this.reviewProjectionSourceInTx(p.change_id, p.id, a.id);
        const ready = this.host.projections.readyReviewInTx(view, source);
        if (ready) {
          formatted.value.policy_epoch = ready.binding.policy_epoch;
          formatted.value.projected_head = hex(ready.binding.projected_commit);
          formatted.value.projected_rewrites = [{ old: p.source_head, new: hex(ready.binding.projected_commit), change_id: p.change_id }];
        }
      }
      return { state: formatted.state, result: formatted.value };
    });
  }
  private recordDecisionOperation(call: ForgeCall, kind: string, fingerprint: string, p: Patchset): void {
    if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM review_operations')!.n >= 4096) fail(429, 'resource_exhausted');
    const identity = deriveForgeReviewIdentity({ kind: 'operation', principal: this.host.principal, request_id: call.request_id }) as { operation_id: string };
    this.charge(1, encoder.encode(call.payload_json).length + 1024);
    this.host.sql.exec("INSERT INTO review_operations(id,account,principal,request_id,fingerprint,kind,phase,generation,epoch,view_name,version,revision,payload,intent,profile,committer,source_mode,mapped_base,change_id,patchset_id,expires_at,authority_until,metadata_bytes,scope_entries,scope_paths) VALUES(?,?,?,?,?,?,'done',0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)",
      identity.operation_id, this.host.accountId, this.host.principal, call.request_id, fingerprint, kind,
      p.epoch, p.view_name, p.version, p.revision, call.payload_json, p.intent, p.profile, p.committer, p.source_mode, p.mapped_base, p.change_id, p.id, this.now() + TTL, this.now() + TTL);
  }
  private decide(call: ForgeCall, input: Extract<ReviewCommand, { operation: 'decide' }>['intent']): Patchset {
    return this.host.transactionSync(() => {
      const change = hex(input.change_id, 16), patchset = hex(input.patchset_id), intent = input.intent;
      this.host.authorizeInTx(intent.kind === 'vote' ? 'review' : 'read');
      const identity = intent.kind === 'vote'
        ? deriveForgeReviewIdentity({ kind: 'vote', change_id: change, patchset_id: patchset, candidate_id: hex(intent.candidate_id), decision: intent.decision })
        : deriveForgeReviewIdentity({ kind: 'comment', change_id: change, patchset_id: patchset, body: intent.body, path: intent.path });
      const fingerprint = (identity as { fingerprint: string }).fingerprint;
      const replay = this.operation(this.host.principal, call.request_id);
      if (replay) {
        if (replay.kind !== intent.kind || replay.fingerprint !== fingerprint || !replay.change_id || !replay.patchset_id) fail(409, 'already_exists');
        const original = this.patchset(replay.change_id, replay.patchset_id); this.readable(original, call.view); return original;
      }
      const p = this.patchset(change, patchset), view = this.readable(p, call.view);
      this.host.authorizeInTx(intent.kind === 'vote' ? 'review' : 'read', p.epoch);
      if (view.description.policy_epoch !== p.epoch) fail(400, 'failed_precondition');
      if (intent.kind === 'comment') {
        const length = encoder.encode(intent.body).length;
        if (length === 0 || length > 4096 || (intent.path !== null && encoder.encode(intent.path).length > 4096)) fail(429, 'resource_exhausted');
        if (intent.path !== null) this.pathAllowed(view, intent.path);
        const ordinal = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM review_comments WHERE change_id=?', change)!.n;
        if (ordinal >= 128) fail(429, 'resource_exhausted');
        this.charge(1, length + (intent.path?.length ?? 0) * 4 + 256);
        this.host.sql.exec('INSERT INTO review_comments(account,change_id,patchset,author,body,path,created_at,ordinal) VALUES(?,?,?,?,?,?,?,?)', this.host.accountId, change, p.id, this.host.principal, intent.body, intent.path, this.now(), ordinal);
      } else {
        const a = this.latest(p), main = this.host.currentMainInTx();
        if (this.change(change).current_patchset !== p.id || !a?.candidate || a.publication_generation === null || a.id !== hex(intent.candidate_id)
            || a.target !== main.head || a.target_generation !== main.generation || this.now() >= a.expires_at) fail(400, 'failed_precondition');
        const ordinal = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM review_decisions WHERE change_id=?', change)!.n;
        if (ordinal >= 128) fail(429, 'resource_exhausted');
        this.charge(1, 512);
        this.host.sql.exec('INSERT INTO review_decisions(account,change_id,patchset,candidate,principal,decision,epoch,ordinal) VALUES(?,?,?,?,?,?,?,?)', this.host.accountId, change, p.id, a.id, this.host.principal, intent.decision, p.epoch, ordinal);
      }
      this.recordDecisionOperation(call, intent.kind, fingerprint, p); return p;
    });
  }
  async handle(call: ForgeCall): Promise<{ state: string; result: unknown }> {
    if (call.account_id !== this.host.accountId) fail(403, 'permission_denied');
    let command: ReviewCommand;
    try { command = normalizeForgeReviewCommand({ account_id: call.account_id, operation: call.operation, view: call.view, request_id: call.request_id, payload_json: call.payload_json }) as ReviewCommand; }
    catch (error) { if (error instanceof Error && error.message.startsWith('resource_exhausted:')) fail(429, 'resource_exhausted'); fail(400, 'invalid_argument'); }
    if (command.operation === 'land') { this.host.transactionSync(() => this.host.authorizeInTx('write')); fail(400, 'failed_precondition'); }
    if (command.operation === 'push') {
      let operation = this.begin(call, command.intent);
      if (operation.phase === 'admission') {
        const done = await this.admission(operation);
        operation = this.host.transactionSync(() => this.operation(this.host.principal, call.request_id)!);
        if (!done || operation.phase === 'admission') { this.host.transactionSync(() => this.current(operation)); return this.pending(operation); }
        return this.reply(this.host.transactionSync(() => this.patchset(operation.change_id!, operation.patchset_id!)), call.view);
      }
      const p = this.host.transactionSync(() => this.patchset(operation.change_id!, operation.patchset_id!));
      await this.prepare(p); return this.reply(p, call.view);
    }
    if (command.operation === 'get') return this.reply(this.host.transactionSync(() => this.patchset(hex(command.intent.change_id, 16), command.intent.patchset_id === null ? undefined : hex(command.intent.patchset_id))), call.view);
    if (command.operation === 'operation') {
      const op = this.host.transactionSync(() => {
        this.host.authorizeInTx('read'); const row = this.operation(this.host.principal, call.request_id);
        if (row) this.host.views.resolveInTx(call.view, array(row.version));
        if (row?.phase === 'admission') this.current(row); return row;
      });
      if (!op) return { state: 'not_found', result: null };
      if (op.phase === 'admission') return this.pending(op);
      return this.reply(this.host.transactionSync(() => this.patchset(op.change_id!, op.patchset_id!)), call.view);
    }
    if (command.operation === 'decide') return this.reply(this.decide(call, command.intent), call.view);
    fail(400, 'failed_precondition');
  }
  private requirementPaths(value: unknown): string[] { return reviewRequirementPaths(value); }
  private pathAllowed(view: ResolvedView, path: string): void {
    if (typeof path !== 'string' || encoder.encode(path).length > 4096) fail(400, 'failed_precondition');
    const paths = this.host.views.pathsAllowedInTx(view, [path]).paths;
    if (paths.length !== 1 || paths[0].path !== path || !paths[0].allowed) fail(403, 'permission_denied');
  }
  private requirement(operation: Operation, view: ResolvedView, requirement: Requirement): void {
    for (const path of this.requirementPaths(requirement)) this.pathAllowed(view, path);
    if (requirement.kind === 'path') return;
    let key: ObjectKey;
    if (requirement.kind === 'copy_origin') {
      const origin = this.host.projections.projectedCopyOriginInTx(view, requirement.copy_id);
      if (origin) {
        if (origin.origin.canonical_path !== requirement.canonical_path || hex(origin.origin.file) !== requirement.file_id) fail(403, 'permission_denied');
        return;
      }
      key = { kind: 5, id: requirement.copy_id };
    } else key = requirement.key;
    const descriptor = this.host.describeObjectInTx(key); if (!descriptor) fail(403, 'permission_denied');
    if (requirement.kind === 'entry' && requirement.expected_size !== null && descriptor.size_bytes !== requirement.expected_size) fail(400, 'failed_precondition');
    const expiry = this.host.requireScopedReferenceInTx(view, requirement.canonical_path, descriptor, this.now());
    if (!safe(expiry) || expiry <= this.now()) fail(403, 'permission_denied'); operation.authority_until = Math.min(operation.authority_until, expiry);
  }
  private validateRequirements(operation: Operation, view: ResolvedView): void {
    // Every requirement was resolved under this same immutable policy epoch.
    // Receipt/catalog witnesses are immutable; their minimum expiry and current
    // epoch fence seal the paged proof without a second unbounded witness walk.
    this.host.views.checkInTx(view);
    if (operation.facts === null || operation.scope_position !== operation.scope_entries || operation.authority_until <= this.now()
        || view.description.policy_epoch !== operation.epoch) fail(400, 'failed_precondition');
    const count = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM review_requirements WHERE owner=?', operation.id)!.n;
    if (count !== operation.scope_entries) fail(503, 'unavailable');
  }
  private sourceProofs(operation: Operation): string[] {
    let jobs: unknown; try { jobs = JSON.parse(operation.source_jobs); } catch { fail(503, 'unavailable'); }
    if (!Array.isArray(jobs) || jobs.length > 2 || jobs.some(job => typeof job !== 'string' || job.length > 128)) fail(503, 'unavailable');
    return jobs;
  }
  private sourceBinding(operation: Operation, root: ObjectKey, index: number): ClosureBinding {
    return { account_id: operation.account, identity: { kind: 'principal', principal: operation.principal }, policy_epoch: operation.epoch,
      purpose: 'review-source', publication: encoded([operation.id, operation.fingerprint, operation.version, operation.revision, operation.source_mode, operation.mapped_base, index, root.id]) };
  }
  private async verifySources(operation: Operation): Promise<boolean> {
    const intent = JSON.parse(operation.intent) as PushIntent;
    const roots = [{ kind: 3, id: hex(intent.source_base) }, { kind: 3, id: hex(intent.source_head) }];
    for (let index = 0; index < roots.length; index++) {
      const root = roots[index], binding = this.sourceBinding(operation, root, index), expected = encoded(binding);
      const jobs = new ClosureJobs({
        binding, sql: this.host.sql, transactionSync: callback => this.host.transactionSync(callback),
        authorizeCurrentInTx: current => { if (encoded(current) !== expected) fail(403, 'permission_denied'); const { view } = this.current(operation); this.validateRequirements(operation, view); },
        describeObjectInTx: key => this.host.describeObjectInTx(key), readVerifiedObject: key => this.host.readVerifiedObject(key),
        attachRootInTx: (current, actual, proofJob) => {
          const { view } = this.current(operation); this.validateRequirements(operation, view);
          if (encoded(current) !== expected || actual.kind !== 3 || actual.id !== root.id || this.sourceProofs(operation)[index] !== proofJob) fail(403, 'permission_denied');
          // Retention uses the host's existing root-liveness proof (including
          // its opaque genesis marker). This private fresh proof remains bound
          // separately in source_jobs and gates admission, never a global grant.
          const retained = this.host.requireRetainedClosureInTx(actual);
          this.host.retainReviewInputInTx(operation.id, actual, retained.proof_job); return { operation: operation.id, root: actual };
        },
      });
      const ids = this.host.transactionSync(() => { this.current(operation); return this.sourceProofs(operation); });
      if (ids.length <= index) {
        const status = await jobs.begin(`source-${operation.id}-${index}`, root);
        this.host.transactionSync(() => { this.current(operation); if (ids.length !== index) fail(503, 'unavailable'); ids.push(status.job_id); operation.source_jobs = encoded(ids); this.save(operation); }); return false;
      }
      const status = jobs.status(ids[index]);
      if (status.phase === 'published') continue;
      if (status.error === 'closure_limit') fail(429, 'resource_exhausted');
      if (!status.complete) {
        const next = await jobs.advance(status.job_id, status.generation);
        this.host.transactionSync(() => this.current(operation));
        if (next.error === 'object_invalid') fail(400, 'failed_precondition');
        if (next.error === 'object_unavailable') fail(503, 'unavailable');
        if (next.error === 'closure_limit') fail(429, 'resource_exhausted');
        return false;
      }
      jobs.publish(status.job_id, status.generation); return false;
    }
    return true;
  }
  private async admission(initial: Operation): Promise<boolean> {
    let operation = initial;
    if (operation.facts === null) {
      const complete = await this.collect(operation.id, () => { this.current(operation); }, operation.metadata_bytes,
        bytes => { operation.metadata_bytes = bytes; this.save(operation); });
      if (!complete) return false;
      const input = this.host.transactionSync(() => {
        this.current(operation); const intent = JSON.parse(operation.intent) as PushIntent;
        return { schema: 1, source_base: hex(intent.source_base), source_head: hex(intent.source_head), lineage: intent.lineage, objects: this.pack(operation.id) };
      });
      let analyzed: AdmissionResult; try { analyzed = reviewAdmission(input) as AdmissionResult; } catch { fail(400, 'failed_precondition'); }
      if (analyzed.state === 'missing_metadata') {
        if (analyzed.kind === 1 || (analyzed.kind === 3 && analyzed.id === ROOT)) fail(503, 'unavailable');
        this.host.transactionSync(() => {
          this.current(operation);
          if (this.one('SELECT 1 FROM review_pack WHERE owner=? AND kind=? AND id=?', operation.id, analyzed.kind, analyzed.id)) fail(503, 'unavailable');
          this.enqueue(operation.id, { kind: analyzed.kind, id: analyzed.id }); this.save(operation);
        }); return false;
      }
      if (analyzed.state === 'limit') fail(429, 'resource_exhausted');
      if (analyzed.state === 'unavailable') fail(503, 'unavailable');
      if (analyzed.state !== 'facts') fail(400, 'failed_precondition');
      const facts = analyzed.facts;
      hex(facts.change_id, 16); hex(facts.base_tree); hex(facts.head_tree);
      if (!Array.isArray(facts.requirements) || facts.requirements.length > 32768) fail(429, 'resource_exhausted');
      this.host.transactionSync(() => {
        this.current(operation); let paths = 0;
        facts.requirements.forEach((requirement, ordinal) => {
          const text = encoded(requirement); paths += this.requirementPaths(requirement).reduce((sum, path) => sum + encoder.encode(path).length, 0);
          if (paths > 4 * 1024 * 1024) fail(429, 'resource_exhausted');
          this.charge(1, encoder.encode(text).length); this.host.sql.exec('INSERT INTO review_requirements(owner,ordinal,value) VALUES(?,?,?)', operation.id, ordinal, text);
        });
        operation.facts = encoded({ change_id: facts.change_id, base_tree: facts.base_tree, head_tree: facts.head_tree });
        operation.scope_entries = facts.requirements.length; operation.scope_paths = paths; operation.scope_position = 0; this.save(operation);
      }); return false;
    }
    const done = this.host.transactionSync(() => {
      const { view } = this.current(operation);
      const requirements = this.rows<{ ordinal: number; value: string }>('SELECT ordinal,value FROM review_requirements WHERE owner=? AND ordinal>=? ORDER BY ordinal LIMIT 128', operation.id, operation.scope_position);
      for (const row of requirements) {
        if (row.ordinal !== operation.scope_position) fail(503, 'unavailable');
        this.requirement(operation, view, JSON.parse(row.value)); operation.scope_position++;
      }
      if (requirements.length === 0 && operation.scope_position !== operation.scope_entries) fail(503, 'unavailable');
      this.save(operation); return operation.scope_position === operation.scope_entries;
    });
    if (!done || !await this.verifySources(operation)) return false;
    const facts = JSON.parse(operation.facts) as { change_id: number[] };
    this.admit(operation, hex(facts.change_id, 16)); return true;
  }
}

type Requirement = { kind: 'path'; canonical_path: string }
  | { kind: 'entry'; canonical_path: string; key: ObjectKey; expected_size: number | null }
  | { kind: 'copy_origin'; canonical_path: string; copy_id: string; file_id: string };
type AdmissionResult = { state: 'facts'; facts: { change_id: number[]; base_tree: number[]; head_tree: number[]; requirements: Requirement[] } }
  | { state: 'missing_metadata'; kind: number; id: string } | { state: 'limit' | 'invalid_metadata' | 'unavailable' };
function reviewRequirementPaths(value: unknown): string[] {
  if (!value || typeof value !== 'object') fail(503, 'unavailable');
  const requirement = value as Requirement;
  if (!['path', 'entry', 'copy_origin'].includes(requirement.kind) || typeof requirement.canonical_path !== 'string'
      || encoder.encode(requirement.canonical_path).length > 4096) fail(503, 'unavailable');
  if (requirement.kind === 'entry' && (![1, 4, 5].includes(requirement.key.kind) || !id(requirement.key.id)
      || (requirement.expected_size !== null && !safe(requirement.expected_size)))) fail(503, 'unavailable');
  if (requirement.kind === 'copy_origin' && (!id(requirement.copy_id) || !id(requirement.file_id))) fail(503, 'unavailable');
  return [requirement.canonical_path];
}

/** Current public-view authority for an EXISTING exact candidate projection.
 * Deliberately has no member/ReviewGet requirement: anonymous authorized views
 * can read already sanitized catalog members. Never selects a latest candidate. */
export function requireReviewProjectionSourceInTx(host: Pick<ForgeReviewsHost, 'accountId' | 'sql' | 'views' | 'describeObjectInTx' | 'requireRetainedClosureInTx'>,
  view: ResolvedView, source: Readonly<ReviewProjectionSource>): void {
  const one = <T>(sql: string, ...args: (string | number)[]): T | undefined => [...host.sql.exec(sql, ...args)][0] as T | undefined;
  host.views.checkInTx(view); const d = view.description;
  if (source.account_id !== host.accountId || source.account_id !== d.account_id || source.view_name !== d.view
      || source.view_version !== hex(d.view_version) || source.projection_epoch !== d.policy_epoch) fail(403, 'permission_denied');
  const p = one<Patchset>('SELECT * FROM review_patchsets WHERE id=? AND change_id=?', source.patchset_id, source.change_id);
  const a = one<Attempt>('SELECT * FROM review_attempts WHERE id=? AND patchset=?', source.candidate_id, source.patchset_id);
  if (!p || !a || p.account !== host.accountId || a.account !== host.accountId || p.version !== source.view_version || p.view_name !== source.view_name
      || p.epoch !== source.review_epoch || a.publication_generation === null || a.publication_generation !== source.attempt_generation || a.candidate === null) fail(403, 'permission_denied');
  const candidate = JSON.parse(a.candidate) as { head: string };
  if (candidate.head !== source.canonical_head || !id(candidate.head) || !a.integrity_job) fail(403, 'permission_denied');
  const proof = one<{ phase: string; root_kind: number; root_id: string; account: string; binding: string; identity: string }>('SELECT phase,root_kind,root_id,account,binding,identity FROM closure_jobs WHERE id=?', a.integrity_job);
  if (!proof || proof.account !== host.accountId || !['ready', 'published'].includes(proof.phase) || proof.root_kind !== 3 || proof.root_id !== candidate.head
      || proof.identity !== JSON.stringify({ kind: 'principal', principal: a.principal })) fail(403, 'permission_denied');
  const publication = encoded([p.id, a.id, a.plan_hash, candidate.head, a.target, a.target_generation, a.build_target, a.mapped_base]);
  const binding = JSON.stringify([host.accountId, { kind: 'principal', principal: a.principal }, p.epoch, 'review-candidate', publication]);
  if (proof.binding !== binding) fail(403, 'permission_denied');
  const root = { kind: 3, id: candidate.head }; if (!host.describeObjectInTx(root)) fail(503, 'unavailable');
  host.requireRetainedClosureInTx(root);
  const relation = one<{ projected_commit: string }>('SELECT projected_commit FROM projection_catalogs WHERE account=? AND version=? AND epoch=? AND projected_commit=? AND retired=0 LIMIT 1', host.accountId, p.version, d.policy_epoch, p.source_base);
  const baseline = relation ? undefined : one<{ projected_commit: string }>("SELECT projected_commit FROM projection_catalogs WHERE account=? AND version=? AND epoch=? AND purpose='baseline' AND retired=0 LIMIT 1", host.accountId, p.version, d.policy_epoch);
  if (source.projected_parent !== (relation?.projected_commit ?? baseline?.projected_commit)) fail(403, 'permission_denied');
  const requirements = [...host.sql.exec('SELECT value FROM review_requirements WHERE owner=? ORDER BY ordinal LIMIT 32769', p.operation)] as unknown as { value: string }[];
  if (requirements.length > 32768) fail(503, 'unavailable');
  for (let offset = 0; offset < requirements.length; offset += 128) {
    const paths = requirements.slice(offset, offset + 128).flatMap(row => reviewRequirementPaths(JSON.parse(row.value)));
    const answers = host.views.pathsAllowedInTx(view, paths).paths;
    if (answers.length !== paths.length || answers.some((answer, index) => answer.path !== paths[index] || !answer.allowed)) fail(403, 'permission_denied');
  }
}
