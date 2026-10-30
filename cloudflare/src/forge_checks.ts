/** Dedicated trusted execution authority. Snapshot preparation is not a check;
 * lease expiry is not cleanup, and accepting a result never publishes main. */
import { inspect, checkerSnapshotPage, checkerManifestStep, checkerLeaseBinding, validateCheckerReceipt, checkerCommandFingerprint, type Descriptor, type ObjectKey } from './core';
import { fail, Failure } from './protocol';
export interface CheckProfile { name: string; version: string }
export interface CandidateScope { account_id: string; view_version: string; policy_epoch: number; change_id: string; patchset_id: string; candidate_id: string; target_head: string; target_generation: number; publication_generation: number }
export interface ReviewCheckCandidate { scope: CandidateScope; head: string; root_tree: string; profile: CheckProfile; expires_at: number; preparer: string; view_name: string; safe_projection_id: string }
export interface ExecutionPolicy { profile: CheckProfile; targets: string[]; test: boolean }
export interface CheckRequest { schema: number; job_id: string; attempt_id: string; repository_id: string; candidate_commit: string; root_tree: string; profile_name: string; profile_version: string }
export interface LeaseRef { lease_id: string; request: CheckRequest }
export interface SnapshotLimits { max_entries: number; max_files: number; max_file_bytes: number; max_total_bytes: number; max_metadata_bytes: number; max_metadata_object_bytes: number; max_path_bytes: number; max_depth: number }
export interface ManifestEntry { path: string; descriptor: Descriptor; value: { kind: 'directory' } | { kind: 'file'; executable: boolean; copy_id: string | null } | { kind: 'symlink'; target_bytes: number } }
interface ManifestTotals { digest: string; entries: number; file_bytes: number; path_bytes: number; metadata_bytes: number }
export interface CheckerLease { schema: number; lease_id: string; generation: number; request: CheckRequest; scope: CandidateScope; manifest: ManifestTotals & { id: string }; expires_at: number; limits: SnapshotLimits }
export interface CheckReceipt { schema: number; request: CheckRequest; workspace_id: string; outcome: 'passed' | 'failed' | 'unavailable'; reason: string; cleanup_confirmed: boolean; evidence: unknown }
export type CheckerCommand = { action: 'claim'; profile_name: string; profile_version: string } | { action: 'get_claim'; claim_id: string }
 | { action: 'renew'; lease: LeaseRef; expected_generation: number } | { action: 'manifest'; lease: LeaseRef; cursor: number; limit: number }
 | { action: 'object'; lease: LeaseRef; key: ObjectKey; offset: number; max_bytes: number }
 | { action: 'complete' | 'release'; lease: LeaseRef; receipt: CheckReceipt } | { action: 'get_result'; lease: LeaseRef; result_request_id: string };
export interface CheckerCall { schema: number; account_id: string; request_id: string; command: CheckerCommand }
export interface ForgeChecksHost {
 accountId: string; principal: string; sql: SqlStorage; transactionSync<T>(callback: () => T): T; nowSeconds(): number;
 authorizeCheckerInTx(profile: CheckProfile): void;
 executionPolicy(profile: CheckProfile): ExecutionPolicy | null;
 eligibleCandidatesInTx(profile: CheckProfile, limit: number): ReviewCheckCandidate[];
 recheckCandidateInTx(candidate: Readonly<ReviewCheckCandidate>): void;
 describeObjectInTx(key: ObjectKey): Descriptor | undefined;
 readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
 retainCheckInTx(leaseId: string, root: ObjectKey): void;
 releaseCheckInTx(leaseId: string, root: ObjectKey): void;
}
export interface CheckStatus { state: 'checks_pending' | 'passed' | 'failed'; valid_until?: number }
interface Claim { account: string; principal: string; request: string; fingerprint: string; candidate: string | null; snapshot: string | null; lease: string | null; phase: string }
interface Snapshot { id: string; account: string; candidate: string; phase: string; generation: number; hash_state: string | null; totals: string | null; hash_cursor: number; metadata_bytes: number; entries: number; path_bytes: number; file_bytes: number; files: number }
interface LeaseRow { id: string; account: string; principal: string; candidate: string; snapshot: string; value: string; binding: string; policy: string; phase: string; result_request: string | null; receipt: string | null; digest: string | null; accepted_until: number | null }
interface Frontier { snapshot: string; path: string; kind: number; id: string; cursor: number; value: string | null }
interface Completion { lease: LeaseRef; result_request_id: string; receipt_digest: string; outcome: CheckReceipt['outcome'] }
const LIMITS: SnapshotLimits = { max_entries: 32768, max_files: 32768, max_file_bytes: 1048576, max_total_bytes: 512 * 1048576, max_metadata_bytes: 32 * 1048576, max_metadata_object_bytes: 256 * 1024, max_path_bytes: 4 * 1048576, max_depth: 128 };
const encoder = new TextEncoder();
function safe(n: number, max = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(n) && n >= 0 && n <= max; }
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function text(value: unknown, max = 65536): string { const result = JSON.stringify(value); if (encoder.encode(result).length > max) fail(429, 'resource_exhausted'); return result; }
function nonce(): string { return Array.from(crypto.getRandomValues(new Uint8Array(32)), b => b.toString(16).padStart(2, '0')).join(''); }
function base64(bytes: Uint8Array): string { let result = ''; for (let i = 0; i < bytes.length; i += 8192) result += String.fromCharCode(...bytes.subarray(i, i + 8192)); return btoa(result); }
function reference(lease: CheckerLease): LeaseRef { return { lease_id: lease.lease_id, request: lease.request }; }
function profile(lease: CheckerLease): CheckProfile { return { name: lease.request.profile_name, version: lease.request.profile_version }; }
/** Candidate scopes contain known fields only. This is equality, not an identity
 * codec: native Wasm owns all protocol fingerprints and manifest hashes. */
export function sameCheckCandidate(a: Readonly<ReviewCheckCandidate>, b: Readonly<ReviewCheckCandidate>): boolean {
 const x = a.scope, y = b.scope;
 return x.account_id === y.account_id && x.view_version === y.view_version && x.policy_epoch === y.policy_epoch && x.change_id === y.change_id
  && x.patchset_id === y.patchset_id && x.candidate_id === y.candidate_id && x.target_head === y.target_head && x.target_generation === y.target_generation
  && x.publication_generation === y.publication_generation && a.head === b.head && a.root_tree === b.root_tree && a.profile.name === b.profile.name
  && a.profile.version === b.profile.version && a.expires_at === b.expires_at && a.preparer === b.preparer && a.view_name === b.view_name && a.safe_projection_id === b.safe_projection_id;
}
export class ForgeChecks {
 constructor(private readonly host: ForgeChecksHost) {
  for (const sql of [
   'CREATE TABLE IF NOT EXISTS check_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,rows INTEGER NOT NULL,bytes INTEGER NOT NULL)',
   'CREATE TABLE IF NOT EXISTS check_claims(account TEXT NOT NULL,principal TEXT NOT NULL,request TEXT NOT NULL,fingerprint TEXT NOT NULL,candidate TEXT,snapshot TEXT,lease TEXT,phase TEXT NOT NULL,PRIMARY KEY(principal,request))',
   'CREATE TABLE IF NOT EXISTS check_snapshots(id TEXT PRIMARY KEY,account TEXT NOT NULL,candidate TEXT NOT NULL,phase TEXT NOT NULL,generation INTEGER NOT NULL,hash_state TEXT,totals TEXT,hash_cursor INTEGER NOT NULL,metadata_bytes INTEGER NOT NULL,entries INTEGER NOT NULL,path_bytes INTEGER NOT NULL,file_bytes INTEGER NOT NULL,files INTEGER NOT NULL)',
   'CREATE TABLE IF NOT EXISTS check_frontier(snapshot TEXT NOT NULL,path TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,cursor INTEGER NOT NULL,value TEXT,PRIMARY KEY(snapshot,path,kind))',
   'CREATE TABLE IF NOT EXISTS check_entries(snapshot TEXT NOT NULL,path TEXT NOT NULL,value TEXT NOT NULL,ordinal INTEGER,PRIMARY KEY(snapshot,path),UNIQUE(snapshot,ordinal))',
   'CREATE TABLE IF NOT EXISTS check_members(snapshot TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,descriptor TEXT NOT NULL,PRIMARY KEY(snapshot,kind,id))',
   'CREATE TABLE IF NOT EXISTS check_leases(id TEXT PRIMARY KEY,account TEXT NOT NULL,principal TEXT NOT NULL,candidate TEXT NOT NULL,snapshot TEXT NOT NULL,value TEXT NOT NULL,binding TEXT NOT NULL,policy TEXT NOT NULL,phase TEXT NOT NULL,result_request TEXT,receipt TEXT,digest TEXT,accepted_until INTEGER)',
   'CREATE TABLE IF NOT EXISTS check_custody(candidate TEXT PRIMARY KEY,lease TEXT NOT NULL)',
   'CREATE TABLE IF NOT EXISTS check_mutations(principal TEXT NOT NULL,request TEXT NOT NULL,fingerprint TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(principal,request))',
  ]) host.sql.exec(sql);
  host.sql.exec('INSERT OR IGNORE INTO check_usage(singleton,account,rows,bytes) VALUES(1,?,0,0)', host.accountId);
 }
 private one<T>(sql: string, ...args: (string | number | null)[]): T | undefined { return [...this.host.sql.exec(sql, ...args)][0] as T | undefined; }
 private rows<T>(sql: string, ...args: (string | number | null)[]): T[] { return [...this.host.sql.exec(sql, ...args)] as T[]; }
 private now(): number { const n = this.host.nowSeconds(); if (!safe(n, Number.MAX_SAFE_INTEGER - 86400)) fail(503, 'unavailable'); return n; }
 private charge(rows: number, bytes: number): void {
  const u = this.one<{ account: string; rows: number; bytes: number }>('SELECT * FROM check_usage WHERE singleton=1');
  if (!u || u.account !== this.host.accountId || !safe(u.rows) || !safe(u.bytes)) fail(503, 'unavailable');
  if (!safe(rows) || !safe(bytes) || u.rows + rows > 262144 || u.bytes + bytes > 32 * 1048576) fail(429, 'resource_exhausted');
  this.host.sql.exec('UPDATE check_usage SET rows=rows+?,bytes=bytes+? WHERE singleton=1', rows, bytes);
 }
 private candidate(row: Snapshot | LeaseRow): ReviewCheckCandidate { const c = JSON.parse(row.candidate) as ReviewCheckCandidate; if (c.scope.account_id !== this.host.accountId) fail(503, 'unavailable'); return c; }
 private authorize(candidate: ReviewCheckCandidate): void {
  this.host.authorizeCheckerInTx(candidate.profile); this.host.recheckCandidateInTx(candidate);
  if (this.now() >= candidate.expires_at) fail(409, 'aborted');
  const policy = this.host.executionPolicy(candidate.profile); if (!policy || policy.profile.name !== candidate.profile.name || policy.profile.version !== candidate.profile.version) fail(409, 'aborted');
 }
 private snapshot(id: string): Snapshot { const row = this.one<Snapshot>('SELECT * FROM check_snapshots WHERE id=? AND account=?', id, this.host.accountId); if (!row) fail(404, 'not_found'); return row; }
 private claim(request: string): Claim | undefined { return this.one<Claim>('SELECT * FROM check_claims WHERE account=? AND principal=? AND request=?', this.host.accountId, this.host.principal, request); }
 private member(snapshot: string, d: Descriptor): void {
  const old = this.one<{ descriptor: string }>('SELECT descriptor FROM check_members WHERE snapshot=? AND kind=? AND id=?', snapshot, d.kind, d.id);
  if (old) { if (!same(JSON.parse(old.descriptor), d)) fail(503, 'unavailable'); return; }
  const value = text(d); this.charge(1, value.length + 128); this.host.sql.exec('INSERT INTO check_members(snapshot,kind,id,descriptor) VALUES(?,?,?,?)', snapshot, d.kind, d.id, value);
 }
 private entry(s: Snapshot, entry: ManifestEntry): void {
  if (this.one('SELECT 1 FROM check_entries WHERE snapshot=? AND path=?', s.id, entry.path)) fail(400, 'failed_precondition');
  const bytes = encoder.encode(entry.path).length, d = entry.descriptor;
  if (!safe(d.size_bytes) || d.format_version !== 1 || ![1, 2, 4].includes(d.kind)) fail(503, 'unavailable');
  if (s.entries >= LIMITS.max_entries || s.path_bytes + bytes > LIMITS.max_path_bytes || entry.path.split('/').length > LIMITS.max_depth
   || (d.kind === 1 && (d.size_bytes > LIMITS.max_file_bytes || s.files >= LIMITS.max_files || s.file_bytes + d.size_bytes > LIMITS.max_total_bytes))) fail(429, 'resource_exhausted');
  const value = text(entry); this.charge(1, value.length * 3 + 128); this.host.sql.exec('INSERT INTO check_entries(snapshot,path,value) VALUES(?,?,?)', s.id, entry.path, value);
  s.entries++; s.path_bytes += bytes; if (d.kind === 1) { s.files++; s.file_bytes += d.size_bytes; } this.member(s.id, d);
 }
 private save(s: Snapshot): void { s.generation++; this.host.sql.exec('UPDATE check_snapshots SET phase=?,generation=?,hash_state=?,totals=?,hash_cursor=?,metadata_bytes=?,entries=?,path_bytes=?,file_bytes=?,files=? WHERE id=?', s.phase, s.generation, s.hash_state, s.totals, s.hash_cursor, s.metadata_bytes, s.entries, s.path_bytes, s.file_bytes, s.files, s.id); }
 private enqueue(s: Snapshot, path: string, key: ObjectKey, value: ManifestEntry['value'] | null): void {
  if (encoder.encode(path).length > 4096 || path.split('/').length > LIMITS.max_depth) fail(429, 'resource_exhausted');
  const count = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM check_frontier WHERE snapshot=?', s.id)!.n;
  if (count >= LIMITS.max_entries) fail(429, 'resource_exhausted');
  this.charge(1, path.length * 3 + 256); this.host.sql.exec('INSERT INTO check_frontier(snapshot,path,kind,id,cursor,value) VALUES(?,?,?,?,0,?)', s.id, path, key.kind, key.id, value === null ? null : text(value));
 }
 private replyClaim(c: Claim): unknown {
  if (c.phase === 'idle') return { state: 'idle' };
  if (c.lease) { const row = this.leaseRow(c.lease); this.host.authorizeCheckerInTx(profile(JSON.parse(row.value))); return { state: 'leased', lease: JSON.parse(row.value) }; }
  if (!c.snapshot) fail(503, 'unavailable'); const s = this.snapshot(c.snapshot); this.authorize(this.candidate(s));
  return { state: 'preparing', request_id: c.request, manifest_id: s.id, generation: s.generation };
 }
 private begin(call: CheckerCall, command: Extract<CheckerCommand, { action: 'claim' }>, fingerprint: string): Claim {
  return this.host.transactionSync(() => {
   const p = { name: command.profile_name, version: command.profile_version }; this.host.authorizeCheckerInTx(p);
   if (!this.host.executionPolicy(p)) fail(400, 'failed_precondition');
   const old = this.claim(call.request_id); if (old) { if (old.fingerprint !== fingerprint) fail(409, 'already_exists'); return old; }
   if (this.one('SELECT 1 FROM check_mutations WHERE principal=? AND request=?', this.host.principal, call.request_id)) fail(409, 'already_exists');
   if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM check_claims')!.n >= 1024) fail(429, 'resource_exhausted');
   const candidates = this.host.eligibleCandidatesInTx(p, 32); if (candidates.length > 32) fail(503, 'unavailable');
   // Unleased preparation has no execution custody. Retire only bounded stale
   // drafts; never release an execution root because its wall clock elapsed.
   for (const pending of this.rows<Snapshot>("SELECT * FROM check_snapshots WHERE phase IN ('walk','hash') LIMIT 9")) {
    try { this.authorize(this.candidate(pending)); }
    catch (error) {
     if (!(error instanceof Failure) || ![400, 403, 409].includes(error.status)) throw error;
     pending.phase = 'obsolete'; this.save(pending);
    }
   }
   const c = candidates.find(c => !this.one('SELECT 1 FROM check_custody WHERE candidate=?', c.scope.candidate_id)
     && checkStatusInTx(this.host.sql, this.host.accountId, c, this.now()).state === 'checks_pending'
     && this.one<{ n: number }>('SELECT COUNT(*) AS n FROM check_leases WHERE account=? AND json_extract(candidate,\'$.scope.candidate_id\')=?', this.host.accountId, c.scope.candidate_id)!.n < 3);
   let snapshot: string | null = null;
   if (c) {
    this.authorize(c);
    if (this.one<{ n: number }>("SELECT COUNT(*) AS n FROM check_snapshots WHERE phase IN ('walk','hash')")!.n >= 8 || this.one<{ n: number }>('SELECT COUNT(*) AS n FROM check_snapshots')!.n >= 256) fail(429, 'resource_exhausted');
    snapshot = nonce(); const binding = text(c); this.charge(1, binding.length * 3 + 1024);
    this.host.sql.exec("INSERT INTO check_snapshots(id,account,candidate,phase,generation,hash_cursor,metadata_bytes,entries,path_bytes,file_bytes,files) VALUES(?,?,?,'walk',0,0,0,0,0,0,0)", snapshot, this.host.accountId, binding);
    this.enqueue(this.snapshot(snapshot), '', { kind: 3, id: c.head }, null);
   }
   this.charge(1, 1024); this.host.sql.exec('INSERT INTO check_claims(account,principal,request,fingerprint,candidate,snapshot,phase) VALUES(?,?,?,?,?,?,?)', this.host.accountId, this.host.principal, call.request_id, fingerprint, c ? text(c) : null, snapshot, c ? 'preparing' : 'idle'); return this.claim(call.request_id)!;
  });
 }
 private async advance(c: Claim): Promise<void> {
  if (!c.snapshot || c.lease) return;
  const selected = this.host.transactionSync(() => { const s = this.snapshot(c.snapshot!); this.authorize(this.candidate(s)); return { s, frontier: this.one<Frontier>('SELECT * FROM check_frontier WHERE snapshot=? ORDER BY kind DESC,path COLLATE BINARY LIMIT 1', s.id) }; });
  const { s, frontier } = selected;
  if (s.phase === 'walk' && frontier) {
   const d = this.host.transactionSync(() => { this.authorize(this.candidate(s)); const d = this.host.describeObjectInTx(frontier); if (!d || !safe(d.size_bytes, LIMITS.max_metadata_object_bytes) || ![2, 3, 4].includes(d.kind)) fail(503, 'unavailable'); return d; });
   let loaded: { descriptor: Descriptor; bytes: Uint8Array }; try { loaded = await this.host.readVerifiedObject(d); } catch (e) { this.host.transactionSync(() => this.authorize(this.candidate(s))); throw e; }
   this.host.transactionSync(() => {
    this.authorize(this.candidate(s)); const live = this.snapshot(s.id); if (live.generation !== s.generation || live.phase !== 'walk') fail(409, 'aborted');
    if (!same(d, loaded.descriptor) || loaded.bytes.length !== d.size_bytes || !same(inspect(d.kind, loaded.bytes), d)) fail(503, 'unavailable');
    if (frontier.cursor === 0) { if (s.metadata_bytes + d.size_bytes > LIMITS.max_metadata_bytes) fail(429, 'resource_exhausted'); s.metadata_bytes += d.size_bytes; this.member(s.id, d); }
    if (d.kind === 4) {
     const info = inspect(4, loaded.bytes) as unknown as Descriptor & { symlink: { target_bytes: number } };
     const value = JSON.parse(frontier.value!) as { kind: 'symlink'; target_bytes: number };
     if (info.symlink?.target_bytes !== value.target_bytes) fail(400, 'failed_precondition'); this.entry(s, { path: frontier.path, descriptor: d, value });
     this.host.sql.exec('DELETE FROM check_frontier WHERE snapshot=? AND path=? AND kind=?', s.id, frontier.path, d.kind);
    } else {
     const page = checkerSnapshotPage({ descriptor: d, bytes_base64: base64(loaded.bytes), path: frontier.path, cursor: frontier.cursor, limit: 128 }) as { descriptor: Descriptor; candidate_root_tree?: string; total: number; next_cursor: number | null; entries: { path: string; key: ObjectKey; value: ManifestEntry['value']; expected_size: number | null }[] };
     if (!same(page.descriptor, d) || page.entries.length > 128) fail(503, 'unavailable');
     if (d.kind === 3) {
      const candidate = this.candidate(s); if (d.id !== candidate.head || page.candidate_root_tree !== candidate.root_tree) fail(400, 'failed_precondition');
      this.enqueue(s, '', { kind: 2, id: candidate.root_tree }, { kind: 'directory' });
     } else {
      if (frontier.cursor === 0) this.entry(s, { path: frontier.path, descriptor: d, value: { kind: 'directory' } });
      for (const e of page.entries) {
       const child = this.host.describeObjectInTx(e.key); if (!child || child.kind !== e.key.kind || child.id !== e.key.id || child.format_version !== 1) fail(503, 'unavailable');
       if (child.kind === 1) { if (e.expected_size !== child.size_bytes || e.value.kind !== 'file') fail(400, 'failed_precondition'); this.entry(s, { path: e.path, descriptor: child, value: e.value }); }
       else if (child.kind === 2 || child.kind === 4) this.enqueue(s, e.path, child, e.value);
       else fail(400, 'failed_precondition');
      }
     }
     if (page.next_cursor === null) this.host.sql.exec('DELETE FROM check_frontier WHERE snapshot=? AND path=? AND kind=?', s.id, frontier.path, d.kind);
     else { if (!safe(page.next_cursor) || page.next_cursor <= frontier.cursor) fail(503, 'unavailable'); this.host.sql.exec('UPDATE check_frontier SET cursor=? WHERE snapshot=? AND path=? AND kind=?', page.next_cursor, s.id, frontier.path, d.kind); }
    }
    this.save(s);
   }); return;
  }
  this.host.transactionSync(() => {
   this.authorize(this.candidate(s)); const live = this.snapshot(s.id); if (live.generation !== s.generation || !['walk', 'hash', 'ready'].includes(s.phase)) fail(409, 'aborted');
   if (s.phase === 'walk') s.phase = 'hash';
   if (s.phase === 'hash') {
    const rows = this.rows<{ path: string; value: string }>('SELECT path,value FROM check_entries WHERE snapshot=? ORDER BY path COLLATE BINARY LIMIT 128 OFFSET ?', s.id, s.hash_cursor);
    const finish = s.hash_cursor + rows.length === s.entries;
    const candidate = this.candidate(s); const result = checkerManifestStep({ candidate_commit: candidate.head, root_tree: candidate.root_tree, state: s.hash_state === null ? null : JSON.parse(s.hash_state), entries: rows.map(row => JSON.parse(row.value)), limits: LIMITS, finish }) as { state: unknown; totals: ManifestTotals | null };
    rows.forEach((row, index) => this.host.sql.exec('UPDATE check_entries SET ordinal=? WHERE snapshot=? AND path=?', s.hash_cursor + index, s.id, row.path));
    s.hash_cursor += rows.length; s.hash_state = text(result.state); if (finish) { if (!result.totals || result.totals.entries !== s.entries) fail(503, 'unavailable'); s.totals = text(result.totals); s.phase = 'ready'; } this.save(s);
   }
   if (s.phase === 'ready') this.mint(c, s);
  });
 }
 private mint(c: Claim, s: Snapshot): void {
  const candidate = this.candidate(s); this.authorize(candidate); const old = this.claim(c.request)!; if (old.lease) return;
  if (this.one('SELECT 1 FROM check_custody WHERE candidate=?', candidate.scope.candidate_id) || checkStatusInTx(this.host.sql, this.host.accountId, candidate, this.now()).state !== 'checks_pending') fail(409, 'aborted');
  if (this.one<{ n: number }>("SELECT COUNT(*) AS n FROM check_leases WHERE account=? AND json_extract(candidate,'$.scope.candidate_id')=?", this.host.accountId, candidate.scope.candidate_id)!.n >= 3) fail(429, 'resource_exhausted');
  if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM check_custody')!.n >= 8 || this.one<{ n: number }>('SELECT COUNT(*) AS n FROM check_leases')!.n >= 1024) fail(429, 'resource_exhausted');
  const lease: CheckerLease = { schema: 1, lease_id: nonce(), generation: 0,
   request: { schema: 1, job_id: candidate.scope.candidate_id, attempt_id: nonce(), repository_id: `forge-${this.host.accountId}`, candidate_commit: candidate.head, root_tree: candidate.root_tree, profile_name: candidate.profile.name, profile_version: candidate.profile.version },
   scope: candidate.scope, manifest: { ...JSON.parse(s.totals!), id: s.id }, expires_at: Math.min(this.now() + 300, candidate.expires_at), limits: LIMITS };
  const binding = (checkerLeaseBinding(lease) as { binding: string }).binding;
  this.charge(2, 8192); this.host.retainCheckInTx(lease.lease_id, { kind: 3, id: candidate.head });
  this.host.sql.exec("INSERT INTO check_leases(id,account,principal,candidate,snapshot,value,binding,policy,phase) VALUES(?,?,?,?,?,?,?,?,'active')", lease.lease_id, this.host.accountId, this.host.principal, s.candidate, s.id, text(lease), binding, text(this.host.executionPolicy(candidate.profile)));
  this.host.sql.exec('INSERT INTO check_custody(candidate,lease) VALUES(?,?)', candidate.scope.candidate_id, lease.lease_id);
  this.host.sql.exec("UPDATE check_claims SET lease=?,phase='leased' WHERE principal=? AND request=?", lease.lease_id, this.host.principal, c.request);
 }
 private leaseRow(id: string): LeaseRow { const row = this.one<LeaseRow>('SELECT * FROM check_leases WHERE id=? AND account=? AND principal=?', id, this.host.accountId, this.host.principal); if (!row) fail(404, 'not_found'); return row; }
 private leased(ref: LeaseRef, live: boolean): { row: LeaseRow; lease: CheckerLease } {
  const row = this.leaseRow(ref.lease_id), lease = JSON.parse(row.value) as CheckerLease;
  // Native binding canonicalizes Request serialization and validates both IDs.
  const supplied = { ...lease, request: ref.request, lease_id: ref.lease_id };
  if ((checkerLeaseBinding(supplied) as { binding: string }).binding !== row.binding) fail(403, 'permission_denied');
  this.host.authorizeCheckerInTx(profile(lease));
  if (live) { this.authorize(this.candidate(row)); if (row.phase !== 'active' || this.now() >= lease.expires_at) fail(409, 'aborted'); const custody = this.one<{ lease: string }>('SELECT lease FROM check_custody WHERE candidate=?', lease.scope.candidate_id); if (custody?.lease !== row.id) fail(409, 'aborted'); }
  return { row, lease };
 }
 private replay(call: CheckerCall, fingerprint: string): unknown | undefined {
  const old = this.one<{ fingerprint: string; value: string }>('SELECT fingerprint,value FROM check_mutations WHERE principal=? AND request=?', this.host.principal, call.request_id);
  if (old) { if (old.fingerprint !== fingerprint) fail(409, 'already_exists'); return JSON.parse(old.value); }
  if (this.claim(call.request_id)) fail(409, 'already_exists'); return undefined;
 }
 private record(call: CheckerCall, fingerprint: string, value: unknown): unknown {
  if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM check_mutations')!.n >= 4096) fail(429, 'resource_exhausted');
  const encoded = text(value); this.charge(1, encoded.length * 3 + 256); this.host.sql.exec('INSERT INTO check_mutations(principal,request,fingerprint,value) VALUES(?,?,?,?)', this.host.principal, call.request_id, fingerprint, encoded); return value;
 }
 private completion(row: LeaseRow, lease: CheckerLease): Completion { if (!row.result_request || !row.digest || !row.receipt) fail(503, 'unavailable'); return { lease: reference(lease), result_request_id: row.result_request, receipt_digest: row.digest, outcome: (JSON.parse(row.receipt) as CheckReceipt).outcome }; }
 async handle(call: CheckerCall): Promise<unknown> {
  if (call.account_id !== this.host.accountId) fail(403, 'permission_denied');
  const c = call.command;
  const fingerprint = (checkerCommandFingerprint(call) as { fingerprint: string }).fingerprint;
  if (c.action === 'claim') { const row = this.begin(call, c, fingerprint); if (row.phase !== 'idle') await this.advance(row); return this.host.transactionSync(() => this.replyClaim(this.claim(call.request_id)!)); }
  if (c.action === 'get_claim') return this.host.transactionSync(() => { const row = this.claim(c.claim_id); if (!row) return { state: 'not_found' }; return this.replyClaim(row); });
  if (c.action === 'object') {
   const selected = this.host.transactionSync(() => {
    const { row, lease } = this.leased(c.lease, true);
    const member = this.one<{ descriptor: string }>('SELECT descriptor FROM check_members WHERE snapshot=? AND kind=? AND id=?', row.snapshot, c.key.kind, c.key.id);
    if (!member || ![1, 2, 3, 4].includes(c.key.kind)) fail(403, 'permission_denied');
    const d = JSON.parse(member.descriptor) as Descriptor; if (!safe(c.offset, d.size_bytes) || !safe(c.max_bytes, 65536)) fail(400, 'invalid_argument'); return { d, lease };
   });
   let loaded: { descriptor: Descriptor; bytes: Uint8Array }; try { loaded = await this.host.readVerifiedObject(selected.d); } catch (e) { this.host.transactionSync(() => this.leased(c.lease, true)); throw e; }
   return this.host.transactionSync(() => { this.leased(c.lease, true); if (!same(selected.d, loaded.descriptor) || loaded.bytes.length !== selected.d.size_bytes || !same(inspect(selected.d.kind, loaded.bytes), selected.d)) fail(503, 'unavailable'); const data = Array.from(loaded.bytes.subarray(c.offset, c.offset + c.max_bytes)); return { state: 'object', chunk: { lease: reference(selected.lease), descriptor: selected.d, offset: c.offset, data, eof: c.offset + data.length === selected.d.size_bytes } }; });
  }
  return this.host.transactionSync(() => {
   const { row, lease } = this.leased(c.lease, false);
   if (c.action === 'get_result') {
    if (row.result_request !== c.result_request_id || row.phase !== 'completed') return { state: 'not_found' };
    return { state: 'completed', completion: this.completion(row, lease) };
   }
   if (c.action === 'manifest') {
    this.leased(c.lease, true); if (!safe(c.cursor, lease.manifest.entries) || !safe(c.limit, 128) || c.limit === 0) fail(400, 'invalid_argument');
    const all = this.rows<{ ordinal: number; value: string }>('SELECT ordinal,value FROM check_entries WHERE snapshot=? AND ordinal>=? ORDER BY ordinal LIMIT ?', row.snapshot, c.cursor, c.limit);
    const entries: ManifestEntry[] = []; let bytes = 0; for (const e of all) { if (e.ordinal !== c.cursor + entries.length) fail(503, 'unavailable'); if (bytes + encoder.encode(e.value).length > 512 * 1024) break; entries.push(JSON.parse(e.value)); bytes += encoder.encode(e.value).length; }
    if (entries.length === 0 && c.cursor !== lease.manifest.entries) fail(503, 'unavailable'); const next = c.cursor + entries.length;
    return { state: 'manifest', page: { lease: reference(lease), manifest_id: lease.manifest.id, digest: lease.manifest.digest, start: c.cursor, entries, next: next === lease.manifest.entries ? null : next } };
   }
   const replay = this.replay(call, fingerprint); if (replay !== undefined) return replay;
   if (c.action === 'renew') {
    this.leased(c.lease, true); if (c.expected_generation !== lease.generation) fail(409, 'aborted');
    lease.generation++; lease.expires_at = Math.min(this.now() + 300, this.candidate(row).expires_at);
    this.host.sql.exec('UPDATE check_leases SET value=? WHERE id=?', text(lease), row.id); return this.record(call, fingerprint, { state: 'leased', lease });
   }
   if (c.action === 'complete' || c.action === 'release') {
    // Freeze execution evidence rules with custody: removal/replacement of the
    // current profile cannot prevent an old lease from confirming cleanup.
    const policy = JSON.parse(row.policy) as ExecutionPolicy;
    let digest: string; try { digest = (validateCheckerReceipt({ request: lease.request, receipt: c.receipt, policy }) as { receipt_digest: string }).receipt_digest; } catch { fail(400, 'invalid_argument'); }
    if (c.action === 'release') {
     if (!c.receipt.cleanup_confirmed || (c.receipt.outcome !== 'unavailable' && digest !== row.digest)) fail(400, 'failed_precondition');
     if (row.phase !== 'completed') this.host.sql.exec("UPDATE check_leases SET phase='released' WHERE id=?", row.id);
     this.closeCustody(row, lease); return this.record(call, fingerprint, { state: 'released', lease: reference(lease) });
    }
    this.leased(c.lease, true);
    const valid = Math.min(this.candidate(row).expires_at, this.now() + 900);
    this.host.sql.exec("UPDATE check_leases SET phase='completed',result_request=?,receipt=?,digest=?,accepted_until=? WHERE id=?", call.request_id, text(c.receipt), digest, valid, row.id);
    if (c.receipt.cleanup_confirmed) this.closeCustody(row, lease);
    const current = this.leaseRow(row.id); return this.record(call, fingerprint, { state: 'completed', completion: this.completion(current, lease) });
   }
   fail(400, 'invalid_argument');
  });
 }
 private closeCustody(row: LeaseRow, lease: CheckerLease): void {
  const custody = this.one<{ lease: string }>('SELECT lease FROM check_custody WHERE candidate=?', lease.scope.candidate_id);
  if (custody?.lease === row.id) { this.host.releaseCheckInTx(row.id, { kind: 3, id: lease.request.candidate_commit }); this.host.sql.exec('DELETE FROM check_custody WHERE candidate=? AND lease=?', lease.scope.candidate_id, row.id); }
 }
}
/** Trusted SQL-only review presentation/landing gate. It grants no checker
 * object reads and does not substitute for current member/candidate authority. */
export function checkStatusInTx(sql: SqlStorage, account: string, candidate: Readonly<ReviewCheckCandidate>, now: number): CheckStatus {
 const exists = [...sql.exec("SELECT 1 FROM sqlite_master WHERE type='table' AND name='check_leases'")][0]; if (!exists) return { state: 'checks_pending' };
 const rows = [...sql.exec("SELECT candidate,receipt,accepted_until FROM check_leases WHERE account=? AND phase='completed' AND json_extract(candidate,'$.scope.candidate_id')=? ORDER BY rowid DESC LIMIT 4", account, candidate.scope.candidate_id)] as unknown as { candidate: string; receipt: string; accepted_until: number }[];
 for (const row of rows) {
  if (!sameCheckCandidate(candidate, JSON.parse(row.candidate))) continue;
  const receipt = JSON.parse(row.receipt) as CheckReceipt; if (!receipt.cleanup_confirmed) continue;
  if (receipt.outcome === 'failed') return { state: 'failed' };
  if (receipt.outcome === 'passed' && safe(row.accepted_until) && now < row.accepted_until && now < candidate.expires_at) return { state: 'passed', valid_until: row.accepted_until };
 }
 return { state: 'checks_pending' };
}
