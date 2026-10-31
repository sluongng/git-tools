/** Typed cross-account dependency preparation. All methods are account-local:
 * only the trusted Worker coordinates peer calls after releasing both queues.
 * An armed export is custody, not a TTL lease. R2 visibility is availability,
 * never a published run, actor upload receipt or arbitrary object read grant. */
import { normalizeForgeTransformCommand as normalizeRaw, deriveForgeTransformIdentity as deriveRaw, mapForgeTransformSelection as mapRaw, buildForgeTransform as buildRaw, verifyForgeTransformLineage as lineageRaw, formatForgeTransform as formatRaw, inspect,
  type Descriptor, type ObjectKey, type ForgeCall } from './core';
import { ClosureJobs, type ClosureBinding } from './closure_jobs';
import type { ForgeViews, ResolvedView } from './forge_views';
import type { ForgeProjections } from './forge_projection';
import { fail, hash, Failure } from './protocol';
function native<T>(call: () => T): T {
 try { return call(); } catch (error) {
  if (error instanceof Failure) throw error;
  if (error instanceof Error && error.message.startsWith('resource_exhausted:')) fail(429, 'resource_exhausted');
  if (error instanceof Error && error.message.startsWith('invalid_argument:')) fail(400, 'invalid_argument');
  fail(503, 'unavailable');
 }
}
function normalizeForgeTransformCommand(input: Parameters<typeof normalizeRaw>[0]): TransformCommand { return native(() => normalizeRaw(input)); }
function deriveForgeTransformIdentity(input: unknown): unknown { return native(() => deriveRaw(input)); }
function mapForgeTransformSelection(input: unknown): unknown { return native(() => mapRaw(input)); }
function buildForgeTransform(input: unknown): unknown { return native(() => buildRaw(input)); }
function verifyForgeTransformLineage(input: unknown): unknown { return native(() => lineageRaw(input)); }
function formatForgeTransform(input: unknown): ReturnType<typeof formatRaw> { return native(() => formatRaw(input)); }
export interface TrustedAuth { principal: string; credential_id: string }
export interface TransformSpec { mappings: { source_prefix: string; destination_prefix: string }[] }
export interface TransformLineage { operation: 'rename' | 'copy'; source_path: string; target_path: string }
export interface TransformStart { source_account: string; source_view: string; source_before_view: string; source_after_view: string; destination_view: string | null; spec: TransformSpec; change_id: string; lineage: TransformLineage[]; expected_patchset: string | null }
export type TransformCommand = { operation: 30 | 32; fingerprint: string; payload: TransformStart }
 | { operation: 31 | 34 | 35; fingerprint: string | null; payload: { run_id: string } }
 | { operation: 33; fingerprint: null; payload: { run_id: string; path: string | null; offset: number | null; max_bytes: number | null; after_path: string | null; limit: number | null } };
export interface TransformDraftBinding { draft_id: string; destination_account: string; principal: string; destination_view: string; destination_view_name: string; destination_epoch: number; destination_base: string; destination_root: string; request_fingerprint: string; spec_id: string; change_id: string }
export interface SourceExportBinding { export_id: string; draft: TransformDraftBinding; source_account: string; source_view: string; before_view: string; after_view: string; before_revision: string; after_revision: string; source_epoch: number; manifest_digest: string }
export interface PreparedTransform { binding: SourceExportBinding; run_id: string; plan_digest: string; artifact: string; output_head: string; artifact_proof: string; output_proof: string; prepared_generation: number }
export interface SourcePermit { permit_id: string; export_id: string; prepared: PreparedTransform }
export type DestinationOutcome = { draft_id: string; export_id: string; state: 'pending' } | { draft_id: string; export_id: string; state: 'committed'; run_id: string; permit_id: string; publication_digest: string } | { draft_id: string; export_id: string; state: 'aborted' };
interface VerifiedLineage extends TransformLineage { source_canonical_path: string; target_canonical_path: string }
interface SourceRow { path: string; canonical_path: string; descriptor: Descriptor; executable: boolean; copy_id: string | null }
interface SelectionRow extends SourceRow { side: 'before' | 'after'; ordinal: number }
interface ExportPage { binding: SourceExportBinding; cursor: number; entries: SelectionRow[]; next: number | null; lineage: unknown[] }
interface ExportFile { binding: SourceExportBinding; side: 'before' | 'after'; path: string; descriptor: Descriptor; bytes: Uint8Array }
export type PeerEvidence = { kind: 'export_pending'; export_id: string; generation: number } | { kind: 'export_ready'; binding: SourceExportBinding }
 | { kind: 'export_page'; page: ExportPage } | { kind: 'export_file'; file: ExportFile } | { kind: 'permit'; permit: SourcePermit }
 | { kind: 'outcome'; outcome: DestinationOutcome } | { kind: 'settled'; export_id: string } | { kind: 'origin'; binding: SourceExportBinding; available: boolean };
export type TransformRpcCommand = { action: 'begin_transform'; call: ForgeCall }
 | { action: 'advance_transform'; job_id: string; expected_generation: number }
 | { action: 'accept_peer'; job_id: string; expected_generation: number; evidence: PeerEvidence }
 | { action: 'begin_export'; draft: TransformDraftBinding; payload: TransformStart }
 | { action: 'export_page'; export_id: string; cursor: number }
 | { action: 'export_file'; export_id: string; side: 'before' | 'after'; path: string }
 | { action: 'arm_export'; export_id: string; prepared: PreparedTransform }
 | { action: 'transform_outcome' | 'abort_transform'; job_id: string; export_id: string }
 | { action: 'settle_export'; export_id: string; outcome: DestinationOutcome }
 | { action: 'authorize_origin'; binding: SourceExportBinding }
 | { action: 'recovery_next' };
export interface NextAction { account_id: string; command: TransformRpcCommand; continuation: { job_id: string; expected_generation: number } }
export interface TransformRpcResult { result?: { state: string; result: unknown }; next?: NextAction; evidence?: PeerEvidence }
export interface TransformReviewSource { run_id: string; account_id: string; destination_view: string; destination_view_name: string; destination_base: string; output_head: string }
export interface ForgeTransformsHost {
 accountId: string; principal: string | null; auth: Readonly<TrustedAuth> | null; sql: SqlStorage; views: ForgeViews; projections: ForgeProjections;
 transactionSync<T>(callback: () => T): T; nowSeconds(): number;
 /** Fresh EXACT current credential row and account ceiling, never aggregation
  * of all credentials sharing a principal. Null auth is cleanup-only. */
 requireCredentialInTx(): void;
 authorizeInTx(action: 'read' | 'write' | 'admin', expectedEpoch?: number): { epoch: number };
 reviewConfiguration(): { profile: { name: string; version: string }; committer: { name: string; email: string } } | null;
 describeObjectInTx(key: ObjectKey): Descriptor | undefined;
 readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
 reserveGeneratedInTx(ownerId: string, descriptor: Descriptor): void;
 stageGenerated(descriptor: Descriptor, bytes: Uint8Array): Promise<unknown>;
 confirmGeneratedInTx(ownerId: string, descriptor: Descriptor, confirmation: unknown): void;
 requireRetainedClosureInTx(root: ObjectKey): { proof_job: string };
 retainRootInTx(ownerKind: string, ownerId: string, root: ObjectKey, proofJob: string): void;
 releaseRootInTx(ownerKind: string, ownerId: string, root: ObjectKey): void;
 pushTransformReview(source: Readonly<TransformReviewSource>, expectedPatchset: string | null): Promise<{ state: string; result: unknown }>;
}
interface Person { name: string; email: string; millis: number; offset: number }
interface Draft { id: string; account: string; principal: string; request_id: string; fingerprint: string; payload: string; binding: string; committer: string; profile: string; phase: string; generation: number; deadline: number; export_id: string | null; source: string | null; page_cursor: number; page_done: number; lineage: string; metadata_bytes: number; plan_hash: string | null; plan_bytes: number; output_cursor: number; artifact_job: string | null; output_job: string | null; permit: string | null; run_id: string | null; publication_digest: string | null; settled: number }
interface Export { id: string; account: string; principal: string; draft: string; payload: string; epoch: number; before_revision: string; after_revision: string; phase: string; generation: number; deadline: number; manifest: string | null; selected: number; path_bytes: number; file_bytes: number; lineage_cursor: number; lineage: string; permit: string | null }
interface NativePin { id: number[]; artifact: number[]; reference: string; generation: number; retired: boolean; protected_roots: string[] }
interface NativeRun { id: number[]; spec: number[]; source_account: string; source_before: number[]; source_after: number[]; source_before_view: number[]; source_after_view: number[]; destination_account: string; destination_view: number[]; destination_base: number[]; output_head: number[]; lineage: unknown[]; destination_paths: string[]; pin: NativePin }
interface RunRow { id: string; account: string; principal: string; draft: string; binding: string; value: string; artifact_proof: string; output_proof: string; generation: number; retired: number }
interface Generated extends Descriptor { bytes_base64: string }
interface Plan { state: 'ready'; run: NativeRun; objects: Generated[] }
interface Pack { kind: number; id: string; size: number | null; data: ArrayBuffer | null }
const encoder = new TextEncoder(), decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });
const MAX_ROWS = 262144, MAX_LEDGER = 32 * 1048576, MAX_FILES = 1024, MAX_PATHS = 1048576, MAX_INPUT = 64 * 1048576, MAX_META = 256 * 1024, MAX_PACK = 4 * 1048576, CHUNK = 65536;
function safe(n: number, max = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(n) && n >= 0 && n <= max; }
function id(n: string, len = 64): boolean { return typeof n === 'string' && n.length === len && /^[0-9a-f]+$/.test(n); }
function hex(bytes: number[], size = 32): string { if (!Array.isArray(bytes) || bytes.length !== size || !bytes.every(n => safe(n, 255))) fail(503, 'unavailable'); return bytes.map(b => b.toString(16).padStart(2, '0')).join(''); }
function array(value: string): number[] { if (!id(value)) fail(400, 'invalid_argument'); return Array.from(value.match(/../g)!, v => parseInt(v, 16)); }
function nonce(): string { return Array.from(crypto.getRandomValues(new Uint8Array(32)), n => n.toString(16).padStart(2, '0')).join(''); }
function encoded(value: unknown, cap = 65536): string { const s = JSON.stringify(value); if (encoder.encode(s).length > cap) fail(429, 'resource_exhausted'); return s; }
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function base64(bytes: Uint8Array): string { let s = ''; for (let i = 0; i < bytes.length; i += 8192) s += String.fromCharCode(...bytes.subarray(i, i + 8192)); return btoa(s); }
function bytes(row: Generated): Uint8Array { let raw: string; try { raw = atob(row.bytes_base64); } catch { fail(503, 'unavailable'); } const b = Uint8Array.from(raw, c => c.charCodeAt(0)); if (b.length !== row.size_bytes || base64(b) !== row.bytes_base64 || !same(inspect(row.kind, b), row)) fail(503, 'unavailable'); return b; }
/** Source epoch mutation guard. No wall-clock test: an armed publication can
 * have committed despite a lost ACK, so only terminal reconciliation clears it. */
export function requireNoArmedExportInTx(sql: SqlStorage, account: string): void {
 if (![...sql.exec("SELECT 1 FROM sqlite_master WHERE type='table' AND name='transform_exports'")].length) return;
 if ([...sql.exec("SELECT 1 FROM transform_exports WHERE account=? AND phase='armed' LIMIT 1", account)].length) fail(409, 'aborted');
}
export class ForgeTransforms {
 constructor(private readonly host: ForgeTransformsHost) {
  for (const statement of [
   'CREATE TABLE IF NOT EXISTS transform_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,rows INTEGER NOT NULL,bytes INTEGER NOT NULL)',
   'CREATE TABLE IF NOT EXISTS transform_drafts(id TEXT PRIMARY KEY,account TEXT NOT NULL,principal TEXT NOT NULL,request_id TEXT NOT NULL,fingerprint TEXT NOT NULL,payload TEXT NOT NULL,binding TEXT NOT NULL,committer TEXT NOT NULL,profile TEXT NOT NULL,phase TEXT NOT NULL,generation INTEGER NOT NULL,deadline INTEGER NOT NULL,export_id TEXT,source TEXT,page_cursor INTEGER NOT NULL,page_done INTEGER NOT NULL,lineage TEXT NOT NULL,metadata_bytes INTEGER NOT NULL,plan_hash TEXT,plan_bytes INTEGER NOT NULL,output_cursor INTEGER NOT NULL,artifact_job TEXT,output_job TEXT,permit TEXT,run_id TEXT,publication_digest TEXT,settled INTEGER NOT NULL DEFAULT 0,UNIQUE(account,principal,request_id))',
   'CREATE TABLE IF NOT EXISTS transform_exports(id TEXT PRIMARY KEY,account TEXT NOT NULL,principal TEXT NOT NULL,draft TEXT NOT NULL,payload TEXT NOT NULL,epoch INTEGER NOT NULL,before_revision TEXT NOT NULL,after_revision TEXT NOT NULL,phase TEXT NOT NULL,generation INTEGER NOT NULL,deadline INTEGER NOT NULL,manifest TEXT,selected INTEGER NOT NULL,path_bytes INTEGER NOT NULL,file_bytes INTEGER NOT NULL,lineage_cursor INTEGER NOT NULL,lineage TEXT NOT NULL,permit TEXT,UNIQUE(account,draft))',
   'CREATE TABLE IF NOT EXISTS transform_selection(owner TEXT NOT NULL,side TEXT NOT NULL,path TEXT NOT NULL,ordinal INTEGER NOT NULL,value TEXT NOT NULL,ready INTEGER NOT NULL,PRIMARY KEY(owner,side,path),UNIQUE(owner,ordinal))',
   'CREATE TABLE IF NOT EXISTS transform_pack(owner TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER,data BLOB,PRIMARY KEY(owner,kind,id))',
   'CREATE TABLE IF NOT EXISTS transform_plan(owner TEXT NOT NULL,ordinal INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(owner,ordinal))',
   'CREATE TABLE IF NOT EXISTS transform_runs(id TEXT PRIMARY KEY,account TEXT NOT NULL,principal TEXT NOT NULL,draft TEXT NOT NULL,binding TEXT NOT NULL,value TEXT NOT NULL,artifact_proof TEXT NOT NULL,output_proof TEXT NOT NULL,generation INTEGER NOT NULL,retired INTEGER NOT NULL)',
   'CREATE TABLE IF NOT EXISTS transform_pin_owners(run_id TEXT NOT NULL,kind TEXT NOT NULL,owner TEXT NOT NULL,PRIMARY KEY(run_id,kind,owner))',
   'CREATE TABLE IF NOT EXISTS transform_receipts(account TEXT NOT NULL,principal TEXT NOT NULL,request_id TEXT NOT NULL,fingerprint TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(account,principal,request_id))',
  ]) host.sql.exec(statement);
  host.sql.exec('INSERT OR IGNORE INTO transform_usage(singleton,account,rows,bytes) VALUES(1,?,0,0)', host.accountId);
 }
 private one<T>(sql: string, ...args: (string | number | null | ArrayBuffer)[]): T | undefined { return [...this.host.sql.exec(sql, ...args)][0] as T | undefined; }
 private rows<T>(sql: string, ...args: (string | number | null | ArrayBuffer)[]): T[] { return [...this.host.sql.exec(sql, ...args)] as T[]; }
 private now(): number { const n = this.host.nowSeconds(); if (!safe(n, Math.floor(Number.MAX_SAFE_INTEGER / 1000) - 86400)) fail(503, 'unavailable'); return n; }
 private actor(): string { if (!this.host.auth || this.host.principal !== this.host.auth.principal) fail(401, 'unauthenticated'); this.host.requireCredentialInTx(); return this.host.auth.principal; }
 private charge(rows: number, bytes: number): void { const u = this.one<{ account: string; rows: number; bytes: number }>('SELECT * FROM transform_usage WHERE singleton=1'); if (!u || u.account !== this.host.accountId || !safe(u.rows) || !safe(u.bytes)) fail(503, 'unavailable'); if (!safe(rows) || !safe(bytes) || u.rows + rows > MAX_ROWS || u.bytes + bytes > MAX_LEDGER) fail(429, 'resource_exhausted'); this.host.sql.exec('UPDATE transform_usage SET rows=rows+?,bytes=bytes+? WHERE singleton=1', rows, bytes); }
 private draft(job: string): Draft { const d = this.one<Draft>('SELECT * FROM transform_drafts WHERE id=? AND account=?', job, this.host.accountId); if (!d) fail(404, 'not_found'); return d; }
 private export(exportId: string): Export { const e = this.one<Export>('SELECT * FROM transform_exports WHERE id=? AND account=?', exportId, this.host.accountId); if (!e) fail(404, 'not_found'); return e; }
 private saved(d: Draft): TransformDraftBinding { return JSON.parse(d.binding) as TransformDraftBinding; }
 private source(e: Export): SourceExportBinding { const b = JSON.parse(e.draft) as TransformDraftBinding, p = JSON.parse(e.payload) as TransformStart; if (!e.manifest) fail(400, 'failed_precondition'); return { export_id: e.id, draft: b, source_account: e.account, source_view: p.source_view, before_view: p.source_before_view, after_view: p.source_after_view, before_revision: e.before_revision, after_revision: e.after_revision, source_epoch: e.epoch, manifest_digest: e.manifest }; }
 private destination(d: Draft, write = true): ResolvedView {
  const live = this.draft(d.id); if (live.generation !== d.generation || live.phase !== d.phase || live.binding !== d.binding || live.principal !== d.principal) fail(409, 'aborted');
  if (write && this.actor() !== d.principal) fail(403, 'permission_denied'); if (!write) this.actor();
  const b = this.saved(d); this.host.authorizeInTx(write ? 'write' : 'read', write ? b.destination_epoch : undefined);
  const view = this.host.views.resolveInTx(b.destination_view_name, array(b.destination_view)); this.host.views.checkInTx(view);
  if (hex(view.description.revision) !== b.destination_base || (write && view.description.policy_epoch !== b.destination_epoch)) fail(409, 'aborted');
  if (write && (['aborted', 'limit'].includes(d.phase) || (d.phase !== 'committed' && this.now() >= d.deadline))) fail(409, 'aborted'); return view;
 }
 private sourceViews(e: Export): { before: ResolvedView; after: ResolvedView } {
  const live = this.export(e.id); if (live.generation !== e.generation || live.phase !== e.phase || live.draft !== e.draft || live.epoch !== e.epoch) fail(409, 'aborted');
  if (this.actor() !== e.principal || !['exporting', 'lineage', 'ready', 'armed'].includes(e.phase)) fail(403, 'permission_denied');
  if (e.phase !== 'armed' && this.now() >= e.deadline) fail(409, 'aborted');
  const p = JSON.parse(e.payload) as TransformStart;
  const before = this.host.views.resolveInTx(p.source_view, array(p.source_before_view)), after = this.host.views.resolveInTx(p.source_view, array(p.source_after_view));
  for (const v of [before, after]) { this.host.views.checkInTx(v); const policy = this.host.views.policyForProjectionInTx(v); if (v.description.policy_epoch !== e.epoch || !policy.selected_spec.export_allowed || !policy.current_spec.export_allowed) fail(403, 'permission_denied'); }
  if (hex(before.description.revision) !== e.before_revision || hex(after.description.revision) !== e.after_revision) fail(409, 'aborted'); return { before, after };
 }
 private saveDraft(d: Draft): void { d.generation++; this.host.sql.exec('UPDATE transform_drafts SET phase=?,generation=?,export_id=?,source=?,page_cursor=?,page_done=?,lineage=?,metadata_bytes=?,plan_hash=?,plan_bytes=?,output_cursor=?,artifact_job=?,output_job=?,permit=?,run_id=?,publication_digest=?,settled=? WHERE id=?', d.phase, d.generation, d.export_id, d.source, d.page_cursor, d.page_done, d.lineage, d.metadata_bytes, d.plan_hash, d.plan_bytes, d.output_cursor, d.artifact_job, d.output_job, d.permit, d.run_id, d.publication_digest, d.settled, d.id); }
 private saveExport(e: Export): void { e.generation++; this.host.sql.exec('UPDATE transform_exports SET phase=?,generation=?,manifest=?,selected=?,path_bytes=?,file_bytes=?,lineage_cursor=?,lineage=?,permit=? WHERE id=?', e.phase, e.generation, e.manifest, e.selected, e.path_bytes, e.file_bytes, e.lineage_cursor, e.lineage, e.permit, e.id); }
 private map(p: TransformStart, root: string, rows: SourceRow[]): Mapped[] {
  let state: unknown = null; const mapped: Mapped[] = [];
  for (let n = 0; n < rows.length; n += 128) {
   const result = mapForgeTransformSelection({ spec: p.spec, destination_root: root, state, rows: rows.slice(n, n + 128).map(r => ({ relative_path: r.path, canonical_path: r.canonical_path, descriptor: r.descriptor, executable: r.executable, copy_id: r.copy_id })) }) as { files: Mapped[]; state: unknown };
   if (!Array.isArray(result.files) || result.files.length > 128) fail(503, 'unavailable'); mapped.push(...result.files); state = result.state;
  }
  return mapped;
 }
 private selection(owner: string, side?: 'before' | 'after'): SelectionRow[] {
  const rows = side ? this.rows<{ value: string }>('SELECT value FROM transform_selection WHERE owner=? AND side=? ORDER BY ordinal LIMIT 1025', owner, side)
   : this.rows<{ value: string }>('SELECT value FROM transform_selection WHERE owner=? ORDER BY ordinal LIMIT 2049', owner);
  if (rows.length > (side ? MAX_FILES : MAX_FILES * 2)) fail(429, 'resource_exhausted'); return rows.map(r => JSON.parse(r.value));
 }
 private destinationPaths(view: ResolvedView, paths: string[]): void {
  for (let n = 0; n < paths.length; n += 128) { const batch = paths.slice(n, n + 128), actual = this.host.views.pathsAllowedInTx(view, batch).paths;
   if (actual.length !== batch.length || actual.some((r, i) => r.path !== batch[i] || !r.allowed)) fail(403, 'permission_denied'); }
 }
 private pending(d: Draft): TransformRpcResult {
  const b = this.saved(d), p = JSON.parse(d.payload) as TransformStart;
  return { result: { state: 'transform_pending', result: { schema_version: 1, request_id: d.request_id, intent_fingerprint: d.fingerprint,
   source_before_view: p.source_before_view, source_after_view: p.source_after_view, change_id: p.change_id, destination_view: b.destination_view,
   destination_base: b.destination_base, job_id: d.id, generation: d.generation, phase: d.phase === 'committed' ? 'review_admission' : 'prepare' } } };
 }
 private next(d: Draft, account: string, command: TransformRpcCommand): TransformRpcResult {
  return { ...this.pending(d), next: { account_id: account, command, continuation: { job_id: d.id, expected_generation: d.generation } } };
 }
 private identity(d: Draft): unknown {
  if (!d.source) fail(400, 'failed_precondition'); const b = this.saved(d), e = JSON.parse(d.source) as SourceExportBinding, p = JSON.parse(d.payload) as TransformStart;
  return { spec: array(b.spec_id), source_account: e.source_account, before: array(e.before_revision), after: array(e.after_revision), before_view: array(e.before_view), after_view: array(e.after_view),
   before_view_name: e.source_view, after_view_name: e.source_view, destination_account: b.destination_account, destination_view: array(b.destination_view), destination_view_name: b.destination_view_name,
   destination_base: array(b.destination_base), change_id: Array.from(p.change_id.match(/../g)!, h => parseInt(h, 16)), lineage: p.lineage };
 }
 private begin(call: ForgeCall): Draft {
  let command: TransformCommand; try { command = normalizeForgeTransformCommand({ account_id: call.account_id, operation: call.operation, view: call.view, request_id: call.request_id, payload_json: call.payload_json }) as TransformCommand; } catch (error) { if (error instanceof Failure) throw error; fail(400, 'invalid_argument'); }
  if (call.account_id !== this.host.accountId || ![30, 32].includes(command.operation) || !command.fingerprint) fail(400, 'invalid_argument');
  const p = command.payload as TransformStart;
  const fingerprint = command.fingerprint;
  if (!fingerprint) fail(400, 'invalid_argument');
  return this.host.transactionSync(() => {
   const principal = this.actor(); this.host.authorizeInTx('write');
   const old = this.one<Draft>('SELECT * FROM transform_drafts WHERE account=? AND principal=? AND request_id=?', this.host.accountId, principal, call.request_id);
   if (old) { if (old.fingerprint !== command.fingerprint) fail(409, 'already_exists'); this.destination(old); return old; }
   if (this.one('SELECT 1 FROM transform_receipts WHERE account=? AND principal=? AND request_id=?', this.host.accountId, principal, call.request_id)) fail(409, 'already_exists');
   if (p.source_account === this.host.accountId || !id(p.source_before_view) || !id(p.source_after_view) || !id(p.change_id, 32)) fail(400, 'invalid_argument');
   if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM transform_drafts')!.n >= 4096 || this.one<{ n: number }>("SELECT COUNT(*) AS n FROM transform_drafts WHERE phase NOT IN ('committed','aborted','limit')")!.n >= 8) fail(429, 'resource_exhausted');
   const config = this.host.reviewConfiguration(); if (!config) fail(400, 'failed_precondition');
   const view = this.host.views.resolveInTx(call.view, p.destination_view === null ? undefined : array(p.destination_view)); this.host.views.checkInTx(view);
   const specId = (deriveForgeTransformIdentity({ kind: 'spec', spec: p.spec }) as { spec_id: string }).spec_id; if (!id(specId)) fail(503, 'unavailable');
   const d = view.description, binding: TransformDraftBinding = { draft_id: nonce(), destination_account: this.host.accountId, principal,
    destination_view: hex(d.view_version), destination_view_name: d.view, destination_epoch: d.policy_epoch, destination_base: hex(d.revision), destination_root: d.root_prefix,
    request_fingerprint: fingerprint, spec_id: specId, change_id: p.change_id };
   const payload = encoded(p, 1048576), frozen = encoded({ name: config.committer.name, email: config.committer.email, millis: this.now() * 1000, offset: 0 });
   this.charge(1, encoder.encode(payload + frozen + encoded(binding)).length + 2048);
   this.host.sql.exec("INSERT INTO transform_drafts(id,account,principal,request_id,fingerprint,payload,binding,committer,profile,phase,generation,deadline,page_cursor,page_done,lineage,metadata_bytes,plan_bytes,output_cursor) VALUES(?,?,?,?,?,?,?,?,?,'export',0,?,0,0,'[]',0,0,0)", binding.draft_id, this.host.accountId, principal, call.request_id, command.fingerprint, payload, encoded(binding), frozen, encoded(config.profile), this.now() + 900);
   this.enqueue(binding.draft_id, { kind: 3, id: binding.destination_base }); return this.draft(binding.draft_id);
  });
 }
 private async beginExport(draft: TransformDraftBinding, p: TransformStart): Promise<PeerEvidence> {
  let e = this.host.transactionSync(() => {
   const principal = this.actor(); if (p.lineage.length > 4096 || new Set(p.lineage.map(h => h.target_path)).size !== p.lineage.length) fail(400, 'invalid_argument'); if (principal !== draft.principal || p.source_account !== this.host.accountId || draft.destination_account === this.host.accountId) fail(403, 'permission_denied');
   const old = this.one<Export>('SELECT * FROM transform_exports WHERE account=? AND draft=?', this.host.accountId, encoded(draft));
   if (old) { if (old.principal !== principal || old.payload !== encoded(p, 1048576)) fail(409, 'already_exists'); this.sourceViews(old); return old; }
   if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM transform_exports')!.n >= 4096 || this.one<{ n: number }>("SELECT COUNT(*) AS n FROM transform_exports WHERE phase IN ('exporting','lineage','ready','armed','cancelling')")!.n >= 8) fail(429, 'resource_exhausted');
   const before = this.host.views.resolveInTx(p.source_view, array(p.source_before_view)), after = this.host.views.resolveInTx(p.source_view, array(p.source_after_view));
   if (before.description.policy_epoch !== after.description.policy_epoch) fail(409, 'aborted');
   const exportId = nonce(); this.charge(1, encoded(draft).length + encoded(p, 1048576).length + 2048);
   this.host.sql.exec("INSERT INTO transform_exports(id,account,principal,draft,payload,epoch,before_revision,after_revision,phase,generation,deadline,selected,path_bytes,file_bytes,lineage_cursor,lineage) VALUES(?,?,?,?,?,?,?,?,'exporting',0,?,0,0,0,0,'[]')", exportId, this.host.accountId, principal, encoded(draft), encoded(p, 1048576), before.description.policy_epoch, hex(before.description.revision), hex(after.description.revision), this.now() + 900);
   const created = this.export(exportId); this.sourceViews(created); return created;
  });
  if (e.phase === 'ready' || e.phase === 'armed') return { kind: 'export_ready', binding: this.host.transactionSync(() => { this.sourceViews(e); return this.source(e); }) };
  const views = this.host.transactionSync(() => this.sourceViews(e));
  for (const view of [views.before, views.after]) {
   const ready = this.host.transactionSync(() => { this.sourceViews(e); return this.host.projections.readyInTx(view); });
   if (!ready) {
    const status = this.host.projections.start(view); if (status.state === 'pending') await this.host.projections.advance(view, { job_id: status.job_id, expected_generation: status.generation });
    this.host.transactionSync(() => this.sourceViews(e)); return { kind: 'export_pending', export_id: e.id, generation: e.generation };
   }
  }
  if (e.phase === 'exporting') this.host.transactionSync(() => {
   this.sourceViews(e); const current = this.export(e.id); if (current.generation !== e.generation) fail(409, 'aborted');
   const b = JSON.parse(e.draft) as TransformDraftBinding; let ordinal = 0;
   for (const [side, view] of [['before', views.before], ['after', views.after]] as const) {
    const paths = this.host.projections.pathsInTx(view); if (paths.length > MAX_FILES) fail(429, 'resource_exhausted');
    const rows: SourceRow[] = paths.map(f => ({ path: f.path, canonical_path: f.canonical_path, descriptor: f.descriptor, executable: f.executable,
     copy_id: f.descriptor.kind === 1 ? this.host.projections.canonicalFileWitnessInTx(view, f.path)?.canonical_copy ?? null : null }));
    const mapped = this.map(p, b.destination_root, rows);
    let sideBytes = 0;
    for (const file of mapped) {
     const row: SelectionRow = { side, ordinal, path: file.source_relative_path, canonical_path: file.source_canonical_path, descriptor: file.descriptor, executable: file.executable, copy_id: file.copy_id };
     if (file.descriptor.kind !== 1 || file.descriptor.size_bytes > 1048576) fail(429, 'resource_exhausted');
     sideBytes += file.descriptor.size_bytes; e.path_bytes += encoder.encode(row.path + row.canonical_path).length;
     if (sideBytes > MAX_INPUT || e.path_bytes > MAX_PATHS) fail(429, 'resource_exhausted');
     const value = encoded(row); this.charge(1, value.length * 3 + 128); this.host.sql.exec('INSERT INTO transform_selection(owner,side,path,ordinal,value,ready) VALUES(?,?,?,?,?,1)', e.id, side, row.path, ordinal++, value);
    }
    e.file_bytes += sideBytes;
   }
   // Empty selections are represented explicitly; phase, not count, controls
   // initialization so empty transforms cannot insert duplicate rows on retry.
   e.selected = ordinal; e.phase = 'lineage'; this.saveExport(e);
  });
  e = this.host.transactionSync(() => this.export(e.id));
  if (e.lineage_cursor < p.lineage.length) {
   const hint = p.lineage[e.lineage_cursor];
   const witnesses = this.host.transactionSync(() => {
    const v = this.sourceViews(e); const source = this.host.projections.canonicalFileWitnessInTx(v.before, hint.source_path), target = this.host.projections.canonicalFileWitnessInTx(v.after, hint.target_path);
    if (!source || !target || !target.canonical_copy) fail(400, 'invalid_argument');
    return { source, target, target_existed_before: this.host.projections.pathsInTx(v.before).some(f => f.path === hint.target_path), source_exists_after: this.host.projections.pathsInTx(v.after).some(f => f.path === hint.source_path) };
   });
   let object: { descriptor: Descriptor; bytes: Uint8Array };
   try { object = await this.host.readVerifiedObject({ kind: 5, id: witnesses.target.canonical_copy! }); }
   catch (error) { this.host.transactionSync(() => this.sourceViews(e)); throw error; }
   this.host.transactionSync(() => {
    this.sourceViews(e); if (this.export(e.id).generation !== e.generation) fail(409, 'aborted');
    const result = verifyForgeTransformLineage({ relation: { operation: hint.operation, source_path: hint.source_path, target_path: hint.target_path,
     source: { canonical_path: witnesses.source.canonical_path, file: witnesses.source.file, canonical_copy: witnesses.source.canonical_copy },
     target: { canonical_path: witnesses.target.canonical_path, file: witnesses.target.file, canonical_copy: witnesses.target.canonical_copy },
     target_existed_before: witnesses.target_existed_before, source_exists_after: witnesses.source_exists_after }, target_copy: { ...object.descriptor, bytes_base64: base64(object.bytes) } }) as { verified: boolean };
    if (!result.verified) fail(400, 'invalid_argument'); const lineage = JSON.parse(e.lineage) as VerifiedLineage[]; lineage.push({ ...hint, source_canonical_path: witnesses.source.canonical_path, target_canonical_path: witnesses.target.canonical_path }); e.lineage = encoded(lineage, 1048576); e.lineage_cursor++; this.saveExport(e);
   }); return { kind: 'export_pending', export_id: e.id, generation: e.generation };
  }
  const manifestInput = this.host.transactionSync(() => { this.sourceViews(e); return encoded({ draft, source_account: e.account, before: e.before_revision, after: e.after_revision, epoch: e.epoch, files: this.selection(e.id), lineage: JSON.parse(e.lineage) }, MAX_PACK); });
  const manifest = await hash(manifestInput);
  return this.host.transactionSync(() => { this.sourceViews(e); if (this.export(e.id).generation !== e.generation) fail(409, 'aborted'); e.manifest = manifest; e.phase = 'ready'; this.saveExport(e); return { kind: 'export_ready', binding: this.source(e) }; });
 }
 private async exportFile(e: Export, side: 'before' | 'after', path: string): Promise<PeerEvidence> {
  if (e.phase !== 'ready' || this.now() >= e.deadline) fail(409, 'aborted');
  const selected = this.host.transactionSync(() => { this.sourceViews(e); if (!e.manifest) fail(400, 'failed_precondition'); const r = this.one<{ value: string }>('SELECT value FROM transform_selection WHERE owner=? AND side=? AND path=?', e.id, side, path); if (!r) fail(403, 'permission_denied'); return JSON.parse(r.value) as SelectionRow; });
  let loaded: { descriptor: Descriptor; bytes: Uint8Array }; try { loaded = await this.host.readVerifiedObject(selected.descriptor); } catch (error) { this.host.transactionSync(() => this.sourceViews(e)); throw error; }
  return this.host.transactionSync(() => { this.sourceViews(e); if (e.phase !== 'ready' || this.now() >= e.deadline) fail(409, 'aborted'); if (loaded.bytes.length > 1048576 || !same(loaded.descriptor, selected.descriptor) || !same(inspect(1, loaded.bytes), selected.descriptor)) fail(503, 'unavailable'); return { kind: 'export_file', file: { binding: this.source(e), side, path, descriptor: selected.descriptor, bytes: loaded.bytes } }; });
 }
 private exportPage(e: Export, cursor: number): PeerEvidence {
  if (e.phase !== 'ready' || this.now() >= e.deadline) fail(409, 'aborted');
  return this.host.transactionSync(() => { this.sourceViews(e); if (!e.manifest || !safe(cursor, e.selected)) fail(400, 'invalid_argument');
   const rows = this.rows<{ ordinal: number; value: string }>('SELECT ordinal,value FROM transform_selection WHERE owner=? AND ordinal>=? ORDER BY ordinal LIMIT 128', e.id, cursor);
   const entries: SelectionRow[] = []; let size = 0;
   for (const r of rows) { if (r.ordinal !== cursor + entries.length) fail(503, 'unavailable'); if (size + encoder.encode(r.value).length > 256 * 1024) break; entries.push(JSON.parse(r.value)); size += encoder.encode(r.value).length; }
   if (!entries.length && cursor !== e.selected) fail(503, 'unavailable'); const next = cursor + entries.length;
   return { kind: 'export_page', page: { binding: this.source(e), cursor, entries, next: next === e.selected ? null : next, lineage: next === e.selected ? JSON.parse(e.lineage) : [] } };
  });
 }
 private acceptBinding(d: Draft, binding: SourceExportBinding): void {
  const b = this.saved(d), p = JSON.parse(d.payload) as TransformStart;
  if (encoded(binding.draft) !== encoded(b) || binding.source_account !== p.source_account || binding.source_view !== p.source_view || binding.before_view !== p.source_before_view || binding.after_view !== p.source_after_view
   || !id(binding.export_id) || !id(binding.before_revision) || !id(binding.after_revision) || !id(binding.manifest_digest) || !safe(binding.source_epoch) || binding.source_epoch < 1) fail(409, 'aborted');
  if (d.source !== null && d.source !== encoded(binding)) fail(409, 'aborted');
 }
 private acceptPage(d: Draft, page: ExportPage): void {
  this.destination(d); this.acceptBinding(d, page.binding);
  if (d.phase !== 'pages' || d.page_done || page.cursor !== d.page_cursor || !Array.isArray(page.entries) || page.entries.length > 128) fail(409, 'aborted');
  if (page.next !== null && (page.next !== page.cursor + page.entries.length || page.entries.length === 0)) fail(503, 'unavailable');
  const b = this.saved(d), p = JSON.parse(d.payload) as TransformStart, all = this.selection(d.id);
  if (all.length + page.entries.length > MAX_FILES * 2) fail(429, 'resource_exhausted');
  for (const [i, row] of page.entries.entries()) {
   if (row.ordinal !== page.cursor + i || !['before', 'after'].includes(row.side) || row.descriptor.kind !== 1 || !safe(row.descriptor.size_bytes, 1048576)) fail(503, 'unavailable');
   const value = encoded(row); this.charge(1, value.length * 3 + 128); this.host.sql.exec('INSERT INTO transform_selection(owner,side,path,ordinal,value,ready) VALUES(?,?,?,?,?,0)', d.id, row.side, row.path, row.ordinal, value);
  }
  const view = this.destination(d); for (const side of ['before', 'after'] as const) this.destinationPaths(view, this.map(p, b.destination_root, this.selection(d.id, side)).map(f => f.destination_path));
  d.page_cursor += page.entries.length;
  if (page.next === null) { if (!Array.isArray(page.lineage) || page.lineage.length > 4096) fail(429, 'resource_exhausted'); const hints = (page.lineage as VerifiedLineage[]).map(h => ({ operation: h.operation, source_path: h.source_path, target_path: h.target_path })); if (encoded(hints, 1048576) !== encoded(p.lineage, 1048576) || (page.lineage as VerifiedLineage[]).some(h => typeof h.source_canonical_path !== 'string' || typeof h.target_canonical_path !== 'string' || encoder.encode(h.source_canonical_path + h.target_canonical_path).length > 8192)) fail(503, 'unavailable'); d.page_done = 1; d.lineage = encoded(page.lineage, 1048576); d.phase = 'seal_selection'; }
  this.saveDraft(d);
 }
 private async acceptFile(d: Draft, file: ExportFile): Promise<void> {
  this.host.transactionSync(() => {
   this.destination(d); this.acceptBinding(d, file.binding);
   const row = this.one<{ value: string; ready: number }>('SELECT value,ready FROM transform_selection WHERE owner=? AND side=? AND path=?', d.id, file.side, file.path);
   if (d.phase !== 'files' || !row || row.ready !== 0 || !(file.bytes instanceof Uint8Array) || file.bytes.length > 1048576 || !same((JSON.parse(row.value) as SelectionRow).descriptor, file.descriptor)) fail(409, 'aborted');
   if (!same(inspect(1, file.bytes), file.descriptor)) fail(400, 'invalid_argument'); this.host.reserveGeneratedInTx(d.id, file.descriptor);
  });
  let token: unknown; try { token = await this.host.stageGenerated(file.descriptor, file.bytes); } catch (error) { this.host.transactionSync(() => this.destination(d)); throw error; }
  this.host.transactionSync(() => { this.destination(d); if (this.draft(d.id).generation !== d.generation) fail(409, 'aborted'); this.host.confirmGeneratedInTx(d.id, file.descriptor, token); this.host.sql.exec('UPDATE transform_selection SET ready=1 WHERE owner=? AND side=? AND path=?', d.id, file.side, file.path); this.saveDraft(d); });
 }
 private enqueue(owner: string, key: ObjectKey): void {
  if (![2, 3].includes(key.kind) || !id(key.id)) fail(503, 'unavailable');
  if (this.one('SELECT 1 FROM transform_pack WHERE owner=? AND kind=? AND id=?', owner, key.kind, key.id)) return;
  if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM transform_pack WHERE owner=?', owner)!.n >= 8192) fail(429, 'resource_exhausted');
  this.charge(1, 128); this.host.sql.exec('INSERT INTO transform_pack(owner,kind,id) VALUES(?,?,?)', owner, key.kind, key.id);
 }
 private async collect(d: Draft): Promise<boolean> {
  let count = 0, size = 0; const deadline = Date.now() + 5000;
  while (count < 16 && Date.now() < deadline) {
   const next = this.host.transactionSync(() => { this.destination(d); return this.one<Pack>('SELECT * FROM transform_pack WHERE owner=? AND data IS NULL ORDER BY kind DESC,id LIMIT 1', d.id); });
   if (!next) return true;
   const descriptor = this.host.transactionSync(() => { this.destination(d); const v = this.host.describeObjectInTx(next); if (!v || !safe(v.size_bytes, MAX_META)) fail(503, 'unavailable'); return v; });
   if (size + descriptor.size_bytes > 256 * 1024) return false;
   let loaded: { descriptor: Descriptor; bytes: Uint8Array }; try { loaded = await this.host.readVerifiedObject(descriptor); } catch (error) { this.host.transactionSync(() => this.destination(d)); throw error; }
   this.host.transactionSync(() => {
    this.destination(d); if (this.draft(d.id).generation !== d.generation || !same(descriptor, loaded.descriptor) || loaded.bytes.length !== descriptor.size_bytes) fail(409, 'aborted');
    const info = inspect(descriptor.kind, loaded.bytes) as Descriptor & { commit?: { root_trees: string[] }; tree?: { entries: { kind: number; id: string }[] } };
    if (!same(info, descriptor)) fail(503, 'unavailable'); if (d.metadata_bytes + descriptor.size_bytes > MAX_PACK) fail(429, 'resource_exhausted');
    this.charge(0, descriptor.size_bytes); this.host.sql.exec('UPDATE transform_pack SET size=?,data=? WHERE owner=? AND kind=? AND id=?', descriptor.size_bytes, loaded.bytes.slice().buffer, d.id, descriptor.kind, descriptor.id);
    if (descriptor.kind === 3) { if (info.commit?.root_trees.length !== 1 || descriptor.id !== this.saved(d).destination_base) fail(400, 'failed_precondition'); this.enqueue(d.id, { kind: 2, id: info.commit.root_trees[0] }); }
    else { if (!info.tree) fail(503, 'unavailable'); for (const entry of info.tree.entries) if (entry.kind === 2) this.enqueue(d.id, entry); }
    d.metadata_bytes += descriptor.size_bytes; this.saveDraft(d);
   }); count++; size += descriptor.size_bytes;
  }
  return false;
 }
 private pack(d: Draft): Generated[] {
  const rows = this.rows<Pack>('SELECT * FROM transform_pack WHERE owner=? ORDER BY kind,id LIMIT 8193', d.id); if (rows.length > 8192 || rows.some(r => r.data === null || r.size === null)) fail(400, 'failed_precondition');
  return rows.map(r => { if (!(r.data instanceof ArrayBuffer) || r.data.byteLength !== r.size) fail(503, 'unavailable'); return { kind: r.kind, id: r.id, size_bytes: r.size!, format_version: 1, bytes_base64: base64(new Uint8Array(r.data)) }; });
 }
 private async build(d: Draft): Promise<void> {
  const input = this.host.transactionSync(() => {
   const view = this.destination(d), b = this.saved(d), p = JSON.parse(d.payload) as TransformStart, source = JSON.parse(d.source!) as SourceExportBinding;
   const beforeRows = this.selection(d.id, 'before'), afterRows = this.selection(d.id, 'after'), before = this.map(p, b.destination_root, beforeRows), after = this.map(p, b.destination_root, afterRows);
   this.destinationPaths(view, [...before, ...after].map(f => f.destination_path)); const edges: unknown[] = [];
   for (const hint of JSON.parse(d.lineage) as VerifiedLineage[]) {
    const to = after.find(r => r.source_relative_path === hint.target_path);
    if (!to || hint.target_canonical_path !== to.source_canonical_path) fail(400, 'invalid_argument'); edges.push({ operation: hint.operation, source_account: source.source_account, source_revision: array(source.before_revision), source_path: hint.source_canonical_path, destination_path: to.destination_path, destination_revision: Array(32).fill(0) });
   }
   for (const file of after) edges.push({ operation: 'imported_origin', source_account: source.source_account, source_revision: array(source.after_revision), source_path: file.source_canonical_path, destination_path: file.destination_path, destination_revision: Array(32).fill(0) });
   return { identity: this.identity(d), committer: JSON.parse(d.committer), destination_root: b.destination_root,
    before: before.map(f => ({ path: f.mapped_path, file: { id: f.descriptor.id, size: f.descriptor.size_bytes, executable: f.executable } })),
    after: after.map(f => ({ path: f.mapped_path, file: { id: f.descriptor.id, size: f.descriptor.size_bytes, executable: f.executable } })), lineage: edges, objects: this.pack(d) };
  });
  const result = buildForgeTransform(input) as Plan | { state: 'missing_metadata'; kind: number; id: string };
  if (result.state === 'missing_metadata') { this.host.transactionSync(() => { this.destination(d); if (this.one('SELECT 1 FROM transform_pack WHERE owner=? AND kind=? AND id=?', d.id, result.kind, result.id)) fail(503, 'unavailable'); this.enqueue(d.id, result); d.phase = 'collect'; this.saveDraft(d); }); return; }
  if (result.state !== 'ready') fail(400, 'failed_precondition'); this.validatePlan(d, result);
  const data = encoder.encode(encoded(result, MAX_PACK)); let digest: string; try { digest = await hash(decoder.decode(data)); } finally { this.host.transactionSync(() => this.destination(d)); }
  this.host.transactionSync(() => { this.destination(d); if (this.draft(d.id).generation !== d.generation) fail(409, 'aborted'); this.charge(Math.ceil(data.length / CHUNK), data.length); for (let n = 0; n < data.length; n += CHUNK) this.host.sql.exec('INSERT INTO transform_plan(owner,ordinal,data) VALUES(?,?,?)', d.id, n / CHUNK, data.slice(n, n + CHUNK).buffer); d.plan_hash = digest; d.plan_bytes = data.length; d.phase = 'stage'; this.saveDraft(d); });
 }
 private validatePlan(d: Draft, plan: Plan): void {
  if (!Array.isArray(plan.objects) || plan.objects.length > 8192 || hex(plan.run.id) !== d.run_id || plan.run.destination_account !== this.host.accountId
   || hex(plan.run.destination_view) !== this.saved(d).destination_view || hex(plan.run.destination_base) !== this.saved(d).destination_base || plan.run.pin.retired || plan.run.pin.generation !== 1) fail(503, 'unavailable');
  const keys = new Set<string>(); let total = 0;
  for (const o of plan.objects) { if (![2, 3, 4, 5].includes(o.kind) || !safe(o.size_bytes, MAX_META) || o.format_version !== 1 || keys.has(`${o.kind}/${o.id}`)) fail(503, 'unavailable'); keys.add(`${o.kind}/${o.id}`); bytes(o); total += o.size_bytes; if (total > MAX_PACK) fail(429, 'resource_exhausted'); }
  if (!keys.has(`3/${hex(plan.run.output_head)}`) || !keys.has(`3/${hex(plan.run.pin.artifact)}`) || plan.run.destination_paths.length > 8192) fail(503, 'unavailable');
  this.destinationPaths(this.destination(d), plan.run.destination_paths);
 }
 private async plan(d: Draft): Promise<Plan> {
  const data = this.host.transactionSync(() => { this.destination(d); if (!d.plan_hash || !safe(d.plan_bytes, MAX_PACK)) fail(503, 'unavailable');
   const all = this.rows<{ ordinal: number; data: ArrayBuffer }>('SELECT ordinal,data FROM transform_plan WHERE owner=? ORDER BY ordinal', d.id), bytes = new Uint8Array(d.plan_bytes); let offset = 0;
   all.forEach((r, i) => { if (r.ordinal !== i || !(r.data instanceof ArrayBuffer) || r.data.byteLength > CHUNK || r.data.byteLength > bytes.length - offset) fail(503, 'unavailable'); bytes.set(new Uint8Array(r.data), offset); offset += r.data.byteLength; }); if (offset !== bytes.length) fail(503, 'unavailable'); return bytes;
  });
  let digest: string; try { digest = await hash(decoder.decode(data)); } finally { this.host.transactionSync(() => this.destination(d)); }
  return this.host.transactionSync(() => { this.destination(d); if (digest !== d.plan_hash || this.draft(d.id).generation !== d.generation) fail(409, 'aborted'); const p = JSON.parse(decoder.decode(data)) as Plan; this.validatePlan(d, p); return p; });
 }
 private async stage(d: Draft): Promise<void> {
  const plan = await this.plan(d); let writes = 0, count = 0; const deadline = Date.now() + 5000;
  while (d.output_cursor < plan.objects.length && count < 16 && Date.now() < deadline) {
   const object = plan.objects[d.output_cursor]; if (writes + object.size_bytes > 256 * 1024) break;
   const data = bytes(object); this.host.transactionSync(() => { this.destination(d); this.host.reserveGeneratedInTx(d.id, object); });
   let confirmation: unknown; try { confirmation = await this.host.stageGenerated(object, data); } catch (error) { this.host.transactionSync(() => this.destination(d)); throw error; }
   this.host.transactionSync(() => { this.destination(d); if (this.draft(d.id).generation !== d.generation) fail(409, 'aborted'); this.host.confirmGeneratedInTx(d.id, object, confirmation); d.output_cursor++; this.saveDraft(d); }); count++; writes += object.size_bytes;
  }
  if (d.output_cursor === plan.objects.length) this.host.transactionSync(() => { this.destination(d); d.phase = 'proof_artifact'; this.saveDraft(d); });
 }
 private closure(d: Draft, plan: Plan, side: 'artifact' | 'output'): ClosureJobs {
  const key = { kind: 3, id: hex(side === 'artifact' ? plan.run.pin.artifact : plan.run.output_head) };
  const publication = encoded([d.id, d.run_id, d.plan_hash, side, key.id, (JSON.parse(d.source!) as SourceExportBinding).manifest_digest]);
  const binding: ClosureBinding = { account_id: this.host.accountId, identity: { kind: 'principal', principal: d.principal }, policy_epoch: this.saved(d).destination_epoch, purpose: 'transform', publication };
  const authorize = () => { const live = this.draft(d.id); this.destination(live); if (live.plan_hash !== d.plan_hash || live.source !== d.source || live.run_id !== d.run_id || !['proof_artifact', 'proof_output', 'prepared'].includes(live.phase)) fail(409, 'aborted'); };
  return new ClosureJobs({ binding, sql: this.host.sql, transactionSync: f => this.host.transactionSync(f), authorizeCurrentInTx: b => { if (encoded(b) !== encoded(binding)) fail(403, 'permission_denied'); authorize(); },
   describeObjectInTx: k => this.host.describeObjectInTx(k), readVerifiedObject: k => this.host.readVerifiedObject(k),
   attachRootInTx: (b, root, proof) => { authorize(); if (encoded(b) !== encoded(binding) || root.kind !== key.kind || root.id !== key.id) fail(403, 'permission_denied'); this.host.retainRootInTx(`transform-stage-${side}`, d.id, root, proof); return { root }; } });
 }
 private async prove(d: Draft): Promise<void> {
  const plan = await this.plan(d), side = d.phase === 'proof_artifact' ? 'artifact' : 'output', field = side === 'artifact' ? 'artifact_job' : 'output_job', jobs = this.closure(d, plan, side);
  if (!d[field]) { const status = await jobs.begin(`transform-${side}-${d.id}`, { kind: 3, id: hex(side === 'artifact' ? plan.run.pin.artifact : plan.run.output_head) });
   this.host.transactionSync(() => { this.destination(d); if (this.draft(d.id).generation !== d.generation) fail(409, 'aborted'); d[field] = status.job_id; this.saveDraft(d); }); return; }
  const status = jobs.status(d[field]!);
  if (status.error === 'object_invalid') fail(400, 'failed_precondition'); if (status.error === 'object_unavailable') fail(503, 'unavailable'); if (status.error === 'closure_limit') fail(429, 'resource_exhausted');
  if (!status.complete) { await jobs.advance(status.job_id, status.generation); this.host.transactionSync(() => this.destination(d)); return; }
  if (status.phase !== 'published') jobs.publish(status.job_id, status.generation);
  this.host.transactionSync(() => { this.destination(d); if (this.draft(d.id).generation !== d.generation) fail(409, 'aborted'); d.phase = side === 'artifact' ? 'proof_output' : 'prepared'; this.saveDraft(d); });
 }
 private async prepared(d: Draft): Promise<PreparedTransform> {
  const plan = await this.plan(d); return this.host.transactionSync(() => { this.destination(d); if (d.phase !== 'prepared' || !d.source || !d.plan_hash || !d.artifact_job || !d.output_job || !d.run_id) fail(400, 'failed_precondition');
   for (const [job, root] of [[d.artifact_job, hex(plan.run.pin.artifact)], [d.output_job, hex(plan.run.output_head)]]) {
    const proof = this.one<{ account: string; phase: string; root_kind: number; root_id: string }>('SELECT account,phase,root_kind,root_id FROM closure_jobs WHERE id=?', job);
    if (!proof || proof.account !== this.host.accountId || proof.phase !== 'published' || proof.root_kind !== 3 || proof.root_id !== root) fail(400, 'failed_precondition'); this.host.requireRetainedClosureInTx({ kind: 3, id: root });
   }
   return { binding: JSON.parse(d.source), run_id: d.run_id, plan_digest: d.plan_hash, artifact: hex(plan.run.pin.artifact), output_head: hex(plan.run.output_head), artifact_proof: d.artifact_job, output_proof: d.output_job, prepared_generation: d.generation };
  });
 }
 private arm(e: Export, prepared: PreparedTransform): PeerEvidence {
  return this.host.transactionSync(() => {
   this.sourceViews(e); if (!e.manifest || encoded(prepared.binding) !== encoded(this.source(e)) || !safe(prepared.prepared_generation) || !id(prepared.plan_digest) || !id(prepared.artifact) || !id(prepared.output_head)) fail(409, 'aborted');
   const p = JSON.parse(e.payload) as TransformStart, b = JSON.parse(e.draft) as TransformDraftBinding;
   const identity = sourceIdentity(this.source(e), p), expected = deriveForgeTransformIdentity({ kind: 'run', identity, spec: p.spec }) as { run_id: string; spec_id: string };
   if (prepared.run_id !== expected.run_id || expected.spec_id !== b.spec_id) fail(409, 'aborted');
   if (e.permit) { const old = JSON.parse(e.permit) as SourcePermit; if (encoded(old.prepared, MAX_PACK) !== encoded(prepared, MAX_PACK)) fail(409, 'already_exists'); return { kind: 'permit', permit: old }; }
   if (e.phase !== 'ready') fail(409, 'aborted');
   const permit: SourcePermit = { permit_id: nonce(), export_id: e.id, prepared };
   // No awaits between fresh selected/current export checks and the fence. All
   // epoch mutations consult this durable state in their own transaction.
   e.phase = 'armed'; e.permit = encoded(permit); this.charge(0, e.permit.length * 3); this.saveExport(e); return { kind: 'permit', permit };
  });
 }
 private async commit(d: Draft, permit: SourcePermit): Promise<void> {
  const plan = await this.plan(d), prepared = await this.prepared(d);
  if (encoded(prepared) !== encoded(permit.prepared) || permit.export_id !== d.export_id || !id(permit.permit_id)) fail(409, 'aborted');
  const publication = await hash(encoded({ permit, artifact: hex(plan.run.pin.artifact), output_head: hex(plan.run.output_head) }));
  this.host.transactionSync(() => {
   this.destination(d); if (this.draft(d.id).generation !== d.generation || d.phase !== 'prepared') fail(409, 'aborted');
   const old = this.one<RunRow>('SELECT * FROM transform_runs WHERE id=?', d.run_id!);
   if (old) {
    const existing = JSON.parse(old.value) as NativeRun;
    if (old.account !== this.host.accountId || old.retired || hex(existing.pin.artifact) !== hex(plan.run.pin.artifact) || hex(existing.output_head) !== hex(plan.run.output_head)
        || hex(existing.destination_view) !== hex(plan.run.destination_view) || hex(existing.destination_base) !== hex(plan.run.destination_base)) fail(409, 'already_exists');
   } else if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM transform_runs')!.n >= 1024) fail(429, 'resource_exhausted');
   // Only exact service-generated/proved closures enter this ledger. Availability
   // rows and user-provided hashes cannot mint provenance or pin membership.
   for (const [label, root, proof] of [['pin', hex(plan.run.pin.artifact), d.artifact_job!], ['output', hex(plan.run.output_head), d.output_job!]]) {
    this.host.requireRetainedClosureInTx({ kind: 3, id: root }); if (!old) this.host.retainRootInTx(`transform-${label}`, d.run_id!, { kind: 3, id: root }, proof);
    this.host.releaseRootInTx(`transform-stage-${label === 'pin' ? 'artifact' : 'output'}`, d.id, { kind: 3, id: root });
   }
   const value = encoded(plan.run, 1048576); if (!old) { this.charge(1 + plan.run.pin.protected_roots.length, value.length * 3 + 2048);
   this.host.sql.exec('INSERT INTO transform_runs(id,account,principal,draft,binding,value,artifact_proof,output_proof,generation,retired) VALUES(?,?,?,?,?,?,?,?,1,0)', d.run_id!, this.host.accountId, d.principal, d.id, d.source!, value, d.artifact_job!, d.output_job!);
   for (const owner of plan.run.pin.protected_roots) this.host.sql.exec("INSERT INTO transform_pin_owners(run_id,kind,owner) VALUES(?,'transform',?)", d.run_id!, owner); }
   d.phase = 'committed'; d.permit = encoded(permit); d.publication_digest = publication; this.saveDraft(d);
  });
 }
 private outcome(job: string, exportId: string, abort = false): DestinationOutcome {
  return this.host.transactionSync(() => {
   const d = this.draft(job), p = JSON.parse(d.payload) as TransformStart;
   if (!id(exportId) || (d.export_id !== null && d.export_id !== exportId) || p.source_account === this.host.accountId) fail(403, 'permission_denied');
   // Cleanup transport is trusted and bound to an exact source SQL custody.
   // It cannot select arbitrary payloads or create a permit/publication.
   if (d.export_id === null && abort) { d.export_id = exportId; this.saveDraft(d); }
   if (d.phase === 'committed') { const permit = JSON.parse(d.permit!) as SourcePermit; return { draft_id: d.id, export_id: exportId, state: 'committed', run_id: d.run_id!, permit_id: permit.permit_id, publication_digest: d.publication_digest! }; }
   if (abort && d.phase !== 'aborted') this.abortLocal(d);
   return { draft_id: d.id, export_id: exportId, state: d.phase === 'aborted' ? 'aborted' : 'pending' };
  });
 }
 private abortLocal(d: Draft): void {
  if (d.phase === 'committed' || d.phase === 'aborted') return;
  d.phase = 'aborted'; this.saveDraft(d);
  for (const [side, job] of [['artifact', d.artifact_job], ['output', d.output_job]]) if (job) {
   const proof = this.one<{ account: string; root_kind: number; root_id: string }>('SELECT account,root_kind,root_id FROM closure_jobs WHERE id=?', job);
   if (proof && proof.account === this.host.accountId && proof.root_kind === 3) this.host.releaseRootInTx(`transform-stage-${side}`, d.id, { kind: 3, id: proof.root_id });
  }
 }
 private settle(e: Export, outcome: DestinationOutcome): PeerEvidence {
  return this.host.transactionSync(() => {
   const live = this.export(e.id); if (live.generation !== e.generation || live.phase !== e.phase || live.draft !== e.draft) fail(409, 'aborted');
   const draft = JSON.parse(e.draft) as TransformDraftBinding;
   if (outcome.draft_id !== draft.draft_id || outcome.export_id !== e.id) fail(403, 'permission_denied');
   if (outcome.state === 'pending') return { kind: 'outcome', outcome };
   const phase = outcome.state === 'committed' ? 'settled_committed' : 'settled_aborted';
   if (e.phase.startsWith('settled_')) { if (e.phase !== phase) fail(409, 'aborted'); return { kind: 'settled', export_id: e.id }; }
   if (outcome.state === 'committed') {
    if (!e.permit || e.phase !== 'armed') fail(409, 'aborted'); const permit = JSON.parse(e.permit) as SourcePermit;
    if (permit.permit_id !== outcome.permit_id || permit.prepared.run_id !== outcome.run_id || !id(outcome.publication_digest)) fail(409, 'aborted');
   }
   // Aborted is a durable destination terminal CAS, not a timer inference.
   e.phase = phase; this.saveExport(e); return { kind: 'settled', export_id: e.id };
  });
 }
 private run(runId: string): RunRow {
  const row = this.one<RunRow>('SELECT * FROM transform_runs WHERE id=? AND account=?', runId, this.host.accountId); if (!row || !id(runId)) fail(404, 'not_found'); return row;
 }
 private runAccess(row: RunRow, action: 'read' | 'write' | 'admin' = 'read', requested?: ResolvedView, trustedSubject = false): { run: NativeRun; view: ResolvedView } {
  if (trustedSubject) { if (this.host.principal === null) fail(403, 'permission_denied'); } else this.actor(); this.host.authorizeInTx(action); const binding = JSON.parse(row.binding) as SourceExportBinding, b = binding.draft, run = JSON.parse(row.value) as NativeRun;
  if (hex(run.id) !== row.id || run.destination_account !== this.host.accountId || hex(run.destination_view) !== b.destination_view || hex(run.destination_base) !== b.destination_base || run.pin.retired !== (row.retired === 1) || run.pin.generation !== row.generation) fail(503, 'unavailable');
  const view = requested ?? this.host.views.resolveInTx(b.destination_view_name, array(b.destination_view)); this.host.views.checkInTx(view);
  if (hex(view.description.view_version) !== b.destination_view || hex(view.description.revision) !== b.destination_base) fail(403, 'permission_denied'); this.destinationPaths(view, run.destination_paths);
  return { run, view };
 }
 /** Exact service-owned transform exception, not a supplied-byte receipt or
  * arbitrary provenance/hash exemption. Source auth is intentionally no longer
  * required: the already-published destination pin owns its imported bytes. */
 requireReviewInTx(view: ResolvedView, runId: string, base: string, head: string): void {
  const row = this.run(runId), { run } = this.runAccess(row, 'write', view, true);
  if (row.retired || hex(run.destination_base) !== base || hex(run.output_head) !== head) fail(403, 'permission_denied');
  const draft = this.draft(row.draft), identity = deriveForgeTransformIdentity({ kind: 'run', identity: this.identity(draft), spec: (JSON.parse(draft.payload) as TransformStart).spec }) as { run_id: string };
  if (identity.run_id !== runId || draft.phase !== 'committed' || draft.run_id !== runId || draft.source !== row.binding) fail(503, 'unavailable');
  for (const [proofId, key] of [[row.artifact_proof, hex(run.pin.artifact)], [row.output_proof, hex(run.output_head)]]) {
   const proof = this.one<{ account: string; phase: string; root_kind: number; root_id: string; binding: string }>('SELECT account,phase,root_kind,root_id,binding FROM closure_jobs WHERE id=?', proofId);
   const side = proofId === row.artifact_proof ? 'artifact' : 'output';
   const publication = encoded([draft.id, draft.run_id, draft.plan_hash, side, key, (JSON.parse(draft.source!) as SourceExportBinding).manifest_digest]);
   const binding = JSON.stringify([this.host.accountId, { kind: 'principal', principal: draft.principal }, this.saved(draft).destination_epoch, 'transform', publication]);
   if (!proof || proof.account !== this.host.accountId || proof.phase !== 'published' || proof.root_kind !== 3 || proof.root_id !== key || proof.binding !== binding) fail(403, 'permission_denied');
   this.host.requireRetainedClosureInTx({ kind: 3, id: key });
  }
 }
 private viewValue(row: RunRow, origin: boolean): unknown {
  const { run } = this.runAccess(row), owners = this.rows<{ owner: string }>('SELECT owner FROM transform_pin_owners WHERE run_id=? ORDER BY kind,owner LIMIT 1025', row.id);
  if (owners.length > 1024) fail(503, 'unavailable'); run.pin.protected_roots = owners.map(o => o.owner);
  return { id: run.id, destination_account: run.destination_account, destination_view: run.destination_view, destination_base: run.destination_base,
   output_head: run.output_head, pin: run.pin, origin: origin ? { account: run.source_account, before: run.source_before, after: run.source_after, lineage: run.lineage } : null };
 }
 private originQuery(row: RunRow, operation: 31 | 35): TransformRpcResult {
  this.host.transactionSync(() => this.runAccess(row));
  const value = this.host.transactionSync(() => this.viewValue(row, false)); const formatted = formatForgeTransform({ kind: operation === 35 ? 'lineage' : 'view', value }) as { state: string; value: unknown };
  return { result: { state: formatted.state, result: formatted.value }, next: { account_id: (JSON.parse(row.binding) as SourceExportBinding).source_account,
   command: { action: 'authorize_origin', binding: JSON.parse(row.binding) }, continuation: { job_id: `query-${operation}-${row.id}`, expected_generation: row.generation } } };
 }
 private authorizeOrigin(binding: SourceExportBinding): PeerEvidence {
  return this.host.transactionSync(() => {
   try {
    this.actor(); if (binding.source_account !== this.host.accountId) fail(403, 'permission_denied');
    const e = this.export(binding.export_id); if (encoded(this.source(e)) !== encoded(binding) || e.phase !== 'settled_committed') fail(403, 'permission_denied');
    const before = this.host.views.resolveInTx(binding.source_view, array(binding.before_view)), after = this.host.views.resolveInTx(binding.source_view, array(binding.after_view));
    for (const [side, view] of [['before', before], ['after', after]] as const) {
     this.host.views.checkInTx(view); const p = this.host.views.policyForProjectionInTx(view); if (!p.selected_spec.export_allowed || !p.current_spec.export_allowed) fail(403, 'permission_denied');
     // Returning lineage can disclose canonical names. Recheck the actual
     // exported paths, not just the visibility of an empty/narrowed view.
     const lineage = JSON.parse(e.lineage) as VerifiedLineage[];
     this.destinationPaths(view, [...this.selection(e.id, side).map(f => f.canonical_path), ...lineage.map(h => side === 'before' ? h.source_canonical_path : h.target_canonical_path)]);
    }
    return { kind: 'origin', binding, available: true };
   } catch (error) { if (!(error instanceof Failure)) throw error; return { kind: 'origin', binding, available: false }; }
  });
 }
 private pinFiles(row: RunRow): Mapped[] {
  const { run } = this.runAccess(row); if (row.retired || run.pin.retired) fail(400, 'failed_precondition');
  this.host.requireRetainedClosureInTx({ kind: 3, id: hex(run.pin.artifact) }); const d = this.draft(row.draft), p = JSON.parse(d.payload) as TransformStart;
  return this.map(p, this.saved(d).destination_root, this.selection(d.id, 'after')).sort((a, b) => a.mapped_path < b.mapped_path ? -1 : a.mapped_path > b.mapped_path ? 1 : 0);
 }
 private async resolvePin(row: RunRow, p: Extract<TransformCommand, { operation: 33 }>['payload']): Promise<TransformRpcResult> {
  const selected = this.host.transactionSync(() => {
   const files = this.pinFiles(row), run = JSON.parse(row.value) as NativeRun;
   if (p.path === null) {
    const limit = p.limit ?? 128; if (!safe(limit, 256) || !limit || p.offset !== null || p.max_bytes !== null) fail(400, 'invalid_argument');
    const all = files.filter(f => p.after_path === null || f.mapped_path > p.after_path).slice(0, limit + 1), next = all.length > limit ? all[limit - 1].mapped_path : null;
    const value = { pin_id: run.pin.id, artifact: run.pin.artifact, files: all.slice(0, limit).map(f => ({ path: f.mapped_path, file_id: array(f.descriptor.id), size_bytes: f.descriptor.size_bytes, executable: f.executable })), next_path: next };
    const formatted = formatForgeTransform({ kind: 'page', value }) as { state: string; value: unknown }; return { reply: { result: { state: formatted.state, result: formatted.value } } };
   }
   if (p.after_path !== null || p.limit !== null) fail(400, 'invalid_argument'); const file = files.find(f => f.mapped_path === p.path); if (!file) fail(404, 'not_found');
   const offset = p.offset ?? 0, limit = p.max_bytes ?? 65536; if (!safe(offset, file.descriptor.size_bytes) || !safe(limit, 65536) || !limit) fail(400, 'invalid_argument'); return { file, offset, limit, run };
  });
  if ('reply' in selected) return selected.reply!;
  let object: { descriptor: Descriptor; bytes: Uint8Array }; try { object = await this.host.readVerifiedObject(selected.file!.descriptor); } catch (error) { this.host.transactionSync(() => this.pinFiles(this.run(row.id))); throw error; }
  return this.host.transactionSync(() => { const fresh = this.run(row.id); this.pinFiles(fresh); if (fresh.generation !== row.generation || !same(object.descriptor, selected.file!.descriptor)) fail(409, 'aborted');
   const data = object.bytes.subarray(selected.offset!, selected.offset! + selected.limit!); const value = { pin_id: selected.run!.pin.id, file_id: array(object.descriptor.id), offset: selected.offset!, bytes: Array.from(data), next_offset: selected.offset! + data.length, eof: selected.offset! + data.length === object.descriptor.size_bytes };
   const formatted = formatForgeTransform({ kind: 'chunk', value }) as { state: string; value: unknown }; return { result: { state: formatted.state, result: formatted.value } };
  });
 }
 private retire(call: ForgeCall, runId: string): TransformRpcResult {
  return this.host.transactionSync(() => {
   const principal = this.actor(), row = this.run(runId), { run } = this.runAccess(row, 'admin');
   const fingerprint = (deriveForgeTransformIdentity({ kind: 'retire', account_id: this.host.accountId, principal, view: call.view, run_id: runId }) as { fingerprint: string }).fingerprint;
   const replay = this.one<{ fingerprint: string; value: string }>('SELECT fingerprint,value FROM transform_receipts WHERE account=? AND principal=? AND request_id=?', this.host.accountId, principal, call.request_id);
   if (replay) { if (replay.fingerprint !== fingerprint) fail(409, 'already_exists'); return { result: { state: 'retired', result: JSON.parse(replay.value) } }; }
   if (this.one('SELECT 1 FROM transform_drafts WHERE account=? AND principal=? AND request_id=?', this.host.accountId, principal, call.request_id)) fail(409, 'already_exists');
   run.pin.protected_roots = this.rows<{ owner: string }>('SELECT owner FROM transform_pin_owners WHERE run_id=? ORDER BY kind,owner LIMIT 1025', runId).map(o => o.owner);
   if (!row.retired) {
    if (this.one('SELECT 1 FROM transform_pin_owners WHERE run_id=? LIMIT 1', runId)) fail(400, 'failed_precondition');
    run.pin.retired = true; run.pin.generation++; if (!safe(run.pin.generation)) fail(429, 'resource_exhausted');
    this.host.releaseRootInTx('transform-pin', runId, { kind: 3, id: hex(run.pin.artifact) }); this.host.releaseRootInTx('transform-output', runId, { kind: 3, id: hex(run.output_head) });
    this.host.sql.exec('UPDATE transform_runs SET value=?,generation=?,retired=1 WHERE id=?', encoded(run, 1048576), run.pin.generation, runId);
   }
   if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM transform_receipts')!.n >= 4096) fail(429, 'resource_exhausted');
   const formatted = formatForgeTransform({ kind: 'pin', value: run.pin }) as { state: string; value: unknown }, value = encoded(formatted.value, 1048576); this.charge(1, value.length * 3 + 256);
   this.host.sql.exec('INSERT INTO transform_receipts(account,principal,request_id,fingerprint,value) VALUES(?,?,?,?,?)', this.host.accountId, principal, call.request_id, fingerprint, value); return { result: { state: formatted.state, result: formatted.value } };
  });
 }
 private async advance(d: Draft): Promise<TransformRpcResult> {
  this.host.transactionSync(() => this.destination(d)); const p = JSON.parse(d.payload) as TransformStart;
  if (d.phase === 'export') return this.next(d, p.source_account, { action: 'begin_export', draft: this.saved(d), payload: p });
  if (d.phase === 'pages') return this.next(d, p.source_account, { action: 'export_page', export_id: d.export_id!, cursor: d.page_cursor });
  if (d.phase === 'seal_selection') {
   const context = this.host.transactionSync(() => {
    this.destination(d); const e = JSON.parse(d.source!) as SourceExportBinding;
    return encoded({ draft: this.saved(d), source_account: e.source_account, before: e.before_revision, after: e.after_revision, epoch: e.source_epoch, files: this.selection(d.id), lineage: JSON.parse(d.lineage) }, MAX_PACK);
   });
   let digest: string; try { digest = await hash(context); } finally { this.host.transactionSync(() => this.destination(d)); }
   this.host.transactionSync(() => { this.destination(d); if (digest !== (JSON.parse(d.source!) as SourceExportBinding).manifest_digest) fail(503, 'unavailable'); d.phase = 'files'; this.saveDraft(d); }); return this.pending(d);
  }
  if (d.phase === 'files') {
   const row = this.host.transactionSync(() => { this.destination(d); return this.one<{ value: string }>('SELECT value FROM transform_selection WHERE owner=? AND ready=0 ORDER BY ordinal LIMIT 1', d.id); });
   if (row) { const file = JSON.parse(row.value) as SelectionRow; return this.next(d, p.source_account, { action: 'export_file', export_id: d.export_id!, side: file.side, path: file.path }); }
   this.host.transactionSync(() => { this.destination(d); d.phase = 'collect'; this.saveDraft(d); }); return this.pending(d);
  }
  if (d.phase === 'collect') { if (await this.collect(d)) this.host.transactionSync(() => { this.destination(d); d.phase = 'build'; this.saveDraft(d); }); return this.pending(d); }
  if (d.phase === 'build') { await this.build(d); return this.pending(d); }
  if (d.phase === 'stage') { await this.stage(d); return this.pending(d); }
  if (d.phase === 'proof_artifact' || d.phase === 'proof_output') { await this.prove(d); return this.pending(d); }
  if (d.phase === 'prepared') return this.next(d, p.source_account, { action: 'arm_export', export_id: d.export_id!, prepared: await this.prepared(d) });
  if (d.phase === 'committed') {
   if (!d.settled) return this.next(d, p.source_account, { action: 'settle_export', export_id: d.export_id!, outcome: this.outcome(d.id, d.export_id!) });
   const row = this.host.transactionSync(() => this.run(d.run_id!)), b = this.saved(d);
   const review = await this.host.pushTransformReview({ run_id: row.id, account_id: this.host.accountId, destination_view: b.destination_view, destination_view_name: b.destination_view_name, destination_base: b.destination_base, output_head: hex((JSON.parse(row.value) as NativeRun).output_head) }, p.expected_patchset);
   this.host.transactionSync(() => { this.destination(d); this.runAccess(this.run(row.id), 'write'); });
   if (review.state === 'admission_pending') return this.pending(d);
   const patchset = (review.result as { patchset_id?: string }).patchset_id;
   if (!patchset || !id(patchset)) fail(503, 'unavailable');
   const formatted = this.host.transactionSync(() => {
    this.destination(d); this.runAccess(this.run(row.id), 'write'); const owner = `review-${patchset}`;
    if (!this.one('SELECT 1 FROM transform_pin_owners WHERE run_id=? AND kind=? AND owner=?', row.id, 'review', owner)) {
     if (this.one<{ n: number }>('SELECT COUNT(*) AS n FROM transform_pin_owners WHERE run_id=?', row.id)!.n >= 1024) fail(429, 'resource_exhausted');
     this.charge(1, 256); this.host.sql.exec("INSERT INTO transform_pin_owners(run_id,kind,owner) VALUES(?,'review',?)", row.id, owner);
    }
    // The review/history service now owns the pin. It must explicitly retire
    // that owner before RetirePin can succeed; a changed current view is not
    // permission to delete historical dependency roots.
    this.host.sql.exec("DELETE FROM transform_pin_owners WHERE run_id=? AND kind='transform' AND owner=?", row.id, `transform-${row.id}`);
    // Format the authoritative post-transfer projection in this same tx; the
    // first ACK must not report an owner that it has already retired. Shared
    // acknowledgement validation can still roll back the owner transfer.
    const value = this.viewValue(this.run(row.id), false);
    return formatForgeTransform({ kind: 'started', value, review, intent: p }) as { state: string; value: unknown };
   });
   return { result: { state: formatted.state, result: formatted.value } };
  }
  fail(409, 'aborted');
 }
 private async accept(job: string, generation: number, evidence: PeerEvidence): Promise<TransformRpcResult> {
  if (evidence.kind === 'outcome') {
   const e = this.host.transactionSync(() => this.export(job)); if (e.generation !== generation && !e.phase.startsWith('settled_')) fail(409, 'aborted'); return { evidence: this.settle(e, evidence.outcome) };
  }
  if (evidence.kind === 'origin') {
   const match = /^query-(31|35)-([0-9a-f]{64})$/.exec(job); if (!match) fail(400, 'invalid_argument');
   return this.host.transactionSync(() => { const row = this.run(match[2]); this.runAccess(row); if (row.generation !== generation || row.binding !== encoded(evidence.binding)) fail(409, 'aborted'); const value = this.viewValue(row, evidence.available), formatted = formatForgeTransform({ kind: match[1] === '35' ? 'lineage' : 'view', value }) as { state: string; value: unknown }; return { result: { state: formatted.state, result: formatted.value } }; });
  }
  const d = this.host.transactionSync(() => { const d = this.draft(job); this.destination(d); if (d.generation !== generation) fail(409, 'aborted'); return d; });
  if (evidence.kind === 'export_pending') {
   this.host.transactionSync(() => { if (d.phase !== 'export' || !id(evidence.export_id) || (d.export_id && d.export_id !== evidence.export_id)) fail(409, 'aborted'); d.export_id = evidence.export_id; this.saveDraft(d); });
  } else if (evidence.kind === 'export_ready') {
   this.host.transactionSync(() => {
    this.destination(d); if (d.phase !== 'export') fail(409, 'aborted'); this.acceptBinding(d, evidence.binding);
    if (d.export_id && d.export_id !== evidence.binding.export_id) fail(409, 'aborted'); d.export_id = evidence.binding.export_id; d.source = encoded(evidence.binding);
    const identity = deriveForgeTransformIdentity({ kind: 'run', identity: this.identity(d), spec: (JSON.parse(d.payload) as TransformStart).spec }) as { run_id: string; spec_id: string };
    if (!id(identity.run_id) || identity.spec_id !== this.saved(d).spec_id) fail(503, 'unavailable'); d.run_id = identity.run_id;
    const previous = this.one<RunRow>('SELECT * FROM transform_runs WHERE id=?', d.run_id);
    if (previous) {
     if (previous.account !== this.host.accountId || previous.retired) fail(400, 'failed_precondition');
     // Native identity freezes the first committer, not the retry's wall clock.
     d.committer = this.draft(previous.draft).committer; this.host.sql.exec('UPDATE transform_drafts SET committer=? WHERE id=?', d.committer, d.id);
    }
    d.phase = 'pages'; this.saveDraft(d);
   });
  } else if (evidence.kind === 'export_page') this.host.transactionSync(() => this.acceptPage(d, evidence.page));
  else if (evidence.kind === 'export_file') await this.acceptFile(d, evidence.file);
  else if (evidence.kind === 'permit') await this.commit(d, evidence.permit);
  else if (evidence.kind === 'settled') this.host.transactionSync(() => { this.destination(d); if (d.phase !== 'committed' || d.export_id !== evidence.export_id) fail(409, 'aborted'); d.settled = 1; this.saveDraft(d); });
  else fail(400, 'invalid_argument');
  return this.pending(this.host.transactionSync(() => this.draft(job)));
 }
 private recovery(): TransformRpcResult {
  return this.host.transactionSync(() => {
   // These are local terminal aborts, not cross-account cleanup guesses. Any
   // concurrently armed source will reconcile our permanent aborted outcome.
   for (const d of this.rows<Draft>("SELECT * FROM transform_drafts WHERE account=? AND phase NOT IN ('committed','aborted','limit') AND deadline<=? ORDER BY deadline,id LIMIT 8", this.host.accountId, this.now())) this.abortLocal(d);
   this.host.sql.exec('CREATE TABLE IF NOT EXISTS transform_recovery(singleton INTEGER PRIMARY KEY CHECK(singleton=1),cursor TEXT NOT NULL)');
   this.host.sql.exec("INSERT OR IGNORE INTO transform_recovery(singleton,cursor) VALUES(1,'')");
   const all = this.rows<Export>("SELECT * FROM transform_exports WHERE account=? AND phase NOT IN ('settled_committed','settled_aborted') ORDER BY id LIMIT 9", this.host.accountId);
   if (all.length > 8) fail(503, 'unavailable'); if (!all.length) return {};
   const cursor = this.one<{ cursor: string }>('SELECT cursor FROM transform_recovery WHERE singleton=1')!.cursor, e = all.find(r => r.id > cursor) ?? all[0];
   this.host.sql.exec('UPDATE transform_recovery SET cursor=? WHERE singleton=1', e.id);
   if (this.now() >= e.deadline && e.phase !== 'armed' && e.phase !== 'cancelling') { e.phase = 'cancelling'; this.saveExport(e); }
   const draft = JSON.parse(e.draft) as TransformDraftBinding;
   return { next: { account_id: draft.destination_account,
    command: { action: this.now() >= e.deadline ? 'abort_transform' : 'transform_outcome', job_id: draft.draft_id, export_id: e.id },
    continuation: { job_id: e.id, expected_generation: e.generation } } };
  });
 }
 async rpc(command: TransformRpcCommand): Promise<TransformRpcResult> {
  // These exact commands can only be called through the nonpublic facade. Null
  // authentication authorizes terminal cleanup of a known ledger record only.
  if (command.action === 'recovery_next') return this.recovery();
  if (command.action === 'transform_outcome' || command.action === 'abort_transform') return { evidence: { kind: 'outcome', outcome: this.outcome(command.job_id, command.export_id, command.action === 'abort_transform') } };
  if (command.action === 'settle_export') return { evidence: this.settle(this.host.transactionSync(() => this.export(command.export_id)), command.outcome) };
  if (command.action === 'accept_peer' && command.evidence.kind === 'outcome') return this.accept(command.job_id, command.expected_generation, command.evidence);
  this.host.transactionSync(() => this.actor());
  if (command.action === 'begin_transform') {
   const call = command.call; if (call.account_id !== this.host.accountId) fail(403, 'permission_denied');
   let op: TransformCommand; try { op = normalizeForgeTransformCommand({ account_id: call.account_id, operation: call.operation, view: call.view, request_id: call.request_id, payload_json: call.payload_json }) as TransformCommand; } catch (error) { if (error instanceof Failure) throw error; fail(400, 'invalid_argument'); }
   if (op.operation === 30 || op.operation === 32) return this.advance(this.begin(call));
   const row = this.host.transactionSync(() => this.run((op.payload as { run_id: string }).run_id));
   if (op.operation === 31 || op.operation === 35) return this.originQuery(row, op.operation);
   if (op.operation === 33) return this.resolvePin(row, op.payload);
   if (op.operation === 34) return this.retire(call, op.payload.run_id);
   fail(400, 'invalid_argument');
  }
  if (command.action === 'advance_transform') { const d = this.host.transactionSync(() => this.draft(command.job_id)); if (d.generation !== command.expected_generation) fail(409, 'aborted'); return this.advance(d); }
  if (command.action === 'accept_peer') return this.accept(command.job_id, command.expected_generation, command.evidence);
  if (command.action === 'begin_export') return { evidence: await this.beginExport(command.draft, command.payload) };
  if (command.action === 'export_page') return { evidence: this.exportPage(this.host.transactionSync(() => this.export(command.export_id)), command.cursor) };
  if (command.action === 'export_file') return { evidence: await this.exportFile(this.host.transactionSync(() => this.export(command.export_id)), command.side, command.path) };
  if (command.action === 'arm_export') return { evidence: this.arm(this.host.transactionSync(() => this.export(command.export_id)), command.prepared) };
  if (command.action === 'authorize_origin') return { evidence: this.authorizeOrigin(command.binding) };
  fail(400, 'invalid_argument');
 }
}
interface Mapped { source_relative_path: string; source_canonical_path: string; mapped_path: string; destination_path: string; descriptor: Descriptor; executable: boolean; copy_id: string | null }
function sourceIdentity(e: SourceExportBinding, p: TransformStart): unknown {
 const b = e.draft; return { spec: array(b.spec_id), source_account: e.source_account, before: array(e.before_revision), after: array(e.after_revision), before_view: array(e.before_view), after_view: array(e.after_view),
  before_view_name: e.source_view, after_view_name: e.source_view, destination_account: b.destination_account, destination_view: array(b.destination_view), destination_view_name: b.destination_view_name, destination_base: array(b.destination_base),
  change_id: Array.from(b.change_id.match(/../g)!, h => parseInt(h, 16)), lineage: p.lineage };
}
