/** Current-policy view SQL authority. The embedding supplies a
 * per-request shared account host; no actor/grant/policy comes from payload JSON.
 * Graph retention and projection/read/search are separate owned modules. */
import { fail } from './protocol';
import type { ForgeCall } from './core';

type NativeId = number[];
export interface ViewSpec {
  name: string; root_prefix: string; include_prefixes: string[]; exclude_prefixes: string[];
  visibility: { kind: 'public' | 'members' } | { kind: 'roles'; roles: string[] };
  revision: NativeId; export_allowed: boolean;
}
export interface ViewDescription {
  account_id: string; view: string; view_version: NativeId; revision: NativeId;
  policy_epoch: number; root_prefix: string;
}
export interface PolicySnapshot {
  accountId: string; principal: string | null; epoch: number; publicView: string | null;
  /** Derived ONLY by shared accounts owner/member logic in the same transaction. */
  grant: { is_admin: boolean; member_roles: string[] | null };
}
interface PolicyInput {
  account_id: string; epoch: number; requested_name: string;
  selected_version: NativeId; selected_spec: ViewSpec;
  current_name: string; current_version: NativeId; current_spec: ViewSpec;
  retired: boolean; grant: PolicySnapshot['grant'];
}
interface Normalized {
  operation: 4 | 5 | 6 | 11; fingerprint: string | null;
  publish?: { spec: ViewSpec; version: NativeId; expected_epoch: number; make_default: boolean; follow_main: boolean };
  retire?: { version: NativeId; expected_epoch: number };
  selection?: { version: NativeId | null };
}
export interface ViewCore {
  normalize(input: { account_id: string; operation: number; view: string; payload_json: string }): Normalized;
  resolvePolicy(input: PolicyInput): ViewDescription;
  pathsAllowed(input: { policy: PolicyInput; paths: string[] }): { paths: { path: string; allowed: boolean; could_contain: boolean }[] };
  advance(input: { account_id: string; spec: ViewSpec; revision: NativeId }): { spec: ViewSpec; version: NativeId };
}
export interface ForgeViewHost {
  accountId: string; principal: string | null;
  sql: SqlStorage; core: ViewCore;
  transactionSync<T>(callback: () => T): T;
  // These are shared account authority/replay helpers, not reimplementations.
  policySnapshotInTx(): PolicySnapshot;
  requireAdminInTx(): PolicySnapshot;
  replayInTx(requestId: string, fingerprint: string): unknown | undefined;
  finishInTx(requestId: string, fingerprint: string, result: unknown): unknown;
  bumpEpochInTx(expected: number): number;
  setPublicViewInTx(version: string | null): void;
  accountWireInTx(): Record<string, unknown>;
  currentMainInTx(): { head: string; generation: number };
  /** Graph lane: exact COMMIT descriptor + verified closure/readiness must
   * already exist. R2 presence alone, known hashes or raw visible flags fail. */
  requireCanonicalRootInTx(head: string): void;
  /** Same authoritative graph/root ledger and SQL tx. Must enforce Forge graph
   * budgets, not accidentally accept the experimental raw 512-object ceiling. */
  retainViewRootInTx(version: string, head: string): void;
  /** Release only this exact immutable version and its exact derived projection
   * roots. Preserve immutable view/projection relation records and tombstones. */
  releaseViewRootsInTx(version: string): void;
}
interface VersionRow { version: string; name: string; spec: string; retired: number }
interface NameRow { name: string; current_version: string; follow_main: number }
const MAX_VIEWS = 64;
const MAX_VERSIONS = 1024;
const contexts = new WeakMap<object, { account: string; principal: string | null; version: string; name: string; epoch: number }>();
/** Structural copies fail the private WeakMap check. Only resolveInTx mints a
 * handle; JSON serialization exposes description, never a reusable grant. */
export interface ResolvedView { readonly description: Readonly<ViewDescription> }
function hex(bytes: NativeId): string {
  if (!Array.isArray(bytes) || bytes.length !== 32 || !bytes.every(v => Number.isInteger(v) && v >= 0 && v < 256)) fail(400, 'invalid_argument');
  return bytes.map(byte => byte.toString(16).padStart(2, '0')).join('');
}
function native(value: string): NativeId {
  if (!/^[0-9a-f]{64}$/.test(value)) fail(503, 'failed_precondition');
  return Array.from(value.match(/../g)!, pair => parseInt(pair, 16));
}
function safeEpoch(epoch: number): void {
  if (!Number.isSafeInteger(epoch) || epoch < 1) fail(503, 'failed_precondition');
}

export class ForgeViews {
  constructor(private readonly host: ForgeViewHost) {
    for (const statement of [
      'CREATE TABLE IF NOT EXISTS forge_view_versions(version TEXT PRIMARY KEY, name TEXT NOT NULL, spec TEXT NOT NULL, retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN(0,1)))',
      'CREATE TABLE IF NOT EXISTS forge_view_names(name TEXT PRIMARY KEY, current_version TEXT NOT NULL, follow_main INTEGER NOT NULL CHECK(follow_main IN(0,1)))',
    ]) host.sql.exec(statement);
  }
  private one<T>(query: string, ...bindings: (string | number | null)[]): T | undefined {
    return [...this.host.sql.exec(query, ...bindings)][0] as T | undefined;
  }
  private rows<T>(query: string, ...bindings: (string | number | null)[]): T[] {
    return [...this.host.sql.exec(query, ...bindings)] as T[];
  }
  private snapshot(): PolicySnapshot {
    const snapshot = this.host.policySnapshotInTx(); safeEpoch(snapshot.epoch);
    if (snapshot.accountId !== this.host.accountId || snapshot.principal !== this.host.principal) fail(503, 'failed_precondition');
    return snapshot;
  }
  private version(version: string): VersionRow {
    const row = this.one<VersionRow>('SELECT version,name,spec,retired FROM forge_view_versions WHERE version=?', version);
    if (!row) fail(403, 'permission_denied');
    if (row.version !== version || ![0, 1].includes(Number(row.retired))) fail(503, 'failed_precondition');
    return row;
  }
  private policy(name: string, requestedVersion?: string): { input: PolicyInput; description: ViewDescription; principal: string | null } {
    const snapshot = this.snapshot();
    const selected = requestedVersion ?? (name === '' ? snapshot.publicView : this.one<NameRow>('SELECT name,current_version,follow_main FROM forge_view_names WHERE name=?', name)?.current_version);
    if (!selected) fail(403, 'permission_denied');
    const row = this.version(selected);
    const current = this.one<NameRow>('SELECT name,current_version,follow_main FROM forge_view_names WHERE name=?', row.name);
    if (!current || current.name !== row.name) fail(403, 'permission_denied');
    const currentRow = this.version(current.current_version);
    if (Number(currentRow.retired) !== 0) fail(503, 'failed_precondition');
    let spec: ViewSpec; let currentSpec: ViewSpec;
    try { spec = JSON.parse(row.spec); currentSpec = JSON.parse(currentRow.spec); }
    catch { fail(503, 'failed_precondition'); }
    const input: PolicyInput = { account_id: snapshot.accountId, epoch: snapshot.epoch, requested_name: name,
      selected_version: native(row.version), selected_spec: spec, current_name: current.name,
      current_version: native(currentRow.version), current_spec: currentSpec,
      retired: Number(row.retired) === 1, grant: snapshot.grant };
    let description: ViewDescription;
    try { description = this.host.core.resolvePolicy(input); }
    catch (error) {
      if (error instanceof Error && error.message === 'permission_denied') fail(403, 'permission_denied');
      fail(503, 'failed_precondition');
    }
    return { input, description, principal: snapshot.principal };
  }
  /** Call only under host SQL transaction. No handle is cached on the DO. */
  resolveInTx(name: string, requestedVersion?: NativeId): ResolvedView {
    const resolved = this.policy(name, requestedVersion === undefined ? undefined : hex(requestedVersion));
    const description = JSON.parse(JSON.stringify(resolved.description)) as ViewDescription;
    Object.freeze(description.view_version); Object.freeze(description.revision); Object.freeze(description);
    const handle = Object.freeze({ description });
    contexts.set(handle, { account: this.host.accountId, principal: resolved.principal,
      version: hex(resolved.description.view_version), name: resolved.description.view, epoch: resolved.description.policy_epoch });
    return handle;
  }
  /** Read host calls this inside its resolve transaction, then performs actual
   * projection/README assembly. The returned handle never becomes RPC JSON. */
  resolveCallInTx(call: ForgeCall): ResolvedView {
    if (call.account_id !== this.host.accountId || call.operation !== 6) fail(400, 'invalid_argument');
    let command: Normalized;
    try { command = this.host.core.normalize({ account_id: call.account_id, operation: call.operation, view: call.view, payload_json: call.payload_json }); }
    catch { fail(400, 'invalid_argument'); }
    if (command.operation !== 6 || !command.selection) fail(503, 'failed_precondition');
    return this.resolveInTx(call.view, command.selection.version ?? undefined);
  }
  /** Every R2 continuation must use this before returning bytes. Re-resolve the
   * actual current named pointer; a follow-main change alone does not bump ACL. */
  checkInTx(view: ResolvedView): void { this.policyForProjectionInTx(view); }
  /** Trusted projection/read host use ONLY; do not expose PolicyInput over HTTP. */
  policyForProjectionInTx(view: ResolvedView): PolicyInput {
    const saved = contexts.get(view);
    if (!saved || saved.account !== this.host.accountId || saved.principal !== this.host.principal) fail(403, 'permission_denied');
    const fresh = this.policy(saved.name, saved.version);
    if (fresh.description.policy_epoch !== saved.epoch) fail(409, 'aborted');
    return fresh.input;
  }
  pathsAllowedInTx(view: ResolvedView, paths: string[]): ReturnType<ViewCore['pathsAllowed']> {
    const policy = this.policyForProjectionInTx(view);
    try { return this.host.core.pathsAllowed({ policy, paths }); }
    catch { fail(400, 'invalid_argument'); }
  }
  private listInTx(): ViewDescription[] {
    // Even an empty list must resolve the current account/actor binding.
    this.snapshot();
    const names = this.rows<NameRow>('SELECT name,current_version,follow_main FROM forge_view_names ORDER BY name LIMIT 65');
    if (names.length > MAX_VIEWS) fail(503, 'failed_precondition');
    const visible: ViewDescription[] = [];
    for (const row of names) {
      try { visible.push(this.policy(row.name, row.current_version).description); }
      catch (error) {
        // Only authorization denial is a filtered row; corrupt SQL is fatal.
        if (!(error instanceof Error && 'status' in error && (error as { status: unknown }).status === 403)) throw error;
      }
    }
    return visible;
  }
  private reserveVersion(spec: ViewSpec, version: string): void {
    const current = this.one<VersionRow>('SELECT version,name,spec,retired FROM forge_view_versions WHERE version=?', version);
    if (current?.retired) fail(400, 'failed_precondition');
    if (current && (current.name !== spec.name || JSON.stringify(JSON.parse(current.spec)) !== JSON.stringify(spec))) fail(503, 'failed_precondition');
    if (!current) {
      const count = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM forge_view_versions')!;
      if (Number(count.n) >= MAX_VERSIONS) fail(429, 'resource_exhausted');
      this.host.sql.exec('INSERT INTO forge_view_versions(version,name,spec,retired) VALUES(?,?,?,0)', version, spec.name, JSON.stringify(spec));
    }
  }
  private publishInTx(command: NonNullable<Normalized['publish']>): ViewDescription {
    const snapshot = this.snapshot();
    if (snapshot.epoch !== command.expected_epoch) fail(409, 'aborted');
    const version = hex(command.version); const revision = hex(command.spec.revision);
    this.host.requireCanonicalRootInTx(revision);
    const old = this.one<NameRow>('SELECT name,current_version,follow_main FROM forge_view_names WHERE name=?', command.spec.name);
    if (!old && Number(this.one<{ n: number }>('SELECT COUNT(*) AS n FROM forge_view_names')!.n) >= MAX_VIEWS) fail(429, 'resource_exhausted');
    this.reserveVersion(command.spec, version);
    this.host.retainViewRootInTx(version, revision);
    let publicVersion = snapshot.publicView;
    if (publicVersion) {
      const previous = this.version(publicVersion);
      if (previous.name === command.spec.name) publicVersion = command.spec.visibility.kind === 'public' ? version : null;
    }
    if (command.make_default) publicVersion = version;
    this.host.sql.exec('INSERT INTO forge_view_names(name,current_version,follow_main) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET current_version=excluded.current_version,follow_main=excluded.follow_main', command.spec.name, version, command.follow_main ? 1 : 0);
    this.host.setPublicViewInTx(publicVersion);
    const epoch = this.host.bumpEpochInTx(snapshot.epoch);
    return { account_id: this.host.accountId, view: command.spec.name, view_version: command.version,
      revision: command.spec.revision, policy_epoch: epoch, root_prefix: command.spec.root_prefix };
  }
  private retireInTx(command: NonNullable<Normalized['retire']>, name: string): Record<string, unknown> {
    const snapshot = this.snapshot(); if (snapshot.epoch !== command.expected_epoch) fail(409, 'aborted');
    const version = hex(command.version); const row = this.version(version);
    if (name !== '' && name !== row.name) fail(400, 'invalid_argument');
    if (snapshot.publicView === version || this.one('SELECT name FROM forge_view_names WHERE current_version=? LIMIT 1', version)) fail(400, 'failed_precondition');
    if (Number(row.retired) === 0) {
      this.host.releaseViewRootsInTx(version);
      this.host.sql.exec('UPDATE forge_view_versions SET retired=1 WHERE version=?', version);
      this.host.bumpEpochInTx(snapshot.epoch);
    }
    return { ...this.host.accountWireInTx(), retired_version: command.version };
  }
  /** Future canonical landing invokes this INSIDE its existing publication tx;
   * this method deliberately never opens a nested transaction or changes epoch. */
  advanceFollowingViewsInTx(newMain: string): void {
    const snapshot = this.snapshot(); this.host.requireCanonicalRootInTx(newMain);
    const following = this.rows<NameRow>('SELECT name,current_version,follow_main FROM forge_view_names WHERE follow_main=1 ORDER BY name LIMIT 65');
    if (following.length > MAX_VIEWS) fail(503, 'failed_precondition');
    const plan: { spec: ViewSpec; version: string; old: string }[] = [];
    let additional = 0;
    for (const named of following) {
      const old = this.version(named.current_version); if (Number(old.retired) !== 0) fail(503, 'failed_precondition');
      const spec = JSON.parse(old.spec) as ViewSpec;
      if (hex(spec.revision) === newMain) continue;
      const next = this.host.core.advance({ account_id: this.host.accountId, spec, revision: native(newMain) });
      const version = hex(next.version); const existing = this.one<VersionRow>('SELECT version,name,spec,retired FROM forge_view_versions WHERE version=?', version);
      if (existing?.retired) fail(400, 'failed_precondition');
      if (!existing) additional++;
      plan.push({ spec: next.spec, version, old: old.version });
    }
    if (Number(this.one<{ n: number }>('SELECT COUNT(*) AS n FROM forge_view_versions')!.n) + additional > MAX_VERSIONS) fail(429, 'resource_exhausted');
    let publicVersion = snapshot.publicView;
    for (const next of plan) {
      this.reserveVersion(next.spec, next.version);
      this.host.retainViewRootInTx(next.version, newMain);
      this.host.sql.exec('UPDATE forge_view_names SET current_version=? WHERE name=?', next.version, next.spec.name);
      if (publicVersion === next.old) publicVersion = next.version;
    }
    if (publicVersion !== snapshot.publicView) this.host.setPublicViewInTx(publicVersion);
  }
  async handle(call: ForgeCall): Promise<{ state: 'complete'; result: unknown }> {
    if (call.account_id !== this.host.accountId) fail(403, 'permission_denied');
    let command: Normalized;
    try { command = this.host.core.normalize({ account_id: call.account_id, operation: call.operation, view: call.view, payload_json: call.payload_json }); }
    catch { fail(400, 'invalid_argument'); }
    if (command.operation !== call.operation) fail(503, 'failed_precondition');
    return this.host.transactionSync(() => {
      if (command.operation === 5) return { state: 'complete', result: this.listInTx() };
      if (command.operation === 6) {
        // ResolveView's projection/README assembly belongs to the read host. This
        // module's resolveInTx returns its opaque capability, not a fake snapshot.
        fail(501, 'unimplemented');
      }
      this.host.requireAdminInTx();
      if (!/^[A-Za-z0-9_.-]{1,128}$/.test(call.request_id) || typeof command.fingerprint !== 'string' || !/^[0-9a-f]{64}$/.test(command.fingerprint)) fail(400, 'invalid_argument');
      const replay = this.host.replayInTx(call.request_id, command.fingerprint);
      if (replay !== undefined) return { state: 'complete', result: replay };
      const result = command.operation === 4 && command.publish ? this.publishInTx(command.publish)
        : command.operation === 11 && command.retire ? this.retireInTx(command.retire, call.view) : fail(503, 'failed_precondition');
      return { state: 'complete', result: this.host.finishInTx(call.request_id, command.fingerprint, result) };
    });
  }
}
