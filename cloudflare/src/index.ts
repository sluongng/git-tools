import { inspect, validateClosure, forgeViewCore, normalizeForgeAccountCommand, validateCheckerReceipt, type Descriptor, type Inspection, type ObjectKey, type ForgeCall } from './core';
import { authenticate, admitsForgeAccount, body, digest, fail, Failure, hash, identifier, input, integer, json, name, protect, record, requestId, type Auth } from './protocol';
import { forgeReply, protectForge, readForgeCall } from './forge';
import { ForgeAccounts, hasForgeAccount, type CanonicalAccountObject } from './forge_accounts';
import { ForgeViews } from './forge_views';
import { ForgeProjections } from './forge_projection';
import { ForgeReads } from './forge_reads';
import { ForgeStorage } from './forge_storage';
import { ClosureJobs } from './closure_jobs';
import { BootstrapImports } from './bootstrap_import';
import { ForgeSearch } from './forge_search';
import { staticUi } from './static_ui';
import { ForgeUploads, requireOwnedUploadInTx } from './forge_uploads';
import { ForgeReviews, requireReviewProjectionSourceInTx, reviewCheckSelectionsInTx, requireReviewCheckCandidateInTx } from './forge_reviews';
import { ForgeChecks, type CheckProfile, type ExecutionPolicy, type ReviewCheckCandidate } from './forge_checks';
import { CHECKER_ROUTE, admitChecker, checkerReply, protectChecker, requireCheckerProfile } from './checker';
import { DurableObject } from 'cloudflare:workers';
import { requireDelegatedForgeCredential } from './protocol';
import { ForgeTransforms, type TrustedAuth, type TransformRpcCommand, type TransformRpcResult } from './forge_transforms';

interface Env {
  ASSETS?: Fetcher;
  REPOSITORIES: DurableObjectNamespace<AccountRepository>;
  OBJECTS: R2Bucket;
  PRINCIPALS_JSON: string;
  EXPERIMENTAL_RAW_BACKEND: string;
  FORGE_BACKEND?: string;
  FORGE_REVIEW_CONFIG_JSON?: string;
  FORGE_CHECK_POLICY_JSON?: string;
  CHECKERS_JSON?: string;
}
interface TransformRpcFrame { schema: 1; account_id: string; auth: TrustedAuth | null; command: TransformRpcCommand }
type TransformRpcEnvelope = { ok: true; value: TransformRpcResult } | { ok: false; status: number; code: string };

async function transformHop(env: Env, account: string, auth: TrustedAuth | null, command: TransformRpcCommand): Promise<TransformRpcResult> {
  const id = env.REPOSITORIES.idFromName(`native-account-v1:${identifier(account)}`);
  const reply = record(await env.REPOSITORIES.get(id).transformRpc({ schema: 1, account_id: account, auth, command }), ['ok', 'value', 'status', 'code'], ['ok']);
  if (reply.ok === true) {
    record(reply, ['ok', 'value']);
    return record(reply.value, ['result', 'next', 'evidence'], []) as TransformRpcResult;
  }
  record(reply, ['ok', 'status', 'code']);
  if (reply.ok !== false || typeof reply.status !== 'number' || !Number.isInteger(reply.status)
      || reply.status < 400 || reply.status > 599 || typeof reply.code !== 'string' || !/^[a-z_]{1,80}$/.test(reply.code)) fail(503, 'unavailable');
  fail(reply.status, reply.code);
}
/** No account queue is held across this peer hop. A request advances only one
 * peer quantum; all authority and continuation state live in the account SQL. */
async function transformQuantum(env: Env, account: string, auth: TrustedAuth | null, command: TransformRpcCommand): Promise<TransformRpcResult> {
  const local = await transformHop(env, account, auth, command);
  return applyTransformPeer(env, auth, local, command => transformHop(env, account, auth, command));
}
async function applyTransformPeer(env: Env, auth: TrustedAuth | null, local: TransformRpcResult,
  apply: (command: TransformRpcCommand) => Promise<TransformRpcResult>): Promise<TransformRpcResult> {
  if (!local.next) return local;
  const next = local.next;
  let peer: TransformRpcResult;
  try { peer = await transformHop(env, next.account_id, auth, next.command); }
  catch (error) {
    if (next.command.action !== 'authorize_origin') throw error;
    // Denied or unavailable source metadata is redacted, not replaced with a
    // source identifier or a claim that retained destination bytes are gone.
    peer = { evidence: { kind: 'origin', binding: next.command.binding, available: false } };
  }
  if (!peer.evidence) fail(503, 'unavailable');
  const applied = await apply({ action: 'accept_peer',
    job_id: next.continuation.job_id, expected_generation: next.continuation.expected_generation, evidence: peer.evidence });
  return applied.result ? applied : { ...applied, result: local.result };
}
type Row = Record<string, string | number | null>;
const MAX_OBJECT = 1024 * 1024;
const MAX_METADATA = 256 * 1024;
const MAX_ACCOUNT_BYTES = 32 * 1024 * 1024;
const MAX_OBJECTS = 512;
const MAX_REQUESTS = 2048;
const ROOT = '0'.repeat(64);
const capabilities = {
  schema_version: 1, experimental: true, backend: 'cloudflare-native-raw',
  account_monorepos: true, immutable_objects: true, reference_cas: true, durable_pins: true,
  raw_account_access: 'read_write', operator_adds: 'repository_provision',
  public_reads: false, views: false, review: false, checked_landing: false,
  query: false, transforms: false, cloud_workspace: false, garbage_collection: false,
  limits: { object_bytes: MAX_OBJECT, metadata_bytes: MAX_METADATA, account_bytes: MAX_ACCOUNT_BYTES,
    objects: MAX_OBJECTS, request_receipts: MAX_REQUESTS, closure_objects: 256, closure_metadata_bytes: 1024 * 1024 },
};
function route(request: Request): { account?: string; operation: string; parts: string[] } {
  const url = new URL(request.url);
  if (url.search || url.hash || url.pathname.includes('%')) fail(400, 'noncanonical_route');
  if (url.pathname === '/v1/capabilities' && request.method === 'GET') return { operation: 'capabilities', parts: [] };
  const parts = url.pathname.split('/');
  if (parts[0] !== '' || parts[1] !== 'v1' || parts[2] !== 'accounts' || parts.length < 5) fail(501, 'unsupported_operation');
  return { account: identifier(parts[3]), operation: parts[4], parts: parts.slice(5) };
}
function objectKey(parts: string[]): ObjectKey {
  if (parts.length !== 2 || !/^[1-5]$/.test(parts[0])) fail(400, 'invalid_object_key');
  return { kind: Number(parts[0]), id: digest(parts[1]) };
}
function r2key(account: string, key: ObjectKey): string { return `native/v1/${account}/${key.kind}/${key.id}`; }
function nativeId(bytes: number[]): string {
  if (!Array.isArray(bytes) || bytes.length !== 32 || !bytes.every(b => Number.isInteger(b) && b >= 0 && b < 256)) fail(503, 'failed_precondition');
  return bytes.map(b => b.toString(16).padStart(2, '0')).join('');
}
/** Admission only. Both ingress and DO call this independently; optional auth
 * never downgrades an invalid supplied credential to an anonymous reader. */
async function admitForge(request: Request, env: Env): Promise<{ auth: Auth | null; call: ForgeCall; bytes: Uint8Array }> {
  if (env.FORGE_BACKEND !== 'enabled') fail(503, 'unavailable');
  const auth = request.headers.has('authorization') ? await authenticate(request, env.PRINCIPALS_JSON) : null;
  if (auth && auth.kind !== 'forge') fail(403, 'permission_denied');
  const framed = await readForgeCall(request);
  if (auth) {
    if (!auth.accounts.includes(framed.call.account_id)) fail(403, 'permission_denied');
  } else {
    if (![5, 6, 7, 8, 9].includes(framed.call.operation)) fail(401, 'unauthenticated');
    if (!admitsForgeAccount(env.PRINCIPALS_JSON, framed.call.account_id)) fail(403, 'permission_denied');
  }
  return { auth, ...framed };
}
function sameDescriptor(expected: Descriptor, actual: Descriptor): void {
  if (expected.kind !== actual.kind || expected.id !== actual.id || expected.size_bytes !== actual.size_bytes || actual.format_version !== 1) fail(400, 'native_descriptor_mismatch');
}
function nativeInspect(kind: number, bytes: Uint8Array): Inspection {
  try { return inspect(kind, bytes); } catch { fail(400, 'invalid_native_object'); }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const presentation = await staticUi(request, env.ASSETS);
    if (presentation) return presentation;
    const path = new URL(request.url).pathname;
    if (path === CHECKER_ROUTE) {
      return protectChecker(async () => {
        const { call, bytes } = await admitChecker(request, env);
        const id = env.REPOSITORIES.idFromName(`native-account-v1:${call.account_id}`);
        return env.REPOSITORIES.get(id).fetch(new Request(request, { body: bytes }));
      });
    }
    if (path === '/api/v1/forge/call') {
      return protectForge(async () => {
        const { auth, call, bytes } = await admitForge(request, env);
        if (call.operation >= 30 && call.operation <= 35) {
          if (!auth) fail(401, 'unauthenticated');
          const step = await transformQuantum(env, call.account_id,
            { principal: auth.principal, credential_id: auth.credential_id }, { action: 'begin_transform', call });
          if (!step.result) fail(503, 'unavailable');
          return forgeReply(call, step.result.state, step.result.result);
        }
        const id = env.REPOSITORIES.idFromName(`native-account-v1:${call.account_id}`);
        return env.REPOSITORIES.get(id).fetch(new Request(request, { body: bytes }));
      });
    }
    if (path === '/api' || path.startsWith('/api/')) return protectForge(async () => fail(501, 'unimplemented'));
    return protect(async () => {
      if (env.EXPERIMENTAL_RAW_BACKEND !== 'enabled') fail(503, 'experimental_backend_disabled');
      const auth = await authenticate(request, env.PRINCIPALS_JSON);
      if (auth.kind !== 'raw') fail(403, 'raw_identity_required');
      const parsed = route(request);
      if (parsed.operation === 'capabilities') return json(capabilities);
      if (!auth.accounts.includes(parsed.account!)) fail(403, 'account_access_denied');
      const id = env.REPOSITORIES.idFromName(`native-account-v1:${parsed.account}`);
      return env.REPOSITORIES.get(id).fetch(request);
    });
  },
};

/** One SQLite authority per account. R2 is immutable payload storage, not a ref authority. */
class RepositoryEngine {
  private sql: SqlStorage;
  private tail: Promise<unknown> = Promise.resolve();
  private active = 0;
  constructor(private ctx: DurableObjectState, private env: Env) {
    this.sql = ctx.storage.sql;
    for (const statement of [
      'CREATE TABLE IF NOT EXISTS repository (singleton INTEGER PRIMARY KEY CHECK(singleton=1), account TEXT NOT NULL)',
      'CREATE TABLE IF NOT EXISTS objects (kind INTEGER NOT NULL, id TEXT NOT NULL, size INTEGER NOT NULL, inspection TEXT NOT NULL, visible INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(kind,id))',
      'CREATE TABLE IF NOT EXISTS requests (id TEXT PRIMARY KEY, principal TEXT NOT NULL, fingerprint TEXT NOT NULL, status INTEGER, result TEXT)',
      'CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, generation INTEGER NOT NULL, head TEXT NOT NULL)',
      'CREATE TABLE IF NOT EXISTS pins (name TEXT PRIMARY KEY, head TEXT NOT NULL, active INTEGER NOT NULL)',
      'CREATE TABLE IF NOT EXISTS forge_validated_roots(kind INTEGER NOT NULL, id TEXT NOT NULL, proof TEXT NOT NULL, PRIMARY KEY(kind,id))',
      'CREATE TABLE IF NOT EXISTS forge_retained_roots(owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL, kind INTEGER NOT NULL, id TEXT NOT NULL, active INTEGER NOT NULL CHECK(active IN(0,1)), PRIMARY KEY(owner_kind,owner_id,kind,id))',
    ]) this.sql.exec(statement);
  }
  async fetch(request: Request): Promise<Response> {
    // Includes R2 awaits: two admitted uploads cannot oversubscribe durable quota,
    // and reads never observe an uncommitted payload. Eviction preserves SQL intents.
    const forge = new URL(request.url).pathname.startsWith('/api/v1/forge/');
    const checker = new URL(request.url).pathname === CHECKER_ROUTE;
    if (this.active >= 16 && checker) return json({ schema: 1, error: { code: 'resource_exhausted' } }, 429);
    if (this.active >= 16) return forge
      ? json({ version: 1, error: { code: 'resource_exhausted', message: 'account_admission_limit' } }, 429)
      : json({ error: 'account_admission_limit' }, 429);
    return this.enqueue(() => checker ? protectChecker(() => this.handleChecker(request)) : forge ? protectForge(() => this.handleForge(request)) : protect(() => this.handle(request)));
  }
  private enqueue<T>(action: () => Promise<T>): Promise<T> {
    this.active++;
    const work = this.tail.then(action).finally(() => { this.active--; });
    this.tail = work.then(() => undefined, () => undefined);
    return work;
  }
  transformRpc(frame: TransformRpcFrame, maintainAlarm = true): Promise<TransformRpcResult> {
    if (this.active >= 16) fail(429, 'resource_exhausted');
    return this.enqueue(async () => {
      try {
      if (frame.schema !== 1 || identifier(frame.account_id) !== frame.account_id || !hasForgeAccount(this.sql)) fail(403, 'permission_denied');
      const repository = this.one('SELECT account FROM repository WHERE singleton=1');
      if (repository?.account !== frame.account_id) fail(403, 'permission_denied');
      if (frame.auth !== null) requireDelegatedForgeCredential(this.env.PRINCIPALS_JSON, frame.auth, frame.account_id);
      else if (!['recovery_next', 'transform_outcome', 'abort_transform', 'settle_export', 'accept_peer'].includes(frame.command.action)) fail(403, 'permission_denied');
      return await this.forgeTransforms(frame.account_id, frame.auth?.principal ?? null, frame.auth).rpc(frame.command);
      } finally {
        // Maintenance is part of admitted work. A rejected RPC must not enqueue
        // a second, unbounded stream of storage operations behind this queue.
        if (maintainAlarm) await this.updateRecoveryAlarm(30_000);
      }
    });
  }
  recoveryAccount(): Promise<string | null> {
    return this.enqueue(async () => {
      const row = this.one('SELECT account FROM repository WHERE singleton=1');
      return row ? String(row.account) : null;
    });
  }
  refreshRecoveryAlarm(delay: number): Promise<void> {
    return this.enqueue(() => this.updateRecoveryAlarm(delay));
  }
  private async updateRecoveryAlarm(delay: number): Promise<void> {
      const exports = this.one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='transform_exports'")
        && this.one("SELECT 1 FROM transform_exports WHERE phase NOT IN ('settled_committed','settled_aborted') LIMIT 1");
      const drafts = this.one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='transform_drafts'")
        && this.one("SELECT 1 FROM transform_drafts WHERE phase NOT IN ('committed','aborted','limit') LIMIT 1");
      if (exports || drafts) {
        if (await this.ctx.storage.getAlarm() === null) await this.ctx.storage.setAlarm(Date.now() + delay);
      } else await this.ctx.storage.deleteAlarm();
  }
  private rows(query: string, ...bindings: (string | number | null)[]): Row[] {
    return [...this.sql.exec(query, ...bindings)] as Row[];
  }
  private one(query: string, ...bindings: (string | number | null)[]): Row | undefined { return this.rows(query, ...bindings)[0]; }
  private replay(id: string, auth: Auth, fingerprint: string): Response | undefined {
    const row = this.one('SELECT * FROM requests WHERE id=?', id);
    if (!row) return undefined;
    if (row.principal !== auth.principal || row.fingerprint !== fingerprint) fail(409, 'request_replay_mismatch');
    if (row.result !== null) return json(JSON.parse(String(row.result)), Number(row.status));
    return undefined;
  }
  private reserve(id: string, auth: Auth, fingerprint: string): void {
    if (this.one('SELECT id FROM requests WHERE id=?', id)) return;
    if (Number(this.one('SELECT COUNT(*) AS n FROM requests')!.n) >= MAX_REQUESTS) fail(429, 'request_quota');
    this.sql.exec('INSERT INTO requests(id,principal,fingerprint) VALUES(?,?,?)', id, auth.principal, fingerprint);
  }
  private finish(id: string, value: unknown, status = 200): Response {
    this.sql.exec('UPDATE requests SET status=?, result=? WHERE id=?', status, JSON.stringify(value), id);
    return json(value, status);
  }
  private async handle(request: Request): Promise<Response> {
    if (this.env.EXPERIMENTAL_RAW_BACKEND !== 'enabled') fail(503, 'experimental_backend_disabled');
    // Re-derive identity for every call; neither request JSON nor a cached grant
    // can impersonate a principal. The DO binding itself is not a public route.
    const auth = await authenticate(request, this.env.PRINCIPALS_JSON);
    if (auth.kind !== 'raw') fail(403, 'raw_identity_required');
    const parsed = route(request); const account = parsed.account!;
    if (!account || !auth.accounts.includes(account)) fail(403, 'account_access_denied');
    // No raw canonical read, upload, ref or pin operation may bypass Forge
    // policy, even for an operator or a formerly raw account grant.
    if (hasForgeAccount(this.sql)) fail(403, 'forge_managed_account');
    const repository = this.one('SELECT account FROM repository WHERE singleton=1');
    if (repository && repository.account !== account) fail(403, 'account_binding_mismatch');
    if (parsed.operation === 'repository' && parsed.parts.length === 0) {
      if (request.method === 'POST') {
        if (!auth.operator) fail(403, 'operator_required');
        const value = record(await input(request), ['request_id']); const id = requestId(value.request_id);
        const fingerprint = await hash(JSON.stringify(['provision', account, auth.principal]));
        const replay = this.replay(id, auth, fingerprint); if (replay) return replay;
        return this.ctx.storage.transactionSync(() => {
          this.reserve(id, auth, fingerprint);
          this.sql.exec('INSERT OR IGNORE INTO repository(singleton,account) VALUES(1,?)', account);
          return this.finish(id, { state: 'complete', account_id: account, repository_id: `forge-${account}` });
        });
      }
      if (request.method === 'GET') {
        if (!repository) fail(404, 'repository_not_found');
        return json({ account_id: account, repository_id: `forge-${account}` });
      }
    }
    if (!repository) fail(404, 'repository_not_found');
    if (parsed.operation === 'objects') {
      const key = objectKey(parsed.parts);
      if (request.method === 'PUT') return this.put(request, account, auth, key);
      if (request.method === 'GET') {
        const object = this.one('SELECT * FROM objects WHERE kind=? AND id=? AND visible=1', key.kind, key.id);
        if (!object) fail(404, 'object_not_found');
        const bytes = await this.read(account, key, Number(object.size));
        return new Response(bytes, { headers: { 'content-type': 'application/octet-stream', 'content-length': String(bytes.byteLength),
          'cache-control': 'no-store', 'x-tinytree-kind': String(key.kind), 'x-tinytree-id': key.id, 'x-tinytree-format': '1' } });
      }
    }
    if (request.method !== 'POST' || parsed.parts.length !== 1) fail(501, 'unsupported_operation');
    if (parsed.operation === 'refs' && parsed.parts[0] === 'read') {
      const value = record(await input(request), ['names']);
      if (!Array.isArray(value.names) || value.names.length > 64) fail(400, 'reference_limit');
      return json({ references: value.names.map(raw => {
        const ref = name(raw); const row = this.one('SELECT * FROM refs WHERE name=?', ref);
        return { name: ref, generation: row ? Number(row.generation) : 0, head: row ? row.head : null };
      }) });
    }
    if (parsed.operation === 'pins' && parsed.parts[0] === 'read') {
      const value = record(await input(request), ['names']);
      if (!Array.isArray(value.names) || value.names.length > 64) fail(400, 'pin_limit');
      return json({ pins: value.names.map(raw => {
        const pin = name(raw); const row = this.one('SELECT * FROM pins WHERE name=?', pin);
        return row ? { name: pin, head: row.head, active: row.active === 1 } : { name: pin, state: 'not_found' };
      }) });
    }
    if (parsed.operation === 'refs' && parsed.parts[0] === 'cas') return this.cas(request, account, auth);
    if (parsed.operation === 'pins' && ['create', 'release'].includes(parsed.parts[0])) return this.pin(request, account, auth, parsed.parts[0]);
    fail(501, 'unsupported_operation');
  }
  private async handleChecker(request: Request): Promise<Response> {
    const { auth, call } = await admitChecker(request, this.env);
    const repository = this.one('SELECT account FROM repository WHERE singleton=1');
    if (!repository || repository.account !== call.account_id || !hasForgeAccount(this.sql)) fail(403, 'permission_denied');
    const account = call.account_id;
    const storage = new ForgeStorage({ accountId: account, sql: this.sql, transactionSync: callback => this.ctx.storage.transactionSync(callback), objects: this.env.OBJECTS });
    // Only SQL-derived preparers reach this factory. A checker grant is never
    // substituted for membership; each current-authority check remains in SQL.
    const subjects = new Map<string, ForgeReviews>();
    const forPreparer = (principal: string): ForgeReviews => {
      let reviews = subjects.get(principal);
      if (!reviews) {
        const api = this.forgeAccounts(account, principal, false, storage);
        const { views, projections } = this.forgeServices(account, principal, api, storage);
        reviews = this.forgeReviews(account, principal, api, storage, views, projections); subjects.set(principal, reviews);
      }
      return reviews;
    };
    this.sql.exec('CREATE TABLE IF NOT EXISTS check_selection_cursor(singleton INTEGER PRIMARY KEY CHECK(singleton=1),position INTEGER NOT NULL)');
    this.sql.exec('INSERT OR IGNORE INTO check_selection_cursor(singleton,position) VALUES(1,0)');
    const checks = new ForgeChecks({ accountId: account, principal: auth.principal, sql: this.sql,
      transactionSync: callback => this.ctx.storage.transactionSync(callback), nowSeconds: () => Math.floor(Date.now() / 1000),
      authorizeCheckerInTx: profile => requireCheckerProfile(this.env.CHECKERS_JSON, auth, account, profile),
      executionPolicy: profile => this.executionPolicy(profile),
      eligibleCandidatesInTx: (profile, limit) => {
        const cursor = Number(this.one('SELECT position FROM check_selection_cursor WHERE singleton=1')!.position);
        if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > 1024 || limit < 1 || limit > 32) fail(500, 'data_loss');
        let offset = cursor, selections = reviewCheckSelectionsInTx(this.sql, account, profile, limit, offset);
        if (!selections.length && offset !== 0) { offset = 0; selections = reviewCheckSelectionsInTx(this.sql, account, profile, limit, 0); }
        this.sql.exec('UPDATE check_selection_cursor SET position=? WHERE singleton=1', selections.length < limit ? 0 : Math.min(1024, offset + selections.length));
        const result: ReviewCheckCandidate[] = [];
        for (const selected of selections) {
          try { result.push(forPreparer(selected.preparer).checkCandidateInTx(selected.change_id, selected.patchset_id, selected.candidate_id)); }
          catch (error) {
            if (!(error instanceof Failure) || !([403, 409].includes(error.status) || (error.status === 400 && error.code === 'failed_precondition'))) throw error;
          }
        }
        return result;
      },
      recheckCandidateInTx: candidate => {
        requireReviewCheckCandidateInTx(this.sql, account, candidate);
        forPreparer(candidate.preparer).requireCheckCandidateInTx(candidate);
      },
      describeObjectInTx: key => storage.describeInTx(key), readVerifiedObject: key => storage.readVerifiedObject(key),
      retainCheckInTx: (lease, root) => {
        if (root.kind !== 3) fail(400, 'failed_precondition'); this.retainedForgeClosureInTx(account, storage, root);
        this.sql.exec("INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES('checker',?,?,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1", lease, root.kind, root.id);
      },
      releaseCheckInTx: (lease, root) => {
        this.sql.exec("UPDATE forge_retained_roots SET active=0 WHERE owner_kind='checker' AND owner_id=? AND kind=? AND id=?", lease, root.kind, root.id);
      },
    });
    return checkerReply(call, await checks.handle(call));
  }
  private async handleForge(request: Request): Promise<Response> {
    const { auth, call } = await admitForge(request, this.env);
    const repository = this.one('SELECT account FROM repository WHERE singleton=1');
    if (repository && repository.account !== call.account_id) fail(403, 'permission_denied');
    // This host is per request. Never cache an actor/grant on the DO instance.
    const storage = new ForgeStorage({ accountId: call.account_id, sql: this.sql, transactionSync: callback => this.ctx.storage.transactionSync(callback), objects: this.env.OBJECTS });
    const api = this.forgeAccounts(call.account_id, auth?.principal ?? null, auth?.operator ?? false, storage);
    let result;
    if ([20, 21, 22, 23, 24].includes(call.operation)) {
      if (!auth) fail(401, 'unauthenticated');
      const { views, projections } = this.forgeServices(call.account_id, auth.principal, api, storage);
      result = await this.forgeReviews(call.account_id, auth.principal, api, storage, views, projections, auth).handle(call);
    } else if (call.operation === 10) {
      const { views, projections } = this.forgeServices(call.account_id, auth?.principal ?? null, api, storage);
      result = await new ForgeUploads({
        accountId: call.account_id, principal: auth?.principal ?? null, sql: this.sql, views,
        transactionSync: callback => this.ctx.storage.transactionSync(callback),
        nowSeconds: () => Math.floor(Date.now() / 1000),
        requireWriterInTx: () => { api.requireWriterInTx(); },
        projectionMemberInTx: (view, descriptor) => {
          const ready = projections.readyInTx(view);
          if (!ready || !this.one('SELECT 1 FROM projection_members WHERE projection=? AND kind=? AND id=?', ready.catalog_id, descriptor.kind, descriptor.id)) return false;
          const member = projections.memberInTx(view, descriptor).descriptor;
          if (member.size_bytes !== descriptor.size_bytes || member.format_version !== descriptor.format_version) fail(503, 'unavailable');
          return true;
        },
        describeObjectInTx: key => storage.describeInTx(key),
        readVerifiedObject: key => storage.readVerifiedObject(key),
        reserveUploadInTx: (session, descriptor) => storage.reserveInTx('upload', session, descriptor),
        stageUpload: (descriptor, bytes) => storage.stage(descriptor, bytes),
        confirmUploadInTx: (session, descriptor, confirmation) => storage.confirmInTx('upload', session, descriptor, confirmation),
        retainUploadInTx: (session, descriptor, expiresAt) => {
          const available = storage.describeInTx(descriptor);
          if (!available || available.size_bytes !== descriptor.size_bytes || available.format_version !== descriptor.format_version) fail(503, 'unavailable');
          this.sql.exec('INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES(\'upload\',?,?,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1', session, descriptor.kind, descriptor.id);
          return { staging_lease: `upload-${session}`, expires_at: expiresAt };
        },
      }).handle(call);
    } else if ([4, 5, 6, 7, 8, 9, 11].includes(call.operation)) {
      const services = this.forgeServices(call.account_id, auth?.principal ?? null, api, storage);
      if ([4, 5, 11].includes(call.operation)) result = await services.views.handle(call);
      else if (call.operation === 8) result = await new ForgeSearch({
        accountId: call.account_id, principal: auth?.principal ?? null, sql: this.sql,
        views: services.views, projections: services.projections,
        transactionSync: callback => this.ctx.storage.transactionSync(callback),
        readVerifiedObject: key => storage.readVerifiedObject(key),
        nowSeconds: () => Math.floor(Date.now() / 1000),
      }).handle(call);
      else result = await new ForgeReads({
        accountId: call.account_id, sql: this.sql, views: services.views, projections: services.projections,
        transactionSync: callback => this.ctx.storage.transactionSync(callback),
        readVerifiedObject: key => storage.readVerifiedObject(key),
        readRange: (descriptor, offset, length) => storage.readRange(descriptor, offset, length),
      }).handle(call);
    } else if ([40, 41, 42, 43, 44].includes(call.operation)) {
      if (!auth) fail(401, 'unauthenticated');
      result = await this.bootstrapImports(call.account_id, auth.principal, api, storage).handle(call);
    } else result = await api.handle(call);
    return forgeReply(call, result.state, result.result);
  }
  private forgeAccounts(accountId: string, principal: string | null, canProvision: boolean, storage: ForgeStorage): ForgeAccounts {
    const staged = new Set<string>();
    const confirmations = new Map<string, unknown>();
    return new ForgeAccounts({
      accountId: accountId, principal: principal, canProvision: canProvision,
      sql: this.sql, transactionSync: callback => this.ctx.storage.transactionSync(callback),
      stageGenesis: objects => this.stageGenesis(accountId, objects, staged, storage, confirmations),
      claimRepositoryInTx: (account, objects, head) => {
        if (account !== accountId || this.one('SELECT account FROM repository WHERE singleton=1')
            || this.one('SELECT name FROM refs LIMIT 1')) fail(400, 'failed_precondition');
        if (objects.length !== 2 || !objects.some(o => o.kind === 3 && o.id === head)) fail(503, 'unavailable');
        for (const object of objects) {
          const key = `${object.kind}/${object.id}`;
          const row = this.one('SELECT size FROM objects WHERE kind=? AND id=?', object.kind, object.id);
          if (!staged.has(key) || !row || Number(row.size) !== object.size_bytes) fail(503, 'unavailable');
          storage.confirmInTx('genesis', 'genesis', object, confirmations.get(key));
        }
        this.sql.exec('INSERT INTO repository(singleton,account) VALUES(1,?)', account);
        this.sql.exec('INSERT INTO refs(name,generation,head) VALUES(\'main\',1,?)', head);
        // stageGenesis checked this exact complete native closure and confirmed
        // both durable puts. Installing proof/root is atomic with the account.
        this.sql.exec('INSERT INTO forge_validated_roots(kind,id,proof) VALUES(3,?,?)', head, JSON.stringify({ validator: 'native-genesis-v1', root: head }));
        this.sql.exec('INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES(\'main\',\'main\',3,?,1)', head);
      },
      currentMainInTx: () => {
        const main = this.one('SELECT generation,head FROM refs WHERE name=\'main\'');
        if (!main) fail(503, 'unavailable');
        return { head: String(main.head), generation: Number(main.generation) };
      },
    });
  }
  private bootstrapImports(account: string, principal: string, api: ForgeAccounts, storage: ForgeStorage): BootstrapImports {
    // These services are request-scoped and initialized before publication's
    // transaction, never constructed inside the no-await attach callback.
    const { views } = this.forgeServices(account, principal, api, storage);
    let genesis: string | undefined;
    const canonicalGenesis = (): string => {
      if (genesis === undefined) {
        genesis = normalizeForgeAccountCommand({ account_id: account, operation: 1, view: '', payload_json: '{}' }).genesis?.head;
        if (!genesis) fail(503, 'failed_precondition');
      }
      return genesis;
    };
    return new BootstrapImports({
      accountId: account, principal, sql: this.sql,
      transactionSync: callback => this.ctx.storage.transactionSync(callback),
      requireOwnerInTx: () => api.requireOwnerInTx(), canonicalGenesis,
      replayInTx: (id, fingerprint) => api.replayInTx(id, fingerprint),
      finishInTx: (id, fingerprint, result) => api.finishInTx(id, fingerprint, result),
      // Canonical availability is account-shared. The import module separately
      // binds per-attempt typed upload receipts; neither grants read authority.
      reserveObjectInTx: descriptor => storage.reserveInTx('bootstrap', 'bootstrap', descriptor),
      stageObject: (descriptor, bytes) => storage.stage(descriptor, bytes),
      confirmObjectInTx: (descriptor, token) => storage.confirmInTx('bootstrap', 'bootstrap', descriptor, token),
      makeClosureJobs: (binding, attach) => {
        if (binding.account_id !== account || binding.identity.kind !== 'principal' || binding.identity.principal !== principal
            || binding.purpose !== 'bootstrap_import') fail(403, 'permission_denied');
        const expected = Object.freeze({ principal, epoch: binding.policy_epoch, publication: binding.publication });
        return new ClosureJobs({
          binding, sql: this.sql, transactionSync: callback => this.ctx.storage.transactionSync(callback),
          authorizeCurrentInTx: selected => {
            const owner = api.requireOwnerInTx();
            if (selected.account_id !== account || selected.identity.kind !== 'principal' || selected.identity.principal !== expected.principal
                || selected.purpose !== 'bootstrap_import' || selected.policy_epoch !== expected.epoch
                || selected.publication !== expected.publication) fail(403, 'permission_denied');
            if (owner.policy_epoch !== expected.epoch) fail(409, 'aborted');
            if (owner.bootstrap_sealed || owner.main.generation !== 1 || owner.main.head !== canonicalGenesis()) fail(400, 'failed_precondition');
          },
          describeObjectInTx: key => storage.describeInTx(key),
          readVerifiedObject: key => storage.readVerifiedObject(key),
          attachRootInTx: attach,
        });
      },
      attachBootstrapInTx: (record, root, jobId) => {
        const owner = api.requireOwnerInTx();
        if (record.account !== account || record.principal !== principal || record.epoch !== owner.policy_epoch
            || record.job_id !== jobId || root.kind !== 3 || root.id !== record.payload.root.id
            || owner.bootstrap_sealed || owner.main.generation !== 1 || owner.main.head !== canonicalGenesis()
            || record.payload.destination.generation !== 1 || record.payload.destination.head !== owner.main.head) fail(409, 'aborted');
        const proof = this.one('SELECT account,identity,root_kind,root_id,phase,nodes,checked_nodes,contexts,checked_contexts,obligations,checked_obligations FROM closure_jobs WHERE id=?', jobId);
        if (!proof || proof.account !== account || proof.identity !== JSON.stringify({ kind: 'principal', principal })
            || proof.root_kind !== 3 || proof.root_id !== root.id || proof.phase !== 'ready'
            || proof.nodes !== proof.checked_nodes || proof.contexts !== proof.checked_contexts
            || proof.obligations !== proof.checked_obligations) fail(400, 'failed_precondition');
        const updated = this.one('UPDATE refs SET head=?,generation=2 WHERE name=\'main\' AND generation=1 AND head=? RETURNING head,generation', root.id, canonicalGenesis());
        if (!updated) fail(409, 'aborted');
        this.sql.exec('INSERT INTO forge_validated_roots(kind,id,proof) VALUES(3,?,?) ON CONFLICT(kind,id) DO UPDATE SET proof=excluded.proof', root.id,
          JSON.stringify({ validator: 'native-closure-v1', job_id: jobId, root: root.id }));
        this.sql.exec('UPDATE forge_retained_roots SET active=0 WHERE owner_kind=\'main\' AND owner_id=\'main\'');
        this.sql.exec('INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES(\'main\',\'main\',3,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1', root.id);
        views.advanceFollowingViewsInTx(root.id);
        api.sealBootstrapInTx(root.id, record.id, jobId);
        return { import_id: record.id, closure_job_id: jobId, root, declared_source: record.payload.source,
          policy_epoch: record.epoch, main: { head: root.id, generation: 2 }, bootstrap_sealed: true };
      },
    });
  }
  private forgeServices(account: string, principal: string | null, api: ForgeAccounts, storage: ForgeStorage): { views: ForgeViews; projections: ForgeProjections } {
    const requireCanonicalRootInTx = (head: string): void => {
      if (!storage.describeInTx({ kind: 3, id: head })) fail(403, 'permission_denied');
      if (this.one('SELECT id FROM forge_validated_roots WHERE kind=3 AND id=?', head)) return;
      // A fully checked retained history proves each typed COMMIT member's
      // complete closure, not just its tip. Availability alone is insufficient.
      if (!this.one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='closure_jobs'")) fail(400, 'failed_precondition');
      const parent = this.one("SELECT j.id,j.root_kind,j.root_id FROM closure_jobs j JOIN closure_nodes n ON n.job=j.id JOIN forge_retained_roots r ON r.kind=j.root_kind AND r.id=j.root_id AND r.active=1 AND r.owner_kind NOT LIKE 'transform-stage-%' WHERE j.account=? AND n.kind=3 AND n.id=? AND n.done=1 AND j.root_kind=3 AND j.phase IN ('ready','published') AND j.nodes=j.checked_nodes AND j.contexts=j.checked_contexts AND j.obligations=j.checked_obligations ORDER BY j.id LIMIT 1", account, head);
      if (!parent) fail(400, 'failed_precondition');
      this.sql.exec('INSERT INTO forge_validated_roots(kind,id,proof) VALUES(3,?,?)', head,
        JSON.stringify({ validator: 'retained-closure-member-v1', root: head, job_id: parent.id,
          container: { kind: parent.root_kind, id: parent.root_id } }));
    };
    const views = new ForgeViews({
      accountId: account, principal, sql: this.sql, core: forgeViewCore,
      transactionSync: callback => this.ctx.storage.transactionSync(callback),
      policySnapshotInTx: () => api.policySnapshotInTx(), requireAdminInTx: () => api.requireAdminInTx(),
      replayInTx: (id, fingerprint) => api.replayInTx(id, fingerprint),
      finishInTx: (id, fingerprint, result) => api.finishInTx(id, fingerprint, result),
      bumpEpochInTx: expected => api.bumpEpochInTx(expected),
      setPublicViewInTx: version => api.setPublicViewInTx(version),
      accountWireInTx: () => api.accountWireInTx(),
      currentMainInTx: () => {
        const row = this.one('SELECT head,generation FROM refs WHERE name=\'main\'');
        if (!row) fail(503, 'failed_precondition');
        return { head: String(row.head), generation: Number(row.generation) };
      },
      requireCanonicalRootInTx,
      retainViewRootInTx: (version, head) => {
        requireCanonicalRootInTx(head);
        this.sql.exec('INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES(\'view\',?,3,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1', version, head);
      },
      releaseViewRootsInTx: version => {
        this.sql.exec('UPDATE forge_retained_roots SET active=0 WHERE owner_kind=\'view\' AND owner_id=?', version);
        this.sql.exec('UPDATE forge_retained_roots SET active=0 WHERE owner_kind=\'projection\' AND owner_id IN(SELECT id FROM projection_catalogs WHERE version=?)', version);
        this.sql.exec('UPDATE projection_catalogs SET retired=1 WHERE version=?', version);
      },
    });
    const confirmations = new Map<string, unknown>();
    const projections = new ForgeProjections({
      accountId: account, principal, sql: this.sql, views,
      transactionSync: callback => this.ctx.storage.transactionSync(callback),
      requireSourceRootInTx: view => {
        views.checkInTx(view);
        const version = nativeId(view.description.view_version), head = nativeId(view.description.revision);
        requireCanonicalRootInTx(head);
        if (!this.one('SELECT 1 FROM forge_retained_roots WHERE owner_kind=\'view\' AND owner_id=? AND kind=3 AND id=? AND active=1', version, head)) fail(403, 'permission_denied');
      },
      requireReviewSourceInTx: (view, source) => requireReviewProjectionSourceInTx({
        accountId: account, sql: this.sql, views,
        describeObjectInTx: key => storage.describeInTx(key),
        requireRetainedClosureInTx: root => this.retainedForgeClosureInTx(account, storage, root),
      }, view, source),
      describeObjectInTx: key => storage.describeInTx(key),
      readVerifiedObject: key => storage.readVerifiedObject(key),
      reserveGeneratedInTx: (job, descriptor) => storage.reserveInTx('projection', job, descriptor),
      putVerifiedGenerated: async (descriptor, bytes) => { confirmations.set(`${descriptor.kind}/${descriptor.id}`, await storage.stage(descriptor, bytes)); },
      confirmGeneratedInTx: (job, descriptor) => storage.confirmInTx('projection', job, descriptor, confirmations.get(`${descriptor.kind}/${descriptor.id}`)),
      closureForProjection: (view, binding, attach, authorizeCurrent) => {
        views.checkInTx(view);
        if (binding.account_id !== account || binding.identity.kind !== 'view' || binding.identity.view_version !== nativeId(view.description.view_version)
            || binding.policy_epoch !== view.description.policy_epoch
            || !['projection-prepare', 'review-projection-prepare'].includes(binding.purpose)) fail(403, 'permission_denied');
        if (binding.purpose === 'review-projection-prepare' && !authorizeCurrent) fail(403, 'permission_denied');
        const expected = Object.freeze({ version: binding.identity.view_version, epoch: binding.policy_epoch, purpose: binding.purpose, publication: binding.publication });
        return new ClosureJobs({
          binding, sql: this.sql, transactionSync: callback => this.ctx.storage.transactionSync(callback),
          authorizeCurrentInTx: selected => {
            views.checkInTx(view);
            authorizeCurrent?.();
            if (selected.account_id !== account || selected.identity.kind !== 'view' || selected.identity.view_version !== expected.version
                || selected.policy_epoch !== expected.epoch || selected.purpose !== expected.purpose || selected.publication !== expected.publication) fail(403, 'permission_denied');
          },
          describeObjectInTx: key => storage.describeInTx(key),
          readVerifiedObject: key => storage.readVerifiedObject(key),
          attachRootInTx: (_binding, root, job) => { if (root.kind !== 3) fail(400, 'failed_precondition'); return attach(job); },
        });
      },
      retainProjectionRootInTx: (projection, head, proofJob) => {
        const proof = this.one('SELECT root_kind,root_id,phase,nodes,checked_nodes,contexts,checked_contexts,obligations,checked_obligations FROM closure_jobs WHERE id=?', proofJob);
        if (!proof || proof.root_kind !== 3 || proof.root_id !== head || proof.phase !== 'ready'
            || proof.nodes !== proof.checked_nodes || proof.contexts !== proof.checked_contexts || proof.obligations !== proof.checked_obligations) fail(400, 'failed_precondition');
        this.sql.exec('INSERT OR IGNORE INTO forge_validated_roots(kind,id,proof) VALUES(3,?,?)', head, JSON.stringify({ validator: 'native-closure-v1', job_id: proofJob, root: head }));
        this.sql.exec('INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES(\'projection\',?,3,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1', projection, head);
      },
    });
    return { views, projections };
  }
  /** Source availability/retention proof only. Callers separately establish
   * current account/view authority and exact source/candidate bindings. */
  private retainedForgeClosureInTx(account: string, storage: ForgeStorage, root: ObjectKey): { proof_job: string } {
    if (!storage.describeInTx(root)
        || !this.one('SELECT 1 FROM forge_retained_roots WHERE kind=? AND id=? AND active=1 LIMIT 1', root.kind, root.id)) fail(400, 'failed_precondition');
    let job: string | undefined;
    let container: ObjectKey | undefined;
    const saved = this.one('SELECT proof FROM forge_validated_roots WHERE kind=? AND id=?', root.kind, root.id);
    if (saved) {
      let proof: { validator?: string; root?: string; job_id?: string; container?: ObjectKey };
      try { proof = JSON.parse(String(saved.proof)); } catch { fail(500, 'data_loss'); }
      if (proof.root !== root.id) fail(500, 'data_loss');
      // Provisioning independently verified these exact two native objects and
      // durably confirmed both puts. It predates generic closure-job state.
      if (root.kind === 3 && proof.validator === 'native-genesis-v1') return { proof_job: `native-genesis-v1:${root.id}` };
      if (typeof proof.job_id === 'string') job = proof.job_id;
      if (proof.validator === 'retained-closure-member-v1') {
        if (root.kind !== 3 || proof.container?.kind !== 3 || !/^[0-9a-f]{64}$/.test(proof.container.id)) fail(500, 'data_loss');
        container = proof.container;
      }
    }
    if (!job && this.one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='forge_upload_sessions'")) {
      const upload = this.one("SELECT integrity_job FROM forge_upload_sessions WHERE account=? AND kind=? AND object_id=? AND state='committed' AND integrity_job IS NOT NULL LIMIT 1", account, root.kind, root.id);
      if (upload) job = String(upload.integrity_job);
    }
    if (!job) {
      // Private preparation completeness is not canonical publication
      // eligibility. Its exact local custody/proof may be reused internally.
      const staged = this.one("SELECT j.id FROM closure_jobs j JOIN forge_retained_roots r ON r.kind=j.root_kind AND r.id=j.root_id AND r.active=1 AND r.owner_kind IN ('transform-stage-artifact','transform-stage-output') WHERE j.account=? AND j.root_kind=? AND j.root_id=? AND j.phase IN ('ready','published') AND j.nodes=j.checked_nodes AND j.contexts=j.checked_contexts AND j.obligations=j.checked_obligations ORDER BY j.id LIMIT 1", account, root.kind, root.id);
      if (staged) job = String(staged.id);
    }
    if (!job) fail(400, 'failed_precondition');
    const proof = this.one('SELECT account,root_kind,root_id,phase,nodes,checked_nodes,contexts,checked_contexts,obligations,checked_obligations FROM closure_jobs WHERE id=?', job);
    const expected = container ?? root;
    if (!proof || proof.account !== account || proof.root_kind !== expected.kind || proof.root_id !== expected.id
        || !['ready', 'published'].includes(String(proof.phase))
        || proof.nodes !== proof.checked_nodes || proof.contexts !== proof.checked_contexts || proof.obligations !== proof.checked_obligations) fail(400, 'failed_precondition');
    if (container && !this.one('SELECT 1 FROM closure_nodes WHERE job=? AND kind=3 AND id=? AND done=1', job, root.id)) fail(400, 'failed_precondition');
    return { proof_job: job };
  }
  private forgeTransforms(account: string, principal: string | null, auth: TrustedAuth | null): ForgeTransforms {
    const storage = new ForgeStorage({ accountId: account, sql: this.sql,
      transactionSync: callback => this.ctx.storage.transactionSync(callback), objects: this.env.OBJECTS });
    const api = this.forgeAccounts(account, principal, false, storage);
    const { views, projections } = this.forgeServices(account, principal, api, storage);
    return new ForgeTransforms({ accountId: account, principal, auth, sql: this.sql, views, projections,
      transactionSync: callback => this.ctx.storage.transactionSync(callback), nowSeconds: () => Math.floor(Date.now() / 1000),
      requireCredentialInTx: () => {
        if (!auth || auth.principal !== principal) fail(403, 'permission_denied');
        requireDelegatedForgeCredential(this.env.PRINCIPALS_JSON, auth, account);
      },
      authorizeInTx: (action, epoch) => {
        if (action !== 'admin') return api.authorityInTx(action, epoch);
        const snapshot = api.requireAdminInTx();
        if (epoch !== undefined && snapshot.epoch !== epoch) fail(409, 'aborted');
        return { epoch: snapshot.epoch };
      },
      reviewConfiguration: () => this.reviewConfiguration(),
      describeObjectInTx: key => storage.describeInTx(key), readVerifiedObject: key => storage.readVerifiedObject(key),
      reserveGeneratedInTx: (owner, descriptor) => storage.reserveInTx('transform', owner, descriptor),
      stageGenerated: (descriptor, bytes) => storage.stage(descriptor, bytes),
      confirmGeneratedInTx: (owner, descriptor, confirmation) => storage.confirmInTx('transform', owner, descriptor, confirmation),
      requireRetainedClosureInTx: root => this.retainedForgeClosureInTx(account, storage, root),
      retainRootInTx: (ownerKind, owner, root, proofJob) => {
        if (!ownerKind.startsWith('transform-') || !storage.describeInTx(root)) fail(503, 'failed_precondition');
        const proof = this.one('SELECT account,root_kind,root_id,phase,nodes,checked_nodes,contexts,checked_contexts,obligations,checked_obligations FROM closure_jobs WHERE id=?', proofJob);
        if (!proof || proof.account !== account || proof.root_kind !== root.kind || proof.root_id !== root.id
            || !['ready', 'published'].includes(String(proof.phase)) || proof.nodes !== proof.checked_nodes
            || proof.contexts !== proof.checked_contexts || proof.obligations !== proof.checked_obligations) fail(400, 'failed_precondition');
        if (!ownerKind.startsWith('transform-stage-')) this.sql.exec('INSERT INTO forge_validated_roots(kind,id,proof) VALUES(?,?,?) ON CONFLICT(kind,id) DO UPDATE SET proof=excluded.proof',
          root.kind, root.id, JSON.stringify({ validator: 'closure-job-v1', root: root.id, job_id: proofJob }));
        this.sql.exec('INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES(?,?,?,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1', ownerKind, owner, root.kind, root.id);
      },
      releaseRootInTx: (ownerKind, owner, root) => {
        if (!ownerKind.startsWith('transform-')) fail(503, 'failed_precondition');
        this.sql.exec('UPDATE forge_retained_roots SET active=0 WHERE owner_kind=? AND owner_id=? AND kind=? AND id=?', ownerKind, owner, root.kind, root.id);
      },
      pushTransformReview: async (source, expectedPatchset) => {
        if (principal === null) fail(403, 'permission_denied');
        const call: ForgeCall = { version: 1, operation: 20, account_id: account, view: source.destination_view_name,
          request_id: `transform-${source.run_id}`, payload_json: JSON.stringify({ source_base: source.destination_base,
            source_head: source.output_head, view_version: source.destination_view, expected_patchset: expectedPatchset,
            predecessor: null, provenance: source.run_id, lineage: [] }) };
        return this.forgeReviews(account, principal, api, storage, views, projections, auth).handle(call);
      },
    });
  }
  private forgeReviews(account: string, principal: string, api: ForgeAccounts, storage: ForgeStorage, views: ForgeViews, projections: ForgeProjections, auth: TrustedAuth | null = null): ForgeReviews {
    let transforms: ForgeTransforms | undefined;
    const ownedHost = { accountId: account, principal, sql: this.sql, views,
      requireWriterInTx: () => { api.requireWriterInTx(); }, describeObjectInTx: (key: ObjectKey) => storage.describeInTx(key) };
    return new ForgeReviews({ accountId: account, principal, sql: this.sql, views, projections,
      transactionSync: callback => this.ctx.storage.transactionSync(callback), nowSeconds: () => Math.floor(Date.now() / 1000),
      authorizeInTx: (action, epoch, subject) => api.authorityInTx(action, epoch, subject),
      currentMainInTx: () => {
        const main = this.one("SELECT head,generation FROM refs WHERE name='main'");
        if (!main) fail(503, 'unavailable'); return { head: String(main.head), generation: Number(main.generation) };
      },
      reviewConfiguration: () => this.reviewConfiguration(),
      requireTransformReviewInTx: (view, run, base, head) => {
        transforms ??= this.forgeTransforms(account, principal, auth);
        transforms.requireReviewInTx(view, run, base, head);
      },
      requireAcceptedCheckPolicyInTx: candidate => this.requireAcceptedCheckPolicyInTx(account, candidate.scope.candidate_id, candidate.head, candidate.root_tree, candidate.profile),
      describeObjectInTx: key => storage.describeInTx(key), readVerifiedObject: key => storage.readVerifiedObject(key),
      requireOwnedCommitInTx: (view, descriptor, now) => {
        if (descriptor.kind !== 3) fail(403, 'permission_denied');
        return requireOwnedUploadInTx(ownedHost, view, '', descriptor, now);
      },
      requireScopedReferenceInTx: (view, path, descriptor, now) => {
        api.authorityInTx('write', view.description.policy_epoch); views.checkInTx(view);
        if (!(path === '' && (descriptor.kind === 2 || descriptor.kind === 3))) {
          const allowed = views.pathsAllowedInTx(view, [path]);
          if (allowed.paths.length !== 1 || !allowed.paths[0].allowed) fail(403, 'permission_denied');
        }
        try {
          const actual = projections.authorizedReadMemberInTx(view, descriptor).descriptor;
          if (actual.size_bytes !== descriptor.size_bytes || actual.format_version !== descriptor.format_version) fail(503, 'unavailable');
          return Number.MAX_SAFE_INTEGER;
        } catch (error) { if (!(error instanceof Failure) || error.status !== 403) throw error; }
        return requireOwnedUploadInTx(ownedHost, view, path, descriptor, now);
      },
      requireRetainedClosureInTx: root => this.retainedForgeClosureInTx(account, storage, root),
      retainReviewInputInTx: (owner, root, proofJob) => {
        if (this.retainedForgeClosureInTx(account, storage, root).proof_job !== proofJob) fail(400, 'failed_precondition');
        this.sql.exec("INSERT OR IGNORE INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES('review-input',?,?,?,1)", owner, root.kind, root.id);
      },
      reserveGeneratedInTx: (attempt, descriptor) => storage.reserveInTx('review-candidate', attempt, descriptor),
      stageGenerated: (descriptor, bytes) => storage.stage(descriptor, bytes),
      confirmGeneratedInTx: (attempt, descriptor, confirmation) => storage.confirmInTx('review-candidate', attempt, descriptor, confirmation),
      retainCandidateInTx: (attempt, root, proofJob) => {
        const proof = this.one('SELECT account,root_kind,root_id,phase,nodes,checked_nodes,contexts,checked_contexts,obligations,checked_obligations FROM closure_jobs WHERE id=?', proofJob);
        if (!proof || proof.account !== account || proof.root_kind !== root.kind || proof.root_id !== root.id || proof.phase !== 'ready'
            || proof.nodes !== proof.checked_nodes || proof.contexts !== proof.checked_contexts || proof.obligations !== proof.checked_obligations) fail(400, 'failed_precondition');
        this.sql.exec('INSERT INTO forge_validated_roots(kind,id,proof) VALUES(?,?,?) ON CONFLICT(kind,id) DO UPDATE SET proof=excluded.proof', root.kind, root.id,
          JSON.stringify({ validator: 'native-closure-v1', root: root.id, job_id: proofJob }));
        this.sql.exec("INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES('review-candidate',?,?,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1", attempt, root.kind, root.id);
      },
      publishMainInTx: input => {
        api.authorityInTx('write');
        if (!Number.isSafeInteger(input.target_generation) || input.target_generation < 1 || input.target_generation >= Number.MAX_SAFE_INTEGER) fail(429, 'resource_exhausted');
        this.requireAcceptedCheckPolicyInTx(account, input.candidate_id, input.head, input.root_tree, this.reviewConfiguration()?.profile);
        const proof = this.one('SELECT account,root_kind,root_id,phase,nodes,checked_nodes,contexts,checked_contexts,obligations,checked_obligations FROM closure_jobs WHERE id=?', input.proof_job);
        if (!proof || proof.account !== account || proof.root_kind !== 3 || proof.root_id !== input.head || !['ready', 'published'].includes(String(proof.phase))
            || proof.nodes !== proof.checked_nodes || proof.contexts !== proof.checked_contexts || proof.obligations !== proof.checked_obligations) fail(400, 'failed_precondition');
        this.retainedForgeClosureInTx(account, storage, { kind: 3, id: input.head });
        const changed = this.one("UPDATE refs SET head=?,generation=generation+1 WHERE name='main' AND head=? AND generation=? RETURNING head,generation", input.head, input.target_head, input.target_generation);
        if (!changed) fail(409, 'aborted');
        this.sql.exec("UPDATE forge_retained_roots SET active=0 WHERE owner_kind='main' AND owner_id='main'");
        this.sql.exec("INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES('main','main',3,?,1) ON CONFLICT(owner_kind,owner_id,kind,id) DO UPDATE SET active=1", input.head);
        views.advanceFollowingViewsInTx(input.head);
        api.sealBootstrapInTx(input.head, null, input.proof_job);
      },
    });
  }
  private requireAcceptedCheckPolicyInTx(account: string, candidate: string, head: string, rootTree: string, profile: CheckProfile | undefined): void {
    const policy = this.executionPolicy(profile);
    if (!policy) fail(400, 'failed_precondition');
    const accepted = this.one("SELECT receipt,digest,accepted_until FROM check_leases WHERE account=? AND phase='completed' AND json_extract(candidate,'$.scope.candidate_id')=? AND json_extract(receipt,'$.outcome')='passed' ORDER BY rowid DESC LIMIT 1", account, candidate);
    if (!accepted || Number(accepted.accepted_until) <= Math.floor(Date.now() / 1000)) fail(400, 'failed_precondition');
    let receipt: { request: { job_id: string; candidate_commit: string; root_tree: string } };
    try { receipt = JSON.parse(String(accepted.receipt)); } catch { fail(500, 'data_loss'); }
    if (!receipt?.request || receipt.request.job_id !== candidate || receipt.request.candidate_commit !== head || receipt.request.root_tree !== rootTree) fail(400, 'failed_precondition');
    let verified: { receipt_digest: string };
    try { verified = validateCheckerReceipt({ request: receipt.request, receipt, policy }); } catch { fail(400, 'failed_precondition'); }
    if (verified.receipt_digest !== accepted.digest) fail(500, 'data_loss');
  }
  private executionPolicy(profile: CheckProfile | undefined): ExecutionPolicy | null {
    const raw = this.env.FORGE_CHECK_POLICY_JSON;
    if (!raw || !profile) return null;
    if (new TextEncoder().encode(raw).byteLength > 16 * 1024) fail(400, 'failed_precondition');
    let value: ExecutionPolicy;
    try { value = JSON.parse(raw); } catch { fail(400, 'failed_precondition'); }
    if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).sort().join(',') !== 'profile,targets,test'
        || !value.profile || Object.keys(value.profile).sort().join(',') !== 'name,version'
        || typeof value.profile.name !== 'string' || !/^[A-Za-z0-9_.-]{1,128}$/.test(value.profile.name)
        || typeof value.profile.version !== 'string' || !/^[0-9a-f]{64}$/.test(value.profile.version)
        || typeof value.test !== 'boolean' || !Array.isArray(value.targets) || !value.targets.length || value.targets.length > 128
        || value.targets.some(target => typeof target !== 'string' || !target.startsWith('//') || !target.includes(':') || target.includes('...')
          || /[\s*?\u0000]/u.test(target) || ['all', 'all-targets', ''].includes(target.split(':').at(-1)!))
        || new Set(value.targets).size !== value.targets.length || value.targets.join('').length > 8192) fail(400, 'failed_precondition');
    const selected = this.reviewConfiguration()?.profile;
    if (!selected || value.profile.name !== selected.name || value.profile.version !== selected.version
        || value.profile.name !== profile.name || value.profile.version !== profile.version) return null;
    return value;
  }
  private reviewConfiguration(): { profile: { name: string; version: string }; committer: { name: string; email: string } } | null {
    const raw = this.env.FORGE_REVIEW_CONFIG_JSON;
    if (raw === undefined || raw === '') return null;
    if (new TextEncoder().encode(raw).byteLength > 16 * 1024) fail(400, 'failed_precondition');
    let config: unknown; try { config = JSON.parse(raw); } catch { fail(400, 'failed_precondition'); }
    const value = config as { profile?: { name?: string; version?: string }; committer?: { name?: string; email?: string } };
    if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).sort().join(',') !== 'committer,profile'
        || !value.profile || Object.keys(value.profile).sort().join(',') !== 'name,version'
        || !value.committer || Object.keys(value.committer).sort().join(',') !== 'email,name'
        || typeof value.profile.name !== 'string' || !/^[A-Za-z0-9_.-]{1,128}$/.test(value.profile.name)
        || typeof value.profile.version !== 'string' || !/^[0-9a-f]{64}$/.test(value.profile.version)
        || ![value.committer.name, value.committer.email].every(v => typeof v === 'string' && v.length > 0
          && new TextEncoder().encode(v).byteLength <= 1024 && !/[\u0000-\u001f\u007f-\u009f]/u.test(v))) fail(400, 'failed_precondition');
    return value as { profile: { name: string; version: string }; committer: { name: string; email: string } };
  }
  private async stageGenesis(account: string, objects: CanonicalAccountObject[], staged: Set<string>, storage: ForgeStorage, confirmations: Map<string, unknown>): Promise<void> {
    if (this.one('SELECT account FROM repository WHERE singleton=1')) fail(400, 'failed_precondition');
    if (objects.length !== 2 || objects[0].kind !== 2 || objects[1].kind !== 3) fail(503, 'unavailable');
    try { validateClosure({ kind: 3, id: objects[1].id }, objects); } catch { fail(503, 'unavailable'); }
    for (const object of objects) {
      this.ctx.storage.transactionSync(() => storage.reserveInTx('genesis', 'genesis', object));
      const key = `${object.kind}/${object.id}`;
      confirmations.set(key, await storage.stage(object, Uint8Array.from(object.bytes)));
      staged.add(key);
    }
  }
  private async put(request: Request, account: string, auth: Auth, key: ObjectKey): Promise<Response> {
    const id = requestId(request.headers.get('x-tinytree-request-id'));
    const limit = key.kind === 1 ? MAX_OBJECT : MAX_METADATA;
    const bytes = await body(request, limit);
    const size = request.headers.get('x-tinytree-size');
    if (!size || !/^\d+$/.test(size) || Number(size) !== bytes.byteLength) fail(400, 'object_size_mismatch');
    const descriptor = nativeInspect(key.kind, bytes);
    sameDescriptor({ ...key, size_bytes: bytes.byteLength, format_version: 1 }, descriptor);
    const fingerprint = await hash(JSON.stringify(['put', account, auth.principal, descriptor.kind, descriptor.id, descriptor.size_bytes, descriptor.format_version]));
    const replay = this.replay(id, auth, fingerprint); if (replay) return replay;
    const result = { state: 'complete', ...key, size_bytes: bytes.byteLength, format_version: 1 };
    const existing = this.one('SELECT * FROM objects WHERE kind=? AND id=?', key.kind, key.id);
    if (existing && Number(existing.size) !== bytes.byteLength) fail(409, 'object_descriptor_conflict');
    this.ctx.storage.transactionSync(() => {
      this.reserve(id, auth, fingerprint);
      if (!existing) {
        const usage = this.one('SELECT COUNT(*) AS n, COALESCE(SUM(size),0) AS bytes FROM objects')!;
        if (Number(usage.n) >= MAX_OBJECTS || Number(usage.bytes) + bytes.byteLength > MAX_ACCOUNT_BYTES) fail(429, 'object_quota');
        this.sql.exec('INSERT INTO objects(kind,id,size,inspection,visible) VALUES(?,?,?,?,0)', key.kind, key.id, bytes.byteLength, JSON.stringify(descriptor));
      }
    });
    // A failed/uncertain put leaves a bounded SQL reservation. It is never served
    // or usable by closure validation; same-intent retries can finish it safely.
    // Every fresh intent confirms actual durable bytes, including safe repair
    // of an existing canonical key. Completed historical replay returned above.
    const written = await this.env.OBJECTS.put(r2key(account, key), bytes);
    if (!written || written.size !== bytes.byteLength) fail(503, 'object_put_unconfirmed');
    return this.ctx.storage.transactionSync(() => {
      this.sql.exec('UPDATE objects SET visible=1 WHERE kind=? AND id=?', key.kind, key.id);
      return this.finish(id, result);
    });
  }
  private async read(account: string, key: ObjectKey, size: number): Promise<Uint8Array> {
    if (size > MAX_OBJECT) fail(503, 'stored_object_limit');
    const object = await this.env.OBJECTS.get(r2key(account, key));
    if (!object || object.size !== size) fail(503, 'object_payload_unavailable');
    const bytes = new Uint8Array(await object.arrayBuffer());
    try { sameDescriptor({ ...key, size_bytes: size, format_version: 1 }, nativeInspect(key.kind, bytes)); }
    catch { fail(503, 'object_payload_invalid'); }
    return bytes;
  }
  private async closure(account: string, head: string): Promise<void> {
    const pending: ObjectKey[] = [{ kind: 3, id: head }]; const seen = new Set<string>();
    const objects: (Descriptor & { bytes?: number[] })[] = []; let metadataBytes = 0;
    while (pending.length) {
      const key = pending.pop()!; if (key.kind === 3 && key.id === ROOT) continue;
      const identity = `${key.kind}:${key.id}`; if (seen.has(identity)) continue; seen.add(identity);
      if (seen.size > 256 || pending.length > 4096) fail(413, 'closure_object_limit');
      const row = this.one('SELECT * FROM objects WHERE kind=? AND id=? AND visible=1', key.kind, key.id);
      if (!row) fail(409, 'closure_object_unavailable');
      const size = Number(row.size);
      if (key.kind === 1) {
        // A successful old upload receipt is authority to consider the FILE,
        // not proof that its current backing bytes remain intact. Publication
        // re-verifies actual R2 bytes, one bounded object at a time.
        await this.read(account, key, size);
        objects.push({ ...key, size_bytes: size, format_version: 1 });
      } else {
        metadataBytes += size; if (metadataBytes > 1024 * 1024) fail(413, 'closure_metadata_limit');
        const bytes = await this.read(account, key, size); const inspected = nativeInspect(key.kind, bytes);
        objects.push({ ...key, size_bytes: size, format_version: 1, bytes: Array.from(bytes) });
        pending.push(...inspected.references);
      }
    }
    try { validateClosure({ kind: 3, id: head }, objects); }
    catch { fail(409, 'invalid_native_closure'); }
  }
  private async cas(request: Request, account: string, auth: Auth): Promise<Response> {
    const value = record(await input(request), ['request_id', 'name', 'expected_generation', 'expected_head', 'head']);
    const id = requestId(value.request_id), ref = name(value.name), generation = integer(value.expected_generation, Number.MAX_SAFE_INTEGER - 1);
    const expected = value.expected_head === null ? null : digest(value.expected_head), head = digest(value.head);
    if (head === ROOT) fail(400, 'intrinsic_root_not_stored_head');
    if ((generation === 0) !== (expected === null)) fail(400, 'expected_reference_binding');
    const fingerprint = await hash(JSON.stringify(['cas', account, auth.principal, ref, generation, expected, head]));
    const replay = this.replay(id, auth, fingerprint); if (replay) return replay;
    await this.closure(account, head);
    return this.ctx.storage.transactionSync(() => {
      this.reserve(id, auth, fingerprint);
      const current = this.one('SELECT * FROM refs WHERE name=?', ref);
      if ((current ? Number(current.generation) : 0) !== generation || (current ? current.head : null) !== expected)
        return this.finish(id, { state: 'stale', name: ref, generation: current ? Number(current.generation) : 0, head: current ? current.head : null }, 409);
      if (!current && Number(this.one('SELECT COUNT(*) AS n FROM refs')!.n) >= 64) fail(429, 'reference_quota');
      this.sql.exec('INSERT INTO refs(name,generation,head) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,head=excluded.head', ref, generation + 1, head);
      return this.finish(id, { state: 'complete', name: ref, generation: generation + 1, head });
    });
  }
  private async pin(request: Request, account: string, auth: Auth, operation: string): Promise<Response> {
    const value = record(await input(request), operation === 'create' ? ['request_id', 'name', 'head'] : ['request_id', 'name', 'expected_head']);
    const id = requestId(value.request_id), pin = name(value.name), head = digest(operation === 'create' ? value.head : value.expected_head);
    if (head === ROOT) fail(400, 'intrinsic_root_not_stored_head');
    const fingerprint = await hash(JSON.stringify(['pin', operation, account, auth.principal, pin, head]));
    const replay = this.replay(id, auth, fingerprint); if (replay) return replay;
    if (operation === 'create') await this.closure(account, head);
    return this.ctx.storage.transactionSync(() => {
      this.reserve(id, auth, fingerprint);
      const current = this.one('SELECT * FROM pins WHERE name=?', pin);
      if (operation === 'create') {
        if (current && (current.head !== head || current.active !== 1)) fail(409, 'pin_identity_conflict');
        if (!current && Number(this.one('SELECT COUNT(*) AS n FROM pins')!.n) >= 64) fail(429, 'pin_quota');
        this.sql.exec('INSERT OR IGNORE INTO pins(name,head,active) VALUES(?,?,1)', pin, head);
      } else {
        if (!current || current.head !== head || current.active !== 1) fail(409, 'pin_unavailable');
        this.sql.exec('UPDATE pins SET active=0 WHERE name=?', pin);
      }
      return this.finish(id, { state: 'complete', name: pin, head, active: operation === 'create' });
    });
  }
}

/** RPC capability boundary. Only these methods cross the namespace binding;
 * the engine's TypeScript-private methods are not exposed as RPC methods. */
export class AccountRepository extends DurableObject<Env> {
  #engine: RepositoryEngine;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#engine = new RepositoryEngine(ctx, env);
  }
  async fetch(request: Request): Promise<Response> { return this.#engine.fetch(request); }
  async transformRpc(frame: TransformRpcFrame): Promise<unknown> {
    let result: TransformRpcEnvelope;
    try { result = { ok: true, value: await this.#engine.transformRpc(frame) }; }
    catch (error) {
      result = error instanceof Failure ? { ok: false, status: error.status, code: error.code }
        : { ok: false, status: 503, code: 'unavailable' };
    }
    return result;
  }
  async alarm(): Promise<void> {
    const account = await this.#engine.recoveryAccount();
    if (!account) return;
    let delay = 60_000;
    const local = (command: TransformRpcCommand) => this.#engine.transformRpc({ schema: 1, account_id: account, auth: null, command }, false);
    try { await applyTransformPeer(this.env, null, await local({ action: 'recovery_next' }), local); }
    catch { delay = 60 * 60_000; } // Preserve uncertain custody; bound unavailable-peer retries.
    finally {
      await this.#engine.refreshRecoveryAlarm(delay);
    }
  }
}
