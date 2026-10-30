import { inspect, validateClosure, type Descriptor, type Inspection, type ObjectKey } from './core';
import { authenticate, body, digest, fail, hash, identifier, input, integer, json, name, protect, record, requestId, type Auth } from './protocol';

interface Env {
  REPOSITORIES: DurableObjectNamespace;
  OBJECTS: R2Bucket;
  PRINCIPALS_JSON: string;
  EXPERIMENTAL_RAW_BACKEND: string;
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
function sameDescriptor(expected: Descriptor, actual: Descriptor): void {
  if (expected.kind !== actual.kind || expected.id !== actual.id || expected.size_bytes !== actual.size_bytes || actual.format_version !== 1) fail(400, 'native_descriptor_mismatch');
}
function nativeInspect(kind: number, bytes: Uint8Array): Inspection {
  try { return inspect(kind, bytes); } catch { fail(400, 'invalid_native_object'); }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return protect(async () => {
      if (env.EXPERIMENTAL_RAW_BACKEND !== 'enabled') fail(503, 'experimental_backend_disabled');
      const auth = await authenticate(request, env.PRINCIPALS_JSON);
      const parsed = route(request);
      if (parsed.operation === 'capabilities') return json(capabilities);
      if (!auth.accounts.includes(parsed.account!)) fail(403, 'account_access_denied');
      const id = env.REPOSITORIES.idFromName(`native-account-v1:${parsed.account}`);
      return env.REPOSITORIES.get(id).fetch(request);
    });
  },
};

/** One SQLite authority per account. R2 is immutable payload storage, not a ref authority. */
export class AccountRepository {
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
    ]) this.sql.exec(statement);
  }
  async fetch(request: Request): Promise<Response> {
    // Includes R2 awaits: two admitted uploads cannot oversubscribe durable quota,
    // and reads never observe an uncommitted payload. Eviction preserves SQL intents.
    if (this.active >= 16) return json({ error: 'account_admission_limit' }, 429);
    this.active++;
    const work = this.tail.then(() => protect(() => this.handle(request))).finally(() => { this.active--; });
    this.tail = work.then(() => undefined, () => undefined);
    return work;
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
    const parsed = route(request); const account = parsed.account!;
    if (!account || !auth.accounts.includes(account)) fail(403, 'account_access_denied');
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
