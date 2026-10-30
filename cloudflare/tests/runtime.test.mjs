import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Miniflare } from 'miniflare';

const bundle = process.env.TINYTREE_WORKER_BUNDLE;
const wasm = process.env.TINYTREE_CORE_WASM;
const fixturePath = process.env.TINYTREE_CONTRACT_FIXTURE;
assert.ok(bundle && wasm && fixturePath, 'declared Worker, RustWasm and native Store fixture are required');
const fixture = JSON.parse(await readFile(fixturePath, 'utf8'));
const objects = new Map(fixture.objects.map(object => [object.name, object]));
const tokenA = 'account-a-runtime-fixture';
const tokenB = 'account-b-runtime-fixture';
const member = 'account-a-member-fixture';
const sha = value => createHash('sha256').update(value).digest('hex');

test('actual RustWasm shares native Forge view identities and strict HTTP framing', async () => {
  const { instance } = await WebAssembly.instantiate(await readFile(wasm), {});
  const core = instance.exports;
  const invoke = (command, value) => {
    const bytes = new TextEncoder().encode(typeof value === 'string' ? value : JSON.stringify(value));
    const pointer = core.tt_alloc(bytes.byteLength);
    assert.ok(pointer);
    try {
      new Uint8Array(core.memory.buffer, pointer, bytes.byteLength).set(bytes);
      const ok = core[command](pointer, bytes.byteLength);
      const result = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(
        new Uint8Array(core.memory.buffer, core.tt_output_ptr(), core.tt_output_len())));
      return { ok, result };
    } finally { core.tt_dealloc(pointer, bytes.byteLength); }
  };
  const spec = { name: 'public', root_prefix: 'pkg', include_prefixes: ['src'],
    exclude_prefixes: ['src/private'], visibility: { kind: 'roles', roles: ['member'] },
    revision: Array(32).fill(1), export_allowed: true };
  const view = invoke('tt_forge_validate_view', { account_id: 'example', spec });
  assert.equal(view.ok, 1);
  assert.deepEqual(view.result, { view_version: 'b459fc5356b0b5ec653a22dba7073aaaa393b65d4ed317cd1f1c6ef6bcb8b2fa', spec });
  assert.equal(invoke('tt_forge_validate_view', { account_id: 'example', spec, actor: 'operator' }).ok, 0);
  const call = { version: 1, operation: 20, account_id: 'example', view: 'public',
    request_id: 'saved-1', payload_json: '{"source_head":"unchanged opaque payload"}' };
  assert.deepEqual(invoke('tt_forge_parse_call', call), { ok: 1, result: call });
  assert.equal(invoke('tt_forge_parse_call', JSON.stringify(call).replace('"operation":20', '"operation":20,"operation":21')).ok, 0);
  assert.equal(invoke('tt_forge_parse_call', { ...call, actor: 'operator' }).ok, 0);
});

const principals = () => [
  { principal: 'operator-a', token_sha256: sha(tokenA), accounts: [fixture.account], operator: true, kind: 'raw' },
  { principal: 'operator-b', token_sha256: sha(tokenB), accounts: ['other'], operator: true, kind: 'raw' },
  { principal: 'member-a', token_sha256: sha(member), accounts: [fixture.account], operator: false, kind: 'raw' },
];

function options(persist, { fault, config = principals(), forge = false } = {}) {
  const modules = [{ type: 'ESModule', path: bundle }, { type: 'CompiledWasm', path: wasm }];
  if (fault) {
    // Test-only transport fault around the REAL R2 binding: after-write failure
    // leaves a genuine stored orphan, not an in-memory fake bucket.
    const entry = fault === 'sql-receipt' ? `import worker, {AccountRepository as Base} from './${path.basename(bundle)}';
      export default worker;
      export class AccountRepository extends Base {
        constructor(ctx, env) {
          const bind = (target, key) => { const value = Reflect.get(target, key); return typeof value === 'function' ? value.bind(target) : value; };
          const sql = new Proxy(ctx.storage.sql, { get(target, key) {
            if (key === 'exec') return (query, ...args) => {
              if (query.startsWith('INSERT INTO forge_account_receipts')) throw new Error('deliberate receipt transaction failure');
              return target.exec(query, ...args);
            };
            return bind(target, key);
          }});
          const storage = new Proxy(ctx.storage, { get(target, key) { return key === 'sql' ? sql : bind(target, key); }});
          const state = new Proxy(ctx, { get(target, key) { return key === 'storage' ? storage : bind(target, key); }});
          super(state, env);
        }
      }` : `import worker, {AccountRepository as Base} from './${path.basename(bundle)}';
      export default worker;
      export class AccountRepository extends Base {
        constructor(ctx, env) {
          const bucket = env.OBJECTS;
          const wrapped = new Proxy(bucket, { get(target, key) {
            if (key === 'put') return async (...args) => {
              ${fault === 'delay' ? 'await new Promise(resolve => setTimeout(resolve, 500)); return target.put(...args);' : `${fault === 'after' ? 'await target.put(...args);' : ''} throw new Error('deliberate test-only R2 transport failure');`}
            };
            const value = Reflect.get(target, key); return typeof value === 'function' ? value.bind(target) : value;
          }});
          super(ctx, {...env, OBJECTS: wrapped});
        }
      }`;
    modules.unshift({ type: 'ESModule', path: path.join(path.dirname(bundle), 'runtime-fault-entry.mjs'), contents: entry });
  }
  return {
    modules, compatibilityDate: '2026-07-08',
    durableObjects: { REPOSITORIES: { className: 'AccountRepository', useSQLite: true } },
    r2Buckets: ['OBJECTS'], durableObjectsPersist: path.join(persist, 'do'), r2Persist: path.join(persist, 'r2'),
    bindings: { PRINCIPALS_JSON: JSON.stringify(config), EXPERIMENTAL_RAW_BACKEND: 'enabled', FORGE_BACKEND: forge ? 'enabled' : 'disabled' },
  };
}
async function call(mf, account, operation, value, { token = tokenA, method = 'POST' } = {}) {
  return mf.dispatchFetch(`https://repository.invalid/v1/accounts/${account}/${operation}`, {
    method, headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    ...(method === 'GET' ? {} : { body: JSON.stringify(value) }),
  });
}
async function value(response, status = 200) {
  const result = await response.json(); assert.equal(response.status, status, JSON.stringify(result)); return result;
}
async function provision(mf, account = fixture.account, token = tokenA) {
  return value(await call(mf, account, 'repository', { request_id: `provision-${account}` }, { token }));
}
async function put(mf, object, id = `put-${object.name}`, account = fixture.account, token = tokenA) {
  return mf.dispatchFetch(`https://repository.invalid/v1/accounts/${account}/objects/${object.kind}/${object.id}`, {
    method: 'PUT', headers: { authorization: `Bearer ${token}`, 'x-tinytree-request-id': id, 'x-tinytree-size': String(object.size_bytes) },
    body: Uint8Array.from(object.bytes),
  });
}
async function uploadFixture(mf) {
  for (const object of fixture.objects) await value(await put(mf, object));
}
function cas(id, expected, object) {
  return { request_id: id, name: 'main', expected_generation: expected.generation, expected_head: expected.head, head: object.id };
}
function nativeReference(result) { return { name: result.name, generation: result.generation, head: result.head }; }
const getObject = (mf, object, token = tokenA, account = fixture.account) => mf.dispatchFetch(
  `https://repository.invalid/v1/accounts/${account}/objects/${object.kind}/${object.id}`, { headers: { authorization: `Bearer ${token}` } });

test('Forge HTTP account authority matches native Store and fences raw access across reload and staging failures', async t => {
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-forge-account-'));
  const owner = 'forge-owner-runtime-token';
  const memberToken = 'forge-member-runtime-token';
  const other = 'forge-other-runtime-token';
  const raw = 'forge-raw-runtime-token';
  const account = fixture.forge.account_id;
  const allowed = [account, 'legacy', 'r2-recovery', 'sql-recovery'];
  const config = [
    { principal: 'forge-owner', token_sha256: sha(owner), accounts: allowed, operator: true, kind: 'forge' },
    { principal: 'forge-member', token_sha256: sha(memberToken), accounts: allowed, operator: false, kind: 'forge' },
    { principal: 'forge-other', token_sha256: sha(other), accounts: allowed, operator: true, kind: 'forge' },
    { principal: 'raw-owner', token_sha256: sha(raw), accounts: allowed, operator: true, kind: 'raw' },
  ];
  let mf = new Miniflare(options(persist, { config, forge: true }));
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const envelope = (operation, payload, request_id, selectedAccount = account) => ({
    version: 1, operation, account_id: selectedAccount, view: 'public', request_id,
    payload_json: typeof payload === 'string' ? payload : JSON.stringify(payload),
  });
  const callForge = (operation, payload = {}, requestId = '', { token = owner, selectedAccount = account } = {}) => mf.dispatchFetch(
    'https://repository.invalid/api/v1/forge/call', { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify(envelope(operation, payload, requestId, selectedAccount)) });
  const forgeValue = async (response, status = 200, code) => {
    const result = await response.json();
    assert.equal(response.status, status, JSON.stringify(result)); assert.equal(result.version, 1);
    if (status !== 200) { if (code) assert.equal(result.error.code, code); return result; }
    assert.equal(result.view, 'public'); assert.equal(result.state, 'complete');
    return JSON.parse(result.result_json);
  };
  await forgeValue(await callForge(1, {}, 'member-create', { token: memberToken }), 403, 'permission_denied');
  await forgeValue(await callForge(1, {}, 'wrong-kind', { token: raw }), 403, 'permission_denied');
  const provisioned = await forgeValue(await callForge(1, {}, 'provision-owner'));
  assert.deepEqual(provisioned, fixture.forge.provisioned);
  assert.deepEqual(await forgeValue(await callForge(1, {}, 'provision-owner')), provisioned);
  assert.deepEqual(await forgeValue(await callForge(2)), provisioned);
  await forgeValue(await callForge(2, {}, '', { token: memberToken }), 403, 'permission_denied');
  await forgeValue(await callForge(1, {}, 'steal-owner', { token: other }), 403, 'permission_denied');
  const member = { principal: 'forge-member', can_write: true, can_review: true, is_admin: true, roles: ['reviewer'] };
  const mutation = { member, expected_epoch: 1 };
  assert.deepEqual(await forgeValue(await callForge(3, mutation, 'add-member')), fixture.forge.added);
  assert.deepEqual(await forgeValue(await callForge(3, { member, expected_epoch: 2 }, 'same-member')), fixture.forge.unchanged);
  // Same request ID belongs to a different principal: it must not replay the
  // owner's receipt and hide this actor's stale expected epoch.
  await forgeValue(await callForge(3, mutation, 'same-member', { token: memberToken }), 409, 'aborted');
  assert.deepEqual(await forgeValue(await callForge(3, { member, expected_epoch: 2 }, 'member-self-noop', { token: memberToken })), fixture.forge.unchanged);
  await forgeValue(await callForge(3, { ...mutation, expected_epoch: 2 }, 'add-member'), 400, 'invalid_argument');
  await forgeValue(await callForge(3, { member: { ...member, principal: 'forge-owner' }, expected_epoch: 2 }, 'change-owner'), 400, 'invalid_argument');
  const duplicate = '{"member":' + JSON.stringify(member) + ',"expected_epoch":2,"expected_epoch":2}';
  await forgeValue(await callForge(3, duplicate, 'ambiguous-member'), 400, 'invalid_argument');
  assert.deepEqual(await forgeValue(await callForge(3, { member: { ...member, is_admin: false }, expected_epoch: 2 }, 'revoke-admin')), fixture.forge.revoked);
  await forgeValue(await callForge(3, { member, expected_epoch: 2 }, 'member-self-noop', { token: memberToken }), 403, 'permission_denied');
  assert.deepEqual(await forgeValue(await callForge(3, mutation, 'add-member')), fixture.forge.added, 'historical receipt never reapplies membership');
  assert.equal((await forgeValue(await callForge(2))).policy_epoch, 3);
  await forgeValue(await callForge(20, {}, 'unsupported-review'), 501, 'unimplemented');
  const malformed = { ...envelope(1, {}, 'actor-injection'), actor: 'forge-owner' };
  await forgeValue(await mf.dispatchFetch('https://repository.invalid/api/v1/forge/call', { method: 'POST', headers: { authorization: `Bearer ${owner}`, 'content-type': 'application/json' }, body: JSON.stringify(malformed) }), 400, 'invalid_argument');
  for (const [operation, payload, method] of [
    ['repository', {}, 'GET'], ['refs/read', { names: ['main'] }, 'POST'],
    ['refs/cas', { request_id: 'raw-bypass', name: 'main', expected_generation: 1, expected_head: null, head: '1'.repeat(64) }, 'POST'],
    ['pins/create', { request_id: 'raw-pin', name: 'hidden', head: '1'.repeat(64) }, 'POST'],
    [`objects/3/${Buffer.from(provisioned.main_revision).toString('hex')}`, {}, 'GET'],
  ]) assert.equal((await call(mf, account, operation, payload, { token: raw, method })).status, 403, operation);
  assert.equal((await call(mf, 'legacy', 'repository', { request_id: 'forge-token-raw' }, { token: owner })).status, 403);
  await value(await call(mf, 'legacy', 'repository', { request_id: 'raw-legacy' }, { token: raw }));
  await forgeValue(await callForge(1, {}, 'adopt-legacy', { selectedAccount: 'legacy' }), 400, 'failed_precondition');
  await mf.dispose(); mf = new Miniflare(options(persist, { config, forge: true }));
  assert.equal((await forgeValue(await callForge(2))).policy_epoch, 3);
  assert.deepEqual(await forgeValue(await callForge(3, mutation, 'add-member')), fixture.forge.added);
  for (const [fault, selectedAccount] of [['after', 'r2-recovery'], ['sql-receipt', 'sql-recovery']]) {
    await mf.dispose(); mf = new Miniflare(options(persist, { config, forge: true, fault }));
    await forgeValue(await callForge(1, {}, 'recover-provision', { selectedAccount }), 503, 'unavailable');
    await forgeValue(await callForge(2, {}, '', { selectedAccount }), 403, 'permission_denied');
    assert.equal((await call(mf, selectedAccount, 'repository', {}, { token: raw, method: 'GET' })).status, 404, 'no partial SQL claim');
    const bucket = await mf.getR2Bucket('OBJECTS');
    assert.ok((await bucket.list({ prefix: `native/v1/${selectedAccount}/` })).objects.length > 0, 'real staged R2 orphan');
    await mf.dispose(); mf = new Miniflare(options(persist, { config, forge: true }));
    const recovered = await forgeValue(await callForge(1, {}, 'recover-provision', { selectedAccount }));
    assert.equal(recovered.main_generation, 1); assert.equal(recovered.policy_epoch, 1);
    assert.deepEqual(await forgeValue(await callForge(1, {}, 'recover-provision', { selectedAccount })), recovered);
    assert.equal((await call(mf, selectedAccount, 'refs/read', { names: ['main'] }, { token: raw })).status, 403);
  }
});

test('actual SQLite DO and R2 conform to native Store CAS, immutable bytes, reload and retained pins', async t => {
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-cloudflare-'));
  let mf = new Miniflare(options(persist));
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  assert.equal((await mf.dispatchFetch('https://repository.invalid/v1/capabilities')).status, 401);
  const caps = await value(await mf.dispatchFetch('https://repository.invalid/v1/capabilities', { headers: { authorization: `Bearer ${tokenA}` } }));
  assert.equal(caps.experimental, true); assert.equal(caps.checked_landing, false); assert.equal(caps.views, false); assert.equal(caps.garbage_collection, false); assert.equal(caps.raw_account_access, 'read_write');
  const firstProvision = await provision(mf); assert.equal(firstProvision.repository_id, fixture.repository);
  assert.deepEqual(await provision(mf), firstProvision);
  await value(await call(mf, fixture.account, 'repository', { request_id: 'second-repo', repository_id: 'second' }), 400);
  await value(await call(mf, fixture.account, 'repository', { request_id: 'member-create' }, { token: member }), 403);
  await provision(mf, 'other', tokenB);
  await value(await call(mf, 'other', 'repository', {}, { method: 'GET' }), 403);
  await value(await call(mf, fixture.account, 'refs/read', { names: ['main'] })).then(result => assert.deepEqual(result.references[0], fixture.native.initial));
  const wrong = { ...objects.get('original_file'), bytes: [...objects.get('original_file').bytes].map(b => b ^ 1) };
  await value(await put(mf, wrong), 400);
  assert.equal((await getObject(mf, wrong)).status, 404);
  await uploadFixture(mf);
  const original = objects.get('original_file');
  assert.deepEqual(new Uint8Array(await (await getObject(mf, original)).arrayBuffer()), Uint8Array.from(original.bytes));
  await value(await put(mf, objects.get('updated_file'), `put-${original.name}`), 409);
  assert.equal((await getObject(mf, original, tokenB, 'other')).status, 404, 'known cross-tenant digest is not object authority');
  await value(await call(mf, 'other', 'refs/cas', cas('cross-tenant', fixture.native.initial, objects.get('first_commit')), { token: tokenB }), 409);
  assert.deepEqual((await value(await call(mf, fixture.account, 'refs/read', { names: ['main'] }))).references[0], fixture.native.initial);
  const first = await value(await call(mf, fixture.account, 'refs/cas', cas('first-publication', fixture.native.initial, objects.get('first_commit'))));
  assert.deepEqual(nativeReference(first), fixture.native.first);
  await value(await call(mf, fixture.account, 'refs/cas', { ...cas('member-branch', fixture.native.initial, objects.get('first_commit')), name: 'member-branch' }, { token: member }));
  await value(await call(mf, fixture.account, 'pins/create', { request_id: 'pin-old', name: 'dependency', head: first.head }));
  const contenders = await Promise.all(['advance-a', 'advance-b'].map(id => call(mf, fixture.account, 'refs/cas', cas(id, first, objects.get('next_commit')))));
  assert.deepEqual(contenders.map(r => r.status).sort(), [200, 409]);
  const replies = await Promise.all(contenders.map(r => r.json()));
  const next = replies.find(r => r.state === 'complete'); assert.deepEqual(nativeReference(next), fixture.native.next);
  const refusalClass = { invalid_native_closure: 'invalid_closure', closure_object_unavailable: 'missing_closure' };
  for (const label of ['wrong_size', 'wrong_copy', 'missing']) {
    const refusal = await value(await call(mf, fixture.account, 'refs/cas', cas(`reject-${label}`, next, objects.get(`${label}_commit`))), 409);
    assert.equal(refusalClass[refusal.error], fixture.native.refusals[label]);
  }
  assert.deepEqual((await value(await call(mf, fixture.account, 'refs/read', { names: ['main'] }))).references[0], fixture.native.after_refusals);
  await value(await call(mf, fixture.account, 'refs/cas', { ...cas('actor-injection', next, objects.get('next_commit')), principal: 'operator-b' }), 400);

  assert.deepEqual(await value(await call(mf, fixture.account, 'refs/cas', cas('first-publication', fixture.native.initial, objects.get('first_commit')))), first, 'historical receipt, not current generation');
  await value(await call(mf, fixture.account, 'refs/cas', cas('first-publication', first, objects.get('next_commit'))), 409);
  assert.equal((await call(mf, fixture.account, 'forge/push', {})).status, 501);
  assert.equal((await mf.dispatchFetch(`https://repository.invalid/v1/accounts/${fixture.account}/objects/${original.kind}/${original.id}`, { method: 'DELETE', headers: { authorization: `Bearer ${tokenA}` } })).status, 501);
  await mf.dispose(); mf = new Miniflare(options(persist));
  assert.deepEqual((await value(await call(mf, fixture.account, 'refs/read', { names: ['main'] }))).references[0], fixture.native.next);
  assert.deepEqual(await value(await call(mf, fixture.account, 'refs/cas', cas('first-publication', fixture.native.initial, objects.get('first_commit')))), first);
  assert.deepEqual((await value(await call(mf, fixture.account, 'pins/read', { names: ['dependency'] }))).pins[0], { name: 'dependency', head: first.head, active: true });
  assert.deepEqual(new Uint8Array(await (await getObject(mf, original)).arrayBuffer()), Uint8Array.from(original.bytes));
  await value(await call(mf, fixture.account, 'pins/release', { request_id: 'release-wrong', name: 'dependency', expected_head: next.head }), 409);
  const released = await value(await call(mf, fixture.account, 'pins/release', { request_id: 'release', name: 'dependency', expected_head: first.head }));
  assert.equal(released.active, false);
  assert.deepEqual(await value(await call(mf, fixture.account, 'pins/release', { request_id: 'release', name: 'dependency', expected_head: first.head })), released);
  await value(await call(mf, fixture.account, 'pins/create', { request_id: 'no-resurrection', name: 'dependency', head: first.head }), 409);
  // Test-only direct R2 corruption: the public read must verify, not blindly stream.
  const bucket = await mf.getR2Bucket('OBJECTS');
  await bucket.put(`native/v1/${fixture.account}/${original.kind}/${original.id}`, Uint8Array.from(original.bytes).map(b => b ^ 1));
  assert.equal((await getObject(mf, original)).status, 503);
  await value(await call(mf, fixture.account, 'refs/cas', cas('corrupt-publication', next, objects.get('next_commit'))), 503);
  assert.deepEqual((await value(await call(mf, fixture.account, 'refs/read', { names: ['main'] }))).references[0], fixture.native.next);
  assert.deepEqual(await value(await call(mf, fixture.account, 'refs/cas', cas('first-publication', fixture.native.initial, objects.get('first_commit')))), first,
    'historical receipt remains historical even if later backing integrity fails');
  await value(await put(mf, original, 'repair-original'));
  assert.deepEqual(new Uint8Array(await (await getObject(mf, original)).arrayBuffer()), Uint8Array.from(original.bytes));
  assert.deepEqual((await value(await call(mf, fixture.account, 'refs/read', { names: ['main'] }))).references[0], fixture.native.next,
    'repairing immutable backing does not publish a new reference generation');
});

test('durable failed R2 intents are inaccessible, bounded and retryable after runtime reload', async t => {
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-cloudflare-fault-'));
  let mf = new Miniflare(options(persist, { fault: 'after' }));
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  await provision(mf);
  const original = objects.get('original_file');
  await value(await put(mf, original, 'orphan-intent'), 503);
  const bucket = await mf.getR2Bucket('OBJECTS');
  assert.ok(await bucket.head(`native/v1/${fixture.account}/${original.kind}/${original.id}`), 'fault happens after actual R2 durable put');
  assert.equal((await getObject(mf, original)).status, 404, 'R2 orphan is not indexed as visible');
  await value(await put(mf, objects.get('updated_file'), 'orphan-intent'), 409, 'pending request remains exactly bound');
  await mf.dispose(); mf = new Miniflare(options(persist));
  assert.equal((await getObject(mf, original)).status, 404);
  const receipt = await value(await put(mf, original, 'orphan-intent'));
  assert.equal(receipt.id, original.id);
  assert.deepEqual(new Uint8Array(await (await getObject(mf, original)).arrayBuffer()), Uint8Array.from(original.bytes));
  // Per-call static grants are re-evaluated after worker/DO reload, not persisted
  // as permanent authority alongside the object's durable metadata.
  await mf.dispose(); mf = new Miniflare(options(persist, { config: principals().filter(p => p.principal !== 'operator-a') }));
  assert.equal((await getObject(mf, original)).status, 401);
});

test('failed puts consume durable reservations before R2 I/O and cannot create unbounded orphans', async t => {
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-cloudflare-quota-'));
  let mf = new Miniflare(options(persist, { fault: 'before' }));
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  await provision(mf);
  const { instance } = await WebAssembly.instantiate(await readFile(wasm), {}); const core = instance.exports;
  for (let i = 0; i < 33; i++) {
    const bytes = new Uint8Array(1024 * 1024).fill(i); const pointer = core.tt_alloc(bytes.length);
    new Uint8Array(core.memory.buffer, pointer, bytes.length).set(bytes);
    assert.equal(core.tt_inspect(1, pointer, bytes.length), 1);
    const descriptor = JSON.parse(new TextDecoder().decode(new Uint8Array(core.memory.buffer, core.tt_output_ptr(), core.tt_output_len())));
    core.tt_dealloc(pointer, bytes.length);
    const response = await put(mf, { ...descriptor, name: `quota-${i}`, bytes }, `quota-${i}`);
    await value(response, i < 32 ? 503 : 429);
  }
  assert.equal((await (await mf.getR2Bucket('OBJECTS')).list()).objects.length, 0, 'fail-before fault did not store payloads');
  await mf.dispose(); mf = new Miniflare(options(persist));
  // Reservation saturation survives reload and refuses even a small new object.
  await value(await put(mf, objects.get('original_file'), 'quota-after-reload'), 429);
});


test('account admission is bounded while the real R2 transport is pending', async t => {
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-cloudflare-admission-'));
  const mf = new Miniflare(options(persist, { fault: 'delay' }));
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  await provision(mf);
  const responses = await Promise.all(Array.from({ length: 24 }, async (_, i) => {
    const response = await put(mf, objects.get('original_file'), `queued-${i}`);
    const status = response.status;
    return { status, result: await value(response, status) };
  }));
  assert.ok(responses.some(response => response.status === 429));
  assert.ok(responses.some(response => response.status === 200));
  assert.ok(responses.filter(response => response.status === 200).length <= 16);
  for (const response of responses) {
    const { status, result } = response;
    if (status === 429) assert.equal(result.error, 'account_admission_limit');
    else assert.equal(status, 200);
  }
});
