import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, stat, rm } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
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
  await forgeValue(await callForge(5, {}, '', { selectedAccount: 'legacy' }), 403, 'permission_denied');
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
  const views = fixture.forge.views;
  const publishPublic = { spec: views.public_spec, expected_epoch: 3, make_default: true };
  await forgeValue(await callForge(4, publishPublic, 'member-cannot-publish', { token: memberToken }), 403, 'permission_denied');
  await forgeValue(await callForge(4, publishPublic, 'add-member'), 400, 'invalid_argument');
  await forgeValue(await callForge(4, { ...publishPublic, spec: { ...views.public_spec, revision: Array(32).fill(9) } }, 'unknown-view-root'), 403, 'permission_denied');
  assert.deepEqual(await forgeValue(await callForge(4, publishPublic, 'public-view')), views.public);
  assert.deepEqual(await forgeValue(await callForge(4, publishPublic, 'public-view')), views.public);
  assert.deepEqual(await forgeValue(await callForge(5, {}, '', { token: other })), [views.public], 'current public policy permits nonmember view discovery');
  assert.deepEqual((await forgeValue(await callForge(2))).public_view, views.public.view_version);
  const publishLocked = { spec: views.locked_spec, expected_epoch: 4, make_default: false };
  assert.deepEqual(await forgeValue(await callForge(4, publishLocked, 'locked-view')), views.locked);
  assert.deepEqual(await forgeValue(await callForge(5, {}, '', { token: other })), [], 'new current member policy removes old public discovery');
  assert.deepEqual(await forgeValue(await callForge(5, {}, '', { token: memberToken })), [views.locked]);
  assert.equal((await forgeValue(await callForge(2))).public_view, null);
  await forgeValue(await callForge(11, { version: views.locked.view_version, expected_epoch: 5 }, 'cannot-retire-current'), 400, 'failed_precondition');
  assert.deepEqual(await forgeValue(await callForge(11, { version: views.public.view_version, expected_epoch: 5 }, 'retire-old-view')), views.retired);
  assert.deepEqual(await forgeValue(await callForge(4, publishPublic, 'public-view')), views.public, 'historical view replay never changes current policy');
  assert.equal((await forgeValue(await callForge(2))).policy_epoch, 6);
  assert.deepEqual(await forgeValue(await callForge(5)), [{ ...views.locked, policy_epoch: 6 }]);
  await forgeValue(await callForge(8, { query: 'fixture', regex: false, limit: 10 }), 400, 'failed_precondition');
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
  assert.equal((await forgeValue(await callForge(2))).policy_epoch, 6);
  assert.deepEqual(await forgeValue(await callForge(5)), [{ ...views.locked, policy_epoch: 6 }]);
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


test('declared tt CLI uses actual workerd account authority and resumes exact intents after SQLite/R2 restart', async t => {
  const tt = process.env.TINYTREE_TT_BINARY;
  assert.ok(tt, 'the shipped tt binary must be declared by the runtime test target');
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-tt-workerd-'));
  const firstAccount = fixture.forge.account_id;
  const peerAccount = 'cli-peer';
  const firstToken = 'tt-workerd-owner-fixture-token';
  const peerToken = 'tt-workerd-peer-fixture-token';
  // Both transport principals can route to both accounts. The real account
  // authority, not a coarse token-routing filter, must deny cross-owner reads.
  const config = [
    { principal: 'forge-owner', token_sha256: sha(firstToken), accounts: [firstAccount, peerAccount], operator: true, kind: 'forge' },
    { principal: 'cli-peer-owner', token_sha256: sha(peerToken), accounts: [firstAccount, peerAccount], operator: true, kind: 'forge' },
  ];
  let mf = new Miniflare({ ...options(persist, { config, forge: true }), host: '127.0.0.1', port: 0 });
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const address = await mf.ready;
  const endpoint = address.origin;
  assert.equal(address.hostname, '127.0.0.1');
  const execute = promisify(execFile);
  async function client(name, token) {
    const root = path.join(persist, name);
    const home = path.join(root, 'home');
    const cwd = path.join(root, 'workspace');
    const emptyPath = path.join(root, 'no-programs');
    await Promise.all([mkdir(home, { recursive: true, mode: 0o700 }), mkdir(cwd, { recursive: true, mode: 0o700 }), mkdir(emptyPath, { recursive: true, mode: 0o700 })]);
    const jjConfig = path.join(root, 'jj.toml');
    await writeFile(jjConfig, '[user]\nname="Runtime Fixture"\nemail="fixture@example.invalid"\n[operation]\nhostname="test"\nusername="test"\n[ui]\npaginate="never"\n', { mode: 0o600 });
    return {
      home, cwd,
      env: { HOME: home, USERPROFILE: home, XDG_CONFIG_HOME: path.join(home, 'config'), JJ_CONFIG: jjConfig, PATH: emptyPath, TERM: 'dumb', TT_RUNTIME_TOKEN: token },
      context: path.join(home, 'config/tinytree/context.json'),
      intent: key => path.join(home, `config/tinytree/requests/request-${key}.json`),
    };
  }
  async function command(selected, args, success = true) {
    let output;
    try {
      // Asynchronous subprocesses let Miniflare's real HTTP/DO/R2 services run.
      output = await execute(tt, ['--no-pager', '--color=never', ...args, '--json'], {
        cwd: selected.cwd, env: selected.env, encoding: 'utf8', timeout: 20000, maxBuffer: 2 * 1024 * 1024,
      });
      assert.ok(success, `command unexpectedly succeeded: ${args.join(' ')}`);
    } catch (error) {
      if (success) throw error;
      assert.equal(typeof error.code, 'number', 'expected a CLI application failure, not a spawn or timeout failure');
      output = error;
    }
    const report = JSON.parse(output.stdout);
    assert.equal(report.schema_version, 1);
    assert.equal(report.state, success ? (args[0] === 'context' ? 'configured' : 'complete') : 'failed');
    if (!success) assert.equal(report.result, null);
    return report;
  }
  const select = (selected, account) => command(selected, ['context', 'set', '--transport', 'forge-https-v1', '--server', endpoint, '--account', account, '--token-env', 'TT_RUNTIME_TOKEN']);
  const login = (selected, account, success = true) => command(selected, ['login', '--transport', 'forge-https-v1', '--server', endpoint, '--account', account, '--token-env', 'TT_RUNTIME_TOKEN'], success);
  const first = await client('first', firstToken);
  const peer = await client('peer', peerToken);
  // Selecting context is local setup, not a login claim. Provision the fresh
  // account through tt before its authenticated account snapshot can be read.
  await select(first, firstAccount);
  const provisioned = (await command(first, ['account', 'provision', '--request-id', 'tt-provision-first'])).result.value;
  const expected = { ...fixture.forge.provisioned, main_revision: Buffer.from(fixture.forge.provisioned.main_revision).toString('hex') };
  assert.deepEqual(provisioned, expected, 'real CLI account result matches the native Store fixture');
  const intent = await readFile(first.intent('tt-provision-first'));
  assert.equal(JSON.parse(intent).context.transport, 'forge_https_v1');
  assert.deepEqual((await command(first, ['account', 'provision', '--request-id', 'tt-provision-first'])).result.value, provisioned);
  assert.deepEqual((await login(first, firstAccount)).result.value, provisioned);
  assert.deepEqual((await command(first, ['account', 'show'])).result.value, provisioned);
  const context = await readFile(first.context);
  assert.equal(JSON.parse(context).transport, 'forge_https_v1');
  assert.equal(JSON.parse(context).token_env, 'TT_RUNTIME_TOKEN');
  assert.equal((await stat(first.context)).mode & 0o777, 0o600);
  assert.ok(!context.includes(firstToken));
  assert.ok(!intent.includes(firstToken));
  await select(peer, peerAccount);
  const peerProvisioned = (await command(peer, ['account', 'provision', '--request-id', 'tt-provision-peer'])).result.value;
  assert.equal(peerProvisioned.account_id, peerAccount);
  assert.equal(peerProvisioned.monorepo_id, `forge-${peerAccount}`);
  assert.notEqual(peerProvisioned.monorepo_id, provisioned.monorepo_id);
  assert.deepEqual((await login(peer, peerAccount)).result.value, peerProvisioned);
  await login(first, peerAccount, false);
  assert.deepEqual(await readFile(first.context), context, 'denied cross-account login keeps the working context');
  await select(first, peerAccount);
  await command(first, ['account', 'show'], false);
  await command(first, ['account', 'provision', '--request-id', 'tt-cannot-steal-peer'], false);
  assert.deepEqual((await command(peer, ['account', 'show'])).result.value, peerProvisioned);
  await select(first, firstAccount);
  const keys = async () => (await (await mf.getR2Bucket('OBJECTS')).list({ prefix: `native/v1/${firstAccount}/` })).objects.map(object => object.key).sort();
  const originalKeys = await keys();
  assert.ok(originalKeys.includes(`native/v1/${firstAccount}/3/${provisioned.main_revision}`));
  await mf.dispose();
  mf = new Miniflare({ ...options(persist, { config, forge: true }), host: '127.0.0.1', port: Number(address.port) });
  assert.equal((await mf.ready).origin, endpoint, 'restart retains the exact context origin, not a rebound request');
  assert.deepEqual((await command(first, ['request', 'resume', 'tt-provision-first'])).result.value, provisioned);
  assert.deepEqual(await readFile(first.intent('tt-provision-first')), intent, 'resume keeps the original private intent bytes');
  assert.deepEqual((await command(first, ['account', 'show'])).result.value, provisioned);
  assert.deepEqual((await command(peer, ['account', 'show'])).result.value, peerProvisioned);
  assert.deepEqual(await keys(), originalKeys, 'provision retry/restart does not stage a new canonical repository');
});

async function closureHarness(t, history = false) {
  const entry = process.env.TINYTREE_CLOSURE_HARNESS_BUNDLE;
  const core = process.env.TINYTREE_CLOSURE_HARNESS_WASM;
  assert.ok(entry && core, 'declared test-only ClosureJobs bundle and RustWasm are required');
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-closure-module-'));
  const configuration = {
    modules: [{ type: 'ESModule', path: entry }, { type: 'CompiledWasm', path: core }],
    compatibilityDate: '2026-07-08',
    durableObjects: { CLOSURES: { className: 'ClosureHarness', useSQLite: true } },
    r2Buckets: ['OBJECTS'], durableObjectsPersist: path.join(persist, 'do'), r2Persist: path.join(persist, 'r2'),
  };
  const state = { mf: new Miniflare(configuration) };
  t.after(async () => { await state.mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const raw = async (action, fields = {}) => {
    const response = await state.mf.dispatchFetch('https://closure-module.invalid/harness', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ action, ...fields }),
    });
    return { code: response.status, value: await response.json() };
  };
  const invoke = async (action, fields = {}, expected = 200) => {
    const result = await raw(action, fields);
    assert.equal(result.code, expected, JSON.stringify(result.value)); return result.value;
  };
  const imported = [...fixture.objects, ...(history ? fixture.closure.objects : [])];
  await invoke('setup', { objects: imported, main: fixture.native.next });
  return {
    invoke, raw,
    bucket: () => state.mf.getR2Bucket('OBJECTS'),
    reload: async () => { await state.mf.dispose(); state.mf = new Miniflare(configuration); await state.mf.ready; },
  };
}
function closureKey(object) { return { kind: object.kind, id: object.id }; }
async function runClosure(harness, status) {
  for (let calls = 0; calls < 24 && !status.complete && status.error === null; calls++) {
    status = await harness.invoke('advance', { job_id: status.job_id, generation: status.generation });
  }
  assert.ok(status.complete || status.error, 'bounded fixture calls must reach ready or an explicit error');
  return status;
}
function jobTables(snapshot) {
  return { jobs: snapshot.jobs, nodes: snapshot.nodes, contexts: snapshot.contexts, obligations: snapshot.obligations, usage: snapshot.usage };
}

test('internal ClosureJobs persists native >256-object frontiers, fences competing advancement and atomically attaches exact roots', async t => {
  assert.ok(fixture.closure.objects.length >= 300);
  const harness = await closureHarness(t, true);
  let status = await harness.invoke('begin', { request_id: 'native-long-history', root: fixture.closure.root });
  assert.equal(status.complete, false);
  assert.deepEqual(await harness.invoke('begin', { request_id: 'native-long-history', root: fixture.closure.root }), status);
  const sameGeneration = { job_id: status.job_id, generation: status.generation };
  // Two real requests race through actual R2 awaits and SQLite transactions.
  const races = await Promise.all([
    harness.raw('advance', sameGeneration),
    harness.raw('advance', sameGeneration),
  ]);
  assert.deepEqual(races.map(result => result.code).sort(), [200, 409]);
  status = races.find(result => result.code === 200).value;
  assert.equal(races.find(result => result.code === 409).value.error, 'aborted');
  assert.equal(status.complete, false);
  assert.ok(status.counts.checked_objects > 0 && status.counts.checked_objects <= 64);
  const progress = await harness.invoke('inspect');
  assert.equal(progress.nodes.length, status.counts.objects, 'concurrent loser cannot double frontier counters');
  assert.equal(progress.nodes.filter(node => node.done === 1).length, status.counts.checked_objects);
  assert.equal(progress.contexts.length, status.counts.contexts);
  assert.equal(progress.obligations.length, status.counts.obligations);
  await harness.reload();
  assert.deepEqual(await harness.invoke('status', { job_id: status.job_id }), status, 'job identity, counters and cursor survive runtime disposal');
  assert.deepEqual(jobTables(await harness.invoke('inspect')), jobTables(progress));
  status = await runClosure(harness, status);
  assert.equal(status.phase, 'ready');
  assert.ok(status.counts.objects > 256);
  assert.equal(status.counts.checked_objects, status.counts.objects);
  assert.equal(status.counts.checked_contexts, status.counts.contexts);
  assert.equal(status.counts.checked_obligations, status.counts.obligations);
  const ready = await harness.invoke('inspect');
  await harness.invoke('fail-attach', { enabled: true });
  assert.equal((await harness.invoke('publish', { job_id: status.job_id, generation: status.generation }, 503)).error, 'fixture_attach_failure');
  const rolledBack = await harness.invoke('inspect');
  assert.deepEqual(jobTables(rolledBack), jobTables(ready), 'failed attach rolls back job phase and receipt');
  assert.equal(rolledBack.state.head, ready.state.head);
  assert.equal(rolledBack.state.generation, ready.state.generation);
  assert.equal(rolledBack.state.attachments, 0, 'callback wrote before throwing; native SQLite must roll it back');
  await harness.invoke('fail-attach', { enabled: false });
  const published = await harness.invoke('publish', { job_id: status.job_id, generation: status.generation });
  assert.equal(published.status.phase, 'published');
  assert.deepEqual(published.result, fixture.closure.native_publication, 'actual native Store publication reference is the oracle');
  assert.deepEqual(await harness.invoke('publish', { job_id: status.job_id, generation: status.generation }), published);
  const replayed = await harness.invoke('inspect');
  assert.equal(replayed.state.attachments, 1);
  assert.equal(replayed.state.generation, fixture.closure.native_publication.generation);
});

test('internal ClosureJobs never marks missing/corrupt bytes or native FILE/copy path violations ready', async t => {
  const harness = await closureHarness(t);
  for (const scenario of [
    { request: 'missing-dependency', root: 'missing_commit', error: 'object_unavailable', native: 'missing', refusal: 'missing_closure' },
    { request: 'wrong-file-size', root: 'wrong_size_commit', error: 'object_invalid', native: 'wrong_size', refusal: 'invalid_closure' },
    { request: 'wrong-copy-path', root: 'wrong_copy_commit', error: 'object_invalid', native: 'wrong_copy', refusal: 'invalid_closure' },
  ]) {
    assert.equal(fixture.native.refusals[scenario.native], scenario.refusal);
    const begin = await harness.invoke('begin', { request_id: scenario.request, root: closureKey(objects.get(scenario.root)) });
    const blocked = await runClosure(harness, begin);
    assert.equal(blocked.complete, false); assert.equal(blocked.error, scenario.error);
    assert.equal((await harness.invoke('publish', { job_id: blocked.job_id, generation: blocked.generation }, 400)).error, 'failed_precondition');
  }
  const file = objects.get('original_file');
  const key = `closure-fixture/${file.kind}/${file.id}`;
  const bucket = await harness.bucket();
  for (const scenario of [
    { request: 'stored-body-missing', mutate: () => bucket.delete(key) },
    { request: 'stored-body-corrupt', mutate: () => bucket.put(key, Uint8Array.from(file.bytes).map(byte => byte ^ 1)) },
  ]) {
    const begin = await harness.invoke('begin', { request_id: scenario.request, root: closureKey(objects.get('first_commit')) });
    await scenario.mutate();
    try {
      const blocked = await runClosure(harness, begin);
      assert.equal(blocked.complete, false); assert.ok(blocked.error);
      assert.equal((await harness.invoke('publish', { job_id: blocked.job_id, generation: blocked.generation }, 400)).error, 'failed_precondition');
    } finally { await bucket.put(key, Uint8Array.from(file.bytes)); }
  }
  assert.equal((await harness.invoke('inspect')).state.attachments, 0);
});

test('internal ClosureJobs rechecks SQL epoch after an admitted R2 read without persisting stale progress', async t => {
  const harness = await closureHarness(t);
  const status = await harness.invoke('begin', { request_id: 'epoch-fence', root: closureKey(objects.get('first_commit')) });
  const before = await harness.invoke('inspect');
  await harness.invoke('arm-read');
  const pending = harness.invoke('advance', { job_id: status.job_id, generation: status.generation }, 403);
  const deadline = Date.now() + 5000;
  while (!(await harness.invoke('inspect')).entered) {
    assert.ok(Date.now() < deadline, 'real R2 read must reach fixture gate');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  await harness.invoke('revoke');
  await harness.invoke('release-read');
  assert.equal((await pending).error, 'permission_denied');
  assert.deepEqual(jobTables(await harness.invoke('inspect')), jobTables(before));
});

// Test-only source import installs a verified root receipt, never main. Every
// view mutation, projection continuation and read below uses production HTTP.
async function projectionRuntime(t, source = fixture.projection) {
  const entry = process.env.TINYTREE_PROJECTION_HARNESS_BUNDLE;
  const core = process.env.TINYTREE_PROJECTION_HARNESS_WASM;
  assert.ok(entry && core && fixture.projection, 'declared projection harness/native oracle required');
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-projection-runtime-'));
  const token = 'wide-projection-owner-fixture';
  const config = [{ principal: source.owner, token_sha256: sha(token), accounts: [source.account_id], operator: true, kind: 'forge' }];
  const configuration = {
    host: '127.0.0.1', port: 0,
    modules: [{ type: 'ESModule', path: entry }, { type: 'CompiledWasm', path: core }],
    compatibilityDate: '2026-07-08',
    durableObjects: { REPOSITORIES: { className: 'AccountRepository', useSQLite: true } },
    r2Buckets: ['OBJECTS'], durableObjectsPersist: path.join(persist, 'do'), r2Persist: path.join(persist, 'r2'),
    bindings: { PRINCIPALS_JSON: JSON.stringify(config), FORGE_BACKEND: 'enabled', EXPERIMENTAL_RAW_BACKEND: 'disabled' },
  };
  const state = { mf: new Miniflare(configuration) };
  const address = await state.mf.ready;
  assert.equal(address.hostname, '127.0.0.1');
  t.after(async () => { await state.mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const call = async (operation, payload = {}, { id = '', auth = token, view = 'public' } = {}) => {
    const response = await state.mf.dispatchFetch('https://projection.invalid/api/v1/forge/call', {
      method: 'POST', headers: { 'content-type': 'application/json', ...(auth === null ? {} : { authorization: `Bearer ${auth}` }) },
      body: JSON.stringify({ version: 1, operation, account_id: source.account_id, view, request_id: id, payload_json: JSON.stringify(payload) }),
    });
    const envelope = await response.json();
    return { status: response.status, envelope, value: typeof envelope.result_json === 'string' ? JSON.parse(envelope.result_json) : undefined };
  };
  const success = async (operation, payload, options) => {
    const result = await call(operation, payload, options);
    assert.equal(result.status, 200, JSON.stringify(result)); return result;
  };
  const control = async (action, payload) => {
    const response = await state.mf.dispatchFetch(`https://projection.invalid/__fixture/${action}?account=${source.account_id}`, { method: 'POST', body: JSON.stringify(payload ?? {}) });
    const value = await response.json(); assert.equal(response.status, 200, JSON.stringify(value)); return value;
  };
  await success(1, {}, { id: 'wide-provision' });
  const before = (await success(2)).value;
  await control('seed', { objects: source.objects, root: source.root });
  assert.deepEqual((await success(2)).value, before, 'trusted fixture import must not mutate protected main');
  const published = await success(4, { spec: source.spec, expected_epoch: 1, make_default: true, follow_main: false }, { id: 'wide-view' });
  assert.deepEqual(published.value, source.published);
  const cliCommand = async args => {
    const tt = process.env.TINYTREE_TT_BINARY;
    assert.ok(tt, 'declared production tt executable required');
    const root = path.join(persist, 'projection-cli');
    const home = path.join(root, 'home');
    const cwd = path.join(root, 'workspace');
    const emptyPath = path.join(root, 'no-programs');
    await Promise.all([home, cwd, emptyPath].map(directory => mkdir(directory, { recursive: true, mode: 0o700 })));
    const jjConfig = path.join(root, 'jj.toml');
    await writeFile(jjConfig, '[user]\nname="Runtime Fixture"\nemail="fixture@example.invalid"\n[operation]\nhostname="test"\nusername="test"\n[ui]\npaginate="never"\n', { mode: 0o600 });
    const env = { HOME: home, USERPROFILE: home, XDG_CONFIG_HOME: path.join(home, 'config'), JJ_CONFIG: jjConfig, PATH: emptyPath, TERM: 'dumb', TT_PROJECTION_TOKEN: token };
    const execute = promisify(execFile);
    const command = async args => {
      const output = await execute(tt, ['--no-pager', '--color=never', ...args, '--json'], {
        cwd, env, encoding: 'utf8', timeout: 30000, maxBuffer: 2 * 1024 * 1024,
      });
      const report = JSON.parse(output.stdout);
      assert.equal(report.schema_version, 1);
      return report;
    };
    const selected = await command(['context', 'set', '--transport', 'forge-https-v1', '--server', address.origin, '--account', source.account_id, '--view', 'public', '--token-env', 'TT_PROJECTION_TOKEN']);
    assert.equal(selected.state, 'configured');
    const shown = await command(args);
    const saved = await readFile(path.join(home, 'config/tinytree/context.json'));
    assert.equal(JSON.parse(saved).transport, 'forge_https_v1');
    assert.ok(!saved.includes(token));
    return shown;
  };
  const showWithCli = async version => {
    const shown = await cliCommand(['view', 'show', '--version', Buffer.from(version).toString('hex')]);
    assert.equal(shown.state, 'complete'); return shown.result.value;
  };
  return { source, call, success, control, showWithCli, cliCommand,
    reload: async () => { await state.mf.dispose(); state.mf = new Miniflare({ ...configuration, port: Number(address.port) }); await state.mf.ready; },
    keys: async () => (await (await state.mf.getR2Bucket('OBJECTS')).list()).objects.map(object => object.key).sort(),
  };
}
function continuation(reply) { return { version: reply.value.view_version, progress: reply.value.progress }; }

test('production Forge projection pages >512 paths, persists pinned progress and serves only ready sanitized public closure', async t => {
  const h = await projectionRuntime(t);
  const first = await h.success(6);
  assert.equal(first.envelope.state, 'pending');
  assert.equal(first.value.projected_commit, undefined, 'pending is not a partial snapshot');
  const version = first.value.view_version;
  const deniedBeforeReady = await h.call(7, { version, path: 'README.md' });
  assert.equal(deniedBeforeReady.status, 400);
  assert.equal(deniedBeforeReady.envelope.error.code, 'failed_precondition');
  const input = continuation(first);
  const advanced = await h.success(6, input);
  assert.equal(advanced.envelope.state, 'pending');
  const beforeReplay = await h.control('inspect');
  assert.deepEqual(await h.success(6, input), advanced, 'lost ACK with stale generation returns current status without advancing');
  assert.deepEqual(await h.control('inspect'), beforeReplay);
  await h.reload();
  // Anonymous and authenticated callers share the same view-tagged work, but
  // each request still traverses the actual production policy checker.
  let ready = await h.success(6, { version }, { auth: null });
  assert.equal(ready.envelope.state, 'pending', 'actual tt must drive remaining cold preparation, not a warm snapshot');
  assert.equal(ready.value.progress.job_id, first.value.progress.job_id);
  const cliSnapshot = await h.showWithCli(version);
  const expectedCli = { ...h.source.expected_snapshot };
  for (const field of ['view_version', 'revision', 'projected_commit', 'projected_tree']) expectedCli[field] = Buffer.from(expectedCli[field]).toString('hex');
  assert.deepEqual(cliSnapshot, expectedCli, 'actual tt HTTPS continuation workflow matches native Store snapshot');
  // The earlier Node continuation is now stale, and must resolve exact readiness
  // without creating another job or switching the pinned immutable version.
  ready = await h.success(6, continuation(ready), { auth: null });
  assert.equal(ready.envelope.state, 'complete');
  assert.deepEqual(ready.value, h.source.expected_snapshot);
  assert.equal(ready.value.paths.length, 701);
  assert.ok(ready.value.readme.content.startsWith('\ufeff'), 'native BOM FILE text must remain exact');
  const readme = (await h.success(7, { version, path: 'README.md' }, { auth: null })).value;
  assert.equal(readme.content, ready.value.readme.content);
  const file = h.source.readme;
  const descriptor = (await h.success(9, { version, kind: 1, id: Array.from(Buffer.from(file.id, 'hex')), max_bytes: 0 }, { auth: null })).value;
  assert.deepEqual(descriptor.data, []); assert.equal(descriptor.size_bytes, file.bytes.length);
  const ranged = (await h.success(9, { version, kind: 1, id: Array.from(Buffer.from(file.id, 'hex')), offset: 1, max_bytes: 7 }, { auth: null })).value;
  assert.deepEqual(ranged.data, file.bytes.slice(1, 8)); assert.equal(ranged.offset, 1);
  for (const object of [h.source.root, h.source.private_file]) {
    const denied = await h.call(9, { version, kind: object.kind, id: Array.from(Buffer.from(object.id, 'hex')), max_bytes: 0 });
    assert.equal(denied.status, 403); assert.equal(denied.envelope.error.code, 'permission_denied');
  }
  await h.reload();
  assert.deepEqual((await h.success(6, { version }, { auth: null })).value, ready.value);
  const invalidToken = await h.call(6, { version }, { auth: 'invalid-not-anonymous' });
  assert.equal(invalidToken.status, 401);
  await h.success(4, { spec: { ...h.source.spec, visibility: { kind: 'members' } }, expected_epoch: 2, make_default: false, follow_main: false }, { id: 'lock-public' });
  for (const operation of [6, 7, 9]) {
    const payload = operation === 6 ? { version } : operation === 7 ? { version, path: 'README.md' } : { version, kind: 1, id: Array.from(Buffer.from(file.id, 'hex')), max_bytes: 0 };
    const denied = await h.call(operation, payload, { auth: null });
    assert.equal(denied.status, 403, 'warm SQL catalogs must not bypass changed visibility');
  }
});

test('production projection preparation reclaims obsolete epoch plans beyond active-job cap without deleting canonical storage', async t => {
  const h = await projectionRuntime(t);
  const sourceKeys = await h.keys();
  const catalogCount = (await h.control('inspect')).objects;
  let epoch = 2;
  const oldJobs = [];
  for (let ordinal = 0; ordinal < 10; ordinal++) {
    let pending = await h.success(6);
    assert.equal(pending.envelope.state, 'pending');
    // Reach a genuine persisted native build plan, stopping before staging.
    for (let calls = 0; calls < 20; calls++) {
      const state = await h.control('inspect');
      if (state.projection_plan_chunks[0].count > 0) break;
      pending = await h.success(6, continuation(pending));
      assert.equal(pending.envelope.state, 'pending');
    }
    const staged = await h.control('inspect');
    assert.ok(staged.projection_plan_chunks[0].count > 0, 'interruption must retain a real plan to reclaim');
    oldJobs.push(pending.value.progress.job_id);
    await h.success(4, { spec: { ...h.source.spec, exclude_prefixes: ['private', `file-${ordinal.toString().padStart(4, '0')}.txt`] }, expected_epoch: epoch, make_default: true, follow_main: false }, { id: `epoch-view-${ordinal}` });
    epoch++;
    const stale = await h.call(6, continuation(pending));
    assert.equal(stale.status, 409); assert.equal(stale.envelope.error.code, 'aborted');
  }
  const next = await h.success(6);
  assert.equal(next.envelope.state, 'pending', 'eleventh preparation remains admitted after >8 interruptions');
  const cleaned = await h.control('inspect');
  for (const id of oldJobs) assert.equal(cleaned.projection_jobs.find(job => job.id === id).phase, 'obsolete');
  assert.equal(cleaned.projection_plan_chunks[0].count, 0);
  assert.equal(cleaned.projection_selected[0].count, 0);
  assert.equal(cleaned.projection_outputs[0].count, 0);
  assert.equal(cleaned.projection_frontier[0].count, 1, 'only the new exact root frontier remains');
  for (const [key, value] of Object.entries(cleaned.projection_usage[0])) {
    if (typeof value === 'number') assert.ok(value >= 0, `${key} cannot underflow`);
  }
  assert.deepEqual(cleaned.objects, catalogCount);
  assert.deepEqual(await h.keys(), sourceKeys, 'epoch cleanup is preparation GC, not payload/catalog/source GC');
});

// No fixture endpoint or subclass is installed here: bootstrap authority, byte
// staging, closure validation and final CAS all traverse the shipped HTTP API.
test('production owner bootstrap imports a wide native closure once, resumes durable progress and atomically follows main', async t => {
  const source = fixture.projection;
  assert.ok(source && source.objects.length > 256, 'canonical native wide import fixture required');
  const account = 'bootstrap-destination';
  const boundaryAccount = 'bootstrap-boundary';
  const ownerToken = 'bootstrap-owner-runtime-fixture';
  const adminToken = 'bootstrap-admin-runtime-fixture';
  const rawToken = 'bootstrap-raw-runtime-fixture';
  const config = [
    { principal: 'bootstrap-owner', token_sha256: sha(ownerToken), accounts: [account, boundaryAccount], operator: true, kind: 'forge' },
    { principal: 'bootstrap-admin', token_sha256: sha(adminToken), accounts: [account, boundaryAccount], operator: true, kind: 'forge' },
    { principal: 'bootstrap-raw', token_sha256: sha(rawToken), accounts: [account, boundaryAccount], operator: true, kind: 'raw' },
  ];
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-bootstrap-runtime-'));
  const configuration = options(persist, { config, forge: true });
  let mf = new Miniflare(configuration);
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const reload = async () => { await mf.dispose(); mf = new Miniflare(configuration); };
  const invoke = async (operation, payload = {}, { id = '', token = ownerToken, selectedAccount = account, expected = 200, code, literalRequestId } = {}) => {
    const requestId = literalRequestId ?? (operation === 42 ? `ia-${payload.import_id}-${payload.expected_generation}` : id);
    const response = await mf.dispatchFetch('https://repository.invalid/api/v1/forge/call', {
      method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ version: 1, operation, account_id: selectedAccount, view: 'public', request_id: requestId, payload_json: JSON.stringify(payload) }),
    });
    const envelope = await response.json();
    assert.equal(response.status, expected, JSON.stringify(envelope));
    assert.equal(envelope.version, 1);
    if (expected !== 200) {
      if (code) assert.equal(envelope.error.code, code);
      return envelope;
    }
    assert.equal(envelope.state, 'complete');
    assert.equal(envelope.operation, operation);
    assert.equal(envelope.account_id, selectedAccount);
    assert.equal(envelope.request_id, requestId);
    return JSON.parse(envelope.result_json);
  };
  const provisioned = await invoke(1, {}, { id: 'bootstrap-provision' });
  assert.equal(provisioned.main_generation, 1);
  const genesis = Buffer.from(provisioned.main_revision).toString('hex');
  await invoke(3, { expected_epoch: 1, member: {
    principal: 'bootstrap-admin', can_write: true, can_review: true, is_admin: true, roles: [],
  } }, { id: 'bootstrap-add-admin' });
  const spec = { name: 'public', root_prefix: '', include_prefixes: [''], exclude_prefixes: [],
    visibility: { kind: 'public' }, revision: provisioned.main_revision, export_allowed: false };
  await invoke(4, { spec, expected_epoch: 2, make_default: true, follow_main: true }, { id: 'bootstrap-follow-main' });
  const before = await invoke(2);
  const namedBefore = await invoke(5);
  assert.equal(before.policy_epoch, 3);
  const beginPayload = { root: source.root,
    source: { repository_id: 'native-import-source', reference: 'main', generation: 2, head: source.root.id },
    destination: { generation: 1, head: genesis } };
  await invoke(40, beginPayload, { id: 'admin-cannot-import', token: adminToken, expected: 403, code: 'permission_denied' });
  await invoke(40, { ...beginPayload, destination: { generation: 1, head: source.root.id } },
    { id: 'wrong-genesis', expected: 400, code: 'invalid_argument' });
  await invoke(40, { ...beginPayload, destination: { generation: 2, head: genesis } },
    { id: 'wrong-main-generation', expected: 400, code: 'invalid_argument' });
  for (const operation of ['repository', 'refs/read', 'refs/cas']) {
    const payload = operation === 'refs/read' ? { names: ['main'] } : operation === 'refs/cas'
      ? { request_id: 'raw-override', name: 'main', expected_generation: 1, expected_head: genesis, head: source.root.id }
      : { request_id: 'raw-reprovision' };
    assert.equal((await call(mf, account, operation, payload, { token: rawToken })).status, 403,
      'raw operator identity cannot bypass a Forge-managed account');
  }
  const begin = await invoke(40, beginPayload, { id: 'bootstrap-begin' });
  assert.equal(begin.root.id, source.root.id);
  assert.deepEqual(await invoke(40, beginPayload, { id: 'bootstrap-begin' }), begin, 'lost Begin ACK returns its original receipt');
  await invoke(40, { ...beginPayload, source: { ...beginPayload.source, generation: 3 } },
    { id: 'bootstrap-begin', expected: 400, code: 'invalid_argument' });
  const importId = begin.import_id;
  const progress = status => ({ import_id: importId, expected_generation: status.generation });
  let status = (await invoke(42, progress(begin.status), { id: 'before-payloads' })).status;
  assert.equal(status.complete, false);
  assert.equal(status.error, 'object_unavailable', 'a declared canonical root is not byte/closure authority');
  const lostAdvance = progress(begin.status);
  await invoke(42, lostAdvance, { expected: 409, code: 'aborted' });
  assert.deepEqual((await invoke(43, { import_id: importId })).status, status,
    'lost Advance ACK recovery reads current status without a second advancement');
  await invoke(42, progress(status), {
    literalRequestId: `ia-${importId}-${begin.status.generation}`, expected: 400, code: 'invalid_argument',
  });
  assert.deepEqual((await invoke(43, { import_id: importId })).status, status,
    'a reused Advance identity with changed generation cannot mutate progress');
  await invoke(44, progress(status), { id: 'premature-finish', expected: 400, code: 'failed_precondition' });
  const file = source.objects.find(object => object.kind === 1 && object.bytes.length > 0);
  assert.ok(file);
  const putPayload = object => ({ import_id: importId,
    descriptor: { kind: object.kind, id: object.id, size_bytes: object.size_bytes, format_version: object.format_version ?? 1 },
    data_base64: Buffer.from(object.bytes).toString('base64') });
  const putId = (object, selectedImport = importId) => `io-${selectedImport}-${object.kind}-${object.id}`;
  const corrupt = Uint8Array.from(file.bytes); corrupt[0] ^= 1;
  await invoke(41, { ...putPayload(file), data_base64: Buffer.from(corrupt).toString('base64') },
    { id: putId(file), expected: 400, code: 'invalid_argument' });
  await invoke(41, { import_id: importId, descriptor: putPayload(file).descriptor },
    { id: putId(file), expected: 400, code: 'invalid_argument' });
  // Small bounded batches stay below production admission; no direct R2 or SQL
  // fixture writes mint import receipts or install source closure membership.
  for (let offset = 0; offset < source.objects.length; offset += 8) {
    await Promise.all(source.objects.slice(offset, offset + 8).map(object =>
      invoke(41, putPayload(object), { id: putId(object) })));
  }
  const largest = source.objects.filter(object => object.kind === 1).sort((a, b) => b.size_bytes - a.size_bytes)[0];
  const acknowledged = await invoke(41, putPayload(largest), { id: putId(largest) });
  assert.equal(acknowledged.state, 'ready');
  await invoke(41, { ...putPayload(file), data_base64: Buffer.from(corrupt).toString('base64') },
    { id: putId(file), expected: 400, code: 'invalid_argument' });
  const stored = await invoke(43, { import_id: importId });
  status = stored.status;
  await reload();
  assert.deepEqual(await invoke(43, { import_id: importId }), stored, 'import and closure progress survive SQLite/R2 reload');
  status = (await invoke(42, progress(status), { id: 'first-fresh-validation' })).status;
  assert.ok(status.counts.checked_objects > 0);
  assert.equal(status.complete, false, '>256 native objects cannot be treated as an all-in-one proof');
  await reload();
  assert.deepEqual((await invoke(43, { import_id: importId })).status, status);
  for (let step = 0; step < 200 && !status.complete; step++) {
    status = (await invoke(42, progress(status), { id: `validate-page-${step}` })).status;
    assert.equal(status.error, null, 'all imported bytes must pass actual native closure validation');
  }
  assert.equal(status.phase, 'ready');
  assert.ok(status.counts.objects > 256);
  assert.equal(status.counts.checked_objects, status.counts.objects);
  assert.equal(status.counts.checked_contexts, status.counts.contexts);
  assert.equal(status.counts.checked_obligations, status.counts.obligations);
  assert.deepEqual(await invoke(2), before, 'closure readiness alone never moves canonical main');
  assert.deepEqual(await invoke(5), namedBefore, 'following view is unchanged before explicit Finish');
  await invoke(44, progress(status), { id: 'admin-cannot-finish', token: adminToken, expected: 403, code: 'permission_denied' });
  await invoke(44, { import_id: importId, expected_generation: status.generation + 1 },
    { id: 'stale-finish-generation', expected: 409, code: 'aborted' });
  assert.deepEqual(await invoke(2), before, 'stale Finish generation cannot advance main');
  const finishPayload = progress(status);
  const finished = await invoke(44, finishPayload, { id: 'bootstrap-finish' });
  await reload();
  assert.deepEqual(await invoke(44, finishPayload, { id: 'bootstrap-finish' }), finished,
    'lost Finish ACK replays exact receipt after seal and runtime reload');
  const after = await invoke(2);
  assert.equal(after.main_generation, 2);
  assert.equal(Buffer.from(after.main_revision).toString('hex'), source.root.id);
  assert.equal(after.policy_epoch, before.policy_epoch, 'following main does not change access policy');
  const namedAfter = await invoke(5);
  assert.equal(namedAfter.length, 1);
  assert.deepEqual(namedAfter[0].revision, after.main_revision);
  assert.notDeepEqual(namedAfter[0].view_version, namedBefore[0].view_version);
  assert.deepEqual(after.public_view, namedAfter[0].view_version, 'default and named following pointers advance together');
  const published = await invoke(43, { import_id: importId });
  assert.equal(published.state, 'published');
  assert.deepEqual(published.receipt, finished);
  await invoke(40, beginPayload, { id: 'sealed-new-begin', expected: 400, code: 'failed_precondition' });
  await invoke(41, putPayload(file), { id: putId(file), expected: 400, code: 'failed_precondition' });
  await invoke(42, finishPayload, { id: 'sealed-advance', expected: 400, code: 'failed_precondition' });
  assert.deepEqual(await invoke(2), after, 'refused reopening cannot mutate main or policy');

  // A second independent native root reaches a FILE exactly at the supported
  // bound. The wide source remains its original native oracle, not a JS codec.
  const boundary = fixture.bootstrap;
  assert.ok(boundary?.largest_file && boundary.objects.length === 3);
  assert.equal(boundary.largest_file.size_bytes, 1024 * 1024);
  const boundaryCall = (operation, payload, id = '') => invoke(operation, payload, { id, selectedAccount: boundaryAccount });
  const boundaryGenesis = await boundaryCall(1, {}, 'boundary-provision');
  const boundaryBegin = await boundaryCall(40, {
    root: boundary.root,
    source: { repository_id: 'native-boundary-source', reference: 'main', generation: 1, head: boundary.root.id },
    destination: { generation: 1, head: Buffer.from(boundaryGenesis.main_revision).toString('hex') },
  }, 'boundary-begin');
  for (const object of boundary.objects) {
    await boundaryCall(41, { ...putPayload(object), import_id: boundaryBegin.import_id }, putId(object, boundaryBegin.import_id));
  }
  let boundaryStatus = boundaryBegin.status;
  for (let step = 0; step < 20 && !boundaryStatus.complete; step++) {
    boundaryStatus = (await boundaryCall(42, { import_id: boundaryBegin.import_id, expected_generation: boundaryStatus.generation }, `boundary-page-${step}`)).status;
    assert.equal(boundaryStatus.error, null);
  }
  assert.equal(boundaryStatus.phase, 'ready');
  assert.equal(boundaryStatus.counts.objects, 3);
  await boundaryCall(44, { import_id: boundaryBegin.import_id, expected_generation: boundaryStatus.generation }, 'boundary-finish');
  const boundaryAfter = await boundaryCall(2, {});
  assert.equal(boundaryAfter.main_generation, 2);
  assert.equal(Buffer.from(boundaryAfter.main_revision).toString('hex'), boundary.root.id);
  assert.deepEqual(await invoke(2), after, 'second account import cannot mutate the first account');
});

test('production search pages real native matches through CLI, replay and reload without crossing current view policy', async t => {
  const source = fixture.search;
  assert.equal(source.expected_hits.length, 131, 'native matcher oracle includes intra-FILE and late cross-file hits');
  const h = await projectionRuntime(t, source);
  const firstCli = await h.cliCommand(['search', '-F', 'needle', '--limit', '17']);
  assert.equal(firstCli.state, 'incomplete', 'fresh CLI search drives a cold projection and returns one bounded page');
  assert.deepEqual(firstCli.result.value.matches, source.expected_hits.slice(0, 17));
  const version = source.expected_snapshot.view_version;
  assert.equal(firstCli.result.value.view_version, Buffer.from(version).toString('hex'));
  const nextCli = await h.cliCommand(['search', '-F', 'needle', '--limit', '17', '--version', firstCli.result.value.view_version,
    '--cursor', firstCli.result.value.cursor]);
  assert.equal(nextCli.state, 'incomplete');
  assert.deepEqual(nextCli.result.value.matches, source.expected_hits.slice(17, 34));

  const query = { version, query: 'needle', regex: false, limit: 17 };
  const first = (await h.success(8, query)).value;
  assert.deepEqual(first.matches, source.expected_hits.slice(0, 17));
  const secondInput = { ...query, cursor: first.cursor };
  const [second, raced] = await Promise.all([h.success(8, secondInput), h.success(8, secondInput)]);
  assert.deepEqual(raced.value, second.value, 'same immutable page input returns one exact result and successor');
  await h.reload();
  assert.deepEqual((await h.success(8, secondInput)).value, second.value, 'lost ACK replay survives DO/SQLite restart');
  for (const [payload, options] of [
    [{ ...secondInput, query: 'changed' }, {}],
    [{ ...secondInput, limit: 18 }, {}],
    [{ ...secondInput, version: undefined }, {}],
    [secondInput, { auth: null }],
  ]) {
    const denied = await h.call(8, payload, options);
    assert.ok([400, 403, 409].includes(denied.status), JSON.stringify(denied));
    assert.equal(denied.value, undefined, 'invalid cursor bindings never return cached snippets');
  }
  const hits = [...first.matches, ...second.value.matches];
  let page = second.value;
  for (let count = 0; page.cursor !== null && count < 40; count++) {
    page = (await h.success(8, { ...query, cursor: page.cursor })).value;
    hits.push(...page.matches);
  }
  assert.equal(page.cursor, null, 'bounded scan terminates rather than recycling a non-progressing input');
  assert.equal(page.complete, false, 'excluded binary FILE is terminal partial coverage');
  assert.deepEqual(page.skipped_files, source.expected_skipped_files);
  assert.deepEqual(hits, source.expected_hits, 'all paths, byte columns, UTF-8/NUL previews and offsets match native Pattern exactly');
  assert.ok(!hits.some(hit => hit.path.includes('private') || hit.path === '02-link' || hit.path === '01-binary.bin'));

  // The late match is after eight references to a verified 1MiB FILE. A bounded
  // first page must not claim coverage of this entire source prefix.
  const lateQuery = { version, query: '^after prefix', regex: true, limit: 100 };
  let late = (await h.success(8, lateQuery, { auth: null })).value;
  assert.deepEqual(late.matches, []);
  assert.equal(late.complete, false);
  assert.ok(late.cursor);
  const lateHits = [];
  for (let count = 0; late.cursor !== null && count < 8; count++) {
    late = (await h.success(8, { ...lateQuery, cursor: late.cursor }, { auth: null })).value;
    lateHits.push(...late.matches);
  }
  assert.equal(late.cursor, null);
  assert.deepEqual(lateHits.map(hit => hit.path), [source.late_match_path]);
  assert.deepEqual(late.skipped_files, source.expected_skipped_files);

  await h.success(4, { spec: { ...source.spec, visibility: { kind: 'members' } }, expected_epoch: 2,
    make_default: false, follow_main: false }, { id: 'search-lock-current' });
  const revoked = await h.call(8, secondInput, { auth: null });
  assert.equal(revoked.status, 403);
  assert.equal(revoked.value, undefined, 'a warmed persisted page never survives public-to-private policy revocation');
  const oldOwnerPage = await h.call(8, secondInput);
  assert.ok([400, 403, 409].includes(oldOwnerPage.status));

  // Pattern validation is the actual native compiler, even with no FILEs to
  // trigger matching. An empty sanitized projection is not a compiler proof.
  const empty = (await h.success(4, { spec: { ...source.spec, include_prefixes: ['absent'] }, expected_epoch: 3,
    make_default: true, follow_main: false }, { id: 'search-empty-view' })).value;
  const emptyVersion = empty.view_version;
  const snapshot = await h.showWithCli(emptyVersion);
  assert.deepEqual(snapshot.paths, []);
  const invalid = await h.call(8, { version: emptyVersion, query: '(', regex: true, limit: 100 }, { auth: null });
  assert.equal(invalid.status, 400);
  const valid = (await h.success(8, { version: emptyVersion, query: 'needle', regex: false, limit: 100 }, { auth: null })).value;
  assert.equal(valid.complete, true); assert.equal(valid.cursor, null); assert.deepEqual(valid.matches, []);
  assert.deepEqual(valid.skipped_files, { binary: 0, oversized: 0 });
});
