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
const uiAssetsDirectory = process.env.TINYTREE_UI_ASSETS_DIRECTORY;
assert.ok(bundle && wasm && fixturePath, 'declared Worker, RustWasm and native Store fixture are required');
const fixture = JSON.parse(await readFile(fixturePath, 'utf8'));
const objects = new Map(fixture.objects.map(object => [object.name, object]));
const tokenA = 'account-a-runtime-fixture';
const tokenB = 'account-b-runtime-fixture';
const member = 'account-a-member-fixture';
const sha = value => createHash('sha256').update(value).digest('hex');
function staticAssets() {
  assert.ok(uiAssetsDirectory, 'declared UI deployment assets required');
  return { directory: uiAssetsDirectory, binding: 'ASSETS',
    routerConfig: { invoke_user_worker_ahead_of_assets: true, has_user_worker: true },
    assetConfig: { html_handling: 'none', not_found_handling: 'none' } };
}

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

function options(persist, { fault, config = principals(), forge = false, ui = false } = {}) {
  const modules = [{ type: 'ESModule', path: bundle }, { type: 'CompiledWasm', path: wasm }];
  if (fault) {
    // Test-only transport fault around the REAL R2 binding: after-write failure
    // leaves a genuine stored orphan, not an in-memory fake bucket.
    const entry = fault === 'sql-receipt' ? `import worker, {AccountRepository as Base} from './${path.basename(bundle)}';
      export default worker;
      export class AccountRepository extends Base {
        #fixtureStorage;
        constructor(ctx, env) {
          super(ctx, env);
          this.#fixtureStorage = ctx.storage;
          // Keep the real platform state/SQL objects. Fail the actual receipt
          // transaction, rather than proxying a branded DurableObjectState.
          ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS forge_account_receipts (account_id TEXT NOT NULL, principal TEXT NOT NULL, request_id TEXT NOT NULL, fingerprint TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(account_id,principal,request_id))');
          ctx.storage.sql.exec("CREATE TRIGGER fixture_receipt_failure BEFORE INSERT ON forge_account_receipts BEGIN SELECT RAISE(ABORT, 'deliberate receipt transaction failure'); END");
        }
        async fetch(request) {
          try { return await super.fetch(request); }
          finally { this.#fixtureStorage.sql.exec('DROP TRIGGER IF EXISTS fixture_receipt_failure'); }
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
    modules, compatibilityDate: '2026-07-08', ...(ui ? { assets: staticAssets() } : {}),
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
  const narrowMemberToken = 'forge-narrow-member-runtime-token';
  const other = 'forge-other-runtime-token';
  const raw = 'forge-raw-runtime-token';
  const account = fixture.forge.account_id;
  const allowed = [account, 'legacy', 'r2-recovery', 'sql-recovery'];
  const config = [
    { principal: 'forge-owner', token_sha256: sha(owner), accounts: allowed, operator: true, kind: 'forge' },
    { principal: 'forge-member', token_sha256: sha(memberToken), accounts: allowed, operator: false, kind: 'forge' },
    { principal: 'forge-member', token_sha256: sha(narrowMemberToken), accounts: [account], operator: false, kind: 'forge' },
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
    'https://repository.invalid/api/v1/forge/call', { method: 'POST', headers: { ...(token === null ? {} : { authorization: `Bearer ${token}` }), 'content-type': 'application/json' },
      body: JSON.stringify(envelope(operation, payload, requestId, selectedAccount)) });
  const forgeValue = async (response, status = 200, code) => {
    const result = await response.json();
    assert.equal(response.status, status, JSON.stringify(result)); assert.equal(result.version, 1);
    if (status !== 200) { if (code) assert.equal(result.error.code, code); return result; }
    assert.equal(result.view, 'public'); assert.equal(result.state, 'complete');
    return JSON.parse(result.result_json);
  };
  const ownerSession = await forgeValue(await callForge(12));
  assert.equal(ownerSession.schema_version, 1); assert.equal(ownerSession.account_id, account);
  assert.equal(ownerSession.identity.kind, 'authenticated'); assert.equal(ownerSession.identity.principal, 'forge-owner');
  assert.match(ownerSession.identity.credential_id, /^[0-9a-f]{64}$/); assert.notEqual(ownerSession.identity.credential_id, sha(owner));
  assert.deepEqual(await forgeValue(await callForge(12)), ownerSession, 'discovery before provisioning is not an account existence proof');
  const wideSession = await forgeValue(await callForge(12, {}, '', { token: memberToken }));
  const narrowSession = await forgeValue(await callForge(12, {}, '', { token: narrowMemberToken }));
  assert.equal(wideSession.identity.principal, narrowSession.identity.principal);
  assert.notEqual(wideSession.identity.credential_id, narrowSession.identity.credential_id, 'actual same-principal credentials do not merge ceilings');
  await forgeValue(await callForge(12, {}, '', { token: narrowMemberToken, selectedAccount: 'legacy' }), 403, 'permission_denied');
  assert.equal((await forgeValue(await callForge(12, {}, '', { token: memberToken, selectedAccount: 'legacy' }))).identity.credential_id, wideSession.identity.credential_id);
  await forgeValue(await callForge(12, {}, '', { token: narrowMemberToken, selectedAccount: 'legacy' }), 403, 'permission_denied');
  await forgeValue(await callForge(12, {}, '', { token: 'invalid-supplied-credential' }), 401, 'unauthenticated');
  await forgeValue(await callForge(12, { principal: 'forged' }), 400, 'invalid_argument');
  await forgeValue(await callForge(12, {}, '', { token: raw }), 403, 'permission_denied');
  await forgeValue(await callForge(1, {}, 'member-create', { token: memberToken }), 403, 'permission_denied');
  await forgeValue(await callForge(1, {}, 'wrong-kind', { token: raw }), 403, 'permission_denied');
  await forgeValue(await callForge(5, {}, '', { selectedAccount: 'legacy' }), 403, 'permission_denied');
  const provisioned = await forgeValue(await callForge(1, {}, 'provision-owner'));
  assert.deepEqual(provisioned, fixture.forge.provisioned);
  assert.deepEqual(await forgeValue(await callForge(1, {}, 'provision-owner')), provisioned);
  assert.deepEqual(await forgeValue(await callForge(2)), provisioned);
  await forgeValue(await callForge(2, {}, '', { token: memberToken }), 403, 'permission_denied');
  await forgeValue(await callForge(12, {}, '', { token: null }), 403, 'permission_denied');
  await forgeValue(await callForge(6, {}, '', { token: memberToken }), 403, 'permission_denied');
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
  assert.deepEqual(await forgeValue(await callForge(12, {}, '', { token: null })), { schema_version: 1, account_id: account, identity: { kind: 'anonymous' } });
  await forgeValue(await callForge(12, {}, '', { token: 'invalid-supplied-credential' }), 401, 'unauthenticated');

  assert.deepEqual(await forgeValue(await callForge(4, publishPublic, 'public-view')), views.public);
  assert.deepEqual(await forgeValue(await callForge(5, {}, '', { token: other })), [views.public], 'current public policy permits nonmember view discovery');
  assert.deepEqual((await forgeValue(await callForge(2))).public_view, views.public.view_version);
  const publishLocked = { spec: views.locked_spec, expected_epoch: 4, make_default: false };
  assert.deepEqual(await forgeValue(await callForge(4, publishLocked, 'locked-view')), views.locked);
  assert.deepEqual(await forgeValue(await callForge(5, {}, '', { token: other })), [], 'new current member policy removes old public discovery');
  await forgeValue(await callForge(12, {}, '', { token: null }), 403, 'permission_denied');
  assert.deepEqual(await forgeValue(await callForge(12, {}, '', { token: memberToken })), wideSession, 'session identity is not a cached ACL grant');

  assert.deepEqual(await forgeValue(await callForge(5, {}, '', { token: memberToken })), [views.locked]);
  assert.equal((await forgeValue(await callForge(2))).public_view, null);
  await forgeValue(await callForge(11, { version: views.locked.view_version, expected_epoch: 5 }, 'cannot-retire-current'), 400, 'failed_precondition');
  assert.deepEqual(await forgeValue(await callForge(11, { version: views.public.view_version, expected_epoch: 5 }, 'retire-old-view')), views.retired);
  assert.deepEqual(await forgeValue(await callForge(4, publishPublic, 'public-view')), views.public, 'historical view replay never changes current policy');
  assert.equal((await forgeValue(await callForge(2))).policy_epoch, 6);
  assert.deepEqual(await forgeValue(await callForge(5)), [{ ...views.locked, policy_epoch: 6 }]);
  await forgeValue(await callForge(8, { query: 'fixture', regex: false, limit: 10 }), 400, 'failed_precondition');
  await forgeValue(await callForge(20, {}, 'malformed-review'), 400, 'invalid_argument');
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
  assert.deepEqual(await forgeValue(await callForge(12)), ownerSession, 'same exact credential remains stable across SQLite restart');
  assert.deepEqual(await forgeValue(await callForge(5)), [{ ...views.locked, policy_epoch: 6 }]);
  assert.deepEqual(await forgeValue(await callForge(3, mutation, 'add-member')), fixture.forge.added);
  for (const [fault, selectedAccount] of [['after', 'r2-recovery'], ['sql-receipt', 'sql-recovery']]) {
    t.diagnostic(`account staging fault=${fault}`);
    await mf.dispose(); mf = new Miniflare(options(persist, { config, forge: true, fault }));
    await forgeValue(await callForge(1, {}, 'recover-provision', { selectedAccount }), 503, 'unavailable');
    await forgeValue(await callForge(2, {}, '', { selectedAccount }), 403, 'permission_denied');
    assert.equal((await call(mf, selectedAccount, 'repository', {}, { token: raw, method: 'GET' })).status, 404, 'no partial SQL claim');
    const bucket = await mf.getR2Bucket('OBJECTS');
    assert.ok((await bucket.list({ prefix: `native/v2/${selectedAccount}/` })).objects.length > 0, 'real staged R2 orphan');
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
  const keys = async () => (await (await mf.getR2Bucket('OBJECTS')).list({ prefix: `native/v2/${firstAccount}/` })).objects.map(object => object.key).sort();
  const originalKeys = await keys();
  const mainLocators = originalKeys.filter(key => key.startsWith(`native/v2/${firstAccount}/3/${provisioned.main_revision}/`));
  assert.equal(mainLocators.length, 1, 'provision replay retains one physical canonical main incarnation');
  const mainLocator = mainLocators[0];
  const attempt = mainLocator.split('/').at(-1);
  assert.match(attempt, /^[0-9a-f]{64}$/);
  const physicalMain = await (await mf.getR2Bucket('OBJECTS')).head(mainLocator);
  assert.ok(physicalMain);
  assert.deepEqual(physicalMain.customMetadata, { account: firstAccount, kind: '3', id: provisioned.main_revision, attempt, format: '1' });
  assert.ok(!mainLocator.includes(firstToken) && !mainLocator.includes(peerToken));
  assert.ok(originalKeys.every(key => !key.startsWith(`native/v2/${peerAccount}/`)), 'owner payload inventory stays account-scoped');
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
    assets: staticAssets(),
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
  const control = async (action, payload, expected = 200) => {
    const response = await state.mf.dispatchFetch(`https://projection.invalid/__fixture/${action}?account=${source.account_id}`, { method: 'POST', body: JSON.stringify(payload ?? {}) });
    const value = await response.json(); assert.equal(response.status, expected, JSON.stringify(value)); return value;
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
      let output;
      try {
        output = await execute(tt, ['--no-pager', '--color=never', ...args, '--json'], {
          cwd, env, encoding: 'utf8', timeout: 30000, maxBuffer: 2 * 1024 * 1024,
        });
      } catch (error) {
        const bounded = value => String(value ?? '').replaceAll(token, '<redacted>').slice(0, 4096);
        throw new Error(JSON.stringify({ stage: 'projection-cli', command: args.slice(0, 2),
          code: error.code, signal: error.signal, killed: error.killed,
          stdout: bounded(error.stdout), stderr: bounded(error.stderr) }));
      }
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
    bucket: () => state.mf.getR2Bucket('OBJECTS'),
    reload: async () => { await state.mf.dispose(); state.mf = new Miniflare({ ...configuration, port: Number(address.port) }); await state.mf.ready; },
    keys: async () => (await (await state.mf.getR2Bucket('OBJECTS')).list()).objects.map(object => object.key).sort(),
  };
}
function continuation(reply) { return { version: reply.value.view_version, progress: reply.value.progress }; }

// Existing public session protocol only. A pending Finalize is NOT a receipt;
// every repeated call names the same immutable session and request intent.
async function uploadObject(invoke, version, object, { id = object.request_id, pending } = {}) {
  const begin = await invoke({ action: 'begin', version, path: object.path, kind: object.kind,
    id: Array.from(Buffer.from(object.id, 'hex')), size_bytes: object.size_bytes }, id);
  assert.equal(begin.status, 200, JSON.stringify(begin));
  assert.equal(begin.envelope.state, begin.value.state);
  const session = begin.value.session_id;
  for (let offset = begin.value.offset; offset < object.bytes.length; offset += 65536) {
    const bytes = object.bytes.slice(offset, offset + 65536);
    const appended = await invoke({ action: 'append', session_id: session, offset, data: bytes }, id);
    assert.equal(appended.status, 200, JSON.stringify(appended));
    assert.deepEqual(appended.value.session_id, session);
    assert.equal(appended.value.offset, offset + bytes.length);
    assert.equal(appended.value.state, 'pending');
    assert.equal(appended.envelope.state, 'pending');
    assert.equal(appended.value.receipt, null);
  }
  let result; let rounds = 0;
  for (; rounds < 512; rounds++) {
    result = await invoke({ action: 'finalize', session_id: session }, id);
    if (result.status !== 200 || result.value.state === 'committed') break;
    assert.deepEqual(result.value.session_id, session);
    assert.equal(result.value.state, 'pending');
    assert.equal(result.envelope.state, 'pending');
    assert.equal(result.value.offset, object.size_bytes);
    assert.equal(result.value.receipt, null);
    if (pending) await pending(result, rounds);
  }
  assert.ok(rounds < 512, 'same upload finalization must terminate within the finite protocol budget');
  return { ...result, session_id: session, rounds: rounds + 1 };
}

test('ordinary scoped uploads match native byte receipts and copy/path authority without publishing main', async t => {
  const h = await projectionRuntime(t);
  const source = h.source.uploads;
  assert.equal(source.expected_wide_file_count, 701);
  const snapshot = await h.showWithCli(source.view_version);
  assert.equal(snapshot.projected_commit, Buffer.from(source.projected_base).toString('hex'));
  const before = { account: (await h.success(2)).value, views: (await h.success(5)).value };
  const invoke = (payload, id) => h.call(10, payload, { id, view: '' });
  let widePending = 0;
  for (const object of source.cases) {
    const result = await uploadObject(invoke, source.view_version, object, {
      pending: async (_reply, round) => {
        if (object.name === 'wide-root-tree') {
          widePending++;
          if (round === 0) await h.reload();
        }
      },
    });
    assert.deepEqual(result.session_id, object.session_id, `${object.name}: native session identity`);
    if (object.expected.state === 'committed') {
      assert.equal(result.status, 200, JSON.stringify(result));
      assert.equal(result.envelope.state, 'committed');
      assert.equal(result.value.state, 'committed');
      assert.equal(result.value.offset, object.expected.offset);
      for (const [field, value] of Object.entries(object.expected.receipt)) assert.deepEqual(result.value.receipt[field], value, `${object.name}: receipt ${field}`);
      assert.equal(typeof result.value.receipt.staging_lease, 'string');
      assert.ok(Number.isSafeInteger(result.value.receipt.expires_at));
      await h.reload();
      assert.deepEqual((await h.success(10, { action: 'finalize', session_id: result.session_id }, { id: object.request_id, view: '' })).value,
        result.value, `${object.name}: lost completion ACK returns one durable receipt`);
      assert.deepEqual((await h.success(10, { action: 'begin', version: source.view_version, path: object.path, kind: object.kind,
        id: Array.from(Buffer.from(object.id, 'hex')), size_bytes: object.size_bytes }, { id: object.request_id, view: '' })).value,
        result.value, `${object.name}: exact default-view Begin replay stays at the saved version`);
    } else {
      assert.notEqual(result.status, 200, `${object.name}: invalid metadata cannot receive an owned byte receipt`);
      assert.equal(result.envelope.error.code, object.expected.code, `${object.name}: native observable refusal`);
    }
  }
  assert.ok(widePending > 0, '701-path metadata scope is resumable work, not one synchronous publication');
  // The vendor TREE is a retained standalone upload root whose CopyHistory is
  // valid only at its canonical nonempty prefix. Production GC must preserve
  // that context and its dependencies, while deleting an unrelated orphan.
  const orphan = await h.control('storage', { action: 'orphan', object: { ...objects.get('original_file'), format_version: 1 } });
  const gcControl = (payload, expected) => h.control('gc', payload, expected);
  const retainedUploads = source.cases.filter(object => ['new-file', 'new-copy', 'nested-tree'].includes(object.name));
  for (const uploaded of retainedUploads) {
    const denied = await h.call(9, { version: source.view_version, kind: uploaded.kind,
      id: Array.from(Buffer.from(uploaded.id, 'hex')), max_bytes: 65536 });
    assert.equal(denied.status, 403, 'supplied-byte ownership is not a baseline view read grant even before GC');
    assert.equal(denied.envelope.error.code, 'permission_denied');
  }
  await gcControl({ action: 'request' });
  const collected = await finishProductionGc(gcControl);
  assert.equal(activeGcJob(collected).phase, 'complete', JSON.stringify(activeGcJob(collected)));
  assert.ok(activeGcJob(collected).deleted >= 1);
  assert.equal(collected.attempts.find(attempt => attempt.attempt_id === orphan.incarnation.attempt_id).state, 'deleted');
  for (const uploaded of retainedUploads) {
    const denied = await h.call(9, { version: source.view_version, kind: uploaded.kind,
      id: Array.from(Buffer.from(uploaded.id, 'hex')), max_bytes: 65536 });
    assert.equal(denied.status, 403, 'GC preserves, rather than broadens, unchanged baseline read authority');
    assert.equal(denied.envelope.error.code, 'permission_denied');
    const incarnation = collected.attempts.find(attempt => attempt.kind === uploaded.kind && attempt.id === uploaded.id && attempt.current === 1);
    assert.ok(incarnation, `${uploaded.name}: retained current catalog incarnation`);
    assert.equal(incarnation.state, 'written');
    const payload = await (await h.bucket()).get(incarnation.locator);
    assert.ok(payload, `${uploaded.name}: genuine retained R2 payload`);
    assert.deepEqual(Array.from(new Uint8Array(await payload.arrayBuffer())), uploaded.bytes);
    assert.deepEqual(payload.customMetadata, physicalMetadata(h.source.account_id, uploaded, incarnation.attempt_id));
  }

  assert.deepEqual((await h.success(2)).value, before.account, 'neither scoped receipts nor canonical closure readiness publishes main');
  assert.deepEqual((await h.success(5)).value, before.views, 'ordinary byte uploads cannot change named/default views');
  const first = source.cases[0];
  const unauthorized = await h.call(10, { action: 'status', session_id: first.session_id }, { auth: null, id: 'anonymous-upload-status' });
  assert.equal(unauthorized.status, 401);
  const closedLand = await h.call(24, { change_id: '0'.repeat(32), patchset_id: '0'.repeat(64), candidate_id: '0'.repeat(64) }, { id: 'upload-cannot-land' });
  assert.equal(closedLand.status, 400, 'byte receipts cannot bypass checked landing authority');
  assert.equal(closedLand.envelope.error.code, 'failed_precondition');
  assert.deepEqual((await h.success(2)).value, before.account, 'closed landing leaves protected main unchanged');
  const pending = await h.success(10, { action: 'begin', version: source.view_version, path: 'stale.txt', kind: first.kind,
    id: Array.from(Buffer.from(first.id, 'hex')), size_bytes: first.size_bytes }, { id: 'obsolete-upload' });
  await h.success(4, { spec: { ...h.source.spec, exclude_prefixes: ['private', 'vendor'] }, expected_epoch: source.policy_epoch,
    make_default: true, follow_main: false }, { id: 'upload-policy-change' });
  for (const session of [first.session_id, pending.value.session_id]) {
    const denied = await h.call(10, { action: 'finalize', session_id: session }, { id: 'obsolete-finalize' });
    assert.equal(denied.status, 409, 'warm committed or pending session cannot silently refresh its policy epoch');
    assert.equal(denied.envelope.error.code, 'aborted');
  }
  const charged = (await h.control('inspect')).forge_storage_usage[0];
  const keys = await h.keys();
  let epoch = source.policy_epoch + 1;
  for (let index = 0; index < 10; index++) {
    const receiving = await h.success(10, { action: 'begin', version: source.view_version,
      path: `interrupted-${index}.txt`, kind: first.kind, id: Array.from(Buffer.from(first.id, 'hex')),
      size_bytes: first.size_bytes }, { id: `epoch-upload-${index}` });
    await h.success(4, { spec: { ...h.source.spec, exclude_prefixes: ['private', 'vendor'] }, expected_epoch: epoch,
      make_default: true, follow_main: false }, { id: `upload-epoch-${index}` });
    epoch++;
    assert.equal((await h.call(10, { action: 'status', session_id: receiving.value.session_id }, { id: `obsolete-status-${index}` })).status, 409);
  }
  await h.success(10, { action: 'begin', version: source.view_version, path: 'after-interruptions.txt', kind: first.kind,
    id: Array.from(Buffer.from(first.id, 'hex')), size_bytes: first.size_bytes }, { id: 'after-ten-epochs' });
  const reclaimed = await h.control('inspect');
  assert.equal(reclaimed.forge_upload_usage[0].active, 1, '>8 obsolete sessions cannot starve the current writer');
  assert.equal(reclaimed.forge_upload_usage[0].assembly_bytes, first.size_bytes);
  assert.equal(reclaimed.forge_upload_buffers[0].count, 1, 'only current private assembly remains');
  assert.equal(reclaimed.forge_storage_usage[0].objects, charged.objects);
  assert.equal(reclaimed.forge_storage_usage[0].bytes, charged.bytes, 'obsolete assembly cleanup cannot refund retained R2 work');
  assert.deepEqual(await h.keys(), keys, 'epoch cleanup does not delete canonical payloads');
});

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
  const readerToken = 'bootstrap-readonly-runtime-fixture';
  const rawToken = 'bootstrap-raw-runtime-fixture';
  const config = [
    { principal: 'bootstrap-owner', token_sha256: sha(ownerToken), accounts: [account, boundaryAccount], operator: true, kind: 'forge' },
    { principal: 'bootstrap-admin', token_sha256: sha(adminToken), accounts: [account, boundaryAccount], operator: true, kind: 'forge' },
    { principal: 'bootstrap-reader', token_sha256: sha(readerToken), accounts: [account], operator: true, kind: 'forge' },
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
    principal: 'bootstrap-admin', can_write: false, can_review: false, is_admin: true, roles: [],
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

  // The shipped post-bootstrap upload API is independent of the sealed import
  // ledger. Provider operator status alone is not an ordinary writer grant;
  // account admin does imply write even when can_write is false.
  await invoke(3, { expected_epoch: after.policy_epoch, member: {
    principal: 'bootstrap-reader', can_write: false, can_review: false, is_admin: false, roles: [],
  } }, { id: 'bootstrap-add-reader' });
  const beforeOrdinary = await invoke(2);
  const uploadInvoke = async (payload, id, token = adminToken) => {
    const response = await mf.dispatchFetch('https://repository.invalid/api/v1/forge/call', { method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ version: 1, operation: 10, account_id: account, view: 'public', request_id: id, payload_json: JSON.stringify(payload) }) });
    const envelope = await response.json();
    return { status: response.status, envelope, value: typeof envelope.result_json === 'string' ? JSON.parse(envelope.result_json) : undefined };
  };
  const ordinaryFile = source.uploads.cases[0];
  const ordinaryBegin = { action: 'begin', version: namedAfter[0].view_version, path: ordinaryFile.path,
    kind: ordinaryFile.kind, id: Array.from(Buffer.from(ordinaryFile.id, 'hex')), size_bytes: ordinaryFile.size_bytes };
  const readonly = await uploadInvoke(ordinaryBegin, 'readonly-is-not-writer', readerToken);
  assert.equal(readonly.status, 403); assert.equal(readonly.envelope.error.code, 'permission_denied');
  const ordinary = await uploadObject(uploadInvoke, namedAfter[0].view_version, ordinaryFile, { id: 'ordinary-after-bootstrap' });
  assert.equal(ordinary.status, 200, JSON.stringify(ordinary));
  assert.equal(ordinary.value.state, 'committed');
  assert.equal(ordinary.value.receipt.id.length, 32);
  await reload();
  assert.deepEqual((await uploadInvoke({ action: 'finalize', session_id: ordinary.session_id }, 'ordinary-after-bootstrap')).value,
    ordinary.value, 'ordinary receipt survives restart and exact lost ACK replay');
  const duplicateChunk = { action: 'append', session_id: ordinary.session_id, offset: 0, data: ordinaryFile.bytes };
  assert.deepEqual((await uploadInvoke(duplicateChunk, 'ordinary-after-bootstrap')).value, ordinary.value,
    'exact committed chunk replay checks the immutable original bytes');
  const conflictingBytes = [...ordinaryFile.bytes]; conflictingBytes[0] ^= 1;
  const conflict = await uploadInvoke({ ...duplicateChunk, data: conflictingBytes }, 'ordinary-after-bootstrap');
  assert.equal(conflict.status, 409); assert.equal(conflict.envelope.error.code, 'already_exists');
  const boundaryFile = { ...fixture.bootstrap.largest_file, path: 'boundary-copy.bin' };
  const largeOrdinary = await uploadObject(uploadInvoke, namedAfter[0].view_version, boundaryFile, { id: 'ordinary-boundary-file' });
  assert.equal(largeOrdinary.status, 200, JSON.stringify(largeOrdinary));
  assert.equal(largeOrdinary.value.receipt.size_bytes, 1024 * 1024, 'ordinary upload supports the actual1MiB storage boundary through64KiB chunks');
  const foreign = await uploadInvoke({ action: 'status', session_id: ordinary.session_id }, 'foreign-session', ownerToken);
  assert.equal(foreign.status, 403, 'even the account owner cannot claim another actor session');
  assert.deepEqual(await invoke(2), beforeOrdinary, 'ordinary supplied bytes do not change main or policy');
  await invoke(40, beginPayload, { id: 'still-sealed-after-upload', expected: 400, code: 'failed_precondition' });

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
  assert.deepEqual(await invoke(2), beforeOrdinary, 'second account import cannot mutate the first account');
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

test('declared deployment packet serves exact same-origin UI assets without API fallback or credential reflection', async t => {
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-ui-assets-'));
  const mf = new Miniflare(options(persist, { forge: true, ui: true }));
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const origin = 'https://repository.invalid';
  const redirected = await mf.dispatchFetch(`${origin}/ui/?account=sample&view=public&version=${'a'.repeat(64)}&token=must-not-reflect`, { redirect: 'manual' });
  assert.equal(redirected.status, 307);
  const destination = new URL(redirected.headers.get('location'), origin);
  assert.equal(destination.searchParams.get('mode'), 'forge');
  assert.equal(destination.searchParams.get('account'), 'sample');
  assert.equal(destination.searchParams.get('view'), 'public');
  assert.equal(destination.searchParams.get('version'), 'a'.repeat(64));
  assert.equal(destination.searchParams.has('token'), false);
  assert.equal(redirected.headers.get('set-cookie'), null);
  for (const [route, file, mime] of [
    ['/ui/?mode=forge', 'index.html', 'text/html'],
    ['/ui/app.js', 'app.js', 'text/javascript'],
    ['/ui/style.css', 'style.css', 'text/css'],
    ['/ui/tinytree_ui.wasm', 'tinytree_ui.wasm', 'application/wasm'],
  ]) {
    const response = await mf.dispatchFetch(origin + route, { headers: { authorization: 'Bearer must-not-reflect', cookie: 'fixture=must-not-reflect' } });
    assert.equal(response.status, 200, route);
    assert.ok(response.headers.get('content-type').startsWith(mime));
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.equal(response.headers.get('x-content-type-options'), 'nosniff');
    assert.equal(response.headers.get('referrer-policy'), 'no-referrer');
    const csp = response.headers.get('content-security-policy');
    assert.ok(csp.includes("connect-src 'self'"));
    assert.ok(csp.includes("object-src 'none'"));
    assert.ok(csp.includes("frame-ancestors 'none'"));
    assert.ok(!csp.includes("'unsafe-inline'"));
    assert.equal(response.headers.get('set-cookie'), null);
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), await readFile(path.join(uiAssetsDirectory, 'ui', file)));
    const head = await mf.dispatchFetch(origin + route, { method: 'HEAD' });
    assert.equal(head.status, 200); assert.equal((await head.arrayBuffer()).byteLength, 0);
  }
  const badAsset = await mf.dispatchFetch(origin + '/ui/app.js', { method: 'POST', body: '{}' });
  assert.equal(badAsset.status, 405); assert.equal(badAsset.headers.get('allow'), 'GET, HEAD');
  const absent = await mf.dispatchFetch(origin + '/ui/not-a-route');
  assert.equal(absent.status, 404);
  const api = await mf.dispatchFetch(origin + '/api/v1/not-a-route');
  assert.equal(api.status, 501); assert.ok(!api.headers.get('content-type')?.startsWith('text/html'));
  const body = await api.json(); assert.equal(body.version, 1); assert.equal(body.error.code, 'unimplemented');
});

// Production HTTP only: no fixture seed route, direct SQL, manufactured upload
// grant or checker verdict. Native main stayed genesis in the admission oracle;
// this runtime deliberately bootstraps the complete source before preparing it.
test('production reviews admit shipped bootstrap source, persist exact patchsets and expose only scoped candidate projections', async t => {
  const started = Date.now();
  const stage = name => t.diagnostic(`review stage=${name} elapsed_ms=${Date.now() - started}`);
  const source = fixture.projection;
  const oracle = source.reviews;
  assert.ok(oracle, 'native scoped upload/admission and pure builder oracle required');
  assert.equal(oracle.admission_oracle.candidate_state_parity, false);
  const token = 'review-owner-runtime-fixture';
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-review-runtime-'));
  const configuration = options(persist, { forge: true, config: [{ principal: source.owner,
    token_sha256: sha(token), accounts: [source.account_id], operator: true, kind: 'forge' }] });
  configuration.host = '127.0.0.1'; configuration.port = 0;
  configuration.bindings.FORGE_REVIEW_CONFIG_JSON = JSON.stringify({ profile: oracle.profile,
    committer: { name: 'Runtime Fixture', email: 'fixture@example.invalid' } });
  let mf = new Miniflare(configuration);
  const address = await mf.ready;
  configuration.port = Number(address.port);
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const reload = async () => { await mf.dispose(); mf = new Miniflare(configuration); await mf.ready; };
  const callReview = async (operation, payload = {}, { id = '', auth = token, signal } = {}) => {
    const response = await mf.dispatchFetch('https://repository.invalid/api/v1/forge/call', {
      signal, method: 'POST', headers: { 'content-type': 'application/json', ...(auth === null ? {} : { authorization: `Bearer ${auth}` }) },
      body: JSON.stringify({ version: 1, operation, account_id: source.account_id, view: 'public', request_id: id, payload_json: JSON.stringify(payload) }),
    });
    const errorBody = response.status === 200 ? undefined : (await response.clone().text()).replaceAll(token, '<redacted>').slice(0, 4096);
    const envelope = await response.json();
    if (response.status === 200) {
      assert.equal(envelope.operation, operation); assert.equal(envelope.request_id, id);
      assert.equal(envelope.account_id, source.account_id); assert.equal(envelope.view, 'public');
    }
    return { status: response.status, envelope, value: typeof envelope.result_json === 'string' ? JSON.parse(envelope.result_json) : undefined,
      ...(errorBody === undefined ? {} : { error_body: errorBody }) };
  };
  const success = async (...args) => { const reply = await callReview(...args); assert.equal(reply.status, 200, JSON.stringify(reply)); return reply; };
  const provisioned = (await success(1, {}, { id: 'review-provision' })).value;
  const begin = (await success(40, { root: source.root,
    source: { repository_id: 'native-review-source', reference: 'main', generation: 1, head: source.root.id },
    destination: { generation: 1, head: Buffer.from(provisioned.main_revision).toString('hex') },
  }, { id: 'review-bootstrap' })).value;
  for (let offset = 0; offset < source.objects.length; offset += 8) {
    await Promise.all(source.objects.slice(offset, offset + 8).map(object => success(41, {
      import_id: begin.import_id,
      descriptor: { kind: object.kind, id: object.id, size_bytes: object.size_bytes, format_version: object.format_version ?? 1 },
      data_base64: Buffer.from(object.bytes).toString('base64'),
    }, { id: `io-${begin.import_id}-${object.kind}-${object.id}` })));
  }
  let imported = begin.status;
  for (let round = 0; round < 200 && !imported.complete; round++) {
    imported = (await success(42, { import_id: begin.import_id, expected_generation: imported.generation },
      { id: `ia-${begin.import_id}-${imported.generation}` })).value.status;
    assert.equal(imported.error, null);
  }
  assert.equal(imported.phase, 'ready');
  await success(44, { import_id: begin.import_id, expected_generation: imported.generation }, { id: 'review-bootstrap-finish' });
  stage('bootstrap-published');
  const main = (await success(2)).value;
  assert.equal(Buffer.from(main.main_revision).toString('hex'), source.root.id);
  assert.equal(main.main_generation, 2);
  await success(4, { spec: source.spec, expected_epoch: 1, make_default: true, follow_main: false }, { id: 'review-public-view' });
  const before = (await success(2)).value;
  assert.equal(before.policy_epoch, 2);
  let baseline = await success(6, {});
  for (let round = 0; round < 512 && baseline.envelope.state === 'pending'; round++) baseline = await success(6, continuation(baseline));
  assert.equal(baseline.envelope.state, 'complete');
  assert.deepEqual(baseline.value, source.expected_snapshot);
  stage('baseline-ready');
  const version = baseline.value.view_version;
  const upload = (payload, id) => callReview(10, payload, { id });
  for (const object of oracle.objects) {
    const result = await uploadObject(upload, version, object);
    assert.equal(result.status, 200, JSON.stringify(result));
    assert.equal(result.value.state, 'committed');
    for (const field of ['kind', 'id', 'size_bytes', 'format_version']) {
      assert.deepEqual(result.value.receipt[field], object.native_receipt[field], `native supplied-byte receipt ${object.name}/${field}`);
    }
  }
  stage('uploads-committed');
  const resultState = reply => ({ state: reply.envelope.state, value: reply.value });
  // Match serde_json's explicit CLI object encoding, including omitted lineage.
  const pushPayload = { expected_patchset: null, predecessor: null, provenance: null,
    source_base: oracle.push.source_base, source_head: oracle.push.source_head, view_version: oracle.push.view_version };
  const push = async (round = -1, previous = 'new', signal) => {
    const reply = await callReview(20, pushPayload, { id: oracle.request_id, signal });
    assert.equal(reply.status, 200, JSON.stringify({ request_id: oracle.request_id, round, previous_state: previous, ...(reply.status === 200 ? {} : { response_body: reply.error_body }) }));
    return reply;
  };
  let review = await push();
  assert.equal(review.envelope.state, 'admission_pending', 'wide admission must be durable work, not a fabricated accepted patchset');
  assert.deepEqual(review.value, { schema_version: 1, request_id: oracle.request_id,
    source_base: oracle.push.source_base, source_head: oracle.push.source_head,
    view_version: oracle.push.view_version, phase: 'admission' });
  assert.deepEqual(resultState(await success(22, {}, { id: oracle.request_id })), resultState(review), 'operation lookup never advances admission');
  await reload();
  assert.deepEqual(resultState(await success(22, {}, { id: oracle.request_id })), resultState(review), 'uncertain admission survives workerd restart');
  // Actual declared tt resumes the same pending server intent over loopback
  // Forge HTTP. Only explicit immutable IDs are used; no mounted authoring claim.
  const cliRoot = path.join(persist, 'review-cli');
  const home = path.join(cliRoot, 'home'), cwd = path.join(cliRoot, 'workspace'), emptyPath = path.join(cliRoot, 'no-programs');
  await Promise.all([home, cwd, emptyPath].map(directory => mkdir(directory, { recursive: true, mode: 0o700 })));
  const jjConfig = path.join(cliRoot, 'jj.toml');
  await writeFile(jjConfig, '[user]\nname="Runtime Fixture"\nemail="fixture@example.invalid"\n[operation]\nhostname="test"\nusername="test"\n[ui]\npaginate="never"\n', { mode: 0o600 });
  const tt = process.env.TINYTREE_TT_BINARY;
  assert.ok(tt, 'declared production tt executable required');
  const env = { HOME: home, USERPROFILE: home, XDG_CONFIG_HOME: path.join(home, 'config'), JJ_CONFIG: jjConfig,
    PATH: emptyPath, TERM: 'dumb', TT_REVIEW_TOKEN: token };
  const cli = async (args, deadline) => {
    let output;
    try {
      const timeout = deadline === undefined ? 70000 : Math.min(70000, deadline - Date.now());
      assert.ok(timeout > 0, 'same-intent admission exceeds its 120-second absolute budget');
      output = await promisify(execFile)(tt, ['--no-pager', '--color=never', ...args, '--json'],
        { cwd, env, encoding: 'utf8', timeout, maxBuffer: 2 * 1024 * 1024 });
    } catch (error) {
      const stderr = String(error.stderr ?? '');
      const bounded = value => String(value ?? '').replaceAll(token, '<redacted>').slice(0, 4096);
      const diagnostic = new Error(JSON.stringify({ stage: 'review-cli', command: args.slice(0, 2),
        code: error.code, signal: error.signal, killed: error.killed,
        stdout: bounded(error.stdout), stderr: bounded(stderr), message: bounded(error.message) }));
      diagnostic.resumableAdmissionDeadline = !error.killed && typeof error.code === 'number'
        && /deadline/i.test(stderr) && stderr.includes(`saved request ${oracle.request_id} can be reconciled with tt request resume`);
      throw diagnostic;
    }
    const report = JSON.parse(output.stdout); assert.equal(report.schema_version, 1); return report;
  };
  assert.equal((await cli(['context', 'set', '--transport', 'forge-https-v1', '--server', address.origin,
    '--account', source.account_id, '--view', 'public', '--token-env', 'TT_REVIEW_TOKEN'])).state, 'configured');
  const contextBytes = await readFile(path.join(home, 'config/tinytree/context.json'), 'utf8');
  assert.equal(JSON.parse(contextBytes).transport, 'forge_https_v1'); assert.ok(!contextBytes.includes(token));
  const journalPath = path.join(home, 'config/tinytree/requests', `request-${oracle.request_id}.json`);
  let originalJournal;
  const retainedIntent = async () => {
    const bytes = await readFile(journalPath);
    const saved = JSON.parse(bytes);
    assert.equal(saved.request_id, oracle.request_id); assert.equal(saved.operation, 20);
    assert.equal(saved.source, null, 'explicit immutable source is not a manufactured workspace acknowledgement');
    assert.deepEqual(saved.context, JSON.parse(contextBytes));
    assert.equal(JSON.stringify(saved.payload), JSON.stringify(pushPayload), 'HTTP and actual CLI use byte-identical original payload JSON');
    assert.ok(!bytes.includes(Buffer.from(token)));
    assert.equal((await stat(journalPath)).mode & 0o777, 0o600);
    assert.equal(await readFile(path.join(home, 'config/tinytree/context.json'), 'utf8'), contextBytes);
    if (originalJournal) assert.deepEqual(bytes, originalJournal, 'timeout/resume retains the exact immutable intent, without acknowledgement or replacement');
    else originalJournal = bytes;
  };
  const admissionDeadline = Date.now() + 120000;
  let cliReview;
  for (let attempt = 0; attempt <= 3; attempt++) {
    if (attempt) await retainedIntent();
    stage(`actual-tt-admission-attempt-${attempt}`);
    try {
      cliReview = await cli(attempt === 0
        ? ['push', '--source-base', oracle.push.source_base, '--source-head', oracle.push.source_head,
          '--view-version', oracle.push.view_version, '--request-id', oracle.request_id]
        : ['request', 'resume', oracle.request_id], admissionDeadline);
      break;
    } catch (error) {
      if (!error.resumableAdmissionDeadline) throw error;
      // A known SDK deadline is uncertainty, not an accepted patchset. Check
      // durable private identity BEFORE any supported resume action.
      await retainedIntent();
      stage(`actual-tt-admission-deadline-${attempt}`);
      assert.ok(attempt < 3 && Date.now() < admissionDeadline, 'same-intent admission exhausted its bounded resume budget');
    }
  }
  assert.ok(cliReview);
  await retainedIntent();
  assert.equal(cliReview.state, 'rebase_pending');
  assert.equal(cliReview.result.value.patchset_id, oracle.patchset_id);
  assert.equal(cliReview.result.value.source_head, oracle.push.source_head);
  review = await success(22, {}, { id: oracle.request_id });
  assert.deepEqual(resultState(review), { state: cliReview.state, value: cliReview.result.value });
  stage('actual-tt-admitted');
  const preparationDeadline = Date.now() + 120000;
  const preparationSignal = AbortSignal.timeout(120000);
  let lastReadState;
  const states = new Set(['admission_pending', review.envelope.state]);
  for (let round = 0; round < 512; round++) {
    assert.ok(Date.now() < preparationDeadline, 'review preparation exceeds its 120-second absolute budget');
    review = await push(round, review.envelope.state, preparationSignal); states.add(review.envelope.state);
    if (round % 16 === 0) stage(`prepare-${round}-${review.envelope.state}`);
    if (review.envelope.state !== 'admission_pending') {
      assert.equal(review.value.change_id, oracle.change_id);
      assert.equal(review.value.patchset_id, oracle.patchset_id, 'native stable patchset identity, not native candidate state parity');
      assert.equal(review.value.source_head, oracle.push.source_head);
      assert.equal(review.value.patchset_number, 1);
      if (lastReadState !== review.envelope.state || review.value.projected_head) {
        assert.deepEqual(resultState(await success(21, { change_id: oracle.change_id }, { signal: preparationSignal })), resultState(review), 'review lookup is read-only');
        assert.deepEqual(resultState(await success(22, {}, { id: oracle.request_id, signal: preparationSignal })), resultState(review), 'operation lookup is read-only after admission');
        lastReadState = review.envelope.state;
      }
      if (review.envelope.state === 'checks_pending' && review.value.projected_head) break;
      assert.ok(['rebase_pending', 'checks_pending'].includes(review.envelope.state), JSON.stringify(review));
    }
  }
  assert.ok(states.has('rebase_pending'));
  assert.equal(review.envelope.state, 'checks_pending');
  assert.ok(review.value.projected_head, 'candidate readiness alone must not grant an unfinished projection');
  assert.ok(Date.now() < preparationDeadline, 'review preparation must complete within 120 seconds');
  stage('candidate-projection-ready');
  const candidate = review.value.candidate;
  assert.equal(candidate.root_tree, oracle.builder_oracle.root_tree, 'actual pure-builder tree retains excluded canonical subtree');
  assert.equal(candidate.target, source.root.id);
  assert.equal(candidate.target_generation, 2);
  assert.deepEqual(candidate.rewrites, [{ old: oracle.push.source_head, new: candidate.head, change_id: oracle.change_id }]);
  assert.deepEqual(review.value.projected_rewrites, [{ old: oracle.push.source_head, new: review.value.projected_head, change_id: oracle.change_id }]);
  assert.deepEqual((await success(6, { version })).value, baseline.value, 'preparation never mutates the baseline view');
  assert.deepEqual((await success(2)).value, before, 'admission, rebase and projection never publish main');
  const readObject = (kind, id, max_bytes = 65536) => callReview(9, { version, kind, id: Array.from(Buffer.from(id, 'hex')), max_bytes });
  const projected = await readObject(3, review.value.projected_head);
  assert.equal(projected.status, 200, JSON.stringify(projected));
  assert.equal(projected.value.kind, 3); assert.ok(projected.value.data.length > 0);
  assert.ok(!Buffer.from(projected.value.data).includes(Buffer.from(source.root.id, 'hex')), 'safe commit cannot expose canonical parent');
  const anonymousProjected = await callReview(9, { version, kind: 3,
    id: Array.from(Buffer.from(review.value.projected_head, 'hex')), max_bytes: 65536 }, { auth: null });
  assert.equal(anonymousProjected.status, 200, JSON.stringify(anonymousProjected));
  assert.deepEqual(anonymousProjected.value, projected.value, 'anonymous public reads use the same typed ready-result catalog');
  const anonymousPrivate = await callReview(9, { version, kind: source.private_file.kind,
    id: Array.from(Buffer.from(source.private_file.id, 'hex')), max_bytes: 0 }, { auth: null });
  assert.equal(anonymousPrivate.status, 403); assert.equal(anonymousPrivate.envelope.error.code, 'permission_denied');
  const authored = oracle.objects.find(object => object.kind === 1);
  assert.deepEqual((await readObject(1, authored.id)).value.data, authored.bytes);
  for (const forbidden of [source.private_file, source.root, { kind: 3, id: candidate.head }]) {
    const denied = await readObject(forbidden.kind, forbidden.id, 0);
    assert.equal(denied.status, 403, JSON.stringify(denied)); assert.equal(denied.envelope.error.code, 'permission_denied');
  }
  await reload();
  assert.deepEqual(resultState(await success(22, {}, { id: oracle.request_id })), resultState(review), 'candidate and scoped result survive reload without advancement');
  stage('scoped-reads-reload-verified');
  const selector = { change_id: oracle.change_id, patchset_id: oracle.patchset_id };
  const staleVote = await callReview(23, { ...selector, decision: 'approve', candidate_id: '0'.repeat(64) }, { id: 'review-stale-vote' });
  assert.equal(staleVote.status, 400); assert.equal(staleVote.envelope.error.code, 'failed_precondition');
  const vote = { ...selector, decision: 'approve', candidate_id: candidate.id };
  const approved = await success(23, vote, { id: 'review-approve' });
  assert.equal(approved.value.approvals, 1);
  assert.deepEqual(await success(23, vote, { id: 'review-approve' }), approved);
  const comment = { ...selector, decision: 'comment', path: 'README.md', body: 'Scoped review comment before policy narrowing' };
  const commented = await success(23, comment, { id: 'review-comment' });
  assert.equal(commented.value.comments.length, 1);
  assert.equal(commented.value.comments[0].body, comment.body);
  assert.deepEqual(await success(23, comment, { id: 'review-comment' }), commented);
  const land = await callReview(24, { ...selector, candidate_id: candidate.id }, { id: 'review-no-checker-land' });
  assert.equal(land.status, 400); assert.equal(land.envelope.error.code, 'failed_precondition', 'approval is not trusted checker evidence');
  assert.deepEqual((await success(2)).value, before);
  await success(4, { spec: { ...source.spec, visibility: { kind: 'members' } }, expected_epoch: 2,
    make_default: false, follow_main: false }, { id: 'review-narrow-policy' });
  const narrowed = await success(21, selector);
  assert.equal(narrowed.envelope.state, 'stale');
  assert.deepEqual(narrowed.value.comments, [], 'warmed unchanged-path comments from a previous policy epoch are not disclosed');
  assert.equal(narrowed.value.approvals, 0);
  const anonymous = await callReview(21, selector, { auth: null });
  assert.equal(anonymous.status, 401);
  assert.equal(anonymous.envelope.error.code, 'unauthenticated');
  const oldVote = await callReview(23, vote, { id: 'review-new-epoch-vote' });
  assert.equal(oldVote.status, 409); assert.equal(oldVote.envelope.error.code, 'aborted');
  stage('votes-policy-verified');
  const after = (await success(2)).value;
  assert.deepEqual(after.main_revision, before.main_revision); assert.equal(after.main_generation, before.main_generation);
});

// Production account/bootstrap/export/review routes only. Native fixture facts
// cover canonical bytes and lineage, not a fabricated passing check verdict.
test('declared tt transforms two bootstrapped monorepos with exact pending replay and retained redacted pins', async t => {
  const f = fixture.transform;
  assert.ok(f?.native.review_admitted);
  const token = 'transform-owner-runtime-fixture', readerToken = 'transform-destination-reader-fixture';
  const accounts = [f.source.account_id, f.destination.account_id];
  const config = [
    { principal: f.owner, token_sha256: sha(token), accounts, operator: true, kind: 'forge' },
    { principal: 'destination-reader', token_sha256: sha(readerToken), accounts: [f.destination.account_id], operator: false, kind: 'forge' },
  ];
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-transform-runtime-'));
  const configuration = { ...options(persist, { config, forge: true }), host: '127.0.0.1', port: 0 };
  configuration.bindings.FORGE_REVIEW_CONFIG_JSON = JSON.stringify({ profile: fixture.checker.profile,
    committer: { name: 'Transform Runtime', email: 'fixture@example.invalid' } });
  let mf = new Miniflare(configuration);
  const address = await mf.ready; configuration.port = Number(address.port);
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const reload = async () => { await mf.dispose(); mf = new Miniflare(configuration); await mf.ready; };
  const call = async (account, operation, payload = {}, { id = '', auth = token } = {}) => {
    const response = await mf.dispatchFetch(address.origin + '/api/v1/forge/call', { method: 'POST', signal: AbortSignal.timeout(30000),
      headers: { 'content-type': 'application/json', authorization: `Bearer ${auth}` },
      body: JSON.stringify({ version: 1, operation, account_id: account, view: 'public', request_id: id, payload_json: JSON.stringify(payload) }),
    });
    const envelope = await response.json();
    if (response.status === 200) {
      assert.equal(envelope.operation, operation); assert.equal(envelope.account_id, account); assert.equal(envelope.request_id, id); assert.equal(envelope.view, 'public');
    }
    return { status: response.status, envelope, value: typeof envelope.result_json === 'string' ? JSON.parse(envelope.result_json) : undefined };
  };
  const success = async (...args) => { const result = await call(...args); assert.equal(result.status, 200, JSON.stringify(result)); return result; };
  for (const source of [f.source, f.destination]) {
    const provisioned = (await success(source.account_id, 1, {}, { id: 'transform-provision' })).value;
    const begun = (await success(source.account_id, 40, { root: source.root,
      source: { repository_id: `native-${source.account_id}`, reference: 'main', generation: 1, head: source.root.id },
      destination: { generation: 1, head: Buffer.from(provisioned.main_revision).toString('hex') } }, { id: 'transform-bootstrap' })).value;
    for (let offset = 0; offset < source.objects.length; offset += 8) {
      await Promise.all(source.objects.slice(offset, offset + 8).map(object => success(source.account_id, 41, { import_id: begun.import_id,
        descriptor: { kind: object.kind, id: object.id, size_bytes: object.size_bytes, format_version: object.format_version },
        data_base64: Buffer.from(object.bytes).toString('base64') }, { id: `io-${begun.import_id}-${object.kind}-${object.id}` })));
    }
    let status = begun.status;
    for (let step = 0; step < 128 && !status.complete; step++) {
      status = (await success(source.account_id, 42, { import_id: begun.import_id, expected_generation: status.generation }, { id: `ia-${begun.import_id}-${status.generation}` })).value.status;
      assert.equal(status.error, null);
    }
    assert.equal(status.phase, 'ready');
    await success(source.account_id, 44, { import_id: begun.import_id, expected_generation: status.generation }, { id: 'transform-bootstrap-finish' });
  }
  await success(f.source.account_id, 4, { spec: f.source.before_spec, expected_epoch: 1, make_default: true, follow_main: false }, { id: 'transform-before-view' });
  await success(f.source.account_id, 4, { spec: f.source.after_spec, expected_epoch: 2, make_default: true, follow_main: false }, { id: 'transform-after-view' });
  await success(f.destination.account_id, 4, { spec: f.destination.spec, expected_epoch: 1, make_default: true, follow_main: false }, { id: 'transform-destination-view' });
  const beforeMain = (await success(f.destination.account_id, 2)).value;
  const tt = process.env.TINYTREE_TT_BINARY; assert.ok(tt);
  const home = path.join(persist, 'cli-home'), cwd = path.join(persist, 'cli-workspace'), emptyPath = path.join(persist, 'no-programs');
  await Promise.all([home, cwd, emptyPath].map(directory => mkdir(directory, { mode: 0o700 })));
  const jjConfig = path.join(home, 'jj.toml');
  await writeFile(jjConfig, '[user]\nname="Runtime Fixture"\nemail="fixture@example.invalid"\n[operation]\nhostname="test"\nusername="test"\n[ui]\npaginate="never"\n', { mode: 0o600 });
  const env = { HOME: home, USERPROFILE: home, XDG_CONFIG_HOME: path.join(home, 'config'), JJ_CONFIG: jjConfig,
    PATH: emptyPath, TERM: 'dumb', TT_TRANSFORM_TOKEN: token };
  const cli = async (args, succeeds = true, deadline) => {
    try {
      const output = await promisify(execFile)(tt, ['--no-pager', '--color=never', ...args, '--json'],
        { cwd, env, encoding: 'utf8', timeout: deadline ? Math.max(1, Math.min(70000, deadline - Date.now())) : 70000, maxBuffer: 2 * 1024 * 1024 });
      assert.ok(succeeds, 'expected server refusal'); const result = JSON.parse(output.stdout); assert.equal(result.schema_version, 1); return result;
    } catch (error) { if (succeeds || !Number.isInteger(error.code)) throw error; return { refused: true }; }
  };
  await cli(['context', 'set', '--transport', 'forge-https-v1', '--server', address.origin, '--account', f.destination.account_id, '--view', 'public', '--token-env', 'TT_TRANSFORM_TOKEN']);
  const planPath = path.join(home, 'transform.json'); await writeFile(planPath, JSON.stringify(f.plan), { mode: 0o600 });
  const key = 'runtime-cross-account-transform';
  const initial = await success(f.destination.account_id, 30, f.plan, { id: key });
  assert.equal(initial.envelope.state, 'transform_pending');
  assert.equal(initial.value.change_id, f.plan.change_id);
  await reload();
  let proposed;
  const deadline = Date.now() + 120000;
  for (let attempt = 0; attempt < 3 && !proposed; attempt++) {
    try {
      assert.ok(Date.now() < deadline, 'transform resume exceeds absolute acceptance budget');
      proposed = await cli(attempt === 0 ? ['transform', 'start', '--plan', planPath, '--request-id', key] : ['request', 'resume', key], true, deadline);
    } catch (error) {
      const stderr = String(error.stderr ?? '');
      if (!/deadline/i.test(stderr) || !stderr.includes(`saved request ${key}`) || Date.now() >= deadline) throw error;
      const journal = JSON.parse(await readFile(path.join(home, 'config/tinytree/requests', `request-${key}.json`), 'utf8'));
      assert.equal(journal.operation, 30); assert.equal(journal.request_id, key); assert.equal(journal.context.transport, 'forge_https_v1');
    }
  }
  assert.ok(proposed);
  const product = proposed.result.value;
  assert.equal(product.transform.id, f.native.run_id);
  assert.equal(product.review.change_id, f.plan.change_id);
  const journalPath = path.join(home, 'config/tinytree/requests', `request-${key}.json`);
  const journalBytes = await readFile(journalPath);
  const resumed = await cli(['request', 'resume', key]);
  assert.deepEqual(resumed.result.value.transform, product.transform);
  for (const field of ['patchset_id', 'source_head', 'source_base', 'provenance', 'change_id']) assert.deepEqual(resumed.result.value.review[field], product.review[field]);
  assert.deepEqual(await readFile(journalPath), journalBytes);
  let prepared = resumed.result.value;
  for (let step = 0; step < 128 && !prepared.review.projected_head; step++) prepared = (await success(f.destination.account_id, 30, f.plan, { id: key })).value;
  assert.ok(prepared.review.candidate && prepared.review.projected_head);
  assert.equal(prepared.review.candidate.root_tree, f.native.output_root, 'actual canonical builder preserves the excluded destination parent subtree');
  await cli(['request', 'reconcile', key], false);
  const savedPlan = JSON.parse(journalBytes).payload;
  assert.equal(savedPlan.destination_view, f.plan.destination_view);
  const rebound = await call(f.destination.account_id, 30, { ...f.plan, change_id: 'ef'.repeat(16) }, { id: key });
  assert.equal(rebound.status, 409); assert.equal(rebound.envelope.error.code, 'already_exists');
  const run = product.transform.id;
  await reload();
  const shown = await cli(['transform', 'show', run]);
  assert.equal(shown.result.value.id, run); assert.ok(shown.result.value.origin);
  const lineage = await cli(['lineage', run]);
  assert.equal(lineage.state, 'complete');
  for (const operation of ['rename', 'copy']) assert.ok(lineage.result.value.origin.lineage.some(edge => edge.operation === operation));
  const lineageFacts = edges => edges.map(edge => ({ operation: edge.operation, source_account: edge.source_account,
    source_revision: Array.isArray(edge.source_revision) ? Buffer.from(edge.source_revision).toString('hex') : edge.source_revision,
    source_path: edge.source_path, destination_path: edge.destination_path })).sort((a, b) => `${a.operation}\0${a.source_path}\0${a.destination_path}`.localeCompare(`${b.operation}\0${b.source_path}\0${b.destination_path}`));
  assert.deepEqual(lineageFacts(lineage.result.value.origin.lineage), lineageFacts(f.native.lineage));
  assert.ok(lineage.result.value.origin.lineage.every(edge => edge.destination_revision === product.transform.output_head));
  const paths = [], fileMetadata = [];
  let afterPath;
  do {
    const page = await cli(['pin', 'resolve', run, '--limit', '2', ...(afterPath ? ['--after-path', afterPath] : [])]);
    fileMetadata.push(...page.result.value.files); paths.push(...page.result.value.files.map(file => file.path)); afterPath = page.result.value.next_path;
  } while (afterPath && paths.length < 8);
  assert.deepEqual(paths, f.pin_bytes.map(file => file.path));
  assert.deepEqual(fileMetadata, f.native.pin_files.files);
  for (const file of f.pin_bytes) {
    const first = await cli(['pin', 'resolve', run, '--path', file.path, '--max-bytes', '5']);
    const rest = await cli(['pin', 'resolve', run, '--path', file.path, '--offset', '5', '--max-bytes', '65536']);
    assert.deepEqual([...first.result.value.bytes, ...rest.result.value.bytes], file.bytes); assert.equal(rest.result.value.eof, true);
  }
  const protectedPin = await call(f.destination.account_id, 34, { run_id: run }, { id: 'retire-protected-transform' });
  assert.equal(protectedPin.status, 400); assert.equal(protectedPin.envelope.error.code, 'failed_precondition');
  await cli(['pin', 'retire', run, '--request-id', 'tt-retire-protected-transform'], false);
  await cli(['pin', 'resolve', 'ff'.repeat(32)], false);
  assert.deepEqual((await success(f.destination.account_id, 2)).value, beforeMain, 'preparation, retained bytes and CLI replay never publish main');
  await success(f.destination.account_id, 3, { expected_epoch: 2, member: { principal: 'destination-reader', can_write: false, can_review: false, is_admin: false, roles: [] } }, { id: 'transform-add-reader' });
  await success(f.source.account_id, 4, { spec: { ...f.source.after_spec, export_allowed: false }, expected_epoch: 3,
    make_default: true, follow_main: false }, { id: 'transform-disable-export' });
  const redacted = await success(f.destination.account_id, 35, { run_id: run }, { auth: readerToken });
  assert.equal(redacted.envelope.state, 'origin_unavailable'); assert.equal(redacted.value.origin, null);
  const retained = await success(f.destination.account_id, 33, { run_id: run, path: f.pin_bytes[0].path, max_bytes: 65536 }, { auth: readerToken });
  assert.deepEqual(retained.value.bytes, f.pin_bytes[0].bytes, 'destination retention survives origin export revocation, not a source-GC claim');
  await success(f.destination.account_id, 4, { spec: { ...f.destination.spec, exclude_prefixes: ['private', 'vendor'] }, expected_epoch: 3,
    make_default: true, follow_main: false }, { id: 'transform-narrow-destination' });
  const denied = await call(f.destination.account_id, 33, { run_id: run }, { auth: readerToken });
  assert.equal(denied.status, 403); assert.equal(denied.envelope.error.code, 'permission_denied');
});

// Fault-only TEST bundle: all source bytes, bootstrap authority and transform
// preparation still use shipped APIs. No fake permit/root/check is installed.
test('private proved transform roots cannot become public views before source arm or after export revocation', { timeout: 180000 }, async t => {
  const f = fixture.transform;
  const entry = process.env.TINYTREE_PROJECTION_HARNESS_BUNDLE;
  const core = process.env.TINYTREE_PROJECTION_HARNESS_WASM;
  assert.ok(f && entry && core, 'declared transform fixture and test-only projection harness required');
  const token = 'transform-private-stage-owner-fixture';
  const accounts = [f.source.account_id, f.destination.account_id];
  const config = [{ principal: f.owner, token_sha256: sha(token), accounts, operator: true, kind: 'forge' }];
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-private-transform-runtime-'));
  const configuration = { ...options(persist, { config, forge: true }), host: '127.0.0.1', port: 0,
    modules: [{ type: 'ESModule', path: entry }, { type: 'CompiledWasm', path: core }] };
  configuration.bindings.EXPERIMENTAL_RAW_BACKEND = 'disabled';
  configuration.bindings.FORGE_REVIEW_CONFIG_JSON = JSON.stringify({ profile: fixture.checker.profile,
    committer: { name: 'Transform Runtime', email: 'fixture@example.invalid' } });
  const mf = new Miniflare(configuration);
  const address = await mf.ready;
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  const call = async (account, operation, payload = {}, { id = '', view = 'public' } = {}) => {
    const response = await mf.dispatchFetch(address.origin + '/api/v1/forge/call', { method: 'POST', signal: AbortSignal.timeout(30000),
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify({ version: 1, operation, account_id: account, view, request_id: id, payload_json: JSON.stringify(payload) }) });
    const envelope = await response.json();
    if (response.status === 200) {
      assert.equal(envelope.operation, operation); assert.equal(envelope.account_id, account);
      assert.equal(envelope.request_id, id); assert.equal(envelope.view, view);
    }
    return { status: response.status, envelope, value: typeof envelope.result_json === 'string' ? JSON.parse(envelope.result_json) : undefined };
  };
  const success = async (...args) => { const result = await call(...args); assert.equal(result.status, 200, JSON.stringify(result)); return result; };
  const control = async (account, block) => {
    const response = await mf.dispatchFetch(`${address.origin}/__fixture/transform-control?account=${account}`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ block_arm_export: block }) });
    assert.equal(response.status, 200); return response.json();
  };
  const peek = async account => {
    const response = await mf.dispatchFetch(`${address.origin}/__fixture/transform-peek?account=${account}`);
    assert.equal(response.status, 200); return response.json();
  };
  // Complete canonical histories enter both accounts solely through ordinary
  // owner bootstrap. In particular the source's before commit is not raw-seeded.
  for (const source of [f.source, f.destination]) {
    const provisioned = (await success(source.account_id, 1, {}, { id: 'private-stage-provision' })).value;
    const begun = (await success(source.account_id, 40, { root: source.root,
      source: { repository_id: `native-${source.account_id}`, reference: 'main', generation: 1, head: source.root.id },
      destination: { generation: 1, head: Buffer.from(provisioned.main_revision).toString('hex') } }, { id: 'private-stage-bootstrap' })).value;
    for (let offset = 0; offset < source.objects.length; offset += 8) {
      await Promise.all(source.objects.slice(offset, offset + 8).map(object => success(source.account_id, 41, { import_id: begun.import_id,
        descriptor: { kind: object.kind, id: object.id, size_bytes: object.size_bytes, format_version: object.format_version },
        data_base64: Buffer.from(object.bytes).toString('base64') }, { id: `io-${begun.import_id}-${object.kind}-${object.id}` })));
    }
    let status = begun.status;
    for (let step = 0; step < 128 && !status.complete; step++) {
      status = (await success(source.account_id, 42, { import_id: begun.import_id, expected_generation: status.generation }, { id: `ia-${begun.import_id}-${status.generation}` })).value.status;
      assert.equal(status.error, null);
    }
    assert.equal(status.phase, 'ready');
    await success(source.account_id, 44, { import_id: begun.import_id, expected_generation: status.generation }, { id: 'private-stage-bootstrap-finish' });
  }
  await success(f.source.account_id, 4, { spec: f.source.before_spec, expected_epoch: 1, make_default: true, follow_main: false }, { id: 'private-stage-before-view' });
  await success(f.source.account_id, 4, { spec: f.source.after_spec, expected_epoch: 2, make_default: true, follow_main: false }, { id: 'private-stage-after-view' });
  await success(f.destination.account_id, 4, { spec: f.destination.spec, expected_epoch: 1, make_default: true, follow_main: false }, { id: 'private-stage-destination-view' });
  const canonicalMain = (await success(f.destination.account_id, 2)).value;
  // Positive routing/control: the identical public-view shape succeeds for an
  // actually canonical root, not for either privately staged proposal root.
  await success(f.destination.account_id, 4, { spec: { name: 'canonical-control', root_prefix: '', include_prefixes: [''], exclude_prefixes: [],
    visibility: { kind: 'public' }, revision: canonicalMain.main_revision, export_allowed: true },
    expected_epoch: canonicalMain.policy_epoch, make_default: false, follow_main: false }, { id: 'private-stage-canonical-control', view: 'canonical-control' });
  const originalMain = (await success(f.destination.account_id, 2)).value;
  assert.equal(originalMain.policy_epoch, canonicalMain.policy_epoch + 1);
  assert.deepEqual(originalMain.main_revision, canonicalMain.main_revision);
  assert.equal(originalMain.main_generation, canonicalMain.main_generation);
  assert.equal((await control(f.source.account_id, true)).block_arm_export, 1);
  const key = 'private-stage-block-before-arm';
  const deadline = Date.now() + 120000;
  let staged;
  for (let step = 0; step < 512; step++) {
    assert.ok(Date.now() < deadline, 'private-stage preparation exceeds absolute acceptance budget');
    const result = await call(f.destination.account_id, 30, f.plan, { id: key });
    if (result.status === 503) {
      assert.equal(result.envelope.error.code, 'unavailable');
      const source = await peek(f.source.account_id);
      const destination = await peek(f.destination.account_id);
      if (source.control.arm_export_calls > 0 && destination.stage_roots.length === 2) { staged = destination; break; }
    } else {
      assert.equal(result.status, 200, JSON.stringify(result));
      assert.equal(result.envelope.state, 'transform_pending', 'run must not commit while source arm is blocked');
    }
  }
  assert.ok(staged, 'must observe the exact two private proof roots before testing publication denial');
  assert.equal(staged.committed_runs, 0); assert.equal(staged.active_pin_roots, 0);
  assert.equal(staged.drafts.length, 1); assert.equal(staged.drafts[0].request_id, key);
  assert.equal(staged.drafts[0].phase, 'prepared');
  assert.match(staged.drafts[0].run_id, /^[0-9a-f]{64}$/);
  const denyPublicRunAndPin = async () => {
    for (const operation of [31, 33]) {
      const result = await call(f.destination.account_id, operation, { run_id: staged.drafts[0].run_id });
      assert.equal(result.status, 404, JSON.stringify(result)); assert.equal(result.envelope.error.code, 'not_found');
    }
  };
  await denyPublicRunAndPin();
  assert.deepEqual(staged.stage_roots.map(root => root.owner_kind).sort(), ['transform-stage-artifact', 'transform-stage-output']);
  assert.equal(new Set(staged.stage_roots.map(root => root.id)).size, 2);
  for (const root of staged.stage_roots) {
    assert.equal(root.kind, 3); assert.equal(root.complete, 1); assert.equal(root.proof_phase, 'published');
  }
  const denyPublicViews = async phase => {
    for (const root of staged.stage_roots) {
      const name = root.owner_kind === 'transform-stage-artifact' ? 'private-artifact-probe' : 'private-output-probe';
      const result = await call(f.destination.account_id, 4, { spec: { name, root_prefix: '', include_prefixes: [''], exclude_prefixes: [],
        visibility: { kind: 'public' }, revision: Array.from(Buffer.from(root.id, 'hex')), export_allowed: true },
        expected_epoch: originalMain.policy_epoch, make_default: false, follow_main: false }, { id: `${phase}-${name}`, view: name });
      assert.equal(result.status, 400, JSON.stringify(result)); assert.equal(result.envelope.error.code, 'failed_precondition');
    }
    assert.deepEqual((await success(f.destination.account_id, 2)).value, originalMain, 'rejected root publication must not mutate account/main/epoch');
  };
  await denyPublicViews('before-revoke');
  await success(f.source.account_id, 4, { spec: { ...f.source.after_spec, export_allowed: false }, expected_epoch: 3,
    make_default: true, follow_main: false }, { id: 'private-stage-revoke-export' });
  const revoked = await peek(f.source.account_id);
  assert.equal(revoked.views.find(view => view.name === f.source.after_spec.name)?.export_allowed, 0, 'ordinary acknowledged view mutation disabled source export');
  await denyPublicViews('after-revoke');
  // Remove only the denial fault. The real source policy must now reject arm;
  // the fixture never produces a permit, even for cleanup/reconciliation.
  await control(f.source.account_id, false);
  const rejected = await call(f.destination.account_id, 30, f.plan, { id: key });
  assert.ok([403, 409].includes(rejected.status), JSON.stringify(rejected));
  const final = await peek(f.destination.account_id);
  assert.equal(final.committed_runs, 0); assert.equal(final.active_pin_roots, 0);
  assert.ok(final.drafts.every(draft => draft.phase !== 'committed'));
  await denyPublicRunAndPin();
  assert.deepEqual((await success(f.destination.account_id, 2)).value, originalMain);
});



// Test-only trusted maintenance actions; product provisioning still uses the
// shipped ingress. The harness uses genuine SQLite/R2 and production lifecycle
// methods, not public deletion authority or a synthetic successful receipt.
async function storageLifecycleRuntime(t, accounts) {
  const entry = process.env.TINYTREE_PROJECTION_HARNESS_BUNDLE;
  const core = process.env.TINYTREE_PROJECTION_HARNESS_WASM;
  assert.ok(entry && core, 'declared storage harness and RustWasm required');
  const persist = await mkdtemp(path.join(tmpdir(), 'tinytree-storage-lifecycle-'));
  const token = 'storage-lifecycle-owner-fixture';
  const configuration = {
    modules: [{ type: 'ESModule', path: entry }, { type: 'CompiledWasm', path: core }],
    compatibilityDate: '2026-07-08',
    durableObjects: { REPOSITORIES: { className: 'AccountRepository', useSQLite: true } },
    r2Buckets: ['OBJECTS'], durableObjectsPersist: path.join(persist, 'do'), r2Persist: path.join(persist, 'r2'),
    bindings: { PRINCIPALS_JSON: JSON.stringify([{ principal: 'storage-owner', token_sha256: sha(token), accounts, operator: true, kind: 'forge' }]),
      FORGE_BACKEND: 'enabled', EXPERIMENTAL_RAW_BACKEND: 'disabled' },
  };
  let mf = new Miniflare(configuration);
  await mf.ready;
  t.after(async () => { await mf.dispose(); await rm(persist, { recursive: true, force: true }); });
  for (const account of accounts) {
    const response = await mf.dispatchFetch('https://storage.invalid/api/v1/forge/call', {
      method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify({ version: 1, operation: 1, account_id: account, view: '', request_id: 'storage-provision', payload_json: '{}' }),
    });
    assert.equal(response.status, 200, await response.text());
  }
  const rawControl = async (account, action, payload) => {
    const response = await mf.dispatchFetch(`https://storage.invalid/__fixture/${action}?account=${account}`, { method: 'POST', body: JSON.stringify(payload) });
    return { status: response.status, value: await response.json() };
  };
  const control = async (account, action, payload, expected = 200) => {
    const result = await rawControl(account, action, payload);
    assert.equal(result.status, expected, JSON.stringify(result.value)); return result.value;
  };
  const call = async (account, operation, payload = {}, { id = '', view = 'public', auth = token } = {}) => {
    const response = await mf.dispatchFetch('https://storage.invalid/api/v1/forge/call', {
      method: 'POST', headers: { 'content-type': 'application/json', ...(auth === null ? {} : { authorization: `Bearer ${auth}` }) },
      body: JSON.stringify({ version: 1, operation, account_id: account, view, request_id: id, payload_json: JSON.stringify(payload) }),
    });
    const envelope = await response.json();
    return { status: response.status, envelope, value: typeof envelope.result_json === 'string' ? JSON.parse(envelope.result_json) : undefined };
  };
  const success = async (...args) => { const result = await call(...args); assert.equal(result.status, 200, JSON.stringify(result)); return result; };
  return {
    control, rawControl, call, success,
    invoke: (account, payload) => control(account, 'storage', payload),
    bucket: () => mf.getR2Bucket('OBJECTS'),
    reload: async () => { await mf.dispose(); mf = new Miniflare(configuration); await mf.ready; },
  };
}
function accountingDelta(after, before) {
  return Object.fromEntries(Object.keys(before).map(key => [key, after[key] - before[key]]));
}
function physicalMetadata(account, object, attempt) {
  return { attempt, account, kind: String(object.kind), id: object.id, format: '1' };
}

test('production storage isolates delayed old DELETE and stale confirmation from a same-ID replacement with exact restart settlement', async t => {
  const account = 'storage-replacement';
  const h = await storageLifecycleRuntime(t, [account]);
  const object = { ...objects.get('original_file'), format_version: 1 };
  const result = await h.invoke(account, { action: 'replace-delayed-delete', object });
  assert.deepEqual(accountingDelta(result.normal.accounting, result.baseline.accounting), {
    logical_objects: 1, logical_bytes: object.size_bytes, reservations: 1,
    physical_payloads: 1, physical_bytes: object.size_bytes, attempt_rows: 1,
  });
  assert.deepEqual(result.confirmedReplay, result.normal, 'exact confirmation replay does not double-charge');
  assert.deepEqual(result.read, object.bytes);
  assert.deepEqual(result.reread, object.bytes, 'replacement remains readable after actual old DELETE');
  assert.deepEqual(result.reservedRetireError, { status: 409, code: 'aborted' }, 'live reservation fences current victim retirement');
  assert.deepEqual(result.staleError, { status: 409, code: 'aborted' }, 'old process-local token cannot resurrect retired incarnation');
  assert.deepEqual(result.deleteError, { status: 503, code: 'unavailable' });
  assert.notEqual(result.original.attempt_id, result.replacement.attempt_id);
  assert.ok(result.replacement.generation > result.original.generation);
  assert.equal(result.puts.length, 2);
  for (const [index, incarnation] of [result.original, result.replacement].entries()) {
    const expected = physicalMetadata(account, object, incarnation.attempt_id);
    assert.equal(result.puts[index].key, `native/v2/${account}/${object.kind}/${object.id}/${incarnation.attempt_id}`);
    assert.deepEqual(result.puts[index].metadata, expected);
  }
  assert.deepEqual(result.deletes, [result.puts[0].key]);
  assert.equal(result.heads[0], null, 'fault occurs after the real old payload was deleted');
  assert.deepEqual(result.heads[1], { size: object.size_bytes, metadata: result.puts[1].metadata });
  assert.deepEqual(accountingDelta(result.after.accounting, result.baseline.accounting), {
    logical_objects: 1, logical_bytes: object.size_bytes, reservations: 1,
    physical_payloads: 2, physical_bytes: 2 * object.size_bytes, attempt_rows: 2,
  }, 'unknown DELETE acknowledgement must retain charge until exact replay');
  assert.equal(result.after.attempts.find(a => a.attempt_id === result.original.attempt_id).state, 'retiring');
  await h.reload();
  const settled = await h.invoke(account, { action: 'delete', attempt_id: result.original.attempt_id });
  assert.deepEqual(accountingDelta(settled.accounting, result.baseline.accounting), {
    logical_objects: 1, logical_bytes: object.size_bytes, reservations: 1,
    physical_payloads: 1, physical_bytes: object.size_bytes, attempt_rows: 2,
  });
  assert.equal(settled.attempts.find(a => a.attempt_id === result.original.attempt_id).state, 'deleted');
  assert.deepEqual(settled.read, object.bytes, 'fresh storage instance reads the replacement after old delete recovery');
  const live = settled.attempts.find(a => a.attempt_id === result.replacement.attempt_id);
  assert.equal(live.current, true); assert.equal(live.state, 'written');
  assert.deepEqual(await h.invoke(account, { action: 'delete', attempt_id: result.original.attempt_id }), settled,
    'replayed settlement cannot release the replacement charge or decrement twice');
});

test('production storage retains uncertain PUT custody across restart and trusted bounded reservation release is exact-once', async t => {
  const accounts = ['storage-lost-put', 'storage-absent-put', 'storage-terminal-owner'];
  const h = await storageLifecycleRuntime(t, accounts);
  const object = { ...objects.get('original_file'), format_version: 1 };
  const written = await h.invoke(accounts[0], { action: 'put-ack-lost', object });
  assert.deepEqual(written.error, { status: 503, code: 'unavailable' });
  assert.equal(written.visible, null);
  const writtenAttempt = written.attempts.find(a => a.descriptor.id === object.id);
  assert.equal(writtenAttempt.state, 'pending'); assert.equal(writtenAttempt.current, false);
  assert.deepEqual(written.head, { size: object.size_bytes, metadata: physicalMetadata(accounts[0], object, writtenAttempt.attempt_id) });
  const absent = await h.invoke(accounts[1], { action: 'put-unobserved', object });
  assert.deepEqual(absent.error, { status: 503, code: 'unavailable' });
  assert.equal(absent.head, null); assert.equal(absent.visible, null);
  const absentAttempt = absent.attempts.find(a => a.descriptor.id === object.id);
  assert.equal(absentAttempt.state, 'pending');
  for (const result of [written, absent]) {
    assert.deepEqual(accountingDelta(result.accounting, result.baseline.accounting), {
      logical_objects: 1, logical_bytes: object.size_bytes, reservations: 1,
      physical_payloads: 1, physical_bytes: object.size_bytes, attempt_rows: 1,
    });
  }
  await h.reload();
  const recovered = await h.invoke(accounts[0], { action: 'recover', attempt_id: writtenAttempt.attempt_id });
  assert.deepEqual(recovered.recovered, { ...writtenAttempt, state: 'written' }, 'exact HEAD may establish terminal write, never logical publication');
  assert.deepEqual(recovered.accounting, written.accounting);
  assert.equal(recovered.visible, null); assert.equal(recovered.read, null);
  const unresolved = await h.invoke(accounts[1], { action: 'recover', attempt_id: absentAttempt.attempt_id });
  assert.deepEqual(unresolved.recovered, absentAttempt, 'HEAD absence cannot prove an outstanding PUT will never arrive');
  assert.deepEqual(unresolved.accounting, absent.accounting);
  assert.deepEqual(await h.invoke(accounts[1], { action: 'recover', attempt_id: absentAttempt.attempt_id }), unresolved);
  const descriptors = [...new Map([...fixture.objects, ...fixture.projection.objects].filter(o => o.kind === 1).map(o => [o.id, { kind: o.kind, id: o.id, size_bytes: o.size_bytes, format_version: 1 }])).values()].slice(0, 3);
  assert.equal(descriptors.length, 3, 'native fixture supplies three distinct FILE reservations');
  const released = await h.invoke(accounts[2], { action: 'release', objects: descriptors });
  assert.equal(released.reserved.accounting.reservations - released.baseline.accounting.reservations, 4);
  assert.deepEqual(released.releases, [{ released: 1, complete: false }, { released: 1, complete: false }, { released: 1, complete: true }, { released: 0, complete: true }]);
  assert.equal(released.after.accounting.reservations - released.baseline.accounting.reservations, 1, 'unrelated live owner reservation is not released');
  assert.equal(released.after.accounting.physical_payloads, released.baseline.accounting.physical_payloads, 'owner release is not physical payload settlement');
  assert.ok(released.retained.objects.some(d => d.id === descriptors[0].id));
  assert.ok(!released.retained.objects.some(d => descriptors.slice(1).some(o => o.id === d.id)));
});

function activeGcJob(snapshot) {
  const active = snapshot.gc_maintenance[0].active;
  return active ? snapshot.gc_jobs.find(job => job.id === active) : snapshot.gc_jobs.at(-1);
}
async function finishProductionGc(control, initial) {
  let snapshot = initial;
  const deadline = Date.now() + 60000;
  for (let quantum = 0; quantum < 512 && Date.now() < deadline; quantum++) {
    if (snapshot && snapshot.gc_maintenance[0].active === null && ['complete', 'failed'].includes(activeGcJob(snapshot)?.phase)) return snapshot;
    snapshot = await control({ action: 'tick' });
  }
  assert.fail(`production GC did not terminate within bounded alarm delivery: ${JSON.stringify(activeGcJob(snapshot))}`);
}
async function readyStorageView(h, account, view = 'public') {
  let reply = await h.success(account, 6, {}, { view });
  for (let round = 0; round < 512 && reply.envelope.state === 'pending'; round++) {
    reply = await h.success(account, 6, continuation(reply), { view });
  }
  assert.equal(reply.envelope.state, 'complete'); return reply.value;
}
async function gcViewFixture(h, account, historical = false) {
  const keep = fixture.checker;
  const seed = async (objects, root) => h.control(account, 'seed', {
    objects: objects.map(object => ({ ...object, format_version: 1 })), root,
  });
  const handoff = root => h.control(account, 'gc', { action: 'handoff-seed', root });
  let epoch = 1, old;
  if (historical) {
    const root = closureKey(objects.get('first_commit'));
    const names = ['original_file', 'updated_file', 'copy_seed', 'nested_copy', 'symlink', 'subtree', 'first_tree', 'first_commit'];
    await seed(names.map(name => objects.get(name)), root);
    const spec = { ...keep.spec, revision: Array.from(Buffer.from(root.id, 'hex')) };
    old = (await h.success(account, 4, { spec, expected_epoch: epoch++, make_default: true, follow_main: false }, { id: 'gc-historical-view' })).value;
    await readyStorageView(h, account);
    await handoff(root);
  }
  await seed(keep.objects, keep.root);
  const current = (await h.success(account, 4, { spec: keep.spec, expected_epoch: epoch++, make_default: true, follow_main: false }, { id: 'gc-public-view' })).value;
  await handoff(keep.root);
  const lockedSpec = { ...keep.spec, name: 'locked', visibility: { kind: 'members' } };
  const locked = (await h.success(account, 4, { spec: lockedSpec, expected_epoch: epoch++, make_default: false, follow_main: false }, { id: 'gc-locked-view', view: 'locked' })).value;
  if (old) await h.success(account, 11, { version: old.view_version, expected_epoch: epoch++ }, { id: 'gc-retire-history' });
  const publicSnapshot = await readyStorageView(h, account);
  const lockedSnapshot = await readyStorageView(h, account, 'locked');
  assert.deepEqual(publicSnapshot.view_version, current.view_version);
  assert.deepEqual(lockedSnapshot.view_version, locked.view_version);
  return { keep, publicSnapshot, lockedSnapshot, epoch };
}

test('production alarm GC restarts strict marking, preserves main/public/locked custody and pending PUTs, and physically deletes retired-view payloads', async t => {
  const account = 'gc-retired-source';
  const h = await storageLifecycleRuntime(t, [account]);
  const views = await gcViewFixture(h, account, true);
  const pending = await h.invoke(account, { action: 'put-unobserved', object: { ...objects.get('updated_file'), format_version: 1 } });
  const pendingAttempt = pending.attempts.find(attempt => attempt.state === 'pending');
  assert.ok(pendingAttempt);
  const control = (payload, expected) => h.control(account, 'gc', payload, expected);
  const before = await control({ action: 'inspect' });
  const victim = before.attempts.find(attempt => attempt.id === objects.get('original_file').id && attempt.state === 'written');
  assert.ok(victim, 'historical canonical/projection closure actually held original FILE bytes');
  const liveIds = new Set(views.keep.objects.map(object => object.id));
  for (const snapshot of [views.publicSnapshot, views.lockedSnapshot]) {
    liveIds.add(Buffer.from(snapshot.projected_commit).toString('hex'));
    liveIds.add(Buffer.from(snapshot.projected_tree).toString('hex'));
  }
  const main = (await h.success(account, 2)).value;
  liveIds.add(Buffer.from(main.main_revision).toString('hex'));
  const livePayloads = [];
  for (const attempt of before.attempts.filter(attempt => liveIds.has(attempt.id) && attempt.state === 'written' && attempt.current === 1)) {
    const object = await (await h.bucket()).get(attempt.locator);
    assert.ok(object); livePayloads.push({ key: attempt.locator, bytes: new Uint8Array(await object.arrayBuffer()) });
  }
  assert.ok(livePayloads.length > 8);
  assert.ok(await (await h.bucket()).get(victim.locator));
  await control({ action: 'request' });
  let progress;
  for (let count = 0; count < 128; count++) {
    progress = await control({ action: 'tick' });
    if (activeGcJob(progress).phase === 'mark' && progress.scratch.length) break;
  }
  assert.equal(activeGcJob(progress).phase, 'mark'); assert.ok(progress.scratch.length > 0);
  const frozen = activeGcJob(progress);
  const refused = await h.call(account, 4, { spec: views.keep.spec, expected_epoch: views.epoch,
    make_default: true, follow_main: false }, { id: 'cannot-author-through-maintenance' });
  assert.equal(refused.status, 503); assert.equal(refused.envelope.error.code, 'unavailable');
  assert.equal(refused.envelope.error.message, 'account_maintenance');
  const oldGeneration = frozen.generation - 1;
  assert.ok(oldGeneration >= 0);
  const stale = await control({ action: 'stale-generation', gc_id: frozen.id, generation: oldGeneration }, 409);
  assert.equal(stale.error, 'aborted');
  await h.reload();
  const reopened = await control({ action: 'inspect' });
  assert.equal(activeGcJob(reopened).id, frozen.id);
  assert.equal(activeGcJob(reopened).generation, activeGcJob(progress).generation);
  await control({ action: 'fail-delete-checkpoint' });
  let interrupted;
  const interruptionDeadline = Date.now() + 60000;
  for (let quantum = 0; quantum < 512 && Date.now() < interruptionDeadline; quantum++) {
    const tick = await h.rawControl(account, 'gc', { action: 'tick' });
    if (tick.status !== 200) {
      assert.equal(tick.status, 500, JSON.stringify(tick));
      assert.equal(tick.value.error, 'fixture_error');
      interrupted = await control({ action: 'inspect' }); break;
    }
  }
  assert.ok(interrupted, 'actual post-DELETE counter checkpoint must hit the injected SQLite interruption');
  const interruptedJob = activeGcJob(interrupted);
  assert.equal(interruptedJob.id, frozen.id); assert.equal(interruptedJob.phase, 'sweep');
  assert.ok(interruptedJob.pending_delete, 'exact deleted-attempt attribution must precede physical DELETE');
  const settledAttempt = interrupted.attempts.find(attempt => attempt.attempt_id === interruptedJob.pending_delete);
  assert.ok(settledAttempt, 'pending deletion names a genuine durable physical attempt');
  assert.equal(settledAttempt.state, 'deleted');
  assert.equal(await (await h.bucket()).get(settledAttempt.locator), null, 'actual R2 delete completed before counter ACK loss');
  assert.equal(interruptedJob.deleted, 0, 'fault rolls back only the first GC deletion checkpoint');
  assert.equal(interrupted.accounting.physical_payloads, before.accounting.physical_payloads - 1);
  assert.equal(interrupted.accounting.physical_bytes, before.accounting.physical_bytes - settledAttempt.size);
  await control({ action: 'clear-delete-checkpoint' });
  await h.reload();
  const recoveredCheckpoint = await control({ action: 'inspect' });
  assert.equal(activeGcJob(recoveredCheckpoint).pending_delete, interruptedJob.pending_delete);
  assert.deepEqual(recoveredCheckpoint.accounting, interrupted.accounting);
  const complete = await finishProductionGc(control, recoveredCheckpoint);
  const job = activeGcJob(complete);
  assert.equal(job.phase, 'complete', JSON.stringify(job)); assert.equal(job.error, null);
  const newlyDeleted = complete.attempts.filter(attempt => attempt.state === 'deleted' && before.attempts.some(previous => previous.attempt_id === attempt.attempt_id && previous.state !== 'deleted'));
  assert.ok(newlyDeleted.length > 0); assert.equal(job.deleted, newlyDeleted.length, 'recovery attributes each physical deletion exactly once including lost counter ACK');
  assert.equal(job.pending_delete, null); assert.ok(job.skipped_pending >= 1);
  assert.equal(complete.accounting.physical_payloads, before.accounting.physical_payloads - newlyDeleted.length);
  assert.equal(complete.accounting.physical_bytes, before.accounting.physical_bytes - newlyDeleted.reduce((sum, attempt) => sum + attempt.size, 0));
  const terminalReplay = await control({ action: 'tick' });
  assert.deepEqual(activeGcJob(terminalReplay), job); assert.deepEqual(terminalReplay.accounting, complete.accounting);
  assert.equal(complete.gc_maintenance[0].active, null);
  assert.equal(complete.scratch.length, 0); assert.equal(complete.scratch_rows, 0);
  assert.equal(complete.gc_roots.length, 0); assert.equal(complete.gc_marks.length, 0);
  assert.equal(await (await h.bucket()).get(victim.locator), null, 'actual R2 old source payload is physically absent');
  for (const live of livePayloads) {
    const object = await (await h.bucket()).get(live.key); assert.ok(object, `retained payload ${live.key}`);
    assert.deepEqual(new Uint8Array(await object.arrayBuffer()), live.bytes);
  }
  assert.equal(complete.attempts.find(attempt => attempt.attempt_id === pendingAttempt.attempt_id).state, 'pending');
  assert.ok(complete.accounting.physical_payloads >= livePayloads.length + 1, 'unobserved PUT still owns a physical charge');
  const staleBinding = await control({ action: 'stale-binding', binding: { account_id: account, gc_id: frozen.id, epoch: frozen.epoch } }, 409);
  assert.equal(staleBinding.error, 'aborted');
  assert.deepEqual((await h.success(account, 2)).value, main, 'GC does not publish a repository revision');
  const publicReadme = await h.success(account, 7, { version: views.publicSnapshot.view_version, path: 'README.md' }, { auth: null });
  assert.match(publicReadme.value.content, /Disposable checked source/);
  const lockedAnonymous = await h.call(account, 7, { version: views.lockedSnapshot.view_version, path: 'README.md' }, { view: 'locked', auth: null });
  assert.equal(lockedAnonymous.status, 403);
});

test('production alarm aborts missing strict roots before deletion, releases its barrier and permits genuine fresh-locator byte repair', async t => {
  const account = 'gc-missing-source';
  const h = await storageLifecycleRuntime(t, [account]);
  const views = await gcViewFixture(h, account);
  await h.invoke(account, { action: 'orphan', object: { ...objects.get('original_file'), format_version: 1 } });
  const control = (payload, expected) => h.control(account, 'gc', payload, expected);
  const before = await control({ action: 'inspect' });
  const readme = views.keep.objects.find(object => object.kind === 1 && Buffer.from(object.bytes).toString('utf8').startsWith('# Disposable checked source'));
  assert.ok(readme);
  const missing = before.attempts.find(attempt => attempt.kind === 1 && attempt.id === readme.id && attempt.state === 'written');
  assert.ok(missing);
  await (await h.bucket()).delete(missing.locator); // Exact test-only backing fault, not a caller-supplied GC victim.
  await control({ action: 'request' });
  const failed = await finishProductionGc(control);
  const job = activeGcJob(failed);
  assert.equal(job.phase, 'failed'); assert.ok(job.error);
  assert.equal(job.retired, 0); assert.equal(job.deleted, 0);
  assert.equal(failed.gc_maintenance[0].active, null);
  assert.equal(failed.scratch.length, 0); assert.equal(failed.scratch_rows, 0);
  assert.deepEqual(failed.accounting, before.accounting);
  assert.deepEqual(failed.attempts, before.attempts, 'failed marking changes no physical victim state');
  for (const attempt of before.attempts.filter(attempt => attempt.state === 'written' && attempt.attempt_id !== missing.attempt_id)) {
    assert.ok(await (await h.bucket()).get(attempt.locator), 'strict failure permits zero other physical deletions');
  }
  // Repair is the supported scoped upload protocol: real bytes, real validation,
  // fresh physical locator. Never PUT over the deleted etag/version-bound key.
  const repaired = await uploadObject((payload, id) => h.call(account, 10, payload, { id }), views.publicSnapshot.view_version,
    { ...readme, path: 'README.md', request_id: 'gc-supported-repair' });
  assert.equal(repaired.status, 200, JSON.stringify(repaired)); assert.equal(repaired.value.state, 'committed');
  const afterRepair = await control({ action: 'inspect' });
  const replacement = afterRepair.attempts.find(attempt => attempt.id === readme.id && attempt.attempt_id !== missing.attempt_id && attempt.state === 'written');
  assert.ok(replacement); assert.notEqual(replacement.locator, missing.locator);
  assert.equal(await (await h.bucket()).get(missing.locator), null);
  const read = await h.success(account, 7, { version: views.publicSnapshot.view_version, path: 'README.md' });
  assert.equal(read.value.content, Buffer.from(readme.bytes).toString('utf8'));
  await control({ action: 'request' });
  const retried = await finishProductionGc(control);
  assert.equal(activeGcJob(retried).phase, 'complete', JSON.stringify(activeGcJob(retried)));
  assert.ok(activeGcJob(retried).deleted >= 1);
});



test('archive schema inventory matches every application DDL and identifies only the exact platform metadata table', async t => {
  const runtime = await projectionRuntime(t);
  const report = await runtime.control('archive-schema');
  assert.ok(report.compiled_objects > 0 && report.actual_objects <= 512);
  assert.ok(report.ddl_bytes <= 4 * 1024 * 1024);
  assert.ok(Array.isArray(report.mismatches));
  assert.ok(report.mismatches.every(row => typeof row.name === 'string' && typeof row.reason === 'string'
    && (row.actual_hash === null || /^[0-9a-f]{64}$/.test(row.actual_hash))
    && (row.expected_hash === null || /^[0-9a-f]{64}$/.test(row.expected_hash))));
  assert.deepEqual(report.mismatches, [{
    type: 'table', name: '_cf_METADATA', table: '_cf_METADATA', reason: 'unknown_object',
    actual_hash: '4eb6b2780dfba9eda11272ea4816ae3e343a9c1290ee04c32c0029b70fb152c0', expected_hash: null,
  }], 'every compiled application DDL must match; only the pinned workerd alarm/bookmark table is outside it');
});
