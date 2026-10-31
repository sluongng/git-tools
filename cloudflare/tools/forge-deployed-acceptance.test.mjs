import test from 'node:test';
import assert from 'node:assert/strict';
import { validateOrigin, validateManifest, validateStep, validateForgeReply, requestForge, assertNoSecrets, goalGates, searchPublic, validateStepReceipt, verifyDenials } from './forge-deployed-acceptance.mjs';
const h = '12'.repeat(32);
function manifest() {
  return { schema_version: 1, origin: 'https://forge.example.com', account_id: 'example', view: 'public', view_version: h, revision: h,
    tt: { path: '/declared/tt', sha256: h }, output_dir: '/private/results',
    actors: Object.fromEntries(['owner', 'agent_a', 'agent_b'].map(name => [name, { root: `/private/${name}`, token_env: `ACCEPT_${name.toUpperCase()}` }])),
    public: { readme_path: 'README.md', readme_sha256: h, query: 'example', regex: false, limit: 10, expected_paths: ['README.md'] },
    denials: ['anonymous', 'invalid_token'].map(actor => ({ actor, view: 'locked', path: 'private/file', version: h })), steps: [] };
}
test('deployed manifest requires exact public HTTPS and rejects override execution', () => {
  assert.equal(validateManifest(manifest()).origin, 'https://forge.example.com');
  for (const origin of ['http://forge.example.com', 'https://localhost', 'https://127.0.0.1', 'https://forge.example.com/path', 'https://user@forge.example.com', 'https://forge.example.com:8443']) assert.throws(() => validateOrigin(origin));
  const step = { id: 'read', actor: 'agent_a', cwd: 'root', argv: ['view', 'show', '--version', h], expect: { kind: 'view', view_version: h, revision: h } };
  assert.equal(validateStep(step), step);
  for (const argv of [['--config', 'alias.x=evil', 'account', 'show'], ['account', 'bootstrap-import', '--source-config', '/private/source'], ['tinytree', 'resume', '/workspace'], ['sh', '-c', 'anything'], ['view', 'show', '--server', 'https://other.example.com'], ['request', 'resume', 'arbitrary']]) assert.throws(() => validateStep({ ...step, argv }));
  assert.throws(() => validateStep({ ...step, cwd: '../outside' }));
  assert.throws(() => validateManifest({ ...manifest(), evidence: { pass: true } }));
});
test('real HTTP boundary refuses redirect, rebound response and secret echo before receipt', async () => {
  const m = manifest();
  const call = { version: 1, operation: 6, account_id: m.account_id, view: m.view, request_id: '', payload_json: '{}' };
  const reply = { ...call, state: 'complete', result_json: '{}' }; delete reply.payload_json;
  assert.deepEqual(validateForgeReply(reply, call), { state: 'complete', value: {} });
  assert.throws(() => validateForgeReply({ ...reply, account_id: 'different' }, call));
  const send = value => async (origin, bytes) => { assert.equal(origin, m.origin); assert.equal(JSON.parse(bytes).operation, 6); return value; };
  await assert.rejects(requestForge(m, 6, {}, undefined, [], send({ status: 302, body: Buffer.from('{}') })), /redirect refused/);
  await assert.rejects(requestForge(m, 6, {}, undefined, [], send({ status: 200, body: Buffer.from(JSON.stringify({ ...reply, view: 'different' })) })), /binding mismatch/);
  const secret = 'synthetic-credential-not-a-real-token';
  await assert.rejects(requestForge(m, 6, {}, secret, [secret], send({ status: 403, body: Buffer.from(JSON.stringify({ error: secret })) })), /secret echo refused/);
  assert.throws(() => assertNoSecrets({ value: Buffer.from(secret).toString('base64') }, [secret]));
});
test('preflight and selected review assertions cannot satisfy full deployment goal', () => {
  const gates = goalGates(); assert.equal(gates.full_goal_complete, false);
  for (const [key, value] of Object.entries(gates)) if (key !== 'full_goal_complete') assert.equal(value, 'not_verified');
  const m = manifest(); m.steps = [{ id: 'review', actor: 'owner', cwd: 'root', argv: ['review', 'show', 'ab'.repeat(16)], expect: { kind: 'review', change_id: 'ab'.repeat(16), patchset_id: h, candidate_id: h, state: 'prepared' } }];
  assert.equal(validateManifest(m), m);
  const approve = { ...m.steps[0], argv: ['review', 'approve', 'ab'.repeat(16), '--patchset', h, '--candidate', h, '--request-id', 'approve-exact'], expect: { ...m.steps[0].expect, state: 'ready_to_land' } };
  assert.equal(validateStep(approve), approve);
  const reply = { operation: 'decide_review', state: 'ready_to_land', result: { account_id: m.account_id, view: m.view, request_id: 'approve-exact', value: { state: 'ready_to_land', change_id: approve.expect.change_id, patchset_id: h, source_base: h, source_head: h, view_version: h, candidate: { id: h, head: h, root_tree: h, target: h, target_generation: 2 }, check_profile: { name: 'checks', version: h }, approvals: 1 } } };
  const fields = validateStepReceipt(m, approve, reply);
  assert.equal(fields.request_id, 'approve-exact'); assert.equal(fields.candidate_id, h); assert.equal(fields.target_generation, 2); assert.deepEqual(fields.profile, { name: 'checks', version: h }); assert.equal(fields.check_evidence, 'server_review_state_only');
  assert.equal(goalGates().full_goal_complete, false);
});

test('public search continues exact pinned query and reports partial coverage honestly', async () => {
  const m = manifest(); const calls = [];
  const send = async (_origin, body) => {
    const call = JSON.parse(body), payload = JSON.parse(call.payload_json); calls.push(payload);
    assert.deepEqual({ ...payload, cursor: null }, { version: [...Buffer.from(h, 'hex')], query: m.public.query, regex: false, limit: 10, cursor: null });
    assert.equal(payload.cursor, calls.length === 1 ? null : 'page-two');
    const result = { account_id: m.account_id, view: m.view, view_version: h, revision: h, policy_epoch: 2,
      matches: [{ path: calls.length === 1 ? 'earlier.txt' : 'README.md', line: 1, column: 1, preview: 'example code' }], complete: calls.length === 2,
      cursor: calls.length === 1 ? 'page-two' : null, skipped_files: { binary: 0, oversized: 0 } };
    return { status: 200, body: Buffer.from(JSON.stringify({ version: 1, operation: 8, account_id: m.account_id, view: m.view, request_id: '', state: 'complete', result_json: JSON.stringify(result) })) };
  };
  assert.deepEqual(await searchPublic(m, [], 2, send), { search_complete: true, search_coverage: 'complete', search_pages: 2, search_terminal: true, skipped_files: { binary: 0, oversized: 0 } });
  const partial = async () => ({ status: 200, body: Buffer.from(JSON.stringify({ version: 1, operation: 8, account_id: m.account_id, view: m.view, request_id: '', state: 'complete', result_json: JSON.stringify({ account_id: m.account_id, view: m.view, view_version: h, revision: h, policy_epoch: 2, matches: [{ path: 'README.md', line: 1, column: 1, preview: 'example code' }], complete: false, cursor: 'remaining', skipped_files: { binary: 1, oversized: 0 } }) })) });
  assert.equal((await searchPublic(m, [], 2, partial)).search_coverage, 'expected_paths_only');
  const cumulativeCalls = [];
  const cumulative = async () => {
    cumulativeCalls.push(1); const response = await partial(); const frame = JSON.parse(response.body); const value = JSON.parse(frame.result_json);
    value.matches[0].path = cumulativeCalls.length === 1 ? 'earlier.txt' : 'README.md';
    value.cursor = cumulativeCalls.length === 1 ? 'next-page' : null;
    frame.result_json = JSON.stringify(value); return { status: 200, body: Buffer.from(JSON.stringify(frame)) };
  };
  const terminalPartial = await searchPublic(m, [], 2, cumulative);
  assert.equal(terminalPartial.search_pages, 2); assert.equal(terminalPartial.search_terminal, true);
  assert.equal(terminalPartial.search_complete, false); assert.equal(terminalPartial.search_coverage, 'expected_paths_only');
  assert.deepEqual(terminalPartial.skipped_files, { binary: 1, oversized: 0 }, 'cumulative 1 then 1 is not two skipped files');
  const falseComplete = async () => { const response = await partial(); const frame = JSON.parse(response.body); const value = JSON.parse(frame.result_json); value.complete = true; value.cursor = null; frame.result_json = JSON.stringify(value); return { status: 200, body: Buffer.from(JSON.stringify(frame)) }; };
  await assert.rejects(searchPublic(m, [], 2, falseComplete), /complete search cannot skip files/);
  cumulativeCalls.length = 0;
  const regressing = async () => { const response = await cumulative(); if (cumulativeCalls.length === 2) { const frame = JSON.parse(response.body); const value = JSON.parse(frame.result_json); value.skipped_files.binary = 0; frame.result_json = JSON.stringify(value); response.body = Buffer.from(JSON.stringify(frame)); } return response; };
  await assert.rejects(searchPublic(m, [], 2, regressing), /skip count regressed/);

  const invalidHit = async () => { const response = await partial(); const frame = JSON.parse(response.body); const value = JSON.parse(frame.result_json); value.matches = [{ path: 'README.md' }]; frame.result_json = JSON.stringify(value); return { status: 200, body: Buffer.from(JSON.stringify(frame)) }; };
  await assert.rejects(searchPublic(m, [], 2, invalidHit));
  const wrongLiteral = async () => { const response = await partial(); const frame = JSON.parse(response.body); const value = JSON.parse(frame.result_json); value.matches[0].preview = 'unrelated text'; frame.result_json = JSON.stringify(value); return { status: 200, body: Buffer.from(JSON.stringify(frame)) }; };
  await assert.rejects(searchPublic(m, [], 2, wrongLiteral), /literal search hit/);
  const absent = { ...m, public: { ...m.public, expected_paths: ['missing.txt'] } };
  calls.length = 0;
  await assert.rejects(searchPublic(absent, [], 2, send), /search_expected_paths_missing_at_terminal/);
});

test('denial requires a positive owner read of the same locked view and resource', async () => {
  const m = manifest(); m.denials = [m.denials[0]];
  const secrets = ['synthetic-owner-token', 'synthetic-agent-a-token', 'synthetic-agent-b-token'];
  const calls = [];
  const send = async (_origin, bytes, token) => {
    const call = JSON.parse(bytes); const payload = JSON.parse(call.payload_json); calls.push({ operation: call.operation, view: call.view, payload, token });
    assert.equal(call.view, 'locked', 'public route cannot stand in for the locked view');
    if (call.operation === 2 || token === secrets[0]) {
      assert.equal(token, secrets[0]);
      const value = call.operation === 2 ? { account_id: m.account_id, policy_epoch: 3 } : {
        account_id: m.account_id, view: 'locked', view_version: h, revision: h, policy_epoch: 3, path: 'private/file', content: 'owner-visible fixture body',
      };
      return { status: 200, body: Buffer.from(JSON.stringify({ version: 1, operation: call.operation, account_id: call.account_id, view: call.view, request_id: '', state: 'complete', result_json: JSON.stringify(value) })) };
    }
    assert.equal(token, undefined); assert.deepEqual(payload, { version: [...Buffer.from(h, 'hex')], path: 'private/file' });
    return { status: 403, body: Buffer.from(JSON.stringify({ version: 1, error: { code: 'permission_denied', message: 'locked' } })) };
  };
  const receipts = await verifyDenials(m, secrets, send);
  assert.deepEqual(calls.map(call => call.operation), [2, 7, 7]); assert.equal(calls[1].token, secrets[0]);
  assert.equal(receipts[0].view, 'locked'); assert.equal(receipts[0].view_version, h); assert.equal(receipts[0].owner_control_readable, true);
  assert.ok(!JSON.stringify(receipts).includes('owner-visible fixture body'));
  const wrongView = async (...args) => { const response = await send(...args); const call = JSON.parse(args[1]); if (call.operation === 7 && args[2] === secrets[0]) { const frame = JSON.parse(response.body); const value = JSON.parse(frame.result_json); value.view = 'public'; frame.result_json = JSON.stringify(value); response.body = Buffer.from(JSON.stringify(frame)); } return response; };
  await assert.rejects(verifyDenials(m, secrets, wrongView));
  const missing = async (...args) => { const call = JSON.parse(args[1]); if (call.operation === 7 && args[2] === secrets[0]) return { status: 403, body: Buffer.from(JSON.stringify({ version: 1, error: { code: 'permission_denied', message: 'unavailable' } })) }; return send(...args); };
  await assert.rejects(verifyDenials(m, secrets, missing), /owner control must read/);
  const incomplete = manifest(); delete incomplete.denials[0].view; assert.throws(() => validateManifest(incomplete));
});
