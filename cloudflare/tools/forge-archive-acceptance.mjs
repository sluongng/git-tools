// Manual same-build archive acceptance. Actual Worker/SQLite/R2 and shipped tt;
// the existing execution-only runner supplies genuine checks, never a verdict hook.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, readFile, writeFile, readdir, stat, lstat, rm, cp, copyFile } from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Miniflare } from 'miniflare';
import { runCheckerAcceptance } from './checker-acceptance.mjs';

const inputs = process.argv.slice(2);
assert.equal(inputs.length, 7, 'existing checker acceptance inputs plus shipped tt required');
const tt = path.resolve(inputs.pop());
const exportToken = randomBytes(32).toString('hex'), otherExportToken = randomBytes(32).toString('hex');
const restoreToken = randomBytes(32).toString('hex'), archiveId = randomBytes(32).toString('hex');
const sha = value => createHash('sha256').update(value).digest('hex');
const hex = value => Buffer.from(value).toString('hex');
const PAGE_LIMIT = 16 * 1024 * 1024 + 65536;
let context, localRoot, target, targetConfiguration, targetOrigin, sourceRelay, targetRelay, privateRelay;
let sourceClient, targetClient, workspace, descriptor, wipClient, wipWorkspace, wipDescriptor, firstWip, secondWip, thirdWip, resumedWip, fifthWip, resumeEvidence, pruneEvidence, markerRecovery, complete = false, cleaned = false;
const fault = { export: true, restore: true, retire: false };
const events = [], physicalEvidence = [];
// This disposable recovery operator both restores and exports the recovered account.
const grants = token => JSON.stringify([{ principal: 'archive-target', token_sha256: sha(token), accounts: ['forge-check'], export: true, restore: true }]);
const redact = value => [exportToken, otherExportToken, restoreToken, context?.token].filter(Boolean)
  .reduce((text, secret) => text.replaceAll(secret, '<redacted>'), String(value ?? '')).slice(0, 8192);

async function bytes(response, maximum = PAGE_LIMIT) {
  const parts = []; let size = 0;
  for await (const part of response.body) { size += part.length; assert.ok(size <= maximum); parts.push(part); }
  return Buffer.concat(parts);
}
async function relay(upstream, action, enabled) {
  const server = createServer(async (request, response) => {
    try {
      assert.equal(request.method, 'POST'); assert.equal(request.url, '/api/v1/archive/call');
      const parts = []; let size = 0;
      for await (const part of request) { size += part.length; assert.ok(size <= PAGE_LIMIT); parts.push(part); }
      const body = Buffer.concat(parts), call = JSON.parse(body);
      const result = await fetch(upstream() + request.url, { method: 'POST', signal: AbortSignal.timeout(60000),
        headers: { authorization: request.headers.authorization, 'content-type': 'application/json' }, body });
      const returned = await bytes(result), parsed = JSON.parse(returned);
      // Only a genuine accepted page ACK is lost. No content, authority or
      // manifest is manufactured or replaced by this test transport.
      const lost = enabled() && call.action === action && result.status === 200 && parsed.next_sequence > 0;
      events.push({ action: call.action, sequence: call.sequence ?? call.page?.sequence, status: result.status,
        archive_id: call.archive_id, next_sequence: parsed.next_sequence, lost_ack: lost });
      if (lost) { response.destroy(); return; }
      response.writeHead(result.status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); response.end(returned);
    } catch { response.destroy(); }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  return { server, origin: `http://127.0.0.1:${server.address().port}` };
}
async function privateTransport(upstream) {
  const server = createServer(async (request, response) => {
    try {
      assert.equal(request.method, 'POST');
      assert.ok(request.url.startsWith('/api/v1/forge/') || request.url === '/api/v1/private-capsule/call');
      const parts = []; let size = 0;
      for await (const part of request) { size += part.length; assert.ok(size <= PAGE_LIMIT); parts.push(part); }
      const body = Buffer.concat(parts), call = JSON.parse(body);
      const result = await fetch(upstream() + request.url, { method: 'POST', signal: AbortSignal.timeout(60000),
        headers: { authorization: request.headers.authorization, 'content-type': 'application/json' }, body });
      const returned = await bytes(result), value = JSON.parse(returned);
      if (result.status >= 400 || value.error) events.push({ action: 'observed_forge_failure', route: request.url,
        operation: call.operation ?? call.action, request_id: call.request_id ?? null, status: result.status,
        error: value.error ?? null });
      const lost = fault.retire && request.url === '/api/v1/private-capsule/call' && call.action === 'retire'
        && call.capsule_id === secondWip?.capsule_id && result.status === 200 && value.state === 'deleted';
      if (lost) {
        fault.retire = false;
        events.push({ action: 'private_retire', capsule_id: call.capsule_id, state: value.state, lost_ack: true });
        response.destroy(); return;
      }
      response.writeHead(result.status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); response.end(returned);
    } catch { response.destroy(); }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  return { server, origin: `http://127.0.0.1:${server.address().port}` };
}
async function capsuleCall(capsule, action, fields = {}) {
  const response = await fetch(targetOrigin + '/api/v1/private-capsule/call', { method: 'POST', signal: AbortSignal.timeout(30000),
    headers: { authorization: `Bearer ${context.token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ schema: 1, account_id: context.source.account_id, capsule_id: capsule, action, ...fields }) });
  const value = JSON.parse(await bytes(response)); assert.equal(response.status, 200, JSON.stringify(value)); return value;
}
const capsuleStatus = capsule => capsuleCall(capsule, 'status');
async function privateSpool(descriptorPath) {
  const directory = path.join(path.dirname(descriptorPath), 'private-backups');
  const names = (await readdir(directory)).filter(name => name.endsWith('.private')).sort();
  const result = [];
  for (const name of names) {
    const file = path.join(directory, name), info = await lstat(file);
    assert.ok(info.isFile() && !info.isSymbolicLink()); assert.equal(info.mode & 0o077, 0);
    result.push({ name, size: info.size, sha256: sha(await readFile(file)) });
  }
  return result;
}
async function archiveCall(origin, token, action, fields = {}, expected = 200, account = context.source.account_id) {
  const response = await fetch(origin + '/api/v1/archive/call', { method: 'POST', signal: AbortSignal.timeout(60000),
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ schema: 1, account_id: account, archive_id: archiveId, action, ...fields }) });
  const value = JSON.parse(await bytes(response));
  assert.equal(response.status, expected, JSON.stringify(value)); return value;
}
async function forgeCall(origin, operation, payload = {}, request = '', expected = 200) {
  const response = await fetch(origin + '/api/v1/forge/call', { method: 'POST', signal: AbortSignal.timeout(30000),
    headers: { authorization: `Bearer ${context.token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ version: 1, operation, account_id: context.source.account_id, view: 'public', request_id: request, payload_json: JSON.stringify(payload) }) });
  const envelope = JSON.parse(await bytes(response, 8 * 1024 * 1024));
  assert.equal(response.status, expected, JSON.stringify(envelope));
  return expected === 200 ? { state: envelope.state, value: JSON.parse(envelope.result_json) } : envelope;
}
async function fenced(origin) {
  const response = await fetch(origin + '/api/v1/forge/call', { method: 'POST', signal: AbortSignal.timeout(30000),
    headers: { authorization: `Bearer ${context.token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ version: 1, operation: 2, account_id: context.source.account_id, view: 'public', request_id: '', payload_json: '{}' }) });
  assert.equal(response.status, 503, 'incomplete restore/export must fence a normally authorized request');
  const value = JSON.parse(await bytes(response)); assert.equal(value.version, 1); assert.ok(value.error);
}
async function client(name, server, archiveToken) {
  const home = path.join(localRoot, name), cwd = path.join(home, 'cwd'), tmp = path.join(home, 'tmp');
  await mkdir(home, { mode: 0o700 }); await mkdir(cwd, { mode: 0o700 }); await mkdir(tmp, { mode: 0o700 });
  const jj = path.join(home, 'jj.toml');
  await writeFile(jj, '[user]\nname="Archive Fixture"\nemail="fixture@example.invalid"\n[operation]\nhostname="test"\nusername="test"\n[ui]\npaginate="never"\n[snapshot]\nmax-new-file-size=2097152\n', { mode: 0o600 });
  const value = { home, cwd, env: { HOME: home, XDG_CONFIG_HOME: path.join(home, 'config'), TMPDIR: tmp, JJ_CONFIG: jj,
    PATH: '/usr/bin:/bin', TERM: 'dumb', TT_FORGE_TOKEN: context.token, TT_ARCHIVE_TOKEN: archiveToken } };
  await select(value, server); return value;
}
async function raw(client, args, { cwd = client.cwd, success = true } = {}) {
  try {
    const argv = args[0] === '__forge_client_stop' ? args : ['--no-pager', '--color=never', ...args];
    const result = await context.execute(tt, argv, { cwd, env: client.env,
      encoding: 'utf8', timeout: args[0] === 'backup' && ['prune', 'resume'].includes(args[1]) ? 330000 : 180000, maxBuffer: 2 * 1024 * 1024 });
    assert.ok(success, `unexpected CLI success: ${args[0]}`); return result.stdout.trim();
  } catch (error) {
    if (!success && typeof error.code === 'number') return error.stdout;
    throw new Error(JSON.stringify({ operation: args[0], code: error.code, signal: error.signal,
      stdout: redact(error.stdout), stderr: redact(error.stderr) }));
  }
}
async function command(client, args, options = {}) {
  const report = JSON.parse(await raw(client, [...args, '--json'], options)); assert.equal(report.schema_version, 1);
  assert.equal(report.state === 'failed', options.success === false); return report;
}
async function select(client, origin) {
  await command(client, ['context', 'set', '--transport', 'forge-https-v1', '--server', origin,
    '--account', context.source.account_id, '--view', 'public', '--token-env', 'TT_FORGE_TOKEN']);
}
async function privateJson(file, maximum = PAGE_LIMIT) {
  const info = await lstat(file); assert.ok(info.isFile() && !info.isSymbolicLink());
  assert.equal(info.mode & 0o077, 0, 'archive source/metadata must be owner-private'); assert.ok(info.size <= maximum);
  return JSON.parse(await readFile(file, 'utf8'));
}
const pageFile = (directory, sequence) => path.join(directory, `page-${String(sequence).padStart(8, '0')}.json`);
async function mounted(root) {
  return (await readFile('/proc/self/mountinfo', 'utf8')).split('\n').some(line => {
    const [fields, kind] = line.split(' - '); return fields.split(' ')[4] === root && kind?.startsWith('fuse');
  });
}
async function verifyPhysical(claim, expectedCandidate) {
  const scratch = path.join(context.root, 'checks');
  let journal;
  for (const name of (await readdir(scratch + '-remote')).filter(name => /^remote-[0-9a-f]{64}\.json$/.test(name))) {
    const saved = await privateJson(path.join(scratch + '-remote', name), 128 * 1024);
    if (saved.binding.claim === claim) journal = saved;
  }
  assert.ok(journal); const receipt = journal.receipt;
  assert.equal(receipt.cleanup_confirmed, true);
  assert.equal(receipt.request.job_id, expectedCandidate.id);
  assert.equal(receipt.request.candidate_commit, expectedCandidate.head);
  assert.equal(receipt.request.root_tree, expectedCandidate.root_tree);
  assert.equal(receipt.request.profile_version, context.profile.version);
  const attempt = path.join(scratch, receipt.workspace_id);
  const proof = await privateJson(path.join(attempt, 'snapshot-proof.json'));
  for (const key of ['before_verified', 'after_verified', 'readonly_superblock', 'capabilities_dropped', 'no_new_privs', 'teardown_confirmed', 'pid_namespace_contained']) assert.equal(proof[key], true);
  const bep = (await readFile(path.join(attempt, 'bep.jsonl'), 'utf8')).trim().split('\n').map(line => JSON.parse(line));
  assert.equal(bep.filter(event => event.started).length, 1);
  assert.ok(bep.some(event => event.id?.testSummary?.label === '//:smoke' && event.testSummary?.overallStatus === (receipt.outcome === 'passed' ? 'PASSED' : 'FAILED')));
  assert.ok((await stat(path.join(attempt, 'execution.compact'))).size > 0);
  if (context.artifacts) {
    assert.match(claim, /^archive-(?:cold-)?restored-[01]$/);
    for (const name of ['bep.jsonl', 'execution.compact', 'snapshot-proof.json']) {
      await copyFile(path.join(attempt, name), path.join(context.artifacts, `${claim}-${name}`));
    }
  }
  physicalEvidence.push({ claim, receipt, namespace_proof: proof });
  return receipt;
}

const workflow = {
  fixtureArgs: ['--client-workspace'],
  configureBindings() {
    return { ARCHIVERS_JSON: JSON.stringify([exportToken, otherExportToken].map(token => ({ principal: 'archive-source',
      token_sha256: sha(token), accounts: ['forge-check'], export: true, restore: false }))) };
  },
  async beforePublish({ source, invoke }) {
    for (const mutation of source.members) await invoke(3, mutation, `client-member-${mutation.member.principal}`);
  },
  async afterPrimary(value) {
    context = value; localRoot = await mkdtemp('/tmp/tfa.');
    if (process.env.TT_CHECKER_ACCEPT_ARTIFACTS_DIR) assert.equal(typeof context.artifacts, 'string', 'configured evidence directory must reach archive workflow');
    const directory = path.join(localRoot, 'archive');
    // Exercise the actual supported Legacy client, not an opaque byte fixture.
    const prior = (await context.invoke(2)).value;
    await context.invoke(4, { spec: { ...context.source.spec, revision: prior.main_revision, export_allowed: true },
      expected_epoch: prior.policy_epoch, make_default: true, follow_main: true }, 'archive-enable-private-capture');
    wipClient = await client('wip-source', context.origin, exportToken);
    wipWorkspace = path.join(wipClient.cwd, 'work');
    const enrolled = await command(wipClient, ['work', wipWorkspace]); wipDescriptor = enrolled.result.client_descriptor;
    assert.ok(await mounted(wipWorkspace));
    const noPending = await command(wipClient, ['backup', 'resume'], { cwd: wipWorkspace, success: false });
    assert.equal(noPending.operation, 'backup_resume'); assert.equal(noPending.state, 'failed');
    await writeFile(path.join(wipWorkspace, 'src/source.txt'), 'unpublished private source before rename\n');
    await raw(wipClient, ['move', 'src/source.txt', 'src/renamed.txt'], { cwd: wipWorkspace });
    await rm(path.join(wipWorkspace, 'readme-link'));
    await writeFile(path.join(wipWorkspace, 'private-wip-added.txt'), 'private addition never pushed before backup\n');
    // Do NOT run status/describe before backup: backup itself must snapshot this.
    await writeFile(path.join(wipWorkspace, 'dirty-before-backup.txt'), 'unsnapshotted private backup input\n');
    const backed = await command(wipClient, ['backup'], { cwd: wipWorkspace });
    assert.equal(backed.state, 'complete'); firstWip = backed.result;
    assert.match(firstWip.capsule_id, /^[0-9a-f]{64}$/); assert.equal(firstWip.generation, 1);
    assert.equal(await raw(wipClient, ['log', '--no-graph', '-r', '@', '-T', 'commit_id'], { cwd: wipWorkspace }), firstWip.snapshot_commit);
    assert.equal(await raw(wipClient, ['op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: wipWorkspace }), firstWip.snapshot_operation);
    if (context.artifacts) await writeFile(path.join(context.artifacts, 'native-wip-backup.json'), JSON.stringify(firstWip, null, 2), { mode: 0o600 });
    await raw(wipClient, ['tinytree', 'unmount', wipWorkspace]);
    await raw(wipClient, ['__forge_client_stop', wipDescriptor]); wipDescriptor = undefined;
    assert.equal(await mounted(wipWorkspace), false);
    await rm(wipClient.home, { recursive: true, force: true });
    // Source descriptor, private object pool, OP store, CAS and config are gone.
    const acknowledged = (await context.invoke(2)).value;
    const positive = context.evidence.find(item => item.label === 'positive'); assert.ok(positive);
    const selector = { change_id: context.source.reviews[0].change_id, patchset_id: context.source.reviews[0].patchset_id, candidate_id: positive.candidate.id };
    const replay = await context.invoke(24, selector, 'accept-positive-land');
    await archiveCall(context.origin, context.token, 'export_begin', {}, 401);
    await archiveCall(context.origin, exportToken, 'export_begin', {}, 403, 'foreign');
    sourceRelay = await relay(() => context.origin, 'export_next', () => fault.export);
    sourceClient = await client('source', sourceRelay.origin, exportToken);
    const firstBackup = await command(sourceClient, ['account', 'backup', directory, '--archive-token-env', 'TT_ARCHIVE_TOKEN', '--archive-id', archiveId], { success: false });
    const lostExport = events.find(event => event.action === 'export_next' && event.lost_ack);
    assert.ok(lostExport, redact(JSON.stringify({ first_backup: firstBackup, transport_events: events })));
    assert.equal(lostExport.archive_id, archiveId);
    await fenced(context.origin);
    await archiveCall(context.origin, otherExportToken, 'status', {}, 403);
    await context.restartWorker(); await fenced(context.origin);
    fault.export = false;
    const backup = await command(sourceClient, ['account', 'backup', directory, '--archive-token-env', 'TT_ARCHIVE_TOKEN']);
    assert.equal(backup.state, 'complete'); assert.equal(backup.result.archive_id, archiveId);
    const manifest = await privateJson(path.join(directory, 'manifest.json'));
    if (context.artifacts) await writeFile(path.join(context.artifacts, 'account-manifest.json'), JSON.stringify(manifest, null, 2), { mode: 0o600 });
    assert.equal(manifest.account_id, context.source.account_id); assert.equal(manifest.archive_id, archiveId);
    assert.equal(manifest.page_count, backup.result.page_count); assert.equal(manifest.chain_root, backup.result.chain_root);
    assert.ok(manifest.page_count > 1 && manifest.payloads > 0);
    assert.ok(manifest.tables.every(table => !/^(archive_|sqlite_|__cf_|_cf_)/.test(table.name)));
    for (const name of ['review_landings', 'review_decisions', 'check_leases', 'forge_storage_attempts']) assert.ok(manifest.tables.find(table => table.name === name)?.rows > 0);
    for (const table of ['review_landings', 'review_decisions', 'check_leases', 'forge_view_versions', 'closure_jobs', 'forge_storage_attempts', 'gc_jobs']) {
      assert.ok(manifest.tables.some(item => item.name === table), `compiled metadata inventory must include ${table}`);
    }
    let boundary = false, privatePayload = false;
    for (let sequence = 0; sequence < manifest.page_count; sequence++) {
      const page = await privateJson(pageFile(directory, sequence)); assert.equal(page.sequence, sequence);
      const encoded = JSON.stringify(page);
      for (const secret of [exportToken, otherExportToken, restoreToken, context.token]) assert.ok(!encoded.includes(secret), 'archive must never contain bearer values');
      assert.equal(page.archive_id, archiveId); assert.equal(page.build_id, manifest.build_id);
      if (page.content.kind === 'private_payload' && page.content.presence === 'present' && page.content.bytes_hex?.length) privatePayload = true;
      if (page.content.kind === 'payload' && page.content.size === 1048576 && page.content.presence === 'present') {
        assert.equal(page.content.bytes_hex, Buffer.alloc(1048576, 'K').toString('hex')); boundary = true;
      }
    }
    assert.ok(boundary, 'archive must contain the real 1MiB source payload');
    assert.ok(privatePayload, 'whole-account archive must contain actual native WIP capsule bytes');
    assert.deepEqual((await context.invoke(2)).value, acknowledged);
    targetConfiguration = { ...context.configuration, port: 0, durableObjectsPersist: path.join(localRoot, 'target-do'),
      r2Persist: path.join(localRoot, 'target-r2'), bindings: { ...context.configuration.bindings, ARCHIVERS_JSON: grants(restoreToken) } };
    target = new Miniflare(targetConfiguration); targetOrigin = (await target.ready).origin;
    targetConfiguration.port = Number(new URL(targetOrigin).port);
    await archiveCall(targetOrigin, restoreToken, 'restore_begin', { manifest: { ...manifest, build_id: '0'.repeat(64) } }, 412);
    // A fresh SQL namespace is not a fresh restore target when an owned legacy
    // payload still exists. Use real canonical fixture bytes, not fabricated IDs.
    const legacy = context.source.objects.find(object => object.kind === 1);
    assert.ok(legacy);
    const bucket = await target.getR2Bucket('OBJECTS');
    const legacyKey = `native/v1/${context.source.account_id}/${legacy.kind}/${legacy.id}`;
    await bucket.put(legacyKey, Buffer.from(legacy.bytes));
    const rejectedLegacy = await archiveCall(targetOrigin, restoreToken, 'restore_begin', { manifest }, 409);
    assert.equal(rejectedLegacy.error.code, 'target_not_empty');
    const retainedLegacy = await bucket.get(legacyKey); assert.ok(retainedLegacy);
    assert.deepEqual(Buffer.from(await retainedLegacy.arrayBuffer()), Buffer.from(legacy.bytes));
    await bucket.delete(legacyKey); // Only the exact object created above.
    // If the refusal had installed a restore job, a rebound manifest would be
    // a job conflict instead of this original fresh-target build rejection.
    await archiveCall(targetOrigin, restoreToken, 'restore_begin', { manifest: { ...manifest, build_id: '0'.repeat(64) } }, 412);
    targetRelay = await relay(() => targetOrigin, 'restore_page', () => fault.restore);
    targetClient = await client('target', targetRelay.origin, restoreToken);
    await command(targetClient, ['account', 'restore', directory, '--archive-token-env', 'TT_ARCHIVE_TOKEN'], { success: false });
    assert.ok(events.some(event => event.action === 'restore_page' && event.lost_ack));
    await fenced(targetOrigin);
    const progress = await archiveCall(targetOrigin, restoreToken, 'status');
    assert.ok(progress.next_sequence > 0 && progress.next_sequence < manifest.page_count);
    const next = await privateJson(pageFile(directory, progress.next_sequence));
    await archiveCall(targetOrigin, restoreToken, 'restore_page', { page: { ...next, hash: next.hash[0] === '0' ? '1' + next.hash.slice(1) : '0' + next.hash.slice(1) } }, 400);
    const bad = path.join(localRoot, 'bad-archive'); await cp(directory, bad, { recursive: true });
    await rm(pageFile(bad, 1));
    await command(targetClient, ['account', 'restore', bad, '--archive-token-env', 'TT_ARCHIVE_TOKEN'], { success: false });
    assert.equal((await archiveCall(targetOrigin, restoreToken, 'status')).next_sequence, progress.next_sequence);
    await target.dispose(); target = new Miniflare(targetConfiguration); assert.equal((await target.ready).origin, targetOrigin);
    await fenced(targetOrigin); fault.restore = false;
    const restored = await command(targetClient, ['account', 'restore', directory, '--archive-token-env', 'TT_ARCHIVE_TOKEN']);
    assert.equal(restored.state, 'complete'); assert.equal(restored.result.archive_id, archiveId);
    assert.equal(restored.result.chain_root, manifest.chain_root);
    assert.deepEqual((await forgeCall(targetOrigin, 2)).value, acknowledged);
    assert.deepEqual(await forgeCall(targetOrigin, 24, selector, 'accept-positive-land'), replay);
    // A fresh, independently provisioned account must never be overwritten.
    const occupied = new Miniflare({ ...targetConfiguration, port: 0, durableObjectsPersist: path.join(localRoot, 'occupied-do'), r2Persist: path.join(localRoot, 'occupied-r2') });
    try {
      const origin = (await occupied.ready).origin;
      const original = await forgeCall(origin, 1, {}, 'occupied-provision');
      const refused = await archiveCall(origin, restoreToken, 'restore_begin', { manifest }, 409);
      assert.equal(refused.error.code, 'target_not_empty');
      assert.deepEqual((await forgeCall(origin, 2)).value.main_revision, original.value.main_revision);
    } finally { await occupied.dispose(); }
    privateRelay = await privateTransport(() => targetOrigin);
    await select(targetClient, privateRelay.origin);
    workspace = path.join(targetClient.cwd, 'work');
    const work = await command(targetClient, ['work', workspace, '--capsule', firstWip.capsule_id]); descriptor = work.result.client_descriptor;
    assert.equal(work.state, 'ready'); assert.deepEqual(work.result.receipt, firstWip);
    assert.equal(await raw(targetClient, ['log', '--no-graph', '-r', '@', '-T', 'commit_id'], { cwd: workspace }), firstWip.snapshot_commit);
    assert.equal(await raw(targetClient, ['op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: workspace }), firstWip.snapshot_operation);
    assert.equal(await readFile(path.join(workspace, 'src/renamed.txt'), 'utf8'), 'unpublished private source before rename\n');
    await assert.rejects(lstat(path.join(workspace, 'src/source.txt')), error => error.code === 'ENOENT');
    await assert.rejects(lstat(path.join(workspace, 'readme-link')), error => error.code === 'ENOENT');
    assert.equal(await readFile(path.join(workspace, 'private-wip-added.txt'), 'utf8'), 'private addition never pushed before backup\n');
    assert.equal(await readFile(path.join(workspace, 'dirty-before-backup.txt'), 'utf8'), 'unsnapshotted private backup input\n');
    assert.ok(await mounted(workspace)); assert.equal(typeof descriptor, 'string');
    assert.deepEqual(await readFile(path.join(workspace, 'boundary.bin')), Buffer.alloc(1048576, 'K'));
    await writeFile(path.join(workspace, 'restored-receipt.txt'), 'genuine checked work after full account restore\n');
    await raw(targetClient, ['describe', '-m', 'Checked work after restored account'], { cwd: workspace });
    let pushed = await command(targetClient, ['push', '--request-id', 'restored-source'], { cwd: workspace });
    for (let round = 0; round < 128 && (pushed.state !== 'checks_pending' || !pushed.result.value.projected_head); round++) pushed = await command(targetClient, ['request', 'resume', 'restored-source'], { cwd: workspace });
    assert.equal(pushed.state, 'checks_pending'); const review = pushed.result.value;
    context.retargetChecker(targetOrigin);
    let passedReceipt;
    for (let round = 0; round < 2 && !passedReceipt; round++) {
      const claim = `archive-restored-${round}`;
      const report = JSON.parse((await context.run(['run-remote', '--account', context.source.account_id, '--profile', 'checks', '--claim-id', claim])).stdout);
      assert.equal(report.result.state, 'completed');
      const intended = report.result.completion.lease.request.job_id === review.candidate.id;
      // The archived cleanup-confirmed cancellation can be selected first. It
      // must genuinely fail its status=1 test, not be erased or fabricated away.
      const candidate = intended ? review.candidate : context.cancellationCandidate;
      const receipt = await verifyPhysical(claim, candidate);
      assert.equal(receipt.outcome, intended ? 'passed' : 'failed');
      if (intended) passedReceipt = receipt;
    }
    assert.ok(passedReceipt, 'the intended restored candidate requires an actual Passed check');
    const exact = { change_id: review.change_id, patchset_id: review.patchset_id, candidate_id: review.candidate.id };
    await forgeCall(targetOrigin, 23, { ...exact, decision: 'approve' }, 'restored-approve');
    const landed = await forgeCall(targetOrigin, 24, exact, 'restored-land'); assert.equal(landed.state, 'landed');
    assert.deepEqual(await forgeCall(targetOrigin, 24, exact, 'restored-land'), landed);
    assert.equal((await forgeCall(targetOrigin, 2)).value.main_generation, acknowledged.main_generation + 1);
    const reconciled = await command(targetClient, ['request', 'reconcile', 'restored-source'], { cwd: workspace });
    assert.equal(reconciled.result.value.local_reconciliation, 'applied');
    // Capture again after a real Land/reconcile: the mapped review base differs
    // from the original enrolled snapshot and must survive another fresh client.
    await writeFile(path.join(workspace, 'after-land-private.txt'), 'post-land unpublished backup input\n');
    const nextBackup = await command(targetClient, ['backup'], { cwd: workspace });
    assert.equal(nextBackup.state, 'complete'); secondWip = nextBackup.result;
    assert.equal(secondWip.workspace_id, firstWip.workspace_id); assert.equal(secondWip.generation, firstWip.generation + 1);
    await writeFile(path.join(workspace, 'before-local-prune.txt'), 'third acknowledged capture for real local retention cleanup\n');
    const thirdBackup = await command(targetClient, ['backup'], { cwd: workspace });
    assert.equal(thirdBackup.state, 'complete'); thirdWip = thirdBackup.result;
    assert.equal(thirdWip.generation, secondWip.generation + 1); assert.equal(thirdWip.workspace_id, firstWip.workspace_id);
    // A real independent whole-intent reservation fills the remaining byte
    // allowance. Native capture4 is preserved locally when its Begin is denied.
    const blocker = randomBytes(32).toString('hex'), blockerWorkspace = randomBytes(32).toString('hex');
    const remainingBytes = 512 * 1024 * 1024 - firstWip.bytes - secondWip.bytes - thirdWip.bytes;
    assert.ok(remainingBytes > 0);
    const currentCapsule = await capsuleStatus(thirdWip.capsule_id);
    await capsuleCall(blocker, 'begin', { workspace_id: blockerWorkspace, expected_generation: 0, selection: currentCapsule.selection,
      descriptor: { format: 'tinytree-private-capture-v1', total_bytes: remainingBytes, chunk_count: Math.ceil(remainingBytes / 65536), chain_root: '9'.repeat(64) } });
    await writeFile(path.join(workspace, 'pending-prune-input.txt'), 'quota-denied native source must remain pending and untouched\n');
    const deniedBackup = await command(targetClient, ['backup'], { cwd: workspace, success: false });
    assert.equal(deniedBackup.state, 'failed');
    const operationBeforePrune = await raw(targetClient, ['--ignore-working-copy', 'op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: workspace });
    const commitBeforePrune = await raw(targetClient, ['--ignore-working-copy', 'log', '--no-graph', '-r', '@', '-T', 'commit_id'], { cwd: workspace });
    const beforeSpool = await privateSpool(descriptor); assert.ok(beforeSpool.length >= 3);
    await writeFile(path.join(workspace, 'dirty-prune-do-not-snapshot.txt'), 'prune must leave unsnapshotted dirty bytes untouched\n');
    fault.retire = true;
    const firstPrune = await command(targetClient, ['backup', 'prune', '--superseded'], { cwd: workspace });
    assert.ok(['pending', 'complete'].includes(firstPrune.state)); assert.equal(firstPrune.operation, 'backup_prune');
    assert.ok(events.some(event => event.action === 'private_retire' && event.capsule_id === secondWip.capsule_id && event.lost_ack), 'actual predecessor Deleted ACK must be lost');
    const pruned = await command(targetClient, ['backup', 'prune', '--superseded'], { cwd: workspace });
    assert.equal(pruned.state, 'complete'); assert.equal(pruned.result.pending, 0); assert.equal(pruned.result.current_capsule, thirdWip.capsule_id);
    const afterSpool = await privateSpool(descriptor), keptNames = new Set(afterSpool.map(item => item.name));
    const removed = beforeSpool.filter(item => !keptNames.has(item.name));
    assert.ok(removed.some(item => item.size === secondWip.bytes), 'genuine locally captured predecessor spool must be reclaimed');
    assert.ok(afterSpool.some(item => item.size === thirdWip.bytes), 'latest acknowledged source remains local');
    assert.equal((await capsuleStatus(firstWip.capsule_id)).state, 'deleted', 'remote-only restored predecessor retires without source-machine ticket');
    assert.equal((await capsuleStatus(secondWip.capsule_id)).state, 'deleted');
    assert.equal((await capsuleStatus(thirdWip.capsule_id)).state, 'sealed');
    for (const old of beforeSpool.filter(item => !removed.some(gone => gone.name === item.name)))
      assert.deepEqual(afterSpool.find(item => item.name === old.name), old, 'prune preserves every nonretired file including the quota-denied pending source');
    assert.equal(await readFile(path.join(workspace, 'pending-prune-input.txt'), 'utf8'), 'quota-denied native source must remain pending and untouched\n');
    assert.equal((await capsuleCall(blocker, 'retire')).state, 'deleted', 'release only this fixture-owned unused reservation');
    assert.equal(await raw(targetClient, ['--ignore-working-copy', 'op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: workspace }), operationBeforePrune, 'prune does not snapshot dirty working copy');
    assert.equal(await readFile(path.join(workspace, 'dirty-prune-do-not-snapshot.txt'), 'utf8'), 'prune must leave unsnapshotted dirty bytes untouched\n');
    const noWork = await command(targetClient, ['backup', 'prune', '--superseded'], { cwd: workspace });
    assert.equal(noWork.state, 'complete'); assert.equal(noWork.result.retired, 0); assert.equal(noWork.result.local_files, 0); assert.equal(noWork.result.pending, 0);
    pruneEvidence = { first_result: firstPrune.result, resumed_result: pruned.result, noop_result: noWork.result, before_spool: beforeSpool, after_spool: afterSpool,
      old_retirement_ack_lost: true, real_local_predecessor_reclaimed: true, restored_predecessor_no_ticket_transplant: true, current_head_preserved: true, dirty_wip_not_snapshotted: true, quota_denied_pending_source_preserved: true };
    // Restart the actual client with the saved pending4 and later dirty bytes.
    // Its original restored capsule1 is now retired: resume must use CURRENT
    // local state and saved pending custody, never replay that old capsule.
    await raw(targetClient, ['tinytree', 'unmount', workspace]);
    await raw(targetClient, ['__forge_client_stop', descriptor]);
    await raw(targetClient, ['tinytree', 'resume', workspace]);
    assert.ok(await mounted(workspace));
    assert.equal(await readFile(path.join(workspace, 'dirty-prune-do-not-snapshot.txt'), 'utf8'), 'prune must leave unsnapshotted dirty bytes untouched\n');
    const resumed = await command(targetClient, ['backup', 'resume'], { cwd: workspace });
    assert.equal(resumed.operation, 'backup_resume'); assert.equal(resumed.state, 'complete');
    assert.equal(resumed.result.replayed, false); assert.equal(resumed.result.newer_working_copy_captured, false);
    resumedWip = resumed.result.receipt;
    assert.equal(resumedWip.generation, thirdWip.generation + 1); assert.equal(resumedWip.workspace_id, thirdWip.workspace_id);
    assert.equal(resumedWip.snapshot_operation, operationBeforePrune); assert.equal(resumedWip.snapshot_commit, commitBeforePrune);
    assert.equal(await raw(targetClient, ['--ignore-working-copy', 'op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: workspace }), operationBeforePrune, 'saved resume never snapshots newer dirty work');
    assert.equal(await readFile(path.join(workspace, 'dirty-prune-do-not-snapshot.txt'), 'utf8'), 'prune must leave unsnapshotted dirty bytes untouched\n');
    const resumedAgain = await command(targetClient, ['backup', 'resume'], { cwd: workspace });
    assert.equal(resumedAgain.state, 'complete'); assert.equal(resumedAgain.result.replayed, true);
    assert.equal(resumedAgain.result.newer_working_copy_captured, false); assert.deepEqual(resumedAgain.result.receipt, resumedWip);
    const fifth = await command(targetClient, ['backup'], { cwd: workspace });
    assert.equal(fifth.state, 'complete'); fifthWip = fifth.result;
    assert.equal(fifthWip.generation, resumedWip.generation + 1); assert.equal(fifthWip.workspace_id, firstWip.workspace_id);
    assert.notEqual(fifthWip.snapshot_commit, resumedWip.snapshot_commit, 'normal new backup includes later dirty input');
    const noResumeFallback = await command(targetClient, ['backup', 'resume'], { cwd: workspace, success: false });
    assert.equal(noResumeFallback.operation, 'backup_resume'); assert.equal(noResumeFallback.state, 'failed', 'ordinary latest backup is not a saved-resume replay fallback');
    const afterResumePrune = await command(targetClient, ['backup', 'prune', '--superseded'], { cwd: workspace });
    assert.equal(afterResumePrune.state, 'complete'); assert.equal(afterResumePrune.result.current_capsule, fifthWip.capsule_id); assert.equal(afterResumePrune.result.pending, 0);
    assert.equal((await capsuleStatus(thirdWip.capsule_id)).state, 'deleted');
    assert.equal((await capsuleStatus(resumedWip.capsule_id)).state, 'deleted');
    assert.equal((await capsuleStatus(fifthWip.capsule_id)).state, 'sealed');
    resumeEvidence = { saved_receipt: resumedWip, replay_result: resumedAgain.result, next_receipt: fifthWip, after_resume_prune: afterResumePrune.result,
      saved_pending_survived_client_restart: true, original_restored_source_retired_before_restart: true, exact_original_pending_resumed: true, newer_dirty_wip_not_captured_by_resume: true,
      exact_resume_ack_replayed: true, ordinary_backup_invalidates_resume_fallback: true, subsequent_backup_captures_new_wip: true, subsequent_covered_prune: true };
    const sourceScope = (await command(targetClient, ['status'], { cwd: workspace })).result.workspace;
    await raw(targetClient, ['tinytree', 'unmount', workspace]);
    await raw(targetClient, ['__forge_client_stop', descriptor]); descriptor = undefined;
    // Re-export the recovered account AFTER real marker compaction. Restore
    // into a second independent SQL/R2 service before recovering capsule5.
    const markerDirectory = path.join(localRoot, 'settled-marker-account');
    const markerArchiveId = randomBytes(32).toString('hex');
    await select(targetClient, targetOrigin);
    const markerBackup = await command(targetClient, ['account', 'backup', markerDirectory,
      '--archive-token-env', 'TT_ARCHIVE_TOKEN', '--archive-id', markerArchiveId]);
    assert.equal(markerBackup.state, 'complete');
    const markerManifest = await privateJson(path.join(markerDirectory, 'manifest.json'));
    for (const table of ['private_marker_pages', 'private_marker_roots', 'private_marker_attempts'])
      assert.ok(markerManifest.tables.find(entry => entry.name === table)?.rows > 0, `archive includes actual ${table}`);
    let coldMarkers = 0;
    for (let sequence = 0; sequence < markerManifest.page_count; sequence++) {
      const page = await privateJson(pageFile(markerDirectory, sequence));
      if (page.content.kind === 'private_payload' && page.content.state === 'tombstoned') {
        assert.equal(page.content.presence, 'present'); assert.equal(page.content.bytes_hex, ''); coldMarkers++;
      }
    }
    assert.ok(coldMarkers > 0, 'second account archive contains permanent compacted marker proofs');
    if (context.artifacts) await writeFile(path.join(context.artifacts, 'marker-account-manifest.json'), JSON.stringify(markerManifest, null, 2), { mode: 0o600 });
    await target.dispose();
    targetConfiguration = { ...targetConfiguration, port: 0, durableObjectsPersist: path.join(localRoot, 'marker-target-do'),
      r2Persist: path.join(localRoot, 'marker-target-r2') };
    target = new Miniflare(targetConfiguration); targetOrigin = (await target.ready).origin;
    targetConfiguration.port = Number(new URL(targetOrigin).port);
    await select(targetClient, targetOrigin);
    const markerRestored = await command(targetClient, ['account', 'restore', markerDirectory, '--archive-token-env', 'TT_ARCHIVE_TOKEN']);
    assert.equal(markerRestored.state, 'complete'); assert.equal(markerRestored.result.chain_root, markerManifest.chain_root);
    for (const old of [firstWip, secondWip, thirdWip, resumedWip]) assert.equal((await capsuleStatus(old.capsule_id)).state, 'deleted');
    assert.equal((await capsuleStatus(fifthWip.capsule_id)).state, 'sealed');
    markerRecovery = { compacted_pages_archived: true, permanent_marker_proofs: coldMarkers,
      fresh_second_sqlite_r2_service: true, exact_archive_chain_restored: true,
      old_terminal_receipts_preserved: true, newest_native_capsule_preserved: true,
      archive_id: markerArchiveId, page_count: markerManifest.page_count, payloads: markerManifest.payloads };
    const finalClient = await client('post-land-target', privateRelay.origin, restoreToken);
    const finalWorkspace = path.join(finalClient.cwd, 'work');
    const final = await command(finalClient, ['work', finalWorkspace, '--capsule', fifthWip.capsule_id]);
    targetClient = finalClient; workspace = finalWorkspace; descriptor = final.result.client_descriptor;
    assert.equal(final.state, 'ready'); assert.deepEqual(final.result.receipt, fifthWip);
    assert.equal(final.result.base, sourceScope.base, 'post-Land mapped review base is not reset to enrollment');
    assert.equal(await raw(targetClient, ['log', '--no-graph', '-r', '@', '-T', 'commit_id'], { cwd: workspace }), fifthWip.snapshot_commit);
    assert.equal(await raw(targetClient, ['op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: workspace }), fifthWip.snapshot_operation);
    assert.equal(await readFile(path.join(workspace, 'after-land-private.txt'), 'utf8'), 'post-land unpublished backup input\n');
    assert.equal(await readFile(path.join(workspace, 'before-local-prune.txt'), 'utf8'), 'third acknowledged capture for real local retention cleanup\n');
    assert.equal(await readFile(path.join(workspace, 'dirty-prune-do-not-snapshot.txt'), 'utf8'), 'prune must leave unsnapshotted dirty bytes untouched\n');
    assert.equal(await readFile(path.join(workspace, 'pending-prune-input.txt'), 'utf8'), 'quota-denied native source must remain pending and untouched\n');
    // A fresh marker-catalog restore must remain writable through a genuine
    // exact-candidate check and checked landing, not just readable metadata.
    await writeFile(path.join(workspace, 'after-marker-restore-checked.txt'), 'checked work after compacted marker recovery\n');
    await raw(targetClient, ['describe', '-m', 'Checked work after marker catalog restore'], { cwd: workspace });
    let coldPush = await command(targetClient, ['push', '--request-id', 'marker-restored-source'], { cwd: workspace });
    for (let round = 0; round < 128 && (coldPush.state !== 'checks_pending' || !coldPush.result.value.projected_head); round++)
      coldPush = await command(targetClient, ['request', 'resume', 'marker-restored-source'], { cwd: workspace });
    assert.equal(coldPush.state, 'checks_pending'); const coldReview = coldPush.result.value;
    context.retargetChecker(targetOrigin);
    let coldReceipt;
    for (let round = 0; round < 2 && !coldReceipt; round++) {
      const claim = `archive-cold-restored-${round}`;
      const report = JSON.parse((await context.run(['run-remote', '--account', context.source.account_id, '--profile', 'checks', '--claim-id', claim])).stdout);
      assert.equal(report.result.state, 'completed');
      const intended = report.result.completion.lease.request.job_id === coldReview.candidate.id;
      const receipt = await verifyPhysical(claim, intended ? coldReview.candidate : context.cancellationCandidate);
      assert.equal(receipt.outcome, intended ? 'passed' : 'failed');
      if (intended) coldReceipt = receipt;
    }
    assert.ok(coldReceipt);
    const coldExact = { change_id: coldReview.change_id, patchset_id: coldReview.patchset_id, candidate_id: coldReview.candidate.id };
    await forgeCall(targetOrigin, 23, { ...coldExact, decision: 'approve' }, 'marker-restored-approve');
    const coldLand = await forgeCall(targetOrigin, 24, coldExact, 'marker-restored-land'); assert.equal(coldLand.state, 'landed');
    assert.deepEqual(await forgeCall(targetOrigin, 24, coldExact, 'marker-restored-land'), coldLand);
    const coldReconcile = await command(targetClient, ['request', 'reconcile', 'marker-restored-source'], { cwd: workspace });
    assert.equal(coldReconcile.result.value.local_reconciliation, 'applied');
    markerRecovery.subsequent_real_check_and_landing = true; markerRecovery.exact_candidate = coldReview.candidate.id;
    await writeFile(path.join(workspace, 'after-restore-private.txt'), 'new work must survive normal restored resume\n');
    await raw(targetClient, ['status'], { cwd: workspace });
    const currentOp = await raw(targetClient, ['op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: workspace });
    await raw(targetClient, ['tinytree', 'unmount', workspace]);
    await raw(targetClient, ['__forge_client_stop', descriptor]);
    await raw(targetClient, ['tinytree', 'resume', workspace]);
    assert.ok(await mounted(workspace));
    assert.equal(await raw(targetClient, ['op', 'log', '--no-graph', '-n', '1', '-T', 'id'], { cwd: workspace }), currentOp, 'normal restored resume does not replay original backup over subsequent WIP');
    assert.equal(await readFile(path.join(workspace, 'after-restore-private.txt'), 'utf8'), 'new work must survive normal restored resume\n');
    if (context.artifacts) await writeFile(path.join(context.artifacts, 'native-wip-recovery.json'), JSON.stringify({ first: firstWip, second: secondWip, third: thirdWip, resumed: resumedWip, fifth: fifthWip, resume: resumeEvidence, prune: pruneEvidence, marker_recovery: markerRecovery, resumed_operation: currentOp, full_account_archive_contains_private_bytes: privatePayload }, null, 2), { mode: 0o600 });
    await this.cleanup({ success: true }); complete = true;
    return { production_worker: true, actual_sqlite_r2: true, source_archive_id: archiveId, build_id: manifest.build_id,
      page_count: manifest.page_count, chain_root: manifest.chain_root, source_replay_preserved: true,
      export_page_ack_reconciled: true, restore_page_ack_reconciled: true, fresh_namespace_restore: true,
      native_private_wip: { genuine_dirty_snapshot: true, complete_native_capsule: true, source_client_state_removed: true, fresh_service_archive_restore: true, exact_operation_and_wc_restored: true, added_removed_renamed_bytes_restored: true, subsequent_real_check_land: true, post_land_review_base_and_generation_preserved: true, normal_resume_preserves_new_wip: true, first_capsule: firstWip, second_capsule: secondWip, third_capsule: thirdWip, resumed_capsule: resumedWip, fifth_capsule: fifthWip, explicit_prune: pruneEvidence, exact_saved_resume: resumeEvidence, settled_marker_recovery: markerRecovery },
      subsequent_actual_bazel_passed: true, exact_candidate: review.candidate.id, subsequent_checked_landing: true, physical_checks: physicalEvidence,
      tamper_and_missing_page_rejected: true, existing_target_preserved: true, legacy_orphan_target_rejected: true, incomplete_restore_fenced: true,
      owned_cleanup: true, deployed_recovery_proof: false };
  },
  async cleanup({ success = false } = {}) {
    if (cleaned) return;
    if (wipWorkspace && await mounted(wipWorkspace)) await raw(wipClient, ['tinytree', 'unmount', wipWorkspace]);
    if (wipDescriptor) { await raw(wipClient, ['__forge_client_stop', wipDescriptor]); wipDescriptor = undefined; }
    if (workspace && await mounted(workspace)) await raw(targetClient, ['tinytree', 'unmount', workspace]);
    if (descriptor) { await raw(targetClient, ['__forge_client_stop', descriptor]); descriptor = undefined; }
    if (workspace) assert.equal(await mounted(workspace), false);
    for (const proxy of [sourceRelay, targetRelay, privateRelay]) if (proxy) { proxy.server.closeAllConnections(); await new Promise(resolve => proxy.server.close(resolve)); }
    if (target) await target.dispose(); cleaned = true;
    // On failure retain the original archive and restore state for explicit
    // recovery; never infer that an incomplete import can be rolled back.
    if (!success && localRoot) {
      console.error(JSON.stringify({ retained_private_archive_fixture: localRoot }));
      console.error(redact(JSON.stringify({ observed_forge_failures: events.filter(event => event.action === 'observed_forge_failure') })));
    }
  },
};
await runCheckerAcceptance({ inputs: [...inputs, tt], archiveWorkflow: workflow });
if (complete && cleaned && localRoot) await rm(localRoot, { recursive: true, force: true });
