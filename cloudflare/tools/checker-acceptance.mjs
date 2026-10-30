// Manual Linux acceptance: production Worker + actual pinned checker/Bazel.
// No provider setup, repository origin, fixture verdict or production test hook.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdtemp, mkdir, readFile, writeFile, readdir, stat, lstat, open, rm, copyFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Miniflare } from 'miniflare';

const inputs = process.argv.slice(2);
assert.equal(inputs.length, 6, 'declared fixture, checker, readonly helper, Bazel, Worker and Wasm arguments required');
const [fixtureBinary, runner, helper, bazel, worker, wasm] = inputs.map(value => path.resolve(value));
const socket = process.env.TT_CHECKER_ACCEPT_RBE_SOCKET;
assert.ok(socket && path.isAbsolute(socket) && (await stat(socket)).isSocket(), 'explicit execution-only RBE socket required');
const instance = process.env.TT_CHECKER_ACCEPT_RBE_INSTANCE ?? '';
const flags = JSON.parse(process.env.TT_CHECKER_ACCEPT_PROFILE_FLAGS_JSON ?? '[]');
assert.ok(Array.isArray(flags) && flags.every(value => typeof value === 'string'));
const artifacts = process.env.TT_CHECKER_ACCEPT_ARTIFACTS_DIR;
if (artifacts) {
  assert.ok(path.isAbsolute(artifacts));
  await mkdir(artifacts, { recursive: true, mode: 0o700 });
  assert.equal((await stat(artifacts)).mode & 0o777, 0o700, 'acceptance artifacts must be private');
}
const temporary = await mkdtemp(path.join(tmpdir(), 'tinytree-checker-accept-'));
const root = path.join(temporary, 'private');
await mkdir(root, { mode: 0o700 });
const home = path.join(root, 'home'), scratch = path.join(root, 'checks'), emptyPath = path.join(root, 'no-programs');
await Promise.all([home, scratch, emptyPath].map(directory => mkdir(directory, { mode: 0o700 })));
const token = randomBytes(32).toString('hex'), checkerToken = randomBytes(32).toString('hex');
const sha = value => createHash('sha256').update(value).digest('hex');
const pin = async file => sha(await readFile(file));
const execute = promisify(execFile);
const environment = { HOME: home, XDG_CONFIG_HOME: path.join(home, 'config'), TMPDIR: root, PATH: emptyPath,
  TT_CHECKER_TOKEN: checkerToken, TERM: 'dumb' };
let mf, relay;
const relayEvents = [];
let lostAck;
const evidence = [];
let stage = 'initialization';
let acceptanceComplete = false;
try {
  const configuration = {
    host: '127.0.0.1', port: 0,
    modules: [{ type: 'ESModule', path: worker }, { type: 'CompiledWasm', path: wasm }],
    compatibilityDate: '2026-07-08',
    durableObjects: { REPOSITORIES: { className: 'AccountRepository', useSQLite: true } },
    r2Buckets: ['OBJECTS'], durableObjectsPersist: path.join(root, 'do'), r2Persist: path.join(root, 'r2'),
    bindings: { FORGE_BACKEND: 'enabled', EXPERIMENTAL_RAW_BACKEND: 'disabled',
      PRINCIPALS_JSON: JSON.stringify([{ principal: 'check-owner', token_sha256: sha(token), accounts: ['forge-check'], operator: true, kind: 'forge' }]) },
  };
  mf = new Miniflare(configuration);
  const origin = (await mf.ready).origin;
  assert.equal(new URL(origin).hostname, '127.0.0.1');
  configuration.port = Number(new URL(origin).port);
  // Test-only transport fault, not a Worker hook or a fabricated response:
  // forward the exact saved command to the production endpoint, then lose only
  // the first genuinely accepted passing Complete acknowledgement.
  relay = createServer(async (request, response) => {
    try {
      assert.equal(request.method, 'POST'); assert.equal(request.url, '/api/v1/checker/call');
      request.setTimeout(10000, () => request.destroy());
      const parts = []; let size = 0;
      for await (const part of request) { size += part.length; assert.ok(size <= 65536); parts.push(part); }
      request.setTimeout(0);
      const bytes = Buffer.concat(parts);
      const call = JSON.parse(bytes);
      const upstream = await fetch(origin + request.url, { method: 'POST', signal: AbortSignal.timeout(30000),
        headers: { 'content-type': request.headers['content-type'], authorization: request.headers.authorization }, body: bytes });
      assert.ok(Number(upstream.headers.get('content-length') ?? 0) <= 1024 * 1024);
      const chunks = []; let length = 0;
      for await (const chunk of upstream.body) { length += chunk.length; assert.ok(length <= 1024 * 1024); chunks.push(chunk); }
      const reply = Buffer.concat(chunks);
      const parsed = JSON.parse(reply);
      const event = { action: call.command.action, request_id: call.request_id, status: upstream.status,
        result_state: parsed.result?.state, lease_id: call.command.lease?.lease_id,
        attempt_id: parsed.result?.lease?.request?.attempt_id };
      relayEvents.push(event);
      if (!lostAck && call.command.action === 'complete' && call.command.receipt.outcome === 'passed'
          && upstream.status === 200 && parsed.result?.state === 'completed') {
        assert.equal(parsed.result.completion.result_request_id, call.request_id);
        lostAck = { completion: parsed.result.completion, event_index: relayEvents.length - 1 };
        response.destroy(); return;
      }
      response.writeHead(upstream.status, { 'content-type': upstream.headers.get('content-type'), 'cache-control': 'no-store' });
      response.end(reply);
    } catch {
      // Never turn a relay/validation failure into a service-issued receipt.
      response.destroy();
    }
  });
  await new Promise((resolve, reject) => { relay.once('error', reject); relay.listen(0, '127.0.0.1', resolve); });
  const checkerOrigin = `http://127.0.0.1:${relay.address().port}`;
  const config = path.join(root, 'runner.toml');
  const quote = JSON.stringify;
  const configText = `scratch = ${quote(scratch)}
rbe_socket = ${quote(socket)}
remote_instance_name = ${quote(instance)}
runner_sha256 = ${quote(await pin(runner))}
cleanup_timeout_seconds = 60
remote_timeout_seconds = 120
max_attempts = 8
workspace_environment = []
[bazel]
path = ${quote(bazel)}
sha256 = ${quote(await pin(bazel))}
[https_snapshot]
origin = ${quote(checkerOrigin)}
credential_environment = "TT_CHECKER_TOKEN"
[https_snapshot.readonly_helper]
path = ${quote(helper)}
sha256 = ${quote(await pin(helper))}
[https_snapshot.limits]
max_entries = 32768
max_files = 32768
max_file_bytes = 1048576
max_total_bytes = 536870912
max_metadata_bytes = 33554432
max_metadata_object_bytes = 262144
max_path_bytes = 4194304
max_depth = 128
[[profiles]]
name = "checks"
repositories = ["forge-forge-check"]
forge_accounts = false
store = "forge-forge-check"
mode = "test"
targets = ["//:smoke"]
flags = ${quote(flags)}
timeout_seconds = 600
force_remote_execution = true
`;
  await writeFile(config, configText, { mode: 0o600 });
  const run = async args => {
    try {
      return await execute(runner, ['--config', config, ...args], { env: environment, cwd: root, encoding: 'utf8', timeout: 780000, maxBuffer: 1024 * 1024 });
    } catch (error) {
      const bounded = value => String(value ?? '').replaceAll(token, '<redacted>').replaceAll(checkerToken, '<redacted>').slice(0, 4096);
      throw new Error(JSON.stringify({ stage, code: error.code, signal: error.signal, stdout: bounded(error.stdout), stderr: bounded(error.stderr) }));
    }
  };
  const profileVersion = (await run(['profile-version', '--profile', 'checks'])).stdout.trim();
  assert.match(profileVersion, /^[0-9a-f]{64}$/);
  const profile = { name: 'checks', version: profileVersion };
  configuration.bindings.CHECKERS_JSON = JSON.stringify([{ principal: 'execution-only', token_sha256: sha(checkerToken), accounts: ['forge-check'], profiles: [profile] }]);
  configuration.bindings.FORGE_REVIEW_CONFIG_JSON = JSON.stringify({ profile, committer: { name: 'Checker Acceptance', email: 'fixture@example.invalid' } });
  configuration.bindings.FORGE_CHECK_POLICY_JSON = JSON.stringify({ profile, targets: ['//:smoke'], test: true });
  await mf.dispose(); mf = new Miniflare(configuration); assert.equal((await mf.ready).origin, origin);
  const generated = await execute(fixtureBinary, ['--checker-profile-version', profileVersion], { env: { HOME: home, PATH: emptyPath, TMPDIR: root }, encoding: 'utf8', timeout: 30000, maxBuffer: 16 * 1024 * 1024 });
  const source = JSON.parse(generated.stdout).checker;
  assert.deepEqual(source.profile, profile);
  const invoke = async (operation, payload = {}, id = '', expected = 200) => {
    const response = await mf.dispatchFetch(origin + '/api/v1/forge/call', { signal: AbortSignal.timeout(30000), method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ version: 1, operation, account_id: source.account_id, view: 'public', request_id: id, payload_json: JSON.stringify(payload) }),
    });
    const envelope = await response.json(); assert.equal(response.status, expected, JSON.stringify(envelope));
    if (expected !== 200) return envelope;
    assert.equal(envelope.operation, operation); assert.equal(envelope.request_id, id); assert.equal(envelope.account_id, source.account_id); assert.equal(envelope.view, 'public');
    return { state: envelope.state, value: JSON.parse(envelope.result_json) };
  };
  stage = 'bootstrap';
  const account = (await invoke(1, {}, 'accept-provision')).value;
  const begin = (await invoke(40, { root: source.root,
    source: { repository_id: 'native-checker-fixture', reference: 'main', generation: 1, head: source.root.id },
    destination: { generation: 1, head: Buffer.from(account.main_revision).toString('hex') } }, 'accept-bootstrap')).value;
  for (const object of source.objects) await invoke(41, { import_id: begin.import_id,
    descriptor: { kind: object.kind, id: object.id, size_bytes: object.size_bytes, format_version: object.format_version },
    data_base64: Buffer.from(object.bytes).toString('base64') }, `io-${begin.import_id}-${object.kind}-${object.id}`);
  let imported = begin.status;
  for (let round = 0; round < 128 && !imported.complete; round++) {
    imported = (await invoke(42, { import_id: begin.import_id, expected_generation: imported.generation }, `ia-${begin.import_id}-${imported.generation}`)).value.status;
    assert.equal(imported.error, null);
  }
  assert.equal(imported.phase, 'ready');
  await invoke(44, { import_id: begin.import_id, expected_generation: imported.generation }, 'accept-bootstrap-finish');
  await invoke(4, { spec: source.spec, expected_epoch: 1, make_default: true, follow_main: false }, 'accept-view');
  let baseline = await invoke(6);
  for (let round = 0; round < 128 && baseline.state === 'pending'; round++) baseline = await invoke(6, { version: baseline.value.view_version, progress: baseline.value.progress });
  assert.equal(baseline.state, 'complete');
  assert.deepEqual(baseline.value.view_version, source.view_version);
  const before = (await invoke(2)).value;
  const deniedChecker = await mf.dispatchFetch(origin + '/api/v1/checker/call', { method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ schema: 1, account_id: source.account_id, request_id: 'owner-is-not-checker',
      command: { action: 'claim', profile_name: profile.name, profile_version: profile.version } }),
  });
  assert.equal(deniedChecker.status, 401, 'ordinary Forge owner cannot submit a checker verdict or claim');
  const privateRead = await invoke(9, { version: source.view_version, kind: source.private_file.kind,
    id: Array.from(Buffer.from(source.private_file.id, 'hex')), max_bytes: 0 }, '', 403);
  assert.equal(privateRead.error.code, 'permission_denied');
  const prepareReview = async selected => {
    stage = `${selected.label}-upload`;
    for (const object of selected.objects) {
      let session = (await invoke(10, { action: 'begin', version: source.view_version, path: object.path, kind: object.kind,
        id: Array.from(Buffer.from(object.id, 'hex')), size_bytes: object.size_bytes }, object.request_id)).value;
      for (let offset = session.offset; offset < object.bytes.length; offset += 65536) {
        session = (await invoke(10, { action: 'append', session_id: session.session_id, offset, data: object.bytes.slice(offset, offset + 65536) }, object.request_id)).value;
      }
      for (let round = 0; round < 128 && session.state !== 'committed'; round++) session = (await invoke(10, { action: 'finalize', session_id: session.session_id }, object.request_id)).value;
      assert.equal(session.state, 'committed'); assert.equal(session.receipt.size_bytes, object.size_bytes);
    }
    stage = `${selected.label}-prepare`;
    let review;
    const prepareDeadline = Date.now() + 120000;
    for (let round = 0; round < 256; round++) {
      assert.ok(Date.now() < prepareDeadline, 'candidate preparation exceeds acceptance budget');
      review = await invoke(20, selected.push, selected.request_id);
      if (review.state !== 'admission_pending') {
        assert.equal(review.value.patchset_id, selected.patchset_id);
        if (review.state === 'checks_pending' && review.value.projected_head) break;
        assert.ok(['rebase_pending', 'checks_pending'].includes(review.state));
      }
    }
    assert.equal(review.state, 'checks_pending'); assert.ok(review.value.projected_head);
    assert.equal(review.value.candidate.root_tree, selected.expected_canonical_tree, 'canonical rebase preserves excluded subtree and earlier positive change');
    return review;
  };
  for (const selected of source.reviews) {
    const review = await prepareReview(selected);
    const candidate = review.value.candidate;
    const selector = { change_id: selected.change_id, patchset_id: selected.patchset_id, candidate_id: candidate.id };
    const current = (await invoke(2)).value;
    stage = `${selected.label}-physical-check`;
    const claim = `accept-${selected.label}`;
    const report = JSON.parse((await run(['run-remote', '--account', source.account_id, '--profile', 'checks', '--claim-id', claim])).stdout);
    assert.equal(report.result.state, 'completed');
    assert.equal(report.result.completion.outcome, selected.label === 'positive' ? 'passed' : 'failed');
    if (selected.label === 'positive') {
      assert.ok(lostAck, 'real production Complete must commit before its acknowledgement is lost');
      assert.deepEqual(report.result.completion, lostAck.completion, 'GetResult reconciles the genuine accepted result');
      const afterLoss = relayEvents.slice(lostAck.event_index + 1);
      assert.ok(afterLoss.length > 0);
      assert.ok(afterLoss.every(event => event.action === 'get_result'), 'ACK loss must not claim, download or re-execute');
      assert.equal(relayEvents.filter(event => event.action === 'complete').length, 1);
      assert.equal(new Set(relayEvents.filter(event => event.result_state === 'leased').map(event => event.attempt_id)).size, 1);
    }
    const control = scratch + '-remote';
    const records = await readdir(control);
    let journal;
    for (const name of records.filter(name => name.startsWith('remote-') && name.endsWith('.json'))) {
      const record = JSON.parse(await readFile(path.join(control, name), 'utf8'));
      if (record.binding.claim === claim) journal = record;
    }
    assert.ok(journal); const receipt = journal.receipt;
    assert.equal(receipt.cleanup_confirmed, true); assert.equal(receipt.request.job_id, candidate.id);
    assert.equal(receipt.request.candidate_commit, candidate.head); assert.equal(receipt.request.root_tree, candidate.root_tree);
    assert.equal(receipt.request.profile_version, profileVersion); assert.deepEqual(receipt.evidence.requested_targets, ['//:smoke']);
    assert.match(receipt.evidence.invocation_id, /^[0-9a-f-]{36}$/);
    assert.equal(receipt.evidence.bazel_exit_code, selected.label === 'positive' ? 0 : 3);
    if (selected.label === 'positive') assert.equal(receipt.evidence.tests_verified, true);
    assert.match(receipt.evidence.bep_blake3, /^[0-9a-f]{64}$/);
    assert.match(receipt.evidence.execution_log_blake3, /^[0-9a-f]{64}$/);
    assert.match(receipt.workspace_id, /^[0-9a-f]{64}$/);
    const attempt = path.join(scratch, receipt.workspace_id);
    const proof = JSON.parse(await readFile(path.join(attempt, 'snapshot-proof.json'), 'utf8'));
    for (const field of ['before_verified', 'after_verified', 'readonly_superblock', 'capabilities_dropped', 'no_new_privs', 'teardown_confirmed', 'pid_namespace_contained']) assert.equal(proof[field], true, field);
    const bep = await readFile(path.join(attempt, 'bep.jsonl'), 'utf8');
    assert.ok(!bep.includes(token) && !bep.includes(checkerToken));
    const events = bep.trim().split('\n').map(line => JSON.parse(line));
    assert.equal(events.filter(event => event.started).length, 1, 'exactly one physical Bazel invocation per execution attempt');
    assert.ok(events.some(event => event.id?.testSummary?.label === '//:smoke' && event.testSummary?.overallStatus === (selected.label === 'positive' ? 'PASSED' : 'FAILED')));
    assert.ok((await stat(path.join(attempt, 'execution.compact'))).size > 0);
    const attemptNames = (await readdir(scratch)).filter(name => /^[0-9a-f]{64}$/.test(name)).sort();
    const requestCount = relayEvents.length;
    assert.deepEqual(JSON.parse((await run(['recover-remote', '--account', source.account_id, '--profile', 'checks', '--claim-id', claim])).stdout), report, 'terminal result recovery never re-executes or selects fresh work');
    assert.deepEqual((await readdir(scratch)).filter(name => /^[0-9a-f]{64}$/.test(name)).sort(), attemptNames);
    assert.equal(relayEvents.length, requestCount, 'terminal recovery does not dispatch fresh execution');
    assert.equal(await readFile(path.join(attempt, 'bep.jsonl'), 'utf8'), bep);
    const checked = await invoke(21, { change_id: selected.change_id });
    assert.equal(checked.state, selected.label === 'positive' ? 'prepared' : 'check_failed');
    await invoke(23, { ...selector, decision: 'approve' }, `approve-${selected.label}`);
    if (selected.label === 'positive') {
      stage = 'positive-land';
      const landed = await invoke(24, selector, 'accept-positive-land');
      assert.equal(landed.state, 'landed');
      assert.deepEqual(await invoke(24, selector, 'accept-positive-land'), landed);
      const after = (await invoke(2)).value;
      assert.equal(after.main_generation, current.main_generation + 1);
      assert.equal(Buffer.from(after.main_revision).toString('hex'), candidate.head);
    } else {
      assert.equal((await invoke(24, selector, 'accept-negative-land', 400)).error.code, 'failed_precondition');
      assert.deepEqual((await invoke(2)).value, current, 'real failed Bazel check cannot publish');
    }
    evidence.push({ label: selected.label, claim_id: claim, receipt, completion: report.result.completion,
      candidate, namespace_proof: proof, forced_remote_execution: true,
      lost_complete_ack_reconciled: selected.label === 'positive' && Boolean(lostAck) });
    if (artifacts) {
      for (const name of ['bep.jsonl', 'execution.compact', 'snapshot-proof.json']) await copyFile(path.join(attempt, name), path.join(artifacts, `${selected.label}-${name}`));
    }
  }
  // A Failed candidate is not eligible for another execution lease. Submit
  // the explicit native metadata-only amendment instead of silently reusing it.
  const amendment = source.cancellation;
  assert.equal(amendment.metadata_only, true);
  const cancellationReview = await prepareReview(amendment);
  const cancelledCandidate = cancellationReview.value.candidate;
  const mainBeforeCancellation = (await invoke(2)).value;
  const directoriesBefore = (await readdir(scratch)).filter(name => /^[0-9a-f]{64}$/.test(name)).sort();
  stage = 'cancellation-observe-bazel';
  const cancellationClaim = 'accept-cancellation';
  let controller;
  let ended = false;
  const completed = new Promise((resolve, reject) => {
    controller = execFile(runner, ['--config', config, 'run-remote', '--account', source.account_id,
      '--profile', 'checks', '--claim-id', cancellationClaim], {
      env: environment, cwd: root, encoding: 'utf8', timeout: 240000, maxBuffer: 1024 * 1024,
    }, (error, stdout, stderr) => {
      ended = true;
      if (error) reject(new Error(JSON.stringify({ stage, code: error.code, signal: error.signal,
        stderr: String(stderr).replaceAll(checkerToken, '<redacted>').slice(0, 4096) })));
      else { try { resolve(JSON.parse(stdout)); } catch (error) { reject(error); } }
    });
  });
  // Observe rejection promptly without leaving an unhandled promise while the
  // bounded file observation is in progress.
  completed.catch(() => {});
  const controlRoot = scratch + '-remote';
  const readCancellationJournal = async () => {
    for (const name of (await readdir(controlRoot)).filter(name => /^remote-[0-9a-f]{64}\.json$/.test(name))) {
      const journal = JSON.parse(await readFile(path.join(controlRoot, name), 'utf8'));
      if (journal.binding.claim === cancellationClaim) return journal;
    }
  };
  const procText = async file => {
    const descriptor = await open(file, 'r');
    try {
      const buffer = Buffer.alloc(65537); let length = 0;
      while (length < buffer.length) {
        const read = await descriptor.read(buffer, length, buffer.length - length, null);
        if (!read.bytesRead) break; length += read.bytesRead;
      }
      assert.ok(length <= 65536, 'owned process metadata exceeds acceptance bound');
      return buffer.subarray(0, length).toString('utf8');
    } finally { await descriptor.close(); }
  };
  const processIdentity = async pid => {
    assert.ok(Number.isSafeInteger(pid) && pid > 0 && pid <= 2147483647);
    const text = await procText(`/proc/${pid}/stat`);
    const end = text.lastIndexOf(')');
    assert.ok(text.startsWith(`${pid} (`) && end > 0);
    const fields = text.slice(end + 1).trim().split(/\s+/);
    assert.ok(fields.length >= 20 && /^[0-9]+$/.test(fields[19]));
    return { pid, state: fields[0], ppid: Number(fields[1]), pgrp: Number(fields[2]), starttime: fields[19] };
  };
  const liveProcess = identity => !['Z', 'X'].includes(identity.state);
  const sameProcess = (a, b) => a.pid === b.pid && a.starttime === b.starttime;
  const namespaceIdentity = async pid => {
    const status = await procText(`/proc/${pid}/status`);
    const line = status.split('\n').find(line => line.startsWith('NSpid:'));
    assert.ok(line, 'PID namespace identity unavailable; no weaker cancellation proof');
    const ids = line.slice(6).trim().split(/\s+/).map(Number);
    assert.ok(ids.length >= 2 && ids.every(id => Number.isSafeInteger(id) && id > 0));
    return ids;
  };
  const unfinishedBazel = async attempt => {
    const bep = await readFile(path.join(attempt, 'bep.jsonl'), 'utf8');
    let started, finished = false;
    for (const line of bep.split('\n').slice(0, -1)) {
      const event = JSON.parse(line); if (event.started) started = event.started;
      if (event.finished || event.id?.buildFinished) finished = true;
    }
    try { await lstat(path.join(attempt, 'snapshot-proof.json')); return undefined; }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    return !finished ? started : undefined;
  };
  const liveSnapshot = async (attempt, group, previous) => {
    const helper = await processIdentity(group);
    if (!liveProcess(helper) || helper.ppid !== controller.pid || helper.pgrp !== group) return undefined;
    if (previous && !sameProcess(helper, previous.helper)) return undefined;
    const children = (await procText(`/proc/${group}/task/${group}/children`)).trim().split(/\s+/).filter(Boolean).map(Number);
    for (const pid of children) {
      const child = await processIdentity(pid);
      if (!liveProcess(child) || child.ppid !== helper.pid || child.pgrp <= 0) continue;
      if (previous && !sameProcess(child, previous.child)) continue;
      const nspid = await namespaceIdentity(pid);
      if (nspid.at(-1) !== 1) continue;
      const started = await unfinishedBazel(attempt);
      if (!started) continue;
      // No exe/ns symlink reads or elevation: nondumpable helpers may forbid
      // those. Re-read exact live identities around the independent BEP check.
      const lastHelper = await processIdentity(helper.pid), lastChild = await processIdentity(child.pid);
      if (!sameProcess(helper, lastHelper) || !sameProcess(child, lastChild)
          || !liveProcess(lastHelper) || !liveProcess(lastChild)
          || lastHelper.ppid !== controller.pid || lastHelper.pgrp !== group || lastChild.ppid !== group) continue;
      return { helper: lastHelper, child: lastChild, nspid, invocation_id: started.uuid };
    }
  };
  let observation;
  const observationDeadline = Date.now() + 60000;
  while (!ended && Date.now() < observationDeadline) {
    const journal = await readCancellationJournal();
    if (journal?.lease) {
      assert.equal(journal.lease.scope.candidate_id, cancelledCandidate.id);
      for (const name of (await readdir(scratch)).filter(name => /^[0-9a-f]{64}$/.test(name) && !directoriesBefore.includes(name))) {
        try {
          const attempt = path.join(scratch, name);
          const saved = JSON.parse(await readFile(path.join(attempt, 'attempt.json'), 'utf8'));
          if (saved.request.attempt_id !== journal.lease.request.attempt_id) continue;
          const owned = JSON.parse(await readFile(path.join(attempt, 'build.stdout.process.json'), 'utf8'));
          if (!Number.isInteger(owned.group) || owned.group <= 0 || owned.quiescent) continue;
          const live = await liveSnapshot(attempt, owned.group);
          const confirmed = JSON.parse(await readFile(path.join(attempt, 'build.stdout.process.json'), 'utf8'));
          if (live && confirmed.group === owned.group && !confirmed.quiescent) {
            observation = { attempt, workspace_id: name, process_group: owned.group, proc: live,
              interpretation: 'live_helper_and_pidns_init_with_bazel_started_not_remote_test_execution_proof' };
            break;
          }
        } catch (error) {
          if (error.code !== 'ENOENT') { controller.kill('SIGTERM'); await completed.catch(() => {}); throw error; }
        }
      }
    }
    if (observation) break;
    await delay(25);
  }
  if (!observation || ended) {
    controller.kill('SIGTERM');
    await completed.catch(() => {});
    assert.fail('bounded actual Bazel cancellation window was missed; no cancellation acceptance is claimed');
  }
  stage = 'cancellation-sigterm-drain';
  let lastLive;
  try { lastLive = await liveSnapshot(observation.attempt, observation.process_group, observation.proc); }
  catch (error) { controller.kill('SIGTERM'); await completed.catch(() => {}); throw error; }
  if (!lastLive || ended) {
    controller.kill('SIGTERM'); await completed.catch(() => {});
    assert.fail('live helper/Bazel identity was lost before signal; no in-flight cancellation acceptance');
  }
  observation.immediately_before_signal = lastLive;
  assert.equal(controller.kill('SIGTERM'), true, 'signal only the exact spawned controller handle');
  if (artifacts) await writeFile(path.join(artifacts, 'cancellation-proc.json'), JSON.stringify(observation, null, 2), { mode: 0o600 });
  const cancelled = await completed;
  assert.equal(cancelled.result.state, 'released');
  const cancelledJournal = await readCancellationJournal();
  const cancelledReceipt = cancelledJournal.receipt;
  assert.equal(cancelledReceipt.outcome, 'unavailable'); assert.equal(cancelledReceipt.cleanup_confirmed, true);
  assert.equal(cancelledReceipt.request.job_id, cancelledCandidate.id);
  assert.equal(cancelledReceipt.request.candidate_commit, cancelledCandidate.head);
  assert.equal(cancelledReceipt.workspace_id, observation.workspace_id);
  assert.notEqual(cancelledReceipt.request.attempt_id, evidence.find(item => item.label === 'negative').receipt.request.attempt_id);
  assert.equal(cancelledJournal.release_receipt.outcome, 'unavailable');
  assert.equal(cancelledJournal.release_receipt.cleanup_confirmed, true);
  const physical = JSON.parse(await readFile(path.join(observation.attempt, 'build.stdout.process.json'), 'utf8'));
  assert.equal(physical.quiescent, true);
  const gone = async identity => {
    try { return !sameProcess(identity, await processIdentity(identity.pid)); }
    catch (error) { if (error.code === 'ENOENT' || error.code === 'ESRCH') return true; throw error; }
  };
  const disappearanceDeadline = Date.now() + 5000;
  while (!(await gone(lastLive.helper) && await gone(lastLive.child)) && Date.now() < disappearanceDeadline) await delay(25);
  assert.equal(await gone(lastLive.helper), true, 'original helper identity must be absent after cleanup');
  assert.equal(await gone(lastLive.child), true, 'original PID namespace init identity must be absent after cleanup');
  observation.original_helper_and_child_absent_after_cleanup = true;
  for (const name of ['source', 'source-staging']) await assert.rejects(lstat(path.join(observation.attempt, name)), { code: 'ENOENT' });
  const cancelledBep = await readFile(path.join(observation.attempt, 'bep.jsonl'), 'utf8');
  const ownedDirectories = (await readdir(scratch)).filter(name => /^[0-9a-f]{64}$/.test(name)).sort();
  assert.equal(ownedDirectories.length, directoriesBefore.length + 1, 'cancellation creates exactly one fresh execution nonce/attempt');
  const requestCount = relayEvents.length;
  assert.deepEqual(JSON.parse((await run(['recover-remote', '--account', source.account_id, '--profile', 'checks', '--claim-id', cancellationClaim])).stdout), cancelled);
  assert.equal(relayEvents.length, requestCount, 'terminal cancellation recovery cannot dispatch another check');
  assert.deepEqual((await readdir(scratch)).filter(name => /^[0-9a-f]{64}$/.test(name)).sort(), ownedDirectories);
  assert.equal(await readFile(path.join(observation.attempt, 'bep.jsonl'), 'utf8'), cancelledBep);
  const cancelledSelector = { change_id: amendment.change_id, patchset_id: amendment.patchset_id, candidate_id: cancelledCandidate.id };
  assert.equal((await invoke(21, { change_id: amendment.change_id })).state, 'checks_pending');
  await invoke(23, { ...cancelledSelector, decision: 'approve' }, 'approve-cancelled');
  assert.equal((await invoke(24, cancelledSelector, 'accept-cancelled-land', 400)).error.code, 'failed_precondition');
  assert.deepEqual((await invoke(2)).value, mainBeforeCancellation);
  evidence.push({ label: 'cancellation', receipt: cancelledReceipt, release: cancelled.result,
    observation, no_remote_test_execution_claim: true, no_reexecution_on_recovery: true });
  if (artifacts) {
    await copyFile(path.join(observation.attempt, 'bep.jsonl'), path.join(artifacts, 'cancellation-bep.jsonl'));
    await copyFile(path.join(observation.attempt, 'build.stdout.process.json'), path.join(artifacts, 'cancellation-process.json'));
    await writeFile(path.join(artifacts, 'cancellation-control.json'), JSON.stringify(cancelledJournal, null, 2), { mode: 0o600 });
    await writeFile(path.join(artifacts, 'cancellation-proc.json'), JSON.stringify(observation, null, 2), { mode: 0o600 });
  }
  assert.equal((await invoke(2)).value.main_generation, before.main_generation + 1);
  const summary = { schema: 1, production_worker: true, execution_only_linux: true, no_synthetic_verdict: true, evidence };
  if (artifacts) await writeFile(path.join(artifacts, 'checker-acceptance.json'), JSON.stringify(summary, null, 2), { mode: 0o600 });
  console.log(JSON.stringify(summary));
  acceptanceComplete = true;
} catch (error) {
  if (artifacts) {
    // Preserve bounded physical evidence even when a receipt/assertion fails.
    // Only this disposable fixture's own attempt files are inspected.
    for (const name of (await readdir(scratch)).filter(value => /^[0-9a-f]{64}$/.test(value))) {
      for (const file of ['build.stdout', 'build.stderr', 'bep.jsonl', 'execution.compact', 'snapshot-proof.json', 'attempt.json', 'receipt.json']) {
        const source = path.join(scratch, name, file);
        try {
          if ((await stat(source)).size <= 32 * 1024 * 1024) await copyFile(source, path.join(artifacts, `failed-${name}-${file}`));
        } catch (copyError) {
          if (copyError.code !== 'ENOENT') throw copyError;
        }
      }
    }
    try {
      const control = scratch + '-remote';
      for (const name of (await readdir(control)).filter(value => /^remote-[0-9a-f]{64}\.json$/.test(value))) {
        const source = path.join(control, name);
        if ((await stat(source)).size <= 128 * 1024) await copyFile(source, path.join(artifacts, `failed-${name}`));
      }
    } catch (copyError) { if (copyError.code !== 'ENOENT') throw copyError; }
  }
  console.error(JSON.stringify({ stage, error: String(error.message).replaceAll(token, '<redacted>').replaceAll(checkerToken, '<redacted>').slice(0, 8192) }));
  process.exitCode = 1;
} finally {
  let disposed = false;
  try {
    if (relay) { relay.closeAllConnections(); await new Promise(resolve => relay.close(resolve)); }
    if (mf) await mf.dispose(); disposed = true;
  }
  finally {
    if (acceptanceComplete && disposed) await rm(temporary, { recursive: true, force: true });
    else console.error(JSON.stringify({ stage, retained_private_fixture: root,
      reason: 'failed acceptance retains original journals, source and SQLite/R2 custody for explicit recovery' }));
  }
}
