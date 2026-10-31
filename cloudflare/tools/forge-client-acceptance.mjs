// Manual Linux acceptance of clean client enrollment against the shipped Worker.
// Loopback proves the actual local wire/authority path, not deployed HTTPS.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, readlink, lstat, rm } from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { runCheckerAcceptance } from './checker-acceptance.mjs';

const inputs = process.argv.slice(2);
assert.equal(inputs.length, 7, 'checker acceptance inputs plus the shipped tt executable required');
const tt = path.resolve(inputs.pop());
const tokens = [randomBytes(32).toString('hex'), randomBytes(32).toString('hex')];
const principals = ['client-a', 'client-b'];
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const hex = bytes => Buffer.from(bytes).toString('hex');
const records = [];
let localRoot, context, clients = [], finished = false, teardownComplete = false;
const secretless = value => tokens.reduce((text, token) => text.replaceAll(token, '<redacted>'), String(value ?? '')).slice(0, 8192);

async function mounted(root) {
  const lines = (await readFile('/proc/self/mountinfo', 'utf8')).trim().split('\n');
  return lines.some(line => {
    const [fields, kind] = line.split(' - ');
    return fields.split(' ')[4] === root && kind?.startsWith('fuse');
  });
}
async function raw(client, args, { cwd = client.root, success = true } = {}) {
  try {
    const argv = args[0] === '__forge_client_stop' ? args : ['--no-pager', '--color=never', ...args];
    const result = await context.execute(tt, argv, {
      cwd, env: client.env, encoding: 'utf8', timeout: 45000, maxBuffer: 2 * 1024 * 1024,
    });
    assert.ok(success, `unexpected success: ${args[0]}`); return result.stdout.trim();
  } catch (error) {
    if (!success && typeof error.code === 'number') return { failed: true, stdout: error.stdout, stderr: error.stderr };
    throw new Error(JSON.stringify({ command: args[0], code: error.code, signal: error.signal,
      stdout: secretless(error.stdout), stderr: secretless(error.stderr) }));
  }
}
async function command(client, args, options = {}) {
  const output = await raw(client, [...args, '--json'], options);
  const result = JSON.parse(typeof output === 'string' ? output : output.stdout);
  assert.equal(result.schema_version, 1);
  if (options.success === false) { assert.equal(result.state, 'failed'); return result; }
  assert.notEqual(result.state, 'failed'); return result;
}
const revision = (client, selected = '@') => raw(client, ['log', '--no-graph', '-r', selected, '-T', 'commit_id']);
const operation = client => raw(client, ['op', 'log', '--no-graph', '-n', '1', '-T', 'id']);
async function select(client, view = 'public') {
  return command(client, ['context', 'set', '--transport', 'forge-https-v1', '--server', context.origin,
    '--account', context.source.account_id, '--view', view, '--token-env', 'TT_FORGE_TOKEN'], { cwd: client.cwd });
}
async function makeClient(index) {
  const home = path.join(localRoot, `h${index}`), cwd = path.join(localRoot, `c${index}`), tmp = path.join(localRoot, `t${index}`);
  for (const directory of [home, cwd, tmp]) await mkdir(directory, { mode: 0o700 });
  const config = path.join(home, 'jj.toml');
  await writeFile(config, '[user]\nname="Client Fixture"\nemail="fixture@example.invalid"\n[operation]\nhostname="test"\nusername="test"\n[ui]\npaginate="never"\n[snapshot]\nmax-new-file-size=2097152\n', { mode: 0o600 });
  const client = { home, cwd, root: path.join(cwd, 'work'), env: {
    HOME: home, USERPROFILE: home, XDG_CONFIG_HOME: path.join(home, 'config'), TMPDIR: tmp,
    JJ_CONFIG: config, PATH: '/usr/bin:/bin', TERM: 'dumb', TT_FORGE_TOKEN: tokens[index],
  }, workspaces: [], descriptors: [] };
  clients.push(client); await select(client); return client;
}
async function work(client, destination = client.root) {
  if (!client.workspaces.includes(destination)) client.workspaces.push(destination);
  const result = await command(client, ['work', destination], { cwd: client.cwd });
  assert.equal(result.state, 'ready');
  assert.equal(result.result.destination, destination);
  assert.ok(await mounted(destination), 'tt work must create an actual Linux FUSE mount');
  const descriptor = result.result.client_descriptor;
  assert.equal(typeof descriptor, 'string', 'work must expose the owned nonsecret lifecycle descriptor');
  assert.ok(path.isAbsolute(descriptor)); client.descriptors.push(descriptor);
  const saved = await readFile(descriptor, 'utf8');
  for (const secret of [...tokens, context.token]) assert.ok(!saved.includes(secret), 'enrollment must store credential references, never bearer values');
  return result.result;
}
async function stopClient(client, descriptor) {
  await raw(client, ['__forge_client_stop', descriptor], { cwd: client.cwd });
  const directory = path.dirname(descriptor), deadline = Date.now() + 15000;
  for (;;) {
    const present = await Promise.all(['control.sock', 'rbe.sock'].map(async name => {
      try { await lstat(path.join(directory, name)); return true; }
      catch (error) { if (error.code === 'ENOENT') return false; throw error; }
    }));
    if (present.every(value => !value)) return;
    assert.ok(Date.now() < deadline, 'owned client transports did not finish shutdown');
    await delay(25);
  }
}
async function stopAndResume(client, expectedOperation) {
  const before = await revision(client);
  await raw(client, ['tinytree', 'unmount', client.root], { cwd: client.cwd });
  assert.equal(await mounted(client.root), false);
  await stopClient(client, client.descriptors[0]);
  // Resume locates the original exact descriptor, not today's named view.
  await raw(client, ['tinytree', 'resume', client.root], { cwd: client.cwd });
  assert.ok(await mounted(client.root));
  assert.equal(await revision(client), before);
  assert.equal(await operation(client), expectedOperation);
}

const workflow = {
  fixtureArgs: ['--client-workspace'],
  configurePrincipals: () => tokens.map((token, index) => ({ principal: principals[index], token_sha256: sha(token),
    accounts: ['forge-check'], operator: false, kind: 'forge' })),
  async beforePublish({ invoke, source }) {
    assert.equal(source.client_workspace, true);
    for (const mutation of source.members) await invoke(3, mutation, `client-member-${mutation.member.principal}`);
  },
  async start(value) {
    context = value;
    localRoot = await mkdtemp('/tmp/tfc.');
    const first = await makeClient(0), second = await makeClient(1);
    const one = await work(first);
    const replay = await work(first);
    assert.deepEqual(replay, one, 'identical tt work target replays original enrollment');
    const two = await work(second);
    assert.equal(one.view_version, hex(value.source.view_version));
    assert.equal(two.view_version, one.view_version);
    assert.notEqual(one.destination, two.destination);
    assert.notEqual(one.client_descriptor, two.client_descriptor);
    assert.match(one.provider_identity, /^[0-9a-f]{64}$/);
    assert.match(two.provider_identity, /^[0-9a-f]{64}$/);
    assert.notEqual(one.provider_identity, two.provider_identity, 'different member/owner-private providers must not share cache identity');
    for (const client of clients) {
      assert.match(await readFile(path.join(client.root, 'README.md'), 'utf8'), /^# Disposable checked source/);
      assert.equal(await readFile(path.join(client.root, 'src/source.txt'), 'utf8'), 'nested client fixture bytes\n');
      assert.equal(await readlink(path.join(client.root, 'readme-link')), 'README.md');
      assert.ok((await lstat(path.join(client.root, 'readme-link'))).isSymbolicLink());
      assert.deepEqual(await readFile(path.join(client.root, 'boundary.bin')), Buffer.alloc(1024 * 1024, 'K'));
      await assert.rejects(readFile(path.join(client.root, 'private/secret.txt')), error => error.code === 'ENOENT');
    }
    // Separate source and operation heads; no copied operator alias or server.
    await writeFile(path.join(second.root, 'peer-wip.txt'), 'independent member iteration\n');
    await raw(second, ['describe', '-m', 'Independent member iteration']);
    records.push({ second_operation: await operation(second), second_revision: await revision(second), second_provider_identity: two.provider_identity, selected: one.view_version });
    await assert.rejects(readFile(path.join(first.root, 'peer-wip.txt')), error => error.code === 'ENOENT');
  },
  async preparePositive(selected) {
    const first = clients[0];
    await writeFile(path.join(first.root, 'receipt.txt'), 'real checked source receipt\n');
    await raw(first, ['describe', '-m', 'Actual client authored checked source']);
    let report = await command(first, ['push', '--request-id', 'client-positive']);
    const deadline = Date.now() + 120000;
    for (let round = 0; round < 128 && (report.state !== 'checks_pending' || !report.result.value.projected_head); round++) {
      assert.ok(Date.now() < deadline, 'client source admission exceeds acceptance budget');
      report = await command(first, ['request', 'resume', 'client-positive']);
    }
    const review = report.result.value;
    assert.equal(report.state, 'checks_pending');
    assert.equal(review.candidate.root_tree, selected.expected_canonical_tree);
    assert.equal(await readFile(path.join(clients[1].root, 'peer-wip.txt'), 'utf8'), 'independent member iteration\n');
    assert.equal(await operation(clients[1]), records[0].second_operation);
    await writeFile(path.join(first.root, 'continued.txt'), 'private WIP after source submission\n');
    await raw(first, ['describe', '-m', 'Continued local iteration']);
    await writeFile(path.join(first.root, 'dirty.txt'), 'unsnapshotted iteration\n');
    return { state: report.state, value: review };
  },
  async afterPositive({ landed }) {
    const first = clients[0];
    assert.equal(landed.state, 'landed');
    const result = await command(first, ['request', 'reconcile', 'client-positive']);
    assert.equal(result.result.value.local_reconciliation, 'applied');
    assert.equal(await readFile(path.join(first.root, 'continued.txt'), 'utf8'), 'private WIP after source submission\n');
    assert.equal(await readFile(path.join(first.root, 'dirty.txt'), 'utf8'), 'unsnapshotted iteration\n');
    assert.equal(await operation(clients[1]), records[0].second_operation);
    records.push({ reconciled: true, selected: records[0].selected });
  },
  async finish() {
    let latest = await context.invoke(6);
    for (let round = 0; round < 128 && latest.state === 'pending'; round++) latest = await context.invoke(6, { version: latest.value.view_version, progress: latest.value.progress });
    assert.equal(latest.state, 'complete');
    assert.notEqual(hex(latest.value.view_version), records[0].selected, 'real landing must advance the named view without rebinding existing clients');
    for (const client of clients) {
      // Snapshot before lifecycle comparison; ordinary metadata commands should
      // then be observational and preserve the exact operation on restart.
      await raw(client, ['status']);
      const op = await operation(client);
      await stopAndResume(client, op);
      const status = await command(client, ['status']);
      assert.equal(status.result.workspace.view_version, records[0].selected);
    }
    assert.equal(await readFile(path.join(clients[0].root, 'dirty.txt'), 'utf8'), 'unsnapshotted iteration\n');
    assert.equal(await readFile(path.join(clients[1].root, 'peer-wip.txt'), 'utf8'), 'independent member iteration\n');
    await context.restartWorker();
    const account = (await context.invoke(2)).value;
    await context.invoke(4, { spec: { ...context.source.spec, name: 'locked', revision: account.main_revision,
      visibility: { kind: 'roles', roles: ['authors'] } }, expected_epoch: account.policy_epoch,
      make_default: false, follow_main: false }, 'client-locked-view', 200, 'locked');
    const second = clients[1];
    await select(second, 'locked');
    const lockedRoot = path.join(second.cwd, 'locked');
    const locked = await work(second, lockedRoot);
    assert.notEqual(locked.provider_identity, records[0].second_provider_identity);
    assert.match(await readFile(path.join(lockedRoot, 'README.md'), 'utf8'), /^# Disposable checked source/);
    const current = (await context.invoke(2)).value;
    await context.invoke(3, { member: { principal: 'client-b', can_write: false, can_review: false,
      is_admin: false, roles: [] }, expected_epoch: current.policy_epoch }, 'client-revoke');
    await command(second, ['view', 'read', 'README.md'], { cwd: second.cwd, success: false });
    await command(second, ['work', path.join(second.cwd, 'denied')], { cwd: second.cwd, success: false });
    assert.equal(await mounted(path.join(second.cwd, 'denied')), false);
    finished = true;
    await this.cleanup();
    return { production_worker: true, actual_linux_fuse: true, clean_native_only_enrollment: true,
      independent_member_workspaces: 2, verified_large_file_bytes: 1048576, real_client_authored_check_land: true,
      private_iteration_preserved: true, exact_snapshot_restart: true, warmed_scope_revoked: true, owned_mounts_and_clients_cleaned: true, deployed_https_proof: false };
  },
  async cleanup() {
    if (teardownComplete) return;
    let clean = true;
    for (const client of clients) {
      for (const root of client.workspaces.toReversed()) {
        if (await mounted(root)) {
          try { await raw(client, ['tinytree', 'unmount', root], { cwd: client.cwd }); }
          catch { clean = false; }
        }
      }
      for (const descriptor of [...new Set(client.descriptors)]) {
        try { await stopClient(client, descriptor); }
        catch { clean = false; }
      }
      for (const root of client.workspaces) if (await mounted(root)) clean = false;
    }
    assert.ok(clean, 'owned client teardown failed; original private state retained');
    teardownComplete = true;
    if (finished && localRoot) await rm(localRoot, { recursive: true, force: true });
    else if (localRoot) console.error(JSON.stringify({ retained_private_client_fixture: localRoot }));
  },
};
await runCheckerAcceptance({ inputs: [...inputs, tt], clientWorkflow: workflow });
