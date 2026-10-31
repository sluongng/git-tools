// Explicit deployed-endpoint acceptance. No local backend or provider setup.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { execFile } from 'node:child_process';
import { lookup } from 'node:dns';
import https from 'node:https';
import { isIP } from 'node:net';
import { lstat, mkdir, readFile, realpath, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const MAX_BODY = 8 * 1024 * 1024;
const ACTORS = ['owner', 'agent_a', 'agent_b'];
const hex = (s, n = 64) => typeof s === 'string' && new RegExp(`^[0-9a-f]{${n}}$`).test(s);
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const exact = (o, keys) => { assert.ok(o && typeof o === 'object' && !Array.isArray(o)); assert.deepEqual(Object.keys(o).sort(), [...keys].sort()); };
const id = value => Array.isArray(value) && value.length === 32 && value.every(n => Number.isInteger(n) && n >= 0 && n <= 255) ? Buffer.from(value).toString('hex') : value;
const bounded = (s, max = 4096) => typeof s === 'string' && s.length > 0 && Buffer.byteLength(s) <= max && !/[\x00-\x1f\x7f]/.test(s);
export function validateOrigin(value) {
  const u = new URL(value);
  assert.equal(u.protocol, 'https:'); assert.equal(u.origin, value);
  assert.equal(u.username + u.password + u.search + u.hash, '');
  assert.ok(!u.port || u.port === '443');
  assert.ok(!isIP(u.hostname) && u.hostname.includes('.') && !u.hostname.endsWith('.') &&
    !/(^|\.)(localhost|local|internal|test|invalid)$/.test(u.hostname), 'public DNS HTTPS origin required');
  return value;
}
function tokenReference(name) {
  assert.ok(/^[A-Z][A-Z0-9_]{1,127}$/.test(name));
  assert.ok(!/^(HOME|PATH|TMPDIR|XDG_.*|TINYTREE_.*|JJ_.*|LD_.*|DYLD_.*|NODE_.*|BASH_ENV)$/.test(name));
}
export function assertNoSecrets(value, secrets) {
  const bytes = typeof value === 'string' ? value : JSON.stringify(value);
  for (const secret of secrets) {
    if (!secret) continue;
    const forms = [secret, JSON.stringify(secret).slice(1, -1), encodeURIComponent(secret), Buffer.from(secret).toString('base64')];
    assert.ok(forms.every(form => !bytes.includes(form)), 'secret echo refused');
  }
}
export function validateStep(step) {
  exact(step, ['id', 'actor', 'cwd', 'argv', 'expect']);
  assert.ok(/^[a-z][a-z0-9_-]{0,63}$/.test(step.id) && ACTORS.includes(step.actor));
  assert.ok(step.cwd === 'root' || (bounded(step.cwd, 1024) && !path.isAbsolute(step.cwd) && step.cwd.split('/').every(p => p && p !== '.' && p !== '..')));
  assert.ok(Array.isArray(step.argv) && step.argv.length <= 16 && step.argv.every(a => bounded(a, 1024)));
  const a = step.argv;
  // A closed grammar, not an arbitrary tt/global-config execution facility.
  let kind;
  if (a.length === 2 && a[0] === 'account' && a[1] === 'show') kind = 'account';
  else if (a.length === 4 && a[0] === 'view' && a[1] === 'show' && a[2] === '--version' && hex(a[3])) kind = 'view';
  else if (a[0] === 'review' && a[1] === 'show' && hex(a[2], 32) && (a.length === 3 || (a.length === 5 && a[3] === '--patchset' && hex(a[4])))) kind = 'review';
  else if (a.length === 9 && a[0] === 'review' && ['approve', 'land'].includes(a[1]) && hex(a[2], 32) &&
    a[3] === '--patchset' && hex(a[4]) && a[5] === '--candidate' && hex(a[6]) && a[7] === '--request-id' && /^[A-Za-z0-9_-]{1,128}$/.test(a[8])) kind = 'review';
  else throw new Error('unsupported deployed workflow argv');
  assert.equal(step.expect.kind, kind);
  if (kind === 'account') { exact(step.expect, ['kind', 'main_generation', 'main_revision']); assert.ok(Number.isSafeInteger(step.expect.main_generation) && step.expect.main_generation > 0 && hex(step.expect.main_revision)); }
  if (kind === 'view') { exact(step.expect, ['kind', 'view_version', 'revision']); assert.ok(hex(step.expect.view_version) && hex(step.expect.revision)); assert.equal(a[3], step.expect.view_version); }
  if (kind === 'review') {
    exact(step.expect, ['kind', 'change_id', 'patchset_id', 'candidate_id', 'state']);
    assert.ok(hex(step.expect.change_id, 32) && hex(step.expect.patchset_id) && hex(step.expect.candidate_id));
    assert.ok(['prepared', 'ready_to_land', 'checks_pending', 'rebase_pending', 'conflict', 'landed'].includes(step.expect.state));
    assert.equal(a[2], step.expect.change_id);
    if (a[3] === '--patchset') assert.equal(a[4], step.expect.patchset_id);
    if (a[5] === '--candidate') assert.equal(a[6], step.expect.candidate_id);
  }
  return step;
}
export function validateManifest(m) {
  exact(m, ['schema_version', 'origin', 'account_id', 'view', 'view_version', 'revision', 'tt', 'output_dir', 'actors', 'public', 'denials', 'steps']);
  assert.equal(m.schema_version, 1); validateOrigin(m.origin);
  assert.ok(/^[a-z0-9-]{1,64}$/.test(m.account_id) && /^[A-Za-z0-9_-]{1,128}$/.test(m.view));
  assert.ok(hex(m.view_version) && hex(m.revision)); exact(m.tt, ['path', 'sha256']);
  assert.ok(path.isAbsolute(m.tt.path) && hex(m.tt.sha256) && path.isAbsolute(m.output_dir));
  exact(m.actors, ACTORS);
  const roots = [];
  for (const actor of ACTORS) { const v = m.actors[actor]; exact(v, ['root', 'token_env']); assert.ok(path.isAbsolute(v.root)); tokenReference(v.token_env); roots.push(path.resolve(v.root)); }
  assert.equal(new Set(ACTORS.map(name => m.actors[name].token_env)).size, 3, 'distinct credential references required');
  roots.push(path.resolve(m.output_dir));
  for (let i = 0; i < roots.length; i++) for (let j = 0; j < i; j++) assert.ok(roots[i] !== roots[j] && !roots[i].startsWith(roots[j] + path.sep) && !roots[j].startsWith(roots[i] + path.sep), 'isolated disjoint roots required');
  exact(m.public, ['readme_path', 'readme_sha256', 'query', 'regex', 'limit', 'expected_paths']);
  assert.ok(bounded(m.public.readme_path, 1024) && !m.public.readme_path.split('/').some(p => !p || p === '..' || p === '.'));
  assert.ok(hex(m.public.readme_sha256) && bounded(m.public.query, 256) && typeof m.public.regex === 'boolean' && Number.isInteger(m.public.limit) && m.public.limit >= 1 && m.public.limit <= 100);
  assert.ok(Array.isArray(m.public.expected_paths) && m.public.expected_paths.length > 0 && m.public.expected_paths.length <= 100 && m.public.expected_paths.every(p => bounded(p, 1024)));
  assert.ok(Array.isArray(m.denials) && m.denials.length > 0 && m.denials.length <= 16);
  for (const d of m.denials) { exact(d, ['actor', 'view', 'path', 'version']); assert.ok(['anonymous', 'invalid_token', 'agent_a', 'agent_b'].includes(d.actor) && /^[A-Za-z0-9_-]{1,128}$/.test(d.view) && bounded(d.path, 1024) && hex(d.version)); }
  assert.ok(m.denials.some(d => d.actor === 'anonymous') && m.denials.some(d => d.actor === 'invalid_token'));
  assert.ok(Array.isArray(m.steps) && m.steps.length <= 64); m.steps.forEach(validateStep);
  assert.equal(new Set(m.steps.map(s => s.id)).size, m.steps.length);
  return m;
}
function publicAddress(address) {
  if (isIP(address) === 4) { const [a,b] = address.split('.').map(Number); return !(a === 0 || a === 10 || a === 127 || a >= 224 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127)); }
  return isIP(address) === 6 && !/^(::|fc|fd|fe[89ab]|ff)/i.test(address) && !address.toLowerCase().startsWith('::ffff:');
}
async function transport(origin, body, token, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const req = https.request(origin + '/api/v1/forge/call', { method: 'POST', agent: false, timeout: timeoutMs,
      lookup(host, options, callback) { lookup(host, { all: true }, (error, entries) => {
        if (error || !entries?.length || entries.some(e => !publicAddress(e.address))) return callback(new Error('nonpublic endpoint resolution refused'));
        if (options?.all) callback(null, entries); else callback(null, entries[0].address, entries[0].family);
      }); },
      headers: { 'content-type': 'application/json', 'content-length': body.length, ...(token ? { authorization: `Bearer ${token}` } : {}) },
    }, response => {
      const chunks = []; let size = 0;
      response.on('data', chunk => { size += chunk.length; if (size > MAX_BODY) { response.destroy(); reject(new Error('response bound exceeded')); } else chunks.push(chunk); });
      response.on('end', () => { clearTimeout(timer); resolve({ status: response.statusCode, body: Buffer.concat(chunks) }); }); response.on('error', () => { clearTimeout(timer); reject(new Error('response unavailable')); });
    });
    const timer = setTimeout(() => req.destroy(new Error('HTTPS deadline exceeded')), timeoutMs);
    req.on('timeout', () => req.destroy(new Error('HTTPS deadline exceeded'))); req.on('error', () => { clearTimeout(timer); reject(new Error('HTTPS request unavailable')); }); req.end(body);
  });
}
export function validateForgeReply(reply, call) {
  exact(reply, ['version', 'operation', 'account_id', 'view', 'request_id', 'state', 'result_json']);
  for (const key of ['version', 'operation', 'account_id', 'view', 'request_id']) assert.equal(reply[key], call[key], 'response binding mismatch');
  assert.ok(/^[a-z][a-z0-9_]{0,63}$/.test(reply.state) && typeof reply.result_json === 'string' && Buffer.byteLength(reply.result_json) <= MAX_BODY);
  return { state: reply.state, value: JSON.parse(reply.result_json) };
}
export async function requestForge(m, operation, payload, token, secrets, send = transport, deadline = Date.now() + 30000) {
  validateOrigin(m.origin);
  const call = { version: 1, operation, account_id: m.account_id, view: m.view, request_id: '', payload_json: JSON.stringify(payload) };
  const body = Buffer.from(JSON.stringify(call)); assert.ok(body.length <= 2 * 1024 * 1024);
  const remaining = Math.min(30000, deadline - Date.now()); assert.ok(remaining > 0, 'HTTPS deadline exceeded');
  const response = await send(m.origin, body, token, remaining);
  assert.ok(Buffer.isBuffer(response.body) && response.body.length <= MAX_BODY); assertNoSecrets(response.body.toString('utf8'), secrets);
  assert.ok(response.status < 300 || response.status >= 400, 'redirect refused');
  if (response.status !== 200) return { status: response.status, error: JSON.parse(response.body.toString('utf8')) };
  return { status: 200, ...validateForgeReply(JSON.parse(response.body.toString('utf8')), call) };
}
function selection(m, value) {
  assert.equal(value.account_id, m.account_id); assert.equal(value.view, m.view);
  assert.equal(id(value.view_version), m.view_version); assert.equal(id(value.revision), m.revision);
  assert.ok(Number.isSafeInteger(value.policy_epoch) && value.policy_epoch > 0);
}
export async function verifyDenials(m, secrets, send = transport, onStage = () => {}) {
  const receipts = [];
  for (const [index, denial] of m.denials.entries()) {
    const scoped = { ...m, view: denial.view };
    const payload = { version: [...Buffer.from(denial.version, 'hex')], path: denial.path };
    onStage(`denial_${index}_owner_account`);
    const account = await requestForge(scoped, 2, {}, secrets[0], secrets, send);
    assert.equal(account.status, 200); assert.equal(account.state, 'complete');
    assert.equal(account.value.account_id, m.account_id);
    assert.ok(Number.isSafeInteger(account.value.policy_epoch) && account.value.policy_epoch > 0);
    onStage(`denial_${index}_owner_read`);
    const owner = await requestForge(scoped, 7, payload, secrets[0], secrets, send);
    assert.equal(owner.status, 200, 'owner control must read the exact locked resource'); assert.equal(owner.state, 'complete');
    assert.equal(owner.value.account_id, m.account_id); assert.equal(owner.value.view, denial.view);
    assert.equal(id(owner.value.view_version), denial.version); assert.ok(hex(id(owner.value.revision)) && id(owner.value.revision) !== '0'.repeat(64));
    assert.equal(owner.value.policy_epoch, account.value.policy_epoch); assert.equal(owner.value.path, denial.path);
    assert.ok(typeof owner.value.content === 'string' && Buffer.byteLength(owner.value.content, 'utf8') > 0, 'owner control requires nonempty file content');
    onStage(`denial_${index}_access`);
    const token = denial.actor === 'anonymous' ? undefined : denial.actor === 'invalid_token' ? randomBytes(32).toString('hex') : secrets[ACTORS.indexOf(denial.actor)];
    const reply = await requestForge(scoped, 7, payload, token, [...secrets, token], send);
    assert.ok([401, 403].includes(reply.status), 'locked read did not fail closed');
    exact(reply.error, ['version', 'error']); assert.equal(reply.error.version, 1); exact(reply.error.error, ['code', 'message']);
    assert.equal(reply.error.error.code, reply.status === 401 ? 'unauthenticated' : 'permission_denied');
    assert.ok(typeof reply.error.error.message === 'string' && Buffer.byteLength(reply.error.error.message) <= 1024);
    receipts.push({ id: `denial_${index}`, actor: denial.actor, view: denial.view, view_version: denial.version,
      policy_epoch: owner.value.policy_epoch, owner_control_readable: true, state: 'verified', status: reply.status });
  }
  return receipts;
}

class SearchFailure extends Error { constructor(code) { super(code); this.code = code; } }
export async function searchPublic(m, secrets, policyEpoch, send = transport, deadline = Date.now() + 60000) {
  const found = new Set(), cursors = new Set(); let cursor = null;
  const skipped = { binary: 0, oversized: 0 };
  for (let page = 1; page <= 128; page++) {
    if (Date.now() >= deadline) throw new SearchFailure('search_deadline_exhausted');
    const reply = await requestForge(m, 8, { version: Buffer.from(m.view_version, 'hex').toJSON().data,
      query: m.public.query, regex: m.public.regex, limit: m.public.limit, cursor }, undefined, secrets, send, deadline);
    assert.equal(reply.status, 200); assert.equal(reply.state, 'complete');
    const value = reply.value; selection(m, value); assert.equal(value.policy_epoch, policyEpoch);
    assert.ok(Array.isArray(value.matches) && value.matches.length <= m.public.limit && typeof value.complete === 'boolean');
    exact(value.skipped_files, ['binary', 'oversized']);
    for (const key of ['binary', 'oversized']) {
      assert.ok(Number.isSafeInteger(value.skipped_files[key]) && value.skipped_files[key] >= 0);
      assert.ok(value.skipped_files[key] >= skipped[key], 'cumulative search skip count regressed');
      skipped[key] = value.skipped_files[key];
    }
    for (const hit of value.matches) {
      exact(hit, ['path', 'line', 'column', 'preview']);
      assert.ok(bounded(hit.path, 1024) && Number.isSafeInteger(hit.line) && hit.line >= 1 && Number.isSafeInteger(hit.column) && hit.column >= 1 && typeof hit.preview === 'string' && Buffer.byteLength(hit.preview) <= 4096);
      if (!m.public.regex) assert.ok(hit.preview.includes(m.public.query), 'literal search hit does not contain query');
      if (m.public.expected_paths.includes(hit.path)) found.add(hit.path);
    }
    assert.ok(value.cursor === null || (typeof value.cursor === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(value.cursor)));
    if (value.complete) {
      assert.equal(value.cursor, null, 'complete search cannot continue');
      assert.equal(skipped.binary + skipped.oversized, 0, 'complete search cannot skip files');
    }
    if (m.public.expected_paths.every(expected => found.has(expected))) return {
      search_complete: value.complete, search_coverage: value.complete ? 'complete' : 'expected_paths_only',
      search_pages: page, search_terminal: value.cursor === null, skipped_files: skipped,
    };
    if (value.cursor === null) throw new SearchFailure('search_expected_paths_missing_at_terminal');
    assert.ok(!cursors.has(value.cursor), 'repeated search cursor refused'); cursors.add(value.cursor); cursor = value.cursor;
  }
  throw new SearchFailure('search_page_budget_exhausted');
}

async function privateDirectory(directory) {
  assert.equal(directory, path.resolve(directory));
  for (let ancestor = directory; ancestor !== path.dirname(ancestor); ancestor = path.dirname(ancestor)) {
    try { assert.ok(!(await lstat(ancestor)).isSymbolicLink(), 'symlink ancestor refused'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  await mkdir(directory, { recursive: true, mode: 0o700 }); const info = await lstat(directory);
  assert.ok(info.isDirectory() && !info.isSymbolicLink() && info.uid === process.getuid() && (info.mode & 0o777) === 0o700);
  assert.equal(await realpath(directory), path.resolve(directory), 'symlink path refused');
}
async function cli(binary, actor, argv, secrets, cwd = actor.root) {
  assertNoSecrets(argv, secrets);
  return new Promise((resolve, reject) => execFile(binary, [...argv, '--json'], { cwd, env: actor.env, timeout: 65000, maxBuffer: MAX_BODY, encoding: 'utf8' }, (error, stdout, stderr) => {
    try { assertNoSecrets(stdout + stderr, secrets); if (error) throw new Error('tt command failed; raw output withheld'); const reply = JSON.parse(stdout); assert.equal(reply.schema_version, 1); assert.equal(reply.error, null); assert.ok(reply.result); resolve(reply); } catch { reject(new Error('tt command or bounded receipt validation failed')); }
  }));
}
export function validateStepReceipt(m, step, reply) {
  const expected = step.expect, v = reply.result.value;
  assert.equal(reply.result.account_id, m.account_id); assert.equal(reply.result.view, m.view);
  const operation = expected.kind === 'account' ? 'get_account' : expected.kind === 'view' ? 'resolve_view' : step.argv[1] === 'show' ? 'get_review' : step.argv[1] === 'land' ? 'land_review' : 'decide_review';
  const requestId = step.argv.length === 9 ? step.argv[8] : '';
  assert.equal(reply.operation, operation); assert.equal(reply.result.request_id, requestId);
  const fields = { operation, request_id: requestId, server_state: reply.state };
  if (expected.kind !== 'review') assert.equal(reply.state, 'complete');
  if (expected.kind === 'view') { selection({ ...m, view_version: expected.view_version, revision: expected.revision }, v); Object.assign(fields, { view_version: expected.view_version, revision: expected.revision, policy_epoch: v.policy_epoch }); }
  if (expected.kind === 'account') { assert.equal(v.account_id, m.account_id); assert.equal(v.main_generation, expected.main_generation); assert.equal(id(v.main_revision), expected.main_revision); Object.assign(fields, { main_generation: v.main_generation, main_revision: id(v.main_revision) }); }
  if (expected.kind === 'review') {
    assert.equal(reply.state, expected.state); assert.equal(v.state, expected.state); assert.equal(v.change_id, expected.change_id); assert.equal(v.patchset_id, expected.patchset_id); assert.equal(v.candidate.id, expected.candidate_id);
    assert.ok(hex(v.source_base) && hex(v.source_head) && id(v.view_version) === m.view_version);
    assert.ok(hex(v.candidate.head) && hex(v.candidate.root_tree) && hex(v.candidate.target) && Number.isSafeInteger(v.candidate.target_generation) && v.candidate.target_generation > 0);
    exact(v.check_profile, ['name', 'version']); assert.ok(bounded(v.check_profile.name, 64) && bounded(v.check_profile.version, 128));
    assert.ok(Number.isSafeInteger(v.approvals) && v.approvals >= 0);
    Object.assign(fields, { change_id: v.change_id, patchset_id: v.patchset_id, candidate_id: v.candidate.id, candidate_head: v.candidate.head,
      candidate_root_tree: v.candidate.root_tree, target_head: v.candidate.target, target_generation: v.candidate.target_generation,
      source_base: v.source_base, source_head: v.source_head, view_version: id(v.view_version), profile: v.check_profile,
      approvals: v.approvals, check_evidence: 'server_review_state_only', land_evidence: reply.state === 'landed' ? 'server_landed_state_only' : 'not_landed' });
    if (v.projected_head !== undefined) { assert.ok(hex(v.projected_head)); fields.projected_head = v.projected_head; }
  }
  return fields;
}

export function goalGates() {
  return { source_freeze: 'not_verified', real_checker_profile_and_bep: 'not_verified', generation_and_two_workspace_authoring: 'not_verified', interrupted_recovery: 'not_verified', cutover: 'not_verified', real_browser: 'not_verified', full_goal_complete: false };
}
export async function run(m, { execute = false } = {}) {
  validateManifest(m); const secrets = ACTORS.map(name => process.env[m.actors[name].token_env]);
  assert.ok(secrets.every(s => bounded(s, 8192)), 'configured token reference unavailable'); assertNoSecrets(m, secrets);
  const binary = await realpath(m.tt.path); const binaryInfo = await lstat(binary); assert.ok(binaryInfo.isFile() && binaryInfo.size <= 256 * 1024 * 1024 && (binaryInfo.mode & 0o022) === 0);
  assert.equal(digest(await readFile(binary)), m.tt.sha256, 'tt binary pin mismatch');
  await privateDirectory(m.output_dir);
  const receipt = { schema_version: 1, origin: m.origin, account_id: m.account_id, view: m.view, view_version: m.view_version, revision: m.revision, tt_sha256: m.tt.sha256, execute, stages: [], deployment_preflight_complete: false, ...goalGates() };
  const save = async () => { assertNoSecrets(receipt, secrets); await writeFile(path.join(m.output_dir, `receipt-${randomBytes(8).toString('hex')}.json`), JSON.stringify(receipt, null, 2), { mode: 0o600, flag: 'wx' }); };
  let currentStage = 'public_resolve';
  try {
    let selected, progress, selectedEpoch; const resolveDeadline = Date.now() + 60000;
    for (let round = 0; round < 512 && Date.now() < resolveDeadline; round++) {
      selected = await requestForge(m, 6, { version: Buffer.from(m.view_version, 'hex').toJSON().data, ...(progress ? { progress } : {}) }, undefined, secrets, transport, resolveDeadline);
      assert.equal(selected.status, 200); selection(m, selected.value);
      if (selectedEpoch !== undefined) assert.equal(selected.value.policy_epoch, selectedEpoch);
      selectedEpoch = selected.value.policy_epoch;
      if (selected.state === 'complete') break;
      assert.equal(selected.state, 'pending'); const next = selected.value.progress; exact(next, ['job_id', 'expected_generation']);
      assert.ok(/^[0-9a-f-]{36}$/.test(next.job_id) && Number.isSafeInteger(next.expected_generation) && next.expected_generation >= 0);
      if (progress) { assert.equal(next.job_id, progress.job_id); assert.ok(next.expected_generation >= progress.expected_generation); }
      progress = next;
    }
    assert.equal(selected.status, 200); assert.equal(selected.state, 'complete', 'public view not ready; preparation remains unverified'); selection(m, selected.value);
    currentStage = 'anonymous_readme';
    const readme = await requestForge(m, 7, { version: Buffer.from(m.view_version, 'hex').toJSON().data, path: m.public.readme_path }, undefined, secrets);
    assert.equal(readme.status, 200); assert.equal(readme.state, 'complete'); selection(m, readme.value); assert.equal(readme.value.policy_epoch, selectedEpoch); assert.equal(readme.value.path, m.public.readme_path); assert.equal(digest(Buffer.from(readme.value.content, 'utf8')), m.public.readme_sha256);
    currentStage = 'anonymous_search';
    const search = await searchPublic(m, secrets, selectedEpoch);
    receipt.stages.push({ id: 'anonymous_readme_search', state: 'verified', ...search });
    const actors = {}; const sessionIdentities = new Set();
    for (const [index, name] of ACTORS.entries()) {
      currentStage = `${name}_pinned_context`;
      const input = m.actors[name]; await privateDirectory(input.root);
      const config = path.join(input.root, 'tinytree'); await privateDirectory(config);
      const expected = { schema_version: 1, transport: 'forge_https_v1', server: m.origin, account_id: m.account_id, view: m.view, token_env: input.token_env, ca_certificate: null, tls_domain: null };
      const jjConfig = path.join(input.root, 'acceptance-jj.toml');
      const jjBytes = '[user]\nname="Acceptance"\nemail="acceptance@example.invalid"\n';
      try { const info = await lstat(jjConfig); assert.ok(info.isFile() && !info.isSymbolicLink() && info.uid === process.getuid() && (info.mode & 0o077) === 0 && info.nlink === 1 && info.size <= 4096); assert.equal(await readFile(jjConfig, 'utf8'), jjBytes); }
      catch (error) { if (error.code !== 'ENOENT') throw error; await writeFile(jjConfig, jjBytes, { mode: 0o600, flag: 'wx' }); }
      const actor = { root: input.root, env: { HOME: input.root, XDG_CONFIG_HOME: path.join(input.root, 'config'), XDG_STATE_HOME: path.join(input.root, 'state'), TINYTREE_CONFIG_DIR: config, JJ_CONFIG: jjConfig, PATH: '', TERM: 'dumb', [input.token_env]: secrets[index] } }; actors[name] = actor;
      try { const file = path.join(config, 'context.json'); const info = await lstat(file); assert.ok(info.isFile() && !info.isSymbolicLink() && info.uid === process.getuid() && info.nlink === 1 && (info.mode & 0o077) === 0 && info.size <= 16384); const current = JSON.parse(await readFile(file, 'utf8')); assert.deepEqual(current, expected, 'prepared context mismatch'); }
      catch (error) { if (error.code !== 'ENOENT') throw error; await cli(binary, actor, ['context', 'set', '--transport', 'forge-https-v1', '--server', m.origin, '--account', m.account_id, '--view', m.view, '--token-env', input.token_env], secrets); }
      const shown = await cli(binary, actor, ['context', 'show'], secrets); assert.deepEqual(shown.result, expected);
      const session = await requestForge(m, 12, {}, secrets[index], secrets);
      assert.equal(session.status, 200); assert.equal(session.state, 'complete'); exact(session.value, ['schema_version', 'account_id', 'identity']);
      assert.equal(session.value.schema_version, 1); assert.equal(session.value.account_id, m.account_id); exact(session.value.identity, ['kind', 'principal', 'credential_id']);
      assert.equal(session.value.identity.kind, 'authenticated'); assert.ok(bounded(session.value.identity.principal, 128) && hex(session.value.identity.credential_id));
      assert.ok(!sessionIdentities.has(session.value.identity.principal), 'actors must authenticate as distinct principals'); sessionIdentities.add(session.value.identity.principal);
      const view = await cli(binary, actor, ['view', 'show', '--version', m.view_version], secrets); selection(m, view.result.value); assert.equal(view.result.value.policy_epoch, selectedEpoch);
      receipt.stages.push({ id: `${name}_pinned_context`, state: 'verified' });
    }
    receipt.stages.push(...await verifyDenials(m, secrets, transport, stage => { currentStage = stage; }));
    receipt.deployment_preflight_complete = true;
    if (execute) for (const step of m.steps) {
      currentStage = `step:${step.id}`;
      const actor = actors[step.actor]; const cwd = step.cwd === 'root' ? actor.root : path.join(actor.root, step.cwd);
      assert.equal(await realpath(cwd), cwd); assert.ok(cwd === actor.root || cwd.startsWith(actor.root + path.sep));
      const reply = await cli(binary, actor, step.argv, secrets, cwd);
      const fields = validateStepReceipt(m, step, reply);
      receipt.stages.push({ id: step.id, state: 'verified', ...fields });
    }
    await save(); return receipt;
  } catch (error) { receipt.failure = { stage: currentStage, code: error instanceof SearchFailure ? error.code : 'acceptance_validation_failed' }; await save(); throw new Error('deployed acceptance failed; raw response withheld'); }
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2); const execute = args[0] === '--execute'; if (execute) args.shift();
  if (args.length !== 1) throw new Error('usage: forge-deployed-acceptance.mjs [--execute] MANIFEST.json');
  try { const bytes = await readFile(args[0]); assert.ok(bytes.length <= 128 * 1024); const receipt = await run(JSON.parse(bytes), { execute }); console.log(JSON.stringify(receipt)); }
  catch { console.error('deployed acceptance failed; inspect owner-private receipt if created'); process.exitCode = 1; }
}
