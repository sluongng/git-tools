// Opt-in immutable snapshot protocol fit, not a source-write pause or cutover.
// All repository interpretation and identity checks use the shipped Rust/Wasm
// bootstrap/closure implementation. This driver has no native object codec.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, realpath, open, mkdtemp, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { Miniflare } from 'miniflare';

const [workerArg, wasmArg] = process.argv.slice(2);
assert.ok(workerArg && wasmArg && process.argv.length === 4, 'declared Worker and Wasm arguments required');
const snapshotArg = process.env.TT_FORGE_IMPORT_SNAPSHOT_DIR;
assert.ok(snapshotArg && path.isAbsolute(snapshotArg), 'explicit absolute TT_FORGE_IMPORT_SNAPSHOT_DIR required');
const MAX_OBJECTS = 32768, MAX_BYTES = 512 * 1024 * 1024, MAX_OBJECT_BYTES = 1024 * 1024;
const MAX_METADATA_BYTES = 256 * 1024;
const MAX_MANIFEST = 16 * 1024 * 1024, MAX_RESPONSE = 8 * 1024 * 1024;
const hex = value => typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
const kinds = { 1: 'file', 2: 'tree', 3: 'commit', 4: 'symlink', 5: 'copy_history' };
const exact = (value, keys) => {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value), 'expected bounded record');
  assert.deepEqual(Object.keys(value).sort(), [...keys].sort(), 'unexpected record fields');
};
async function privateDirectory(directory) {
  const stat = await lstat(directory);
  assert.ok(stat.isDirectory() && !stat.isSymbolicLink() && (stat.mode & 0o077) === 0, 'snapshot directory must be owner-private and nonsymlink');
  assert.equal(stat.uid, process.getuid());
}
async function privateFile(file, maximum, expectedSize) {
  const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat();
    assert.ok(stat.isFile() && stat.nlink === 1 && stat.uid === process.getuid() && (stat.mode & 0o077) === 0, 'snapshot file must be a single-link owner-private regular file');
    assert.ok(stat.size <= maximum, 'snapshot file exceeds the existing representation bound');
    if (expectedSize !== undefined) assert.equal(stat.size, expectedSize, 'snapshot file differs from its descriptor length');
    const data = await handle.readFile(); assert.equal(data.length, stat.size); return data;
  } finally { await handle.close(); }
}
const snapshot = path.resolve(snapshotArg);
assert.equal(await realpath(snapshot), snapshot, 'snapshot directory may not traverse symlinks');
await privateDirectory(snapshot); await privateDirectory(path.join(snapshot, 'objects'));
try { await lstat(path.join(snapshot, 'INCOMPLETE')); assert.fail('incomplete source snapshots cannot be imported'); }
catch (error) { if (error.code !== 'ENOENT') throw error; }
const manifest = JSON.parse((await privateFile(path.join(snapshot, 'manifest.json'), MAX_MANIFEST)).toString('utf8'));
exact(manifest, ['schema_version', 'scope', 'repository_id', 'intrinsic_commit_root', 'report', 'objects']);
assert.equal(manifest.schema_version, 1); assert.equal(manifest.scope, 'explicit-reference-reachable-closure');
assert.equal(manifest.intrinsic_commit_root, true);
assert.ok(typeof manifest.repository_id === 'string' && manifest.repository_id.length > 0 && manifest.repository_id.length <= 128);
const report = manifest.report;
assert.equal(report.complete, true); assert.equal(report.error, null);
assert.deepEqual(report.initial_refs, report.final_refs, 'source exporter must have observed the same exact reference before and after its walk');
assert.ok(Array.isArray(report.initial_refs) && report.initial_refs.length === 1, 'this fit probe selects exactly main');
const selected = report.initial_refs[0];
assert.equal(selected.name, 'main'); assert.ok(hex(selected.head) && selected.head !== '0'.repeat(64));
assert.ok(Number.isSafeInteger(selected.generation) && selected.generation > 0);
assert.ok(Array.isArray(manifest.objects) && manifest.objects.length > 0 && manifest.objects.length <= MAX_OBJECTS);
const typed = new Map(), totals = {};
let totalBytes = 0;
for (const object of manifest.objects) {
  exact(object, ['kind', 'id', 'size_bytes', 'format_version', 'file']);
  assert.ok(kinds[object.kind] && hex(object.id) && object.format_version === 1);
  assert.ok(!(object.kind === 3 && object.id === '0'.repeat(64)), 'intrinsic ROOT has no uploaded body');
  assert.ok(Number.isSafeInteger(object.size_bytes) && object.size_bytes >= 0 && object.size_bytes <= (object.kind === 1 ? MAX_OBJECT_BYTES : MAX_METADATA_BYTES), 'object exceeds current bootstrap object budget');
  assert.equal(object.file, `objects/${object.kind}-${object.id}.bin`, 'snapshot paths must be deterministic typed filenames');
  const key = `${object.kind}:${object.id}`; assert.ok(!typed.has(key), 'duplicate typed snapshot object'); typed.set(key, object);
  totalBytes += object.size_bytes; assert.ok(totalBytes <= MAX_BYTES, 'snapshot exceeds the current account byte allowance');
  const total = totals[kinds[object.kind]] ??= { objects: 0, bytes: 0 }; total.objects++; total.bytes += object.size_bytes;
}
assert.ok(typed.has(`3:${selected.head}`), 'the exact selected head must be supplied');
assert.equal(manifest.objects.length, report.unique_objects); assert.equal(totalBytes, report.unique_bytes);
for (const [kind, total] of Object.entries(totals)) {
  assert.equal(total.objects, report.by_kind[kind]?.objects); assert.equal(total.bytes, report.by_kind[kind]?.bytes);
}
assert.ok(report.max_metadata_bytes <= MAX_METADATA_BYTES, 'metadata exceeds the existing canonical budget');

const temporary = await mkdtemp(path.join(process.env.TEST_TMPDIR ?? tmpdir(), 'tinytree-import-fit-'));
const token = randomBytes(32).toString('hex'), account = 'snapshot-fit';
const sha = value => createHash('sha256').update(value).digest('hex'); // Authentication only, never native identity.
const configuration = {
  modules: [{ type: 'ESModule', path: path.resolve(workerArg) }, { type: 'CompiledWasm', path: path.resolve(wasmArg) }],
  compatibilityDate: '2026-07-08', host: '127.0.0.1', port: 0,
  durableObjects: { REPOSITORIES: { className: 'AccountRepository', useSQLite: true } },
  r2Buckets: ['OBJECTS'], durableObjectsPersist: path.join(temporary, 'do'), r2Persist: path.join(temporary, 'r2'),
  bindings: { PRINCIPALS_JSON: JSON.stringify([{ principal: 'snapshot-owner', token_sha256: sha(token), accounts: [account], operator: true, kind: 'forge' }]),
    FORGE_BACKEND: 'enabled', EXPERIMENTAL_RAW_BACKEND: 'disabled' },
};
let mf, origin, stage = 'open-worker', complete = false, uploaded = 0, progressRounds = 0;
const deadline = Date.now() + 20 * 60 * 1000;
const requireTime = () => assert.ok(Date.now() < deadline, 'immutable snapshot fit exceeded its absolute twenty-minute budget');
async function restart() {
  requireTime(); if (mf) await mf.dispose(); mf = new Miniflare(configuration);
  const address = await mf.ready;
  if (origin) assert.equal(address.origin, origin, 'restart must preserve the exact import endpoint');
  else { origin = address.origin; configuration.port = Number(address.port); }
}
async function invoke(operation, payload = {}, requestId = '', view = '') {
  requireTime();
  const call = { version: 1, operation, account_id: account, view, request_id: requestId, payload_json: JSON.stringify(payload) };
  const body = JSON.stringify(call); assert.ok(Buffer.byteLength(body) <= 2 * 1024 * 1024, 'encoded Forge request exceeds the existing ingress cap');
  const response = await fetch(origin + '/api/v1/forge/call', { method: 'POST', signal: AbortSignal.timeout(Math.max(1, Math.min(60000, deadline - Date.now()))),
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body });
  const parts = []; let bytes = 0;
  for await (const part of response.body) { bytes += part.length; assert.ok(bytes <= MAX_RESPONSE); parts.push(part); }
  const reply = JSON.parse(Buffer.concat(parts).toString('utf8'));
  if (response.status !== 200) throw new Error(`Forge operation ${operation} refused: HTTP ${response.status}, code ${reply.error?.code ?? 'invalid_error'}`);
  exact(reply, ['version', 'operation', 'account_id', 'view', 'request_id', 'state', 'result_json']);
  for (const field of ['version', 'operation', 'account_id', 'view', 'request_id']) assert.equal(reply[field], call[field], 'Forge response binding mismatch');
  assert.ok(typeof reply.state === 'string' && typeof reply.result_json === 'string');
  return { state: reply.state, value: JSON.parse(reply.result_json) };
}
let summary;
try {
  await restart(); stage = 'provision';
  const provisioned = (await invoke(1, {}, 'snapshot-fit-provision')).value;
  const begin = { root: { kind: 3, id: selected.head },
    source: { repository_id: manifest.repository_id, reference: 'main', generation: selected.generation, head: selected.head },
    destination: { generation: 1, head: Buffer.from(provisioned.main_revision).toString('hex') } };
  stage = 'begin'; const begun = (await invoke(40, begin, 'snapshot-fit-begin')).value;
  const importId = begun.import_id; assert.match(importId, /^[0-9a-f-]{36}$/);
  async function put(object) {
    const bytes = await privateFile(path.join(snapshot, object.file), MAX_OBJECT_BYTES, object.size_bytes);
    const result = (await invoke(41, { import_id: importId,
      descriptor: { kind: object.kind, id: object.id, size_bytes: object.size_bytes, format_version: object.format_version }, data_base64: bytes.toString('base64') },
      `io-${importId}-${object.kind}-${object.id}`)).value;
    assert.equal(result.import_id, importId); assert.equal(result.id, object.id); assert.equal(result.kind, object.kind);
    assert.equal(result.size_bytes, object.size_bytes); assert.equal(result.state, 'ready');
  }
  stage = 'upload'; const middle = Math.max(1, Math.floor(manifest.objects.length / 2));
  for (const [first, last] of [[0, middle], [middle, manifest.objects.length]]) {
    for (let offset = first; offset < last; offset += 8) {
      await Promise.all(manifest.objects.slice(offset, Math.min(offset + 8, last)).map(put));
      uploaded = Math.min(offset + 8, last);
      if (uploaded % 256 < 8) console.error(JSON.stringify({ stage, objects: uploaded, total: manifest.objects.length }));
    }
    if (first === 0) {
      await restart(); assert.deepEqual((await invoke(40, begin, 'snapshot-fit-begin')).value, begun);
      await put(manifest.objects[middle - 1]); // Same immutable request after restart, not a second intent.
    }
  }
  stage = 'closure'; let status = (await invoke(43, { import_id: importId })).value.status;
  let restartedProgress = false;
  for (; progressRounds < 4096 && !status.complete; progressRounds++) {
    const payload = { import_id: importId, expected_generation: status.generation };
    status = (await invoke(42, payload, `ia-${importId}-${status.generation}`)).value.status;
    if (status.error !== null) throw new Error(`native closure refused corpus shape: ${status.error}`);
    if (!restartedProgress) {
      await restart(); const resumed = (await invoke(43, { import_id: importId })).value;
      assert.deepEqual(resumed.status, status, 'closure frontier must survive actual Worker restart'); status = resumed.status; restartedProgress = true;
    }
    if (progressRounds % 32 === 0) console.error(JSON.stringify({ stage, rounds: progressRounds, phase: status.phase, counts: status.counts }));
  }
  assert.ok(restartedProgress, 'this real corpus must exercise resumable preparation');
  assert.equal(status.phase, 'ready'); assert.equal(status.complete, true);
  assert.equal(status.counts.objects, report.unique_objects); assert.equal(status.counts.checked_objects, report.unique_objects);
  assert.equal(status.counts.bytes, report.unique_bytes); assert.equal(status.counts.edges, report.reference_edges);
  assert.equal(status.counts.contexts, status.counts.checked_contexts); assert.equal(status.counts.obligations, status.counts.checked_obligations);
  stage = 'publish'; const finish = { import_id: importId, expected_generation: status.generation };
  const receipt = (await invoke(44, finish, 'snapshot-fit-finish')).value;
  assert.equal(receipt.bootstrap_sealed, true); assert.equal(receipt.main.head, selected.head); assert.equal(receipt.main.generation, 2);
  await restart(); assert.deepEqual((await invoke(44, finish, 'snapshot-fit-finish')).value, receipt);
  const accountState = (await invoke(2)).value; assert.equal(Buffer.from(accountState.main_revision).toString('hex'), selected.head); assert.equal(accountState.main_generation, 2);
  // Independent byte equality of every retained canonical COMMIT, including
  // all original ancestors/predecessors. This is test-owned R2 inspection, not
  // a new public canonical-object grant or a JavaScript object decoder.
  stage = 'retained-ancestry'; const commits = new Map(manifest.objects.filter(object => object.kind === 3).map(object => [object.id, object]));
  const observed = new Set(), bucket = await mf.getR2Bucket('OBJECTS'); let cursor;
  do {
    const page = await bucket.list({ prefix: `native/v2/${account}/3/`, limit: 256, include: ['customMetadata'], ...(cursor ? { cursor } : {}) });
    for (const object of page.objects) {
      const descriptor = commits.get(object.customMetadata?.id); if (!descriptor) continue;
      assert.equal(object.customMetadata.account, account); assert.equal(object.customMetadata.kind, '3'); assert.equal(object.size, descriptor.size_bytes);
      assert.ok(!observed.has(descriptor.id), 'unexpected duplicate canonical COMMIT incarnation');
      const stored = await bucket.get(object.key); assert.ok(stored);
      const original = await privateFile(path.join(snapshot, descriptor.file), MAX_OBJECT_BYTES, descriptor.size_bytes);
      assert.ok(Buffer.from(await stored.arrayBuffer()).equals(original), 'retained original canonical COMMIT bytes differ'); observed.add(descriptor.id);
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  assert.equal(observed.size, commits.size, 'every original ancestor COMMIT remains in destination custody');
  summary = { schema_version: 1, kind: 'immutable_snapshot_protocol_fit', source_writes_paused: false, deployed_https: false, authority_cutover: false,
    selected: { reference: 'main', generation: selected.generation, head: selected.head }, objects: report.unique_objects, bytes: report.unique_bytes,
    by_kind: totals, parent_edges: report.commit_parent_edges, predecessor_edges: report.commit_predecessor_edges,
    closure: status, bootstrap: receipt, worker_restarts: { during_upload: true, during_closure: true, after_publish: true }, retained_original_commits: observed.size };
  complete = true;
} finally {
  if (mf) await mf.dispose();
  if (complete) await rm(temporary, { recursive: true, force: true });
  else console.error(`Immutable snapshot fit failed at ${stage}; disposable Worker evidence retained at ${temporary}`);
}
const artifacts = process.env.TT_FORGE_IMPORT_FIT_ARTIFACTS_DIR;
if (artifacts) {
  assert.ok(path.isAbsolute(artifacts)); await privateDirectory(artifacts);
  await writeFile(path.join(artifacts, 'import-fit.json'), JSON.stringify(summary, null, 2), { mode: 0o600, flag: 'wx' });
}
console.log(JSON.stringify(summary));
