// Test-only loopback browser acceptance. Uses declared production assets and
// real workerd/SQLite/R2; source seeding is explicitly a test harness, not an
// import or deployment-authority proof. No operator credentials are consumed.
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Miniflare } from 'miniflare';

const directory = fileURLToPath(new URL('../', import.meta.url));
const fixtureBinary = process.argv[2];
const args = process.argv.slice(3);
if (!fixtureBinary || args.length !== 2 || args[0] !== '--receipt') throw new Error('declared fixture and --receipt path required');
const receiptPath = resolve(args[1]);
const source = JSON.parse(execFileSync(resolve(fixtureBinary), [], { encoding: 'utf8', timeout: 30_000,
  maxBuffer: 16 * 1024 * 1024 })).search;
const temporary = await mkdtemp(join(tmpdir(), 'tinytree-browser-'));
const token = 'public-browser-acceptance-fixture-not-a-secret';
const config = [{ principal: source.owner, token_sha256: createHash('sha256').update(token).digest('hex'),
  accounts: [source.account_id], operator: true, kind: 'forge' }];
const mf = new Miniflare({
  host: '127.0.0.1', port: 0,
  modules: [{ type: 'ESModule', path: resolve(directory, 'projection-harness/worker.mjs') },
    { type: 'CompiledWasm', path: resolve(directory, 'projection-harness/core.wasm') }],
  compatibilityDate: '2026-07-08',
  assets: { directory: resolve(directory, 'release/ui-assets'), binding: 'ASSETS',
    routerConfig: { invoke_user_worker_ahead_of_assets: true, has_user_worker: true },
    assetConfig: { html_handling: 'none', not_found_handling: 'none' } },
  durableObjects: { REPOSITORIES: { className: 'AccountRepository', useSQLite: true } },
  r2Buckets: ['OBJECTS'], durableObjectsPersist: join(temporary, 'do'), r2Persist: join(temporary, 'r2'),
  bindings: { PRINCIPALS_JSON: JSON.stringify(config), FORGE_BACKEND: 'enabled', EXPERIMENTAL_RAW_BACKEND: 'disabled' },
});
let stopped = false;
async function stop() {
  if (stopped) return; stopped = true;
  clearTimeout(timer);
  await mf.dispose(); await rm(temporary, { recursive: true, force: true });
}
const timer = setTimeout(() => void stop(), 15 * 60 * 1000);
process.once('SIGINT', () => void stop()); process.once('SIGTERM', () => void stop());
try {
  const endpoint = (await mf.ready).origin;
  const call = async (operation, payload = {}, request_id = '') => {
    const response = await mf.dispatchFetch(`${endpoint}/api/v1/forge/call`, { method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ version: 1, operation, account_id: source.account_id, view: 'public', request_id,
        payload_json: JSON.stringify(payload) }) });
    const envelope = await response.json();
    if (response.status !== 200) throw new Error(`fixture Forge setup failed (${response.status})`);
    return JSON.parse(envelope.result_json);
  };
  await call(1, {}, 'browser-provision');
  const seeded = await mf.dispatchFetch(`${endpoint}/__fixture/seed?account=${source.account_id}`, { method: 'POST',
    body: JSON.stringify({ objects: source.objects, root: source.root }) });
  if (seeded.status !== 200) throw new Error(`fixture canonical seed failed (${seeded.status})`);
  const published = await call(4, { spec: source.spec, expected_epoch: 1, make_default: true, follow_main: false }, 'browser-public');
  // This independent named view reaches only the source's excluded subtree.
  const lockedSpec = { ...source.spec, name: 'locked', root_prefix: 'private', include_prefixes: [''],
    exclude_prefixes: [], visibility: { kind: 'members' } };
  // The envelope view must match the publication's name.
  const lockedResponse = await mf.dispatchFetch(`${endpoint}/api/v1/forge/call`, { method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ version: 1, operation: 4, account_id: source.account_id, view: 'locked',
      request_id: 'browser-locked', payload_json: JSON.stringify({ spec: lockedSpec, expected_epoch: 2,
        make_default: false, follow_main: false }) }) });
  if (lockedResponse.status !== 200) throw new Error(`fixture locked view setup failed (${lockedResponse.status})`);
  const hex = value => Buffer.from(value).toString('hex');
  const receipt = { schema_version: 1, state: 'ready', endpoint, account_id: source.account_id,
    public_url: `${endpoint}/ui/?mode=forge&account=${source.account_id}&view=public`,
    pinned_public_url: `${endpoint}/ui/?mode=forge&account=${source.account_id}&view=public&version=${hex(published.view_version)}`,
    locked_url: `${endpoint}/ui/?mode=forge&account=${source.account_id}&view=locked`,
    public_fixture_token: token, search_query: 'needle', expected_hit_count: source.expected_hits.length,
    expected_readme: source.expected_snapshot.readme, private_path: 'secret.txt', current_policy_epoch: 3,
    public_spec: source.spec, maximum_lifetime_seconds: 900,
    proof_scope: 'loopback actual browser only; no provider deployment or canonical-source cutover' };
  await writeFile(receiptPath, JSON.stringify(receipt, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  console.log(JSON.stringify({ state: 'ready', endpoint, receipt: receiptPath }));
} catch (error) { await stop(); throw error; }
