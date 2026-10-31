import assert from 'node:assert/strict';
import test from 'node:test';
import { HealthFailure, probeOnce, runSamples } from './forge-health.mjs';

const config = {
  origin: 'https://forge.example.dev', account: 'tinytree', view: 'public',
  readmePath: 'README.md', searchPath: 'src/guide.md', searchQuery: 'SaaS',
  requestTimeoutMs: 100, probeTimeoutMs: 1000,
};
const viewVersion = Array(32).fill(1);
const revision = Array(32).fill(2);
const baseIdentity = { account_id: 'tinytree', view: 'public', view_version: viewVersion,
  revision, policy_epoch: 7, root_prefix: '' };

function response(value, status = 200, headers = {}) {
  return new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json', ...headers } });
}

function transport({ alter = () => {}, request } = {}) {
  return async (url, options) => {
    assert.equal(url, 'https://forge.example.dev/api/v1/forge/call');
    assert.equal(options.method, 'POST');
    assert.equal(options.redirect, 'manual');
    assert.equal(options.headers.authorization, undefined, 'public probe must not send a bearer token');
    const call = JSON.parse(options.body);
    assert.equal(call.version, 1);
    assert.equal(call.account_id, 'tinytree');
    assert.equal(call.view, 'public');
    assert.equal(call.request_id, '');
    const payload = JSON.parse(call.payload_json);
    request?.(call, payload);
    const result = call.operation === 6 ? {
      ...baseIdentity, complete: true, paths: ['README.md', 'src/guide.md', 'binary.dat', 'oversized.dat'],
      readme: { path: 'README.md', content: '# SaaS\nPublic guide\n' }, readme_error: null,
      projected_commit: Array(32).fill(3), projected_tree: Array(32).fill(4),
    } : call.operation === 7 ? {
      ...baseIdentity, path: 'README.md', content: '# SaaS\nPublic guide\n',
    } : {
      ...baseIdentity, matches: [{ path: 'src/guide.md', line: 1, column: 1, preview: 'SaaS guide' }],
      complete: true, cursor: null, skipped_files: { binary: 0, oversized: 0 },
    };
    const altered = alter(call, result);
    if (altered instanceof Response) return altered;
    return response({ version: 1, operation: call.operation, account_id: 'tinytree', view: 'public',
      request_id: '', state: 'complete', result_json: JSON.stringify(altered ?? result) });
  };
}

async function rejectsCode(promise, stage, code) {
  await assert.rejects(promise, error => error instanceof HealthFailure
    && error.stage === stage && error.code === code);
}

test('public readiness binds ResolveView, README bytes, and a complete literal search hit', async () => {
  const calls = [];
  const result = await probeOnce(config, { fetchImpl: transport({ request: (call, payload) => calls.push({ call, payload }) }) });
  assert.equal(result.state, 'application_ready');
  assert.deepEqual(result.checks, { resolve: 'ready', readme: 'readable',
    search: { state: 'expected_match', pages: 1, coverage_complete: true, skipped_files: { binary: 0, oversized: 0 } } });
  assert.deepEqual(calls.map(item => item.call.operation), [6, 7, 8]);
  assert.equal(calls[0].payload && Object.keys(calls[0].payload).length, 0);
  assert.deepEqual(calls[1].payload, { version: viewVersion, path: 'README.md' });
  assert.deepEqual(calls[2].payload, { version: viewVersion, query: 'SaaS', regex: false, limit: 100 });
});

test('rejects a response bound to another account', async () => {
  const fetchImpl = transport({ alter: (call, result) => call.operation === 6
    ? { ...result, account_id: 'other-account' } : result });
  await rejectsCode(probeOnce(config, { fetchImpl }), 'resolve', 'identity_mismatch');
});

test('rejects a changed view version between resolve and README read', async () => {
  const fetchImpl = transport({ alter: (call, result) => call.operation === 7
    ? { ...result, view_version: Array(32).fill(9) } : result });
  await rejectsCode(probeOnce(config, { fetchImpl }), 'readme', 'view_binding_changed');
});

test('refuses redirects instead of following them', async () => {
  const fetchImpl = async () => new Response('', { status: 302, headers: { location: 'https://elsewhere.example/' } });
  await rejectsCode(probeOnce(config, { fetchImpl }), 'operation_6', 'redirect_refused');
});

test('rejects an empty README even when the service claims a complete view', async () => {
  const fetchImpl = transport({ alter: (call, result) => call.operation === 6
    ? { ...result, readme: { path: 'README.md', content: '' } } : result });
  await rejectsCode(probeOnce(config, { fetchImpl }), 'resolve', 'readme_unavailable');
});

test('an expected partial-page hit proves readiness without claiming full coverage', async () => {
  const fetchImpl = transport({ alter: (call, result) => call.operation === 8
    ? { ...result, complete: false, cursor: 'next-page', skipped_files: { binary: 1, oversized: 0 } } : result });
  const result = await probeOnce(config, { fetchImpl });
  assert.deepEqual(result.checks.search, { state: 'expected_match', pages: 1,
    coverage_complete: false, skipped_files: { binary: 1, oversized: 0 } });
});

test('continues the exact query and identity to find a later-page match', async () => {
  let page = 0;
  const queries = [];
  const fetchImpl = transport({
    request: (call, payload) => { if (call.operation === 8) queries.push(payload); },
    alter: (call, result) => {
      if (call.operation !== 8) return result;
      page++;
      return page === 1
        ? { ...result, matches: [], complete: false, cursor: 'next-page', skipped_files: { binary: 1, oversized: 0 } }
        : { ...result, complete: false, cursor: null, skipped_files: { binary: 1, oversized: 1 } };
    },
  });
  const result = await probeOnce(config, { fetchImpl });
  assert.deepEqual(queries, [
    { version: viewVersion, query: 'SaaS', regex: false, limit: 100 },
    { version: viewVersion, query: 'SaaS', regex: false, limit: 100, cursor: 'next-page' },
  ]);
  assert.deepEqual(result.checks.search, { state: 'expected_match', pages: 2,
    coverage_complete: false, skipped_files: { binary: 1, oversized: 1 } });
});

test('rejects falsely complete coverage with skipped files', async () => {
  const fetchImpl = transport({ alter: (call, result) => call.operation === 8
    ? { ...result, skipped_files: { binary: 1, oversized: 0 } } : result });
  await rejectsCode(probeOnce(config, { fetchImpl }), 'search', 'invalid_search_result');
});

test('rejects invalid zero-based search hit locations', async () => {
  const fetchImpl = transport({ alter: (call, result) => call.operation === 8
    ? { ...result, matches: [{ path: 'src/guide.md', line: 0, column: 1, preview: 'SaaS' }] } : result });
  await rejectsCode(probeOnce(config, { fetchImpl }), 'search', 'invalid_search_hit');
});

test('bounded pagination refuses endlessly incomplete search with no expected hit', async () => {
  let pages = 0;
  const fetchImpl = transport({ alter: (call, result) => {
    if (call.operation !== 8) return result;
    pages++;
    return { ...result, matches: [], complete: false, cursor: `page-${pages}` };
  } });
  await rejectsCode(probeOnce(config, { fetchImpl }), 'search', 'search_page_budget_exceeded');
  assert.equal(pages, 128);
});

test('a rejected response is aborted even when its body never completes', async () => {
  let signal;
  const fetchImpl = async (_url, options) => {
    signal = options.signal;
    return new Response(new ReadableStream({ start() {} }), { status: 302,
      headers: { location: 'https://elsewhere.example/' } });
  };
  await rejectsCode(probeOnce(config, { fetchImpl }), 'operation_6', 'redirect_refused');
  assert.equal(signal.aborted, true);
});

test('enforces a finite total deadline for a stalled request', async () => {
  const fetchImpl = (_url, { signal }) => new Promise((resolve, reject) => {
    signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
  });
  await rejectsCode(probeOnce({ ...config, requestTimeoutMs: 5, probeTimeoutMs: 10 }, { fetchImpl }),
    'deadline', 'request_timeout');
});

test('emits only open and recovery transition events across good, bad, good probes', async () => {
  let resolveCount = 0;
  const fetchImpl = transport({ alter: (call, result) => {
    if (call.operation === 6) {
      resolveCount++;
      if (resolveCount === 2) return response({ version: 1, operation: 6, account_id: 'wrong',
        view: 'public', request_id: '', state: 'complete', result_json: '{}' });
    }
    return result;
  } });
  const samples = [], events = [];
  const result = await runSamples({ config, count: 3, intervalSeconds: 60, sleep: async () => {},
    probe: value => probeOnce(value, { fetchImpl }), onSample: value => samples.push(value),
    onEvent: async value => events.push(value) });
  assert.equal(result.failures, 1);
  assert.deepEqual(samples.map(value => value.state), ['application_ready', 'application_unavailable', 'application_ready']);
  assert.deepEqual(events.map(value => value.event), ['alert_open', 'alert_recovered']);
  assert.equal(events[0].code, 'invalid_envelope');
  assert.equal(events[1].identity.view_version, Buffer.from(viewVersion).toString('hex'));
});
