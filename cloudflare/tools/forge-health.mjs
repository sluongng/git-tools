#!/usr/bin/env node
// Bounded public Forge application-readiness probe. This is not a provider
// health check, browser acceptance, or proof that alerts have been observed.
import { open, lstat, realpath } from 'node:fs/promises';
import { constants as fsConstants } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { isIP } from 'node:net';

const ROUTE = '/api/v1/forge/call';
const MAX_BODY_BYTES = 8 * 1024 * 1024;
const MAX_RESOLVE_ROUNDS = 32;
const MAX_SEARCH_PAGES = 128;
const DEFAULT_REQUEST_TIMEOUT_MS = 5000;
const DEFAULT_PROBE_TIMEOUT_MS = 30000;
const MAX_PROBE_TIMEOUT_MS = 120000;
const MIN_INTERVAL_SECONDS = 60;
const MAX_COUNT = 1440;
const encoder = new TextEncoder();

export class HealthFailure extends Error {
  constructor(stage, code) {
    super(`${stage}:${code}`);
    this.name = 'HealthFailure';
    this.stage = stage;
    this.code = code;
  }
}

function fail(stage, code) { throw new HealthFailure(stage, code); }
function safeName(value, expression) { return typeof value === 'string' && expression.test(value); }

function publicHttpsOrigin(value) {
  let url;
  try { url = new URL(value); } catch { fail('arguments', 'invalid_origin'); }
  const host = url.hostname.toLowerCase().replace(/^\[|\]$/g, '');
  if (url.protocol !== 'https:' || url.username || url.password || url.pathname !== '/' || url.search || url.hash
      || !host || isIP(host) !== 0 || !host.includes('.')
      || host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local')
      || host.endsWith('.internal') || host.endsWith('.test') || host.endsWith('.invalid')) {
    fail('arguments', 'origin_must_be_public_https');
  }
  return url.origin;
}

function relativePath(value, field) {
  if (typeof value !== 'string' || value.length < 1 || encoder.encode(value).byteLength > 4096
      || value.startsWith('/') || value.includes('\\') || value.split('/').some(part => !part || part === '.' || part === '..')
      || /[\u0000-\u001f\u007f-\u009f]/u.test(value)) {
    fail('arguments', `invalid_${field}`);
  }
  return value;
}

function normalizeConfig(input) {
  const origin = publicHttpsOrigin(input.origin);
  if (!safeName(input.account, /^[a-z0-9-]{1,64}$/)) fail('arguments', 'invalid_account');
  if (!safeName(input.view, /^[A-Za-z0-9_.-]{1,128}$/)) fail('arguments', 'invalid_view');
  const readmePath = relativePath(input.readmePath, 'readme_path');
  const searchPath = relativePath(input.searchPath, 'search_path');
  if (typeof input.searchQuery !== 'string' || encoder.encode(input.searchQuery).byteLength < 1
      || encoder.encode(input.searchQuery).byteLength > 1024 || input.searchQuery.includes('\n')
      || input.searchQuery.includes('\r')) fail('arguments', 'invalid_search_query');
  const requestTimeoutMs = input.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  const probeTimeoutMs = input.probeTimeoutMs ?? DEFAULT_PROBE_TIMEOUT_MS;
  if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 1 || requestTimeoutMs > probeTimeoutMs
      || !Number.isSafeInteger(probeTimeoutMs) || probeTimeoutMs < 1 || probeTimeoutMs > MAX_PROBE_TIMEOUT_MS) {
    fail('arguments', 'invalid_timeout');
  }
  return { origin, account: input.account, view: input.view, readmePath, searchPath,
    searchQuery: input.searchQuery, requestTimeoutMs, probeTimeoutMs };
}

function byteId(value, stage, field) {
  if (!Array.isArray(value) || value.length !== 32
      || !value.every(byte => Number.isInteger(byte) && byte >= 0 && byte <= 255)
      || value.every(byte => byte === 0)) fail(stage, `invalid_${field}`);
  return value;
}

function validateIdentity(value, config, stage, expected = undefined) {
  if (!value || value.account_id !== config.account || value.view !== config.view) fail(stage, 'identity_mismatch');
  const version = byteId(value.view_version, stage, 'view_version');
  const revision = byteId(value.revision, stage, 'revision');
  if (!Number.isSafeInteger(value.policy_epoch) || value.policy_epoch < 1) fail(stage, 'invalid_policy_epoch');
  const identity = { view_version: Buffer.from(version).toString('hex'), revision: Buffer.from(revision).toString('hex'), policy_epoch: value.policy_epoch };
  if (expected && (identity.view_version !== expected.view_version || identity.revision !== expected.revision
      || identity.policy_epoch !== expected.policy_epoch)) fail(stage, 'view_binding_changed');
  return { version, identity };
}

async function boundedJson(response, stage) {
  if (response.status >= 300 && response.status < 400) fail(stage, 'redirect_refused');
  if (response.status !== 200) fail(stage, 'http_status');
  if (response.redirected === true) fail(stage, 'redirect_refused');
  const mediaType = (response.headers.get('content-type') ?? '').split(';', 1)[0].trim().toLowerCase();
  if (mediaType !== 'application/json') fail(stage, 'invalid_content_type');
  const declared = response.headers.get('content-length');
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > MAX_BODY_BYTES)) fail(stage, 'response_too_large');
  if (!response.body) fail(stage, 'empty_response');
  const reader = response.body.getReader();
  const chunks = []; let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > MAX_BODY_BYTES) { await reader.cancel(); fail(stage, 'response_too_large'); }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  let parsed;
  try { parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks, length))); }
  catch { fail(stage, 'invalid_json'); }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) fail(stage, 'invalid_json');
  return parsed;
}

function resultEnvelope(envelope, config, operation, stage) {
  const keys = ['version', 'operation', 'account_id', 'view', 'request_id', 'state', 'result_json'];
  if (!envelope || Object.keys(envelope).sort().join(',') !== keys.sort().join(',')
      || envelope.version !== 1 || envelope.operation !== operation || envelope.account_id !== config.account
      || envelope.view !== config.view || envelope.request_id !== ''
      || typeof envelope.state !== 'string' || typeof envelope.result_json !== 'string'
      || encoder.encode(envelope.result_json).byteLength > MAX_BODY_BYTES) fail(stage, 'invalid_envelope');
  let result;
  try { result = JSON.parse(envelope.result_json); } catch { fail(stage, 'invalid_result_json'); }
  if (!result || typeof result !== 'object' || Array.isArray(result)) fail(stage, 'invalid_result');
  return { state: envelope.state, result };
}

async function requestForge(config, operation, payload, deadline, fetchImpl, now) {
  const remaining = deadline - now();
  if (remaining <= 0) fail('deadline', 'probe_deadline_exceeded');
  const controller = new AbortController();
  const timeoutMs = Math.min(config.requestTimeoutMs, remaining);
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeoutMs);
  let response;
  try {
    response = await fetchImpl(`${config.origin}${ROUTE}`, {
      method: 'POST',
      redirect: 'manual',
      signal: controller.signal,
      headers: { accept: 'application/json', 'content-type': 'application/json' },
      body: JSON.stringify({ version: 1, operation, account_id: config.account, view: config.view,
        request_id: '', payload_json: JSON.stringify(payload) }),
    });
    const envelope = await boundedJson(response, `operation_${operation}`);
    return resultEnvelope(envelope, config, operation, `operation_${operation}`);
  } catch (error) {
    if (error instanceof HealthFailure) throw error;
    if (timedOut || controller.signal.aborted) fail('deadline', timeoutMs >= remaining ? 'probe_deadline_exceeded' : 'request_timeout');
    fail(`operation_${operation}`, 'transport_error');
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}

function identityOf(value, config, stage, expected) {
  const { version, identity } = validateIdentity(value, config, stage, expected);
  return { version, identity };
}

export async function probeOnce(input, { fetchImpl = globalThis.fetch, now = Date.now } = {}) {
  const config = normalizeConfig(input);
  if (typeof fetchImpl !== 'function') fail('arguments', 'fetch_unavailable');
  const deadline = now() + config.probeTimeoutMs;
  let selection;
  let progress;
  let snapshot;
  for (let round = 0; round < MAX_RESOLVE_ROUNDS; round++) {
    const call = await requestForge(config, 6, progress === undefined ? {} : { version: selection.version, progress }, deadline, fetchImpl, now);
    if (call.state !== 'pending' && call.state !== 'complete') fail('resolve', 'invalid_state');
    const current = identityOf(call.result, config, 'resolve', selection?.identity);
    if (!selection) selection = current;
    if (call.state === 'complete') {
      snapshot = call.result;
      break;
    }
    if (call.result.complete !== undefined || !call.result.progress || typeof call.result.progress !== 'object'
        || !safeName(call.result.progress.job_id, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
        || !Number.isSafeInteger(call.result.progress.expected_generation) || call.result.progress.expected_generation < 0) {
      fail('resolve', 'invalid_progress');
    }
    progress = call.result.progress;
  }
  if (!snapshot) fail('resolve', 'projection_not_ready');
  if (snapshot.complete !== true || !Array.isArray(snapshot.paths) || snapshot.paths.length > 10000
      || !snapshot.paths.includes(config.readmePath) || !snapshot.paths.includes(config.searchPath)) fail('resolve', 'incomplete_projection');
  if (snapshot.readme_error !== null || !snapshot.readme || snapshot.readme.path !== config.readmePath
      || typeof snapshot.readme.content !== 'string' || encoder.encode(snapshot.readme.content).byteLength === 0
      || encoder.encode(snapshot.readme.content).byteLength > 1024 * 1024) fail('resolve', 'readme_unavailable');

  const fileCall = await requestForge(config, 7, { version: selection.version, path: config.readmePath }, deadline, fetchImpl, now);
  if (fileCall.state !== 'complete') fail('readme', 'invalid_state');
  const file = fileCall.result;
  validateIdentity(file, config, 'readme', selection.identity);
  if (file.path !== config.readmePath || typeof file.content !== 'string'
      || encoder.encode(file.content).byteLength === 0 || encoder.encode(file.content).byteLength > 1024 * 1024
      || file.content !== snapshot.readme.content) fail('readme', 'readme_mismatch');

  let cursor = null, previousSkipped = { binary: 0, oversized: 0 }, search, searchPages = 0;
  for (let page = 0; page < MAX_SEARCH_PAGES; page++) {
    const payload = { version: selection.version, query: config.searchQuery, regex: false, limit: 100 };
    if (cursor !== null) payload.cursor = cursor;
    const searchCall = await requestForge(config, 8, payload, deadline, fetchImpl, now);
    if (searchCall.state !== 'complete') fail('search', 'invalid_state');
    search = searchCall.result;
    validateIdentity(search, config, 'search', selection.identity);
    if (typeof search.complete !== 'boolean' || (search.cursor !== null
        && (typeof search.cursor !== 'string' || search.cursor.length < 1 || search.cursor.length > 128
          || !/^[A-Za-z0-9_-]+$/.test(search.cursor)))
        || !Array.isArray(search.matches) || search.matches.length > 100 || !search.skipped_files
        || !Number.isSafeInteger(search.skipped_files.binary) || search.skipped_files.binary < 0
        || !Number.isSafeInteger(search.skipped_files.oversized) || search.skipped_files.oversized < 0
        || search.skipped_files.binary + search.skipped_files.oversized > snapshot.paths.length
        || search.skipped_files.binary < previousSkipped.binary || search.skipped_files.oversized < previousSkipped.oversized
        || (search.complete && (search.cursor !== null || search.skipped_files.binary !== 0
          || search.skipped_files.oversized !== 0))) fail('search', 'invalid_search_result');
    for (const hit of search.matches) {
      if (!hit || typeof hit.path !== 'string' || !Number.isSafeInteger(hit.line) || hit.line < 1
          || !Number.isSafeInteger(hit.column) || hit.column < 1 || typeof hit.preview !== 'string') {
        fail('search', 'invalid_search_hit');
      }
    }
    searchPages++;
    if (search.matches.some(hit => hit.path === config.searchPath && hit.preview.includes(config.searchQuery))) break;
    if (search.complete || search.cursor === null) fail('search', 'expected_match_missing');
    previousSkipped = search.skipped_files;
    cursor = search.cursor;
  }
  if (!search || !search.matches.some(hit => hit.path === config.searchPath && hit.preview.includes(config.searchQuery))) {
    fail('search', 'search_page_budget_exceeded');
  }
  return { state: 'application_ready', origin: config.origin, account_id: config.account, view: config.view,
    identity: selection.identity, checks: { resolve: 'ready', readme: 'readable',
      search: { state: 'expected_match', pages: searchPages, coverage_complete: search.complete,
        skipped_files: search.skipped_files } } };
}

export async function runSamples({ config, count, intervalSeconds = 0, probe = probeOnce,
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)), onSample = () => {}, onEvent = () => {} }) {
  if (!Number.isSafeInteger(count) || count < 1 || count > MAX_COUNT
      || (count > 1 && (!Number.isSafeInteger(intervalSeconds) || intervalSeconds < MIN_INTERVAL_SECONDS))) {
    fail('arguments', 'invalid_schedule');
  }
  const origin = publicHttpsOrigin(config.origin);
  let prior = null, failures = 0;
  for (let index = 0; index < count; index++) {
    if (index > 0) await sleep(intervalSeconds * 1000);
    let identity;
    let sample;
    try {
      const result = await probe(config);
      identity = result.identity;
      sample = { schema_version: 1, event: 'sample', sequence: index + 1, timestamp: new Date().toISOString(),
        origin, account_id: config.account, view: config.view, state: 'application_ready', checks: result.checks, identity };
    } catch (error) {
      failures++;
      const stage = error instanceof HealthFailure ? error.stage : 'probe';
      const code = error instanceof HealthFailure ? error.code : 'probe_failed';
      sample = { schema_version: 1, event: 'sample', sequence: index + 1, timestamp: new Date().toISOString(),
        origin, account_id: config.account, view: config.view, state: 'application_unavailable',
        checks: null, identity: null, error: { stage, code } };
    }
    onSample(sample);
    if (sample.state === 'application_unavailable' && prior !== 'application_unavailable') {
      await onEvent({ schema_version: 1, event: 'alert_open', timestamp: sample.timestamp, origin,
        account_id: config.account, view: config.view, failed_check: sample.error.stage, code: sample.error.code });
    } else if (sample.state === 'application_ready' && prior === 'application_unavailable') {
      await onEvent({ schema_version: 1, event: 'alert_recovered', timestamp: sample.timestamp, origin,
        account_id: config.account, view: config.view, identity: sample.identity });
    }
    prior = sample.state;
  }
  return { failures };
}

async function openEventLog(filename) {
  if (!path.isAbsolute(filename)) fail('arguments', 'event_path_must_be_absolute');
  const resolved = path.resolve(filename);
  const parent = path.dirname(resolved);
  const parentInfo = await lstat(parent).catch(() => null);
  if (!parentInfo || !parentInfo.isDirectory() || parentInfo.isSymbolicLink()
      || parentInfo.uid !== process.getuid() || (parentInfo.mode & 0o077) !== 0
      || await realpath(parent) !== parent) fail('events', 'event_directory_not_private');
  const existingInfo = await lstat(resolved).catch(error => error.code === 'ENOENT' ? null : fail('events', 'event_log_unavailable'));
  if (existingInfo && (!existingInfo.isFile() || existingInfo.isSymbolicLink())) fail('events', 'event_log_not_private');
  const flags = fsConstants.O_WRONLY | fsConstants.O_APPEND | fsConstants.O_CREAT | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK;
  let handle;
  try { handle = await open(resolved, flags | fsConstants.O_EXCL, 0o600); }
  catch (error) {
    if (error.code !== 'EEXIST') fail('events', 'event_log_unavailable');
    try { handle = await open(resolved, flags); } catch { fail('events', 'event_log_unavailable'); }
  }
  const info = await handle.stat();
  if (!info.isFile() || info.uid !== process.getuid() || (info.mode & 0o777) !== 0o600) {
    await handle.close(); fail('events', 'event_log_not_private');
  }
  const append = async event => {
    const bytes = encoder.encode(`${JSON.stringify(event)}\n`);
    await handle.writeFile(bytes);
    await handle.sync();
  };
  append.close = () => handle.close();
  return append;
}

function integer(value, name, min, max) {
  if (!/^\d+$/.test(value ?? '')) fail('arguments', `invalid_${name}`);
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < min || number > max) fail('arguments', `invalid_${name}`);
  return number;
}

export function parseArgs(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index];
    if (flag === '--help' || flag === '-h') return { help: true };
    if (!['--origin', '--account', '--view', '--readme-path', '--search-path', '--search-query',
      '--events', '--request-timeout-ms', '--probe-timeout-ms', '--once', '--interval-seconds', '--count'].includes(flag)) {
      fail('arguments', 'unknown_argument');
    }
    if (values.has(flag)) fail('arguments', 'duplicate_argument');
    if (flag === '--once') { values.set(flag, true); continue; }
    if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) fail('arguments', 'missing_argument_value');
    values.set(flag, argv[++index]);
  }
  for (const required of ['--origin', '--account', '--view', '--readme-path', '--search-path', '--search-query']) {
    if (!values.has(required)) fail('arguments', `missing_${required.slice(2).replaceAll('-', '_')}`);
  }
  const once = values.has('--once');
  const hasInterval = values.has('--interval-seconds'), hasCount = values.has('--count');
  if (once ? hasInterval || hasCount : !(hasInterval && hasCount)) fail('arguments', 'select_once_or_finite_schedule');
  const config = normalizeConfig({ origin: values.get('--origin'), account: values.get('--account'), view: values.get('--view'),
    readmePath: values.get('--readme-path'), searchPath: values.get('--search-path'), searchQuery: values.get('--search-query'),
    requestTimeoutMs: values.has('--request-timeout-ms') ? integer(values.get('--request-timeout-ms'), 'request_timeout_ms', 1, 30000) : undefined,
    probeTimeoutMs: values.has('--probe-timeout-ms') ? integer(values.get('--probe-timeout-ms'), 'probe_timeout_ms', 1, MAX_PROBE_TIMEOUT_MS) : undefined });
  if (once) return { config, count: 1, intervalSeconds: 0, events: values.get('--events') };
  return { config, count: integer(values.get('--count'), 'count', 1, MAX_COUNT),
    intervalSeconds: integer(values.get('--interval-seconds'), 'interval_seconds', MIN_INTERVAL_SECONDS, 86400),
    events: values.get('--events') };
}

export const HELP = `Usage:\n  node cloudflare/tools/forge-health.mjs --origin https://HOST --account SLUG --view VIEW --readme-path PATH --search-path PATH --search-query TEXT --once [--events FILE]\n  node cloudflare/tools/forge-health.mjs --origin https://HOST --account SLUG --view VIEW --readme-path PATH --search-path PATH --search-query TEXT --interval-seconds 60 --count 10 [--events FILE]\n\nProbes the public Forge API (ResolveView, ReadFile, Search). Events are local JSONL only; no external notification is sent.\n`;

async function main() {
  let eventWriter;
  try {
    const args = parseArgs(process.argv.slice(2));
    if (args.help) { process.stdout.write(HELP); return; }
    if (args.events) eventWriter = await openEventLog(args.events);
    const result = await runSamples({ ...args,
      onSample: sample => process.stdout.write(`${JSON.stringify(sample)}\n`),
      onEvent: async event => {
        process.stdout.write(`${JSON.stringify(event)}\n`);
        await eventWriter?.(event);
      },
    });
    if (result.failures > 0) process.exitCode = 1;
  } catch (error) {
    const stage = error instanceof HealthFailure ? error.stage : 'runtime';
    const code = error instanceof HealthFailure ? error.code : 'probe_failed';
    process.stderr.write(`${JSON.stringify({ schema_version: 1, event: 'fatal', state: 'application_unavailable', stage, code })}\n`);
    process.exitCode = 2;
  } finally {
    await eventWriter?.close?.();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  void main();
}
