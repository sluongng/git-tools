// Browser I/O bridge. Rendering and local path ranking live in the WASM module.
const $ = (selector) => document.querySelector(selector);
const encoder = new TextEncoder();
const decoder = new TextDecoder();
let wasm;
let token = '';
let snapshot = null;
let suggestions = [];
let activeSuggestion = -1;
let generation = 0;
let requestSequence = 0;
let controller = null;
let requestTimer = null;
let searchSession = null;
const locationContext = new URLSearchParams(location.search);
let account = locationContext.get('account') || '';
const forgeOnly = locationContext.get('mode') === 'forge';
const accountMode = Boolean(account) || forgeOnly;
let pinnedVersion = locationContext.get('version');
const selectedView = () => $('#reference').value.trim();
const digestHex = (bytes) => Array.isArray(bytes) ? bytes.map((b) => b.toString(16).padStart(2, '0')).join('') : bytes;
const MAX_RESPONSE = 8 * 1024 * 1024;
const validId = value => typeof value === 'string' && /^[0-9a-f]{64}$/.test(value) && !/^0+$/.test(value);
const validNativeId = value => Array.isArray(value) && value.length === 32 && value.every(b => Number.isInteger(b) && b >= 0 && b <= 255) && value.some(b => b !== 0);
const safeCount = value => Number.isSafeInteger(value) && value >= 0;
const exact = (value, fields) => value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === fields.length && fields.every(field => Object.hasOwn(value, field));
async function boundedJson(response) {
  if (!response.body) throw new Error('The server returned no response body.');
  const reader = response.body.getReader(); const parts = []; let length = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read(); if (done) break;
      length += value.byteLength;
      if (length > MAX_RESPONSE) { await reader.cancel(); throw new Error('The response exceeded the browser byte limit.'); }
      parts.push(value);
    }
    const bytes = new Uint8Array(length); let offset = 0;
    for (const part of parts) { bytes.set(part, offset); offset += part.byteLength; }
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  } finally { reader.releaseLock(); }
}
function responseError(response, data) {
  const message = data?.error?.message || (typeof data?.error === 'string' ? data.error : data?.message) || `Request failed (${response.status}).`;
  const error = new Error(response.status === 401 ? 'An access token is required or the supplied token is invalid.' : message);
  error.status = response.status; error.requiresToken = response.status === 401; return error;
}
async function forge(operation, payload, request, view = selectedView()) {
  const request_id = crypto.randomUUID(); const selectedAccount = account;
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch('/api/v1/forge/call', {
    method: 'POST', headers,
    body: JSON.stringify({ version: 1, operation, account_id: selectedAccount, view, request_id, payload_json: JSON.stringify(payload) }),
    signal: request.signal, credentials: 'omit', redirect: 'error', cache: 'no-store',
  });
  const envelope = await boundedJson(response);
  if (!response.ok) throw responseError(response, envelope);
  if (!exact(envelope, ['version', 'operation', 'account_id', 'view', 'request_id', 'state', 'result_json']) || envelope.version !== 1 || envelope.account_id !== selectedAccount || envelope.operation !== operation || envelope.view !== view || envelope.request_id !== request_id || !['pending', 'complete'].includes(envelope.state) || typeof envelope.result_json !== 'string') throw new Error('Unexpected Forge response binding.');
  return { state: envelope.state, value: JSON.parse(envelope.result_json) };
}
function sameContext(data, view) {
  return data.account_id === account && data.view === view.view && digestHex(data.view_version) === digestHex(view.view_version) && digestHex(data.revision) === digestHex(view.revision) && data.policy_epoch === view.policy_epoch && data.root_prefix === view.root_prefix;
}
function checkContext(data, name, version) {
  if (data.account_id !== account || typeof data.view !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(data.view) || (name && data.view !== name) || !validNativeId(data.view_version) || !validNativeId(data.revision) || !safeCount(data.policy_epoch) || data.policy_epoch === 0 || typeof data.root_prefix !== 'string' || (version && digestHex(data.view_version) !== digestHex(version))) throw new Error('Unexpected immutable view identity.');
}
async function resolveView(request, name) {
  if (pinnedVersion && !validId(pinnedVersion)) throw new Error('Version must be an exact nonzero lowercase hex identity.');
  let version = pinnedVersion ? pinnedVersion.match(/../g).map(byte => parseInt(byte, 16)) : null;
  let progress = null; let selection = null;
  for (let step = 0; step < 512; step++) {
    if (Date.now() >= request.deadline) throw new Error('View preparation timed out. Reopen this pinned view to continue.');
    let reply;
    for (let retry = 0; ; retry++) {
      try { reply = await forge(6, { version, ...(progress ? { progress } : {}) }, request, name); break; }
      catch (error) {
        // Only retry an already pinned continuation, never a fresh/default
        // selection or a server authorization/protocol error.
        if (!progress || retry >= 2 || error.status || error.name === 'AbortError' || !(error instanceof TypeError)) throw error;
      }
    }
    const data = reply.value; checkContext(data, name, version);
    if (selection && !sameContext(data, selection)) { const error = new Error('View policy changed during preparation. Reopen the view explicitly.'); error.status = 409; throw error; }
    if (reply.state === 'complete') {
      if (!exact(data, ['account_id','view','view_version','revision','policy_epoch','root_prefix','projected_commit','projected_tree','paths','readme','readme_error','complete']) || data.complete !== true || !validNativeId(data.projected_commit) || !validNativeId(data.projected_tree) || !Array.isArray(data.paths) || data.paths.length > 10000 || data.paths.some(path => typeof path !== 'string') || data.paths.reduce((n, path) => n + encoder.encode(path).length, 0) > 1024 * 1024 || !(data.readme === null || (exact(data.readme, ['path','content']) && typeof data.readme.path === 'string' && typeof data.readme.content === 'string')) || !(data.readme_error === null || typeof data.readme_error === 'string')) throw new Error('The view snapshot is incomplete or invalid.');
      return data;
    }
    if (!exact(data, ['account_id','view','view_version','revision','policy_epoch','root_prefix','progress']) || !exact(data.progress, ['job_id','expected_generation']) || typeof data.progress.job_id !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(data.progress.job_id) || !safeCount(data.progress.expected_generation) || (progress && (progress.job_id !== data.progress.job_id || progress.expected_generation > data.progress.expected_generation))) throw new Error('Unexpected view preparation progress.');
    selection = data; version = data.view_version; progress = data.progress;
    status('Preparing this immutable view…');
    await new Promise((resolve, reject) => {
      const abort = () => { clearTimeout(timer); reject(new DOMException('Cancelled', 'AbortError')); };
      const timer = setTimeout(() => { request.signal.removeEventListener('abort', abort); resolve(); }, 10);
      if (request.signal.aborted) abort(); else request.signal.addEventListener('abort', abort, { once: true });
    });
  }
  throw new Error('View preparation reached its bounded work limit. Reopen the pinned view explicitly.');
}

function present(input) {
  const bytes = encoder.encode(JSON.stringify(input));
  const pointer = wasm.tt_alloc(bytes.length);
  try {
    new Uint8Array(wasm.memory.buffer, pointer, bytes.length).set(bytes);
    const output = wasm.tt_command(pointer, bytes.length);
    // A WASM call can grow memory; read from the current buffer afterward.
    return JSON.parse(decoder.decode(new Uint8Array(wasm.memory.buffer, output, wasm.tt_result_len())));
  } finally { wasm.tt_free(pointer, bytes.length); }
}

function status(message, error = false) {
  $('#status').textContent = message;
  $('#status').classList.toggle('error', error);
}

function empty(title, description) {
  const box = document.createElement('div');
  box.className = 'empty-state';
  const heading = document.createElement('h1');
  heading.textContent = title;
  const paragraph = document.createElement('p');
  paragraph.textContent = description;
  box.append(heading, paragraph);
  $('#document').replaceChildren(box);
}

async function api(action, body, signal) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(`/ui/api/${action}`, {
    method: 'POST', headers, body: JSON.stringify(body), signal,
    credentials: 'omit', redirect: 'error', cache: 'no-store',
  });
  const data = await boundedJson(response);
  if (!response.ok) throw responseError(response, data);
  return data;
}

function beginRequest() {
  controller?.abort();
  clearTimeout(requestTimer);
  controller = new AbortController();
  const activeController = controller;
  const deadline = Date.now() + 60000;
  requestTimer = setTimeout(() => activeController.abort(), 60000);
  return { sequence: ++requestSequence, revisionGeneration: generation, signal: controller.signal, deadline };
}
function current(request) { return request.sequence === requestSequence && request.revisionGeneration === generation; }

function showFailure(error, request, title) {
  if (!current(request)) return;
  if (error.name === 'AbortError') {
    if (Date.now() < request.deadline) return;
    error = new Error('The request timed out. Retry the exact selected view explicitly.');
  }
  clearTimeout(requestTimer);
  if ([401,403,409].includes(error.status)) resetSnapshot();
  if (error.requiresToken) $('#connection').open = true;
  status(error.message, true); empty(title, error.message);
  $('#search-pagination').hidden = true;
}

function resetSnapshot() {
  generation++;
  controller?.abort();
  requestSequence++;
  clearTimeout(requestTimer);
  snapshot = null; searchSession = null; suggestions = [];
  $('#suggestions').replaceChildren();
  $('#search-page-label').textContent = '';
  $('#search').value = '';
  $('#search-pagination').hidden = true;
  if (wasm) present({ op: 'manifest', paths: [] });
  $('#search').disabled = true;
  $('#search-submit').disabled = true;
  $('#revision').textContent = 'No revision selected'; $('#revision').title = '';
  $('#view-version').hidden = true; $('#view-version').textContent = ''; $('#view-version').title = '';
  $('#path-count').textContent = 'Open a reference to explore.';
  $('#connection-dot').classList.remove('connected');
  $('#back-to-readme').hidden = true;
  $('#document-path').textContent = 'README';
  $('#document').className = '';
  empty('Your repository, in view.', 'Open a reference to read its README and explore the code.');
  closeSuggestions();
}

function showReadme() {
  if (!snapshot) return;
  controller?.abort(); clearTimeout(requestTimer); requestSequence++;
  searchSession = null; $('#search-pagination').hidden = true;
  closeSuggestions();
  $('#back-to-readme').hidden = true;
  $('#document-path').textContent = snapshot.readme?.path || 'README';
  if (snapshot.readme) {
    $('#document').className = 'markdown';
    $('#document').innerHTML = present({ op: 'markdown', ...snapshot.readme }).html;
  } else if (snapshot.readme_error) {
    $('#document').className = '';
    empty('This README cannot be displayed.', snapshot.readme_error);
  } else {
    $('#document').className = '';
    empty('A little room for an introduction.', 'No root README was found at this revision. Use search to find files or explore the repository contents.');
  }
  status(snapshot.paths_complete ? '' : 'File suggestions cover part of this repository; the path limit was reached.');
}

async function openRepository(openCurrent = false) {
  if (accountMode) {
    const chosen = $('#account').value.trim();
    if (!/^[a-z0-9-]{1,64}$/.test(chosen)) {
      resetSnapshot(); status('Choose an account to open its public README.');
      empty('Choose a tree to explore.', 'Enter an account name, then open its public view.'); return;
    }
    if (chosen !== account || openCurrent) pinnedVersion = null;
    account = chosen;
  }
  const repository = accountMode ? account : $('#repository').value;
  const reference = $('#reference').value.trim();
  if (!repository || (!accountMode && !reference)) return;
  resetSnapshot();
  const request = beginRequest();
  status('Opening the reference and reading its README…');
  empty('Opening this tree…', 'The README and file suggestions are on their way.');
  try {
    const data = accountMode ? await resolveView(request, reference) : await api('snapshot', { repository, reference }, request.signal);
    if (accountMode) {
      data.paths_complete = data.complete; data.version = data.view_version; data.revision = digestHex(data.revision);
    }
    if (!current(request)) return;
    if (accountMode) {
      pinnedVersion = digestHex(data.version);
      const url = new URL(location.href); url.searchParams.set('account', account); url.searchParams.set('view', reference); url.searchParams.set('version', pinnedVersion);
      history.replaceState(null, '', url); document.title = `${account} · tinytree`;
    }
    snapshot = data;
    present({ op: 'manifest', paths: data.paths });
    $('#search').disabled = false;
    $('#search-submit').disabled = false;
    $('#view-version').hidden = !accountMode;
    if (accountMode) { $('#view-version').textContent = `View version ${digestHex(data.version).slice(0, 12)}`; $('#view-version').title = digestHex(data.version); }
    $('#revision').textContent = accountMode ? `Canonical revision ${data.revision.slice(0, 12)}` : `${data.revision.slice(0, 12)} · immutable revision`;
    $('#revision').title = data.revision;
    $('#path-count').textContent = `${data.paths.length.toLocaleString()}${data.paths_complete ? '' : '+'} files in view`;
    $('#connection-dot').classList.add('connected');
    showReadme();
  } catch (error) {
    showFailure(error, request, 'This tree is not open yet.');
  }
}

async function connect() {
  token = $('#token').value.trim();
  resetSnapshot();
  const request = beginRequest();
  status('Connecting…');
  try {
    if (accountMode) { $('#connection').open = false; await openRepository(); return; }
    const data = await api('repositories', {}, request.signal);
    if (!current(request)) return;
    const repositories = Array.isArray(data) ? data : data.repositories;
    const previous = $('#repository').value;
    $('#repository').replaceChildren(...repositories.map((name) => {
      const option = document.createElement('option'); option.value = name; option.textContent = name; return option;
    }));
    if (repositories.includes(previous)) $('#repository').value = previous;
    $('#connection').open = false;
    if (!repositories.length) {
      status('No repositories are available for this connection.');
      empty('No trees to explore yet.', 'This connection does not have access to any repositories.');
      return;
    }
    await openRepository();
  } catch (error) {
    if (!current(request) || error.name === 'AbortError') return;
    if (error.requiresToken) $('#connection').open = true;
    status(error.message, true);
  }
}

function closeSuggestions() {
  $('#suggestions').hidden = true;
  $('#search').setAttribute('aria-expanded', 'false');
  $('#search').removeAttribute('aria-activedescendant');
  activeSuggestion = -1;
}

function selectSuggestion(index) {
  activeSuggestion = index;
  for (const [position, item] of [...$('#suggestions').children].entries()) item.setAttribute('aria-selected', String(position === index));
  if (index >= 0) {
    $('#search').setAttribute('aria-activedescendant', `suggestion-${index}`);
    $('#suggestions').children[index]?.scrollIntoView({ block: 'nearest' });
  } else $('#search').removeAttribute('aria-activedescendant');
}

function updateSuggestions() {
  if (!snapshot) return;
  suggestions = present({ op: 'suggest', query: $('#search').value }).options;
  activeSuggestion = -1;
  $('#search').removeAttribute('aria-activedescendant');
  $('#suggestions').replaceChildren(...suggestions.map((option, index) => {
    const item = document.createElement('li');
    item.id = `suggestion-${index}`;
    item.setAttribute('role', 'option'); item.setAttribute('aria-selected', 'false');
    const icon = document.createElement('span'); icon.className = 'suggestion-icon'; icon.textContent = option.kind === 'file' ? '↳' : '⌕'; icon.setAttribute('aria-hidden', 'true');
    const label = document.createElement('span'); label.className = 'suggestion-label'; label.textContent = option.label;
    const detail = document.createElement('span'); detail.className = 'suggestion-detail'; detail.textContent = option.detail;
    item.append(icon, label, detail);
    item.addEventListener('pointerdown', (event) => { event.preventDefault(); activateSuggestion(index); });
    return item;
  }));
  $('#suggestions').hidden = suggestions.length === 0;
  $('#search').setAttribute('aria-expanded', String(suggestions.length > 0));
}

function activateSuggestion(index) {
  const option = suggestions[index];
  if (!option) return;
  closeSuggestions();
  if (option.kind === 'file') openFile(option.value);
  else searchContents(option.value);
}

async function searchContents(query) {
  if (!snapshot || !query.trim()) return;
  const session = { query, view: snapshot, limit: 100, regex: false, inputs: [null], page: 0, next: null };
  searchSession = session;
  await loadSearchPage(session, 0);
}
async function loadSearchPage(session, index) {
  if (searchSession !== session || !snapshot || index < 0 || index >= 64) return;
  closeSuggestions();
  const request = beginRequest(); const view = session.view;
  $('#document-path').textContent = 'Search'; $('#back-to-readme').hidden = false;
  $('#search-pagination').hidden = true;
  status('Searching repository contents…');
  $('#document').className = 'results'; empty('Following that thread…', 'Searching this exact immutable view.');
  try {
    let data;
    if (accountMode) {
      const reply = await forge(8, { version: view.version, query: session.query, regex: session.regex, limit: session.limit, cursor: session.inputs[index] }, request, view.view);
      data = reply.value;
      if (!sameContext(data, view)) { const error = new Error('Search view policy changed. Reopen the selected view.'); error.status = 409; throw error; }
      if (reply.state !== 'complete' || !exact(data, ['account_id','view','view_version','revision','policy_epoch','root_prefix','matches','complete','cursor','skipped_files']) || !sameContext(data, view) || !Array.isArray(data.matches) || data.matches.length > session.limit || typeof data.complete !== 'boolean' || !(data.cursor === null || (typeof data.cursor === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(data.cursor))) || (data.complete && data.cursor !== null) || !exact(data.skipped_files, ['binary','oversized']) || !safeCount(data.skipped_files.binary) || !safeCount(data.skipped_files.oversized) || data.matches.some(hit => !exact(hit, ['path','line','column','preview']) || typeof hit.path !== 'string' || typeof hit.preview !== 'string' || !safeCount(hit.line) || hit.line === 0 || !safeCount(hit.column) || hit.column === 0)) throw new Error('Unexpected search page or immutable view binding.');
    } else {
      data = await api('search', { repository: view.repository, revision: view.revision, query: session.query }, request.signal);
      data.cursor = null; data.skipped_files = { binary: 0, oversized: 0 };
    }
    if (!current(request) || searchSession !== session) return;
    session.page = index; session.next = data.cursor;
    $('#document').className = 'results';
    $('#document').innerHTML = present({ op: 'results', query: session.query, matches: data.matches, complete: data.complete, has_more: data.cursor !== null, skipped_files: data.skipped_files }).html;
    $('#search-pagination').hidden = !accountMode;
    $('#search-previous').disabled = index === 0;
    $('#search-next').disabled = data.cursor === null || index >= 63;
    $('#search-page-label').textContent = `Page ${index + 1}`;
    const skipped = data.skipped_files.binary + data.skipped_files.oversized;
    status(data.complete ? `${data.matches.length} matches on this page. Search complete.` : data.cursor !== null ? (index >= 63 ? 'Navigation limit reached. Refine the query; coverage remains partial.' : 'More results are available. Choose Next to continue this exact query.') : `Partial results. ${skipped ? `${data.skipped_files.binary} binary and ${data.skipped_files.oversized} oversized files were skipped.` : 'The scan limit was reached.'}`);
    $('#document').focus({ preventScroll: true });
  } catch (error) {
    showFailure(error, request, 'This search page is not available.');
  }
}

async function openFile(path, line = null, anchor = null) {
  if (!snapshot) return;
  closeSuggestions();
  searchSession = null; $('#search-pagination').hidden = true;
  const request = beginRequest();
  const view = snapshot;
  $('#document-path').textContent = path;
  $('#back-to-readme').hidden = false;
  status('Reading file…');
  $('#document').className = '';
  empty('Opening this file…', path);
  try {
    let data;
    if (accountMode) {
      const reply = await forge(7, { version: view.version, path }, request, view.view); data = reply.value;
      if (!sameContext(data, view)) { const error = new Error('File view policy changed. Reopen the selected view.'); error.status = 409; throw error; }
      if (reply.state !== 'complete' || !exact(data, ['account_id','view','view_version','revision','policy_epoch','root_prefix','path','content']) || !sameContext(data, view) || data.path !== path || typeof data.content !== 'string') throw new Error('Unexpected file/view response binding.');
    } else data = await api('file', { repository: view.repository, revision: view.revision, path }, request.signal);
    if (!current(request)) return;
    const markdown = !line && /\.(md|markdown)$/i.test(path);
    $('#document').className = markdown ? 'markdown' : 'source-document';
    $('#document').innerHTML = present(markdown ? { op: 'markdown', content: data.content, path } : { op: 'source', content: data.content, line }).html;
    status('');
    $('#document').focus({ preventScroll: true });
    if (line) $(`#line-${line}`)?.scrollIntoView({ block: 'center' });
    else if (anchor) {
      const target = document.getElementById(anchor);
      if (target && $('#document').contains(target)) target.scrollIntoView({ block: 'start' });
    }
  } catch (error) {
    showFailure(error, request, 'This file is not available.');
  }
}

$('#search-next').addEventListener('click', () => {
  const session = searchSession; if (!session?.next) return;
  session.inputs[session.page + 1] = session.next;
  loadSearchPage(session, session.page + 1);
});
$('#search-previous').addEventListener('click', () => {
  if (searchSession && searchSession.page > 0) loadSearchPage(searchSession, searchSession.page - 1);
});
for (const input of [$('#account'), $('#reference')]) input.addEventListener('change', () => {
  pinnedVersion = null; resetSnapshot();
  const url = new URL(location.href); url.searchParams.delete('version'); history.replaceState(null, '', url);
});

$('#repository-form').addEventListener('submit', (event) => { event.preventDefault(); if (wasm) openRepository(true); });
$('#connection-form').addEventListener('submit', (event) => { event.preventDefault(); if (wasm) connect(); });
$('#disconnect').addEventListener('click', () => {
  token = ''; $('#token').value = ''; resetSnapshot();
  status('Disconnected.'); empty('Your repository, in view.', 'Connect again to explore this server.');
});
$('#search-form').addEventListener('submit', (event) => {
  event.preventDefault();
  if (activeSuggestion >= 0) activateSuggestion(activeSuggestion);
  else searchContents($('#search').value);
});
$('#search').addEventListener('input', () => {
  const hadSearch = searchSession !== null;
  if (hadSearch) { controller?.abort(); clearTimeout(requestTimer); requestSequence++; }
  searchSession = null;
  $('#search-pagination').hidden = true; $('#search-page-label').textContent = '';
  $('#search-previous').disabled = true; $('#search-next').disabled = true;
  if (hadSearch) {
    $('#document').className = '';
    empty('The search query changed.', 'Submit the new query to search this same pinned view.');
    status('Query changed. Previous search cursors were cleared.');
  }
  updateSuggestions();
});
$('#search').addEventListener('focus', updateSuggestions);
$('#search').addEventListener('keydown', (event) => {
  if (event.key === 'Escape') { event.preventDefault(); closeSuggestions(); }
  else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
    event.preventDefault();
    if ($('#suggestions').hidden) updateSuggestions();
    if (!suggestions.length) return;
    const delta = event.key === 'ArrowDown' ? 1 : -1;
    selectSuggestion(activeSuggestion < 0 ? (delta > 0 ? 0 : suggestions.length - 1) : (activeSuggestion + delta + suggestions.length) % suggestions.length);
  }
});
$('#search-form').addEventListener('focusout', (event) => {
  if (!$('#search-form').contains(event.relatedTarget)) closeSuggestions();
});
document.addEventListener('pointerdown', (event) => { if (!$('#search-form').contains(event.target)) closeSuggestions(); });
document.addEventListener('keydown', (event) => {
  const editing = event.target.closest('input,textarea,select,[contenteditable="true"]');
  if (((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') || (event.key === '/' && !editing)) {
    if (!$('#search').disabled) { event.preventDefault(); $('#search').focus(); }
  }
});
for (const button of [$('#readme-home'), $('#back-to-readme')]) button.addEventListener('click', () => {
  if (accountMode && snapshot?.readme) openFile(snapshot.readme.path);
  else showReadme();
});
$('#document').addEventListener('click', (event) => {
  const match = event.target.closest('[data-path]');
  if (match) { openFile(match.dataset.path, Number(match.dataset.line) || null); return; }
  const link = event.target.closest('a[href^="#file:"]');
  if (link) {
    event.preventDefault();
    const [path, anchor] = link.getAttribute('href').slice(6).split('#');
    openFile(decodeURIComponent(path), null, anchor ? decodeURIComponent(anchor) : null);
  }
});

if (accountMode) {
  $('#repository-field').hidden = true; $('#account-field').hidden = false; $('#account').value = account;
  $('label[for=reference]').textContent = 'View';
  $('#reference').value = locationContext.get('view') || ''; $('#reference').placeholder = 'Default public view';
  $('#open-repository').title = 'Open the current selected view';
  document.title = account ? `${account} · tinytree` : 'tinytree · choose an account';
}

try {
  const response = await fetch('/ui/tinytree_ui.wasm', { credentials: 'omit', redirect: 'error' });
  if (!response.ok) throw new Error('The browser module could not be loaded.');
  const module = await WebAssembly.instantiateStreaming(response, {});
  wasm = module.instance.exports;
  await connect();
} catch (error) {
  status(`Cannot start the browser: ${error.message}`, true);
}
