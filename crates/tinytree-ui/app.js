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
const locationContext = new URLSearchParams(location.search);
const account = locationContext.get('account') || '';
const accountMode = Boolean(account);
const selectedView = () => $('#reference').value.trim();
const digestHex = (bytes) => Array.isArray(bytes) ? bytes.map((b) => b.toString(16).padStart(2, '0')).join('') : bytes;
async function forge(operation, payload, signal) {
  const envelope = await api('forge', { operation, account_id: account, view: selectedView(), request_id: crypto.randomUUID(), payload }, signal);
  if (envelope.schema_version !== 1 || envelope.account_id !== account || envelope.operation !== operation || envelope.view !== selectedView()) throw new Error('Unexpected view response.');
  return envelope.result;
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
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (response.status === 401) {
      const error = new Error('An access token is required. Connect to open this repository.');
      error.requiresToken = true;
      throw error;
    }
    throw new Error(data.error || data.message || `Request failed (${response.status}).`);
  }
  return data;
}

function beginRequest() {
  controller?.abort();
  controller = new AbortController();
  return { sequence: ++requestSequence, revisionGeneration: generation, signal: controller.signal };
}
function current(request) { return request.sequence === requestSequence && request.revisionGeneration === generation; }

function resetSnapshot() {
  generation++;
  controller?.abort();
  requestSequence++;
  snapshot = null;
  if (wasm) present({ op: 'manifest', paths: [] });
  $('#search').disabled = true;
  $('#search-submit').disabled = true;
  $('#revision').textContent = 'No revision selected';
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
  controller?.abort(); requestSequence++;
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

async function openRepository() {
  const repository = accountMode ? account : $('#repository').value;
  const reference = $('#reference').value.trim();
  if (!repository || (!accountMode && !reference)) return;
  resetSnapshot();
  const request = beginRequest();
  status('Opening the reference and reading its README…');
  empty('Opening this tree…', 'The README and file suggestions are on their way.');
  try {
    const data = accountMode ? await forge('resolve_view', {}, request.signal) : await api('snapshot', { repository, reference }, request.signal);
    if (accountMode) { data.paths_complete = data.complete; data.version = data.view_version; data.revision = digestHex(data.revision); }
    if (!current(request)) return;
    snapshot = data;
    present({ op: 'manifest', paths: data.paths });
    $('#search').disabled = false;
    $('#search-submit').disabled = false;
    $('#revision').textContent = `${data.revision.slice(0, 12)} · immutable revision`;
    $('#revision').title = data.revision;
    $('#path-count').textContent = `${data.paths.length.toLocaleString()}${data.paths_complete ? '' : '+'} files in view`;
    $('#connection-dot').classList.add('connected');
    showReadme();
  } catch (error) {
    if (!current(request) || error.name === 'AbortError') return;
    if (error.requiresToken) $('#connection').open = true;
    status(error.message, true);
    empty('This tree is not open yet.', 'Check the repository, reference, and connection, then try again.');
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
  closeSuggestions();
  const request = beginRequest();
  const view = snapshot;
  $('#document-path').textContent = 'Search';
  $('#back-to-readme').hidden = false;
  status('Searching repository contents…');
  $('#document').className = 'results';
  empty('Following that thread…', 'Searching file contents at the selected revision.');
  try {
    const data = accountMode ? await forge('search', { version: view.version, query, regex: false, limit: 100 }, request.signal) : await api('search', { repository: view.repository, revision: view.revision, query }, request.signal);
    if (!current(request)) return;
    $('#document').className = 'results';
    $('#document').innerHTML = present({ op: 'results', query, matches: data.matches, complete: data.complete }).html;
    status(data.complete ? `${data.matches.length} matches. Search complete.` : 'Partial results. A search limit was reached; more matches may exist.');
    $('#document').focus({ preventScroll: true });
  } catch (error) {
    if (!current(request) || error.name === 'AbortError') return;
    if (error.requiresToken) $('#connection').open = true;
    status(error.message, true);
  }
}

async function openFile(path, line = null, anchor = null) {
  if (!snapshot) return;
  closeSuggestions();
  const request = beginRequest();
  const view = snapshot;
  $('#document-path').textContent = path;
  $('#back-to-readme').hidden = false;
  status('Reading file…');
  $('#document').className = '';
  empty('Opening this file…', path);
  try {
    const data = accountMode ? await forge('read_file', { version: view.version, path }, request.signal) : await api('file', { repository: view.repository, revision: view.revision, path }, request.signal);
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
    if (!current(request) || error.name === 'AbortError') return;
    if (error.requiresToken) $('#connection').open = true;
    status(error.message, true);
  }
}

$('#repository-form').addEventListener('submit', (event) => { event.preventDefault(); if (wasm) openRepository(); });
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
$('#search').addEventListener('input', updateSuggestions);
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
  showReadme();
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
  const option = document.createElement('option'); option.value = account; option.textContent = account;
  $('#repository').replaceChildren(option); $('#repository').disabled = true;
  $('label[for=repository]').textContent = 'Account'; $('label[for=reference]').textContent = 'View';
  $('#reference').value = locationContext.get('view') || ''; $('#reference').placeholder = 'Default public view';
  document.title = `${account} · tinytree`;
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
