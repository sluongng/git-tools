/** Bounded cross-file search over current-policy projection catalogs. Cursor
 * rows cache derived read pages, never canonical grants or retention authority. */
import { normalizeForgeSearchCommand, searchForgeFile, type Descriptor, type ForgeCall, type ForgeSearchCommand, type ObjectKey } from './core';
import type { ForgeViews, ResolvedView, ViewDescription } from './forge_views';
import type { ForgeProjections, ProjectionPath, ReadyProjection } from './forge_projection';
import { fail } from './protocol';

export interface ForgeSearchHost {
  accountId: string; principal: string | null; sql: SqlStorage;
  views: ForgeViews; projections: ForgeProjections;
  transactionSync<T>(callback: () => T): T;
  readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
  nowSeconds(): number;
}
export interface SearchHit { path: string; line: number; column: number; preview: string }
export interface SearchResponse extends ViewDescription {
  matches: SearchHit[]; complete: boolean; cursor: string | null;
  skipped_files: { binary: number; oversized: number };
}
interface Selection {
  view: ResolvedView; description: Readonly<ViewDescription>; ready: ReadyProjection;
  files: ProjectionPath[]; binding: string;
}
interface Page {
  token: string; account: string; principal_scope: string; binding: string;
  position_path: string | null; position_offset: number; skipped_binary: number; skipped_oversized: number;
  generation: number; expires_at: number; result_json: string | null; next_token: string | null; bytes: number;
}
interface Usage { account: string; rows: number; bytes: number }
interface FileResult {
  state: 'matches' | 'binary' | 'limit' | 'unavailable';
  matches: { line: number; column: number; preview: string }[];
  complete: boolean; next_offset: number | null; verified_bytes: number;
  usage: { input_bytes: number; work: number; output_bytes: number };
}
const MAX_FILES = 1024, MAX_FILE = 1024 * 1024, MAX_READS = 8, MAX_INPUT = 8 * 1024 * 1024;
const MAX_WORK = 64 * 1024 * 1024, MAX_MATCH_OUTPUT = 256 * 1024, MAX_RESPONSE = 2 * 1024 * 1024;
const MAX_ROWS = 256, MAX_BYTES = 32 * 1024 * 1024, TTL = 15 * 60;
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });
function safe(value: number, maximum = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(value) && value >= 0 && value <= maximum; }
function hex(value: number[]): string {
  if (!Array.isArray(value) || value.length !== 32 || !value.every(v => safe(v, 255))) fail(503, 'unavailable');
  return value.map(v => v.toString(16).padStart(2, '0')).join('');
}
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function base64(bytes: Uint8Array): string {
  let text = ''; for (let start = 0; start < bytes.length; start += 8192) text += String.fromCharCode(...bytes.subarray(start, start + 8192)); return btoa(text);
}
function encode(value: unknown, maximum: number): string {
  const text = JSON.stringify(value); if (encoder.encode(text).byteLength > maximum) fail(429, 'resource_exhausted'); return text;
}
export class ForgeSearch {
  private readonly account: string;
  private readonly principal: string | null;
  private readonly scope: string;
  constructor(private readonly host: ForgeSearchHost) {
    this.account = host.accountId; this.principal = host.principal;
    if (!/^[a-z0-9-]{1,64}$/.test(this.account) || (this.principal !== null && (this.principal.length === 0
        || encoder.encode(this.principal).byteLength > 128 || decoder.decode(encoder.encode(this.principal)) !== this.principal
        || /[\u0000-\u001f\u007f-\u009f]/u.test(this.principal)))) fail(400, 'invalid_argument');
    // No nullable unique-key ambiguity and no synthetic anonymous principal.
    this.scope = JSON.stringify(this.principal === null ? ['anonymous'] : ['principal', this.principal]);
    for (const statement of [
      'CREATE TABLE IF NOT EXISTS forge_search_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,rows INTEGER NOT NULL,bytes INTEGER NOT NULL)',
      'CREATE TABLE IF NOT EXISTS forge_search_pages(token TEXT PRIMARY KEY,account TEXT NOT NULL,principal_scope TEXT NOT NULL,binding TEXT NOT NULL,position_path TEXT,position_offset INTEGER NOT NULL,skipped_binary INTEGER NOT NULL,skipped_oversized INTEGER NOT NULL,generation INTEGER NOT NULL,expires_at INTEGER NOT NULL,result_json TEXT,next_token TEXT,bytes INTEGER NOT NULL)',
      'CREATE INDEX IF NOT EXISTS forge_search_expiry ON forge_search_pages(expires_at,token)',
    ]) host.sql.exec(statement);
    host.transactionSync(() => {
      host.sql.exec('INSERT OR IGNORE INTO forge_search_usage(singleton,account,rows,bytes) VALUES(1,?,0,0)', this.account); this.usage();
    });
  }
  private one<T>(query: string, ...bindings: (string | number | null)[]): T | undefined { return [...this.host.sql.exec(query, ...bindings)][0] as T | undefined; }
  private now(): number { const value = this.host.nowSeconds(); if (!safe(value, Number.MAX_SAFE_INTEGER - TTL)) fail(503, 'unavailable'); return value; }
  private usage(): Usage {
    const value = this.one<Usage>('SELECT * FROM forge_search_usage WHERE singleton=1');
    if (!value || value.account !== this.account || !safe(value.rows, MAX_ROWS) || !safe(value.bytes, MAX_BYTES)) fail(503, 'unavailable'); return value;
  }
  private clean(now: number): void {
    const expired = [...this.host.sql.exec('SELECT token,bytes FROM forge_search_pages WHERE expires_at<=? ORDER BY expires_at,token LIMIT 16', now)] as unknown as { token: string; bytes: number }[];
    for (const row of expired) {
      this.host.sql.exec('DELETE FROM forge_search_pages WHERE token=?', row.token);
      this.host.sql.exec('UPDATE forge_search_usage SET rows=rows-1,bytes=bytes-? WHERE singleton=1', row.bytes);
    }
  }
  private capture(call: ForgeCall, command: ForgeSearchCommand): Selection {
    const view = this.host.views.resolveInTx(call.view, command.version ?? undefined); this.host.views.checkInTx(view);
    const d = view.description; const ready = this.host.projections.readyInTx(view); if (!ready) fail(400, 'failed_precondition');
    if (d.account_id !== this.account || !safe(d.policy_epoch) || d.policy_epoch < 1) fail(503, 'unavailable');
    const paths = this.host.projections.pathsInTx(view);
    if (paths.length > MAX_FILES || paths.reduce((n, file) => n + encoder.encode(file.path).byteLength, 0) > 1024 * 1024) fail(429, 'resource_exhausted');
    // Preserve authoritative SQL binary order; stripping a fixed root prefix
    // preserves this order. Never locale-sort names or follow Symlink entries.
    const files = paths.filter(file => file.descriptor.kind === 1);
    const binding = encode([1, this.account, this.scope, d.view, hex(d.view_version), hex(d.revision), d.policy_epoch,
      d.root_prefix, ready.catalog_id, hex(ready.binding.projected_commit), hex(ready.binding.projected_tree), command.fingerprint], 16 * 1024);
    return { view, description: d, ready, files, binding };
  }
  private check(selection: Selection, file?: ProjectionPath): void {
    this.host.views.checkInTx(selection.view);
    const ready = this.host.projections.readyInTx(selection.view);
    if (!ready || ready.catalog_id !== selection.ready.catalog_id
        || hex(ready.binding.projected_commit) !== hex(selection.ready.binding.projected_commit)
        || hex(ready.binding.projected_tree) !== hex(selection.ready.binding.projected_tree)) fail(409, 'aborted');
    if (file) {
      const decisions = this.host.views.pathsAllowedInTx(selection.view, [file.canonical_path]);
      if (decisions.paths.length !== 1 || decisions.paths[0].allowed !== true) fail(403, 'permission_denied');
      const member = this.host.projections.memberInTx(selection.view, file.descriptor);
      if (member.projection.catalog_id !== selection.ready.catalog_id || !same(member.descriptor, file.descriptor)) fail(409, 'aborted');
    }
  }
  private measure(row: Page): number {
    // Conservative encoded-row bound, including escaped cached-result text and
    // room for this byte-count field. No SQLite row approaches an unbounded blob.
    const { bytes: _bytes, ...value } = row; return encoder.encode(JSON.stringify(value)).byteLength + 32;
  }
  private fresh(selection: Selection, path: string | null, offset: number, binary: number, oversized: number): Page {
    const page: Page = { token: crypto.randomUUID(), account: this.account, principal_scope: this.scope, binding: selection.binding,
      position_path: path, position_offset: offset, skipped_binary: binary, skipped_oversized: oversized, generation: 0,
      expires_at: this.now() + TTL, result_json: null, next_token: null, bytes: 0 };
    page.bytes = this.measure(page); return page;
  }
  private insert(row: Page): void {
    this.host.sql.exec('INSERT INTO forge_search_pages(token,account,principal_scope,binding,position_path,position_offset,skipped_binary,skipped_oversized,generation,expires_at,result_json,next_token,bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
      row.token, row.account, row.principal_scope, row.binding, row.position_path, row.position_offset, row.skipped_binary, row.skipped_oversized, row.generation, row.expires_at, row.result_json, row.next_token, row.bytes);
  }
  private load(token: string, selection: Selection): Page {
    this.check(selection);
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(token)) fail(400, 'invalid_argument');
    const row = this.one<Page>('SELECT * FROM forge_search_pages WHERE token=?', token);
    if (!row || row.expires_at <= this.now()) fail(409, 'aborted');
    if (row.account !== this.account || row.principal_scope !== this.scope) fail(403, 'permission_denied');
    if (row.binding !== selection.binding) fail(409, 'aborted');
    if (!safe(row.position_offset, MAX_FILE) || !safe(row.skipped_binary, MAX_FILES) || !safe(row.skipped_oversized, MAX_FILES)
        || row.skipped_binary + row.skipped_oversized > selection.files.length || !safe(row.expires_at) || !safe(row.generation, 1)
        || (row.result_json === null ? row.generation !== 0 : row.generation !== 1)
        || !safe(row.bytes, MAX_RESPONSE) || (row.result_json !== null && encoder.encode(row.result_json).byteLength > MAX_RESPONSE)) fail(503, 'unavailable');
    return row;
  }
  private cached(row: Page): SearchResponse {
    if (row.result_json === null || row.generation !== 1) fail(503, 'unavailable');
    try { return JSON.parse(row.result_json) as SearchResponse; } catch { fail(503, 'unavailable'); }
  }
  async handle(call: ForgeCall): Promise<{ state: 'complete'; result: SearchResponse }> {
    if (call.account_id !== this.account) fail(403, 'permission_denied');
    let command: ForgeSearchCommand;
    // Shared normalizer compiles the actual Pattern even for empty projections.
    try { command = normalizeForgeSearchCommand({ account_id: call.account_id, operation: call.operation, view: call.view, payload_json: call.payload_json }); }
    catch { fail(400, 'invalid_argument'); }
    if (command.operation !== 8 || !/^[0-9a-f]{64}$/.test(command.fingerprint) || !safe(command.limit, 100) || command.limit < 1) fail(400, 'invalid_argument');
    const start = this.host.transactionSync(() => {
      const selection = this.capture(call, command); const now = this.now(); this.clean(now);
      if (command.cursor !== null) return { selection, row: this.load(command.cursor, selection) };
      const row = this.fresh(selection, selection.files[0]?.path ?? null, 0, 0, 0); const usage = this.usage();
      if (usage.rows >= MAX_ROWS || usage.bytes + row.bytes > MAX_BYTES || row.bytes > MAX_RESPONSE) fail(429, 'resource_exhausted');
      this.insert(row); this.host.sql.exec('UPDATE forge_search_usage SET rows=rows+1,bytes=bytes+? WHERE singleton=1', row.bytes);
      return { selection, row };
    });
    if (start.row.result_json !== null) return { state: 'complete', result: this.cached(start.row) };
    try {
      const result = await this.page(start.selection, start.row, command);
      this.host.transactionSync(() => this.check(start.selection));
      return { state: 'complete', result };
    }
    catch (error) { this.host.transactionSync(() => this.check(start.selection)); throw error; }
  }
  private async page(selection: Selection, input: Page, command: ForgeSearchCommand): Promise<SearchResponse> {
    let index = input.position_path === null ? selection.files.length : selection.files.findIndex(file => file.path === input.position_path);
    if (index < 0 || (input.position_path === null && input.position_offset !== 0)) fail(409, 'aborted');
    let offset = input.position_offset, binary = input.skipped_binary, oversized = input.skipped_oversized;
    let calls = 0, bytesRead = 0, work = 0, output = 0; const matches: SearchHit[] = [];
    const deadline = Date.now() + 5000; let conservative = false;
    const initialIndex = index, initialOffset = offset;
    while (index < selection.files.length) {
      const file = selection.files[index]; const expected = file.descriptor;
      this.host.transactionSync(() => this.check(selection, file));
      if (!safe(expected.size_bytes) || expected.kind !== 1 || expected.format_version !== 1 || !safe(offset, expected.size_bytes)) fail(503, 'unavailable');
      if (Date.now() >= deadline) break;
      if (expected.size_bytes > MAX_FILE) { oversized++; index++; offset = 0; continue; }
      if (calls >= MAX_READS || bytesRead + expected.size_bytes > MAX_INPUT || work >= MAX_WORK || output >= MAX_MATCH_OUTPUT) break;
      let loaded: { descriptor: Descriptor; bytes: Uint8Array };
      try { loaded = await this.host.readVerifiedObject(expected); }
      catch (error) { this.host.transactionSync(() => this.check(selection, file)); throw error; }
      // Authority precedes even storage-integrity/error handling after the await.
      this.host.transactionSync(() => this.check(selection, file));
      if (!same(expected, loaded.descriptor) || loaded.bytes.byteLength !== expected.size_bytes) fail(503, 'unavailable');
      calls++; bytesRead += expected.size_bytes;
      const remainingWork = MAX_WORK - work, remainingOutput = MAX_MATCH_OUTPUT - output;
      let result: FileResult;
      try {
        result = searchForgeFile({ schema: 1, descriptor: expected, bytes_base64: base64(loaded.bytes), query: command.query,
          regex: command.regex, limit: command.limit - matches.length, start_offset: offset,
          remaining_budget: { max_work: remainingWork, max_output_bytes: remainingOutput } }) as FileResult;
      } catch { this.host.transactionSync(() => this.check(selection, file)); fail(400, 'invalid_argument'); }
      if (!result || !Array.isArray(result.matches) || !result.usage || result.verified_bytes !== expected.size_bytes
          || !safe(result.usage.input_bytes, MAX_FILE) || !safe(result.usage.work) || !safe(result.usage.output_bytes)) fail(503, 'unavailable');
      if (result.state === 'unavailable') fail(503, 'unavailable');
      if (result.state === 'limit') {
        // A failed tightened attempt returns NO hits from this FILE. Earlier
        // files remain a real bounded page; this FILE/offset stays unconsumed.
        if (result.matches.length !== 0 || (remainingWork === MAX_WORK && remainingOutput === MAX_MATCH_OUTPUT)) fail(429, 'resource_exhausted');
        break;
      }
      if (result.usage.work > remainingWork || result.usage.output_bytes > remainingOutput) fail(503, 'unavailable');
      work += result.usage.work; output += result.usage.output_bytes;
      if (result.state === 'binary') {
        if (result.matches.length !== 0 || result.next_offset !== null) fail(503, 'unavailable');
        binary++; index++; offset = 0; continue;
      }
      if (result.state !== 'matches' || result.complete !== (result.next_offset === null) || result.matches.length > command.limit - matches.length
          || (result.next_offset !== null && (!safe(result.next_offset, expected.size_bytes) || result.next_offset <= offset))) fail(503, 'unavailable');
      for (const hit of result.matches) {
        if (!safe(hit.line, MAX_FILE + 1) || hit.line < 1 || !safe(hit.column, MAX_FILE + 1) || hit.column < 1 || typeof hit.preview !== 'string') fail(503, 'unavailable');
        matches.push({ path: file.path, line: hit.line, column: hit.column, preview: hit.preview });
      }
      if (result.next_offset !== null) offset = result.next_offset; else { index++; offset = 0; }
      if (matches.length >= command.limit) { conservative = true; break; }
    }
    const pending = index < selection.files.length || conservative;
    if (pending && index === initialIndex && offset === initialOffset && binary === input.skipped_binary && oversized === input.skipped_oversized && matches.length === 0) {
      // Never emit a perpetual same-position successor for an unprocessable
      // first FILE or a deadline that allowed no progress at all.
      fail(Date.now() >= deadline ? 504 : 429, Date.now() >= deadline ? 'deadline_exceeded' : 'resource_exhausted');
    }
    return this.host.transactionSync(() => {
      this.check(selection); this.clean(this.now());
      const current = this.load(input.token, selection);
      // Queueing is not the CAS guard. Concurrent work on the same immutable
      // input returns its first committed exact result/successor, never advances.
      if (current.result_json !== null) return this.cached(current);
      if (current.generation !== input.generation || current.position_path !== input.position_path || current.position_offset !== input.position_offset
          || current.skipped_binary !== input.skipped_binary || current.skipped_oversized !== input.skipped_oversized) fail(409, 'aborted');
      const successor = pending ? this.fresh(selection, selection.files[index]?.path ?? null, offset, binary, oversized) : null;
      const result: SearchResponse = { ...selection.description, matches, complete: !pending && binary === 0 && oversized === 0,
        cursor: successor?.token ?? null, skipped_files: { binary, oversized } };
      const text = encode(result, MAX_RESPONSE);
      const completed: Page = { ...current, result_json: text, next_token: successor?.token ?? null, generation: 1 };
      completed.bytes = this.measure(completed);
      const usage = this.usage(); const nextBytes = usage.bytes - current.bytes + completed.bytes + (successor?.bytes ?? 0);
      if (completed.bytes > MAX_RESPONSE || (successor && successor.bytes > MAX_RESPONSE) || usage.rows + (successor ? 1 : 0) > MAX_ROWS || nextBytes > MAX_BYTES) fail(429, 'resource_exhausted');
      if (successor) this.insert(successor);
      this.host.sql.exec('UPDATE forge_search_pages SET result_json=?,next_token=?,generation=1,bytes=? WHERE token=? AND generation=? AND result_json IS NULL', text, completed.next_token, completed.bytes, current.token, current.generation);
      this.host.sql.exec('UPDATE forge_search_usage SET rows=rows+?,bytes=? WHERE singleton=1', successor ? 1 : 0, nextBytes);
      return result;
    });
  }
}
