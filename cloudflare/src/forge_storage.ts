/** Canonical availability with durable physical custody. This module grants no
 * ACL/root authority. Hosts authorize reads/publication and supply GC victims. */
import { inspect, type Descriptor, type Inspection, type ObjectKey } from './core';
import { fail, Failure } from './protocol';
export interface ForgeStorageHost {
 accountId: string; sql: SqlStorage; objects: R2Bucket;
 transactionSync<T>(callback: () => T): T;
 /** Trusted maintenance barrier/epoch participation; caller already holds tx. */
 mutationInTx?(mode: 'authoring' | 'maintenance'): void;
}
interface Catalog { size: number; inspection: string; visible: number }
interface Usage { account: string; objects: number; bytes: number; reservations: number }
interface PhysicalUsage { account: string; payloads: number; payload_bytes: number; attempt_rows: number; inspection_bytes: number }
interface Reservation { size: number; format: number }
interface Binding { generation: number; attempt_id: string | null }
interface Attempt { attempt_id: string; kind: number; id: string; size: number; inspection: string; locator: string; state: 'pending' | 'written' | 'retiring' | 'deleted'; etag: string | null; version: string | null }
export interface StorageIncarnation { descriptor: Descriptor; attempt_id: string; generation: number }
export interface StorageAttempt { attempt_id: string; descriptor: Descriptor; state: Attempt['state']; current: boolean }
export interface StorageAccounting { logical_objects: number; logical_bytes: number; reservations: number; physical_payloads: number; physical_bytes: number; attempt_rows: number }
interface Confirmation { descriptor: Descriptor; inspection: string; attempt_id: string; generation: number }
const MAX_OBJECTS = 32768, MAX_BYTES = 512 * 1024 * 1024, MAX_RESERVATIONS = 131072, MAX_ATTEMPTS = 131072, MAX_PENDING = 16;
const MAX_CUSTODY_METADATA = 64 * 1024 * 1024;
const MAX_FILE = 1024 * 1024, MAX_METADATA = 256 * 1024, MAX_INSPECTION = 1024 * 1024;
const encoder = new TextEncoder(), decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });
function safe(n: number, max = Number.MAX_SAFE_INTEGER): boolean { return Number.isSafeInteger(n) && n >= 0 && n <= max; }
function equal(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function validKey(key: ObjectKey): boolean { return Number.isInteger(key.kind) && key.kind >= 1 && key.kind <= 5 && /^[0-9a-f]{64}$/.test(key.id) && !(key.kind === 3 && key.id === '0'.repeat(64)); }
function descriptor(d: Descriptor): Descriptor {
 if (!validKey(d) || d.format_version !== 1 || !safe(d.size_bytes)) fail(400, 'invalid_argument');
 if (d.size_bytes > (d.kind === 1 ? MAX_FILE : MAX_METADATA)) fail(429, 'resource_exhausted');
 return { kind: d.kind, id: d.id, size_bytes: d.size_bytes, format_version: 1 };
}
function owner(kind: string, id: string): void {
 if (!/^[A-Za-z0-9_.-]{1,64}$/.test(kind) || id.length === 0 || encoder.encode(id).byteLength > 1024
  || decoder.decode(encoder.encode(id)) !== id || /[\u0000-\u001f\u007f-\u009f]/u.test(id)) fail(400, 'invalid_argument');
}
function attemptId(): string { return Array.from(crypto.getRandomValues(new Uint8Array(32)), b => b.toString(16).padStart(2, '0')).join(''); }
export class ForgeStorage {
 private readonly account: string;
 private readonly confirmations = new WeakMap<object, Confirmation>();
 constructor(private readonly host: ForgeStorageHost) {
  if (!/^[a-z0-9-]{1,64}$/.test(host.accountId)) fail(400, 'invalid_argument'); this.account = host.accountId;
  for (const sql of [
   'CREATE TABLE IF NOT EXISTS forge_storage_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,objects INTEGER NOT NULL,bytes INTEGER NOT NULL,reservations INTEGER NOT NULL)',
   'CREATE TABLE IF NOT EXISTS forge_storage_reservations(owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,format INTEGER NOT NULL,PRIMARY KEY(owner_kind,owner_id,kind,id))',
   'CREATE TABLE IF NOT EXISTS forge_storage_physical(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,payloads INTEGER NOT NULL,payload_bytes INTEGER NOT NULL,attempt_rows INTEGER NOT NULL,inspection_bytes INTEGER NOT NULL)',
   'CREATE TABLE IF NOT EXISTS forge_storage_attempts(attempt_id TEXT PRIMARY KEY,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,inspection TEXT NOT NULL,locator TEXT NOT NULL UNIQUE,state TEXT NOT NULL,etag TEXT,version TEXT)',
   'CREATE INDEX IF NOT EXISTS forge_storage_attempt_state ON forge_storage_attempts(state,attempt_id)',
   'CREATE TABLE IF NOT EXISTS forge_storage_bindings(kind INTEGER NOT NULL,id TEXT NOT NULL,generation INTEGER NOT NULL,attempt_id TEXT,PRIMARY KEY(kind,id))',
  ]) this.exec(sql);
  this.transaction(() => {
   if (!this.one<Usage>('SELECT * FROM forge_storage_usage WHERE singleton=1')) {
    const u = this.one<{ n: number; bytes: number }>('SELECT COUNT(*) AS n,COALESCE(SUM(size),0) AS bytes FROM objects')!;
    if (!safe(u.n, MAX_OBJECTS) || !safe(u.bytes, MAX_BYTES)) fail(429, 'resource_exhausted');
    this.exec('INSERT INTO forge_storage_usage(singleton,account,objects,bytes,reservations) VALUES(1,?,?,?,0)', this.account, u.n, u.bytes);
   }
   if (!this.one<PhysicalUsage>('SELECT * FROM forge_storage_physical WHERE singleton=1')) {
    this.exec('INSERT INTO forge_storage_physical(singleton,account,payloads,payload_bytes,attempt_rows,inspection_bytes) VALUES(1,?,0,0,0,0)', this.account);
   }
   this.usage(); this.physical();
  });
 }
 private exec(sql: string, ...args: (string | number | null)[]) { try { return this.host.sql.exec(sql, ...args); } catch { fail(503, 'unavailable'); } }
 private rows<T>(sql: string, ...args: (string | number | null)[]): T[] { return [...this.exec(sql, ...args)] as T[]; }
 private one<T>(sql: string, ...args: (string | number | null)[]): T | undefined { return this.rows<T>(sql, ...args)[0]; }
 private transaction<T>(f: () => T): T { try { return this.host.transactionSync(f); } catch (e) { if (e instanceof Failure) throw e; fail(503, 'unavailable'); } }
 private mutation(mode: 'authoring' | 'maintenance' = 'authoring'): void { this.host.mutationInTx?.(mode); }
 private usage(): Usage {
  const u = this.one<Usage>('SELECT * FROM forge_storage_usage WHERE singleton=1');
  if (!u || !safe(u.objects, MAX_OBJECTS) || !safe(u.bytes, MAX_BYTES) || !safe(u.reservations, MAX_RESERVATIONS)) fail(500, 'data_loss');
  if (u.account !== this.account) fail(403, 'permission_denied'); return u;
 }
 private physical(): PhysicalUsage {
  const u = this.one<PhysicalUsage>('SELECT * FROM forge_storage_physical WHERE singleton=1');
  if (!u || !safe(u.payloads, MAX_OBJECTS) || !safe(u.payload_bytes, MAX_BYTES) || !safe(u.attempt_rows, MAX_ATTEMPTS) || !safe(u.inspection_bytes, MAX_CUSTODY_METADATA)) fail(500, 'data_loss');
  if (u.account !== this.account) fail(403, 'permission_denied'); return u;
 }
 private catalog(k: ObjectKey): Catalog | undefined { return this.one<Catalog>('SELECT size,inspection,visible FROM objects WHERE kind=? AND id=?', k.kind, k.id); }
 private binding(k: ObjectKey): Binding { const b = this.one<Binding>('SELECT generation,attempt_id FROM forge_storage_bindings WHERE kind=? AND id=?', k.kind, k.id); if (b && !safe(b.generation)) fail(500, 'data_loss'); return b ?? { generation: 0, attempt_id: null }; }
 private attempt(id: string): Attempt {
  if (!/^[0-9a-f]{64}$/.test(id)) fail(400, 'invalid_argument');
  const a = this.one<Attempt>('SELECT * FROM forge_storage_attempts WHERE attempt_id=?', id); if (!a) fail(404, 'not_found');
  if (!validKey(a) || !safe(a.size, a.kind === 1 ? MAX_FILE : MAX_METADATA) || !['pending','written','retiring','deleted'].includes(a.state)) fail(500, 'data_loss'); return a;
 }
 private d(a: Attempt): Descriptor { return { kind: a.kind, id: a.id, size_bytes: a.size, format_version: 1 }; }
 private checkedCatalog(k: ObjectKey): Descriptor | undefined {
  if (!validKey(k)) fail(400, 'invalid_argument'); this.usage();
  const row = this.catalog(k); if (!row || Number(row.visible) !== 1) return undefined;
  if (typeof row.inspection !== 'string' || encoder.encode(row.inspection).length > MAX_INSPECTION) fail(500, 'data_loss');
  let info: Inspection; try { info = JSON.parse(row.inspection); } catch { fail(500, 'data_loss'); }
  const d = descriptor({ kind: k.kind, id: k.id, size_bytes: Number(row.size), format_version: 1 }); if (!info || !equal(d, info)) fail(500, 'data_loss'); return d;
 }
 describeInTx(k: ObjectKey): Descriptor | undefined { const d = this.checkedCatalog(k); if (d) this.incarnationInTx(k); return d; }
 incarnationInTx(k: ObjectKey): StorageIncarnation | undefined {
  const d = this.checkedCatalog(k); if (!d) return undefined; const b = this.binding(k); if (!b.attempt_id) fail(500, 'data_loss'); const a = this.attempt(b.attempt_id);
  if (a.state !== 'written' || !equal(d, this.d(a)) || a.inspection !== this.catalog(k)!.inspection) fail(500, 'data_loss'); return { descriptor: d, attempt_id: a.attempt_id, generation: b.generation };
 }
 reserveInTx(ownerKind: string, ownerId: string, input: Descriptor): void {
  owner(ownerKind, ownerId); const d = descriptor(input), u = this.usage(), old = this.catalog(d);
  const r = this.one<Reservation>('SELECT size,format FROM forge_storage_reservations WHERE owner_kind=? AND owner_id=? AND kind=? AND id=?', ownerKind, ownerId, d.kind, d.id);
  if (old && old.size !== d.size_bytes) fail(400, 'invalid_argument');
  if (r) { if (r.size !== d.size_bytes || r.format !== 1 || !old) fail(400, 'failed_precondition'); return; }
  this.mutation(); if (u.reservations >= MAX_RESERVATIONS || (!old && (u.objects >= MAX_OBJECTS || u.bytes + d.size_bytes > MAX_BYTES))) fail(429, 'resource_exhausted');
  if (!old) { this.exec("INSERT INTO objects(kind,id,size,inspection,visible) VALUES(?,?,?,'{}',0)", d.kind, d.id, d.size_bytes); this.exec('UPDATE forge_storage_usage SET objects=objects+1,bytes=bytes+? WHERE singleton=1', d.size_bytes); }
  this.exec('INSERT INTO forge_storage_reservations(owner_kind,owner_id,kind,id,size,format) VALUES(?,?,?,?,?,?)', ownerKind, ownerId, d.kind, d.id, d.size_bytes, 1); this.exec('UPDATE forge_storage_usage SET reservations=reservations+1 WHERE singleton=1');
 }
 private metadata(a: Attempt): Record<string, string> { return { attempt: a.attempt_id, account: this.account, kind: String(a.kind), id: a.id, format: '1' }; }
 private matches(a: Attempt, object: R2Object): boolean { const m = object.customMetadata; return object.size === a.size && (a.etag === null || a.etag === object.etag) && (a.version === null || a.version === object.version) && !!m && Object.entries(this.metadata(a)).every(([k,v]) => m[k] === v); }
 async stage(input: Descriptor, inputBytes: Uint8Array): Promise<unknown> {
  const d = descriptor(input); if (inputBytes.length !== d.size_bytes) fail(400, 'invalid_argument'); const data = inputBytes.slice(); let info: Inspection;
  try { info = inspect(d.kind, data); } catch { fail(400, 'invalid_argument'); } if (!equal(info, d)) fail(400, 'invalid_argument'); const inspection = JSON.stringify(info);
  if (encoder.encode(inspection).length > MAX_INSPECTION) fail(429, 'resource_exhausted');
  const captured = this.transaction(() => {
   const row = this.catalog(d), r = this.one<Reservation>('SELECT size,format FROM forge_storage_reservations WHERE kind=? AND id=? LIMIT 1', d.kind, d.id), b = this.binding(d), u = this.physical();
   if (!row || row.size !== d.size_bytes || !r || r.size !== d.size_bytes || r.format !== 1) fail(400, 'failed_precondition'); this.mutation();
   if (u.payloads >= MAX_OBJECTS || u.payload_bytes + d.size_bytes > MAX_BYTES || u.attempt_rows >= MAX_ATTEMPTS || u.inspection_bytes + encoder.encode(inspection).length > MAX_CUSTODY_METADATA || this.one<{n:number}>("SELECT COUNT(*) AS n FROM forge_storage_attempts WHERE state='pending'")!.n >= MAX_PENDING) fail(429, 'resource_exhausted');
   const id = attemptId(), locator = `native/v2/${this.account}/${d.kind}/${d.id}/${id}`;
   this.exec("INSERT INTO forge_storage_attempts(attempt_id,kind,id,size,inspection,locator,state) VALUES(?,?,?,?,?,?,'pending')", id, d.kind, d.id, d.size_bytes, inspection, locator);
   this.exec('UPDATE forge_storage_physical SET payloads=payloads+1,payload_bytes=payload_bytes+?,attempt_rows=attempt_rows+1,inspection_bytes=inspection_bytes+? WHERE singleton=1', d.size_bytes, encoder.encode(inspection).length);
   return { a: this.attempt(id), generation: b.generation };
  });
  let written: R2Object | null; try { written = await this.host.objects.put(captured.a.locator, data, { customMetadata: this.metadata(captured.a) }); } catch { fail(503, 'unavailable'); }
  if (!written || !this.matches(captured.a, written)) fail(503, 'unavailable');
  this.transaction(() => { const live = this.attempt(captured.a.attempt_id); if (live.state !== 'pending') fail(409, 'aborted'); this.mutation(); this.exec("UPDATE forge_storage_attempts SET state='written',etag=?,version=? WHERE attempt_id=?", written!.etag, written!.version, live.attempt_id); });
  const token = Object.freeze({}); this.confirmations.set(token, { descriptor: d, inspection, attempt_id: captured.a.attempt_id, generation: captured.generation }); return token;
 }
 confirmInTx(ownerKind: string, ownerId: string, input: Descriptor, token: unknown): void {
  owner(ownerKind, ownerId); const d = descriptor(input), proof = token !== null && typeof token === 'object' ? this.confirmations.get(token) : undefined;
  const r = this.one<Reservation>('SELECT size,format FROM forge_storage_reservations WHERE owner_kind=? AND owner_id=? AND kind=? AND id=?', ownerKind, ownerId, d.kind, d.id), row = this.catalog(d), b = this.binding(d);
  if (!proof || !equal(d, proof.descriptor) || !r || r.size !== d.size_bytes || r.format !== 1 || !row || row.size !== d.size_bytes) fail(400, 'failed_precondition');
  const a = this.attempt(proof.attempt_id); if (a.state !== 'written' || a.inspection !== proof.inspection || !equal(this.d(a), d)) fail(409, 'aborted');
  if (b.attempt_id === a.attempt_id && b.generation === proof.generation + 1) return;
  if (b.generation !== proof.generation || b.generation >= Number.MAX_SAFE_INTEGER) fail(409, 'aborted'); this.mutation();
  this.exec('INSERT INTO forge_storage_bindings(kind,id,generation,attempt_id) VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET generation=excluded.generation,attempt_id=excluded.attempt_id', d.kind, d.id, b.generation + 1, a.attempt_id);
  this.exec('UPDATE objects SET inspection=?,visible=1 WHERE kind=? AND id=?', proof.inspection, d.kind, d.id);
 }
 private unchanged(s: StorageIncarnation): void { const now = this.incarnationInTx(s.descriptor); if (!now || now.attempt_id !== s.attempt_id || now.generation !== s.generation || !equal(now.descriptor, s.descriptor)) fail(409, 'aborted'); }
 private async body(object: R2ObjectBody, size: number): Promise<Uint8Array> {
  const reader = object.body.getReader(), data = new Uint8Array(size); let n = 0;
  try { for (;;) { let item: ReadableStreamReadResult<Uint8Array>; try { item = await reader.read(); } catch { fail(503, 'unavailable'); } if (item.done) break;
   if (!(item.value instanceof Uint8Array) || item.value.length > size - n) { await reader.cancel().catch(() => undefined); fail(500, 'data_loss'); } data.set(item.value, n); n += item.value.length;
  } } finally { reader.releaseLock(); } if (n !== size) fail(500, 'data_loss'); return data;
 }
 async readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }> {
  const s = this.transaction(() => { const s = this.incarnationInTx(key); if (!s) fail(404, 'not_found'); return s; }), a = this.transaction(() => this.attempt(s.attempt_id));
  let object: R2ObjectBody | null; try { object = await this.host.objects.get(a.locator); } catch { fail(503, 'unavailable'); }
  if (!object || object.size !== a.size || !this.matches(a, object)) fail(503, 'unavailable');
  const data = await this.body(object, a.size); let actual: Inspection; try { actual = inspect(a.kind, data); } catch { fail(500, 'data_loss'); }
  if (!equal(actual, s.descriptor)) fail(500, 'data_loss'); this.transaction(() => this.unchanged(s)); return { descriptor: s.descriptor, bytes: data };
 }
 async readRange(input: Descriptor, offset: number, length: number): Promise<Uint8Array> {
  const d = descriptor(input), s = this.transaction(() => { const s = this.incarnationInTx(d); if (!s || !equal(s.descriptor, d)) fail(404, 'not_found'); return s; });
  if (!safe(offset, d.size_bytes) || !safe(length, 65536)) fail(400, 'out_of_range'); const size = Math.min(length, d.size_bytes - offset); if (!size) { this.transaction(() => this.unchanged(s)); return new Uint8Array(); }
  const a = this.transaction(() => this.attempt(s.attempt_id)); let object: R2ObjectBody | null; try { object = await this.host.objects.get(a.locator, { range: { offset, length: size } }); } catch { fail(503, 'unavailable'); }
  if (!object || object.size !== d.size_bytes || !this.matches(a, object)) fail(503, 'unavailable'); const range = object.range;
  if (!range || !('offset' in range) || !('length' in range) || range.offset !== offset || range.length !== size) fail(500, 'data_loss'); const data = await this.body(object, size); this.transaction(() => this.unchanged(s)); return data;
 }
 accountingInTx(): StorageAccounting { const u = this.usage(), p = this.physical(); return { logical_objects: u.objects, logical_bytes: u.bytes, reservations: u.reservations, physical_payloads: p.payloads, physical_bytes: p.payload_bytes, attempt_rows: p.attempt_rows }; }
 attemptsInTx(after: string | null = null, limit = 128): { attempts: StorageAttempt[]; next: string | null } {
  if (!safe(limit, 128) || !limit || (after !== null && (typeof after !== 'string' || !/^[0-9a-f]{64}$/.test(after)))) fail(400, 'invalid_argument');
  const rows = this.rows<Attempt>('SELECT * FROM forge_storage_attempts WHERE attempt_id>? ORDER BY attempt_id LIMIT ?', after ?? '', limit + 1), selected = rows.slice(0, limit);
  return { attempts: selected.map(a => ({ attempt_id: a.attempt_id, descriptor: this.d(a), state: a.state, current: this.binding(a).attempt_id === a.attempt_id })), next: rows.length > limit ? selected[selected.length - 1].attempt_id : null };
 }
 /** Logical direct custody marks; incomplete metadata reservations are NOT
  * complete closure roots. Parent merges these with its strict root walk. */
 reservedKeysInTx(after: ObjectKey | null = null, limit = 128): { objects: Descriptor[]; next: ObjectKey | null } {
  if (!safe(limit, 128) || !limit || (after !== null && !validKey(after))) fail(400, 'invalid_argument');
  const rows = this.rows<{ kind: number; id: string; size: number; maximum: number }>('SELECT kind,id,MIN(size) AS size,MAX(size) AS maximum FROM forge_storage_reservations WHERE kind>? OR (kind=? AND id>?) GROUP BY kind,id ORDER BY kind,id LIMIT ?', after?.kind ?? 0, after?.kind ?? 0, after?.id ?? '', limit + 1);
  const selected = rows.slice(0, limit), objects = selected.map(r => { if (r.size !== r.maximum) fail(500, 'data_loss'); return descriptor({ kind: r.kind, id: r.id, size_bytes: r.size, format_version: 1 }); });
  const last = selected[selected.length - 1]; return { objects, next: rows.length > limit ? { kind: last.kind, id: last.id } : null };
 }
 /** HEAD proves only a specific completed write. Absence says nothing about a
  * pending late PUT and never clears its durable charge/custody. */
 async recoverAttempt(id: string): Promise<StorageAttempt> {
  const a = this.transaction(() => this.attempt(id));
  if (a.state === 'pending') {
   let head: R2Object | null; try { head = await this.host.objects.head(a.locator); } catch { fail(503, 'unavailable'); }
   if (head) { if (!this.matches(a, head)) fail(500, 'data_loss'); this.transaction(() => { const live = this.attempt(id); if (live.state === 'pending') { this.mutation('maintenance'); this.exec("UPDATE forge_storage_attempts SET state='written',etag=?,version=? WHERE attempt_id=?", head!.etag, head!.version, id); } }); }
  }
  return this.transaction(() => { const live = this.attempt(id); return { attempt_id: id, descriptor: this.d(live), state: live.state, current: this.binding(live).attempt_id === id }; });
 }
 /** Parent supplies a marked exact current victim under its maintenance gate.
  * Without that snapshot only a noncurrent terminal-write orphan can retire. */
 retireInTx(id: string, expectedCurrent?: StorageIncarnation): void {
  const a = this.attempt(id), b = this.binding(a); if (expectedCurrent && (expectedCurrent.attempt_id !== id || !equal(expectedCurrent.descriptor, this.d(a)))) fail(409, 'aborted'); if (a.state === 'retiring' || a.state === 'deleted') return;
  if (a.state !== 'written') fail(409, 'aborted');
  if (b.attempt_id === id) {
   if (this.one('SELECT 1 FROM forge_storage_reservations WHERE kind=? AND id=? LIMIT 1', a.kind, a.id)) fail(409, 'aborted');
   if (!expectedCurrent || expectedCurrent.attempt_id !== id || expectedCurrent.generation !== b.generation || !equal(expectedCurrent.descriptor, this.d(a))) fail(409, 'aborted');
   if (b.generation >= Number.MAX_SAFE_INTEGER) fail(429, 'resource_exhausted'); this.mutation('maintenance');
   this.exec('UPDATE forge_storage_bindings SET generation=generation+1,attempt_id=NULL WHERE kind=? AND id=?', a.kind, a.id); this.exec('UPDATE objects SET visible=0 WHERE kind=? AND id=?', a.kind, a.id);
  } else { if (expectedCurrent) fail(409, 'aborted'); this.mutation('maintenance'); }
  this.exec("UPDATE forge_storage_attempts SET state='retiring' WHERE attempt_id=?", id);
 }
 /** Only durable retirement permits DELETE. Every retry targets the same old
  * locator; a replacement has an unrelated locator and cannot be removed. */
 async deleteRetired(id: string): Promise<void> {
  const a = this.transaction(() => { const a = this.attempt(id); if (!['retiring','deleted'].includes(a.state) || this.binding(a).attempt_id === id) fail(409, 'aborted'); if (a.state !== 'deleted') this.mutation('maintenance'); return a; });
  if (a.state === 'deleted') return;
  try { await this.host.objects.delete(a.locator); } catch { fail(503, 'unavailable'); }
  this.transaction(() => { const live = this.attempt(id); if (live.state === 'deleted') return; if (live.state !== 'retiring' || live.locator !== a.locator || this.binding(live).attempt_id === id) fail(409, 'aborted'); this.mutation('maintenance'); const p = this.physical(); if (p.payloads < 1 || p.payload_bytes < a.size || p.inspection_bytes < encoder.encode(a.inspection).length) fail(500, 'data_loss');
   this.exec("UPDATE forge_storage_attempts SET state='deleted',inspection='' WHERE attempt_id=?", id); this.exec('UPDATE forge_storage_physical SET payloads=payloads-1,payload_bytes=payload_bytes-?,inspection_bytes=inspection_bytes-? WHERE singleton=1', a.size, encoder.encode(a.inspection).length);
  });
 }
 /** Owning state machine must already be terminal. Strings are internal owner
  * keys, never a public user deletion API. Payload custody remains independent. */
 releaseReservationsInTx(ownerKind: string, ownerId: string, limit = 128): { released: number; complete: boolean } {
  owner(ownerKind, ownerId); if (!safe(limit, 128) || !limit) fail(400, 'invalid_argument'); const rows = this.rows<{kind:number;id:string}>('SELECT kind,id FROM forge_storage_reservations WHERE owner_kind=? AND owner_id=? ORDER BY kind,id LIMIT ?', ownerKind, ownerId, limit + 1), selected = rows.slice(0, limit);
  if (selected.length) { this.mutation(); const u = this.usage(); if (u.reservations < selected.length) fail(500, 'data_loss'); for (const r of selected) this.exec('DELETE FROM forge_storage_reservations WHERE owner_kind=? AND owner_id=? AND kind=? AND id=?', ownerKind, ownerId, r.kind, r.id); this.exec('UPDATE forge_storage_usage SET reservations=reservations-? WHERE singleton=1', selected.length); }
  return { released: selected.length, complete: rows.length <= limit };
 }
 discardLogicalInTx(key: ObjectKey): void {
  if (!validKey(key)) fail(400, 'invalid_argument'); const row = this.catalog(key); if (!row) return;
  if (row.visible !== 0 || this.binding(key).attempt_id || this.one('SELECT 1 FROM forge_storage_reservations WHERE kind=? AND id=? LIMIT 1', key.kind, key.id)
   || this.one("SELECT 1 FROM forge_storage_attempts WHERE kind=? AND id=? AND state='pending' LIMIT 1", key.kind, key.id)) fail(409, 'aborted');
  this.mutation('maintenance'); const u = this.usage(); if (u.objects < 1 || u.bytes < row.size) fail(500, 'data_loss'); this.exec('DELETE FROM objects WHERE kind=? AND id=?', key.kind, key.id); this.exec('UPDATE forge_storage_usage SET objects=objects-1,bytes=bytes-? WHERE singleton=1', row.size);
 }
}
