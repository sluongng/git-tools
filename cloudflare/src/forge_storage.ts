/** Immutable payload AVAILABILITY only: not authentication, ACL, upload or root
 * authority. Callers must authorize before and after awaits. No physical GC. */
import { inspect, type Descriptor, type Inspection, type ObjectKey } from './core';
import { fail, Failure } from './protocol';

export interface ForgeStorageHost {
  accountId: string; sql: SqlStorage; objects: R2Bucket;
  transactionSync<T>(callback: () => T): T;
}
interface Catalog { size: number; inspection: string; visible: number }
interface Usage { account: string; objects: number; bytes: number; reservations: number }
interface Reservation { size: number; format: number }
interface Confirmation { descriptor: Descriptor; inspection: string }
const MAX_OBJECTS = 32768, MAX_BYTES = 512 * 1024 * 1024, MAX_RESERVATIONS = 131072;
const MAX_FILE = 1024 * 1024, MAX_METADATA = 256 * 1024, MAX_INSPECTION = 1024 * 1024;
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });
function equal(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function validKey(key: ObjectKey): boolean { return Number.isInteger(key.kind) && key.kind >= 1 && key.kind <= 5 && /^[0-9a-f]{64}$/.test(key.id) && !(key.kind === 3 && key.id === '0'.repeat(64)); }
function descriptor(d: Descriptor): Descriptor {
  if (!validKey(d) || d.format_version !== 1 || !Number.isSafeInteger(d.size_bytes) || d.size_bytes < 0) fail(400, 'invalid_argument');
  if (d.size_bytes > (d.kind === 1 ? MAX_FILE : MAX_METADATA)) fail(429, 'resource_exhausted');
  return { kind: d.kind, id: d.id, size_bytes: d.size_bytes, format_version: 1 };
}
function owner(kind: string, id: string): void {
  if (!/^[A-Za-z0-9_.-]{1,64}$/.test(kind) || id.length === 0 || encoder.encode(id).byteLength > 1024
      || decoder.decode(encoder.encode(id)) !== id || /[\u0000-\u001f\u007f-\u009f]/u.test(id)) fail(400, 'invalid_argument');
}
export class ForgeStorage {
  private readonly account: string;
  private readonly confirmations = new WeakMap<object, Confirmation>();
  constructor(private readonly host: ForgeStorageHost) {
    if (!/^[a-z0-9-]{1,64}$/.test(host.accountId)) fail(400, 'invalid_argument'); this.account = host.accountId;
    for (const sql of [
      'CREATE TABLE IF NOT EXISTS forge_storage_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,objects INTEGER NOT NULL,bytes INTEGER NOT NULL,reservations INTEGER NOT NULL)',
      'CREATE TABLE IF NOT EXISTS forge_storage_reservations(owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,format INTEGER NOT NULL,PRIMARY KEY(owner_kind,owner_id,kind,id))',
    ]) this.exec(sql);
    this.transaction(() => {
      if (!this.one<Usage>('SELECT * FROM forge_storage_usage WHERE singleton=1')) {
        // One-time adoption of this account's existing canonical catalog (usually
        // its two genesis objects). Raw account adoption is forbidden by host.
        const usage = this.one<{ n: number; bytes: number }>('SELECT COUNT(*) AS n,COALESCE(SUM(size),0) AS bytes FROM objects')!;
        if (!Number.isSafeInteger(usage.n) || usage.n < 0 || usage.n > MAX_OBJECTS || !Number.isSafeInteger(usage.bytes) || usage.bytes < 0 || usage.bytes > MAX_BYTES) fail(429, 'resource_exhausted');
        this.exec('INSERT INTO forge_storage_usage(singleton,account,objects,bytes,reservations) VALUES(1,?,?,?,0)', this.account, usage.n, usage.bytes);
      }
      this.usage();
    });
  }
  private exec(query: string, ...bindings: (string | number | null)[]) {
    try { return this.host.sql.exec(query, ...bindings); } catch { fail(503, 'unavailable'); }
  }
  private transaction<T>(callback: () => T): T {
    try { return this.host.transactionSync(callback); }
    catch (error) { if (error instanceof Failure) throw error; fail(503, 'unavailable'); }
  }
  private one<T>(query: string, ...bindings: (string | number | null)[]): T | undefined { return [...this.exec(query, ...bindings)][0] as T | undefined; }
  private usage(): Usage {
    const value = this.one<Usage>('SELECT * FROM forge_storage_usage WHERE singleton=1');
    if (!value || !Number.isSafeInteger(value.objects) || value.objects < 0 || value.objects > MAX_OBJECTS
        || !Number.isSafeInteger(value.bytes) || value.bytes < 0 || value.bytes > MAX_BYTES
        || !Number.isSafeInteger(value.reservations) || value.reservations < 0 || value.reservations > MAX_RESERVATIONS) fail(500, 'data_loss');
    if (value.account !== this.account) fail(403, 'permission_denied'); return value;
  }
  private key(key: ObjectKey): string { return `native/v1/${this.account}/${key.kind}/${key.id}`; }
  private catalog(key: ObjectKey): Catalog | undefined { return this.one<Catalog>('SELECT size,inspection,visible FROM objects WHERE kind=? AND id=?', key.kind, key.id); }
  describeInTx(key: ObjectKey): Descriptor | undefined {
    if (!validKey(key)) fail(400, 'invalid_argument'); this.usage();
    const row = this.catalog(key); if (!row || Number(row.visible) !== 1) return undefined;
    if (typeof row.inspection !== 'string' || row.inspection.length > MAX_INSPECTION || encoder.encode(row.inspection).byteLength > MAX_INSPECTION) fail(500, 'data_loss');
    let inspection: Inspection; try { inspection = JSON.parse(row.inspection); } catch { fail(500, 'data_loss'); }
    const value: Descriptor = { kind: key.kind, id: key.id, size_bytes: Number(row.size), format_version: 1 };
    if (!Number.isSafeInteger(value.size_bytes) || value.size_bytes < 0 || value.size_bytes > (key.kind === 1 ? MAX_FILE : MAX_METADATA)
        || !inspection || !equal(value, inspection)) fail(500, 'data_loss');
    return value;
  }
  /** Caller holds the authoritative SQL transaction and authorization. */
  reserveInTx(ownerKind: string, ownerId: string, input: Descriptor): void {
    owner(ownerKind, ownerId); const d = descriptor(input); const usage = this.usage();
    const previous = this.one<Reservation>('SELECT size,format FROM forge_storage_reservations WHERE owner_kind=? AND owner_id=? AND kind=? AND id=?', ownerKind, ownerId, d.kind, d.id);
    const existing = this.catalog(d);
    if (existing && Number(existing.size) !== d.size_bytes) fail(400, 'invalid_argument');
    if (previous) {
      if (Number(previous.size) !== d.size_bytes || Number(previous.format) !== d.format_version || !existing) fail(400, 'invalid_argument'); return;
    }
    if (usage.reservations >= MAX_RESERVATIONS || (!existing && (usage.objects >= MAX_OBJECTS || usage.bytes + d.size_bytes > MAX_BYTES))) fail(429, 'resource_exhausted');
    if (!existing) {
      this.exec('INSERT INTO objects(kind,id,size,inspection,visible) VALUES(?,?,?,\'{}\',0)', d.kind, d.id, d.size_bytes);
      this.exec('UPDATE forge_storage_usage SET objects=objects+1,bytes=bytes+? WHERE singleton=1', d.size_bytes);
    }
    this.exec('INSERT INTO forge_storage_reservations(owner_kind,owner_id,kind,id,size,format) VALUES(?,?,?,?,?,?)', ownerKind, ownerId, d.kind, d.id, d.size_bytes, d.format_version);
    this.exec('UPDATE forge_storage_usage SET reservations=reservations+1 WHERE singleton=1');
  }
  async stage(input: Descriptor, inputBytes: Uint8Array): Promise<unknown> {
    const d = descriptor(input);
    if (inputBytes.byteLength !== d.size_bytes) fail(400, 'invalid_argument');
    // Pin caller-owned mutable input before verification and asynchronous PUT.
    const bytes = inputBytes.slice(); let verified: Inspection;
    try { verified = inspect(d.kind, bytes); } catch { fail(400, 'invalid_argument'); }
    if (!equal(d, verified)) fail(400, 'invalid_argument');
    const inspection = JSON.stringify(verified);
    if (encoder.encode(inspection).byteLength > MAX_INSPECTION) fail(429, 'resource_exhausted');
    this.transaction(() => {
      this.usage(); const row = this.catalog(d);
      const reservation = this.one<Reservation>('SELECT size,format FROM forge_storage_reservations WHERE kind=? AND id=? LIMIT 1', d.kind, d.id);
      if (!row || Number(row.size) !== d.size_bytes || !reservation || Number(reservation.size) !== d.size_bytes || Number(reservation.format) !== 1) fail(400, 'failed_precondition');
      // Never demote an existing visible object during correct-byte repair.
      this.exec('UPDATE objects SET inspection=? WHERE kind=? AND id=?', inspection, d.kind, d.id);
    });
    let written: R2Object | null;
    try { written = await this.host.objects.put(this.key(d), bytes); } catch { fail(503, 'unavailable'); }
    if (!written || written.size !== d.size_bytes) fail(503, 'unavailable');
    const token = Object.freeze({}); this.confirmations.set(token, { descriptor: d, inspection }); return token;
  }
  /** Caller reauthorizes and holds its SQL publication/receipt transaction. */
  confirmInTx(ownerKind: string, ownerId: string, input: Descriptor, token: unknown): void {
    owner(ownerKind, ownerId); const d = descriptor(input); this.usage();
    const proof = token !== null && typeof token === 'object' ? this.confirmations.get(token) : undefined;
    const reservation = this.one<Reservation>('SELECT size,format FROM forge_storage_reservations WHERE owner_kind=? AND owner_id=? AND kind=? AND id=?', ownerKind, ownerId, d.kind, d.id);
    const row = this.catalog(d);
    if (!proof || !equal(d, proof.descriptor) || !reservation || Number(reservation.size) !== d.size_bytes || Number(reservation.format) !== 1
        || !row || Number(row.size) !== d.size_bytes || row.inspection !== proof.inspection) fail(400, 'failed_precondition');
    this.exec('UPDATE objects SET visible=1 WHERE kind=? AND id=?', d.kind, d.id);
    // Retain weak token validity for caller transaction rollback/retry. It is
    // never persisted/serializable authority and cannot survive another instance.
  }
  private async body(object: R2ObjectBody, size: number): Promise<Uint8Array> {
    const reader = object.body.getReader(); const bytes = new Uint8Array(size); let offset = 0;
    try {
      for (;;) {
        let item: ReadableStreamReadResult<Uint8Array>; try { item = await reader.read(); } catch { fail(503, 'unavailable'); }
        if (item.done) break;
        if (!(item.value instanceof Uint8Array) || item.value.byteLength > size - offset) {
          await reader.cancel().catch(() => undefined); fail(500, 'data_loss');
        }
        bytes.set(item.value, offset); offset += item.value.byteLength;
      }
    } finally { reader.releaseLock(); }
    if (offset !== size) fail(500, 'data_loss'); return bytes;
  }
  private visible(key: ObjectKey): Descriptor { const d = this.describeInTx(key); if (!d) fail(404, 'not_found'); return d; }
  private unchanged(d: Descriptor): void { if (!equal(d, this.visible(d))) fail(500, 'data_loss'); }
  async readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }> {
    const stable = { kind: key.kind, id: key.id }; const d = this.transaction(() => this.visible(stable));
    let object: R2ObjectBody | null; try { object = await this.host.objects.get(this.key(d)); } catch { fail(503, 'unavailable'); }
    if (!object) fail(503, 'unavailable'); if (object.size !== d.size_bytes) fail(500, 'data_loss');
    const bytes = await this.body(object, d.size_bytes); let actual: Inspection;
    try { actual = inspect(d.kind, bytes); } catch { fail(500, 'data_loss'); }
    if (!equal(d, actual)) fail(500, 'data_loss'); this.transaction(() => this.unchanged(d)); return { descriptor: d, bytes };
  }
  /** Range reads trust immutable canonical backing and prior closure proof;
   * they do not rehash an entire FILE per chunk or mint a read capability. */
  async readRange(input: Descriptor, offset: number, length: number): Promise<Uint8Array> {
    const d = descriptor(input); this.transaction(() => this.unchanged(d));
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > d.size_bytes || !Number.isSafeInteger(length) || length < 0 || length > 65536) fail(400, 'out_of_range');
    const size = Math.min(length, d.size_bytes - offset); if (size === 0) return new Uint8Array();
    let object: R2ObjectBody | null; try { object = await this.host.objects.get(this.key(d), { range: { offset, length: size } }); } catch { fail(503, 'unavailable'); }
    if (!object) fail(503, 'unavailable');
    const range = object.range;
    if (object.size !== d.size_bytes || !range || !('offset' in range) || !('length' in range) || range.offset !== offset || range.length !== size) fail(500, 'data_loss');
    const bytes = await this.body(object, size); this.transaction(() => this.unchanged(d)); return bytes;
  }
}
