/** One-time owner bootstrap authority. Not a route or source-freeze
 * proof. The host owns durable payload storage and the irreversible main CAS. */
import { normalizeBootstrapImport, inspect, type Descriptor, type ForgeCall, type ObjectKey } from './core';
import { ClosureJobs, type ClosureBinding } from './closure_jobs';
import { fail } from './protocol';

export interface ImportSource { repository_id: string; reference: 'main'; generation: number; head: string }
export interface ImportBegin { root: ObjectKey; source: ImportSource; destination: { generation: 1; head: string } }
export type ImportCommand =
  | { operation: 40; fingerprint: string; payload: ImportBegin }
  | { operation: 41; fingerprint: string; payload: { import_id: string; descriptor: Descriptor; data_base64: string } }
  | { operation: 42 | 44; fingerprint: string; payload: { import_id: string; expected_generation: number } }
  | { operation: 43; fingerprint: string; payload: { import_id: string } };
export interface ImportOwner {
  account_id: string; owner: string; policy_epoch: number;
  main: { head: string; generation: number }; bootstrap_sealed: boolean;
}
export interface BootstrapImportRecord {
  id: string; account: string; principal: string; epoch: number; begin_request: string;
  begin_fingerprint: string; payload: ImportBegin; job_id: string | null;
}
export interface BootstrapImportHost {
  accountId: string; principal: string;
  sql: SqlStorage;
  transactionSync<T>(callback: () => T): T;
  /** Shared immutable account owner check; never is_admin/operator substitution.
   * A missing provision-created bootstrap-state row must fail closed. */
  requireOwnerInTx(): ImportOwner;
  canonicalGenesis(): string;
  replayInTx(requestId: string, fingerprint: string): unknown | undefined;
  finishInTx(requestId: string, fingerprint: string, result: unknown): unknown;
  /** Factory uses current caller auth and exact principal/epoch/purpose binding.
   * It must enforce current ownership, open seal and original epoch on job work.
   * The callback is invoked inside ClosureJobs.publish's SQL transaction. */
  makeClosureJobs(binding: ClosureBinding, attach: (binding: ClosureBinding, root: ObjectKey, jobId: string) => unknown): ClosureJobs;
  /** Persistent account quotas include invisible pending reservations:
   * <=32768 distinct typed objects and <=512MiB; exact retries never recharge. */
  reserveObjectInTx(descriptor: Descriptor): void;
  /** Native-verified original bytes only. Confirm an actual durable R2 PUT and
   * exact size, returning an opaque request-owned confirmation, not JSON proof. */
  stageObject(descriptor: Descriptor, bytes: Uint8Array): Promise<unknown>;
  confirmObjectInTx(descriptor: Descriptor, confirmation: unknown): void;
  /** Atomic no-await callback: exact genesis generation1 CAS -> imported head
   * generation2, canonical retained proof, following-view roots, and irreversible
   * bootstrap seal. No checker verdict/profile or post-bootstrap main override. */
  attachBootstrapInTx(record: BootstrapImportRecord, root: ObjectKey, jobId: string): unknown;
}
interface ImportRow {
  id: string; account: string; principal: string; epoch: number; begin_request: string;
  begin_fingerprint: string; payload: string; job_id: string | null;
  object_count: number; finish_request: string | null; finish_fingerprint: string | null; finish_result: string | null;
}
interface ObjectRow { size: number; format: number; ready: number }
const encoder = new TextEncoder();
const MAX_RECORD = 4096;
const MAX_RECEIPT = 64 * 1024;
const MAX_ATTEMPTS = 4;
const MAX_OBJECTS = 32768;
function same(a: Descriptor, b: Descriptor): boolean { return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version; }
function encoded(value: unknown, maximum: number): string {
  let result: string | undefined; try { result = JSON.stringify(value); } catch { fail(503, 'failed_precondition'); }
  if (result === undefined || encoder.encode(result).byteLength > maximum) fail(413, 'resource_exhausted'); return result;
}

export class BootstrapImports {
  private readonly accountId: string;
  private readonly principal: string;
  constructor(private readonly host: BootstrapImportHost) {
    this.accountId = host.accountId; this.principal = host.principal;
    for (const statement of [
      'CREATE TABLE IF NOT EXISTS bootstrap_imports(id TEXT PRIMARY KEY,account TEXT NOT NULL,principal TEXT NOT NULL,epoch INTEGER NOT NULL,begin_request TEXT NOT NULL,begin_fingerprint TEXT NOT NULL,payload TEXT NOT NULL,job_id TEXT,object_count INTEGER NOT NULL DEFAULT 0,finish_request TEXT,finish_fingerprint TEXT,finish_result TEXT,UNIQUE(account,principal,begin_request))',
      'CREATE TABLE IF NOT EXISTS bootstrap_import_objects(import_id TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,size INTEGER NOT NULL,format INTEGER NOT NULL,ready INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(import_id,kind,id))',
    ]) host.sql.exec(statement);
  }
  private one<T>(query: string, ...bindings: (string | number | null)[]): T | undefined { return [...this.host.sql.exec(query, ...bindings)][0] as T | undefined; }
  private owner(): ImportOwner {
    const owner = this.host.requireOwnerInTx();
    if (owner.account_id !== this.accountId || owner.owner !== this.principal) fail(403, 'permission_denied');
    return owner;
  }
  private load(id: string): ImportRow {
    const row = this.one<ImportRow>('SELECT * FROM bootstrap_imports WHERE id=?', id);
    if (!row || row.account !== this.accountId || row.principal !== this.principal) fail(403, 'permission_denied'); return row;
  }
  private record(row: ImportRow): BootstrapImportRecord {
    return { id: row.id, account: row.account, principal: row.principal, epoch: row.epoch, begin_request: row.begin_request,
      begin_fingerprint: row.begin_fingerprint, payload: JSON.parse(row.payload), job_id: row.job_id };
  }
  private active(row: ImportRow, owner: ImportOwner): BootstrapImportRecord {
    if (owner.bootstrap_sealed !== false || row.finish_result !== null) fail(400, 'failed_precondition');
    if (owner.policy_epoch !== row.epoch) fail(409, 'aborted');
    const record = this.record(row); const genesis = this.host.canonicalGenesis();
    if (owner.main.generation !== 1 || owner.main.head !== genesis || record.payload.destination.generation !== 1
        || record.payload.destination.head !== genesis) fail(409, 'aborted');
    return record;
  }
  private jobs(record: BootstrapImportRecord, attach?: (binding: ClosureBinding, root: ObjectKey, jobId: string) => unknown): ClosureJobs {
    const binding: ClosureBinding = { account_id: record.account, identity: { kind: 'principal', principal: record.principal },
      policy_epoch: record.epoch, purpose: 'bootstrap_import',
      publication: encoded({ import_id: record.id, begin_fingerprint: record.begin_fingerprint, root: record.payload.root,
        source: record.payload.source, destination: record.payload.destination }, MAX_RECORD) };
    return this.host.makeClosureJobs(binding, attach ?? (() => fail(400, 'failed_precondition')));
  }
  async handle(call: ForgeCall): Promise<{ state: 'complete'; result: unknown }> {
    if (call.account_id !== this.accountId) fail(403, 'permission_denied');
    this.host.transactionSync(() => this.owner());
    if (![40, 41, 42, 43, 44].includes(call.operation)) fail(501, 'unimplemented');
    let command: ImportCommand;
    try { command = normalizeBootstrapImport({ account_id: call.account_id, operation: call.operation, view: call.view, request_id: call.request_id, payload_json: call.payload_json }); }
    catch (error) { if (error instanceof Error && error.message.startsWith('resource_exhausted:')) fail(413, 'resource_exhausted'); fail(400, 'invalid_argument'); }
    if (command.operation !== call.operation || !/^[0-9a-f]{64}$/.test(command.fingerprint)) fail(503, 'failed_precondition');
    switch (command.operation) {
      case 40: return { state: 'complete', result: await this.begin(call.request_id, command.fingerprint, command.payload) };
      case 41: return { state: 'complete', result: await this.put(command.payload) };
      case 42: return { state: 'complete', result: await this.advance(command.payload.import_id, command.payload.expected_generation) };
      case 43: return { state: 'complete', result: this.get(command.payload.import_id) };
      case 44: return { state: 'complete', result: this.finish(call.request_id, command.fingerprint, command.payload.import_id, command.payload.expected_generation) };
    }
  }
  private async begin(requestId: string, fingerprint: string, payload: ImportBegin): Promise<unknown> {
    const initial = this.host.transactionSync(() => {
      const owner = this.owner();
      const replay = this.host.replayInTx(requestId, fingerprint); if (replay !== undefined) return { replay };
      let row = this.one<ImportRow>('SELECT * FROM bootstrap_imports WHERE account=? AND principal=? AND begin_request=?', this.accountId, this.principal, requestId);
      if (row) {
        if (row.begin_fingerprint !== fingerprint || row.payload !== encoded(payload, MAX_RECORD)) fail(400, 'invalid_argument');
        this.active(row, owner); return { row };
      }
      if (owner.bootstrap_sealed !== false || owner.main.generation !== 1 || owner.main.head !== this.host.canonicalGenesis()
          || payload.destination.generation !== 1 || payload.destination.head !== owner.main.head) fail(400, 'failed_precondition');
      const count = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM bootstrap_imports WHERE account=?', this.accountId)!;
      if (Number(count.n) >= MAX_ATTEMPTS) fail(429, 'resource_exhausted');
      const id = crypto.randomUUID(); const body = encoded(payload, MAX_RECORD);
      this.host.sql.exec('INSERT INTO bootstrap_imports(id,account,principal,epoch,begin_request,begin_fingerprint,payload) VALUES(?,?,?,?,?,?,?)', id, this.accountId, this.principal, owner.policy_epoch, requestId, fingerprint, body);
      row = this.load(id); return { row };
    });
    if ('replay' in initial) return initial.replay;
    const row = initial.row!; const record = this.record(row);
    // Crash before link/receipt: this deterministic job intent finds the same
    // ClosureJobs row on retry; the pending import does not consume another slot.
    const progress = await this.jobs(record).begin(`bi-${record.id}`, record.payload.root);
    return this.host.transactionSync(() => {
      const owner = this.owner(); const current = this.load(row.id);
      const replay = this.host.replayInTx(requestId, fingerprint); if (replay !== undefined) return replay;
      const record = this.active(current, owner);
      if (current.begin_fingerprint !== fingerprint || (current.job_id !== null && current.job_id !== progress.job_id)) fail(400, 'invalid_argument');
      this.host.sql.exec('UPDATE bootstrap_imports SET job_id=? WHERE id=?', progress.job_id, row.id);
      const result = { import_id: row.id, closure_job_id: progress.job_id, root: record.payload.root, declared_source: record.payload.source,
        destination: record.payload.destination, policy_epoch: record.epoch, status: progress,
        limits: { object_bytes: 1024 * 1024, metadata_bytes: 256 * 1024, account_objects: MAX_OBJECTS, account_bytes: 512 * 1024 * 1024, attempts: MAX_ATTEMPTS } };
      encoded(result, MAX_RECEIPT); return this.host.finishInTx(requestId, fingerprint, result);
    });
  }
  private async put(payload: { import_id: string; descriptor: Descriptor; data_base64: string }): Promise<unknown> {
    const record = this.host.transactionSync(() => this.active(this.loadOwned(payload.import_id), this.owner()));
    if (record.job_id === null) fail(400, 'failed_precondition');
    // Native normalizer has already bounded canonical base64 and decoded size.
    // Still verify ORIGINAL bytes before even a descriptor-only historical ACK.
    let binary: string; try { binary = atob(payload.data_base64); } catch { fail(400, 'invalid_argument'); }
    const maximum = payload.descriptor.kind === 1 ? 1024 * 1024 : 256 * 1024;
    if (binary.length > maximum || binary.length !== payload.descriptor.size_bytes) fail(400, 'invalid_argument');
    const bytes = Uint8Array.from(binary, byte => byte.charCodeAt(0));
    let verified: Descriptor; try { verified = inspect(payload.descriptor.kind, bytes); } catch { fail(400, 'invalid_argument'); }
    if (!same(payload.descriptor, verified)) fail(400, 'invalid_argument');
    const descriptor: Descriptor = { kind: verified.kind, id: verified.id, size_bytes: verified.size_bytes, format_version: verified.format_version };
    const previous = this.host.transactionSync(() => {
      const row = this.loadOwned(record.id); this.active(row, this.owner());
      const previous = this.one<ObjectRow>('SELECT size,format,ready FROM bootstrap_import_objects WHERE import_id=? AND kind=? AND id=?', record.id, descriptor.kind, descriptor.id);
      if (previous && (Number(previous.size) !== descriptor.size_bytes || Number(previous.format) !== descriptor.format_version)) fail(400, 'invalid_argument');
      if (previous?.ready) return true;
      this.host.reserveObjectInTx(descriptor);
      if (!previous) {
        if (row.object_count >= MAX_OBJECTS) fail(429, 'resource_exhausted');
        this.host.sql.exec('INSERT INTO bootstrap_import_objects(import_id,kind,id,size,format) VALUES(?,?,?,?,?)', record.id, descriptor.kind, descriptor.id, descriptor.size_bytes, descriptor.format_version);
        this.host.sql.exec('UPDATE bootstrap_imports SET object_count=object_count+1 WHERE id=?', record.id);
      }
      return false;
    });
    const result = { import_id: record.id, ...descriptor, state: 'ready' };
    if (previous) return result;
    const confirmation = await this.host.stageObject(descriptor, bytes);
    return this.host.transactionSync(() => {
      this.active(this.loadOwned(record.id), this.owner());
      const row = this.one<ObjectRow>('SELECT size,format,ready FROM bootstrap_import_objects WHERE import_id=? AND kind=? AND id=?', record.id, descriptor.kind, descriptor.id);
      if (!row || Number(row.size) !== descriptor.size_bytes || Number(row.format) !== descriptor.format_version) fail(503, 'failed_precondition');
      this.host.confirmObjectInTx(descriptor, confirmation);
      this.host.sql.exec('UPDATE bootstrap_import_objects SET ready=1 WHERE import_id=? AND kind=? AND id=?', record.id, descriptor.kind, descriptor.id);
      return result;
    });
  }
  private loadOwned(id: string): ImportRow { this.owner(); return this.load(id); }
  private async advance(id: string, generation: number): Promise<unknown> {
    const record = this.host.transactionSync(() => { const owner = this.owner(); return this.active(this.load(id), owner); });
    if (record.job_id === null) fail(400, 'failed_precondition');
    const progress = await this.jobs(record).advance(record.job_id, generation);
    this.host.transactionSync(() => this.active(this.loadOwned(id), this.owner()));
    return { import_id: id, status: progress };
  }
  private get(id: string): unknown {
    const snapshot = this.host.transactionSync(() => {
      const owner = this.owner(); const row = this.load(id); const record = this.record(row);
      if (row.finish_result !== null) return { result: { import_id: id, state: 'published', receipt: JSON.parse(row.finish_result) } };
      const obsolete = owner.policy_epoch !== record.epoch;
      if (owner.bootstrap_sealed !== false || obsolete || record.job_id === null) return { result: { import_id: id, root: record.payload.root,
        declared_source: record.payload.source, destination: record.payload.destination, state: owner.bootstrap_sealed !== false ? 'closed' : obsolete ? 'obsolete' : 'pending_begin',
        policy_epoch: record.epoch, obsolete, bootstrap_sealed: owner.bootstrap_sealed, status: null } };
      return { record };
    });
    if ('result' in snapshot) return snapshot.result;
    const record = snapshot.record!;
    // Instantiate/query ClosureJobs OUTSIDE the wrapper tx: its own methods
    // establish current auth and transaction boundaries, without nested txs.
    const status = this.jobs(record).status(record.job_id!);
    this.host.transactionSync(() => { const owner = this.owner(); this.active(this.load(id), owner); });
    return { import_id: id, root: record.payload.root, declared_source: record.payload.source, destination: record.payload.destination,
      policy_epoch: record.epoch, obsolete: false, bootstrap_sealed: false, status };
  }
  private finish(requestId: string, fingerprint: string, id: string, generation: number): unknown {
    const initial = this.host.transactionSync(() => {
      const owner = this.owner(); const row = this.load(id);
      // Historical completion stays replayable by CURRENT owner after later
      // epoch/main movement, without weakening generic ClosureJobs epoch rules.
      const replay = this.host.replayInTx(requestId, fingerprint); if (replay !== undefined) return { replay };
      if (row.finish_result !== null) {
        if (row.finish_request !== requestId || row.finish_fingerprint !== fingerprint) fail(400, 'invalid_argument');
        return { replay: JSON.parse(row.finish_result) };
      }
      return { record: this.active(row, owner) };
    });
    if ('replay' in initial) return initial.replay;
    const record = initial.record!; if (record.job_id === null) fail(400, 'failed_precondition');
    const jobs = this.jobs(record, (binding, root, jobId) => {
      const owner = this.owner(); const row = this.load(id); const current = this.active(row, owner);
      if (current.job_id !== jobId || root.kind !== 3 || root.id !== current.payload.root.id || binding.purpose !== 'bootstrap_import'
          || binding.identity.kind !== 'principal' || binding.identity.principal !== this.principal || binding.policy_epoch !== current.epoch) fail(400, 'failed_precondition');
      // This callback is ALREADY inside ClosureJobs.publish's SQL transaction.
      const result = this.host.attachBootstrapInTx(current, root, jobId);
      const after = this.owner();
      if (after.bootstrap_sealed !== true || after.main.generation !== 2 || after.main.head !== root.id || after.policy_epoch !== current.epoch) fail(503, 'failed_precondition');
      const receipt = encoded(result, MAX_RECEIPT);
      this.host.sql.exec('UPDATE bootstrap_imports SET finish_request=?,finish_fingerprint=?,finish_result=? WHERE id=?', requestId, fingerprint, receipt, id);
      return this.host.finishInTx(requestId, fingerprint, result);
    });
    return jobs.publish(record.job_id, generation).result;
  }
}
