/** Internal account maintenance. Roots and direct custody come only from the
 * trusted host; canonical graph semantics remain in ClosureJobs/Wasm. Physical
 * victims, DELETE retries and charge settlement belong solely to ForgeStorage. */
import type { ObjectKey } from './core';
import type { ClosureJobs, ClosureJobStatus } from './closure_jobs';
import type { ForgeStorage, StorageAttempt } from './forge_storage';
import { fail, Failure } from './protocol';

export interface GcRoot extends ObjectKey { tree_prefix: string }
export interface GcBinding { account_id: string; gc_id: string; epoch: number }
export interface ForgeGcHost {
  accountId: string;
  sql: SqlStorage;
  transactionSync<T>(callback: () => T): T;
  /** Strictly increasing (kind,id,UTF-8 tree_prefix), distinct contexts. All sources are trusted SQL. */
  rootPageInTx(after: GcRoot | null, limit: number): { roots: GcRoot[]; next: GcRoot | null };
  /** Direct protection only: an incomplete upload is not a validated root. */
  custodyPageInTx(after: ObjectKey | null, limit: number): { objects: ObjectKey[]; next: ObjectKey | null };
  /** purpose=account-gc; publication=JSON.stringify([gc_id,epoch,kind,id]);
   * tree_prefix separately binds exact TREE context; authorization calls requireBindingInTx. Never publish this scratch proof. */
  closureForRoot(binding: GcBinding, root: GcRoot): ClosureJobs;
  /** Its mutation callback must pass this exact binding to mutationInTx. */
  storageForMaintenance(binding: GcBinding): ForgeStorage;
}
type Phase = 'roots' | 'custody' | 'mark' | 'sweep' | 'abort' | 'compact' | 'complete' | 'failed';
interface Job {
  id: string; account: string; epoch: number; generation: number; phase: Phase;
  root_cursor: string | null; custody_cursor: string | null; attempt_cursor: string | null; pending_delete: string | null;
  roots: number; marked: number; strict_work: number; retired: number; deleted: number;
  skipped_pending: number; error: string | null;
}
interface Root extends GcRoot { job: string | null; generation: number | null; phase: 'pending' | 'copy' | 'dispose' | 'done'; copy_kind: number; copy_id: string }
interface Gate { account: string; epoch: number; active: string | null }
export interface GcStatus {
  gc_id: string; epoch: number; generation: number; phase: Phase; complete: boolean;
  counts: { roots: number; marked: number; strict_work: number; retired: number; deleted: number; skipped_pending: number };
  error: string | null;
}
const PAGE = 128, MAX_ROOTS = 4096, MAX_MARKS = 32768, MAX_WORK = 2_000_000, MAX_JOBS = 16;
const ROOT = '0'.repeat(64);
function validKey(key: ObjectKey): boolean { return !!key && Number.isInteger(key.kind) && key.kind >= 1 && key.kind <= 5 && /^[0-9a-f]{64}$/.test(key.id) && !(key.kind === 3 && key.id === ROOT); }
function compare(a: ObjectKey, b: ObjectKey): number { return a.kind - b.kind || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0); }
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
function validRoot(root: GcRoot): boolean {
  if (!validKey(root) || typeof root.tree_prefix !== 'string') return false;
  const bytes = encoder.encode(root.tree_prefix);
  if (bytes.byteLength > 4096 || decoder.decode(bytes) !== root.tree_prefix) return false;
  if (root.kind !== 2) return root.tree_prefix === '';
  if (root.tree_prefix === '') return true;
  const parts = root.tree_prefix.split('/');
  return !parts.some(part => !part || part === '.' || part === '..' || /[\\\u0000-\u001f\u007f-\u009f]/u.test(part));
}
/** SQLite BINARY compares UTF-8 bytes, not JavaScript UTF-16 code units. */
function compareRoot(a: GcRoot, b: GcRoot): number {
  const key = compare(a, b); if (key) return key;
  const left = encoder.encode(a.tree_prefix), right = encoder.encode(b.tree_prefix);
  for (let i = 0; i < Math.min(left.length, right.length); i++) if (left[i] !== right[i]) return left[i] - right[i];
  return left.length - right.length;
}
function rootCursor(value: string | null): GcRoot | null {
  const root = cursor(value) as GcRoot | null;
  if (root !== null && !validRoot(root)) fail(500, 'data_loss'); return root;
}
async function rootRequest(gc: string, root: GcRoot): Promise<string> {
  const bytes = await crypto.subtle.digest('SHA-256', encoder.encode(JSON.stringify([root.kind, root.id, root.tree_prefix])));
  const hash = Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, '0')).join('');
  return `g-${gc}-${hash}`;
}
function cursor(value: string | null): ObjectKey | null {
  if (value === null) return null;
  let key: ObjectKey; try { key = JSON.parse(value); } catch { fail(500, 'data_loss'); }
  if (!validKey(key)) fail(500, 'data_loss'); return key;
}

export class ForgeGc {
  constructor(private readonly host: ForgeGcHost) {
    for (const sql of [
      'CREATE TABLE IF NOT EXISTS gc_maintenance(singleton INTEGER PRIMARY KEY CHECK(singleton=1),account TEXT NOT NULL,epoch INTEGER NOT NULL,active TEXT)',
      'CREATE TABLE IF NOT EXISTS gc_jobs(id TEXT PRIMARY KEY,account TEXT NOT NULL,epoch INTEGER NOT NULL,generation INTEGER NOT NULL,phase TEXT NOT NULL,root_cursor TEXT,custody_cursor TEXT,attempt_cursor TEXT,pending_delete TEXT,roots INTEGER NOT NULL,marked INTEGER NOT NULL,strict_work INTEGER NOT NULL,retired INTEGER NOT NULL,deleted INTEGER NOT NULL,skipped_pending INTEGER NOT NULL,error TEXT)',
      'CREATE TABLE IF NOT EXISTS gc_roots(gc TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,tree_prefix TEXT NOT NULL,job TEXT,generation INTEGER,phase TEXT NOT NULL,copy_kind INTEGER NOT NULL DEFAULT 0,copy_id TEXT NOT NULL DEFAULT \'\',PRIMARY KEY(gc,kind,id,tree_prefix))',
      'CREATE TABLE IF NOT EXISTS gc_marks(gc TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,PRIMARY KEY(gc,kind,id))',
    ]) host.sql.exec(sql);
    host.transactionSync(() => {
      host.sql.exec('INSERT OR IGNORE INTO gc_maintenance(singleton,account,epoch,active) VALUES(1,?,1,NULL)', host.accountId);
      this.gate();
    });
  }
  private one<T>(sql: string, ...args: (string | number | null)[]): T | undefined { return [...this.host.sql.exec(sql, ...args)][0] as T | undefined; }
  private gate(): Gate {
    const gate = this.one<Gate>('SELECT account,epoch,active FROM gc_maintenance WHERE singleton=1');
    if (!gate || gate.account !== this.host.accountId) fail(403, 'permission_denied');
    if (!Number.isSafeInteger(gate.epoch) || gate.epoch < 1) fail(500, 'data_loss'); return gate;
  }
  mutationInTx(mode: 'authoring' | 'maintenance', binding?: GcBinding): void {
    if (mode === 'maintenance') { if (!binding) fail(409, 'gc_binding_required'); this.requireBindingInTx(binding); return; }
    const gate = this.gate(); if (gate.active !== null) fail(503, 'account_maintenance');
    if (gate.epoch >= Number.MAX_SAFE_INTEGER) fail(429, 'resource_exhausted');
    this.host.sql.exec('UPDATE gc_maintenance SET epoch=epoch+1 WHERE singleton=1');
  }
  requireBindingInTx(binding: GcBinding): void {
    const gate = this.gate();
    if (binding.account_id !== gate.account || binding.gc_id !== gate.active || binding.epoch !== gate.epoch) fail(409, 'aborted');
  }
  private binding(job: Job): GcBinding { return { account_id: job.account, gc_id: job.id, epoch: job.epoch }; }
  private load(id: string, generation?: number): Job {
    const job = this.one<Job>('SELECT * FROM gc_jobs WHERE id=?', id);
    if (!job || job.account !== this.host.accountId) fail(404, 'not_found');
    if (generation !== undefined && (!Number.isSafeInteger(generation) || generation !== job.generation)) fail(409, 'aborted');
    if (job.phase !== 'complete' && job.phase !== 'failed') this.requireBindingInTx(this.binding(job));
    return job;
  }
  private report(j: Job): GcStatus {
    return { gc_id: j.id, epoch: j.epoch, generation: j.generation, phase: j.phase, complete: j.phase === 'complete',
      counts: { roots: j.roots, marked: j.marked, strict_work: j.strict_work, retired: j.retired, deleted: j.deleted, skipped_pending: j.skipped_pending }, error: j.error };
  }
  private save(j: Job): void {
    if (j.generation >= Number.MAX_SAFE_INTEGER) fail(429, 'resource_exhausted'); j.generation++;
    this.host.sql.exec('UPDATE gc_jobs SET generation=?,phase=?,root_cursor=?,custody_cursor=?,attempt_cursor=?,pending_delete=?,roots=?,marked=?,strict_work=?,retired=?,deleted=?,skipped_pending=?,error=? WHERE id=?',
      j.generation,j.phase,j.root_cursor,j.custody_cursor,j.attempt_cursor,j.pending_delete,j.roots,j.marked,j.strict_work,j.retired,j.deleted,j.skipped_pending,j.error,j.id);
  }
  active(): GcStatus | undefined { return this.host.transactionSync(() => { const gate = this.gate(); return gate.active === null ? undefined : this.report(this.load(gate.active)); }); }
  status(id: string): GcStatus { return this.host.transactionSync(() => this.report(this.load(id))); }
  begin(): GcStatus {
    return this.host.transactionSync(() => {
      const gate = this.gate(); if (gate.active !== null) return this.report(this.load(gate.active));
      // Completed runs retain only a compact summary; keep at most 16 summaries.
      const count = Number(this.one<{ n: number }>('SELECT COUNT(*) AS n FROM gc_jobs')!.n);
      if (count >= MAX_JOBS) {
        const old = this.one<{ id: string }>("SELECT id FROM gc_jobs WHERE phase IN('complete','failed') ORDER BY rowid LIMIT 1");
        if (!old) fail(429, 'resource_exhausted'); this.host.sql.exec('DELETE FROM gc_jobs WHERE id=?', old.id);
      }
      const id = crypto.randomUUID();
      this.host.sql.exec("INSERT INTO gc_jobs(id,account,epoch,generation,phase,roots,marked,strict_work,retired,deleted,skipped_pending) VALUES(?,?,?,0,'roots',0,0,0,0,0,0)", id,gate.account,gate.epoch);
      this.host.sql.exec('UPDATE gc_maintenance SET active=? WHERE singleton=1', id);
      return this.report(this.load(id));
    });
  }
  private checkPage(keys: ObjectKey[], after: ObjectKey | null, next: ObjectKey | null, contextual = false): void {
    if (!Array.isArray(keys) || keys.length > PAGE) fail(500, 'data_loss');
    let previous = after;
    const valid = (key: ObjectKey): boolean => contextual ? validRoot(key as GcRoot) : validKey(key);
    const order = (a: ObjectKey, b: ObjectKey): number => contextual ? compareRoot(a as GcRoot, b as GcRoot) : compare(a, b);
    for (const key of keys) { if (!valid(key) || (previous && order(previous,key) >= 0)) fail(500, 'data_loss'); previous = key; }
    if (next !== null && (!keys.length || !valid(next) || !previous || order(next,previous) !== 0)) fail(500, 'data_loss');
  }
  private mark(j: Job, key: ObjectKey): void {
    if (!validKey(key)) fail(500, 'data_loss');
    if (this.one('SELECT 1 FROM gc_marks WHERE gc=? AND kind=? AND id=?',j.id,key.kind,key.id)) return;
    if (j.marked >= MAX_MARKS) fail(429, 'gc_mark_limit');
    this.host.sql.exec('INSERT INTO gc_marks(gc,kind,id) VALUES(?,?,?)',j.id,key.kind,key.id); j.marked++;
  }
  private inventories(j: Job): void {
    if (j.phase === 'roots') {
      const page = this.host.rootPageInTx(rootCursor(j.root_cursor),PAGE); this.checkPage(page.roots,rootCursor(j.root_cursor),page.next,true);
      for (const key of page.roots) {
        if (j.roots >= MAX_ROOTS) fail(429,'gc_root_limit');
        this.host.sql.exec("INSERT INTO gc_roots(gc,kind,id,tree_prefix,phase) VALUES(?,?,?,?,'pending')",j.id,key.kind,key.id,key.tree_prefix); j.roots++;
      }
      j.root_cursor = page.next === null ? null : JSON.stringify(page.next); if (page.next === null) j.phase = 'custody';
    } else {
      const page = this.host.custodyPageInTx(cursor(j.custody_cursor),PAGE); this.checkPage(page.objects,cursor(j.custody_cursor),page.next);
      for (const key of page.objects) this.mark(j,key);
      j.custody_cursor = page.next === null ? null : JSON.stringify(page.next); if (page.next === null) j.phase = 'mark';
    }
    this.save(j);
  }
  private proofReady(status: ClosureJobStatus): void {
    const c = status.counts;
    if (!status.complete || status.phase !== 'ready' || c.objects !== c.checked_objects || c.contexts !== c.checked_contexts || c.obligations !== c.checked_obligations) fail(409,'gc_proof_not_ready');
  }
  private async markRoot(j: Job): Promise<void> {
    const root = this.host.transactionSync(() => {
      this.load(j.id,j.generation);
      return this.one<Root>("SELECT kind,id,tree_prefix,job,generation,phase,copy_kind,copy_id FROM gc_roots WHERE gc=? AND phase<>'done' ORDER BY kind,id,tree_prefix COLLATE BINARY LIMIT 1",j.id);
    });
    if (!root) { this.host.transactionSync(() => { const current=this.load(j.id,j.generation); current.phase='sweep'; this.save(current); }); return; }
    const binding=this.binding(j), jobs=this.host.closureForRoot(binding,root);
    if (root.phase === 'pending') {
      let status = root.job === null ? await jobs.begin(await rootRequest(j.id,root),root) : jobs.status(root.job);
      if (status.phase === 'limit') fail(409,'gc_closure_blocked');
      if (root.job !== null && !status.complete) status=await jobs.advance(status.job_id,status.generation);
      this.host.transactionSync(() => {
        const current=this.load(j.id,j.generation);
        if (status.error || status.phase === 'limit') fail(409,'gc_closure_blocked');
        if (status.complete) {
          this.proofReady(status);
          const c=status.counts, work=c.objects+c.edges+c.contexts+c.obligations;
          if (!Number.isSafeInteger(work) || current.strict_work+work>MAX_WORK) fail(429,'gc_work_limit'); current.strict_work+=work;
        }
        this.host.sql.exec('UPDATE gc_roots SET job=?,generation=?,phase=? WHERE gc=? AND kind=? AND id=? AND tree_prefix=?',status.job_id,status.generation,status.complete?'copy':'pending',j.id,root.kind,root.id,root.tree_prefix);
        this.save(current);
      });
      return;
    }
    if (root.job === null || root.generation === null) fail(500,'data_loss');
    const proofStatus = root.phase === 'copy' ? jobs.status(root.job) : undefined;
    this.host.transactionSync(() => {
      const current=this.load(j.id,j.generation);
      if (root.phase === 'copy') {
        this.proofReady(proofStatus!);
        if (proofStatus!.generation !== root.generation) fail(409,'aborted');
        const page=[...this.host.sql.exec('SELECT kind,id FROM closure_nodes WHERE job=? AND done=1 AND (kind>? OR (kind=? AND id>?)) ORDER BY kind,id LIMIT ?',root.job,root.copy_kind,root.copy_kind,root.copy_id,PAGE)] as unknown as ObjectKey[];
        for(const key of page) this.mark(current,key);
        const last=page[page.length-1];
        this.host.sql.exec('UPDATE gc_roots SET phase=?,copy_kind=?,copy_id=? WHERE gc=? AND kind=? AND id=? AND tree_prefix=?',page.length<PAGE?'dispose':'copy',last?.kind??root.copy_kind,last?.id??root.copy_id,j.id,root.kind,root.id,root.tree_prefix);
      } else {
        if(jobs.disposeGcScratchInTx(root.job!,root.generation!,PAGE)) this.host.sql.exec("UPDATE gc_roots SET phase='done' WHERE gc=? AND kind=? AND id=? AND tree_prefix=?",j.id,root.kind,root.id,root.tree_prefix);
      }
      this.save(current);
    });
  }
  private discardSettledLogicalInTx(j: Job, storage: ForgeStorage, key: ObjectKey): void {
    // Also needed when a previous DELETE settled but its GC cursor ACK was lost.
    // Marks, live reservations, pending writes or a replacement incarnation all
    // independently prevent logical-accounting release.
    if (!this.one('SELECT 1 FROM gc_marks WHERE gc=? AND kind=? AND id=?', j.id, key.kind, key.id)
        && !this.one('SELECT 1 FROM forge_storage_reservations WHERE kind=? AND id=? LIMIT 1', key.kind, key.id)
        && !this.one("SELECT 1 FROM forge_storage_attempts WHERE kind=? AND id=? AND state='pending' LIMIT 1", key.kind, key.id)
        && !storage.incarnationInTx(key)) storage.discardLogicalInTx(key);
  }
  private async sweep(j: Job): Promise<void> {
    const storage = this.host.storageForMaintenance(this.binding(j));
    // Checkpoint a bounded metadata-only prefix before the first physical await.
    // A failed HEAD/DELETE leaves exactly that attempt unconsumed, while all
    // preceding retained/deleted entries remain durably skipped on restart.
    const selected = this.host.transactionSync(() => {
      const current = this.load(j.id, j.generation);
      const page = storage.attemptsInTx(current.attempt_cursor, PAGE);
      if (current.pending_delete !== null) {
        // Selection/retirement checkpoint precedes DELETE. It must remain the
        // first unconsumed attempt until its exact receipt is acknowledged.
        const first = page.attempts[0];
        if (!first || first.attempt_id !== current.pending_delete
            || (first.state !== 'retiring' && first.state !== 'deleted')) fail(409, 'gc_delete_binding');
      }
      let action: StorageAttempt | undefined;
      let last = false;
      for (let i = 0; i < page.attempts.length; i++) {
        const attempt = page.attempts[i];
        if (attempt.state === 'pending') {
          action = attempt;
        } else if (attempt.state === 'deleted') {
          this.discardSettledLogicalInTx(current, storage, attempt.descriptor);
          if (current.pending_delete === attempt.attempt_id) {
            current.deleted++;
            current.pending_delete = null;
          }
        } else {
          const marked = !!this.one('SELECT 1 FROM gc_marks WHERE gc=? AND kind=? AND id=?', current.id, attempt.descriptor.kind, attempt.descriptor.id);
          const ownedUnbound = !attempt.current && attempt.state === 'written'
            && !!this.one('SELECT 1 FROM forge_storage_reservations WHERE kind=? AND id=? LIMIT 1', attempt.descriptor.kind, attempt.descriptor.id)
            && !storage.incarnationInTx(attempt.descriptor);
          if (!(attempt.current && marked) && !ownedUnbound) {
            const incarnation = attempt.current ? storage.incarnationInTx(attempt.descriptor) : undefined;
            if (attempt.current && (!incarnation || incarnation.attempt_id !== attempt.attempt_id)) fail(409, 'aborted');
            if (current.pending_delete !== null && current.pending_delete !== attempt.attempt_id) fail(409, 'gc_delete_binding');
            storage.retireInTx(attempt.attempt_id, incarnation);
            current.pending_delete = attempt.attempt_id;
            if (attempt.state === 'written') current.retired++;
            action = attempt;
          }
        }
        if (action) {
          last = page.next === null && i === page.attempts.length - 1;
          break;
        }
        current.attempt_cursor = attempt.attempt_id;
      }
      if (!action && page.next === null) {
        if (current.pending_delete !== null) fail(409, 'gc_delete_binding');
        current.phase = 'compact';
      }
      this.save(current);
      return { current, action, last };
    });
    const { current, action, last } = selected;
    if (!action) return;
    if (action.state === 'pending') {
      // Absence cannot establish that the original PUT ended. One HEAD only;
      // a proven write is retired/deleted by a subsequent quantum, not this one.
      const recovered = await storage.recoverAttempt(action.attempt_id);
      this.host.transactionSync(() => {
        const live = this.load(j.id, current.generation);
        if (recovered.attempt_id !== action.attempt_id) fail(500, 'data_loss');
        if (recovered.state === 'pending') {
          live.skipped_pending++;
          live.attempt_cursor = action.attempt_id;
          if (last) live.phase = 'compact';
        } else if (recovered.state !== 'written') {
          fail(409, 'aborted');
        }
        this.save(live);
      });
      return;
    }
    // Retirement and the prefix checkpoint already committed together. DELETE
    // and its exact-once physical accounting remain exclusively storage-owned.
    await storage.deleteRetired(action.attempt_id);
    this.host.transactionSync(() => {
      const live = this.load(j.id, current.generation);
      if (live.pending_delete !== action.attempt_id) fail(409, 'gc_delete_binding');
      this.discardSettledLogicalInTx(live, storage, action.descriptor);
      live.deleted++;
      live.pending_delete = null;
      live.attempt_cursor = action.attempt_id;
      if (last) live.phase = 'compact';
      this.save(live);
    });
  }
  private compact(j: Job): void {
    if (j.pending_delete !== null) fail(409, 'gc_delete_binding');
    for(const table of ['gc_roots','gc_marks']) {
      const page=[...this.host.sql.exec(`SELECT rowid AS row FROM ${table} WHERE gc=? LIMIT ?`,j.id,PAGE)] as unknown as {row:number}[];
      for(const row of page) this.host.sql.exec(`DELETE FROM ${table} WHERE rowid=? AND gc=?`,row.row,j.id);
      if(page.length) { this.save(j); return; }
    }
    j.phase=j.error === null ? 'complete' : 'failed';this.save(j);this.host.sql.exec('UPDATE gc_maintenance SET active=NULL WHERE singleton=1 AND active=?',j.id);
  }
  private async abort(j: Job): Promise<void> {
    const root = this.host.transactionSync(() => {
      this.load(j.id, j.generation);
      return this.one<Root>("SELECT kind,id,tree_prefix,job,generation,phase,copy_kind,copy_id FROM gc_roots WHERE gc=? ORDER BY kind,id,tree_prefix COLLATE BINARY LIMIT 1", j.id);
    });
    if (root) {
      const jobs = this.host.closureForRoot(this.binding(j), root);
      const request = await rootRequest(j.id, root);
      // begin() may have committed before its caller saved the returned ID.
      // Recover only the exact internal root request, never a product proof.
      const scratch = this.host.transactionSync(() => {
        this.load(j.id, j.generation);
        const found = [...this.host.sql.exec("SELECT id,generation FROM closure_jobs WHERE account=? AND request_id=? AND json_extract(binding,'$[3]')='account-gc' AND json_extract(binding,'$[4]')=? LIMIT 2",
          j.account, request, JSON.stringify([j.id,j.epoch,root.kind,root.id]))] as unknown as {id:string;generation:number}[];
        if (found.length > 1) fail(500, 'data_loss');
        return found[0];
      });
      this.host.transactionSync(() => {
        const current = this.load(j.id, j.generation);
        if (!scratch || jobs.disposeGcScratchInTx(scratch.id, scratch.generation, PAGE, 'aborted')) {
          this.host.sql.exec('DELETE FROM gc_roots WHERE gc=? AND kind=? AND id=? AND tree_prefix=?', j.id, root.kind, root.id, root.tree_prefix);
        }
        this.save(current);
      });
      return;
    }
    this.host.transactionSync(() => this.compact(this.load(j.id, j.generation)));
  }
  async advance(id: string, expectedGeneration: number): Promise<GcStatus> {
    const job = this.host.transactionSync(() => this.load(id, expectedGeneration));
    if (job.phase === 'complete' || job.phase === 'failed') return this.report(job);
    try {
      if (job.phase === 'roots' || job.phase === 'custody') this.host.transactionSync(() => this.inventories(this.load(id, expectedGeneration)));
      else if (job.phase === 'mark') await this.markRoot(job);
      else if (job.phase === 'sweep') await this.sweep(job);
      else if (job.phase === 'abort') await this.abort(job);
      else this.host.transactionSync(() => this.compact(this.load(id, expectedGeneration)));
    } catch (error) {
      // No physical retirement can have started in these phases. Persist a
      // bounded failure and discard only our own scratch before lifting repair
      // writes. Sweep failures instead retain exact custody for retry.
      if (job.phase !== 'roots' && job.phase !== 'custody' && job.phase !== 'mark') throw error;
      this.host.transactionSync(() => {
        const current = this.load(id, expectedGeneration);
        current.phase = 'abort';
        current.error = error instanceof Failure ? error.code : 'gc_mark_failed';
        this.save(current);
      });
    }
    return this.status(id);
  }
}
