/** Public bounded view presentation over current-policy opaque views and exact
 * ready projection catalogs. R2 availability or a known digest is never a grant.
 * This class is constructed per request, never cached with an actor on the DO. */
import { normalizeForgeReadCommand, type Descriptor, type ForgeCall, type ObjectKey } from './core';
import type { ForgeViews, ResolvedView, ViewDescription } from './forge_views';
import type { ForgeProjections, ProjectionPath, ProjectionStatus } from './forge_projection';
import { fail } from './protocol';

export type ReadCommand = {
  operation: 7; version: number[] | null; path: string;
} | {
  operation: 9; version: number[] | null; kind: number; id: number[]; offset: number; max_bytes: number;
};
export interface ForgeReadHost {
  readonly accountId: string;
  readonly sql: SqlStorage;
  readonly views: ForgeViews;
  readonly projections: ForgeProjections;
  transactionSync<T>(callback: () => T): T;
  /** Verify complete canonical bytes from the authoritative immutable payload.
   * Called only after exact view/catalog membership and preview-size admission. */
  readVerifiedObject(key: ObjectKey): Promise<{ descriptor: Descriptor; bytes: Uint8Array }>;
  /** Read only from the immutable payload bound to this verified descriptor.
   * Never resolve a different key or return an unverified mutable R2 version. */
  readRange(descriptor: Descriptor, offset: number, length: number): Promise<Uint8Array>;
}
const MAX_PATHS = 1024;
const MAX_PREVIEW = 1024 * 1024;
const MAX_CHUNK = 64 * 1024;
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const encoder = new TextEncoder();
class PreviewFailure extends Error {
  constructor(readonly reason: 'utf8' | 'size') { super(reason); }
}
function hex(value: number[]): string {
  if (!Array.isArray(value) || value.length !== 32 || !value.every(b => Number.isInteger(b) && b >= 0 && b <= 255)) fail(503, 'unavailable');
  return value.map(b => b.toString(16).padStart(2, '0')).join('');
}
function same(a: Descriptor, b: Descriptor): boolean {
  return a.kind === b.kind && a.id === b.id && a.size_bytes === b.size_bytes && a.format_version === b.format_version;
}
function sameId(a: number[], b: number[]): boolean { return hex(a) === hex(b); }
function asciiLower(value: string): string { return value.replace(/[A-Z]/g, c => c.toLowerCase()); }
function checkedDescription(view: ResolvedView, account: string): Readonly<ViewDescription> {
  const d = view.description;
  if (d.account_id !== account || !Number.isSafeInteger(d.policy_epoch) || d.policy_epoch < 1) fail(503, 'unavailable');
  hex(d.view_version); hex(d.revision);
  return d;
}
function previewMessage(error: PreviewFailure): string {
  return error.reason === 'utf8' ? 'forge file preview requires UTF-8' : 'forge file preview exceeds byte limit';
}

export class ForgeReads {
  constructor(private readonly host: ForgeReadHost) {}
  private checkInTx(view: ResolvedView): void {
    this.host.views.checkInTx(view);
    checkedDescription(view, this.host.accountId);
  }
  private readyInTx(view: ResolvedView): NonNullable<ReturnType<ForgeProjections['readyInTx']>> {
    this.checkInTx(view);
    const ready = this.host.projections.readyInTx(view);
    if (!ready) fail(400, 'failed_precondition');
    return ready;
  }
  private pathsInTx(view: ResolvedView): ProjectionPath[] {
    this.readyInTx(view);
    const paths = this.host.projections.pathsInTx(view);
    if (paths.length > MAX_PATHS || paths.reduce((bytes, p) => bytes + encoder.encode(p.path).byteLength, 0) > 1024 * 1024) fail(429, 'resource_exhausted');
    return paths;
  }
  private fileInTx(view: ResolvedView, path: string): Descriptor {
    const file = this.pathsInTx(view).find(file => file.path === path);
    if (!file) fail(403, 'permission_denied');
    if (file.descriptor.kind !== 1) fail(400, 'invalid_argument'); // Never follow symlinks.
    const membership = this.host.projections.memberInTx(view, file.descriptor);
    if (!same(membership.descriptor, file.descriptor)) fail(503, 'unavailable');
    return membership.descriptor;
  }
  private recheckFileInTx(view: ResolvedView, path: string, expected: Descriptor): void {
    const current = this.fileInTx(view, path);
    if (!same(expected, current)) fail(409, 'aborted');
  }
  private async file(view: ResolvedView, path: string): Promise<{ path: string; content: string }> {
    const expected = this.host.transactionSync(() => this.fileInTx(view, path));
    if (expected.size_bytes > MAX_PREVIEW) throw new PreviewFailure('size');
    let loaded: Awaited<ReturnType<ForgeReadHost['readVerifiedObject']>>;
    try { loaded = await this.host.readVerifiedObject(expected); }
    catch (error) {
      this.host.transactionSync(() => this.recheckFileInTx(view, path, expected));
      throw error;
    }
    this.host.transactionSync(() => this.recheckFileInTx(view, path, expected));
    if (!same(expected, loaded.descriptor) || loaded.bytes.byteLength !== expected.size_bytes) fail(503, 'unavailable');
    let content: string;
    try { content = decoder.decode(loaded.bytes); }
    catch { throw new PreviewFailure('utf8'); }
    return { path, content };
  }
  private status(view: ResolvedView, status: ProjectionStatus): ProjectionStatus {
    this.host.transactionSync(() => this.checkInTx(view));
    const d = checkedDescription(view, this.host.accountId); const s = status.selection;
    if (s.account_id !== d.account_id || s.view !== d.view || s.policy_epoch !== d.policy_epoch
        || s.root_prefix !== d.root_prefix || !sameId(s.view_version, d.view_version) || !sameId(s.revision, d.revision)
        || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(status.job_id)
        || !Number.isSafeInteger(status.generation) || status.generation < 0) fail(503, 'unavailable');
    if (status.state === 'limit') fail(429, 'resource_exhausted');
    if (status.state === 'unavailable') fail(503, 'unavailable');
    if (status.state !== 'pending' && status.state !== 'ready') fail(503, 'unavailable');
    return status;
  }
  private pending(view: ResolvedView, status: ProjectionStatus): { state: 'pending'; result: unknown } {
    const d = checkedDescription(view, this.host.accountId);
    return { state: 'pending', result: { account_id: d.account_id, view: d.view, view_version: d.view_version,
      revision: d.revision, policy_epoch: d.policy_epoch, root_prefix: d.root_prefix,
      progress: { job_id: status.job_id, expected_generation: status.generation } } };
  }
  private async snapshot(view: ResolvedView): Promise<unknown> {
    const initial = this.host.transactionSync(() => ({ ready: this.readyInTx(view), paths: this.pathsInTx(view) }));
    let readme: { path: string; content: string } | null = null;
    let readme_error: string | null = null;
    const path = ['readme.md', 'readme.markdown', 'readme.txt', 'readme'].map(wanted =>
      initial.paths.find(file => file.descriptor.kind === 1 && asciiLower(file.path) === wanted)?.path).find(path => path !== undefined);
    if (path !== undefined) {
      try { readme = await this.file(view, path); }
      catch (error) {
        // Only preview-specific failures are presentation data. Authorization,
        // epoch, catalog, missing payload and storage failures remain failures.
        if (!(error instanceof PreviewFailure)) throw error;
        readme_error = previewMessage(error);
      }
    }
    this.host.transactionSync(() => {
      const ready = this.readyInTx(view);
      if (ready.catalog_id !== initial.ready.catalog_id || !sameId(ready.binding.projected_commit, initial.ready.binding.projected_commit)
          || !sameId(ready.binding.projected_tree, initial.ready.binding.projected_tree)) fail(409, 'aborted');
    });
    const d = checkedDescription(view, this.host.accountId);
    // Exact existing NativeBroker snapshot shape. No progress/job/plan/private
    // origins or canonical path list survives readiness into this response.
    return { account_id: d.account_id, view: d.view, view_version: d.view_version, revision: d.revision,
      policy_epoch: d.policy_epoch, root_prefix: d.root_prefix,
      projected_commit: initial.ready.binding.projected_commit, projected_tree: initial.ready.binding.projected_tree,
      paths: initial.paths.map(file => file.path), readme, readme_error, complete: true };
  }
  private async resolve(call: ForgeCall): Promise<{ state: 'pending' | 'complete'; result: unknown }> {
    const { view, progress } = this.host.transactionSync(() => this.host.views.resolveCallInTx(call));
    let status: ProjectionStatus;
    if (progress === null) {
      // Starting a new/default selection creates or finds its exact durable job;
      // it never performs an unnamed advance or implicitly restarts a failed job.
      status = this.status(view, this.host.projections.start(view));
    } else {
      status = this.status(view, this.host.projections.status(view, progress.job_id));
      if (status.state === 'pending' && status.generation === progress.expected_generation) {
        this.host.transactionSync(() => this.checkInTx(view));
        try { status = this.status(view, await this.host.projections.advance(view, progress)); }
        catch (error) { this.host.transactionSync(() => this.checkInTx(view)); throw error; }
      }
      // Lost ACK: a stale supplied generation returns current pending progress
      // without advancing again. Ready status can always return its snapshot.
    }
    if (status.state === 'pending') return this.pending(view, status);
    return { state: 'complete', result: await this.snapshot(view) };
  }
  private async object(view: ResolvedView, command: Extract<ReadCommand, { operation: 9 }>): Promise<unknown> {
    const key = { kind: command.kind, id: hex(command.id) };
    const expected = this.host.transactionSync(() => {
      this.readyInTx(view);
      return this.host.projections.memberInTx(view, key).descriptor;
    });
    if (!Number.isSafeInteger(expected.size_bytes) || expected.size_bytes < 0) fail(503, 'unavailable');
    if (command.offset > expected.size_bytes) fail(400, 'invalid_argument');
    const length = Math.min(command.max_bytes, expected.size_bytes - command.offset);
    const recheck = () => {
      this.readyInTx(view);
      if (!same(expected, this.host.projections.memberInTx(view, key).descriptor)) fail(409, 'aborted');
    };
    let bytes: Uint8Array = new Uint8Array();
    if (length !== 0) {
      try { bytes = await this.host.readRange(expected, command.offset, length); }
      catch (error) { this.host.transactionSync(recheck); throw error; }
    }
    this.host.transactionSync(recheck);
    if (bytes.byteLength !== length) fail(503, 'unavailable');
    const d = checkedDescription(view, this.host.accountId);
    return { kind: command.kind, id: command.id, data: Array.from(bytes), revision: d.revision,
      view_version: d.view_version, policy_epoch: d.policy_epoch, size_bytes: expected.size_bytes,
      format_version: expected.format_version, offset: command.offset, eof: command.offset + length === expected.size_bytes };
  }
  async handle(call: ForgeCall): Promise<{ state: 'pending' | 'complete'; result: unknown }> {
    if (call.account_id !== this.host.accountId) fail(403, 'permission_denied');
    if (call.operation === 8) fail(501, 'unimplemented');
    if (call.operation === 6) return this.resolve(call);
    if (call.operation !== 7 && call.operation !== 9) fail(501, 'unimplemented');
    let command: ReadCommand;
    try { command = normalizeForgeReadCommand({ account_id: call.account_id, operation: call.operation, view: call.view, payload_json: call.payload_json }); }
    catch (error) {
      if (error instanceof Error && error.message.startsWith('read limit:')) fail(429, 'resource_exhausted');
      fail(400, 'invalid_argument');
    }
    if (command.operation !== call.operation) fail(503, 'unavailable');
    const view = this.host.transactionSync(() => this.host.views.resolveInTx(call.view, command.version ?? undefined));
    if (command.operation === 9) {
      if (!Number.isSafeInteger(command.offset) || command.offset < 0 || !Number.isSafeInteger(command.max_bytes)
          || command.max_bytes < 0 || command.max_bytes > MAX_CHUNK) fail(400, 'invalid_argument');
      return { state: 'complete', result: await this.object(view, command) };
    }
    try {
      const file = await this.file(view, command.path);
      this.host.transactionSync(() => this.checkInTx(view));
      return { state: 'complete', result: { ...checkedDescription(view, this.host.accountId), ...file } };
    } catch (error) {
      if (error instanceof PreviewFailure) fail(error.reason === 'size' ? 429 : 400, error.reason === 'size' ? 'resource_exhausted' : 'invalid_argument');
      throw error;
    }
  }
}
