import coreModule from './core.wasm';
import type { ViewCore } from './forge_views';

export interface ObjectKey { kind: number; id: string }
export interface Descriptor extends ObjectKey { size_bytes: number; format_version: number }
export interface Inspection extends Descriptor { references: ObjectKey[] }
interface CoreExports {
  memory: WebAssembly.Memory;
  tt_alloc(length: number): number;
  tt_dealloc(pointer: number, length: number): void;
  tt_inspect(kind: number, pointer: number, length: number): number;
  tt_validate_closure(pointer: number, length: number): number;
  tt_forge_validate_view(pointer: number, length: number): number;
  tt_forge_parse_call(pointer: number, length: number): number;
  tt_forge_account_command(pointer: number, length: number): number;
  tt_forge_build_candidate(pointer: number, length: number): number;
  tt_forge_build_projection(pointer: number, length: number): number;
  tt_closure_step(pointer: number, length: number): number;
  tt_forge_view_command(pointer: number, length: number): number;
  tt_forge_view_policy(pointer: number, length: number): number;
  tt_forge_view_paths(pointer: number, length: number): number;
  tt_forge_advance_view(pointer: number, length: number): number;
  tt_forge_search_file(pointer: number, length: number): number;
  tt_forge_search_command(pointer: number, length: number): number;
  tt_forge_read_command(pointer: number, length: number): number;
  tt_forge_import_command(pointer: number, length: number): number;
  tt_forge_upload_command(pointer: number, length: number): number;
  tt_forge_upload_scope_page(pointer: number, length: number): number;
  tt_forge_review_command(pointer: number, length: number): number;
  tt_forge_review_identity(pointer: number, length: number): number;
  tt_forge_review_format(pointer: number, length: number): number;
  tt_forge_review_admission(pointer: number, length: number): number;
  tt_output_ptr(): number;
  tt_output_len(): number;
}
const core = new WebAssembly.Instance(coreModule, {}).exports as unknown as CoreExports;
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });

function invoke(bytes: Uint8Array, command: (pointer: number) => number): unknown {
  if (bytes.byteLength > 8 * 1024 * 1024) throw new Error('native validation input limit');
  const pointer = core.tt_alloc(bytes.byteLength);
  if (!pointer && bytes.byteLength !== 0) throw new Error('native allocation failed');
  try {
    new Uint8Array(core.memory.buffer, pointer, bytes.byteLength).set(bytes);
    const ok = command(pointer);
    const length = core.tt_output_len();
    if (length > 4 * 1024 * 1024) throw new Error('native validation output limit');
    const result = JSON.parse(decoder.decode(new Uint8Array(core.memory.buffer, core.tt_output_ptr(), length)));
    if (ok !== 1) throw new Error(typeof result.error === 'string' ? result.error : 'native validation failed');
    return result;
  } finally { core.tt_dealloc(pointer, bytes.byteLength); }
}

export function inspect(kind: number, bytes: Uint8Array): Inspection {
  return invoke(bytes, pointer => core.tt_inspect(kind, pointer, bytes.byteLength)) as Inspection;
}
export function validateClosure(root: ObjectKey, objects: (Descriptor & { bytes?: number[] })[]): void {
  const bytes = encoder.encode(JSON.stringify({ root, objects }));
  const result = invoke(bytes, pointer => core.tt_validate_closure(pointer, bytes.byteLength)) as { valid?: boolean };
  if (result.valid !== true) throw new Error('native closure validation failed');
}

export interface ForgeCall {
  version: 1;
  operation: number;
  account_id: string;
  view: string;
  request_id: string;
  payload_json: string;
}

/** Strict Rust framing parser, not a trusted actor or current-policy decision. */
export function parseForgeCall(bytes: Uint8Array): ForgeCall {
  return invoke(bytes, pointer => core.tt_forge_parse_call(pointer, bytes.byteLength)) as ForgeCall;
}

/** Canonical Rust view validation and identity; callers still authorize policy. */
export function validateForgeView(account_id: string, spec: unknown): { view_version: string; spec: unknown } {
  const bytes = encoder.encode(JSON.stringify({ account_id, spec }));
  return invoke(bytes, pointer => core.tt_forge_validate_view(pointer, bytes.byteLength)) as { view_version: string; spec: unknown };
}

/** Strict account command normalization; the DO still authenticates and owns policy. */
export function normalizeForgeAccountCommand(input: Pick<ForgeCall, 'account_id' | 'operation' | 'view' | 'payload_json'>): import('./forge_accounts').AccountCommand {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_account_command(pointer, bytes.byteLength)) as import('./forge_accounts').AccountCommand;
}

/** Intent normalization only. Owner, one-time seal, storage availability and
 * canonical publication remain the embedding's current SQL authority. */
export function normalizeBootstrapImport(input: Pick<ForgeCall, 'account_id' | 'operation' | 'view' | 'request_id' | 'payload_json'>): import('./bootstrap_import').ImportCommand {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_import_command(pointer, bytes.byteLength)) as import('./bootstrap_import').ImportCommand;
}

/** Public selectors plus the host's actual authenticated principal. This
 * internal adapter must never receive principal/role fields from public JSON. */
export function normalizeForgeUploadCommand(input: Pick<ForgeCall, 'account_id' | 'operation' | 'view' | 'request_id' | 'payload_json'> & { principal: string }): import('./forge_uploads').UploadCommand {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_upload_command(pointer, bytes.byteLength)) as import('./forge_uploads').UploadCommand;
}

/** Canonical scope requirements only. Current policy and exact byte-ownership
 * witnesses are resolved by the authoritative upload host, not this adapter. */
export function uploadScopePage(input: import('./forge_uploads').UploadScopeInput): import('./forge_uploads').UploadScopePage {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_upload_scope_page(pointer, bytes.byteLength)) as import('./forge_uploads').UploadScopePage;
}

/** Strict shared public intent only. Authenticated grants, selected profiles,
 * source ownership and candidate authority are supplied separately by SQL. */
export function normalizeForgeReviewCommand(input: Pick<ForgeCall, 'account_id' | 'operation' | 'view' | 'request_id' | 'payload_json'>): import('./forge_reviews').ReviewCommand {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_review_command(pointer, bytes.byteLength)) as import('./forge_reviews').ReviewCommand;
}
/** Internal exact native identity algorithms. An output digest is not proof of
 * source possession, policy, a successful check or permission to publish. */
export function deriveForgeReviewIdentity(input: unknown): Record<string, string> {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_review_identity(pointer, bytes.byteLength)) as Record<string, string>;
}
export function formatForgeReview(input: unknown): { state: string; value: Record<string, unknown> } {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_review_format(pointer, bytes.byteLength)) as { state: string; value: Record<string, unknown> };
}
/** Complete bounded delta facts or an explicit continuation/limit result. SQL
 * must satisfy every current authority requirement before admitting a patchset. */
export function reviewAdmission(input: unknown): unknown {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_review_admission(pointer, bytes.byteLength));
}

/** Internal server-selected context only; never forward a product payload here. */
export function buildForgeCandidate(input: unknown): unknown {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_build_candidate(pointer, bytes.byteLength));
}

/** Internal authorized selection only; output includes host-private copy origins.
 * Construction is not proof of ACL, object availability or publication. */
export function buildForgeProjection(input: unknown): unknown {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_build_projection(pointer, bytes.byteLength));
}

/** Internal native graph/path facts. SQL owns monotonic frontier coverage and
 * fresh FILE verification; a page is not a complete closure or read grant. */
export function closureStep(input: unknown): unknown {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_closure_step(pointer, bytes.byteLength));
}

/** Internal per-FILE search only. The host owns path authorization, current
 * policy, cross-file budgets, opaque cursors and unsupported-file accounting. */
export function searchForgeFile(input: unknown): unknown {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_search_file(pointer, bytes.byteLength));
}

export interface ForgeSearchCommand {
  operation: 8; version: number[] | null; query: string; regex: boolean;
  limit: number; cursor: string | null; fingerprint: string;
}
/** Shared selector normalization and real native pattern preflight, not a
 * persisted compiler capability or current-policy/object read grant. */
export function normalizeForgeSearchCommand(input: Pick<ForgeCall, 'account_id' | 'operation' | 'view' | 'payload_json'>): ForgeSearchCommand {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_search_command(pointer, bytes.byteLength)) as ForgeSearchCommand;
}

/** Strict public read selectors only; SQL-resolved policy/catalog owns access. */
export function normalizeForgeReadCommand(input: Pick<ForgeCall, 'account_id' | 'operation' | 'view' | 'payload_json'>): import('./forge_reads').ReadCommand {
  const bytes = encoder.encode(JSON.stringify(input));
  return invoke(bytes, pointer => core.tt_forge_read_command(pointer, bytes.byteLength)) as import('./forge_reads').ReadCommand;
}

/** Pure canonical policy helpers. Only SQL-derived context/grants may reach the
 * policy/path/advance methods; normalized public selectors are not authority. */
export const forgeViewCore: ViewCore = {
  normalize(input) {
    const bytes = encoder.encode(JSON.stringify(input));
    return invoke(bytes, pointer => core.tt_forge_view_command(pointer, bytes.byteLength)) as ReturnType<ViewCore['normalize']>;
  },
  resolvePolicy(input) {
    const bytes = encoder.encode(JSON.stringify(input));
    return invoke(bytes, pointer => core.tt_forge_view_policy(pointer, bytes.byteLength)) as ReturnType<ViewCore['resolvePolicy']>;
  },
  pathsAllowed(input) {
    const bytes = encoder.encode(JSON.stringify(input));
    return invoke(bytes, pointer => core.tt_forge_view_paths(pointer, bytes.byteLength)) as ReturnType<ViewCore['pathsAllowed']>;
  },
  advance(input) {
    const bytes = encoder.encode(JSON.stringify(input));
    return invoke(bytes, pointer => core.tt_forge_advance_view(pointer, bytes.byteLength)) as ReturnType<ViewCore['advance']>;
  },
};
