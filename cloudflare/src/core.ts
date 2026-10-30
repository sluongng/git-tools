import coreModule from './core.wasm';

export interface ObjectKey { kind: number; id: string }
export interface Descriptor extends ObjectKey { size_bytes: number; format_version: number }
export interface Inspection extends Descriptor { references: ObjectKey[] }
interface CoreExports {
  memory: WebAssembly.Memory;
  tt_alloc(length: number): number;
  tt_dealloc(pointer: number, length: number): void;
  tt_inspect(kind: number, pointer: number, length: number): number;
  tt_validate_closure(pointer: number, length: number): number;
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
