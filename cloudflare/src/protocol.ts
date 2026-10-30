export class Failure extends Error {
  constructor(public status: number, public code: string) { super(code); }
}
export function fail(status: number, code: string): never { throw new Failure(status, code); }
export function json(value: unknown, status = 200): Response {
  return Response.json(value, { status, headers: { 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' } });
}
export function record(value: unknown, keys: string[], required = keys): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(400, 'invalid_json_object');
  const object = value as Record<string, unknown>;
  if (Object.keys(object).some(k => !keys.includes(k)) || required.some(k => !Object.hasOwn(object, k))) fail(400, 'unknown_or_missing_fields');
  return object;
}
export function identifier(value: unknown): string {
  if (typeof value !== 'string' || !/^[a-z0-9-]{1,64}$/.test(value)) fail(400, 'invalid_account');
  return value;
}
export function requestId(value: unknown): string {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_.-]{1,64}$/.test(value)) fail(400, 'invalid_request_id');
  return value;
}
export function digest(value: unknown): string {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) fail(400, 'invalid_object_id');
  return value;
}
export function name(value: unknown): string {
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$/.test(value)
      || value.split('/').some(p => !p || p === '.' || p === '..')) fail(400, 'invalid_name');
  return value;
}
export function integer(value: unknown, maximum = Number.MAX_SAFE_INTEGER): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value > maximum) fail(400, 'invalid_integer');
  return value;
}
export async function body(request: Request, maximum: number): Promise<Uint8Array> {
  const header = request.headers.get('content-length');
  if (header !== null && (!/^\d+$/.test(header) || Number(header) > maximum)) fail(413, 'body_limit');
  const reader = request.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = []; let size = 0;
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Failure(408, 'body_deadline'));
      void reader.cancel().catch(() => undefined);
    }, 10_000);
  });
  try {
    for (;;) {
      const next = await Promise.race([reader.read(), deadline]); if (next.done) break;
      size += next.value.byteLength;
      if (size > maximum) { await reader.cancel(); fail(413, 'body_limit'); }
      chunks.push(next.value);
    }
  } finally { clearTimeout(timer!); reader.releaseLock(); }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}
export async function input(request: Request): Promise<unknown> {
  if (request.headers.get('content-type')?.split(';')[0] !== 'application/json') fail(415, 'json_required');
  try { return JSON.parse(new TextDecoder('utf-8', { fatal: true, ignoreBOM: false }).decode(await body(request, 16 * 1024))); }
  catch (error) { if (error instanceof Failure) throw error; fail(400, 'invalid_json'); }
}
export async function hash(value: string): Promise<string> {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), b => b.toString(16).padStart(2, '0')).join('');
}
export interface Auth { principal: string; operator: boolean; accounts: string[] }
export async function authenticate(request: Request, secret: string | undefined): Promise<Auth> {
  const authorization = request.headers.get('authorization') ?? '';
  if (!authorization.startsWith('Bearer ') || authorization.length > 520) fail(401, 'authentication_required');
  if (!secret || secret.length > 32 * 1024) fail(503, 'principal_configuration_invalid');
  let principals: unknown;
  try { principals = JSON.parse(secret); } catch { fail(503, 'principal_configuration_invalid'); }
  if (!Array.isArray(principals) || principals.length < 1 || principals.length > 32) fail(503, 'principal_configuration_invalid');
  const supplied = await hash(authorization.slice(7));
  let matched: Auth | undefined; const seen = new Set<string>();
  try { for (const raw of principals) {
    const p = record(raw, ['principal', 'token_sha256', 'accounts', 'operator']);
    if (typeof p.principal !== 'string' || !/^[A-Za-z0-9_.-]{1,64}$/.test(p.principal)
        || typeof p.operator !== 'boolean' || !Array.isArray(p.accounts) || p.accounts.length > 64
        || typeof p.token_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(p.token_sha256)
        || seen.has(p.token_sha256)) fail(503, 'principal_configuration_invalid');
    seen.add(p.token_sha256);
    const accounts = p.accounts.map(identifier);
    let difference = 0;
    for (let i = 0; i < supplied.length; i++) difference |= supplied.charCodeAt(i) ^ p.token_sha256.charCodeAt(i);
    if (difference === 0) matched = { principal: p.principal, operator: p.operator, accounts };
  } } catch { fail(503, 'principal_configuration_invalid'); }
  if (!matched) fail(401, 'authentication_required');
  return matched;
}
export async function protect(action: () => Promise<Response>): Promise<Response> {
  try { return await action(); }
  catch (error) {
    if (error instanceof Failure) return json({ error: error.code }, error.status);
    return json({ error: 'backend_unavailable' }, 503);
  }
}
