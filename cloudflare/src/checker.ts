/** Dedicated trusted executor ingress. This identity never becomes a Forge
 * member, raw repository principal, reviewer or canonical publisher. */
import { parseCheckerCall } from './core';
import type { CheckerCall, CheckProfile } from './forge_checks';
import { body, fail, Failure, hash, json, record } from './protocol';

export const CHECKER_ROUTE = '/api/v1/checker/call';
export interface CheckerEnvironment { FORGE_BACKEND?: string; CHECKERS_JSON?: string }
export interface CheckerAuth { principal: string; accounts: string[]; profiles: CheckProfile[] }
interface ConfiguredChecker extends CheckerAuth { token_sha256: string }
const encoder = new TextEncoder();
function configured(secret: string | undefined): ConfiguredChecker[] {
  if (!secret || encoder.encode(secret).byteLength > 128 * 1024) fail(503, 'unavailable');
  let parsed: unknown; try { parsed = JSON.parse(secret); } catch { fail(503, 'unavailable'); }
  if (!Array.isArray(parsed) || parsed.length === 0 || parsed.length > 32) fail(503, 'unavailable');
  const principals = new Set<string>(), tokens = new Set<string>(); const result: ConfiguredChecker[] = [];
  try {
    for (const raw of parsed) {
      const value = record(raw, ['principal', 'token_sha256', 'accounts', 'profiles']);
      if (typeof value.principal !== 'string' || !/^[A-Za-z0-9_.-]{1,64}$/.test(value.principal)
          || typeof value.token_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(value.token_sha256)
          || principals.has(value.principal) || tokens.has(value.token_sha256)
          || !Array.isArray(value.accounts) || !value.accounts.length || value.accounts.length > 64
          || !value.accounts.every(account => typeof account === 'string' && /^[a-z0-9-]{1,64}$/.test(account))
          || new Set(value.accounts).size !== value.accounts.length
          || !Array.isArray(value.profiles) || !value.profiles.length || value.profiles.length > 16) fail(503, 'unavailable');
      const seen = new Set<string>(); const profiles: CheckProfile[] = [];
      for (const rawProfile of value.profiles) {
        const profile = record(rawProfile, ['name', 'version']);
        if (typeof profile.name !== 'string' || !/^[A-Za-z0-9_.-]{1,128}$/.test(profile.name)
            || typeof profile.version !== 'string' || !/^[0-9a-f]{64}$/.test(profile.version)
            || seen.has(`${profile.name}/${profile.version}`)) fail(503, 'unavailable');
        seen.add(`${profile.name}/${profile.version}`); profiles.push({ name: profile.name, version: profile.version });
      }
      principals.add(value.principal); tokens.add(value.token_sha256);
      result.push({ principal: value.principal, token_sha256: value.token_sha256, accounts: value.accounts as string[], profiles });
    }
  } catch { fail(503, 'unavailable'); }
  return result;
}
export async function admitChecker(request: Request, env: CheckerEnvironment): Promise<{ auth: CheckerAuth; call: CheckerCall; bytes: Uint8Array }> {
  if (env.FORGE_BACKEND !== 'enabled') fail(503, 'unavailable');
  const url = new URL(request.url);
  if (url.pathname !== CHECKER_ROUTE || url.search || url.hash || request.method !== 'POST'
      || request.headers.get('content-type')?.split(';')[0].trim().toLowerCase() !== 'application/json') fail(400, 'invalid_argument');
  const authorization = request.headers.get('authorization');
  if (!authorization?.startsWith('Bearer ') || authorization.length > 4120) fail(401, 'unauthenticated');
  const supplied = await hash(authorization.slice(7)); let auth: CheckerAuth | undefined;
  for (const checker of configured(env.CHECKERS_JSON)) {
    let difference = 0; for (let i = 0; i < supplied.length; i++) difference |= supplied.charCodeAt(i) ^ checker.token_sha256.charCodeAt(i);
    if (difference === 0) auth = { principal: checker.principal, accounts: [...checker.accounts], profiles: checker.profiles.map(profile => ({ ...profile })) };
  }
  if (!auth) fail(401, 'unauthenticated');
  const bytes = await body(request, 64 * 1024);
  let call: CheckerCall; try { call = parseCheckerCall(bytes) as CheckerCall; } catch { fail(400, 'invalid_argument'); }
  if (!auth.accounts.includes(call.account_id)) fail(403, 'permission_denied');
  return { auth, call, bytes };
}
/** Re-read the current configured grant in the DO's authoritative transaction;
 * a request-scoped auth object alone is not a continuing profile permission. */
export function requireCheckerProfile(secret: string | undefined, auth: Readonly<CheckerAuth>, account: string, profile: CheckProfile): void {
  const current = configured(secret).find(checker => checker.principal === auth.principal);
  if (!current || !current.accounts.includes(account)
      || !current.profiles.some(p => p.name === profile.name && p.version === profile.version)) fail(403, 'permission_denied');
}
export function checkerReply(call: CheckerCall, result: unknown): Response {
  const bytes = JSON.stringify({ schema: 1, account_id: call.account_id, request_id: call.request_id, result });
  if (encoder.encode(bytes).byteLength > 1024 * 1024) fail(429, 'resource_exhausted');
  return new Response(bytes, { headers: { 'content-type': 'application/json', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' } });
}
const codes: Record<number, string[]> = {
  400: ['invalid_argument', 'failed_precondition', 'out_of_range'], 401: ['unauthenticated'], 403: ['permission_denied'],
  404: ['not_found'], 409: ['aborted', 'already_exists'], 429: ['resource_exhausted'],
  500: ['internal', 'unknown', 'data_loss'], 501: ['unimplemented'], 503: ['unavailable'], 504: ['deadline_exceeded'],
};
export async function protectChecker(action: () => Promise<Response>): Promise<Response> {
  try { return await action(); }
  catch (error) {
    if (!(error instanceof Failure)) return json({ schema: 1, error: { code: 'unavailable' } }, 503);
    let status = error.status; if (status === 408) status = 504; if (status === 413) status = 429; if (status === 415) status = 400;
    if (!codes[status]) status = 500;
    const code = codes[status].includes(error.code) ? error.code : codes[status][0];
    return json({ schema: 1, error: { code } }, status);
  }
}
