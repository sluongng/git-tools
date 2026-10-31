/** HTTP framing only. Account policy and publication authority live in the DO. */
import { parseForgeCall, type ForgeCall } from './core';
import { body, fail, Failure, json } from './protocol';

export const FORGE_ROUTE = '/api/v1/forge/call';
const MAX_REQUEST = 2 * 1024 * 1024;
const MAX_RESPONSE = 8 * 1024 * 1024;
const encoder = new TextEncoder();
const operations = new Set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 20, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35, 40, 41, 42, 43, 44]);
const mutations = new Set([1, 3, 4, 10, 11, 20, 23, 24, 30, 32, 34, 40, 41, 42, 44]);
const codes: Record<number, string[]> = {
  400: ['invalid_argument', 'failed_precondition', 'out_of_range'],
  401: ['unauthenticated'], 403: ['permission_denied'], 404: ['not_found'],
  409: ['aborted', 'already_exists'], 429: ['resource_exhausted'],
  500: ['internal', 'unknown', 'data_loss'], 501: ['unimplemented'],
  503: ['unavailable'], 504: ['deadline_exceeded'],
};

export async function readForgeCall(request: Request): Promise<{ call: ForgeCall; bytes: Uint8Array }> {
  const url = new URL(request.url);
  if (url.pathname !== FORGE_ROUTE || url.search || url.hash || request.method !== 'POST') fail(400, 'invalid_argument');
  if (request.headers.get('content-type')?.split(';')[0].trim().toLowerCase() !== 'application/json') fail(400, 'invalid_argument');
  const bytes = await body(request, MAX_REQUEST);
  let call: ForgeCall;
  try { call = parseForgeCall(bytes); } catch { fail(400, 'invalid_argument'); }
  if (!operations.has(call.operation) || (mutations.has(call.operation) && !call.request_id)) fail(400, 'invalid_argument');
  return { call, bytes };
}

export function forgeReply(call: ForgeCall, state: string, result: unknown): Response {
  const result_json = JSON.stringify(result);
  const response = JSON.stringify({ version: 1, operation: call.operation, account_id: call.account_id,
    view: call.view, request_id: call.request_id, state, result_json });
  if (encoder.encode(response).byteLength > MAX_RESPONSE) fail(429, 'resource_exhausted');
  return new Response(response, { headers: { 'content-type': 'application/json', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' } });
}

export async function protectForge(action: () => Promise<Response>): Promise<Response> {
  try { return await action(); }
  catch (error) {
    if (!(error instanceof Failure)) return json({ version: 1, error: { code: 'unavailable', message: 'forge_backend_unavailable' } }, 503);
    let status = error.status;
    if (status === 408) status = 504;
    if (status === 413) status = 429;
    if (status === 415) status = 400;
    if (!codes[status]) status = 500;
    const code = codes[status].includes(error.code) ? error.code : codes[status][0];
    // Failure codes are stable service constants, never caught child/SQL text.
    const message = /^[a-z0-9_]{1,128}$/.test(error.code) ? error.code : 'forge_operation_failed';
    return json({ version: 1, error: { code, message } }, status);
  }
}
