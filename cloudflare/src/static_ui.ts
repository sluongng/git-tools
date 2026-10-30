/** Public presentation assets only. No repository authority, Linux fallback,
 * credential forwarding or SPA/API fallback belongs in this route layer. */
const CSP = "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; connect-src 'self'; img-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
const MIME: Record<string, string> = {
  '/ui/index.html': 'text/html; charset=utf-8',
  '/ui/app.js': 'text/javascript; charset=utf-8',
  '/ui/style.css': 'text/css; charset=utf-8',
  '/ui/tinytree_ui.wasm': 'application/wasm',
};
const HTML = new Set(['/', '/ui', '/ui/', '/ui/index.html']);
const SELECTORS = ['account', 'view', 'version'];
function headers(contentType?: string): Headers {
  const result = new Headers({ 'cache-control': 'no-store', 'x-content-type-options': 'nosniff',
    'referrer-policy': 'no-referrer', 'content-security-policy': CSP });
  if (contentType) result.set('content-type', contentType); return result;
}
function error(request: Request, status: number, code: string, allow = false): Response {
  const values = headers('text/plain; charset=utf-8'); if (allow) values.set('allow', 'GET, HEAD');
  return new Response(request.method === 'HEAD' ? null : code, { status, headers: values });
}
/** Undefined means this is not a UI route; callers retain independent API/raw
 * routing. Known missing assets and unknown /ui/ paths never become index200. */
export async function staticUi(request: Request, assets?: Fetcher): Promise<Response | undefined> {
  const url = new URL(request.url); const html = HTML.has(url.pathname);
  const assetPath = html ? '/ui/index.html' : url.pathname;
  if (!html && !Object.hasOwn(MIME, assetPath)) {
    if (url.pathname.startsWith('/ui/')) return error(request, 404, 'ui_route_not_found');
    return undefined;
  }
  if (request.method !== 'GET' && request.method !== 'HEAD') return error(request, 405, 'ui_method_not_allowed', true);
  if (!assets) return error(request, 503, 'ui_assets_unavailable');
  if (html) {
    const path = url.pathname === '/' || url.pathname === '/ui' ? '/ui/' : url.pathname;
    const keys = [...url.searchParams.keys()];
    const needsRedirect = path !== url.pathname || url.searchParams.getAll('mode').length !== 1 || url.searchParams.get('mode') !== 'forge'
      || keys.some(key => key !== 'mode' && !SELECTORS.includes(key))
      || SELECTORS.some(key => url.searchParams.getAll(key).length > 1) || url.hash !== '';
    if (needsRedirect) {
      // Preserve only public selection fields. Never reflect token/auth/config
      // parameters or incoming credentials into a redirect URL.
      const target = new URL(path, url.origin);
      for (const key of SELECTORS) { const value = url.searchParams.get(key); if (value !== null) target.searchParams.set(key, value); }
      target.searchParams.set('mode', 'forge');
      const values = headers(); values.set('location', target.pathname + target.search);
      return new Response(null, { status: 307, headers: values });
    }
  }
  // A fresh request strips ALL caller headers, cookies, query and fragment.
  // /ui/ is mapped internally, not redirected to an asset-service/API endpoint.
  const assetUrl = new URL(assetPath, url.origin);
  let response: Response;
  try { response = await assets.fetch(new Request(assetUrl.toString(), { method: request.method, headers: new Headers(), redirect: 'manual' })); }
  catch { return error(request, 503, 'ui_assets_unavailable'); }
  if (response.status !== 200) {
    // Asset-service HTML redirects/fallback errors are never forwarded as routes.
    await response.body?.cancel().catch(() => undefined);
    return error(request, response.status === 404 ? 404 : 503, response.status === 404 ? 'ui_asset_not_found' : 'ui_assets_unavailable');
  }
  const values = headers(MIME[assetPath]);
  // Only representation headers survive. No Set-Cookie, Location, authentication
  // challenge, CDN cache policy or provider-private headers are forwarded.
  for (const name of ['content-length', 'content-encoding']) {
    const value = response.headers.get(name); if (value !== null) values.set(name, value);
  }
  if (request.method === 'HEAD') {
    await response.body?.cancel().catch(() => undefined);
    return new Response(null, { status: 200, headers: values });
  }
  return new Response(response.body, { status: 200, headers: values });
}
