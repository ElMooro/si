/**
 * justhodl-yahoo-proxy v1.0.1
 *
 * Proxies requests to Yahoo Finance through Cloudflare's anycast network.
 * Solves the AWS Lambda → Yahoo 429 problem (Yahoo aggressively rate-limits
 * cloud-provider IPs).
 *
 * Usage from Lambda:
 *   GET https://justhodl-yahoo-proxy.raafouis.workers.dev/options/SPY
 *   GET https://justhodl-yahoo-proxy.raafouis.workers.dev/options/SPY?date=1715798400
 *   GET https://justhodl-yahoo-proxy.raafouis.workers.dev/quote/SPY
 *
 * Returns the raw Yahoo JSON response unchanged.
 *
 * Security:
 *   - Restricted to specific Yahoo endpoints (no open proxy)
 *   - Auth via x-justhodl-token header (matches existing AI proxy pattern)
 *   - Token stored as encrypted Worker secret (set by deploy workflow)
 */

const ALLOWED_PATHS = {
  // Options chain endpoints
  options: 'https://query2.finance.yahoo.com/v7/finance/options/',
  // Quote endpoints (basic)
  quote: 'https://query2.finance.yahoo.com/v7/finance/quote',
  // Chart/historical data
  chart: 'https://query2.finance.yahoo.com/v8/finance/chart/',
  // Search endpoint
  search: 'https://query2.finance.yahoo.com/v1/finance/search',
};

const ALLOWED_ORIGINS = [
  'https://justhodl.ai',
  'https://www.justhodl.ai',
];

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: corsHeaders(request),
      });
    }

    // Health check
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response(JSON.stringify({
        ok: true,
        proxy: 'justhodl-yahoo-proxy',
        endpoints: Object.keys(ALLOWED_PATHS),
        deployed_at: 'see CF dashboard',
      }), {
        headers: { 'content-type': 'application/json', ...corsHeaders(request) },
      });
    }

    // Auth check (from Lambda — server-side calls include the token).
    // Pages aren't expected to call this directly.
    const authHeader = request.headers.get('x-justhodl-token');
    const expectedToken = env.PROXY_TOKEN;
    if (expectedToken && authHeader !== expectedToken) {
      return new Response(JSON.stringify({ error: 'unauthorized' }), {
        status: 401,
        headers: { 'content-type': 'application/json', ...corsHeaders(request) },
      });
    }

    // Parse path: /options/SPY → endpoint=options, symbol=SPY
    const parts = url.pathname.split('/').filter(Boolean);
    if (parts.length < 2) {
      return errResponse(400, 'usage: /{endpoint}/{symbol}', request);
    }
    const [endpoint, ...rest] = parts;
    const symbol = rest.join('/');

    if (!ALLOWED_PATHS[endpoint]) {
      return errResponse(400, `unknown endpoint: ${endpoint}. Allowed: ${Object.keys(ALLOWED_PATHS).join(', ')}`, request);
    }

    // Build target URL
    let targetUrl;
    if (endpoint === 'quote') {
      // /quote?symbols=SPY,QQQ
      const symbols = url.searchParams.get('symbols') || symbol;
      targetUrl = `${ALLOWED_PATHS.quote}?symbols=${encodeURIComponent(symbols)}`;
    } else if (endpoint === 'search') {
      const q = url.searchParams.get('q') || symbol;
      targetUrl = `${ALLOWED_PATHS.search}?q=${encodeURIComponent(q)}`;
    } else {
      // /options/SPY or /chart/SPY
      targetUrl = ALLOWED_PATHS[endpoint] + encodeURIComponent(symbol);
      // Forward query string
      const params = [];
      for (const [k, v] of url.searchParams) {
        params.push(`${encodeURIComponent(k)}=${encodeURIComponent(v)}`);
      }
      if (params.length) targetUrl += '?' + params.join('&');
    }

    // Forward the request with browser-like headers
    try {
      const yfRequest = new Request(targetUrl, {
        method: 'GET',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json,text/javascript,*/*;q=0.01',
          'Accept-Language': 'en-US,en;q=0.9',
          'Cache-Control': 'no-cache',
        },
        cf: {
          // Cache 60s at the edge (options data updates slowly)
          cacheTtl: 60,
          cacheEverything: true,
        },
      });
      const response = await fetch(yfRequest);
      const body = await response.text();
      return new Response(body, {
        status: response.status,
        headers: {
          'content-type': response.headers.get('content-type') || 'application/json',
          'x-proxy-status': String(response.status),
          'x-proxy-source': 'yahoo-finance',
          ...corsHeaders(request),
        },
      });
    } catch (e) {
      return errResponse(502, 'upstream fetch failed: ' + (e.message || 'unknown'), request);
    }
  },
};

function corsHeaders(request) {
  const origin = request.headers.get('origin');
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : '*';
  return {
    'access-control-allow-origin': allowed,
    'access-control-allow-methods': 'GET, OPTIONS',
    'access-control-allow-headers': 'content-type, x-justhodl-token',
    'access-control-max-age': '86400',
  };
}

function errResponse(status, message, request) {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: { 'content-type': 'application/json', ...corsHeaders(request) },
  });
}
