/**
 * justhodl-ai-proxy
 *
 * Cloudflare Worker that proxies browser requests to AWS Lambdas:
 *   POST /            → justhodl-ai-chat   (with auth token)
 *   POST /chat        → justhodl-ai-chat   (alias)
 *   GET  /research?…  → justhodl-stock-ai-research  (no auth)
 *   POST /investor    → justhodl-investor-agents    (no auth, body {ticker})
 *   GET  /agent/<n>   → AGENT_LAMBDAS[n]            (no auth, generic data agents)
 *
 * Why a Worker instead of direct Lambda URLs:
 *   1. Lambda URLs (*.lambda-url.us-east-1.on.aws) are blocked by some
 *      adblock lists & ISP filters. api.justhodl.ai is on the user's
 *      own domain so it can't be blocked that way.
 *   2. ai-chat needs an auth token kept off the browser.
 *   3. Stable origin for CORS, easier to extend later.
 *
 * Security layers:
 *   1. Origin allowlist (only justhodl.ai / www.justhodl.ai)
 *   2. Method allowlist per path
 *   3. Body size cap on POSTs
 *   4. AGENT_LAMBDAS whitelist — only listed agents are reachable
 *   5. Upstream auth where required (ai-chat token attached here)
 *
 * CORS preflight (OPTIONS) handled at the Worker; never reaches Lambda.
 */

const LAMBDA_AI_CHAT     = 'https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/';
const LAMBDA_AI_RESEARCH = 'https://obcsgkzlvicwc6htdmj5wg6yae0tfmya.lambda-url.us-east-1.on.aws/';
const LAMBDA_INVESTOR_AGENTS = 'https://7qufoauxzhqwnrsmdjjwt46wy40zzdyp.lambda-url.us-east-1.on.aws/';

// Whitelist of generic data agents reachable via /agent/<key>
// All return JSON via Function URL with permissive CORS.
const AGENT_LAMBDAS = {
  'volatility':      'https://w4bvakowhpbkvy3mqkzp66xfaa0kpfqi.lambda-url.us-east-1.on.aws/',
  'dollar':          'https://us3uynmi23u676v3szd27ldwuq0jeqee.lambda-url.us-east-1.on.aws/',
  'bonds':           'https://s57bexwijusq7jukyxishguffe0nukpw.lambda-url.us-east-1.on.aws/',
  'bea':             'https://hnqqkbf7y6avoda5v4rexwk3440ibbru.lambda-url.us-east-1.on.aws/',
  'manufacturing':   'https://atjkdhikbinborcc2pujbs3jxm0iugqe.lambda-url.us-east-1.on.aws/',
  'banking':         'https://iru4ado3aki625pnswcpycrniq0ctjba.lambda-url.us-east-1.on.aws/',
  'trends':          'https://ohmu2l54tpbgm6jk7e5s6q3jpe0cqvij.lambda-url.us-east-1.on.aws/',
  'sentiment':       'https://rtfrjcj43osvg4u4vza5bhf4pm0udfmo.lambda-url.us-east-1.on.aws/',
  'secretary':       'https://nqzbtg3pnxhcyj4rdlmft2vu4y0xxfvk.lambda-url.us-east-1.on.aws/',
  'macro-brief':     'https://qhmg6ybf5qhkviw2c6tlizar4e0xhbrc.lambda-url.us-east-1.on.aws/',
};

const ALLOWED_ORIGINS = new Set([
  'https://justhodl.ai',
  'https://www.justhodl.ai',
]);

const MAX_BODY_BYTES = 32 * 1024; // 32 KB per request

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.has(origin) ? origin : 'https://justhodl.ai';
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '300',
    'Vary': 'Origin',
  };
}

function json(status, data, origin) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...corsHeaders(origin),
      'Content-Type': 'application/json; charset=utf-8',
    },
  });
}

async function handleAiChat(request, env, origin) {
  if (request.method !== 'POST') {
    return json(405, { error: 'Method not allowed (use POST)' }, origin);
  }
  if (!env.AI_CHAT_TOKEN) {
    return json(500, { error: 'Worker misconfigured (missing AI_CHAT_TOKEN)' }, origin);
  }
  const cl = parseInt(request.headers.get('Content-Length') || '0', 10);
  if (cl > MAX_BODY_BYTES) {
    return json(413, { error: 'Request body too large' }, origin);
  }
  const body = await request.text();
  if (body.length > MAX_BODY_BYTES) {
    return json(413, { error: 'Request body too large' }, origin);
  }
  let upstream;
  try {
    upstream = await fetch(LAMBDA_AI_CHAT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Origin': 'https://justhodl.ai',
        'x-justhodl-token': env.AI_CHAT_TOKEN,
      },
      body,
    });
  } catch (e) {
    return json(502, { error: 'Upstream unreachable', detail: String(e) }, origin);
  }
  const text = await upstream.text();
  return new Response(text, {
    status: upstream.status,
    headers: {
      ...corsHeaders(origin),
      'Content-Type': upstream.headers.get('Content-Type') || 'application/json',
    },
  });
}

async function handleAiResearch(request, env, origin) {
  if (request.method !== 'GET') {
    return json(405, { error: 'Method not allowed (use GET)' }, origin);
  }
  // Forward query string to upstream Lambda
  const url = new URL(request.url);
  const targetUrl = LAMBDA_AI_RESEARCH + url.search;
  let upstream;
  try {
    upstream = await fetch(targetUrl, {
      method: 'GET',
      headers: {
        'Origin': 'https://justhodl.ai',
      },
    });
  } catch (e) {
    return json(502, { error: 'Upstream unreachable', detail: String(e) }, origin);
  }
  const text = await upstream.text();
  return new Response(text, {
    status: upstream.status,
    headers: {
      ...corsHeaders(origin),
      'Content-Type': upstream.headers.get('Content-Type') || 'application/json',
      'Cache-Control': upstream.headers.get('Cache-Control') || 'public, max-age=3600',
    },
  });
}

async function handleInvestorAgents(request, env, origin) {
  // Lambda requires POST with {"ticker": "AAPL"} body
  if (request.method !== 'POST') {
    return json(405, { error: 'Method not allowed (use POST with {ticker})' }, origin);
  }
  const cl = parseInt(request.headers.get('Content-Length') || '0', 10);
  if (cl > MAX_BODY_BYTES) {
    return json(413, { error: 'Request body too large' }, origin);
  }
  const body = await request.text();
  if (body.length > MAX_BODY_BYTES) {
    return json(413, { error: 'Request body too large' }, origin);
  }
  let upstream;
  try {
    upstream = await fetch(LAMBDA_INVESTOR_AGENTS, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Origin': 'https://justhodl.ai',
      },
      body,
    });
  } catch (e) {
    return json(502, { error: 'Upstream unreachable', detail: String(e) }, origin);
  }
  const text = await upstream.text();
  return new Response(text, {
    status: upstream.status,
    headers: {
      ...corsHeaders(origin),
      'Content-Type': upstream.headers.get('Content-Type') || 'application/json',
    },
  });
}

async function handleAgent(request, env, origin, agentKey, subPath) {
  // Generic data-agent proxy. Looks up agentKey in AGENT_LAMBDAS,
  // forwards GET request (preserving query string + sub-path).
  if (request.method !== 'GET') {
    return json(405, { error: 'Method not allowed (use GET)' }, origin);
  }
  const upstreamBase = AGENT_LAMBDAS[agentKey];
  if (!upstreamBase) {
    return json(404, { error: 'Unknown agent', agent: agentKey, available: Object.keys(AGENT_LAMBDAS) }, origin);
  }
  const url = new URL(request.url);
  // Build upstream URL: base + subPath + query
  let targetUrl = upstreamBase;
  if (subPath) {
    targetUrl = upstreamBase.replace(/\/$/, '') + '/' + subPath.replace(/^\/+/, '');
  }
  if (url.search) {
    targetUrl += url.search;
  }
  let upstream;
  try {
    upstream = await fetch(targetUrl, {
      method: 'GET',
      headers: { 'Origin': 'https://justhodl.ai' },
    });
  } catch (e) {
    return json(502, { error: 'Upstream unreachable', detail: String(e), agent: agentKey }, origin);
  }
  const text = await upstream.text();
  return new Response(text, {
    status: upstream.status,
    headers: {
      ...corsHeaders(origin),
      'Content-Type': upstream.headers.get('Content-Type') || 'application/json',
      // 60s cache — agents update at varying cadences (5min - hourly).
      // Saves Lambda invocations + makes pages snappy on revisit.
      'Cache-Control': 'public, max-age=60',
    },
  });
}

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';

    // CORS preflight — handle before Origin check
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    // Origin check
    if (!ALLOWED_ORIGINS.has(origin)) {
      return json(403, { error: 'Forbidden' }, origin);
    }

    // /agent/<key>[/<subpath>]
    const agentMatch = path.match(/^\/agent\/([a-z0-9-]+)(\/.*)?$/);
    if (agentMatch) {
      return handleAgent(request, env, origin, agentMatch[1], agentMatch[2] || '');
    }

    // Path-based routing
    if (path === '/research') {
      return handleAiResearch(request, env, origin);
    }
    if (path === '/investor') {
      return handleInvestorAgents(request, env, origin);
    }
    if (path === '/' || path === '/chat') {
      return handleAiChat(request, env, origin);
    }

    return json(404, { error: 'Not found', path }, origin);
  },
};
