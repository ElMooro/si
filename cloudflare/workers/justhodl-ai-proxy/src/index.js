/**
 * justhodl-ai-proxy
 *
 * Cloudflare Worker that proxies browser requests to AWS Lambdas:
 *   POST /            → justhodl-ai-chat   (with auth token)
 *   POST /chat        → justhodl-ai-chat   (alias)
 *   GET  /research?…  → justhodl-stock-ai-research  (no auth)
 *   POST /investor    → justhodl-investor-agents    (no auth, body {ticker})
 *   POST /portfolio-admin → justhodl-portfolio-admin (manager PIN x-mgr-pass;
 *                            Worker injects the Lambda infra token)
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
 *   1. Verified bearer identity + owner/admin authorization on /ai control routes
 *   2. Atomic per-subject/per-route quotas and replay checks for AI mutations
 *   3. Origin allowlist as CORS/browser policy only (never AI authorization)
 *   4. Method allowlist and body caps
 *   5. AGENT_LAMBDAS whitelist — only listed agents are reachable
 *   6. Upstream auth where required (ai-chat token attached here)
 *
 * CORS preflight (OPTIONS) handled at the Worker; never reaches Lambda.
 */

const LAMBDA_AI_CHAT     = 'https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/';
const LAMBDA_AI_RESEARCH = 'https://obcsgkzlvicwc6htdmj5wg6yae0tfmya.lambda-url.us-east-1.on.aws/';
const LAMBDA_INVESTOR_AGENTS = 'https://7qufoauxzhqwnrsmdjjwt46wy40zzdyp.lambda-url.us-east-1.on.aws/';
const LAMBDA_PORTFOLIO_ADMIN = 'https://e726eujwijpeg2slgssddw2yee0stboa.lambda-url.us-east-1.on.aws/';

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
  'chart-data':      'https://zsgb72zf4ayw6ajw7phbyq6wzq0haobh.lambda-url.us-east-1.on.aws/',
  // ↑ chart-pro.html catalog + multi-source historical chart data
  'cftc-positioning': 'https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/',
  // ↑ positioning/index.html + cot-extremes.html — COT futures + signals + futures
};

const ALLOWED_ORIGINS = new Set([
  'https://justhodl.ai',
  'https://www.justhodl.ai',
]);

const MAX_BODY_BYTES = 32 * 1024; // 32 KB per request
const DEFAULT_AI_BODY_BYTES = 64 * 1024;
const DEFAULT_REPLAY_WINDOW_SECONDS = 5 * 60;
const MAX_QUERY_BYTES = 4096;
const DATA_PROXY = 'https://justhodl-data-proxy.raafouis.workers.dev';
const GUARD_RECORD_KEY = 'guard:v1';
const REPLAY_STORAGE_PREFIX = 'replay:v2:';
const REPLAY_EXPIRY_PREFIX = 'replay-expiry:v2:';
const REPLAY_CLEANUP_BATCH_SIZE = 64;

// Exact control-plane routes. Every route is owner/admin only and has an
// independent per-subject quota. Every POST is treated as mutating because
// even "read-like" refreshes invoke AWS APIs and can publish new snapshots.
const AI_CONTROL_ROUTES = new Map([
  ['GET /status',             { limit: 60, windowSeconds: 60 }],
  ['GET /health',             { limit: 60, windowSeconds: 60 }],
  ['GET /inventory',          { limit: 30, windowSeconds: 60 }],
  ['GET /model',              { limit: 60, windowSeconds: 60 }],
  ['GET /read',               { limit: 30, windowSeconds: 60 }],
  ['POST /inventory',         { limit: 6,  windowSeconds: 60, mutating: true }],
  ['POST /catalog',           { limit: 6,  windowSeconds: 60, mutating: true }],
  ['POST /dataset/build',     { limit: 3,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /deploy',            { limit: 3,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /embed',             { limit: 6,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /train/classifier',  { limit: 3,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /train/finetune',    { limit: 2,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /train/curve',       { limit: 2,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /train/automl',      { limit: 2,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /deploy-trained',    { limit: 3,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /infer',             { limit: 20, windowSeconds: 60, mutating: true, paid: true }],
  ['POST /endpoint/delete',   { limit: 6,  windowSeconds: 3600, mutating: true }],
  ['POST /job/stop',          { limit: 6,  windowSeconds: 3600, mutating: true }],
  ['POST /policy',            { limit: 6,  windowSeconds: 3600, mutating: true }],
  ['POST /hyperpod/create',   { limit: 1,  windowSeconds: 86400, mutating: true, paid: true }],
  ['POST /market-read',       { limit: 4,  windowSeconds: 3600, mutating: true, paid: true }],
  ['POST /governance/signals/validate',                { limit: 60, windowSeconds: 60, mutating: true }],
  ['POST /governance/signals/ingest',                  { limit: 30, windowSeconds: 60, mutating: true }],
  ['POST /governance/features/assemble',               { limit: 30, windowSeconds: 60, mutating: true }],
  ['POST /governance/outcomes/label',                  { limit: 30, windowSeconds: 60, mutating: true }],
  ['POST /governance/splits/walk-forward',             { limit: 20, windowSeconds: 60, mutating: true }],
  ['POST /governance/splits/cpcv',                     { limit: 20, windowSeconds: 60, mutating: true }],
  ['POST /governance/predictions/write',               { limit: 30, windowSeconds: 60, mutating: true }],
  ['POST /governance/predictions/grade',               { limit: 30, windowSeconds: 60, mutating: true }],
  ['POST /governance/models/package-request',          { limit: 10, windowSeconds: 60, mutating: true }],
  ['POST /governance/models/card',                     { limit: 10, windowSeconds: 60, mutating: true }],
  ['POST /governance/models/mlflow-lineage/validate',  { limit: 30, windowSeconds: 60, mutating: true }],
  ['POST /governance/deployment/readiness',            { limit: 30, windowSeconds: 60, mutating: true }],
  ['POST /governance/deployment/canary-plan',          { limit: 10, windowSeconds: 60, mutating: true }],
]);

globalThis.__jhAiJwksCache = globalThis.__jhAiJwksCache || new Map();

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.has(origin) ? origin : 'https://justhodl.ai';
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, x-mgr-pass, X-Brain-Pin, Authorization, X-JH-Service-Token',
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

function aiCorsHeaders(origin) {
  const headers = {
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Authorization, Content-Type, X-Request-Nonce, X-Request-Timestamp',
    'Access-Control-Max-Age': '300',
    'Vary': 'Origin, Authorization',
  };
  // CORS is only a browser read policy. An unlisted/missing Origin neither
  // grants nor denies access; the verified bearer identity does that.
  if (ALLOWED_ORIGINS.has(origin)) headers['Access-Control-Allow-Origin'] = origin;
  return headers;
}

function aiJson(status, data, origin, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...aiCorsHeaders(origin),
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'private, no-store',
      'X-Content-Type-Options': 'nosniff',
      ...extraHeaders,
    },
  });
}

function splitCsv(value) {
  return new Set(String(value || '').split(',').map(v => v.trim().toLowerCase()).filter(Boolean));
}

function base64UrlBytes(value) {
  if (!/^[A-Za-z0-9_-]+$/.test(value || '')) throw new Error('bad base64url');
  const padded = value.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - value.length % 4) % 4);
  const raw = atob(padded);
  return Uint8Array.from(raw, c => c.charCodeAt(0));
}

function base64UrlJson(value) {
  const text = new TextDecoder('utf-8', { fatal: true }).decode(base64UrlBytes(value));
  return JSON.parse(text);
}

function jwtAlgorithm(header) {
  if (header.alg === 'RS256') return {
    import: { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    verify: { name: 'RSASSA-PKCS1-v1_5' },
  };
  if (header.alg === 'ES256') return {
    import: { name: 'ECDSA', namedCurve: 'P-256' },
    verify: { name: 'ECDSA', hash: 'SHA-256' },
  };
  throw new Error('unsupported jwt algorithm');
}

function audienceMatches(actual, expected) {
  if (!expected) return true;
  const values = Array.isArray(actual) ? actual : [actual];
  return values.includes(expected);
}

async function loadJwks(url) {
  const now = Date.now();
  const cached = globalThis.__jhAiJwksCache.get(url);
  if (cached && cached.expiresAt > now) return cached.keys;
  let response;
  try {
    response = await fetch(url, { headers: { Accept: 'application/json' }, redirect: 'error' });
  } catch (_) {
    if (cached && cached.staleUntil > now) return cached.keys;
    throw new Error('identity backend unavailable');
  }
  if (!response.ok) {
    if (cached && cached.staleUntil > now) return cached.keys;
    throw new Error('identity backend unavailable');
  }
  const document = await response.json();
  if (!document || !Array.isArray(document.keys) || !document.keys.length) {
    throw new Error('identity backend unavailable');
  }
  const maxAgeMatch = (response.headers.get('Cache-Control') || '').match(/max-age=(\d+)/i);
  const ttl = Math.min(3600, Math.max(60, Number(maxAgeMatch && maxAgeMatch[1]) || 300));
  globalThis.__jhAiJwksCache.set(url, {
    keys: document.keys,
    expiresAt: now + ttl * 1000,
    staleUntil: now + 24 * 3600 * 1000,
  });
  return document.keys;
}

async function verifyJwt(token, env) {
  if (token.length > 8192) throw new Error('invalid bearer');
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('invalid bearer');
  const header = base64UrlJson(parts[0]);
  const claims = base64UrlJson(parts[1]);
  if (!header || typeof header.kid !== 'string' || !claims || typeof claims !== 'object') {
    throw new Error('invalid bearer');
  }
  const algorithm = jwtAlgorithm(header);
  const keys = await loadJwks(env.AUTH_JWKS_URL);
  const jwk = keys.find(k => k && k.kid === header.kid && (!k.alg || k.alg === header.alg));
  if (!jwk) throw new Error('invalid bearer');
  const key = await crypto.subtle.importKey('jwk', jwk, algorithm.import, false, ['verify']);
  const valid = await crypto.subtle.verify(
    algorithm.verify,
    key,
    base64UrlBytes(parts[2]),
    new TextEncoder().encode(parts[0] + '.' + parts[1]),
  );
  if (!valid) throw new Error('invalid bearer');

  const now = Math.floor(Date.now() / 1000);
  const leeway = 30;
  if (!Number.isFinite(claims.exp) || claims.exp < now - leeway) throw new Error('invalid bearer');
  if (Number.isFinite(claims.nbf) && claims.nbf > now + leeway) throw new Error('invalid bearer');
  if (Number.isFinite(claims.iat) && claims.iat > now + leeway) throw new Error('invalid bearer');
  if (env.AUTH_ISSUER && claims.iss !== env.AUTH_ISSUER) throw new Error('invalid bearer');
  if (!audienceMatches(claims.aud, env.AUTH_AUDIENCE)) throw new Error('invalid bearer');
  if (typeof claims.sub !== 'string' || !claims.sub || claims.sub.length > 256) throw new Error('invalid bearer');
  return claims;
}

async function verifyWithSupabase(token, env) {
  if (!env.SUPABASE_URL || !env.SUPABASE_ANON_KEY) throw new Error('identity backend unavailable');
  let response;
  try {
    response = await fetch(env.SUPABASE_URL.replace(/\/+$/, '') + '/auth/v1/user', {
      headers: { apikey: env.SUPABASE_ANON_KEY, Authorization: 'Bearer ' + token },
      redirect: 'error',
    });
  } catch (_) {
    throw new Error('identity backend unavailable');
  }
  if (response.status === 401 || response.status === 403) throw new Error('invalid bearer');
  if (!response.ok) throw new Error('identity backend unavailable');
  const user = await response.json();
  if (!user || typeof user.id !== 'string' || !user.id || user.id.length > 256) {
    throw new Error('invalid bearer');
  }
  return {
    sub: user.id,
    email: user.email,
    app_metadata: user.app_metadata,
    user_metadata: user.user_metadata,
    role: user.role,
  };
}

async function verifiedBearerIdentity(request, env) {
  const header = request.headers.get('Authorization') || '';
  const match = header.match(/^Bearer ([^\s]+)$/i);
  if (!match) return { error: 'missing' };
  const token = match[1];
  try {
    let claims;
    if (env.AUTH_JWKS_URL) {
      try {
        claims = await verifyJwt(token, env);
      } catch (error) {
        // Supabase projects may still issue legacy symmetric tokens, which
        // cannot be verified from JWKS. Its authenticated user endpoint is
        // the fail-closed verifier for that migration case.
        if (!env.SUPABASE_URL || !env.SUPABASE_ANON_KEY) throw error;
        claims = await verifyWithSupabase(token, env);
      }
    } else {
      claims = await verifyWithSupabase(token, env);
    }
    const subject = claims.sub;
    const email = String(claims.email || '').trim().toLowerCase();
    const ownerSubjects = splitCsv(env.OWNER_SUBJECTS);
    const ownerEmails = splitCsv(env.OWNER_EMAILS);
    const allowedAdminRoles = splitCsv(env.ADMIN_ROLES || 'admin');
    const metadata = claims.app_metadata && typeof claims.app_metadata === 'object' ? claims.app_metadata : {};
    const roles = []
      .concat(metadata.roles || [])
      .concat(metadata.role || [])
      .concat(claims.roles || [])
      .map(v => String(v).toLowerCase());
    let role = null;
    if (ownerSubjects.has(subject.toLowerCase()) || (email && ownerEmails.has(email))) role = 'owner';
    else if (roles.some(r => allowedAdminRoles.has(r))) role = 'admin';
    return { subject, email, role, token };
  } catch (error) {
    return { error: error && error.message === 'identity backend unavailable' ? 'unavailable' : 'invalid' };
  }
}

function aiRoute(request, path) {
  const subPath = path === '/ai' ? '/status' : path.slice(3);
  const key = request.method + ' ' + subPath;
  return { subPath, key, policy: AI_CONTROL_ROUTES.get(key) };
}

function configuredRoutePolicy(env, key, base) {
  let override;
  try {
    const all = JSON.parse(env.AI_ROUTE_QUOTAS || '{}');
    override = all && typeof all === 'object' ? all[key] : null;
  } catch (_) {
    override = null;
  }
  const limit = Number(override && override.limit);
  const windowSeconds = Number(override && (override.window_seconds || override.windowSeconds));
  return {
    ...base,
    limit: Number.isSafeInteger(limit) && limit >= 1 && limit <= 10000 ? limit : base.limit,
    windowSeconds: Number.isSafeInteger(windowSeconds) && windowSeconds >= 1 && windowSeconds <= 86400
      ? windowSeconds : base.windowSeconds,
  };
}

function bodyLimit(env) {
  const configured = Number(env.AI_MAX_BODY_BYTES);
  return Number.isSafeInteger(configured) && configured >= 1024 && configured <= 1024 * 1024
    ? configured : DEFAULT_AI_BODY_BYTES;
}

async function boundedJsonBody(request, maxBytes) {
  const declaredHeader = request.headers.get('Content-Length');
  if (declaredHeader !== null) {
    if (!/^\d+$/.test(declaredHeader)) return { error: 'invalid content length', status: 400 };
    if (Number(declaredHeader) > maxBytes) return { error: 'request body too large', status: 413 };
  }
  const contentType = (request.headers.get('Content-Type') || '').split(';', 1)[0].trim().toLowerCase();
  if (contentType !== 'application/json') return { error: 'application/json required', status: 415 };
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > maxBytes) return { error: 'request body too large', status: 413 };
  let text;
  let value;
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    value = JSON.parse(text || '{}');
  } catch (_) {
    return { error: 'invalid JSON body', status: 400 };
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return { error: 'JSON object required', status: 400 };
  }
  return { text: text || '{}', value };
}

async function sha256Hex(value) {
  const bytes = typeof value === 'string' ? new TextEncoder().encode(value) : value;
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  return Array.from(digest, b => b.toString(16).padStart(2, '0')).join('');
}

function replayHeaders(request, env) {
  const nonce = request.headers.get('X-Request-Nonce') || '';
  const timestamp = request.headers.get('X-Request-Timestamp') || '';
  if (!/^[A-Za-z0-9._~-]{16,128}$/.test(nonce)) return { error: 'valid request nonce required' };
  if (!/^\d{10,13}$/.test(timestamp)) return { error: 'valid request timestamp required' };
  let epochMs = Number(timestamp);
  if (timestamp.length <= 10) epochMs *= 1000;
  const configured = Number(env.AI_REPLAY_WINDOW_SECONDS);
  const windowSeconds = Number.isSafeInteger(configured) && configured >= 30 && configured <= 900
    ? configured : DEFAULT_REPLAY_WINDOW_SECONDS;
  if (!Number.isSafeInteger(epochMs) || Math.abs(Date.now() - epochMs) > windowSeconds * 1000) {
    return { error: 'request timestamp outside allowed window' };
  }
  return { nonce, timestamp, windowSeconds };
}

async function guardRequest(env, input) {
  const binding = env.AI_REQUEST_GUARD;
  if (!binding) throw new Error('guard unavailable');
  // Small interface used by unit tests and compatible wrapper bindings.
  if (typeof binding.check === 'function') return binding.check(input);
  if (typeof binding.idFromName !== 'function' || typeof binding.get !== 'function') {
    throw new Error('guard unavailable');
  }
  const subjectHash = await sha256Hex(input.subject);
  const stub = binding.get(binding.idFromName(subjectHash));
  const response = await stub.fetch('https://guard.internal/check', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });
  if (!response.ok) throw new Error('guard unavailable');
  return response.json();
}

function quotaHeaders(result, policy, denied = false) {
  const headers = { 'RateLimit-Limit': String(policy.limit) };
  if (Number.isFinite(result && result.remaining)) headers['RateLimit-Remaining'] = String(Math.max(0, result.remaining));
  if (Number.isFinite(result && result.retryAfter)) {
    headers['RateLimit-Reset'] = String(Math.max(1, Math.ceil(result.retryAfter)));
    if (denied) headers['Retry-After'] = headers['RateLimit-Reset'];
  }
  return headers;
}

async function handleAiControl(request, env, origin, path, search) {
  const requestId = crypto.randomUUID();
  const route = aiRoute(request, path);
  if (!route.policy) return aiJson(404, { error: 'unknown AI control route', request_id: requestId }, origin);
  if (new TextEncoder().encode(search || '').byteLength > MAX_QUERY_BYTES) {
    return aiJson(414, { error: 'query string too large', request_id: requestId }, origin);
  }

  const identity = await verifiedBearerIdentity(request, env);
  if (identity.error === 'missing' || identity.error === 'invalid') {
    return aiJson(401, { error: 'authentication required', request_id: requestId }, origin, {
      'WWW-Authenticate': 'Bearer',
    });
  }
  if (identity.error === 'unavailable') {
    return aiJson(503, { error: 'authentication service unavailable', request_id: requestId }, origin);
  }
  if (identity.role !== 'owner' && identity.role !== 'admin') {
    return aiJson(403, { error: 'owner or administrator access required', request_id: requestId }, origin);
  }

  const policy = configuredRoutePolicy(env, route.key, route.policy);
  let body;
  let replayKey;
  let replayTtlSeconds;
  if (request.method === 'POST') {
    const replay = replayHeaders(request, env);
    if (replay.error) return aiJson(400, { error: replay.error, request_id: requestId }, origin);
    body = await boundedJsonBody(request, bodyLimit(env));
    if (body.error) return aiJson(body.status, { error: body.error, request_id: requestId }, origin);
    // A nonce is single-use for the verified subject regardless of route,
    // timestamp, or body. Otherwise a replay could reuse the nonce while
    // changing an unkeyed field to evade duplicate detection.
    replayKey = await sha256Hex(identity.subject + '\n' + replay.nonce);
    replayTtlSeconds = replay.windowSeconds;
  }

  let guardResult;
  try {
    guardResult = await guardRequest(env, {
      subject: identity.subject,
      route: route.key,
      limit: policy.limit,
      windowSeconds: policy.windowSeconds,
      replayKey,
      replayTtlSeconds,
    });
  } catch (_) {
    if (policy.mutating || policy.paid) {
      return aiJson(503, { error: 'AI request guard unavailable', request_id: requestId }, origin);
    }
    guardResult = { allowed: true, degraded: true };
  }
  if (guardResult && (guardResult.reason === 'replay' || guardResult.replay === true)) {
    return aiJson(409, { error: 'duplicate request rejected', request_id: requestId }, origin, quotaHeaders(guardResult, policy, true));
  }
  if (!guardResult || (guardResult.allowed !== true && guardResult.success !== true)) {
    return aiJson(429, { error: 'AI route quota exceeded', request_id: requestId }, origin, quotaHeaders(guardResult, policy, true));
  }

  try {
    const upstream = await fetch(DATA_PROXY + '/ai' + route.subPath + (search || ''), {
      method: request.method,
      headers: {
        Authorization: 'Bearer ' + identity.token,
        ...(request.method === 'POST' ? { 'Content-Type': 'application/json' } : {}),
      },
      body: request.method === 'POST' ? body.text : undefined,
      redirect: 'error',
      cache: 'no-store',
    });
    const text = await upstream.text();
    if (!upstream.ok) {
      const status = upstream.status === 401 || upstream.status === 403 || upstream.status === 404 || upstream.status === 429
        ? upstream.status : (upstream.status >= 400 && upstream.status < 500 ? 400 : 502);
      return aiJson(status, { error: 'AI control request failed', request_id: requestId }, origin, quotaHeaders(guardResult, policy));
    }
    let payload;
    try {
      payload = JSON.parse(text);
    } catch (_) {
      return aiJson(502, { error: 'invalid AI control response', request_id: requestId }, origin, quotaHeaders(guardResult, policy));
    }
    return aiJson(upstream.status, payload, origin, {
      ...quotaHeaders(guardResult, policy),
      ...(guardResult.degraded ? { 'X-RateLimit-Status': 'degraded-read-only' } : {}),
    });
  } catch (_) {
    return aiJson(502, { error: 'AI control service unavailable', request_id: requestId }, origin, quotaHeaders(guardResult, policy));
  }
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

async function handlePortfolioAdmin(request, env, origin) {
  // POST /portfolio-admin → justhodl-portfolio-admin Function URL.
  // Two-layer auth: the caller must present the manager PIN (x-mgr-pass);
  // the Worker then injects the real Lambda infra token so that powerful
  // credential never has to live in the browser.
  if (request.method !== 'POST') {
    return json(405, { error: 'Method not allowed (use POST)' }, origin);
  }
  if (!env.PORTFOLIO_ADMIN_TOKEN || !env.PORTFOLIO_MGR_PASS) {
    return json(500, { error: 'Worker misconfigured (portfolio secrets missing)' }, origin);
  }
  if (request.headers.get('x-mgr-pass') !== env.PORTFOLIO_MGR_PASS) {
    return json(403, { ok: false, err: 'forbidden' }, origin);
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
    upstream = await fetch(LAMBDA_PORTFOLIO_ADMIN, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Origin': 'https://justhodl.ai',
        'x-justhodl-token': env.PORTFOLIO_ADMIN_TOKEN,
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

// Forward /brain and /journal to the data-proxy worker (server-side, so browser
// adblock/extension filters on *.workers.dev don't apply). Passes through method,
// body, query string, and the brain pin header. Re-adds CORS for the browser.
async function handleDataProxy(request, origin, path, search) {
  try {
    const upstream = DATA_PROXY + path + (search || '');
    const init = { method: request.method, headers: {} };
    const ct = request.headers.get('Content-Type'); if (ct) init.headers['Content-Type'] = ct;
    const bp = request.headers.get('X-Brain-Pin'); if (bp) init.headers['X-Brain-Pin'] = bp;
    // audit 2026-09-08 INST-01: the data-proxy now authenticates Brain/Journal callers by a verified
    // Supabase Bearer token (or the service secret). Forward both so the bridge is not an auth bypass.
    const az = request.headers.get('Authorization'); if (az) init.headers['Authorization'] = az;
    const st = request.headers.get('X-JH-Service-Token'); if (st) init.headers['X-JH-Service-Token'] = st;
    if (request.method !== 'GET' && request.method !== 'HEAD') init.body = await request.text();
    const r = await fetch(upstream, init);
    const text = await r.text();
    return new Response(text, { status: r.status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...corsHeaders(origin) } });
  } catch (e) {
    return json(502, { error: 'brain proxy failed', detail: String(e).slice(0, 100) }, origin);
  }
}

function replayStorageKey(replayKey) {
  return REPLAY_STORAGE_PREFIX + replayKey;
}

function replayExpiryStorageKey(expiry, replayKey) {
  // Fixed-width timestamps keep the index ordered by expiration time.
  return REPLAY_EXPIRY_PREFIX + String(expiry).padStart(16, '0') + ':' + replayKey;
}

function parseReplayExpiryStorageKey(key) {
  const suffix = key.slice(REPLAY_EXPIRY_PREFIX.length);
  const separator = suffix.indexOf(':');
  if (separator < 1) return null;
  const expiryText = suffix.slice(0, separator);
  const replayKey = suffix.slice(separator + 1);
  const expiry = Number(expiryText);
  if (!/^\d{16}$/.test(expiryText)
      || !Number.isSafeInteger(expiry)
      || !/^[a-f0-9]{64}$/.test(replayKey)) return null;
  return { expiry, replayKey };
}

async function cleanupExpiredReplayEntries(txn, now) {
  // Replay lookup keys cannot carry a platform TTL, so maintain a second,
  // expiration-ordered index. Only a fixed batch is inspected/deleted per
  // request; live entries are never evicted merely to cap nonce count.
  const expiryEntries = await txn.list({
    prefix: REPLAY_EXPIRY_PREFIX,
    limit: REPLAY_CLEANUP_BATCH_SIZE,
  });
  const expired = [];
  const invalidIndexKeys = [];
  for (const [indexKey] of expiryEntries) {
    const parsed = parseReplayExpiryStorageKey(indexKey);
    if (!parsed) {
      invalidIndexKeys.push(indexKey);
      continue;
    }
    // The index is sorted oldest-first. Once a valid future expiration is
    // reached, all subsequent valid entries are also still live.
    if (parsed.expiry > now) break;
    expired.push({ ...parsed, indexKey, storageKey: replayStorageKey(parsed.replayKey) });
  }

  const indexKeysToDelete = invalidIndexKeys.concat(expired.map(entry => entry.indexKey));
  if (expired.length) {
    const storageKeys = [...new Set(expired.map(entry => entry.storageKey))];
    const currentExpiries = await txn.get(storageKeys);
    const replayKeysToDelete = [];
    for (const entry of expired) {
      // A nonce may have been accepted again after its earlier expiration.
      // Delete the lookup only when this index still describes that generation.
      if (currentExpiries.get(entry.storageKey) === entry.expiry) {
        replayKeysToDelete.push(entry.storageKey);
      }
    }
    if (replayKeysToDelete.length) await txn.delete(replayKeysToDelete);
  }
  if (indexKeysToDelete.length) await txn.delete(indexKeysToDelete);
}

// Atomic quota/replay backend. One Durable Object is selected per verified
// subject. Quotas remain in one record, while each accepted nonce has its own
// lookup key plus an expiration-ordered cleanup index. The transaction keeps
// quota consumption and full-window nonce retention serialized and fail-closed.
export class AiRequestGuard {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    if (request.method !== 'POST') {
      return new Response(JSON.stringify({ error: 'method not allowed' }), {
        status: 405,
        headers: { 'Content-Type': 'application/json', Allow: 'POST' },
      });
    }
    let input;
    try {
      input = await request.json();
    } catch (_) {
      return new Response(JSON.stringify({ error: 'invalid request' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    const route = String(input.route || '');
    const limit = Number(input.limit);
    const windowSeconds = Number(input.windowSeconds);
    const replayKey = input.replayKey ? String(input.replayKey) : '';
    const replayTtlSeconds = Number(input.replayTtlSeconds);
    if (!/^(GET|POST) \/[a-z0-9/-]{1,64}$/.test(route)
        || !Number.isSafeInteger(limit) || limit < 1 || limit > 10000
        || !Number.isSafeInteger(windowSeconds) || windowSeconds < 1 || windowSeconds > 86400
        || (replayKey && !/^[a-f0-9]{64}$/.test(replayKey))
        || (replayKey && (!Number.isSafeInteger(replayTtlSeconds) || replayTtlSeconds < 30 || replayTtlSeconds > 900))) {
      return new Response(JSON.stringify({ error: 'invalid request' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const now = Date.now();
    const result = await this.state.storage.transaction(async txn => {
      const record = await txn.get(GUARD_RECORD_KEY) || { quotas: {} };
      record.quotas = record.quotas && typeof record.quotas === 'object' ? record.quotas : {};
      // Compatibility for objects written by the initial implementation.
      // That map was capped at 512; keep its still-live nonces enforceable
      // while bounded batches age it out, but never add new entries to it.
      record.replays = record.replays && typeof record.replays === 'object' ? record.replays : {};
      let legacyCleanupCount = 0;
      for (const [key, expiry] of Object.entries(record.replays)) {
        if (legacyCleanupCount >= REPLAY_CLEANUP_BATCH_SIZE) break;
        if (!Number.isFinite(expiry) || expiry <= now) {
          delete record.replays[key];
          legacyCleanupCount += 1;
        }
      }

      await cleanupExpiredReplayEntries(txn, now);
      const storedReplayExpiry = replayKey
        ? Number(await txn.get(replayStorageKey(replayKey)))
        : 0;
      const legacyReplayExpiry = replayKey ? Number(record.replays[replayKey]) : 0;
      const activeReplayExpiry = Math.max(
        Number.isFinite(storedReplayExpiry) ? storedReplayExpiry : 0,
        Number.isFinite(legacyReplayExpiry) ? legacyReplayExpiry : 0,
      );
      if (replayKey && activeReplayExpiry > now) {
        await txn.put(GUARD_RECORD_KEY, record);
        return { allowed: false, reason: 'replay', remaining: 0, retryAfter: Math.ceil((activeReplayExpiry - now) / 1000) };
      }

      let quota = record.quotas[route];
      if (!quota || !Number.isFinite(quota.startedAt) || now - quota.startedAt >= windowSeconds * 1000) {
        quota = { startedAt: now, count: 0 };
      }
      if (quota.count >= limit) {
        record.quotas[route] = quota;
        await txn.put(GUARD_RECORD_KEY, record);
        return {
          allowed: false,
          reason: 'quota',
          remaining: 0,
          retryAfter: Math.max(1, Math.ceil((quota.startedAt + windowSeconds * 1000 - now) / 1000)),
        };
      }
      quota.count += 1;
      record.quotas[route] = quota;
      if (replayKey) {
        const replayExpiry = now + replayTtlSeconds * 1000;
        await txn.put(replayStorageKey(replayKey), replayExpiry);
        await txn.put(replayExpiryStorageKey(replayExpiry, replayKey), replayKey);
      }
      await txn.put(GUARD_RECORD_KEY, record);
      return {
        allowed: true,
        remaining: Math.max(0, limit - quota.count),
        retryAfter: Math.max(1, Math.ceil((quota.startedAt + windowSeconds * 1000 - now) / 1000)),
      };
    });
    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
    });
  }
}

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';

    // AI control routes authenticate independently of Origin. CORS determines
    // whether browser JavaScript can read the response, never who may act.
    if (path === '/ai' || path.startsWith('/ai/')) {
      if (request.method === 'OPTIONS') {
        return new Response(null, { status: 204, headers: aiCorsHeaders(origin) });
      }
      return handleAiControl(request, env, origin, path, url.search);
    }

    // CORS preflight — handle before Origin check
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    // Origin check
    if (!ALLOWED_ORIGINS.has(origin)) {
      return json(403, { error: 'Forbidden' }, origin);
    }

    // /brain and /journal → forward to the data-proxy worker. The browser can't
    // reach *.workers.dev directly (adblock/wallet-extension filters), but
    // api.justhodl.ai is on the user's own domain and never blocked. Worker→worker
    // server-side fetch is unaffected by browser filters.
    if (['/brain', '/journal', '/brain-debug', '/brain-purge', '/private-artifact', '/plan/self', '/plan/service', '/create-checkout', '/billing-portal', '/ask'].includes(path) || path.startsWith('/owner-api/')) {
      return handleDataProxy(request, origin, path, url.search);
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
    if (path === '/portfolio-admin') {
      return handlePortfolioAdmin(request, env, origin);
    }
    if (path === '/' || path === '/chat') {
      return handleAiChat(request, env, origin);
    }

    return json(404, { error: 'Not found', path }, origin);
  },
};
