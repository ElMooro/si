import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../src/index.js', import.meta.url), 'utf8');
const moduleUrl = 'data:text/javascript;base64,' + Buffer.from(source).toString('base64');
const { default: worker, AiRequestGuard } = await import(moduleUrl);

const ISSUER = 'https://identity.example.test';
const AUDIENCE = 'authenticated';
const JWKS_URL = ISSUER + '/.well-known/jwks.json';
const DATA_PROXY = 'https://justhodl-data-proxy.raafouis.workers.dev';
const nowSeconds = () => Math.floor(Date.now() / 1000);

const keyPair = await crypto.subtle.generateKey(
  {
    name: 'RSASSA-PKCS1-v1_5',
    modulusLength: 2048,
    publicExponent: new Uint8Array([1, 0, 1]),
    hash: 'SHA-256',
  },
  true,
  ['sign', 'verify'],
);
const publicJwk = await crypto.subtle.exportKey('jwk', keyPair.publicKey);
publicJwk.kid = 'test-key';
publicJwk.alg = 'RS256';
publicJwk.use = 'sig';

function b64url(value) {
  return Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)).toString('base64url');
}

async function tokenFor(overrides = {}, signingKey = keyPair.privateKey) {
  const header = b64url({ alg: 'RS256', typ: 'JWT', kid: 'test-key' });
  const claims = b64url({
    iss: ISSUER,
    aud: AUDIENCE,
    sub: 'owner-subject',
    email: 'owner@example.test',
    iat: nowSeconds() - 5,
    exp: nowSeconds() + 600,
    ...overrides,
  });
  const signature = await crypto.subtle.sign(
    { name: 'RSASSA-PKCS1-v1_5' },
    signingKey,
    new TextEncoder().encode(header + '.' + claims),
  );
  return header + '.' + claims + '.' + Buffer.from(signature).toString('base64url');
}

function allowGuard(overrides = {}) {
  const calls = [];
  return {
    calls,
    async check(input) {
      calls.push(input);
      return { allowed: true, remaining: 9, retryAfter: 60, ...overrides };
    },
  };
}

function baseEnv(overrides = {}) {
  return {
    AUTH_JWKS_URL: JWKS_URL,
    AUTH_ISSUER: ISSUER,
    AUTH_AUDIENCE: AUDIENCE,
    OWNER_EMAILS: 'owner@example.test',
    ADMIN_ROLES: 'admin',
    AI_REQUEST_GUARD: allowGuard(),
    ...overrides,
  };
}

function jsonResponse(body, status = 200, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}

function fetchHarness({ jwksUrl = JWKS_URL, upstream = jsonResponse({ ok: true }) } = {}) {
  const calls = [];
  const fn = async (input, init = {}) => {
    const url = String(input);
    calls.push({ url, init });
    if (url === jwksUrl) return jsonResponse({ keys: [publicJwk] }, 200, { 'Cache-Control': 'max-age=600' });
    if (url.startsWith(DATA_PROXY + '/ai')) {
      return typeof upstream === 'function' ? upstream(input, init) : upstream;
    }
    throw new Error('unexpected fetch: ' + url);
  };
  return { fn, calls };
}

async function runWithFetch(fetchImpl, operation) {
  const original = globalThis.fetch;
  globalThis.fetch = fetchImpl;
  try {
    return await operation();
  } finally {
    globalThis.fetch = original;
  }
}

function aiRequest(path, {
  method = 'GET',
  token,
  origin = 'https://justhodl.ai',
  body,
  nonce = 'nonce-1234567890abcdef',
  timestamp = String(Date.now()),
  headers = {},
} = {}) {
  const requestHeaders = new Headers(headers);
  if (origin !== null) requestHeaders.set('Origin', origin);
  if (token) requestHeaders.set('Authorization', 'Bearer ' + token);
  if (method === 'POST') {
    if (!requestHeaders.has('Content-Type')) requestHeaders.set('Content-Type', 'application/json');
    if (nonce !== null) requestHeaders.set('X-Request-Nonce', nonce);
    if (timestamp !== null) requestHeaders.set('X-Request-Timestamp', timestamp);
  }
  return new Request('https://api.justhodl.ai' + path, {
    method,
    headers: requestHeaders,
    body: method === 'POST' ? (typeof body === 'string' ? body : JSON.stringify(body ?? {})) : undefined,
  });
}

test('missing bearer is rejected even with an allowed Origin', async () => {
  let fetched = false;
  const response = await runWithFetch(async () => {
    fetched = true;
    throw new Error('must not fetch');
  }, () => worker.fetch(aiRequest('/ai/inventory'), baseEnv()));

  assert.equal(response.status, 401);
  assert.equal(response.headers.get('Access-Control-Allow-Origin'), 'https://justhodl.ai');
  assert.equal(response.headers.get('WWW-Authenticate'), 'Bearer');
  assert.equal((await response.json()).error, 'authentication required');
  assert.equal(fetched, false);
});

test('invalid, expired, and wrong-audience bearer tokens are rejected', async () => {
  const expired = await tokenFor({ exp: nowSeconds() - 100 });
  const wrongAudience = await tokenFor({ aud: 'other-service' });
  const tamperedParts = (await tokenFor()).split('.');
  tamperedParts[2] = (tamperedParts[2][0] === 'A' ? 'B' : 'A') + tamperedParts[2].slice(1);
  const tampered = tamperedParts.join('.');
  const harness = fetchHarness();

  await runWithFetch(harness.fn, async () => {
    for (const token of [expired, wrongAudience, tampered]) {
      const response = await worker.fetch(aiRequest('/ai/inventory', { token }), baseEnv());
      assert.equal(response.status, 401);
      assert.equal((await response.json()).error, 'authentication required');
    }
  });
  assert.equal(harness.calls.some(call => call.url.startsWith(DATA_PROXY)), false);
});

test('a verified non-owner cannot use the AI control plane', async () => {
  const token = await tokenFor({ sub: 'regular-user', email: 'user@example.test' });
  const harness = fetchHarness();
  const response = await runWithFetch(harness.fn, () =>
    worker.fetch(aiRequest('/ai/inventory', { token }), baseEnv()));

  assert.equal(response.status, 403);
  assert.equal((await response.json()).error, 'owner or administrator access required');
  assert.equal(harness.calls.some(call => call.url.startsWith(DATA_PROXY)), false);
});

test('legacy bearer tokens are verified by Supabase user introspection without trusting user metadata', async () => {
  const calls = [];
  const fetchImpl = async (input, init = {}) => {
    const url = String(input);
    calls.push({ url, init });
    if (url === 'https://supabase.example.test/auth/v1/user') {
      assert.equal(init.headers.Authorization, 'Bearer legacy-token-which-is-long-enough');
      assert.equal(init.headers.apikey, 'publishable-key');
      return jsonResponse({
        id: 'legacy-owner',
        email: 'owner@example.test',
        app_metadata: {},
        user_metadata: { role: 'admin' },
      });
    }
    if (url === DATA_PROXY + '/ai/inventory') return jsonResponse({ ok: true });
    throw new Error('unexpected fetch: ' + url);
  };
  const response = await runWithFetch(fetchImpl, () =>
    worker.fetch(
      aiRequest('/ai/inventory', { token: 'legacy-token-which-is-long-enough' }),
      baseEnv({
        AUTH_JWKS_URL: '',
        SUPABASE_URL: 'https://supabase.example.test',
        SUPABASE_ANON_KEY: 'publishable-key',
      }),
    ));

  assert.equal(response.status, 200);
  assert.equal(calls.some(call => call.url === DATA_PROXY + '/ai/inventory'), true);

  const userOnly = await runWithFetch(async (input) => {
    if (String(input) === 'https://supabase.example.test/auth/v1/user') {
      return jsonResponse({
        id: 'not-owner',
        email: 'not-owner@example.test',
        app_metadata: {},
        user_metadata: { role: 'admin' },
      });
    }
    throw new Error('upstream must not be called');
  }, () => worker.fetch(
    aiRequest('/ai/inventory', { token: 'another-legacy-token-long-enough' }),
    baseEnv({
      AUTH_JWKS_URL: '',
      SUPABASE_URL: 'https://supabase.example.test',
      SUPABASE_ANON_KEY: 'publishable-key',
    }),
  ));
  assert.equal(userOnly.status, 403);
});

test('verified owners and signed admin roles are authorized and attributed by subject/route', async () => {
  const ownerToken = await tokenFor();
  const adminToken = await tokenFor({
    sub: 'admin-subject',
    email: 'admin@example.test',
    app_metadata: { roles: ['admin'] },
  });
  const guard = allowGuard();
  const harness = fetchHarness({
    upstream: (input) => jsonResponse({ ok: true, target: String(input) }),
  });

  await runWithFetch(harness.fn, async () => {
    const ownerResponse = await worker.fetch(
      aiRequest('/ai/inventory', { token: ownerToken }),
      baseEnv({ AI_REQUEST_GUARD: guard }),
    );
    const adminResponse = await worker.fetch(
      aiRequest('/ai/model?model_id=abc', { token: adminToken }),
      baseEnv({ AI_REQUEST_GUARD: guard }),
    );
    assert.equal(ownerResponse.status, 200);
    assert.equal(adminResponse.status, 200);
  });

  assert.deepEqual(
    guard.calls.map(call => [call.subject, call.route]),
    [['owner-subject', 'GET /inventory'], ['admin-subject', 'GET /model']],
  );
  const forwarded = harness.calls.filter(call => call.url.startsWith(DATA_PROXY));
  assert.equal(forwarded[0].url, DATA_PROXY + '/ai/inventory');
  assert.equal(forwarded[1].url, DATA_PROXY + '/ai/model?model_id=abc');
  assert.equal(forwarded[0].init.headers.Authorization, 'Bearer ' + ownerToken);
});

test('governance routes are exact, owner-only, and forwarded without wildcard matching', async () => {
  const ownerToken = await tokenFor();
  const viewerToken = await tokenFor({
    sub: 'viewer-subject', email: 'viewer@example.test', app_metadata: { roles: ['viewer'] },
  });
  const guard = allowGuard();
  const harness = fetchHarness({ upstream: jsonResponse({ ok: true }) });
  await runWithFetch(harness.fn, async () => {
    const owner = await worker.fetch(aiRequest('/ai/governance/signals/validate', {
      method: 'POST', token: ownerToken, body: { envelope: {} },
    }), baseEnv({ AI_REQUEST_GUARD: guard }));
    const viewer = await worker.fetch(aiRequest('/ai/governance/signals/validate', {
      method: 'POST', token: viewerToken, body: { envelope: {} }, nonce: 'viewer-nonce-123456789',
    }), baseEnv({ AI_REQUEST_GUARD: guard }));
    const wildcard = await worker.fetch(aiRequest('/ai/governance/signals/validate/extra', {
      method: 'POST', token: ownerToken, body: {}, nonce: 'wildcard-nonce-1234567',
    }), baseEnv({ AI_REQUEST_GUARD: guard }));
    assert.equal(owner.status, 200);
    assert.equal(viewer.status, 403);
    assert.equal(wildcard.status, 404);
  });
  assert.equal(guard.calls[0].route, 'POST /governance/signals/validate');
  assert.equal(harness.calls.some(call => call.url === DATA_PROXY + '/ai/governance/signals/validate'), true);
});

test('CORS is not authorization and an unlisted Origin is not an identity gate', async () => {
  const noAuth = await worker.fetch(
    aiRequest('/ai/inventory', { origin: 'https://justhodl.ai' }),
    baseEnv(),
  );
  assert.equal(noAuth.status, 401);

  const token = await tokenFor();
  const harness = fetchHarness();
  const authenticated = await runWithFetch(harness.fn, () =>
    worker.fetch(
      aiRequest('/ai/inventory', { token, origin: 'https://attacker.example' }),
      baseEnv(),
    ));
  assert.equal(authenticated.status, 200);
  assert.equal(authenticated.headers.get('Access-Control-Allow-Origin'), null);
});

test('per-subject, per-route quota denial returns 429 without calling upstream', async () => {
  const token = await tokenFor();
  const seen = new Map();
  const guard = {
    async check(input) {
      const key = input.subject + '|' + input.route;
      const count = (seen.get(key) || 0) + 1;
      seen.set(key, count);
      return count === 1
        ? { allowed: true, remaining: 0, retryAfter: 60 }
        : { allowed: false, reason: 'quota', remaining: 0, retryAfter: 42 };
    },
  };
  const harness = fetchHarness();

  await runWithFetch(harness.fn, async () => {
    const first = await worker.fetch(
      aiRequest('/ai/inventory', { token }),
      baseEnv({ AI_REQUEST_GUARD: guard, AI_ROUTE_QUOTAS: '{"GET /inventory":{"limit":1,"window_seconds":60}}' }),
    );
    const second = await worker.fetch(
      aiRequest('/ai/inventory', { token }),
      baseEnv({ AI_REQUEST_GUARD: guard, AI_ROUTE_QUOTAS: '{"GET /inventory":{"limit":1,"window_seconds":60}}' }),
    );
    assert.equal(first.status, 200);
    assert.equal(second.status, 429);
    assert.equal(second.headers.get('RateLimit-Limit'), '1');
    assert.equal(second.headers.get('Retry-After'), '42');
    assert.equal((await second.json()).error, 'AI route quota exceeded');
  });
  assert.equal(harness.calls.filter(call => call.url.startsWith(DATA_PROXY)).length, 1);
});

test('paid or mutating requests fail closed when the guard is missing or throws', async () => {
  const token = await tokenFor();
  const harness = fetchHarness();

  await runWithFetch(harness.fn, async () => {
    for (const guard of [undefined, { async check() { throw new Error('backend secret'); } }]) {
      const response = await worker.fetch(
        aiRequest('/ai/infer', { method: 'POST', token, body: { text: 'hello' } }),
        baseEnv({ AI_REQUEST_GUARD: guard }),
      );
      assert.equal(response.status, 503);
      const payload = await response.json();
      assert.equal(payload.error, 'AI request guard unavailable');
      assert.equal(JSON.stringify(payload).includes('backend secret'), false);
    }
  });
  assert.equal(harness.calls.some(call => call.url.startsWith(DATA_PROXY)), false);
});

test('read-only status remains available in explicitly marked degraded guard mode', async () => {
  const token = await tokenFor();
  const harness = fetchHarness();
  const response = await runWithFetch(harness.fn, () =>
    worker.fetch(
      aiRequest('/ai', { token, origin: null }),
      baseEnv({ AI_REQUEST_GUARD: undefined }),
    ));

  assert.equal(response.status, 200);
  assert.equal(response.headers.get('X-RateLimit-Status'), 'degraded-read-only');
  assert.equal(response.headers.get('Access-Control-Allow-Origin'), null);
  assert.equal(harness.calls.find(call => call.url.startsWith(DATA_PROXY)).url, DATA_PROXY + '/ai/status');
});

test('POST replay headers are required, freshness checked, and duplicate hashes rejected', async () => {
  const token = await tokenFor();
  const replayKeys = new Set();
  const guard = {
    async check(input) {
      if (replayKeys.has(input.replayKey)) return { allowed: false, reason: 'replay', retryAfter: 240 };
      replayKeys.add(input.replayKey);
      return { allowed: true, remaining: 5, retryAfter: 60 };
    },
  };
  const harness = fetchHarness();

  await runWithFetch(harness.fn, async () => {
    const missing = await worker.fetch(
      aiRequest('/ai/infer', { method: 'POST', token, nonce: null, body: { text: 'x' } }),
      baseEnv({ AI_REQUEST_GUARD: guard }),
    );
    assert.equal(missing.status, 400);
    assert.equal((await missing.json()).error, 'valid request nonce required');

    const stale = await worker.fetch(
      aiRequest('/ai/infer', {
        method: 'POST',
        token,
        timestamp: String(Date.now() - 10 * 60 * 1000),
        body: { text: 'x' },
      }),
      baseEnv({ AI_REQUEST_GUARD: guard }),
    );
    assert.equal(stale.status, 400);
    assert.equal((await stale.json()).error, 'request timestamp outside allowed window');

    const timestamp = String(Date.now());
    const options = {
      method: 'POST',
      token,
      timestamp,
      nonce: 'same-nonce-123456789',
      body: { text: 'same' },
    };
    const first = await worker.fetch(aiRequest('/ai/infer', options), baseEnv({ AI_REQUEST_GUARD: guard }));
    const replay = await worker.fetch(
      aiRequest('/ai/infer', { ...options, body: { text: 'changed replay body' } }),
      baseEnv({ AI_REQUEST_GUARD: guard }),
    );
    assert.equal(first.status, 200);
    assert.equal(replay.status, 409);
    assert.equal((await replay.json()).error, 'duplicate request rejected');
  });
  assert.equal(harness.calls.filter(call => call.url.startsWith(DATA_PROXY)).length, 1);
});

test('POST bodies enforce declared and actual byte limits and valid JSON content type', async () => {
  const token = await tokenFor();
  const harness = fetchHarness();
  const env = baseEnv({ AI_MAX_BODY_BYTES: '1024' });

  await runWithFetch(harness.fn, async () => {
    const declared = await worker.fetch(
      aiRequest('/ai/infer', {
        method: 'POST',
        token,
        body: {},
        headers: { 'Content-Length': '2048' },
      }),
      env,
    );
    assert.equal(declared.status, 413);

    const actual = await worker.fetch(
      aiRequest('/ai/infer', {
        method: 'POST',
        token,
        body: { text: 'x'.repeat(1100) },
      }),
      env,
    );
    assert.equal(actual.status, 413);

    const wrongType = await worker.fetch(
      aiRequest('/ai/infer', {
        method: 'POST',
        token,
        body: '{}',
        headers: { 'Content-Type': 'text/plain' },
      }),
      env,
    );
    assert.equal(wrongType.status, 415);
  });
  assert.equal(harness.calls.some(call => call.url.startsWith(DATA_PROXY)), false);
});

test('identity and upstream backend failures return safe errors without exception or upstream details', async () => {
  const uncachedUrl = ISSUER + '/unavailable/jwks.json';
  const token = await tokenFor();
  const identityResponse = await runWithFetch(async () => {
    throw new Error('private identity diagnostic');
  }, () => worker.fetch(
    aiRequest('/ai/inventory', { token }),
    baseEnv({ AUTH_JWKS_URL: uncachedUrl }),
  ));
  assert.equal(identityResponse.status, 503);
  assert.equal(JSON.stringify(await identityResponse.json()).includes('private identity diagnostic'), false);

  const harness = fetchHarness({
    upstream: new Response('sensitive AWS stack and credential', { status: 500 }),
  });
  const upstreamResponse = await runWithFetch(harness.fn, () =>
    worker.fetch(aiRequest('/ai/inventory', { token }), baseEnv()));
  assert.equal(upstreamResponse.status, 502);
  const upstreamPayload = await upstreamResponse.json();
  assert.equal(upstreamPayload.error, 'AI control request failed');
  assert.equal(JSON.stringify(upstreamPayload).includes('sensitive'), false);
});

test('existing non-AI research routing and Origin gate remain unchanged', async () => {
  const calls = [];
  const upstream = async (input, init) => {
    calls.push({ url: String(input), init });
    return jsonResponse({ research: true });
  };
  const allowed = await runWithFetch(upstream, () =>
    worker.fetch(new Request('https://api.justhodl.ai/research?ticker=AAPL', {
      headers: { Origin: 'https://justhodl.ai' },
    }), {}));
  assert.equal(allowed.status, 200);
  assert.deepEqual(await allowed.json(), { research: true });
  assert.match(calls[0].url, /lambda-url.*\?ticker=AAPL$/);

  const denied = await worker.fetch(new Request('https://api.justhodl.ai/research?ticker=AAPL', {
    headers: { Origin: 'https://attacker.example' },
  }), {});
  assert.equal(denied.status, 403);
});

class MemoryStorage {
  constructor() {
    this.values = new Map();
    this.listCalls = [];
  }

  async transaction(operation) {
    return operation(this);
  }

  async get(key) {
    if (Array.isArray(key)) {
      return new Map(key
        .filter(item => this.values.has(item))
        .map(item => [item, structuredClone(this.values.get(item))]));
    }
    return structuredClone(this.values.get(key));
  }

  async put(key, value) {
    this.values.set(key, structuredClone(value));
  }

  async delete(key) {
    if (Array.isArray(key)) {
      let deleted = 0;
      for (const item of key) deleted += this.values.delete(item) ? 1 : 0;
      return deleted;
    }
    return this.values.delete(key);
  }

  async list(options = {}) {
    this.listCalls.push(structuredClone(options));
    let entries = [...this.values.entries()].sort(([left], [right]) => left.localeCompare(right));
    if (options.prefix) entries = entries.filter(([key]) => key.startsWith(options.prefix));
    if (options.start) entries = entries.filter(([key]) => key >= options.start);
    if (options.startAfter) entries = entries.filter(([key]) => key > options.startAfter);
    if (options.end) entries = entries.filter(([key]) => key < options.end);
    if (options.reverse) entries.reverse();
    if (Number.isSafeInteger(options.limit)) entries = entries.slice(0, options.limit);
    return new Map(entries.map(([key, value]) => [key, structuredClone(value)]));
  }
}

test('AiRequestGuard atomically enforces route quotas and replay keys', async () => {
  const guard = new AiRequestGuard({ storage: new MemoryStorage() });
  const check = async body => {
    const response = await guard.fetch(new Request('https://guard.internal/check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }));
    assert.equal(response.status, 200);
    return response.json();
  };
  const base = { route: 'GET /inventory', limit: 2, windowSeconds: 60 };
  assert.equal((await check(base)).allowed, true);
  assert.equal((await check(base)).allowed, true);
  const denied = await check(base);
  assert.equal(denied.allowed, false);
  assert.equal(denied.reason, 'quota');

  const replayBase = {
    route: 'POST /infer',
    limit: 2,
    windowSeconds: 60,
    replayKey: 'a'.repeat(64),
    replayTtlSeconds: 300,
  };
  assert.equal((await check(replayBase)).allowed, true);
  const replay = await check(replayBase);
  assert.equal(replay.allowed, false);
  assert.equal(replay.reason, 'replay');
});

test('AiRequestGuard retains an unexpired nonce after 512 additional fresh nonces', async () => {
  const storage = new MemoryStorage();
  const guard = new AiRequestGuard({ storage });
  const check = async replayKey => {
    const response = await guard.fetch(new Request('https://guard.internal/check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        route: 'POST /infer',
        limit: 1000,
        windowSeconds: 60,
        replayKey,
        replayTtlSeconds: 300,
      }),
    }));
    assert.equal(response.status, 200);
    return response.json();
  };

  const original = 'f'.repeat(64);
  assert.equal((await check(original)).allowed, true);
  for (let index = 0; index < 512; index += 1) {
    const fresh = index.toString(16).padStart(64, '0');
    assert.equal((await check(fresh)).allowed, true);
  }

  const replay = await check(original);
  assert.equal(replay.allowed, false);
  assert.equal(replay.reason, 'replay');
  assert.equal(
    [...storage.values.keys()].filter(key => key.startsWith('replay:v2:')).length,
    513,
  );
});

test('AiRequestGuard prunes expired nonce storage in bounded batches', async () => {
  const storage = new MemoryStorage();
  const now = Date.now();
  for (let index = 0; index < 130; index += 1) {
    const replayKey = index.toString(16).padStart(64, '0');
    const expiry = now - 10_000 - index;
    await storage.put('replay:v2:' + replayKey, expiry);
    await storage.put(
      'replay-expiry:v2:' + String(expiry).padStart(16, '0') + ':' + replayKey,
      replayKey,
    );
  }

  const guard = new AiRequestGuard({ storage });
  const response = await guard.fetch(new Request('https://guard.internal/check', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ route: 'GET /inventory', limit: 10, windowSeconds: 60 }),
  }));
  assert.equal(response.status, 200);
  assert.equal((await response.json()).allowed, true);
  assert.equal(storage.listCalls.at(-1).limit, 64);
  assert.equal(
    [...storage.values.keys()].filter(key => key.startsWith('replay-expiry:v2:')).length,
    66,
  );
  assert.equal(
    [...storage.values.keys()].filter(key => key.startsWith('replay:v2:')).length,
    66,
  );
});
