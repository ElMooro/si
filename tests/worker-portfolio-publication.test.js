// Current production module with invented packets and transactional in-memory
// storage. No network, actual accounts, provider probes or archived-code execution.
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const fs = require('node:fs');
const root = path.join(__dirname, '..');
const src = path.join(root, 'cloudflare/workers/justhodl-data-proxy/src');
const encoder = new TextEncoder(), decoder = new TextDecoder();
const ADMIN = 'invented-portfolio-service-only-446';
const KIND = 'private-artifact:portfolio-risk';
const clone = x => structuredClone(x);
const api = import(pathToFileURL(path.join(src, 'portfolio-publication.js')).href);
const modulePromise = import(pathToFileURL(path.join(src, 'index.js')).href);

class Storage {
  constructor() { this.map = new Map(); this.writes = []; this.failAt = null; }
  methods(map) {
    return {
      get: async key => clone(map.get(key)),
      put: async (key, value) => {
        assert.equal(typeof key, 'string');
        const size = value instanceof Uint8Array ? value.byteLength : encoder.encode(JSON.stringify(value)).byteLength;
        assert.ok(size + encoder.encode(key).byteLength < 2 * 1024 * 1024, 'SQLite value bound');
        this.writes.push(key); if (this.failAt?.(key)) throw Error('SYNTHETIC_PRIVATE_EXCEPTION_MUST_NOT_LEAK');
        map.set(key, clone(value));
      },
      delete: async key => map.delete(key),
      list: async ({ prefix, limit }) => new Map([...map].filter(([k]) => k.startsWith(prefix)).sort(([a], [b]) => a.localeCompare(b)).slice(0, limit).map(([k, v]) => [k, clone(v)])),
    };
  }
  async get(key) { return this.methods(this.map).get(key); }
  async put(key, value) { return this.methods(this.map).put(key, value); }
  async list(options) { return this.methods(this.map).list(options); }
  async transaction(fn) {
    const staged = clone(this.map);
    const result = await fn(this.methods(staged));
    this.map = staged; return result;
  }
}

async function fixture(legacy = '{"legacy":true,"zero":0,"null":null,"unknown":"雪"}\n') {
  const { default: worker, WorkspaceCoordinator } = await modulePromise;
  const p = await api, storage = new Storage(), objects = new Map(), state = { reads: 0, puts: 0, routes: [], legacy, barrier: null };
  globalThis.__jhTokCache = new Map();
  globalThis.fetch = async (url, init) => {
    assert.equal(url, 'https://synthetic.invalid/auth/v1/user');
    const token = init.headers.Authorization;
    if (token === 'Bearer owner_synthetic_token_446') return Response.json({ id: 'synthetic-owner-446', email: 'owner@synthetic.invalid' });
    if (token === 'Bearer other_synthetic_token_446') return Response.json({ id: 'synthetic-other-446', email: 'other@synthetic.invalid' });
    return Response.json({ error: 'invalid' }, { status: 401 });
  };
  const env = {
    ADMIN_TOKEN: ADMIN, SUPABASE_URL: 'https://synthetic.invalid', SUPABASE_SERVICE_KEY: 'synthetic-service', OWNER_EMAILS: 'owner@synthetic.invalid',
    USER_DATA: {
      async get(key, options) {
        if (key === 'owner:uids') return null;
        assert.equal(key, KIND); state.reads++; if (state.onLegacyRead) state.onLegacyRead(); if (state.barrier) await state.barrier;
        if (state.legacy === null) return null;
        assert.equal(options.type, 'arrayBuffer');
        return typeof state.legacy === 'string' ? encoder.encode(state.legacy).buffer : state.legacy;
      },
      async put(key, value) { assert.equal(key, KIND); state.puts++; state.legacy = value; },
    },
    WORKSPACE_COORDINATOR: {
      idFromName(name) { assert.equal(name, p.RISK_OBJECT); return name; },
      get(name) { return { async fetch(request) {
        state.routes.push({ method: request.method, name });
        if (!objects.has(name)) objects.set(name, new WorkspaceCoordinator({ storage }, env));
        const pending = objects.get(name).fetch(request);
        if (state.onEnqueued) state.onEnqueued(request);
        return pending;
      } }; },
    },
  };
  async function call(method = 'GET', body, extra = {}, route = '/private-artifact?kind=portfolio-risk', role = 'service') {
    const authorization = role === 'service' ? { 'X-JH-Service-Token': ADMIN } : role === 'anon' ? {} : { Authorization: 'Bearer ' + role + '_synthetic_token_446' };
    return worker.fetch(new Request('https://worker.synthetic.invalid' + route, {
      method, headers: { ...authorization, ...extra }, ...(body === undefined ? {} : { body }),
    }), env, { waitUntil() { throw Error('No asynchronous external writes'); } });
  }
  async function reserve(minimum = 0) {
    const response = await call('POST', JSON.stringify({ minimum_revision: minimum }), {}, '/private-artifact?kind=portfolio-risk&action=reserve');
    assert.equal(response.status, 200, await response.clone().text()); return response.json();
  }
  function packet(ticket, suffix = '') {
    const value = { status: 'INCOMPLETE', generated_at: null, permissions: { sizing_eligible: false }, alerts_sent: 0,
      snapshot_binding: { encoding: 'typed-json-binary64.v1', value_sha256: 'a'.repeat(64), generated_at: null },
      complete_unknown: { zero: 0, absent: null, boolean: false, text: '雪 😀' + suffix },
      publication: { schema_version: p.RISK_PROTOCOL, revision: ticket.revision, source_etag: '"synthetic-source-etag"',
        source_value_sha256: 'a'.repeat(64), started_at: '2026-09-18T17:00:00.123456+00:00' } };
    return ' \n' + JSON.stringify(value) + '\n';
  }
  async function publish(ticket, raw = packet(ticket), overrides = {}) {
    return call('PUT', raw, { 'X-JH-Publication-Token': ticket.token, 'X-JH-Body-SHA256': await p.riskDigest(encoder.encode(raw)), ...overrides });
  }
  return { p, storage, state, env, call, reserve, packet, publish, restart() { objects.clear(); } };
}

test('legacy full packets and existing authorized reads remain usable before activation', async () => {
  const f = await fixture(), original = f.state.legacy;
  for (const role of ['owner', 'service']) {
    const r = await f.call('GET', undefined, {}, '/data/portfolio/risk.json', role);
    assert.equal(r.status, 200); assert.equal(await r.text(), original);
    assert.equal(r.headers.get('Cache-Control'), 'private, no-store'); assert.equal(r.headers.get('Vary'), 'Authorization');
  }
  const raw = ' {"status":"INCOMPLETE","no_timestamp":true,"zero":-0.0,"unknown":"雪"} \n';
  const write = await f.call('PUT', raw); assert.equal(write.status, 200);
  assert.equal((await write.json()).publication_ordering, 'legacy_unverified');
  assert.equal(f.state.legacy, raw); assert.equal(f.storage.map.size, 0);
});

test('all aliases preserve owner/service boundary and cannot reserve as owner', async () => {
  const f = await fixture();
  for (const role of ['anon', 'other']) for (const route of ['/data/portfolio/risk.json', '/portfolio/risk.json', '/private-artifact?kind=portfolio-risk&action=reserve']) {
    for (const method of ['GET', 'HEAD', 'POST', 'PUT']) {
      const response = await f.call(method, ['GET', 'HEAD'].includes(method) ? undefined : '{}', {}, route, role);
      assert.equal(response.status, role === 'anon' ? 401 : 403);
    }
  }
  for (const method of ['POST', 'PUT']) assert.equal((await f.call(method, '{}', {}, '/private-artifact?kind=portfolio-risk&action=reserve', 'owner')).status, 403);
  assert.equal((await f.call('PUT', '{}', {}, '/data/portfolio/risk.json')).status, 405);
  assert.equal(f.state.reads, 0); assert.equal(f.state.puts, 0); assert.equal(f.state.routes.length, 0);
});

test('durable revisions survive restart, monotonic minimum and bounded reservations', async () => {
  const f = await fixture();
  assert.equal((await f.reserve()).revision, 1); f.restart();
  assert.equal((await f.reserve(20)).revision, 21);
  for (let i = 0; i < 140; i++) await f.reserve();
  assert.equal(f.storage.map.get('risk:counter'), 161);
  assert.equal([...f.storage.map.keys()].filter(k => k.startsWith('risk:reservation:')).length, 128);
  assert.equal(f.state.puts, 0); assert.equal(f.state.reads, 0);
});

test('later reserved packet wins; older and legacy arrivals cannot roll it back', async () => {
  const f = await fixture(), legacy = f.state.legacy, old = await f.reserve(), fresh = await f.reserve();
  const newer = f.packet(fresh, 'newer'); assert.equal((await f.publish(fresh, newer)).status, 200);
  assert.equal((await f.publish(old, f.packet(old, 'old'))).status, 409);
  assert.equal((await f.call('PUT', '{"legacy":"late"}')).status, 409);
  assert.equal(f.state.legacy, legacy); assert.equal(f.state.puts, 0);
  f.state.legacy = '{"legacy":"simulated old Worker in flight"}'; f.restart();
  const get = await f.call(); assert.equal(await get.text(), newer);
  const storedLegacy = f.storage.map.get('risk:legacy');
  assert.equal(decoder.decode(f.storage.map.get(storedLegacy.prefix + '0')), legacy);
  assert.equal(storedLegacy.sha256, await f.p.riskDigest(encoder.encode(legacy)));
});

test('same revision exact retry is idempotent; token or bytes conflict is rejected', async () => {
  const f = await fixture(), ticket = await f.reserve(), raw = f.packet(ticket);
  const first = await f.publish(ticket, raw); assert.equal((await first.json()).status, 'published');
  const count = f.storage.writes.length;
  const retry = await f.publish(ticket, raw); assert.equal((await retry.json()).status, 'unchanged'); assert.equal(f.storage.writes.length, count);
  assert.equal((await f.publish(ticket, raw + ' ')).status, 409);
  assert.equal((await f.publish({ ...ticket, token: '11111111-1111-4111-8111-111111111111' }, raw)).status, 409);
  const get = await f.call(); assert.equal(await get.text(), raw); assert.equal(get.headers.get('X-JH-Body-SHA256'), await f.p.riskDigest(encoder.encode(raw)));
  assert.ok(!raw.includes(ticket.token));
});

test('complete 3 MiB Unicode packet is chunked under platform limits and survives restart', async () => {
  const f = await fixture(), ticket = await f.reserve(), raw = f.packet(ticket, '雪'.repeat(1024 * 1024));
  assert.ok(encoder.encode(raw).byteLength > 3 * 1024 * 1024);
  const r = await f.publish(ticket, raw); assert.equal(r.status, 200, await r.clone().text());
  assert.ok(f.storage.map.get('risk:current').body.chunks > 10); f.restart();
  assert.equal(await (await f.call()).text(), raw);
  const head = await f.call('HEAD'); assert.equal(head.status, 200); assert.equal(await head.text(), '');
  assert.equal(Number(head.headers.get('Content-Length')), encoder.encode(raw).byteLength);
});

test('chunk/pointer transaction rollback preserves prior full body and allows safe retry', async () => {
  for (const target of ['risk:body:2:1', 'risk:current', 'risk:governed']) {
    const f = await fixture(), first = await f.reserve(); assert.equal((await f.publish(first)).status, 200);
    const before = clone(f.storage.map), next = await f.reserve(), raw = f.packet(next, 'x'.repeat(300000));
    const reserved = clone(f.storage.map); f.storage.failAt = key => key === target;
    const response = await f.publish(next, raw); assert.equal(response.status, 503); assert.ok(!(await response.text()).includes('SYNTHETIC_PRIVATE'));
    assert.deepEqual(f.storage.map, reserved); f.restart();
    assert.equal(await (await f.call()).text(), f.packet(first));
    f.storage.failAt = null; assert.equal((await f.publish(next, raw)).status, 200);
    assert.equal(await (await f.call()).text(), raw); assert.ok(!f.storage.map.has(before.get('risk:current').body.prefix + '0'));
  }
});

test('first migration transaction failure never changes KV or leaves partial chunks', async () => {
  const f = await fixture(), ticket = await f.reserve(), before = clone(f.storage.map), legacy = f.state.legacy;
  f.storage.failAt = key => key === 'risk:current';
  assert.equal((await f.publish(ticket)).status, 503); assert.deepEqual(f.storage.map, before); assert.equal(f.state.legacy, legacy);
  f.restart(); assert.equal(await (await f.call()).text(), legacy);
});

test('stored missing/corrupt chunk or lost pointer cannot fall back to stale KV', async () => {
  for (const damage of ['missing', 'changed', 'pointer', 'marker', 'manifest']) {
    const f = await fixture(), ticket = await f.reserve(); assert.equal((await f.publish(ticket)).status, 200);
    const head = f.storage.map.get('risk:current'), key = head.body.prefix + '0';
    if (damage === 'missing') f.storage.map.delete(key);
    if (damage === 'changed') f.storage.map.get(key)[5] ^= 1;
    if (damage === 'pointer') f.storage.map.delete('risk:current');
    if (damage === 'marker') f.storage.map.delete('risk:governed');
    if (damage === 'manifest') head.body.bytes += 1;
    f.restart(); const reads = f.state.reads;
    for (const method of ['GET', 'HEAD']) assert.equal((await f.call(method)).status, 503, damage);
    assert.equal(f.state.reads, reads); assert.equal((await f.publish(ticket)).status, 503, damage);
  }
});

test('no legacy body is valid; malformed legacy bytes block migration without loss', async () => {
  const empty = await fixture(null), ticket = await empty.reserve(); assert.equal((await empty.publish(ticket)).status, 200);
  for (const body of ['{"duplicate":1,"duplicate":2}', '{"bad":1e999}', '{"bad":"\\ud800"}', '{"incomplete":', new Uint8Array([0xff]).buffer]) {
    const f = await fixture(body), t = await f.reserve(), before = clone(f.storage.map);
    assert.equal((await f.publish(t)).status, 503); assert.deepEqual(f.storage.map, before);
    assert.equal(f.state.legacy, body); assert.equal(f.state.puts, 0);
  }
});

test('strict incoming JSON, proofs, and revision types fail without publication writes', async () => {
  const f = await fixture(), ticket = await f.reserve(), good = f.packet(ticket), before = clone(f.storage.map);
  const bad = ['{}{}', '{"a":1,"a":2}', '{"x":1e999}', '{"x":"\\ud800"}', '[1]', 'null', '\ufeff{}', '{"x":true,}', '{"a":'+ '['.repeat(129)+'0'+']'.repeat(129)+'}'];
  for (const raw of bad) assert.equal((await f.call('PUT', raw)).status, 400, raw.slice(0, 30));
  assert.equal((await f.publish(ticket, good, { 'X-JH-Body-SHA256': '0'.repeat(64) })).status, 400);
  assert.equal((await f.publish(ticket, good, { 'X-JH-Publication-Token': 'bad' })).status, 400);
  for (const val of [true, 0, -1, 1.5, '1', Number.MAX_SAFE_INTEGER + 1]) {
    const d = JSON.parse(good); d.publication.revision = val;
    assert.equal((await f.publish(ticket, JSON.stringify(d))).status, 400);
  }
  assert.deepEqual(f.storage.map, before); assert.equal(f.state.puts, 0);
});

test('unissued or pruned reservation is rejected and cannot manufacture future rank', async () => {
  const f = await fixture(), ticket = await f.reserve();
  assert.equal((await f.publish({ ...ticket, revision: 999 })).status, 409);
  for (let i = 0; i < 128; i++) await f.reserve();
  assert.equal((await f.publish(ticket)).status, 409); assert.ok(!f.storage.map.has('risk:current'));
});

test('invalid/exhausted counters and minimum revisions fail explicitly', async () => {
  for (const bad of [true, -1, 0.5, '2', Number.MAX_SAFE_INTEGER + 1]) {
    const f = await fixture(); f.storage.map.set('risk:counter', bad);
    assert.equal((await f.call('POST', '{}', {}, '/private-artifact?kind=portfolio-risk&action=reserve')).status, 503);
  }
  const f = await fixture(); f.storage.map.set('risk:counter', Number.MAX_SAFE_INTEGER);
  assert.equal((await f.call('POST', '{}', {}, '/private-artifact?kind=portfolio-risk&action=reserve')).status, 503);
  for (const bad of [null, true, -1, 0.5, '2', Number.MAX_SAFE_INTEGER]) {
    assert.equal((await f.call('POST', JSON.stringify({ minimum_revision: bad }), {}, '/private-artifact?kind=portfolio-risk&action=reserve')).status, 400);
  }
});

test('publication identity must match complete snapshot binding, including value types', async () => {
  const f = await fixture(), ticket = await f.reserve(), good = JSON.parse(f.packet(ticket));
  for (const edit of [d => { d.publication.source_value_sha256 = ['a'.repeat(64)]; }, d => { d.snapshot_binding.value_sha256 = 'b'.repeat(64); },
    d => { delete d.snapshot_binding; }, d => { d.snapshot_binding.encoding = 'unknown'; }]) {
    const d = clone(good); edit(d); assert.equal((await f.publish(ticket, JSON.stringify(d))).status, 400);
  }
  assert.ok(!f.storage.map.has('risk:current'));
});

test('external legacy await is serialized with later publication and reservations', { timeout: 5000 }, async () => {
  const f = await fixture(), first = await f.reserve(), second = await f.reserve();
  let release, entered, enqueued; const legacyEntered = new Promise(resolve => { entered = resolve; });
  const bothQueued = new Promise(resolve => { enqueued = resolve; }), methods = [];
  f.state.barrier = new Promise(resolve => { release = resolve; }); f.state.onLegacyRead = entered;
  // Publish hashes the request asynchronously before reaching the coordinator.
  // Establish actual queue order; a timer cannot prove which hash completed first.
  const old = f.publish(first); await legacyEntered;
  f.state.onEnqueued = request => { methods.push(request.method); if (methods.length === 2) enqueued(); };
  const newer = f.publish(second), reserved = f.reserve(); await bothQueued;
  assert.deepEqual(methods.sort(), ['POST', 'PUT']); assert.ok(!f.storage.map.has('risk:current'));
  assert.equal(f.storage.map.get('risk:counter'), 2); assert.equal(f.state.reads, 1);
  f.state.barrier = null; release(); assert.equal((await old).status, 200); assert.equal((await newer).status, 200);
  assert.equal((await reserved).revision, 3);
  assert.equal(await (await f.call()).text(), f.packet(second));
});

test('later preparation can enter first and legitimately supersede an older reservation', async () => {
  const f = await fixture(), first = await f.reserve(), second = await f.reserve();
  let release; const preparation = new Promise(resolve => { release = resolve; });
  const old = (async () => { await preparation; return f.publish(first); })();
  assert.equal((await f.publish(second)).status, 200); release();
  assert.equal((await old).status, 409); assert.equal(await (await f.call()).text(), f.packet(second));
});

test('complete request framing rejects length/encoding/oversize and cancels a stalled read', async () => {
  const { riskBody } = await api;
  for (const h of [{ 'Content-Length': '3' }, { 'Content-Length': '1,2' }, { 'Content-Length': '-1' }, { 'Content-Encoding': 'gzip' }]) {
    await assert.rejects(riskBody(new Request('https://synthetic.invalid', { method: 'PUT', body: '{}', headers: h })));
  }
  let cancelled = false;
  const stream = new ReadableStream({ start(controller) { controller.enqueue(encoder.encode('{')); }, cancel() { cancelled = true; } });
  await assert.rejects(riskBody(new Request('https://synthetic.invalid', { method: 'PUT', body: stream, duplex: 'half' }), 100, 10), /deadline/);
  assert.equal(cancelled, true);
  const f = await fixture(), before = clone(f.storage.map);
  assert.equal((await f.call('PUT', new Uint8Array(20000001))).status, 413); assert.deepEqual(f.storage.map, before);
});

test('missing coordinator refuses private reads without falling through to KV', async () => {
  const f = await fixture(); delete f.env.WORKSPACE_COORDINATOR;
  assert.equal((await f.call()).status, 503); assert.equal(f.state.reads, 0);
});

test('whole original synthetic evidence fixture remains available and unmodified', async () => {
  const raw = fs.readFileSync(path.join(root, 'tests/fixtures/pre-portfolio-publication/complete-synthetic.json'));
  assert.equal(raw.length, 162346);
  assert.equal(await (await api).riskDigest(raw), '26f909241fb6d5ee856c9791fadc881d3411c3391d52731a7f575b3f517c8fb9');
});
