// ops 6551 (Khalid, 2026-10-20, after a $2,800 October bill): fresh publications keep a REVALIDATED edge copy.
// The copy is reused only when S3 answers a conditional GET with 304, so a visitor always receives what S3 serves
// right now (no stale window) while an unchanged multi-MB file stops costing a full S3 read ($0.09/GB) per request.
// Runs the REAL worker module in Node with a Map-backed Cache API and a scripted S3. No network.
// The feature needs no Worker setting: it runs for requests that came through the Cloudflare edge (`request.cf` is set
// there), so the deployed Worker configuration stays exactly the reviewed baseline. `edge()` marks a test request so.
const test = require('node:test'), assert = require('node:assert/strict'), path = require('node:path');
const { pathToFileURL } = require('node:url');
const source = pathToFileURL(path.join(__dirname, '../cloudflare/workers/justhodl-data-proxy/src/index.js')).href;

const ENV = {};
// A request as the Cloudflare edge delivers it: `request.cf` is present (Node's Request has none of its own).
function edge(url, init) {
  const request = new Request(url, init || {});
  Object.defineProperty(request, 'cf', { value: { colo: 'TEST', country: 'US' } });
  return request;
}
const BIG = n => JSON.stringify({ generation: n, rows: 'x'.repeat(90 * 1024) });       // > 64 KB
const SMALL = n => JSON.stringify({ generation: n });

// A scripted S3: `object` is the current object {body, etag}; honours If-None-Match like S3 does.
function setup(object) {
  const state = { calls: [], stored: new Map(), puts: [], waits: [], object, status: 200 };
  globalThis.caches = { default: {
    async match(req) { return state.stored.get(req.url)?.clone(); },
    async put(req, response) { state.puts.push(req.url); state.stored.set(req.url, response.clone()); },
  } };
  globalThis.fetch = async (input, options) => {
    const headers = new Headers((options && options.headers) || {});
    state.calls.push({ url: String(input), options, inm: headers.get('If-None-Match'), ims: headers.get('If-Modified-Since') });
    if (state.status !== 200) return new Response('', { status: state.status });
    if (headers.get('If-None-Match') && headers.get('If-None-Match') === state.object.etag) return new Response(null, { status: 304 });
    const h = { 'Content-Type': 'application/json', ETag: state.object.etag };
    if (state.object.lastModified) h['Last-Modified'] = state.object.lastModified;
    return new Response(state.object.body, { status: 200, headers: h });
  };
  state.context = { waitUntil(p) { state.waits.push(p); } };
  return state;
}
async function settle(state) { await Promise.all(state.waits.splice(0)); }

test('first request pulls the object, stores a copy, and the visitor still gets no-store', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: BIG(1), etag: '"e1"' });
  const r = await worker.fetch(edge('https://justhodl.ai/data/share-flows.json'), ENV, state.context);
  await settle(state);
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('Cache-Control'), 'no-store');
  assert.equal(r.headers.get('X-Edge-Cache'), 'MISS');
  assert.equal(JSON.parse(await r.text()).generation, 1);
  assert.equal(state.calls.length, 1);
  assert.equal(state.calls[0].options.cache, 'no-store');
  assert.equal(state.calls[0].options.cf, undefined, 'a cf TTL cannot be combined with no-store');
  assert.equal(state.calls[0].inm, null, 'nothing to validate on a cold cache');
  assert.equal(state.puts.length, 1);
  assert.ok(state.puts[0].includes('-rev-v1__/data/share-flows.json'), 'copies live in their own namespace');
});

test('unchanged object: S3 answers 304 and the stored bytes are served, with no body from S3', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: BIG(1), etag: '"e1"' });
  await worker.fetch(edge('https://justhodl.ai/data/share-flows.json'), ENV, state.context); await settle(state);
  const r = await worker.fetch(edge('https://justhodl.ai/data/share-flows.json'), ENV, state.context); await settle(state);
  assert.equal(state.calls.length, 2, 'every request still asks S3 (that is what keeps it fresh)');
  assert.equal(state.calls[1].inm, '"e1"');
  assert.equal(state.calls[1].options.cache, 'no-store');
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('Cache-Control'), 'no-store');
  assert.equal(r.headers.get('X-Edge-Cache'), 'REVALIDATED');
  assert.equal(r.headers.get('ETag'), '"e1"');
  assert.equal(JSON.parse(await r.text()).generation, 1);
  assert.equal(state.puts.length, 1, 'a 304 stores nothing new');
});

test('changed object: the visitor gets the NEW body immediately, never the stored one', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: BIG(1), etag: '"e1"' });
  await worker.fetch(edge('https://justhodl.ai/data/best-setups.json'), ENV, state.context); await settle(state);
  state.object = { body: BIG(2), etag: '"e2"' };                               // an engine republished
  let r = await worker.fetch(edge('https://justhodl.ai/data/best-setups.json'), ENV, state.context); await settle(state);
  assert.equal(JSON.parse(await r.text()).generation, 2);
  assert.equal(r.headers.get('X-Edge-Cache'), 'MISS');
  assert.equal(state.calls[1].inm, '"e1"');
  assert.equal(state.puts.length, 2, 'the new generation replaces the stored copy');
  r = await worker.fetch(edge('https://justhodl.ai/data/best-setups.json'), ENV, state.context); await settle(state);
  assert.equal(state.calls[2].inm, '"e2"');
  assert.equal(r.headers.get('X-Edge-Cache'), 'REVALIDATED');
  assert.equal(JSON.parse(await r.text()).generation, 2);
});

test('Last-Modified is the validator when S3 sends no ETag', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: BIG(1), etag: null, lastModified: 'Tue, 20 Oct 2026 06:00:00 GMT' });
  globalThis.fetch = async (input, options) => {
    const headers = new Headers((options && options.headers) || {});
    state.calls.push({ ims: headers.get('If-Modified-Since'), inm: headers.get('If-None-Match') });
    if (headers.get('If-Modified-Since') === 'Tue, 20 Oct 2026 06:00:00 GMT') return new Response(null, { status: 304 });
    return new Response(BIG(1), { status: 200, headers: { 'Last-Modified': 'Tue, 20 Oct 2026 06:00:00 GMT' } });
  };
  await worker.fetch(edge('https://justhodl.ai/data/dark-pool.json'), ENV, state.context); await settle(state);
  const r = await worker.fetch(edge('https://justhodl.ai/data/dark-pool.json'), ENV, state.context); await settle(state);
  assert.equal(state.calls[1].ims, 'Tue, 20 Oct 2026 06:00:00 GMT');
  assert.equal(r.headers.get('X-Edge-Cache'), 'REVALIDATED');
});

test('an object S3 no longer serves is an error, never the stored copy', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: BIG(1), etag: '"e1"' });
  await worker.fetch(edge('https://justhodl.ai/data/forensic-screen.json'), ENV, state.context); await settle(state);
  state.status = 403;
  const r = await worker.fetch(edge('https://justhodl.ai/data/forensic-screen.json'), ENV, state.context);
  assert.equal(r.status, 403);
  assert.equal(r.headers.get('Cache-Control'), 'no-store');
});

test('small bodies are not stored (the 200 is cheaper than the bookkeeping)', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: SMALL(1), etag: '"s1"' });
  await worker.fetch(edge('https://justhodl.ai/data/signal-board.json'), ENV, state.context); await settle(state);
  const r = await worker.fetch(edge('https://justhodl.ai/data/signal-board.json'), ENV, state.context); await settle(state);
  assert.equal(state.puts.length, 0);
  assert.equal(state.calls[1].inm, null);
  assert.equal(JSON.parse(await r.text()).generation, 1);
});

test('exact reads, the research brief, retired aliases and Range requests never use an edge copy', async () => {
  const worker = (await import(source)).default;
  for (const [p, init] of [
    ['/data/share-flows.json?exact=1'], ['/data/intelligence-report.json'], ['/intelligence-report.json'],
    ['/data/ciss.json'], ['/data/defcon.json'], ['/data/share-flows.json', { headers: { Range: 'bytes=0-99' } }],
  ]) {
    const state = setup({ body: BIG(1), etag: '"e1"' });
    await worker.fetch(edge('https://justhodl.ai' + p, init || {}), ENV, state.context); await settle(state);
    await worker.fetch(edge('https://justhodl.ai' + p, init || {}), ENV, state.context); await settle(state);
    assert.equal(state.puts.length, 0, p + ' must not be stored');
    assert.ok(state.calls.every(c => c.inm === null), p + ' must never send a validator');
  }
});

test('files outside the mutable-publication list keep their ordinary TTL cache', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: BIG(1), etag: '"e1"' });
  await worker.fetch(edge('https://justhodl.ai/data/some-ordinary-feed.json'), ENV, state.context); await settle(state);
  assert.ok(state.puts.every(u => !u.includes('-rev-v1__')), 'only mutable publications use the revalidated namespace');
  assert.equal(state.puts.length, 1);
});

test('a request without request.cf (unit tests, local previews) keeps the plain no-cache path', async () => {
  const worker = (await import(source)).default;
  const state = setup({ body: BIG(1), etag: '"e1"' });
  await worker.fetch(new Request('https://justhodl.ai/data/share-flows.json'), ENV, state.context); await settle(state);
  const r = await worker.fetch(new Request('https://justhodl.ai/data/share-flows.json'), ENV, state.context); await settle(state);
  assert.equal(state.puts.length, 0);
  assert.ok(state.calls.every(c => c.inm === null));
  assert.equal(r.headers.get('Cache-Control'), 'no-store');
  assert.equal(r.headers.get('X-Edge-Cache'), 'MISS');
});

test('the Worker configuration declares no setting for this feature (release evidence needs it unchanged)', () => {
  const fs = require('node:fs');
  const dir = path.join(__dirname, '../cloudflare/workers/justhodl-data-proxy');
  assert.ok(!/JH_EDGE_REVALIDATE/.test(fs.readFileSync(path.join(dir, 'wrangler.toml'), 'utf8')));
  assert.ok(!/env\.JH_EDGE_REVALIDATE/.test(fs.readFileSync(path.join(dir, 'src/index.js'), 'utf8')));
});
