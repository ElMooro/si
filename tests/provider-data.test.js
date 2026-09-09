const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const helper = require('../jh-provider-data.js');
const manifest = {provider: 'ecb', n_pages: 20, extra: {zero: 0, missing: null}};
function buildBlocks(chunks) {
  let offset = 0;
  const bodies = chunks.map(rows => Buffer.from(rows.map(r => JSON.stringify(r)).join('\n') + '\n'));
  const blocks = chunks.map((rows, i) => { const b = {k: rows[0].id, o: offset, c: bodies[i].length, n: rows.length}; offset += b.c; return b; });
  return {doc: {provider: 'ecb', flow: 'FLOW', entry_schema: 2, n: chunks.reduce((n, a) => n + a.length, 0), bytes: offset, blocks}, bodies};
}
test('path identity and page guards deny traversal and unsupported families before any request', async () => {
  let reads = 0;
  const request = async () => { reads++; return Response.json({}); };
  for (const value of [null, '../brain', 'ecb/../brain', 'ecb%2fsecret', 'ECb', 'x?y', '']) assert.throws(() => helper.createClient(value, request));
  const client = helper.createClient('ecb', request);
  for (const name of ['../brain', 'FLOW/path', 'x%2fsecret', 'x?y']) await assert.rejects(client.blocks(name));
  await assert.rejects(client.seriesPage(20, manifest));
  await assert.rejects(client.catalogPage(-1, {n_pages: 1}));
  await assert.rejects(helper.createClient('fred', request).seriesManifest());
  assert.equal(reads, 0);
});
test('catalogue and original page readers preserve full payloads and require matching declared pages', async () => {
  const calls = [];
  const doc = {page: 19, count: 501, rows: Array.from({length: 501}, (_, i) => ({id: String(i), nested: {zero: 0, missing: null}})), extra: ['all fields']};
  const client = helper.createClient('ecb', async key => { calls.push(key); return Response.json(doc); });
  const record = await client.seriesPage(19, manifest);
  assert.equal(calls[0], '/data/providers/ecb/series/page-0019.json');
  assert.deepEqual(record.document, doc);
  assert.equal(record.producer, 'justhodl-series-extractor');
  await assert.rejects(client.seriesPage(18, manifest), /identity/);
  const cat = helper.createClient('ecb', async () => Response.json({slug: 'eurostat', n_pages: 0, keys: []}));
  await assert.rejects(cat.catalog(), /identity/);
  const missing = helper.createClient('ecb', async () => new Response('', {status: 404}));
  await assert.rejects(missing.catalogPage(0, {n_pages: 1}), /HTTP 404/);
});
test('all declared byte blocks are reachable, reconcile byte range and expose every source row', async () => {
  const first = [{id: 'A', p: 0, v: 0}];
  const last = Array.from({length: 401}, (_, i) => ({id: 'B' + i, p: 19, v: 0, g: null, extra: {full: 'é'}}));
  const fixture = buildBlocks([first, last]), calls = [];
  const client = helper.createClient('ecb', async (key, options) => {
    calls.push({key, options});
    if (key.endsWith('.blocks.json')) return Response.json(fixture.doc);
    const b = fixture.doc.blocks[1];
    return new Response(fixture.bodies[1], {status: 206, headers: {'Content-Range': `bytes ${b.o}-${b.o + b.c - 1}/${fixture.doc.bytes}`}});
  });
  const map = await client.blocks('FLOW');
  const result = await client.block(1, map.document, manifest);
  assert.deepEqual(result.document.rows, last);
  assert.deepEqual(result.document.source_pages, [19]);
  const b = fixture.doc.blocks[1];
  assert.equal(calls[1].options.headers.Range, `bytes=${b.o}-${b.o + b.c - 1}`);
  assert.match(result.document.completeness, /compact index/);
});
test('full-file range fallback, truncated bytes, forged count and source pointers fail closed', async () => {
  const fixture = buildBlocks([[{id: 'A', p: 0}]]), b = fixture.doc.blocks[0];
  const header = `bytes 0-${b.c - 1}/${fixture.doc.bytes}`;
  let bodyRead = false;
  const noRange = helper.createClient('ecb', async () => ({status: 200, arrayBuffer: () => {bodyRead = true; throw Error('must not read');}}));
  await assert.rejects(noRange.block(0, fixture.doc, manifest), /no full-file body/);
  assert.equal(bodyRead, false);
  for (const response of [
    () => new Response(fixture.bodies[0], {status: 206, headers: {'Content-Range': 'bytes 1-2/3'}}),
    () => new Response(fixture.bodies[0].subarray(1), {status: 206, headers: {'Content-Range': header}}),
  ]) await assert.rejects(helper.createClient('ecb', response).block(0, fixture.doc, manifest), /byte/);
  const bad = {...fixture.doc, n: 2};
  await assert.rejects(helper.createClient('ecb', async () => Response.json(bad)).blocks('FLOW'), /reconcile/);
  const pointer = buildBlocks([[{id: 'A', p: 100}]]), bp = pointer.doc.blocks[0];
  await assert.rejects(helper.createClient('ecb', async () => new Response(pointer.bodies[0], {status: 206, headers: {'Content-Range': `bytes 0-${bp.c - 1}/${bp.c}`}})).block(0, pointer.doc, manifest), /pointer/);
});
test('selection guard suppresses late responses and late failures', async () => {
  const run = helper.newestOnly(), pending = {}, rendered = [], errors = [];
  const old = run(() => new Promise(resolve => {pending.old = resolve;}), x => rendered.push(x), x => errors.push(x));
  const current = run(async () => 'new', x => rendered.push(x), x => errors.push(x));
  await current; pending.old('old'); await old;
  assert.deepEqual(rendered, ['new']); assert.deepEqual(errors, []);
  const failed = run(() => new Promise((resolve, reject) => {pending.fail = reject;}), x => rendered.push(x), x => errors.push(x));
  await run(async () => 'latest', x => rendered.push(x), x => errors.push(x));
  pending.fail(new Error('obsolete failure')); await failed;
  assert.deepEqual(rendered, ['new', 'latest']); assert.deepEqual(errors, []);
});
test('actual page controller traverses final catalogue, >300 search hits and original pages with complete inspection', async () => {
  class Element {
    constructor() { this.children = []; this.value = ''; this.textContent = ''; this.disabled = false; this.hidden = false; }
    replaceChildren(...nodes) { this.children = nodes; }
    append(...nodes) { this.children.push(...nodes); }
    addEventListener(name, fn) { this[name] = fn; }
    click() { return this.onclick(); }
    scrollIntoView() {}
  }
  const ids = [...fs.readFileSync(require('node:path').join(__dirname, '../provider.html'), 'utf8').matchAll(/id="([^"]+)"/g)].map(x => x[1]);
  const elements = Object.fromEntries(ids.map(id => [id, new Element()]));
  const captures = [], requests = [], flows = Object.fromEntries(Array.from({length: 301}, (_, i) => ['FLOW' + String(i).padStart(3, '0'), {lo: 0, hi: 19, series: 0}]));
  const old = {document: global.document, location: global.location, fetch: global.fetch, inspector: global.JHDataInspector};
  global.document = {getElementById: id => elements[id], createElement: () => new Element()};
  global.location = {search: '?p=ecb'};
  global.JHDataInspector = {inspect: (node, data, label) => captures.push({node, data, label})};
  global.fetch = async key => {
    requests.push(key);
    if (key === '/data/providers/ecb.json') return Response.json({slug: 'ecb', name: 'ECB', n_keys: 101, n_pages: 1, keys: [{key: '<img src=x onerror=alert(1)>', bytes: 0, age_h: null}], extra: {zero: 0, missing: null}});
    if (key.endsWith('/page-000.json')) return Response.json({page: 0, keys: [{key: 'final', bytes: 0}], extra: {all: true}});
    if (key.endsWith('/series-manifest.json')) return Response.json(manifest);
    if (key.endsWith('/flows.json.gz')) return Response.json({flows, unknown_metadata: [0, null]});
    if (key.endsWith('/series/page-0019.json')) return Response.json({page: 19, count: 501, rows: Array.from({length: 501}, (_, i) => ({id: String(i), zero: 0, missing: null}))});
    throw Error('Unexpected request ' + key);
  };
  try {
    helper.start(); await new Promise(resolve => setImmediate(resolve));
    assert.equal(elements.files.children[0].children[0].textContent, '<img src=x onerror=alert(1)>');
    elements['catalog-page'].value = '0'; await elements['catalog-load'].click();
    assert.equal(elements.files.children[0].children[0].textContent, 'final');
    elements['flow-search'].value = 'FLOW'; elements['flow-search'].input();
    for (let i = 0; i < 6; i++) elements['flow-next'].click();
    assert.equal(elements.flows.children[0].children[0].textContent, 'FLOW300');
    assert.equal(elements['flow-next'].disabled, true);
    elements['series-page'].value = '19'; await elements['series-load'].click();
    const last = captures.find(c => c.data.key.endsWith('/series/page-0019.json'));
    assert.equal(last.data.document.rows.length, 501); assert.equal(last.data.document.rows[500].missing, null);
    assert.match(captures.find(c => c.data.key.endsWith('/flows.json.gz')).label, /Ops-generated/);
    assert.equal(requests.some(key => key.includes('<img')), false);
  } finally {
    global.document = old.document; global.location = old.location; global.fetch = old.fetch; global.JHDataInspector = old.inspector;
  }
});
