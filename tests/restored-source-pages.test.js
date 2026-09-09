const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const root = path.join(__dirname, '..');

function loadPage(page, request) {
  const text = fs.readFileSync(path.join(root, page), 'utf8');
  const elements = new Map();
  const get = id => {
    if (!elements.has(id)) elements.set(id, {innerHTML: '', textContent: '', value: id === 'query' ? 'Treasury' : '', hidden: false,
      addEventListener() {}, querySelectorAll() {return [];}, classList: {add() {}, remove() {}}});
    return elements.get(id);
  };
  const calls = [];
  const context = vm.createContext({console, Date, URL, encodeURIComponent, setInterval() {throw new Error('Unexpected refresh loop');},
    document: {getElementById: get, querySelector: get, querySelectorAll() {return [];}},
    fetch: async (url, options) => {calls.push({url, options}); return request(url, options);},
  });
  context.window = context;
  for (const match of text.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) vm.runInContext(match[1], context);
  return {context, elements, get, calls, text};
}
const tick = () => new Promise(resolve => setImmediate(resolve));
const response = data => ({ok: true, json: async () => data});

test('Stocks renders Investment engine fields and preserves complete typed response', async () => {
  const payload = {engine: 'justhodl-invest', schema: 'invest/0.1', generated_at: '2026-09-09T12:00:00Z', method_notes: 'Measured industry gates',
    industry_gates: {technology: {pass: false}}, leading_indicators: {rates: {status: 'TURNING'}},
    stock_picks: [{ticker: 'NVDA', industry: 'Semiconductors', status: 'OK', composite_score: 0, industry_median_score: null,
      proxy_etf: 'SOXX', vs_industry_etf: 'UNDERPERFORM_EXPECTED', reweighted: false, thesis: '<img src=x>', raw: {value: 0, available: false, absent: null}},
      {industry: 'Unknown industry', status: 'INSUFFICIENT_DATA', reason: 'no stock universe'}],
    extra_future_field: {nested: ['retain', 42, null, true]}};
  const view = loadPage('stocks/index.html', () => response(payload));
  await tick();
  assert.match(view.calls[0].url, /\/data\/invest\.json/);
  assert.equal(view.get('count').textContent, '2 / 2');
  assert.match(view.get('rows').innerHTML, /0\.0/);
  assert.match(view.get('rows').innerHTML, /INSUFFICIENT_DATA/);
  assert.match(view.get('rows').innerHTML, /&lt;img src=x&gt;/);
  assert.deepEqual(JSON.parse(view.get('full').textContent), payload);
  assert.match(view.get('asof').textContent, /2026-09-09T12:00:00Z/);
  assert.doesNotMatch(view.text, /stock-picks-data\.json|earningsGrowth|dcfValue/);
  view.get('filter').value = 'NVDA';
  vm.runInContext('renderRows()', view.context);
  assert.equal(view.get('count').textContent, '1 / 2');
  assert.deepEqual(JSON.parse(view.get('full').textContent), payload);
});

test('Stocks failed refresh clears old research and rejects unrelated schemas', async () => {
  let valid = true;
  const view = loadPage('stocks/index.html', () => response(valid
    ? {engine: 'justhodl-invest', schema: 'invest/0.1', stock_picks: []} : {earningsGrowth: {totalScanned: 100}}));
  await tick();
  valid = false;
  await vm.runInContext('loadData()', view.context);
  assert.equal(view.get('full').textContent, '');
  assert.match(view.get('status').textContent, /unavailable/);
  assert.match(view.get('rows').innerHTML, /No verified engine response/);
});

test('Search uses current API, keeps server order and every response field, opens complete series', async () => {
  const searchPayload = {q: 'Treasury', built_at: 'index-date', total: 300, warehouse_more: true,
    rows: [{id: 'fred:DGS10', name: 'Treasury 10Y', provider: 'fred', kind: 'series', chartable: true, extra: null},
      {id: 'fred:DGS2', name: 'Treasury 2Y', provider: 'fred', kind: 'series', chartable: true}], facets: [{provider: 'fred', n: 300}]};
  const seriesPayload = {id: 'fred:DGS10', unit: 'percent', obs: [['2026-09-08', 4.1], ['2026-09-09', null]], source: 'fred', extra: false};
  const view = loadPage('archive/exponential-search-dashboard.html', url => response(url.includes('/series?') ? seriesPayload : searchPayload));
  await tick();
  assert.match(view.calls[0].url, /\/symsearch\?q=Treasury&limit=200$/);
  assert.doesNotMatch(view.text, /i70jxru6md|openbb.*api-gateway/i);
  assert.match(view.get('status').textContent, /refine the query/);
  assert.deepEqual(JSON.parse(view.get('search-json').textContent), searchPayload);
  assert.ok(view.get('results').innerHTML.indexOf('Treasury 10Y') < view.get('results').innerHTML.indexOf('Treasury 2Y'));
  await vm.runInContext('loadSeries({id:"fred:DGS10",name:"Treasury 10Y"})', view.context);
  assert.match(view.calls[1].url, /\/series\?id=fred%3ADGS10$/);
  assert.deepEqual(JSON.parse(view.get('series-json').textContent), seriesPayload);
});

test('Archived PRO labels source retirement and preserves legacy fields even if old renderer fails', async () => {
  const payload = {generated: '2026-02-26T00:00:00Z', unrecognized_legacy_field: {raw: ['historical', 0, false, null]}, trade_ideas: null};
  const view = loadPage('archive/pro.html', () => response(payload));
  await tick();
  assert.match(view.text, /Retired source — historical data only/);
  assert.match(view.get('archive-asof').textContent, /2026-02-26/);
  assert.match(view.get('archive-asof').textContent, /producer is unverified/);
  assert.deepEqual(JSON.parse(view.get('archive-json').textContent), payload);
  assert.equal(view.calls.length, 1);
  assert.doesNotMatch(view.text, /setInterval\(load/);
});
