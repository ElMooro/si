// chart.html's open tabs (SPY, QQQ, IWM ...) re-read their last price and day change on their own.
// Khalid 2026-11-20: "there are a lot of tabs that open automatically but they don't update automatically they should
// be updating automatically". Before: one read per tab when the page opened (the boot line), never again.
// The block is run here as the page runs it, against stand-ins for the engine's own helpers and the tab strip.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '..');
const engine = fs.readFileSync(path.join(root, 'jh-chart-engine.js'), 'utf8');
const BLOCK = fs.readFileSync(path.join(__dirname, 'fixtures/chart-macro-line/engine-tab-quotes.block.txt'), 'utf8');

function world(opts) {
  opts = opts || {};
  const spans = {}, calls = { fetch: [], lastPx: [], renderTabs: 0, intervals: [], listeners: {} };
  const tabEl = s => ({ getAttribute: () => s, querySelector: () => spans[s] || null });
  const ctx = {
    TABS: opts.tabs || ['SPY', 'QQQ', 'AAPL'], quotes: opts.quotes || {}, liveOn: true, replay: { on: false }, PROXY: 'https://proxy.test',
    fmt: p => Number(p).toFixed(2), bare: s => (String(s).indexOf(':') >= 0 ? String(s).split(':').pop() : String(s)),
    observationId: s => (/^FRED:/.test(String(s)) ? String(s) : null),
    lastPx: s => { calls.lastPx.push(s); return Promise.resolve((opts.daily || {})[s] || null); },
    renderTabs: () => { calls.renderTabs++; ctx.TABS.forEach(s => { if (ctx.quotes[s] && !spans[s]) spans[s] = { textContent: '', className: '' }; }); },
    fetch: url => { calls.fetch.push(url); const body = typeof opts.snapshot === 'function' ? opts.snapshot(url) : opts.snapshot; return body === 'fail' ? Promise.reject(new Error('net')) : Promise.resolve({ ok: !!body, json: () => Promise.resolve(body) }); },
    document: { hidden: false, getElementById: id => (id === 'tabs' ? { querySelectorAll: () => ctx.TABS.map(tabEl) } : null), addEventListener: (ev, fn) => { calls.listeners[ev] = fn; } },
    setInterval: (fn, ms) => { calls.intervals.push([fn, ms]); return 1; }, encodeURIComponent, Promise, Date, Object, String, Number, Math,
  };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(BLOCK, ctx);
  (opts.painted || []).forEach(s => { spans[s] = { textContent: 'old', className: 'up' }; });
  return { ctx, spans, calls };
}
const snap = rows => ({ tickers: Object.fromEntries(Object.entries(rows).map(([t, r]) => [t, { price: r[0], prevClose: r[1], dayClose: r[2] == null ? r[0] : r[2], updatedTs: 1 }])), ts: 1 });

test('the block is in the engine once, right after the live-tick timer, and the first read at page open is still there', () => {
  assert.equal(engine.split(BLOCK).length, 2);
  assert.ok(engine.indexOf('  liveT=setInterval(tickLive, 15000);\n' + BLOCK) > 0);
  assert.match(engine, /TABS\.forEach\(function\(s\)\{ lastPx\(s\)\.then\(function\(px\)\{ if\(px\)\{ quotes\[s\]=px; renderTabs\(\); \} \}\); \}\);/);
  assert.ok(BLOCK.split('').every(c => c.charCodeAt(0) < 128), 'ASCII');
});

test('a timer drives it: a check every 5 seconds, a full pass every 30, and again when the page comes back on screen', async () => {
  const w = world({ snapshot: snap({ SPY: [700, 690], QQQ: [600, 610], AAPL: [300, 300] }), painted: ['SPY', 'QQQ', 'AAPL'] });
  assert.deepEqual(w.calls.intervals.map(x => x[1]), [5000]);
  assert.equal(typeof w.calls.listeners.visibilitychange, 'function');
  w.calls.intervals[0][0]();                                    // the first tick: nothing was ever read -> a full pass
  await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
  assert.equal(w.calls.fetch.length, 1);
  assert.equal(w.calls.fetch[0], 'https://proxy.test/quotes?tickers=' + encodeURIComponent('SPY,QQQ,AAPL'));
  w.calls.intervals[0][0]();                                    // 5 seconds later: nothing missing, 30 seconds not up
  await new Promise(r => setImmediate(r));
  assert.equal(w.calls.fetch.length, 1);
  assert.equal(w.ctx.jhTabQuotes().passes, 1); assert.equal(w.ctx.jhTabQuotes().every_ms, 30000);
});

test('every tab takes the snapshot\'s price and its change against the prior close, patched in place', async () => {
  const w = world({ snapshot: snap({ SPY: [715.87, 730.2], QQQ: [645.04, 640.0], AAPL: [349.21, 349.21] }), painted: ['SPY', 'QQQ', 'AAPL'],
    quotes: { SPY: { last: 730.2, chg: 0.01, chgv: 7, spark: [1, 2, 3] } } });
  await w.ctx.refreshTabQuotes(false);
  assert.equal(w.spans.SPY.textContent, '715.87 -1.96%'); assert.equal(w.spans.SPY.className, 'dn');
  assert.equal(w.spans.QQQ.textContent, '645.04 +0.79%'); assert.equal(w.spans.QQQ.className, 'up');
  assert.equal(w.spans.AAPL.textContent, '349.21 +0.00%'); assert.equal(w.spans.AAPL.className, 'up');
  assert.deepEqual(Array.from(w.ctx.quotes.SPY.spark), [1, 2, 3], 'the sparkline read at page open is kept');
  assert.ok(Math.abs(w.ctx.quotes.SPY.chgv - (715.87 - 730.2)) < 1e-9);
  assert.equal(w.calls.lastPx.length, 0, 'no daily-bar read for a tab the snapshot answered');
  assert.equal(w.calls.renderTabs, 0, 'no rebuild of the strip: a click in progress is not torn down');
  assert.deepEqual(JSON.parse(JSON.stringify(w.ctx.jhTabQuotes().tabs.map(t => [t.s, t.src]))), [['SPY', 'snapshot'], ['QQQ', 'snapshot'], ['AAPL', 'snapshot']]);
  // the next pass with a new print moves the tab again
  const w2 = world({ snapshot: url => snap({ SPY: [w2n.px, 730.2] }), tabs: ['SPY'], painted: ['SPY'] }), w2n = { px: 716.0 };
  await w2.ctx.refreshTabQuotes(false); assert.equal(w2.spans.SPY.textContent, '716.00 -1.94%');
  w2n.px = 720.5; await w2.ctx.refreshTabQuotes(false); assert.equal(w2.spans.SPY.textContent, '720.50 -1.33%');
  assert.equal(w2.ctx.jhTabQuotes().passes, 2); assert.equal(w2.ctx.jhTabQuotes().changed, 2);
});

test('what the snapshot does not carry is read from its own daily bars, every pass', async () => {
  const daily = { BTCUSDT: { last: 91000, chg: 0.02, chgv: 1800, spark: [] }, 'I:SPX': { last: 6800, chg: -0.01, chgv: -68, spark: [] }, VIX: { last: 22, chg: 0.1, chgv: 2, spark: [] } };
  const w = world({ tabs: ['SPY', 'BTCUSDT', 'I:SPX', 'VIX', 'FRED:DGS10'], snapshot: snap({ SPY: [700, 690] }), daily, painted: ['SPY', 'BTCUSDT', 'I:SPX', 'VIX'] });
  await w.ctx.refreshTabQuotes(false);
  assert.equal(w.calls.fetch[0], 'https://proxy.test/quotes?tickers=' + encodeURIComponent('SPY,VIX'), 'only what looks like a US ticker is asked of the snapshot');
  assert.deepEqual(w.calls.lastPx.slice().sort(), ['BTCUSDT', 'I:SPX', 'VIX'], 'crypto and an index go to daily bars; VIX too once the snapshot has no row for it; a macro series is left alone');
  assert.equal(w.spans.BTCUSDT.textContent, '91000.00 +2.00%'); assert.equal(w.spans['I:SPX'].className, 'dn'); assert.equal(w.spans.VIX.textContent, '22.00 +10.00%');
  w.calls.fetch.length = 0; w.calls.lastPx.length = 0;
  await w.ctx.refreshTabQuotes(false);
  assert.equal(w.calls.fetch[0], 'https://proxy.test/quotes?tickers=SPY', 'VIX is not asked of the snapshot again');
  assert.deepEqual(w.calls.lastPx.slice().sort(), ['BTCUSDT', 'I:SPX', 'VIX']);
  assert.equal(w.ctx.jhTabQuotes().tabs.find(t => t.s === 'VIX').src, 'daily bars');
});

test('before the session has a bar the snapshot is not taken: the tab keeps yesterday\'s close, it is not zeroed', async () => {
  const old = { last: 730.2, chg: -0.0031, chgv: -2.27, spark: [] };
  const w = world({ tabs: ['SPY', 'NEWT'], quotes: { SPY: old }, snapshot: snap({ SPY: [730.2, 730.2, 0], NEWT: [12, 12, 0] }), daily: { NEWT: { last: 12.5, chg: 0.04, chgv: 0.5, spark: [] } }, painted: ['SPY'] });
  await w.ctx.refreshTabQuotes(false);
  assert.equal(w.ctx.quotes.SPY, old, 'SPY keeps the quote it had');
  assert.deepEqual(w.calls.lastPx, ['NEWT'], 'a tab with no quote at all falls back to its daily bars');
  assert.equal(w.ctx.quotes.NEWT.last, 12.5);
  assert.ok(w.calls.renderTabs >= 1, 'a tab that had no figures is drawn with them');
  // a failed call changes nothing and breaks nothing
  const w2 = world({ tabs: ['SPY'], quotes: { SPY: old }, snapshot: 'fail', painted: ['SPY'] });
  await w2.ctx.refreshTabQuotes(false);
  assert.equal(w2.ctx.quotes.SPY, old); assert.equal(w2.ctx.jhTabQuotes().passes, 1);
});

test('a tab opened later gets its figures on the next 5-second check, and one that cannot be priced is retried once a minute, not every tick', async () => {
  const w = world({ tabs: ['SPY'], snapshot: url => snap(/NVDA/.test(url) ? { NVDA: [150, 145] } : { SPY: [700, 690] }), painted: ['SPY'] });
  await w.ctx.refreshTabQuotes(false);
  w.ctx.TABS.push('NVDA', 'ZZZZQ');
  w.calls.fetch.length = 0;
  w.calls.intervals[0][0]();
  await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
  assert.equal(w.calls.fetch.length, 1);
  assert.equal(w.calls.fetch[0], 'https://proxy.test/quotes?tickers=' + encodeURIComponent('NVDA,ZZZZQ'), 'only the tabs without figures are asked for');
  assert.equal(w.ctx.quotes.NVDA.last, 150); assert.equal(w.ctx.quotes.ZZZZQ, undefined);
  w.calls.fetch.length = 0; w.calls.lastPx.length = 0;
  w.calls.intervals[0][0]();
  await new Promise(r => setImmediate(r));
  assert.equal(w.calls.fetch.length + w.calls.lastPx.length, 0, 'the unpriceable tab is not hammered');
});

test('nothing is read while auto refresh is off, in replay, or with the page off screen', async () => {
  const w = world({ snapshot: snap({ SPY: [700, 690] }), tabs: ['SPY'], painted: ['SPY'] });
  vm.runInContext('liveOn=false', w.ctx); await w.ctx.refreshTabQuotes(false);
  vm.runInContext('liveOn=true; replay.on=true', w.ctx); await w.ctx.refreshTabQuotes(false);
  vm.runInContext('replay.on=false; document.hidden=true', w.ctx); await w.ctx.refreshTabQuotes(false);
  assert.equal(w.calls.fetch.length, 0); assert.equal(w.ctx.jhTabQuotes().passes, 0);
  vm.runInContext('document.hidden=false', w.ctx);
  w.calls.listeners.visibilitychange();
  await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
  assert.equal(w.calls.fetch.length, 1, 'back on screen: read at once');
});
