'use strict';
// Money flow (global-flow-desk.html + jh-money-map.js) on the documents the engine's own code built from a cut of the real
// warehouse (flow_map.build on the 135 funds and 75 days of ops 6962; tests/fixtures/money-map-135.json.gz).
// Khalid 2026-11-25: "Enhance that engine and tracks what etfs and sectors is the money flowing out and flowing into and covers as
// many asset classes and etfs as possible. And add a button on charts.html for it call it flow. Check if that button doesnt exist
// first before you add it".
// The page measures nothing. These tests hold what it draws to the documents: every row behind a bar is the engine's own group
// (the funds behind it add up to it), the page's own adding-up finds a changed figure, the research view that was the page is
// still there and one click away, and the chart's Flow button is a new button beside Flows, which is where and what it was.
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const R = path.resolve(__dirname, '..');
const M = require(path.join(R, 'jh-money-map.js'));
const FIX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(R, 'tests/fixtures/money-map-135.json.gz'))));
const DOC = FIX.main, FDOC = FIX.funds, HIST = FIX.history, SHARDS = FIX.shards;
const FUNDS = M.fundObjects(FDOC);
const JS = fs.readFileSync(path.join(R, 'jh-money-map.js'), 'utf8');
const HTML = fs.readFileSync(path.join(R, 'global-flow-desk.html'), 'utf8');
const CHART_ENGINE = fs.readFileSync(path.join(R, 'jh-chart-engine.js'), 'utf8');
const CHART_RAIL = fs.readFileSync(path.join(R, 'jh-chart-tvrail.js'), 'utf8');
const CHART_HTML = fs.readFileSync(path.join(R, 'chart.html'), 'utf8');
const ledger = require('./helpers/chart-macro-line-preservation.cjs');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const clone = v => JSON.parse(JSON.stringify(v));

test('the fixture is the engine\'s own build and the page reads its contract', () => {
  assert.equal(DOC.contract, 'global-flow-map.v1');
  assert.equal(DOC.engine, 'justhodl-global-flow-desk');
  assert.equal(FDOC.contract, 'global-flow-map-funds.v1');
  assert.equal(HIST.contract, 'global-flow-map-history.v1');
  assert.equal(FUNDS.length, 135);
  assert.deepEqual(M.WINDOWS.map(w => w.id), DOC.windows.map(w => w.key));
  assert.equal(M.DOCS.main, '/data/global-flow-map.json');
  assert.equal(M.DOCS.funds, '/' + DOC.documents.funds);
  assert.equal(M.DOCS.history, '/' + DOC.documents.history);
  assert.equal(M.DOCS.shard + '<first letter>.json', '/' + DOC.documents.fund_history);
  // every column the engine publishes for a fund has a name and a way to print it on the page
  assert.deepEqual(Object.keys(M.COLS), FDOC.cols);
  for (const c of M.TABLE_COLS) assert.ok(FDOC.cols.includes(c), c);
  // and every table of the document has a card
  assert.deepEqual(M.KINDS.map(k => k.id).sort(), Object.keys(DOC.groups).sort());
});

test('money is printed the same way everywhere', () => {
  assert.equal(M.usd(15574448107170.56), '$15.57T');
  assert.equal(M.flow(-6060084785.8), '−$6.06B');
  assert.equal(M.flow(12828781140.46), '+$12.8B');
  assert.equal(M.flow(156e9), '+$156B');
  assert.equal(M.flow(8.7e6), '+$8.7M');
  assert.equal(M.flow(-162e6), '−$162M');
  assert.equal(M.flow(12345), '+$12K');
  assert.equal(M.flow(0), '$0');
  assert.equal(M.flow(null), 'n/a');
  assert.equal(M.flow(NaN), 'n/a');
  assert.equal(M.pct(-0.772), '−0.77%');
  assert.equal(M.pct(16.9), '+16.9%');
  assert.equal(M.pct(137611), '+137611%');
  assert.equal(M.pct(undefined), 'n/a');
  assert.equal(M.int(5770), '5,770');
  assert.equal(M.dayName('2026-11-24'), 'Tue 24 Nov 2026');
  assert.equal(M.esc('<b a="1">&\'</b>'), '&lt;b a=&quot;1&quot;&gt;&amp;&#39;&lt;/b&gt;');
});

test('a card\'s rows are the document\'s rows, the most in first', () => {
  for (const w of M.WINDOWS) {
    const rows = M.rowsOf(DOC, 'segments', w.id);
    assert.equal(rows.length, DOC.groups.segments.length);
    for (let i = 1; i < rows.length; i++) assert.ok(rows[i - 1].net >= rows[i].net, w.id);
    for (const r of rows) {
      const g = DOC.groups.segments.find(x => x.id === r.id);
      assert.equal(r.net, g.w[w.id].net);
      assert.equal(r.in, g.w[w.id].in);
      assert.equal(r.out, g.w[w.id].out);
      assert.equal(r.pct, g.w[w.id].pct);
      assert.equal(r.n, g.n);
    }
    const byPct = M.rowsOf(DOC, 'segments', w.id, 'pct').filter(r => r.pct != null);
    for (let i = 1; i < byPct.length; i++) assert.ok(byPct[i - 1].pct >= byPct[i].pct);
  }
});

test('the funds behind a row are the engine\'s own group: they add up to it in every table and every window', () => {
  let checked = 0;
  for (const k of M.KINDS) {
    if (k.through) continue;                              // split by weights: no fund is wholly behind a row
    for (const g of DOC.groups[k.id]) {
      const own = M.members(FUNDS, DOC, k.id, g.id);
      assert.equal(own.length, g.n, k.id + ' ' + g.id + ' fund count');
      for (const w of M.WINDOWS) {
        const sum = own.reduce((a, f) => a + (f[w.id] || 0), 0);
        assert.ok(Math.abs(sum - g.w[w.id].net) <= 5, k.id + ' ' + g.id + ' ' + w.id + ': ' + sum + ' against ' + g.w[w.id].net);
        checked++;
      }
    }
  }
  assert.ok(checked > 400, String(checked));
  // a late fund stands behind nothing
  const late = FUNDS.filter(f => !M.isCurrent(f, DOC));
  assert.ok(late.length > 0);
  for (const k of M.KINDS) for (const g of DOC.groups[k.id]) for (const f of M.members(FUNDS, DOC, k.id, g.id)) assert.ok(M.isCurrent(f, DOC));
});

test('the page adds the funds up again, and says so when a figure does not hold', () => {
  const ok = M.audit(DOC, FUNDS);
  assert.equal(ok.ok, true, JSON.stringify(ok.wrong.slice(0, 3)));
  assert.equal(ok.current, DOC.quality.funds_current);
  assert.equal(ok.rows, (DOC.groups.segments.length + 2) * 6);
  // one group row changed by a dollar amount
  const d1 = clone(DOC);
  d1.groups.segments[0].w.w1.net += 1e6;
  const a1 = M.audit(d1, FUNDS);
  assert.equal(a1.ok, false);
  assert.deepEqual(a1.wrong.map(x => [x[0], x[1]]), [[d1.groups.segments[0].id, 'w1']]);
  // one fund changed: its row and both totals no longer hold
  const f2 = clone(FDOC), i = f2.cols.indexOf('m1'), row = f2.rows.find(r => r[0] === 'VOO');
  row[i] += 5e6;
  const a2 = M.audit(DOC, M.fundObjects(f2));
  assert.deepEqual(a2.wrong.map(x => x[0] + ' ' + x[1]).sort(), ['Equity · United States m1', 'total all m1', 'total plain m1']);
  // a funds document cut short is seen, not passed
  const f3 = clone(FDOC);
  f3.rows = f3.rows.slice(0, 60);
  assert.equal(M.audit(DOC, M.fundObjects(f3)).ok, false);
  // nothing to add up is not a pass
  assert.equal(M.audit(DOC, []).ok, false);
});

test('the headline and the sentence under it are the document\'s totals', () => {
  for (const w of M.WINDOWS) {
    const h = M.headline(DOC, w.id), t = DOC.totals.plain.w[w.id];
    assert.equal(h.net, t.net);
    assert.equal(h.in, t.in);
    assert.equal(h.out, t.out);
    assert.equal(h.n, DOC.totals.plain.n);
    assert.equal(h.lev_net, DOC.totals.leveraged.w[w.id].net);
    assert.notEqual(h.top.id, M.LEV);                      // bets for and against the market are never the headline class
    const s = M.reading(DOC, w.id);
    assert.ok(s.includes(M.usd(Math.abs(t.net))), s);
    assert.ok(s.includes(t.net >= 0 ? ' went into ' : ' came out of '));
    assert.ok(s.includes(M.int(t.n_in)) && s.includes(M.int(t.n_out)));
    assert.ok(s.includes(h.top.id));
    assert.ok(!/undefined|NaN|null/.test(s), s);
  }
  // no opinion is offered: the sentence states amounts and names, nothing about what to do
  assert.ok(!/\b(buy|sell|bullish|bearish|should|signal)\b/i.test(M.reading(DOC, 'w1')));
});

test('bars: one scale on both sides of zero, and zero where the money says', () => {
  const a = M.axis([19.3e9, 8.98e9, -2.98e9, -0.1e9, null]);
  assert.ok(Math.abs(a.zero - 100 * 2.98 / (19.3 + 2.98)) < 1e-9);
  assert.ok(Math.abs(19.3e9 * a.unit - (100 - a.zero)) < 1e-9);          // the largest inflow reaches the right edge
  assert.ok(Math.abs(2.98e9 * a.unit - a.zero) < 1e-9);                  // and the largest outflow the left, on the same scale
  const allIn = M.axis([5, 2, 1]);
  assert.ok(allIn.zero <= 1 && Math.abs(5 * allIn.unit - (100 - allIn.zero)) < 1e-9);
  const allOut = M.axis([-5, -2]);
  assert.ok(allOut.zero >= 99 && Math.abs(5 * allOut.unit - allOut.zero) < 1e-9);
  const lop = M.axis([1000, -1]);                                         // a sliver of outflow still has room to be seen
  assert.equal(lop.zero, 6);
  assert.ok(1000 * lop.unit <= 94 + 1e-9);
  assert.deepEqual(M.axis([null, 0]), { zero: 50, unit: 0 });
});

test('the rotation matrix colours a share of assets, with a neutral middle and both palettes', () => {
  for (const key of Object.keys(M.PALETTES)) {
    const p = M.PALETTES[key];
    const rgb = h => 'rgb(' + [1, 3, 5].map(i => parseInt(h.slice(i, i + 2), 16)).join(',') + ')';
    assert.equal(M.heat(0, 3, p), rgb(p.mid));
    assert.equal(M.heat(3, 3, p), rgb(p.pos));
    assert.equal(M.heat(99, 3, p), rgb(p.pos));
    assert.equal(M.heat(-3, 3, p), rgb(p.neg));
    assert.equal(M.heat(null, 3, p), p.none);
    assert.notEqual(M.heat(1, 3, p), M.heat(2, 3, p));
    assert.notEqual(M.heat(1, 3, p), M.heat(-1, 3, p));
  }
  // the two measured palettes of the Risk Map, unchanged
  const risk = fs.readFileSync(path.join(R, 'jh-risk-map.js'), 'utf8');
  for (const key of ['rg', 'cb']) for (const k of ['neg', 'pos', 'mid', 'none', 'up', 'dn']) assert.ok(risk.includes(k + ': "' + M.PALETTES[key][k] + '"'), key + ' ' + k);
  // a day moves less than a year: the full-colour mark grows with the window
  const full = M.WINDOWS.map(w => w.full);
  for (let i = 1; i < full.length; i++) assert.ok(full[i] > full[i - 1]);
});

test('every fund: search, sort both ways, and the whole list as a file', () => {
  const cur = FUNDS.filter(f => M.isCurrent(f, DOC));
  const big = M.sortFunds(cur, 'assets', 'desc');
  assert.deepEqual(big.slice(0, 3).map(f => f.t), ['VOO', 'SPY', 'VTI']);
  for (const c of FDOC.cols) {
    const up = M.sortFunds(FUNDS, c, 'asc'), down = M.sortFunds(FUNDS, c, 'desc');
    assert.equal(up.length, FUNDS.length);
    assert.equal(down.length, FUNDS.length);
    const filled = f => !(f[c] == null || f[c] === '');
    // an empty cell is last whichever way the column is turned
    const firstEmptyUp = up.findIndex(f => !filled(f)), firstEmptyDown = down.findIndex(f => !filled(f));
    if (firstEmptyUp >= 0) assert.ok(up.slice(firstEmptyUp).every(f => !filled(f)), c);
    if (firstEmptyDown >= 0) assert.ok(down.slice(firstEmptyDown).every(f => !filled(f)), c);
    const key = f => c === 'flags' ? f[c].length : f[c], text = M.COLS[c][1] === 'text' || M.COLS[c][1] === 'date';
    const cmp = (x, y) => text ? String(x).localeCompare(String(y)) : x - y;
    const u = up.filter(filled).map(key), d = down.filter(filled).map(key);
    for (let i = 1; i < u.length; i++) { assert.ok(cmp(u[i - 1], u[i]) <= 0, c + ' ascending'); assert.ok(cmp(d[i - 1], d[i]) >= 0, c + ' descending'); }
  }
  assert.equal(M.searchFunds(FUNDS, 'spy')[0].t, 'SPY');
  assert.ok(M.searchFunds(FUNDS, 'XL').every(f => f.t.startsWith('XL') || /XL/i.test(f.name + ' ' + f.issuer)));
  assert.ok(M.searchFunds(FUNDS, 'vanguard').length > 5);
  assert.ok(M.searchFunds(FUNDS, 'vanguard').every(f => /vanguard/i.test(f.name + ' ' + f.issuer + ' ' + f.t)));
  assert.equal(M.searchFunds(FUNDS, '  ').length, FUNDS.length);
  const csv = M.csvOf(FUNDS, FDOC.cols).trimEnd().split('\n');
  assert.equal(csv.length, FUNDS.length + 1);
  assert.equal(csv[0], FDOC.cols.join(','));
  const spy = csv.find(l => l.startsWith('SPY,'));
  assert.ok(spy.includes('SPDR S&P 500 ETF Trust') && spy.includes(String(FUNDS.find(f => f.t === 'SPY').w1)));
  assert.equal(M.csvOf([{ t: 'A,B', name: 'say "hi"', flags: ['x', 'y'] }], ['t', 'name', 'flags']), 't,name,flags\n"A,B","say ""hi""",x; y\n');
  // what a cell prints
  assert.equal(M.cellText('streak', -3), '3 out');
  assert.equal(M.cellText('streak', 4), '4 in');
  assert.equal(M.cellText('chk', 1), 'matches (next day)');
  assert.equal(M.cellText('chk', -1), 'differs');
  assert.equal(M.cellText('flags', ['2026-11-04 split 3-for-1']), '11-04 split 3-for-1');
  assert.equal(M.cellText('w1', -6060084785.8), '−$6.06B');
  assert.equal(M.cellText('assets', 764916664787.85), '$765B');
  assert.equal(M.cellText('p_w1', -0.772), '−0.77%');
  assert.equal(M.cellText('name', null), '');
});

test('day by day: the chart draws the history document\'s own days', () => {
  const key = 'segments:Equity · United States';
  const s = M.seriesOf(HIST, key, 21, false);
  assert.equal(s.values.length, 21);
  assert.deepEqual(s.values, HIST.series[key].slice(-21));
  assert.equal(s.dates[20], DOC.as_of);
  assert.equal(s.sum, HIST.series[key].slice(-21).reduce((a, b) => a + b, 0));
  const c = M.seriesOf(HIST, key, 21, true);
  assert.equal(c.values[20], s.sum);                                     // the running total ends at the sum of the days
  assert.equal(M.seriesOf(HIST, key, 9999, false).values.length, HIST.dates.length);
  assert.deepEqual(M.seriesOf(HIST, 'no such row', 21, false).values, []);
  const svg = M.chartSvg(s, M.PALETTES.rg, { label: 'US equity' });
  assert.equal((svg.match(/<rect x=/g) || []).length, s.values.filter(v => v).length);
  assert.ok(svg.includes('data-n="21"') && svg.includes('aria-label="US equity"'));
  const pos = (svg.match(new RegExp('fill="' + M.PALETTES.rg.pos + '"', 'g')) || []).length, neg = (svg.match(new RegExp('fill="' + M.PALETTES.rg.neg + '"', 'g')) || []).length;
  assert.equal(pos, s.values.filter(v => v > 0).length);
  assert.equal(neg, s.values.filter(v => v < 0).length);
  const line = M.chartSvg(c, M.PALETTES.cb, { cumulative: true });
  assert.ok(line.includes('<polyline') && !line.includes('<rect x='));
  assert.ok(M.chartSvg({ dates: [], values: [] }, M.PALETTES.rg).includes('No history'));
  assert.equal(M.niceMax(7.3e9), 1e10);
  assert.equal(M.niceMax(1.9e9), 2e9);
  assert.equal(M.niceMax(0), 1);
  // a fund's own days are in the letter file its ticker starts with
  const spy = SHARDS.S.funds.SPY;
  assert.equal(spy.f.length, SHARDS.S.dates.length);
  assert.ok(Math.abs(spy.f.slice(-5).reduce((a, b) => a + (b || 0), 0) - FUNDS.find(f => f.t === 'SPY').w1) < 5);
});

test('the page: the money map first, the research view that was the page beneath it and one click away', () => {
  const root = HTML.indexOf('<div id="mm-root">'), script = HTML.indexOf('<script src="/jh-money-map.js?v='), main = HTML.indexOf('<main id="research-view" hidden>');
  assert.ok(root > 0 && script > root && main > script);
  assert.equal(HTML.split('<main').length, 2);
  // every element the research scripts fill is still there, inside the research view, and its three scripts still load
  for (const id of ['state', 'asof', 'permanent', 'cards', 'brief', 'selection', 'detail', 'history-title', 'chart-window', 'range', 'chart', 'history', 'prev', 'next', 'page-count', 'download-history', 'comparison', 'gaps',
    'tic-context', 'scenario-form', 'positions', 'add-position', 's-days', 'scenario-result', 'scenario-rows', 'download-scenario', 'contexts', 'evidence']) {
    const at = HTML.indexOf('id="' + id + '"');
    assert.ok(at > main, id);
    assert.equal(HTML.split('id="' + id + '"').length, 2, id);
  }
  for (const s of ['/jh-tic-research.js', '/jh-flow-research.js', '/jh-flow-research-page.js', '/jh-nav-drawer.js', '/sidebar.js']) assert.ok(HTML.includes('<script src="' + s + '" defer></script>'), s);
  assert.ok(HTML.includes('<h1>Compare the flows. Understand the exposure.</h1>'));
  // the script shows the research view for its own links (#research, ?run=) and has a button for each view
  assert.ok(JS.includes('root.location.hash === "#research"') && JS.includes('/(^|[?&])run=/.test(root.location.search)'));
  assert.ok(JS.includes('<button data-view="map" class="on">Money map</button>') && JS.includes('<button data-view="research">Research view</button>'));
  assert.ok(JS.includes('getElementById("research-view")'));
});

test('the script: plain ASCII, its colours kept from the re-skin, nothing but the engine\'s documents read', () => {
  assert.ok(/^[\x00-\x7f]*$/.test(JS), 'ASCII only');
  assert.ok(JS.includes('jh-reskin-skip'));
  const urls = [...JS.matchAll(/["'](\/data\/[^"']+)["']/g)].map(m => m[1]);
  assert.deepEqual([...new Set(urls)].sort(), ['/data/global-flow-map-funds.json', '/data/global-flow-map-history.json', '/data/global-flow-map.json', '/data/global-flow-map/funds/']);
  assert.ok(!/https?:\/\//.test(JS.replace(/\/\*[\s\S]*?\*\//g, '')), 'no other host');
  // the measured colours are in this file only: the page's own style sheet has none of them
  for (const key of ['rg', 'cb']) for (const k of ['neg', 'pos', 'up', 'dn']) assert.ok(!HTML.includes(M.PALETTES[key][k]), k);
  // a ticker hands off to the chart inside the chart's workspace, and is an ordinary link outside it
  assert.ok(JS.includes('root.top.jhOpenSymbol(b.getAttribute("data-chart"))'));
  assert.ok(JS.includes('href="/chart.html?symbol='));
  // every key of the main document has a place on the page (the whole document is browsable too)
  for (const k of Object.keys(DOC)) assert.ok(JS.includes('doc.' + k) || JS.includes('"' + k + '"') || JS.includes('.' + k), k);
  for (const k of Object.keys(DOC.quality)) assert.ok(JS.includes('q.' + k) || JS.includes('.' + k + ' '), 'quality.' + k);
  assert.ok(JS.includes('JSON.stringify(doc, null, 1)'));
});

test('the chart has a Flow button: a new one beside Flows, which is where and what it was', () => {
  // Khalid: "Check if that button doesnt exist first before you add it". It did not: Flows (plural) opens the Inflows & outflows desk.
  const flows = "<button class='wsico wsdesk' id=btn-flows title='Inflows & outflows'><span class=g>⇄</span><span class=l>Flows</span></button>";
  const mine = "<button class='wsico wsdesk' id=btn-mflow title='Money flow · where money is flowing in and out: every ETF by asset class, sector, country and fund'><span class=g>⇅</span><span class=l>Flow</span></button>";
  assert.equal(CHART_ENGINE.split(flows).length, 2, 'Flows is untouched');
  assert.equal(CHART_ENGINE.split(mine).length, 2, 'one Flow button');
  assert.equal(CHART_ENGINE.split('<span class=l>Flow</span>').length, 2, 'and only one button is called Flow');
  assert.ok(CHART_ENGINE.indexOf(mine) === CHART_ENGINE.indexOf(flows) + flows.length + '"+\n      "'.length, 'right after Flows');
  assert.ok(CHART_ENGINE.includes('var bflw=document.getElementById("btn-flows"); if(bflw) bflw.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("flows"); };'));
  assert.ok(CHART_ENGINE.includes('var bmfl=document.getElementById("btn-mflow"); if(bmfl) bmfl.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("mflow"); };'));
  const ids = [...CHART_ENGINE.matchAll(/<button class='wsico wsdesk' id=(btn-[a-z0-9]+) /g)].map(m => m[1]);
  assert.deepEqual(ids.slice(0, 4), ['btn-macro', 'btn-flows', 'btn-mflow', 'btn-heat']);
  assert.equal(new Set(ids).size, ids.length);
  // the workspace: this page, under its own key; the two flow entries that were there are as they were
  assert.ok(CHART_RAIL.includes('      flows: ["Inflows & outflows", "/inflows-outflows.html"],\n      mflow: ["Money flow · where money is flowing in and out", "/global-flow-desk.html"],\n'));
  assert.ok(CHART_RAIL.includes('      fl: ["Fund flows", "/flow.html"],'));
  assert.equal(CHART_RAIL.split('mflow:').length, 2);
  assert.ok(fs.existsSync(path.join(R, 'global-flow-desk.html')));
  // both script tags carry the mark so a browser never pairs a new button with an old rail
  const tags = CHART_HTML.match(/<script src="\/jh-chart-(?:engine|tvrail)\.js\?[^"]+"><\/script>/g);
  assert.equal(tags.length, 2);
  for (const t of tags) assert.ok(t.includes('&cbe=20261125&mflow=20261126&'), t.slice(0, 60));
  assert.equal(CHART_HTML.split('&mflow=20261126').length, 3);
});

test('the chart\'s byte ledger takes the Flow button back out, right after the twelve desk tabs', () => {
  const e0 = ledger.peelMoneyFlowEngine(ledger.peelChartTabsEngine(CHART_ENGINE, 'jh-chart-engine.js'), 'jh-chart-engine.js'), r0 = ledger.peelMoneyFlowRail(ledger.peelChartTabsRail(CHART_RAIL, 'jh-chart-tvrail.js'), 'jh-chart-tvrail.js'), h0 = ledger.peelMoneyFlowHtml(ledger.peelChartTabsHtml(CHART_HTML, 'chart.html'), 'chart.html');
  // the files as they were after the Credit button (2026-11-25), to the byte
  assert.equal(sha(e0), '094ff82407fd6b3a28ec423ad2ab71532224029df6ab0a5cc72bbedb2a82f486');
  assert.equal(sha(r0), '05f865f3572218e7cd8ff2c35641296e3406807b3815cbc9a900b99840987f10');
  assert.equal(sha(h0), 'fbf73726719c926e12f1026ef0e1196e437920a97cd85084d91489b0729db752');
  assert.ok(!e0.includes('btn-mflow') && !r0.includes('mflow') && !h0.includes('&mflow='));
  assert.equal(ledger.peelMoneyFlowEngine(ledger.peelChartTabsEngine(e0, 'jh-chart-engine.js'), 'jh-chart-engine.js'), e0);
  assert.equal(ledger.peelMoneyFlowRail(ledger.peelChartTabsRail(CHART_RAIL, 'jh-chart-engine.js'), 'jh-chart-engine.js'), CHART_RAIL);
  assert.match(fs.readFileSync(path.join(R, 'tests/helpers/chart-macro-line-preservation.cjs'), 'utf8'), /raw=peelChartTabsHtml\(raw,file\);raw=peelMoneyFlowEngine\(raw,file\);raw=peelMoneyFlowRail\(raw,file\);raw=peelMoneyFlowHtml\(raw,file\);raw=peelCreditBoardEngine\(raw,file\);/);
  // the whole ledger still walks every chart file back to where it began
  for (const [f, raw] of [['jh-chart-engine.js', CHART_ENGINE], ['jh-chart-tvrail.js', CHART_RAIL], ['chart.html', CHART_HTML]]) assert.notEqual(sha(ledger.normalize(raw, f)), sha(raw), f);
});
