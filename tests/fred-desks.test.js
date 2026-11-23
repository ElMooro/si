'use strict';
// The three FRED desks beside 13F on the chart (2026-11-17): dollar indexes, Fed securities held outright, ICE BofA indices.
// Catalog shape, the DXY rebuilt from FRED's six exchange rates, the calendar arithmetic behind week / month / quarter / year,
// the generated pages and the chart buttons.  Real observations only (tests/fixtures/fred-desks.json, read by ops 6834).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '..');
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
const html = read('chart-pro.html');
const bundle = read('assets/jh-workspaces.js');
const fx = JSON.parse(read('tests/fixtures/fred-desks.json'));

function block(src, head) {
  // the balanced {...} or [...] that follows `head`
  const at = src.indexOf(head);
  assert.ok(at >= 0, 'missing ' + head);
  let i = at + head.length;
  while (src[i] !== '{' && src[i] !== '[') i++;
  const open = src[i], close = open === '{' ? '}' : ']';
  let depth = 0, quote = null;
  for (let j = i; j < src.length; j++) {
    const c = src[j];
    if (quote) { if (c === '\\') j++; else if (c === quote) quote = null; continue; }
    if (c === '"' || c === "'" || c === '`') { quote = c; continue; }
    if (c === open) depth++;
    else if (c === close && --depth === 0) return src.slice(at, j + 1);
  }
  throw new Error('unbalanced ' + head);
}
const catalog = name => Function('return ' + block(html, '  static ' + name + ' = ').replace('  static ' + name + ' = ', ''))();
const member = name => block(html, '  static ' + name);

// the dollar desk: 95 FRED series in 8 families, and since 2026-11-22 the "Dollar shortage" family: three ECB measurements and five Federal Reserve lines
const DESKS = { dollar: ['DOLLAR_CATALOG', 103, 9], soma: ['SOMA_CATALOG', 222, 25], bofa: ['BOFA_CATALOG', 192, 16] };

test('each desk holds every series once', () => {
  for (const [desk, [name, n, families]] of Object.entries(DESKS)) {
    const cats = catalog(name);
    const ids = cats.flatMap(c => c.items.map(i => i[0]));
    assert.equal(cats.length, families, desk + ' families');
    assert.equal(ids.length, n, desk + ' series');
    assert.equal(new Set(ids).size, n, desk + ' has a series twice');
    assert.equal(new Set(cats.map(c => c.cat)).size, families, desk + ' has a family name twice');
    for (const c of cats) for (const it of c.items) {
      assert.ok(/^(calc:[a-z]+|[A-Z0-9]+|flows:ecb-dollar\/(AUX|FED|USD)_[A-Z0-9_]+)$/.test(it[0]), desk + ' id ' + it[0]);
      assert.equal(it[0].startsWith('flows:'), desk === 'dollar' && c.cat.startsWith('Dollar shortage'), 'a site document feeds only the Dollar shortage family: ' + it[0]);
      assert.ok(it[1] && it[1].length <= 96 && it[2], desk + ' title / unit of ' + it[0]);
      assert.ok(!/[{}`]/.test(it[1] + c.cat), 'a brace in a title breaks the page generator: ' + it[0]);
    }
  }
});

test('the ICE BofA desk is the four measures of 48 indexes', () => {
  const cats = catalog('BOFA_CATALOG');
  const ids = cats.flatMap(c => c.items.map(i => i[0]));
  assert.ok(ids.every(id => id.startsWith('BAML')));
  const by = { OAS: 0, 'effective yield': 0, 'yield to worst': 0, 'total return': 0 };
  cats.forEach(c => c.items.forEach(i => { const k = i[1].split(' · ').pop(); assert.ok(k in by, i[1]); by[k]++; assert.equal(i[2], k === 'total return' ? 'index' : '%'); }));
  assert.deepEqual(by, { OAS: 48, 'effective yield': 48, 'yield to worst': 48, 'total return': 48 });
});

test('held outright: 103 lines the H.4.1 prints today, 60 of them changes, then the lines that ended', () => {
  const cats = catalog('SOMA_CATALOG');
  const live = cats.filter(c => !/^Ended/.test(c.cat)), ended = cats.filter(c => /^Ended/.test(c.cat));
  assert.equal(live.reduce((n, c) => n + c.items.length, 0), 103);
  assert.equal(ended.reduce((n, c) => n + c.items.length, 0), 119);
  const changes = cats.flatMap(c => c.items.filter(i => i[3] === 'C'));
  assert.equal(changes.length, 60);
  assert.ok(changes.every(i => /XCH(1|52)NWW$/.test(i[0]) && /change/.test(i[1])), 'a change tag on a level');
  assert.ok(cats.flatMap(c => c.items).filter(i => i[3] !== 'C').every(i => !/XCH(1|52)NWW$/.test(i[0])), 'a change line without its tag');
  assert.equal(ended.filter(c => /district \d+\)$/.test(c.cat)).length, 12);
  assert.ok(cats.flatMap(c => c.items).every(i => i[2] === '$M'));
});

test('the desk registry carries the three desks with their own storage', () => {
  const desks = Function('return ' + block(html, '  static DESKS = ').replace('  static DESKS = ', ''))();
  for (const [desk, [name]] of Object.entries(DESKS)) {
    assert.equal(desks[desk].catalog, name);
    assert.equal(desks[desk].key, 'jh_' + desk + '_workspace_v1');
  }
  assert.equal(desks.macro.key, 'jh_macro_workspace_v2');
  assert.equal(desks.flows.key, 'jh_flows_workspace_v1');
  assert.equal(new Set(Object.values(desks).map(d => d.key)).size, Object.keys(desks).length);
});

function engine() {
  // the page's own code for the arithmetic, lifted out of chart-pro.html
  const src = 'class M {\n  static HISTORY_FROM = "1996-01-01";\n' +
    ['CALC = ', 'dayOf(', 'isoOf(', 'packHistory(', 'monthsBack(', 'metricFromObservations(', 'async fetchCalc('].map(member).map(m => /;\s*$/.test(m) || m.trim().endsWith('}') ? m : m + ';').join('\n') +
    '\n}\nreturn M;';
  return Function(src)();
}

test('a month, a quarter and a year back stay on the calendar at a month end', () => {
  const M = engine();
  const back = (d, n) => new Date(M.monthsBack(Date.parse(d + 'T00:00:00Z'), n)).toISOString().slice(0, 10);
  assert.equal(back('2019-12-31', 3), '2019-09-30');
  assert.equal(back('2026-03-31', 1), '2026-02-28');
  assert.equal(back('2024-02-29', 12), '2023-02-28');
  assert.equal(back('2026-05-31', 1), '2026-04-30');
  assert.equal(back('2026-11-13', 1), '2026-10-13');
  assert.equal(back('2026-01-15', 3), '2025-10-15');
  assert.equal(back('2026-11-13', 12), '2025-11-13');
});

const obsOf = rows => rows.map(([date, value]) => ({ date, value }));

test('week / month / quarter / year percentages are the runner\'s own figures', () => {
  const M = engine();
  for (const [id, row] of Object.entries(fx.series)) {
    const metric = M.metricFromObservations(obsOf(row.obs));
    assert.equal(metric.asof, row.want.last, id);
    assert.equal(metric.last, row.want.value, id);
    for (const hz of ['W', 'M', 'Q', 'Y']) {
      if (row.want.chg[hz] == null) assert.equal(metric.changes[hz], null, id + ' ' + hz);
      else assert.ok(Math.abs(metric.changes[hz] - row.want.chg[hz]) < 6e-5, id + ' ' + hz + ' ' + metric.changes[hz] + ' vs ' + row.want.chg[hz]);
    }
  }
});

test('the DXY rebuilt from FRED\'s six exchange rates', async () => {
  const M = engine();
  const spec = M.CALC.dxy;
  assert.deepEqual(spec.legs.map(l => l[0]), ['FRED:DEXUSEU', 'FRED:DEXJPUS', 'FRED:DEXUSUK', 'FRED:DEXCAUS', 'FRED:DEXSDUS', 'FRED:DEXSZUS']);
  assert.ok(Math.abs(spec.legs.reduce((n, l) => n + Math.abs(l[1]), 0) - 1) < 1e-12, 'ICE weights sum to 100%');
  assert.deepEqual(spec.legs.filter(l => l[1] < 0).map(l => l[0]), ['FRED:DEXUSEU', 'FRED:DEXUSUK'], 'the two rates quoted in dollars enter inverted');
  assert.equal(spec.k, 50.14348112);
  M.fetchMetric = async id => M.metricFromObservations(obsOf(fx.legs[id.slice(5)]));
  const metric = await M.fetchCalc('calc:dxy');
  assert.equal(metric.asof, fx.dxy.last[0]);
  assert.ok(Math.abs(metric.last - fx.dxy.last[1]) < 1e-9, metric.last + ' vs ' + fx.dxy.last[1]);
  for (const hz of ['W', 'M', 'Q', 'Y']) assert.ok(Math.abs(metric.changes[hz] - fx.dxy.chg[hz]) < 6e-5, hz + ' ' + metric.changes[hz] + ' vs ' + fx.dxy.chg[hz]);
  // every one of the runner's last twelve days, to the last digit that matters
  const mine = new Map();
  for (let i = 0; i < metric.hist.t.length; i++) mine.set(M.isoOf(metric.hist.t[i]), metric.hist.v[i]);
  for (const [day, value] of fx.dxy.tail) assert.ok(Math.abs(mine.get(day) - value) < 1e-9, day);
  // and it sits beside the market's own close (ops 6834: 124 common days, worst gap 0.631%)
  assert.ok(Math.abs(metric.last / fx.dxy.market.latest[2] - 1) < 0.01);
  await assert.rejects(M.fetchCalc('calc:nothing'), /Unknown calculated series/);
});

test('the generated bundle and pages carry the desks', () => {
  for (const name of ['DOLLAR_CATALOG', 'SOMA_CATALOG', 'BOFA_CATALOG', 'fetchCalc', 'isChangeSeries', 'monthsBack']) assert.ok(bundle.includes(name), 'bundle lacks ' + name + ' (run scripts/build_workspaces.py)');
  assert.equal(block(bundle, '  static BOFA_CATALOG = '), block(html, '  static BOFA_CATALOG = '), 'bundle is older than chart-pro.html');
  assert.ok(/MacroData\.configure\(MacroData\.DESKS && MacroData\.DESKS\[which\] \? which : 'macro'\)/.test(bundle));
  const pages = { 'dollar-indexes.html': ['dollar', 'Dollar indexes'], 'securities-held-outright.html': ['soma', 'Fed securities held outright'], 'ice-bofa-indices.html': ['bofa', 'ICE BofA indices'] };
  for (const [file, [desk, title]] of Object.entries(pages)) {
    const page = read(file);
    assert.ok(page.includes("JH_WS.boot('" + desk + "')"), file + ' boots ' + desk);
    assert.ok(page.includes('<title>' + title + ' · JustHodl AI</title>'), file + ' title');
    assert.ok(page.includes('id="macro-modal-title">') && page.includes(title + '</div>'), file + ' heading');
    assert.ok(!page.includes('Macro &amp; Economic Data</div>'), file + ' still carries the Macro heading');
    assert.ok(page.includes('/assets/jh-workspaces.js') && page.includes('id="macro-focus"'), file + ' engine + full-screen view');
    for (const other of Object.keys(pages)) if (other !== file) assert.ok(page.includes('href="/' + other + '"'), file + ' links ' + other);
  }
  for (const untouched of ['macro-economic-data.html', 'inflows-outflows.html']) assert.ok(!/dollar-indexes|ice-bofa/.test(read(untouched)));
});

test('three buttons beside 13F open the three desks', () => {
  const eng = read('jh-chart-engine.js'), rail = read('jh-chart-tvrail.js');
  const order = ['btn-cds', 'btn-usd', 'btn-soma', 'btn-bofa', 'btn-13f', 'btn-ldr'].map(id => eng.indexOf("id=" + id + " title="));
  assert.ok(order.every(i => i > 0) && order.every((v, i) => i === 0 || v > order[i - 1]), 'CDS, Dollar, Outright, BofA, 13F, Leaders in that order: ' + order);
  for (const [id, kind, page] of [['btn-usd', 'dollar', '/dollar-indexes.html'], ['btn-soma', 'soma', '/securities-held-outright.html'], ['btn-bofa', 'bofa', '/ice-bofa-indices.html']]) {
    assert.ok(eng.includes('getElementById("' + id + '")') && eng.includes('window.jhOpenWorkspace("' + kind + '")'), id + ' click');
    assert.ok(new RegExp('\\b' + kind + ': \\["[^"]+", "' + page.replace(/[.]/g, '\\.') + '"\\]').test(rail), kind + ' rail meta');
    assert.ok(fs.existsSync(path.join(ROOT, page.slice(1))), page);
  }
});

// ── percent change from a year ago on every chart of the held outright desk (2026-11-23) ──
// Khalid: "on all the charts on: Fed securities held outright you should also add the option to view the chart on year to year
// percentage change. add it to every single chart".  Real weekly histories and the runner's own Python figures (ops 6928).
const zlib = require('node:zlib');
const yfx = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, 'tests/fixtures/held-outright-yoy.json.gz'))).toString('utf8'));
const YOY_MEMBERS = ['dayOf(', 'isoOf(', 'packHistory(', 'historyAt(', 'fmtHist(', 'yoyDesk(', 'viewOf(', 'yoyHistory(', 'yoyState(', 'yoyScale(', 'yoyAt(', 'fmtYoy(', 'fmtExact(', 'yoyLine(',
  'viewButtons(', 'historySvg(', 'turnsCaption(', 'histSlot('];
function yoyEngine(from) {
  const tags = new Set(catalog('SOMA_CATALOG').flatMap(c => c.items.filter(i => i[3] === 'C').map(i => 'FRED:' + i[0])));
  const src = 'const UI = { esc: s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;") };\nclass M {\n  static HISTORY_FROM = ' + JSON.stringify(from || '1996-01-01') + ';\n' +
    '  static YOY_BACK = 364;\n  static YOY_FENCE = 3;\n  static desk = "soma";\n  static state = { categories: [] };\n  static isChangeSeries(symbol) { return tags.has(String(symbol)); }\n' +
    [block(html, '  static DESKS = ')].concat(YOY_MEMBERS.map(member)).map(m => /;\s*$/.test(m) || m.trim().endsWith('}') ? m : m + ';').join('\n') + '\n}\nreturn M;';
  return Function('tags', src)(tags);
}
const histOf = (M, id) => M.packHistory(obsOf(yfx.series[id]));
const TURNS = [{ name: '2008 financial crisis', top: { date: '2007-10-09', value: 1, move: -0.57, until: '2009-03-09' }, bottom: { date: '2009-03-09', value: 1, move: 4, until: '2020-02-19' } },
  { name: 'COVID', top: { date: '2020-02-19', value: 1, move: -0.34, until: '2020-03-23' }, bottom: { date: '2020-03-23', value: 1, move: 1.1, until: '2022-01-03' } }];

test('year over year: only the held outright desk offers it, and a chart opens as Level', () => {
  const desks = Function('return ' + block(html, '  static DESKS = ').replace('  static DESKS = ', ''))();
  assert.deepEqual(Object.keys(desks).filter(k => desks[k].yoy), ['soma']);
  const M = yoyEngine();
  assert.equal(M.viewOf({}), 'level', 'nothing chosen');
  M.state.chartView = 'yoy';
  assert.equal(M.viewOf({}), 'yoy', 'the desk switch');
  assert.equal(M.viewOf({ view: 'level' }), 'level', 'a chart keeps its own choice against the desk');
  M.state.chartView = undefined;
  assert.equal(M.viewOf({ view: 'yoy' }), 'yoy', 'a chart on its own');
  M.desk = 'macro';
  assert.equal(M.viewOf({ view: 'yoy' }), 'level', 'a desk without the option never draws it');
  assert.equal(M.histSlot({}), '<div class="macro-hist" title="Click for the full-screen history"><div class="macro-hist-head"><span>1996 → today</span><span class="macro-hist-lg"><i class="t"></i>top<i class="b"></i>bottom</span></div><div class="macro-hist-svg"><span class="macro-hist-wait">history…</span></div><div class="macro-hist-cap"></div></div>', 'the other desks keep their strip as it was');
  M.desk = 'soma';
  const slot = M.histSlot({});
  assert.ok(/data-view="level" aria-pressed="true"/.test(slot) && /data-view="yoy" aria-pressed="false"/.test(slot) && slot.includes('macro-hist-yoy'), 'both buttons on the strip, Level on');
});

test('year over year: every figure is the runner\'s own, by FRED\'s formula', () => {
  const M = yoyEngine();
  let checked = 0;
  for (const [id, f] of Object.entries(yfx.facts)) {
    if (!f.yoy_n || /change/.test(f.cls)) continue;
    const st = M.yoyState({ symbol: 'FRED:' + id }, histOf(M, 'FRED:' + id)), y = st.y, k = y.t.length - 1, sc = M.yoyScale(y);
    assert.ok(st.ok, id);
    assert.equal(y.t.length, f.yoy_n, id + ' figures');
    assert.equal(M.isoOf(y.t[k]), f.yoy_last[0], id + ' last day');
    assert.ok(Math.abs(y.v[k] - f.yoy_last[1]) < 1e-9, id + ' last ' + y.v[k] + ' vs ' + f.yoy_last[1]);
    assert.equal(y.level[k], f.yoy_last[2], id + ' level');
    assert.equal(y.base[k], f.yoy_last[3], id + ' year-ago print');
    assert.equal(y.t[k] - y.baseDay[k], 364, id + ' the same weekday 52 weeks earlier');
    assert.ok(Math.abs(sc.min - f.yoy_min[1]) < 1e-9 && M.isoOf(sc.minDay) === f.yoy_min[0], id + ' low');
    assert.ok(Math.abs(sc.max - f.yoy_max[1]) < 1e-6 * Math.max(1, Math.abs(sc.max)) && M.isoOf(sc.maxDay) === f.yoy_max[0], id + ' high');
    let crossings = 0, last = null;
    for (let i = 1; i < y.t.length; i++) if ((y.v[i] > 0) !== (y.v[i - 1] > 0)) { crossings++; last = M.isoOf(y.t[i]); }
    assert.equal(crossings, f.crossings, id + ' crossings of zero');
    assert.equal(last, f.last_cross, id + ' last crossing');
    for (let i = 0; i < y.t.length; i++) assert.ok(y.base[i] !== 0 && Number.isFinite(y.v[i]), id + ' a figure off a zero print');
    checked++;
  }
  assert.equal(checked, 7);
  // a weekly line with zero prints: no figure there, and none invented
  const mbs = M.yoyHistory(histOf(M, 'FRED:WSHOMCB'));
  assert.equal(mbs.t.length, 880);
  assert.ok(M.isoOf(mbs.t[0]) > '2009-01-01', 'no figure while the year-ago print was zero: first ' + M.isoOf(mbs.t[0]));
});

test('year over year: the year is the Federal Reserve\'s own', () => {
  // the H.4.1 prints "change from a year ago" beside the level: the level less its print 364 days earlier has to be that line
  const M = yoyEngine(), y = M.yoyHistory(histOf(M, 'FRED:WSHOSHO')), fed = new Map(yfx.series['FRED:RESPPALGXCH52NWW']);
  let weeks = 0;
  for (let i = 0; i < y.t.length; i++) {
    const printed = fed.get(M.isoOf(y.t[i]));
    assert.ok(printed !== undefined, M.isoOf(y.t[i]));
    assert.ok(Math.abs((y.level[i] - y.base[i]) - printed) <= 1.5, M.isoOf(y.t[i]) + ': ' + (y.level[i] - y.base[i]) + ' vs the H.4.1 ' + printed);
    weeks++;
  }
  assert.equal(weeks, 1197);
});

test('year over year: a change, a line across zero or one that ended before 1996 is not made a percentage; a line below zero says what a plus means', () => {
  const M = yoyEngine();
  const change = M.yoyState({ symbol: 'FRED:RESPPALGXCH52NWW' }, histOf(M, 'FRED:RESPPALGXCH52NWW'));
  assert.deepEqual([change.ok, change.code], [false, 'change']);
  const weekly = histOf(M, 'FRED:RESPPALGXCH1NWW');
  const crosses = M.yoyState({ symbol: 'FRED:NOT_TAGGED' }, weekly);
  assert.deepEqual([crosses.ok, crosses.code], [false, 'crosses'], 'the same prints without the tag are caught by their sign');
  const ended = M.yoyState({ symbol: 'FRED:RAGSOCID' }, histOf(M, 'FRED:RAGSOCID'));
  assert.deepEqual([ended.ok, ended.code], [false, 'none']);
  const below = M.yoyState({ symbol: 'FRED:WUDSHO' }, histOf(M, 'FRED:WUDSHO')), k = below.y.t.length - 1;
  assert.deepEqual([below.ok, below.code], [true, 'below']);
  assert.ok(below.y.level[k] < below.y.base[k] && below.y.base[k] < 0 && below.y.v[k] > 0, 'FRED\'s formula as it stands: a discount that grew reads plus');
  assert.match(M.yoyLine(below, 'yoy', M.yoyScale(below.y)), /below zero: by FRED's formula a plus means it grew in size/);
  assert.equal(M.yoyLine(change, 'yoy', null), 'Drawn as it is: this line is itself a change, so a percentage of it would mislead.');
  assert.equal(M.yoyLine(change, 'level', null), '', 'nothing said while Level is what was asked for');
  assert.match(M.viewButtons('level', change, 'view'), /class="macro-view-btn off" data-view="yoy" aria-pressed="false" aria-disabled="true"/);
});

test('year over year: the first year of the strip has its year-ago prints', () => {
  const M = yoyEngine('2010-01-01'), hist = histOf(M, 'FRED:WSHOSHO'), y = M.yoyHistory(hist);
  assert.equal(M.isoOf(hist.t[0]), '2010-01-06');
  assert.ok(hist.lead.length >= 52 && hist.lead.length <= 58 && M.isoOf(hist.lead[0][0]) >= '2008-11-27', 'only the 400 days before the strip are kept: ' + hist.lead.length);
  assert.equal(M.isoOf(y.t[0]), '2010-01-06', 'the first print of the strip already has a figure');
  assert.equal(M.isoOf(y.baseDay[0]), '2009-01-07');
  assert.equal(y.t.length, hist.t.length);
  const whole = yoyEngine(), all = whole.yoyHistory(histOf(whole, 'FRED:WSHOSHO')), at = all.t.indexOf(y.t[0]);
  assert.ok(Math.abs(all.v[at] - y.v[0]) < 1e-12, 'the same figure as on the whole history');
});

test('year over year: the scale takes in every figure unless a few stand far outside the rest', () => {
  const M = yoyEngine();
  const scaleOf = id => { const y = M.yoyHistory(histOf(M, id)); return Object.assign(M.yoyScale(y), { last: y.v[y.v.length - 1] }); };
  const tsy = scaleOf('FRED:WSHOTSL');
  assert.deepEqual([tsy.cutLo, tsy.cutHi, tsy.lo, tsy.hi], [false, false, tsy.min, tsy.max], 'Treasuries: nothing left out');
  const mbs = scaleOf('FRED:WSHOMCB');
  assert.ok(mbs.cutHi && !mbs.cutLo && mbs.max > 17000 && mbs.hi < 200 && mbs.lo === mbs.min, 'mortgage-backed securities started from almost nothing: ' + JSON.stringify(mbs));
  const bills = scaleOf('FRED:WSHOBL');
  assert.ok(bills.cutHi && bills.max > 16e6 && bills.hi === bills.last && bills.lo === -100, 'bills: the latest figure is never left off the scale: ' + JSON.stringify(bills));
  for (const id of Object.keys(yfx.series)) {
    const y = M.yoyHistory(histOf(M, id));
    if (!y || y.t.length < 2) continue;
    const sc = M.yoyScale(y), last = y.v[y.v.length - 1];
    assert.ok(sc.lo <= 0 && sc.hi >= 0 && sc.lo <= last && sc.hi >= last && sc.lo >= Math.min(sc.min, 0) && sc.hi <= Math.max(sc.max, 0), id + ' ' + JSON.stringify(sc));
    assert.equal(sc.cutHi, sc.max > sc.hi, id);
    assert.equal(sc.cutLo, sc.min < sc.lo, id);
  }
  const few = M.yoyScale({ t: Int32Array.from([1, 2, 3]), v: Float64Array.from([-72, 900000, -100]) });
  assert.deepEqual([few.cutHi, few.lo, few.hi], [false, -100, 900000], 'too few figures to call any of them far out');
});

test('year over year: the strip has a zero line, breaks where figures are missing and marks an edge it stops at; Level is drawn as before', () => {
  const M = yoyEngine();
  const draw = (id, big) => { const y = M.yoyHistory(histOf(M, id)); return M.historySvg(y, TURNS, { w: big ? 1200 : 360, h: big ? 520 : 132, units: '%', big, yoy: M.yoyScale(y) }); };
  const count = (s, needle) => s.split(needle).length - 1;
  const total = draw('FRED:WSHOSHO', false);
  assert.ok(total.includes('aria-label="percent change from a year ago since 1996"') && total.includes('data-yoy="1197"'));
  assert.equal(count(total, 'class="macro-hist-zero"'), 1);
  assert.equal(count(total, 'class="macro-hist-line"'), 1, 'an unbroken weekly line is one run');
  assert.ok(total.includes('>0%</text>'));
  const bills = draw('FRED:WSHOBL', false);
  assert.ok(count(bills, 'class="macro-hist-line"') >= 3, 'bills were zero for years: the line breaks instead of bridging them');
  assert.ok(bills.includes('data-cut="hi"') && count(bills, 'class="macro-hist-cut"') === 1);
  const big = draw('FRED:WSHOMCB', true);
  assert.ok(/off the scale along this edge · high \+17,092% in 2010-01/.test(big) && /now −9\.0%/.test(big), 'the big chart prints what the scale leaves out and the latest figure');
  assert.ok(big.includes('percent change from a year ago · dotted grey'));
  for (const svg of [total, bills, big]) assert.ok(!/NaN|undefined|Infinity/.test(svg));
  const level = M.historySvg(histOf(M, 'FRED:WSHOSHO'), TURNS, { w: 360, h: 132, units: '$M' });
  assert.ok(level.includes('aria-label="history since 1996"') && !/macro-hist-zero|data-yoy|macro-hist-cut/.test(level));
  assert.equal(count(level, 'class="macro-hist-line"'), 1);
  assert.equal(M.turnsCaption(M.yoyHistory(histOf(M, 'FRED:WSHOMCB')), TURNS, '%', true), '2008 ▲— · COVID ▲−14.1% ▼−14.6%', 'no figure borrowed from the far side of a gap');
});

test('year over year: how a figure is written', () => {
  const M = yoyEngine();
  assert.deepEqual([0.04, -0.04, 3.349, -7.07, 197, 999.96, 17091.9, 16302100, -100, NaN].map(v => M.fmtYoy(v)),
    ['0.0%', '0.0%', '+3.3%', '−7.1%', '+197.0%', '+1000.0%', '+17,092%', '+16,302,100%', '−100.0%', '—']);
  const st = M.yoyState({ symbol: 'FRED:WSHOSHO' }, histOf(M, 'FRED:WSHOSHO'));
  assert.equal(M.yoyLine(st, 'yoy', M.yoyScale(st.y)), '<b>+3.3%</b> from a year ago · 2026-11-18: 6,472,944 against 6,263,192 on 2025-11-19 · off the scale: high +277.8% in 2010-01');
  assert.equal(M.yoyLine(st, 'level', null), '');
});

test('year over year: the generated bundle and stylesheet carry it', () => {
  for (const name of ['static yoyHistory(', 'static yoyScale(', 'static setAllViews(', 'static paintFocusChart(', 'data-allview']) assert.ok(bundle.includes(name), 'bundle lacks ' + name + ' (run scripts/build_workspaces.py)');
  assert.equal(block(bundle, '  static yoyHistory('), block(html, '  static yoyHistory('), 'bundle is older than chart-pro.html');
  assert.equal(block(bundle, '  static historySvg('), block(html, '  static historySvg('), 'bundle is older than chart-pro.html');
  const css = read('assets/jh-workspaces.css');
  for (const rule of ['.macro-view-btn.on', '.macro-viewbar', '.macro-hist-zero', '.macro-hist-cut', '.macro-focus-view']) assert.ok(css.includes(rule), 'stylesheet lacks ' + rule);
});
