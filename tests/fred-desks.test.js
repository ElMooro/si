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

// the dollar desk: 95 FRED series in 8 families, and since 2026-11-22 the three ECB measurements of the "Dollar shortage" family
const DESKS = { dollar: ['DOLLAR_CATALOG', 98, 9], soma: ['SOMA_CATALOG', 222, 25], bofa: ['BOFA_CATALOG', 192, 16] };

test('each desk holds every series once', () => {
  for (const [desk, [name, n, families]] of Object.entries(DESKS)) {
    const cats = catalog(name);
    const ids = cats.flatMap(c => c.items.map(i => i[0]));
    assert.equal(cats.length, families, desk + ' families');
    assert.equal(ids.length, n, desk + ' series');
    assert.equal(new Set(ids).size, n, desk + ' has a series twice');
    assert.equal(new Set(cats.map(c => c.cat)).size, families, desk + ' has a family name twice');
    for (const c of cats) for (const it of c.items) {
      assert.ok(/^(calc:[a-z]+|[A-Z0-9]+|flows:ecb-dollar\/AUX_[A-Z_]+)$/.test(it[0]), desk + ' id ' + it[0]);
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
