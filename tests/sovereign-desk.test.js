'use strict';
// The Sovereign stress desk (sovereign-desk.html, jh-sovereign-desk.js), its chart button, and the CISS page's repairs -
// on real numbers: the documents the CISS engine (sovereign document 1.1.0) compiled in its dry run of 2026-11-22
// 16:23 UTC (ops 6913) and rows of the live warehouse (ops 6912), cut into tests/fixtures/sovereign-desk.json.gz.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), zlib = require('node:zlib');
const { createHash, webcrypto } = require('node:crypto');
if (!globalThis.crypto) globalThis.crypto = webcrypto;
const ROOT = path.join(__dirname, '..');
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
const FX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures', 'sovereign-desk.json.gz'))));
const D = FX.desk, M = D.measures, H = FX.history.series;
const desk = require('../jh-sovereign-desk.js'), ciss = require('../jh-ciss-research.js');
const TODAY = '2026-11-22', NOW = Date.parse('2026-11-22T15:00:00Z');
const near = (a, b, tol, what) => assert.ok(Math.abs(a - b) <= tol, `${what}: ${a} against ${b}`);

test('the fixture is the engine\'s real document', () => {
  assert.equal(D.contract, 'ciss-sovereign.v1');
  assert.deepEqual(D.errors, {});
  assert.equal(D.version, '1.1.0');
  assert.equal(Object.keys(M).length, 266);
  assert.equal(D.countries.length, 30);
  assert.equal(D.episodes.length, 22);
});

test('numbers are written in their own unit, with a real minus sign and no negative zero', () => {
  assert.equal(desk.fmt('index', 0.17391), '0.1739');
  assert.equal(desk.fmt('bp', 59.81), '59.8 bp');
  assert.equal(desk.fmt('pct', 4.0396), '4.04%');
  assert.equal(desk.fmt('eur_bn', -453.798), '−€453.8 bn');
  assert.equal(desk.fmt('eur_bn', 1004.296), '€1,004.3 bn');
  assert.equal(desk.fmt('pct_gdp', 119.07), '119.1%');
  assert.equal(desk.fmt('index', -0.00001), '0.0000');
  assert.equal(desk.fmt('index', null), '—');
  assert.equal(desk.fmtChange('index', 0.09557), '+0.0956');
  assert.equal(desk.fmtChange('bp', -2.77), '−2.8 bp');
  assert.equal(desk.fmtChange('pct', 0.8386), '+0.84 pp');
  assert.equal(desk.fmtChange('bp', 0.01), '0.0 bp');
  assert.equal(desk.fmtChange('bp', null), '—');
});

test('a reading says the period it stands for and how old it is today', () => {
  assert.equal(desk.when(M.SOV_FR), '19 Nov 2026');
  assert.equal(desk.when(M.Y10_FR), 'Oct 2026');
  assert.equal(desk.when(M.DEBT_FR), 'Q2 2026');
  assert.deepEqual(desk.age(M.SOV_FR, TODAY), { days: 3, old: false });
  assert.equal(desk.age(M.SOV_FR, '2026-12-05').old, true);                 // sixteen days without a daily reading
  assert.equal(desk.age(M.Y10_FR, TODAY).old, false);
  assert.equal(desk.age(M.Y10_FR, '2027-03-01').old, true);
  assert.equal(desk.age(M.DEBT_FR, TODAY).old, false);                       // a quarter is allowed its lag
});

test('the board ranks the eleven governments by SovCISS and reads each against Germany on the same day', () => {
  const b = desk.board(D);
  assert.equal(b.sovereign.length, 11);
  assert.deepEqual(b.sovereign.map(r => r.area), D.cross_section.ranking.map(r => r.area));
  assert.equal(b.sovereign[0].area, 'FR');
  for (const r of b.sovereign) {
    if (r.area === 'DE') { assert.equal(r.vsGermany, null); continue; }
    near(r.vsGermany, r.m.sovciss.value - M.SOV_DE.value, 1e-9, r.area + ' against Germany');
    for (const kind of ['yield', 'spread', 'target', 'debt', 'clifs']) assert.ok(r.m[kind], r.area + ' ' + kind);
    const peaks = r.m.sovciss.peaks;
    assert.equal(r.crisis[0], Math.max(peaks.italy2011.hi[0], peaks.spain2012.hi[0]), r.area + ' 2011-12 peak');
  }
  assert.equal(b.others.length, 16);
  assert.ok(b.sovereign.every(r => r.key === desk.ckey(r.country)));
  assert.ok(b.others.every(r => !r.m.sovciss));
  const spreads = b.others.map(r => r.m.spread ? r.m.spread.value : -Infinity);
  assert.deepEqual(spreads, [...spreads].sort((x, y) => y - x));
  assert.equal(b.systemic.length, 13);
  assert.equal(desk.commonWhen(b.sovereign, 'yield'), 'Oct 2026');
  assert.equal(desk.commonWhen(b.sovereign, 'target'), 'Sep 2026');
});

test('the opening lines are the document\'s own numbers', () => {
  const lines = desk.read(D), rank = D.cross_section.ranking;
  assert.equal(lines.length, 6);
  const week = b => b.m.sovciss.change['1w'], rows = desk.board(D).sovereign, up = [...rows].sort((x, y) => week(y) - week(x))[0], down = [...rows].sort((x, y) => week(x) - week(y))[0];
  assert.equal(lines[4], 'Over the past week: largest rise ' + up.country + ' ' + desk.fmtChange('index', week(up)) + '; largest fall ' + down.country + ' ' + desk.fmtChange('index', week(down)) + '.');
  assert.ok(lines[5].includes('United States ' + desk.fmt('index', M.CISS_US.value)) && lines[5].includes('China ' + desk.fmt('index', M.CISS_CN.value)) && lines[5].includes('euro area ' + desk.fmt('index', M.EA_CISS.value)), lines[5]);
  assert.ok(lines[0].includes(desk.fmt('index', M.EA_SOV_GDP.value)) && lines[0].includes('19 Nov 2026') && lines[0].includes('lower than 76%'), lines[0]);
  assert.ok(lines[0].includes('record 0.7514, 29 Nov 2011'), lines[0]);
  assert.ok(lines[1].startsWith('Highest: France ' + desk.fmt('index', rank[0].value) + ', Belgium ' + desk.fmt('index', rank[1].value)), lines[1]);
  assert.ok(lines[1].includes('Lowest: Portugal ' + desk.fmt('index', rank[10].value)), lines[1]);
  assert.ok(lines[2].includes(desk.fmt('bp', M.EA_CURVE_10Y.value)), lines[2]);
  assert.ok(lines[3].includes('France ' + desk.fmt('bp', M.S10_FR.value)), lines[3]);
});

test('spreads and totals are what their parts say, on the same date', () => {
  near(M.EA_CURVE_10Y.value, (M.EA_ALL_10Y.value - M.EA_AAA_10Y.value) * 100, 0.011, 'all issuers minus AAA, 10 years');
  assert.equal(M.EA_CURVE_10Y.date, M.EA_ALL_10Y.date);
  for (const c of D.countries.filter(c => c.ids.spread)) {
    const s = M[c.ids.spread], y = M[c.ids.yield];
    if (y.date !== M.Y10_DE.date) continue;
    near(s.value, (y.value - M.Y10_DE.value) * 100, 0.011, c.area + ' spread to Germany');
  }
  near(M.EA_SOV_RANGE.value, D.cross_section.range, 1e-9, 'highest minus lowest');
  near(D.cross_section.range, D.cross_section.ranking[0].value - D.cross_section.ranking[10].value, 1.1e-5, 'range of the ranking');
  const owed = Object.values(M).filter(m => m.group === 'target' && m.area !== '4F' && m.date === M.EA_TGT_DEBT.date && m.value < 0).reduce((s, m) => s - m.value, 0);
  near(M.EA_TGT_DEBT.value, owed, 0.02, 'TARGET liabilities');
});

test('a stress measure is coloured by its place in its own history; a yield, a balance and a debt ratio are not', () => {
  assert.equal(desk.worseUp(M.SOV_FR), true);
  assert.equal(desk.worseUp(M.S10_IT), true);
  assert.equal(desk.worseUp(M.Y10_FR), null);
  assert.equal(desk.worseUp(M.TGT_DE), null);
  assert.equal(desk.worseUp(M.DEBT_FR), null);
  assert.equal(desk.heat(M.SOV_FR), 2);                                       // 75.7: above seventy, below eighty-five
  assert.equal(desk.heat(M.SOV_PT), 0);
  assert.equal(desk.heat(M.Y10_FR), 0);
  assert.equal(desk.standing(M.SOV_PT), 'lower than 98% of its readings since 2000');
  assert.equal(desk.standing(M.SOV_FR), 'higher than 75% of its readings since 2000');
});

test('the crisis table reads every window from the measures\' own peaks', () => {
  for (const set of desk.SETS) for (const id of set.ids) assert.ok(M[id], set.id + ' names ' + id);
  for (const id of D.euro_area.concat(D.contributions, D.auxiliaries)) assert.ok(M[id], 'the document names ' + id);
  const c = desk.crisis(D, desk.SETS[0].ids);
  assert.equal(c.cols.length, 18);                                            // the euro area, eleven daily series, six ended monthly ones
  assert.equal(c.rows.length, 19);                                            // no SovCISS existed in 1987, 1992 or 1998
  assert.ok(!c.rows.some(r => ['crash1987', 'erm1992', 'ltcm1998'].includes(r.ep.id)));
  const row = c.rows.find(r => r.ep.id === 'italy2011'), italy = c.cols.findIndex(m => m.id === 'SOV_IT');
  assert.deepEqual([row.cells[italy].v, row.cells[italy].d], M.SOV_IT.peaks.italy2011.hi);
  assert.equal(row.cells[italy].v, 0.99032);
  assert.equal(c.rows.find(r => r.ep.id === 'greece2015').cells[c.cols.findIndex(m => m.id === 'SOV_GR')].v, 0.92855);
  for (const r of c.rows) for (const x of r.cells) assert.ok(x === null || (x.share >= 0 && x.share <= 1));
  for (const r of c.rows) assert.ok(r.cells.slice(0, 12).every(Boolean), r.ep.id + ': every daily series has a reading from 2001 on');
  assert.ok(c.record.every(x => x.share === 1));
  // a series the ECB stopped has peaks and a record, and no "now"
  const poland = c.cols.findIndex(m => m.id === 'SOVM_PL');
  assert.equal(c.now[poland], null);
  assert.ok(c.now.slice(0, 12).every(Boolean));
  assert.deepEqual([c.record[poland].v, c.record[poland].d], M.SOVM_PL.high);
  assert.equal(c.rows.find(r => r.ep.id === 'sept2001').cells[poland], null);  // Poland's series begins in November 2002
  near(c.now[c.cols.findIndex(m => m.id === 'SOV_FR')].share, M.SOV_FR.value / M.SOV_FR.high[0], 1e-9, 'France now against its record');
  // a central bank that owes on TARGET is read at its lowest, one that is owed at its highest
  assert.equal(desk.side(M.TGT_IT), 'lo');
  assert.equal(desk.side(M.TGT_DE), 'hi');
  const euro = desk.crisis(D, desk.SETS.find(s => s.id === 'euro').ids), it = euro.cols.findIndex(m => m.id === 'TGT_IT'), de = euro.cols.findIndex(m => m.id === 'TGT_DE');
  const gilts = euro.rows.find(r => r.ep.id === 'gilts2022');
  assert.deepEqual([gilts.cells[it].v, gilts.cells[de].v], [M.TGT_IT.peaks.gilts2022.lo[0], M.TGT_DE.peaks.gilts2022.hi[0]]);
  assert.ok(gilts.cells[it].v < -600 && gilts.cells[de].v > 1200);
});

test('the euro crisis is where the data says it is', () => {
  assert.deepEqual(M.EA_SOV_GDP.high, [0.75141, '2011-11-29']);
  assert.deepEqual(M.EA_SOV_GDP.tops[0], [0.75141, '2011-11-29']);
  assert.equal(desk.windowOf(D.episodes, '2011-11-29').id, 'italy2011');
  assert.equal(desk.windowOf(D.episodes, '2012-07-26').id, 'spain2012');
  assert.equal(desk.windowOf(D.episodes, '2019-06-03'), null);
  assert.deepEqual(M.EA_CURVE_10Y.high, [194.81, '2012-07-23']);              // three days before "whatever it takes"
  assert.ok(M.S10_GR.peaks.spain2012.hi[0] > 2500);
  const days = M.EA_SOV_GDP.tops.map(t => Date.parse(t[1]) / 86400000);
  for (let i = 0; i < days.length; i++) for (let j = 0; j < i; j++) assert.ok(Math.abs(days[i] - days[j]) >= 120);
});

test('a history ends on the measure\'s own reading and never exceeds its daily high', () => {
  for (const id of Object.keys(H)) {
    const s = H[id], m = M[id];
    assert.equal(s.d.length, s.v.length, id);
    assert.equal(s.d[s.d.length - 1], m.date, id + ' last date');
    assert.equal(s.v[s.v.length - 1], m.value, id + ' last value');
    assert.deepEqual([...s.d].sort(), s.d, id + ' ascending');
    assert.ok(Math.max(...s.v) <= m.high[0] + 1e-9 && Math.min(...s.v) >= m.low[0] - 1e-9, id + ' inside its daily range');
  }
  const five = desk.visible(H.EA_SOV_GDP, 5);
  assert.ok(five.d[0] >= '2021-11-19' && five.d[0] < '2021-11-27');
  assert.equal(five.d[five.d.length - 1], '2026-11-19');
  assert.equal(desk.visible(H.EA_SOV_GDP, 0).d.length, H.EA_SOV_GDP.d.length);
  assert.deepEqual(desk.visible(null, 5), { d: [], v: [] });
  const b = desk.bands(D.episodes, '2011-01-01', '2012-12-31');
  assert.deepEqual(b.map(x => x.ep.id), ['italy2011', 'spain2012']);
  assert.ok(b[0].x0 > 0.2 && b[0].x1 < 0.6 && b[1].x0 > b[0].x1);
  assert.equal(desk.bands(D.episodes, '2000-01-01', '2026-11-19').length, 19);
  assert.equal(desk.bands(D.episodes, '1980-01-01', '2026-11-19').length, 22);
  // a year either side of a window, and the part of a history inside it
  const ep = D.episodes.find(e => e.id === 'italy2011'), span = desk.around(ep);
  assert.deepEqual(span, ['2010-07-01', '2012-12-31']);
  const cut = desk.visible(H.SOV_IT, 0, span);
  assert.ok(cut.d.length > 100 && cut.d[0] >= span[0] && cut.d[cut.d.length - 1] <= span[1]);
  assert.ok(Math.max(...cut.v) > 0.9);
});

test('a small line is drawn inside its box, and nothing is drawn from one point', () => {
  const p = desk.sparkPath(M.SOV_FR.spark, 110, 22);
  assert.match(p, /^M0\.0,/);
  const ys = p.split(' ').map(s => +s.split(',')[1]);
  assert.ok(ys.every(y => y >= 0 && y <= 22));
  assert.equal(desk.sparkPath([1], 10, 10), '');
  assert.equal(desk.sparkPath([3, 3, 3], 10, 10).includes('NaN'), false);
});

test('links stay inside the site\'s data and name the series at the ECB', () => {
  assert.equal(desk.ecbPage(M.SOV_FR), 'https://data.ecb.europa.eu/data/datasets/CISS/CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX');
  assert.equal(desk.ecbPage(M.EA_CURVE_10Y), null);                           // a difference of two series has no page of its own
  assert.equal(desk.dataPath(M.SOV_FR.evidence.key), '/' + M.SOV_FR.evidence.key);
  assert.equal(desk.dataPath('data/../secret.json'), null);
  assert.equal(desk.dataPath('https://example.com/x'), null);
  for (const m of Object.values(M)) assert.ok(m.evidence || m.derived, m.id + ' has an original or a stated derivation');
});

test('the world table shows reported figures, widest default-swap spread first, blanks left blank', () => {
  const rows = desk.world(FX.world);
  assert.equal(rows.length, FX.world.countries.length);
  const cds = rows.map(r => typeof r.cds === 'number' ? r.cds : -1);
  assert.deepEqual(cds, [...cds].sort((a, b) => b - a));
  const russia = rows.find(r => r.country === 'Russia');
  assert.equal(russia.cds, null);
  assert.ok(!('stress' in russia) && !('regime' in russia));                  // no score of the page's own
  assert.deepEqual(desk.world(null), []);
});

test('the page has every element its script uses and loads the script once, versioned', () => {
  const html = read('sovereign-desk.html'), js = read('jh-sovereign-desk.js');
  const used = new Set([...js.matchAll(/\$\("(sov-[a-z-]+)"\)/g)].map(m => m[1]));
  assert.ok(used.size >= 10);
  // an element is in the page, or the script writes it itself (the many-series chart of the Crisis view)
  for (const id of used) assert.ok(html.includes('id="' + id + '"') || js.includes('id=' + id + ' '), 'neither the page nor the script makes #' + id);
  assert.ok(html.includes('id="sov-pal"'));
  assert.equal(html.split('/jh-sovereign-desk.js?v=').length, 2);
  for (const view of ['desk', 'crisis', 'world', 'sources']) assert.ok(html.includes('data-view="' + view + '"'));
  assert.ok(!/target=_top/.test(js));                                         // inside the chart's overlay a link must not replace the chart
});

test('the chart has a Sovereign button that opens this desk', () => {
  const engine = read('jh-chart-engine.js'), rail = read('jh-chart-tvrail.js'), chart = read('chart.html');
  assert.equal(engine.split("id=btn-sov ").length, 2);
  const iRepo = engine.indexOf('id=btn-repo'), iSov = engine.indexOf('id=btn-sov'), iNext = engine.indexOf('id=btn-usd');
  assert.ok(iRepo > 0 && iSov > iRepo && iNext > iSov);
  assert.equal(engine.slice(iRepo, iSov).split('<button').length, 2, 'right after Repo: nothing but the Repo button\'s own tail before it');
  assert.ok(engine.includes('var bsov=document.getElementById("btn-sov"); if(bsov) bsov.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("sov"); };'));
  assert.ok(rail.includes('sov: ["Sovereign stress · SovCISS, spreads, TARGET, crisis peaks", "/sovereign-desk.html"],'));
  assert.ok(fs.existsSync(path.join(ROOT, 'sovereign-desk.html')));
  const tags = chart.split('\n').filter(l => /jh-chart-(engine|tvrail)\.js\?/.test(l));
  assert.equal(tags.length, 2);
  for (const t of tags) assert.ok(t.includes('&sov=20261122'), 'a browser must not pair the new button with an old rail: ' + t.slice(0, 60));
});

// ---- the second round, 2026-11-22: more countries, tags, sorting, charts in Crisis peaks ----
test('the United States, China and the United Kingdom have the ECB\'s CISS, back to 1980 for two of them', () => {
  assert.deepEqual(D.world_ciss, ['CISS_US', 'CISS_CN', 'CISS_GB']);
  assert.deepEqual([M.CISS_US.start, M.CISS_GB.start, M.CISS_CN.start], ['1980-01-02', '1980-01-02', '2006-10-10']);
  assert.deepEqual(M.CISS_US.peaks.crash1987.hi, [0.71944, '1987-11-09']);
  assert.deepEqual(M.CISS_US.peaks.gfc2008.hi, [0.89665, '2008-11-20']);
  const b = desk.board(D);
  assert.equal(b.systemic.length, 13);
  assert.ok(['US', 'CN', 'GB'].every(a => b.systemic.some(r => r.area === a)));
  const set = desk.SETS.find(s => s.id === 'ciss'), c = desk.crisis(D, set.ids);
  assert.equal(c.cols.length, 14);
  const row = c.rows.find(r => r.ep.id === 'crash1987'), us = c.cols.findIndex(m => m.id === 'CISS_US'), cn = c.cols.findIndex(m => m.id === 'CISS_CN');
  assert.equal(row.cells[us].v, 0.71944);
  assert.equal(row.cells[cn], null);                                          // China's series begins in 2006
  assert.equal(H.CISS_US.d[0] < '1980-01-10', true);
});

test('the six governments whose SovCISS the ECB stopped keep their history and never pose as current', () => {
  const b = desk.board(D);
  assert.deepEqual(b.ended.map(r => r.area).sort(), ['CZ', 'DK', 'GB', 'HU', 'PL', 'SE']);
  const last = b.ended.map(r => r.m.sovciss_ended.value);
  assert.deepEqual(last, [...last].sort((x, y) => y - x));
  for (const r of b.ended) {
    const m = r.m.sovciss_ended;
    assert.deepEqual([m.status, m.date, m.freq, m.group], ['ended', '2025-04-30', 'M', 'sovciss_ended'], r.area);
    assert.equal(desk.heat(m), 0);
    assert.ok(r.crisis && m.high[0] > 0.5, r.area);
    assert.ok(!b.sovereign.some(x => x.area === r.area));
  }
  assert.equal(H.SOVM_PL.d[H.SOVM_PL.d.length - 1], '2025-04-30');
  assert.equal(Object.values(M).filter(m => m.group === 'sovciss').length, 11);  // the ECB publishes a daily SovCISS for eleven governments and no more
});

test('balance, balance before interest and interest paid hold together, as the ECB publishes them', () => {
  let checked = 0;
  for (const c of D.countries) {
    const d = M[c.ids.deficit], p = M[c.ids.primary], i = M[c.ids.interest];
    if (!d || !p || !i || d.date !== p.date || d.date !== i.date) continue;
    near(d.value, p.value - i.value, 0.03, c.area + ': balance = balance before interest - interest');
    checked++;
  }
  assert.ok(checked >= 25);
  assert.deepEqual([M.DEF_FR.value, M.PRIM_FR.value, M.INT_FR.value, M.DEF_FR.date], [-5.11, -2.7, 2.41, '2026-06-30']);
  assert.equal(desk.when(M.DEF_FR), 'Q2 2026');
  assert.equal(desk.worseUp(M.DEF_FR), null);
});

test('a tagged country moves to the top; inside the tagged and inside the rest the chosen sort holds, both ways', () => {
  const rows = desk.board(D).sovereign, sov = r => r.m.sovciss.value, areas = list => list.map(r => r.area);
  const plain = desk.arrange(rows, { tags: {}, value: sov, dir: -1 });
  assert.deepEqual(areas(plain), D.cross_section.ranking.map(r => r.area));
  assert.deepEqual(areas(desk.arrange(rows, { tags: {}, value: sov, dir: 1 })), [...areas(plain)].reverse());
  const tags = { italy: 'red', portugal: 'amber', spain: 'red' };              // amber is listed before red; Italy's SovCISS is above Spain's
  assert.deepEqual(areas(desk.arrange(rows, { tags, value: sov, dir: -1 })).slice(0, 4), ['PT', 'IT', 'ES', 'FR']);
  assert.deepEqual(areas(desk.arrange(rows, { tags, value: sov, dir: 1 })).slice(0, 4), ['PT', 'ES', 'IT', 'FI']);
  assert.equal(desk.arrange(rows, { tags, value: sov, dir: -1 }).length, 11);
  assert.deepEqual(areas(desk.arrange(rows, { tags, value: sov, dir: -1, show: 'tagged' })), ['PT', 'IT', 'ES']);
  assert.deepEqual(areas(desk.arrange(rows, { tags, value: sov, dir: -1, show: 'red' })), ['IT', 'ES']);
  assert.deepEqual(desk.arrange(rows, { tags, value: sov, dir: -1, show: 'green' }), []);
  // a country with no reading in the sorted column goes last whichever way the sort runs: Greece has no country CISS
  const ciss = r => r.m.ciss ? r.m.ciss.value : null;
  assert.equal(areas(desk.arrange(rows, { tags: {}, value: ciss, dir: -1 })).pop(), 'GR');
  assert.equal(areas(desk.arrange(rows, { tags: {}, value: ciss, dir: 1 })).pop(), 'GR');
  assert.equal(areas(desk.arrange(rows, { tags: {}, value: ciss, dir: 1 }))[0], 'NL');
  assert.equal(areas(desk.arrange(rows, { tags: {}, value: ciss, dir: -1 }))[0], 'FR');
  // by name
  assert.deepEqual(areas(desk.arrange(rows, { tags: {}, value: r => r.country, dir: 1 })).slice(0, 3), ['AT', 'BE', 'FI']);
  assert.deepEqual([desk.tagRank(tags, 'portugal'), desk.tagRank(tags, 'italy'), desk.tagRank(tags, 'france'), desk.tagRank({ france: 'no-such-colour' }, 'france')], [0, 1, -1, -1]);
  assert.equal(desk.TAGS.length, 6);
});

test('a heading sorts high to low first, flips when clicked again, and a name starts A to Z', () => {
  assert.deepEqual(desk.nextSort(['sovciss', -1], 'sovciss', -1), ['sovciss', 1]);
  assert.deepEqual(desk.nextSort(['sovciss', 1], 'sovciss', -1), ['sovciss', -1]);
  assert.deepEqual(desk.nextSort(['sovciss', -1], 'ciss', -1), ['ciss', -1]);
  assert.deepEqual(desk.nextSort(['sovciss', -1], 'country', 1), ['country', 1]);
  assert.deepEqual(desk.nextSort(null, 'cds', -1), ['cds', -1]);
});

test('one key for a country across the ECB\'s names and the world document\'s; CDS and ratings are joined by it', () => {
  assert.equal(desk.ckey(' Czech Republic '), 'czechia');
  assert.equal(desk.ckey('Czechia'), 'czechia');
  assert.equal(desk.ckey('United States'), 'united states');
  const W = desk.joinWorld(FX.world), rows = desk.board(D).sovereign;
  for (const r of rows) assert.ok(W[r.key] && typeof W[r.key].cds === 'number' && W[r.key].rating, r.country + ' has a CDS and a rating in the world document');
  const fr = FX.world.countries.find(c => c.country === 'France');
  assert.deepEqual([W.france.cds, W.france.rating], [fr.cds_bp, fr.rating]);
  assert.ok(W.czechia && !('stress' in W.france));
  assert.deepEqual(desk.joinWorld(null), {});
  assert.ok(desk.world(FX.world).every(r => r.key === desk.ckey(r.country)));
});

test('in a set, tagged countries lead the columns after the set\'s first series', () => {
  const names = Object.fromEntries(D.countries.map(c => [c.area, c.country])), set = desk.SETS[0];
  assert.deepEqual(desk.orderIds(set.ids, M, {}, names), set.ids);
  const got = desk.orderIds(set.ids, M, { greece: 'red', poland: 'amber' }, names);
  assert.deepEqual(got.slice(0, 4), ['EA_SOV_GDP', 'SOVM_PL', 'SOV_GR', 'SOV_DE']);
  assert.equal(got.length, set.ids.length);
  assert.deepEqual(desk.orderIds(['NOPE', 'EA_SOV_GDP'], M, {}, names), ['EA_SOV_GDP']);
  assert.equal(desk.lineColour(3, 'red'), desk.TAGS.find(t => t[0] === 'red')[1]);
  assert.notEqual(desk.lineColour(3, null), desk.lineColour(4, null));
  assert.equal(desk.SETS.length, 4);
});

test('tags and sorting are kept in the browser, and the page offers them', () => {
  const html = read('sovereign-desk.html'), js = read('jh-sovereign-desk.js');
  assert.ok(js.includes('localStorage.getItem(KEY)') && js.includes('localStorage.setItem(KEY'));
  assert.ok(/try \{ var kept = JSON\.parse/.test(js), 'a browser that keeps nothing must still open the desk');
  for (const hook of ['data-tag=', 'data-sort=', 'data-show=', 'data-cleartags', 'data-ep=', 'data-line=', 'data-mini=']) assert.ok(js.includes(hook), hook);
  for (const css of ['.tg{', '#sov-pal{', 'th.sort{', '.legend{', '.minis{']) assert.ok(html.includes(css), css);
});

// ---- the third round, 2026-11-22: a chart on the World tab, all countries or only the tagged ones ----
const WH = FX.world_history;
test('the World tab\'s histories: four sets, a country only where the site holds a history, each line naming its source', () => {
  assert.deepEqual([WH.contract, WH.version], ['sovereign-world.v1', '1.0.0']);
  assert.deepEqual(WH.sets.map(s => s.id), ['yield', 'cds', 'yield_reported', 'cds_reported']);
  assert.equal(WH.countries.length, 44);
  const allowed = { yield: ['ecb', 'oecd'], cds: ['dtcc'], yield_reported: ['reported'], cds_reported: ['reported'] };
  for (const s of WH.sets) {
    const held = WH.countries.filter(c => c.ids[s.id]);
    assert.equal(s.n, held.length, s.id);
    assert.deepEqual(WH.missing[s.id], WH.countries.filter(c => !c.ids[s.id]).map(c => c.country), s.id + ': the missing are named');
    for (const c of held) {
      const m = WH.measures[c.ids[s.id]];
      assert.ok(m && m.set === s.id && m.country === c.country && allowed[s.id].includes(m.source) && m.source_label && m.unit === s.unit, c.country + ' ' + s.id);
      assert.ok(WH.series_ids.includes(m.id) && m.n >= 2);
    }
  }
  assert.deepEqual(WH.sets.map(s => s.n), [30, 25, 44, 38]);
  assert.equal(Object.keys(WH.measures).length, 137);
  assert.equal(WH.reported_since, '2026-09-26');
  // decades of monthly yields from the OECD and the ECB; two years of measured CDS; the table's own figures since late September
  const us = WH.measures.WY_UNITED_STATES, tr = WH.measures.WC_TURKEY, ry = WH.measures.RY_TURKEY;
  assert.deepEqual([us.source, us.start, us.freq, us.key], ['oecd', '1953-04-30', 'M', 'fred:IRLTLT01USM156N']);
  assert.deepEqual(us.high, [15.32, '1981-09-30']);
  assert.deepEqual([WH.measures.WY_ITALY.source, WH.measures.WY_ITALY.start], ['ecb', '1991-03-31']);
  assert.deepEqual([tr.source, tr.start, tr.freq, tr.unit], ['dtcc', '2024-09-03', 'D', 'bp']);
  assert.deepEqual([ry.source, ry.start], ['reported', '2026-09-26']);
  assert.ok(ry.source_label.includes('kept by the site since 26 September 2026'));
  assert.equal(WH.measures.WY_RUSSIA.status, 'ended');                         // its source stopped in 2018; kept for its history
  for (const id of Object.keys(WH.series)) {
    const s = WH.series[id], m = WH.measures[id];
    assert.ok(s.d.length === m.n && s.d[0] === m.start && s.d[s.d.length - 1] === m.date && s.v[s.v.length - 1] === m.value, id);
  }
  assert.ok(!WH.missing.yield.includes('Germany') && WH.missing.yield.includes('Turkey') && WH.missing.cds_reported.includes('Vietnam'));
});

test('on the World chart tagged countries lead in the order of their colours, the rest follow by their latest reading', () => {
  const plain = desk.worldCols(WH, 'yield', {});
  assert.equal(plain.length, 30);
  assert.deepEqual(plain.slice(0, 3).map(c => c.country), ['Mexico', 'South Africa', 'Russia']);
  const values = plain.map(c => c.m.value);
  assert.deepEqual(values, [...values].sort((a, b) => b - a));
  const tags = { turkey: 'red', japan: 'blue', 'united states': 'green' };    // red, green, blue is the order of the palette
  assert.deepEqual(desk.worldCols(WH, 'yield', tags).slice(0, 3).map(c => c.country), ['United States', 'Japan', 'Mexico']);   // Turkey has no monthly yield
  assert.deepEqual(desk.worldCols(WH, 'cds', tags).slice(0, 4).map(c => c.country), ['Turkey', 'United States', 'Japan', 'Colombia']);
  assert.deepEqual(desk.worldCols(WH, 'yield_reported', tags).slice(0, 3).map(c => [c.country, c.id, c.key]), [['Turkey', 'RY_TURKEY', 'turkey'], ['United States', 'RY_UNITED_STATES', 'united states'], ['Japan', 'RY_JAPAN', 'japan']]);
  assert.equal(desk.worldCols(WH, 'yield_reported', {}).length, 44);
  assert.deepEqual(desk.worldCols(WH, 'no-such-set', tags), []);
  assert.deepEqual(desk.worldCols(null, 'yield', tags), []);
  assert.deepEqual(desk.worldCols({ error: 'HTTP 404' }, 'yield', tags), []);
  // the ECB says Czechia, the world table Czech Republic: one key, so a tag made on the Desk reaches the World chart
  assert.equal(desk.worldCols(WH, 'yield', { czechia: 'amber' })[0].country, 'Czech Republic');
  assert.ok(desk.worldIds(WH).czechia.yield === 'WY_CZECHIA' && !('czech republic' in desk.worldIds(WH)));
});

test('"Only tagged" draws the tagged lines and a set\'s leading aggregate; a click on one name flips that line', () => {
  const shown = (mode, tagged, lead, flipped) => desk.lineShown(mode, tagged, lead, flipped);
  assert.deepEqual([shown('all', null, false), shown('all', 'red', false), shown('none', 'red', true)], [true, true, false]);
  assert.deepEqual([shown('tagged', 'red', false), shown('tagged', null, false), shown('tagged', undefined, true)], [true, false, true]);
  assert.deepEqual([shown('tagged', null, false, 1), shown('tagged', 'red', false, 1), shown('all', null, false, 1), shown('none', null, false, 1)], [true, false, false, true]);
  assert.equal(shown('something else', null, false), true);
});

test('a cell of the world table opens the long history when there is one, the table\'s own record otherwise', () => {
  const ids = desk.worldIds(WH);
  assert.equal(desk.worldPick(ids.germany, 'yield', 'yield'), 'WY_GERMANY');
  assert.equal(desk.worldPick(ids.germany, 'yield', 'cds_reported'), 'RY_GERMANY');          // on an "as reported" set, the reported series first
  assert.equal(desk.worldPick(ids.turkey, 'yield', 'yield'), 'RY_TURKEY');                   // no monthly yield is held for Turkey
  assert.equal(desk.worldPick(ids.turkey, 'cds', 'yield'), 'WC_TURKEY');
  assert.equal(desk.worldPick(ids.turkey, 'cds', 'yield_reported'), 'RC_TURKEY');
  assert.equal(desk.worldPick(ids.vietnam, 'cds', 'cds'), null);                             // neither measured nor reported
  assert.equal(desk.worldPick(ids.turkey, 'name', 'yield'), 'WC_TURKEY');                    // the set on screen first, then whatever is held
  assert.equal(desk.worldPick(ids.turkey, 'name', 'cds_reported'), 'RC_TURKEY');
  assert.equal(desk.worldPick(ids.vietnam, 'name', 'cds'), 'RY_VIETNAM');
  assert.equal(desk.worldPick(undefined, 'name', 'yield'), null);
  assert.equal(desk.worldPick({}, 'yield', 'yield'), null);
});

test('days a sovereign did not trade are not joined across a long silence; the ECB\'s and the OECD\'s regular series are', () => {
  assert.equal(desk.GAP, 45);
  assert.equal(desk.gapOf(WH.measures.WC_TURKEY), 45);
  assert.equal(desk.gapOf(WH.measures.RY_TURKEY), 45);
  assert.equal(desk.gapOf(WH.measures.WY_UNITED_STATES), 0);                                  // monthly
  assert.equal(desk.gapOf(M.SOV_FR), 0);                                                      // the desk's own ECB series
  assert.equal(desk.gapOf(null), 0);
  // Thailand's CDS printed once in 2024 and three times in November 2026: a dot and a short line, not a line across two years
  const th = WH.series.WC_THAILAND, day = iso => Date.UTC(+iso.slice(0, 4), +iso.slice(5, 7) - 1, +iso.slice(8, 10)) / 86400000;
  assert.equal(th.d.length, 4);
  assert.ok(day(th.d[1]) - day(th.d[0]) > desk.GAP && day(th.d[3]) - day(th.d[1]) <= desk.GAP);
});

test('the page draws the World chart with the same controls as Crisis peaks and keeps "Only tagged" between visits', () => {
  const html = read('sovereign-desk.html'), js = read('jh-sovereign-desk.js');
  for (const hook of ['data-wset=', '/data/ciss-sovereign-world.json', 'data-olines=', 'data-oyears=', 'id=sov-wmiss', 'function manyHtml(', 'function worldCtx(']) assert.ok(js.includes(hook), hook);
  assert.equal((js.match(/function manyHtml\(/g) || []).length, 1);
  assert.ok(js.includes('manyHtml(setCtx())') && js.includes('manyHtml(ctx)'), 'one description of the chart of many, used by both views');
  assert.ok(/kept\.lines === "tagged"/.test(js) && /prefs\.lines = over\.mode === "tagged"/.test(js));
  assert.ok(js.includes('if (over.mode === "tagged") over.mode = "all"'), 'clearing the last tag must not leave an empty chart');
  assert.ok(html.includes('.gbtn small{'));
  assert.ok(/The World tab's charts draw the histories the site holds/.test(html));
});

// ---- the fourth and fifth rounds, 2026-11-22: a Dollar shortage tab - the ECB's three, then the Federal Reserve's lines beside them ----
// FX.dollar is the document the engine (dollar_shortage 1.1.0) compiled in its dry run (ops 6925), daily series cut to
// their last 400 readings and most weekly ones to 60 (series_n keeps the true counts); FX.dollar_old is the 1.0.0 document.
const DS = FX.dollar, DO = FX.dollar_old, DM = DS.measures;
const ECB3 = ['AUX_EURUSD', 'AUX_FX_EA', 'AUX_FX_NONEA'];
test('the dollar-shortage document keeps the desk\'s three ECB measurements figure for figure', () => {
  assert.deepEqual([DS.contract, DS.version, DS.ecb], ['ciss-dollar-shortage.v1', '1.1.0', ECB3]);
  assert.deepEqual(desk.DOLLAR, ECB3);
  for (const id of ECB3) {
    const m = DM[id], d = M[id];
    for (const k of ['value', 'date', 'n', 'start', 'high', 'low', 'unit', 'freq', 'key', 'label']) assert.deepEqual(m[k], d[k], id + '.' + k + ' is the desk document\'s');
    assert.deepEqual(m.peaks.gfc2008, d.peaks.gfc2008);
    assert.equal(DS.series_n[id], m.n);
    assert.ok(m.what && m.evidence && m.evidence.sha256.length === 64 && m.source === 'ecb');
  }
  assert.deepEqual(ECB3.map(id => DS.series_n[id]), [7141, 1455, 1455]);
  assert.equal(H.AUX_EURUSD, undefined);                                                     // (this fixture carries only a few of the desk's weekly histories)
  assert.deepEqual(DS.sources.filter(x => x.name !== 'fred').map(x => x.path), ['EXR/D.USD.EUR.SP00.A', 'ILM/W.U2.C.A030000.U2.Z06', 'ILM/W.U2.C.A020000.U4.Z06']);
  // the ECB's dollar operations are where its own series has them
  assert.deepEqual(DM.AUX_FX_EA.high, [248.854, '2008-12-05']);
  assert.deepEqual(DM.AUX_FX_EA.tops.slice(0, 4).map(x => x[1].slice(0, 4)), ['2008', '2020', '2009', '2012']);
  const ea = DS.series.AUX_FX_EA;
  assert.equal(Math.max(...ea.v), 248.854);
  assert.equal(ea.d[ea.v.indexOf(248.854)], '2008-12-05');
  // the reserves item is named as what it is, once
  assert.equal(DM.AUX_FX_NONEA.short, 'Eurosystem foreign reserves other than gold');
  assert.equal(DM.AUX_FX_NONEA.label, 'Claims on non-euro-area residents in foreign currency');
  assert.ok(/IMF/.test(DM.AUX_FX_NONEA.what) && /does not measure dollars lent/.test(DM.AUX_FX_NONEA.what) && DM.AUX_FX_NONEA.worse === null && DM.AUX_FX_NONEA.part === 'reserves');
});

test('twelve Federal Reserve measures stand beside them, in five parts, each with its own history', () => {
  assert.deepEqual(DS.parts.map(p => [p.id, p.ids]), [
    ['backstop', ['FED_SWAPS', 'FED_FIMA', 'AUX_FX_EA']],
    ['price', ['USD_DXY', 'USD_BROAD', 'USD_AFE', 'USD_EME', 'AUX_EURUSD']],
    ['official', ['FED_CUSTODY_13W', 'FED_CUSTODY_UST', 'FED_FOREIGN_RRP']],
    ['funding', ['USD_SOFR_IOR', 'USD_TED', 'H8_NET_DUE']],
    ['reserves', ['AUX_FX_NONEA']]]);
  assert.deepEqual(DS.order, DS.parts.flatMap(p => p.ids));
  assert.deepEqual([Object.keys(DM).length, DS.coverage.fed, DS.coverage.ecb, DS.coverage.status, DS.errors], [15, 12, 3, 'measured', {}]);
  for (const id of DS.order) {
    const m = DM[id], x = DS.series[id];
    assert.equal(DS.series_n[id], m.n, id);
    assert.deepEqual([x.d[x.d.length - 1], x.v[x.v.length - 1]], [m.date, m.value], id + ': the series ends at the measure');
    assert.ok(m.short && m.what && m.part && ['higher', 'lower', null].includes(m.worse), id);
    if (m.source === 'fred') assert.ok(m.series.length && m.source_label && m.read_at && m.key === m.series.map(k => 'fred:' + k).join(', '), id);
  }
  // each is where the Federal Reserve's own record has it
  assert.deepEqual(DM.FED_SWAPS.high, [583.135, '2008-12-17']);
  assert.deepEqual(DM.FED_SWAPS.tops.slice(0, 4).map(x => x[1].slice(0, 7)), ['2008-12', '2020-05', '2009-04', '2012-02']);     // the same four episodes as the ECB's lending
  assert.deepEqual(DM.AUX_FX_EA.tops.slice(0, 4).map(x => x[1].slice(0, 7)), ['2008-12', '2020-05', '2009-04', '2012-02']);
  assert.deepEqual([DM.FED_SWAPS.start, DM.FED_FIMA.start, DM.FED_CUSTODY_UST.start, DM.FED_CUSTODY_13W.start], ['2007-12-26', '2020-04-08', '2007-07-04', '2007-10-03']);   // no placeholder zeros
  assert.deepEqual(DM.FED_FIMA.high, [60, '2023-03-22']);
  assert.deepEqual(DM.USD_TED.high, [458, '2008-10-10']);
  assert.deepEqual([DM.USD_TED.status, DM.USD_TED.date], ['ended', '2022-01-21']);
  assert.deepEqual(DM.USD_SOFR_IOR.high, [315, '2019-09-17']);
  assert.deepEqual(DM.H8_NET_DUE.low, [-615.423, '2008-05-28']);
  assert.deepEqual(DM.FED_CUSTODY_13W.low, [-163.729, '2014-03-12']);
  assert.ok(DM.FED_CUSTODY_13W.peaks.covid2020.lo[0] < -100);
  assert.deepEqual([DM.FED_CUSTODY_UST.worse, DM.FED_CUSTODY_13W.worse, DM.FED_FOREIGN_RRP.worse, DM.H8_NET_DUE.worse], [null, 'lower', null, null]);
  assert.ok(DM.USD_DXY.low[0] > 70 && DM.USD_DXY.low[0] < 73 && DM.USD_DXY.low[1].startsWith('2008') && DM.USD_DXY.high[0] > 120 && DM.USD_DXY.high[1].startsWith('2001'));
  for (const id of DS.order) if (id !== 'USD_TED') assert.equal(DM[id].status, 'current', id);
  // the 13-week change is the level less the level 91 days earlier
  const c13 = DS.series.FED_CUSTODY_13W, whole = Object.fromEntries(c13.d.map((d, i) => [d, c13.v[i]])), lv = DS.series.FED_CUSTODY_UST, last = lv.d.length - 1;
  near(whole[lv.d[last]], lv.v[last] - lv.v[last - 13], 0.0015, 'the 13-week change');
});

test('amounts are written to the precision they are read at', () => {
  assert.equal(desk.fmt('usd_bn', 2544.011), '$2,544.0 bn');
  assert.equal(desk.fmt('usd_bn', 0.146), '$0.146 bn');                                       // a facility that is hardly used is read to the million
  assert.equal(desk.fmt('usd_bn', 0.146, true), '$0.1 bn');                                   // but not on an axis that spans hundreds
  assert.equal(desk.fmt('usd_bn', 0), '$0.0 bn');
  assert.equal(desk.fmt('usd_bn', -615.423), '−$615.4 bn');
  assert.equal(desk.fmt('idx', 103.3578), '103.36');
  assert.equal(desk.fmtChange('usd_bn', -0.025), '−0.025 bn');
  assert.equal(desk.fmtChange('usd_bn', -6.878, true), '−6.9 bn');                            // a change of a level in the thousands
  assert.equal(desk.fmtChange('idx', 1.2083), '+1.21');
  assert.equal(desk.fmt('eur_bn', 0.146), '€0.1 bn');                                         // the desk's own unit is untouched
});

test('the Dollar shortage tab reads the document in its parts, the old document as three, and the desk\'s own until either arrives', () => {
  const full = desk.dollarData(DS, D, { series: {} });
  assert.deepEqual([full.full, full.named, full.ids, full.episodes.length, full.errors], [true, true, DS.order, 22, {}]);
  assert.deepEqual(full.parts.map(p => [p.id, p.ids.length]), [['backstop', 3], ['price', 5], ['official', 3], ['funding', 3], ['reserves', 1]]);
  assert.equal(full.measures.FED_SWAPS, DM.FED_SWAPS);
  assert.equal(full.series.AUX_EURUSD, DS.series.AUX_EURUSD);
  // the document from before it named its parts: the three, under the names the page now uses
  const old = desk.dollarData(DO, D, null);
  assert.deepEqual([DO.version, old.full, old.named, old.ids, old.parts.length, old.parts[0].id], ['1.0.0', true, false, ECB3, 1, 'ecb']);
  assert.deepEqual(ECB3.map(id => [old.measures[id].short, old.measures[id].worse]), [['EUR/USD', 'lower'], ['Eurosystem foreign-currency lending to euro-area banks', 'higher'], ['Eurosystem foreign reserves other than gold', null]]);
  assert.equal(DO.measures.AUX_FX_NONEA.short, 'Foreign-currency claims abroad');             // the fixture is untouched by that
  for (const missing of [null, { loading: true }, { error: 'HTTP 404' }, { contract: 'something-else', measures: {}, series: {} }]) {
    const back = desk.dollarData(missing, D, { series: { AUX_EURUSD: { d: ['2026-11-20'], v: [1.099] } } });
    assert.deepEqual([back.full, back.named, back.ids], [false, false, ECB3]);
    assert.equal(back.measures.AUX_FX_EA.value, M.AUX_FX_EA.value);
    assert.equal(back.measures.AUX_FX_NONEA.short, 'Eurosystem foreign reserves other than gold');
    assert.deepEqual(back.series.AUX_EURUSD.v, [1.099]);
  }
  assert.deepEqual(desk.dollarData(null, { measures: {} }, null).ids, []);
  assert.deepEqual(desk.dollarData(null, null, null).ids, []);
  // a part whose measures are all missing is not shown; a missing measure costs only itself
  const thin = desk.dollarData({ ...DS, measures: { AUX_FX_EA: DM.AUX_FX_EA, USD_DXY: DM.USD_DXY } }, D, null);
  assert.deepEqual(thin.parts.map(p => [p.id, p.ids]), [['backstop', ['AUX_FX_EA']], ['price', ['USD_DXY']]]);
});

test('each measure says which way a shortage moves it, and how far it stands that way', () => {
  assert.deepEqual(DS.order.map(id => desk.dollarSide(DM[id])), ['hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'lo', 'lo', null, null, 'hi', 'hi', null, null]);
  assert.equal(desk.dollarSide(null), null);
  assert.equal(desk.dollarName(DM.USD_DXY), 'DXY, rebuilt from the Fed\'s H.10 rates');
  assert.equal(desk.dollarName({ label: 'A label' }), 'A label');
  // a lending line by its share of its record: $0.146 bn against $583.135 bn, not "higher than most of its idle weeks"
  near(desk.dollarPlace(DM.FED_SWAPS), 0.146 / 583.135 * 100, 1e-9, 'swap lines');
  assert.equal(desk.dollarPlaceText(DM.FED_SWAPS), 'under 0.1% of its record');
  assert.equal(desk.dollarPlaceText(DM.AUX_FX_EA), '8.1% of its record');
  assert.deepEqual(['FED_SWAPS', 'FED_FIMA', 'AUX_FX_EA'].map(id => desk.dollarHeat(DM[id])), [0, 0, 0]);
  assert.deepEqual([40, 10, 26, 55].map(pc => desk.dollarHeat({ ...DM.FED_SWAPS, value: DM.FED_SWAPS.high[0] * pc / 100 })), [3, 2, 3, 4]);
  assert.equal(desk.dollarPlaceText({ ...DM.FED_SWAPS, value: 233.254 }), '40% of its record');
  // every other measure by the share of its own readings it stands beyond, in the direction of a shortage
  assert.equal(desk.dollarPlace(DM.USD_DXY), DM.USD_DXY.pctile);
  assert.equal(desk.dollarPlace(DM.AUX_EURUSD), 100 - DM.AUX_EURUSD.pctile);
  assert.equal(desk.dollarPlaceText(DM.AUX_EURUSD), 'lower than ' + Math.floor(100 - DM.AUX_EURUSD.pctile) + '% of its readings');
  assert.equal(desk.dollarPlaceText(DM.USD_DXY), 'higher than ' + Math.floor(DM.USD_DXY.pctile) + '% of its readings');
  assert.equal(desk.dollarHeat(DM.USD_DXY), DM.USD_DXY.pctile >= 95 ? 4 : DM.USD_DXY.pctile >= 85 ? 3 : DM.USD_DXY.pctile >= 70 ? 2 : DM.USD_DXY.pctile >= 50 ? 1 : 0);
  // no side, no place; an ended series has none either
  for (const id of ['FED_CUSTODY_UST', 'FED_FOREIGN_RRP', 'H8_NET_DUE', 'AUX_FX_NONEA', 'USD_TED']) assert.deepEqual([desk.dollarPlace(DM[id]), desk.dollarHeat(DM[id])], [null, 0], id);
  assert.equal(desk.dollarPlaceText(DM.USD_TED), 'ended at its source');
  assert.equal(desk.dollarPlaceText(DM.AUX_FX_NONEA), desk.standing(DM.AUX_FX_NONEA).replace(/ since \d{4}$/, ''));
  // the old document's lending line has no part: it is placed by its readings, as before
  assert.equal(desk.dollarPlace(desk.dollarData(DO, D, null).measures.AUX_FX_EA), DO.measures.AUX_FX_EA.pctile);
});

test('the Dollar shortage tab opens in words made of the measures\' own numbers', () => {
  const lines = desk.dollarRead(desk.dollarData(DS, D, null)), lend = DM.AUX_FX_EA, eur = DM.AUX_EURUSD, res = DM.AUX_FX_NONEA;
  assert.equal(lines.length, 6);
  assert.ok(lines[0].startsWith('Dollars lent by central banks. The Federal Reserve\'s swap lines are drawn for $0.146 bn in the week to 18 Nov 2026 ('), lines[0]);
  assert.ok(lines[0].includes('They have stood above $100 bn at 4 separate peaks: $583.1 bn on 17 Dec 2008, $448.9 bn on 27 May 2020, $282.9 bn on 22 Apr 2009, $109.1 bn on 15 Feb 2012.'), lines[0]);
  assert.ok(lines[0].includes('Its repos with foreign central banks stand at $0.001 bn; their record is $60.0 bn on 22 Mar 2023.'), lines[0]);
  assert.ok(lines[0].includes('The Eurosystem\'s foreign-currency lending to euro-area banks is ' + desk.fmt('eur_bn', lend.value) + ' in the week to ' + desk.fmtDate(lend.date) + ' — ' + desk.standing(lend)), lines[0]);
  assert.ok(lines[0].endsWith('It has stood above €100 bn at 4 separate peaks: €248.9 bn on 5 Dec 2008, €153.8 bn on 8 May 2020, €142.1 bn on 10 Apr 2009, €100.6 bn on 10 Feb 2012.'), lines[0]);
  assert.ok(lines[1].startsWith('The price of the dollar. DXY, rebuilt from the Federal Reserve\'s H.10 rates, is ' + desk.fmt('idx', DM.USD_DXY.value) + ' on ' + desk.fmtDate(DM.USD_DXY.date) + ': '), lines[1]);
  assert.ok(lines[1].includes('EUR/USD, the ECB\'s reference rate, is ' + desk.fmt('index', eur.value) + ' on ' + desk.fmtDate(eur.date) + ': ') && lines[1].includes('highest 1.5990 on 15 Jul 2008, lowest 0.8252 on 26 Oct 2000'), lines[1]);
  assert.ok(lines[2].startsWith('Foreign official dollars. Foreign central banks keep ' + desk.fmt('usd_bn', DM.FED_CUSTODY_UST.value) + ' of Treasuries in custody at the Federal Reserve'), lines[2]);
  assert.ok(lines[2].includes('Over 13 weeks that is ' + desk.fmtChange('usd_bn', DM.FED_CUSTODY_13W.value, true) + '; the largest 13-week fall on record is −$163.7 bn in the week to 12 Mar 2014.'), lines[2]);
  assert.ok(lines[3].startsWith('Banks\' dollar funding. SOFR is ') && lines[3].includes('the record is 315.0 bp on 17 Sep 2019.') && lines[3].includes('the lowest reading is −$615.4 bn on 28 May 2008.')
    && lines[3].endsWith('The TED spread ended on 21 Jan 2022; its record is 458.0 bp on 10 Oct 2008.'), lines[3]);
  assert.ok(lines[4].startsWith('The Eurosystem\'s foreign reserves other than gold are ' + desk.fmt('eur_bn', res.value)) && lines[4].includes(desk.standing(res)) && lines[4].endsWith('It is not a gauge of dollars lent.'), lines[4]);
  assert.ok(lines[5].startsWith('Not here: a cross-currency basis'));
  assert.ok(!lines.join(' ').match(/NaN|undefined|null/));
  // SOFR under the rate on reserves is said so; a US office that is owed says so
  const under = desk.dollarRead({ measures: { USD_SOFR_IOR: { ...DM.USD_SOFR_IOR, value: -4 }, H8_NET_DUE: { ...DM.H8_NET_DUE, value: -590.194 } } });
  assert.ok(under[0].includes('SOFR is 4.0 bp below the rate') && under[0].includes('are owed a net $590.2 bn by their offices abroad'), under[0]);
  // the old document: the three lines as before, nothing claimed about what is not there
  const old = desk.dollarRead(desk.dollarData(DO, D, null));
  assert.equal(old.length, 3);
  assert.ok(old[0].startsWith('Foreign-currency lending to euro-area banks is ') && old[1].startsWith('EUR/USD, the ECB\'s reference rate, is ') && old[2].startsWith('The Eurosystem\'s foreign reserves other than gold are '), old.join(' | '));
  // a series that never stood above 100 says nothing about peaks; a missing measure costs only its line
  const quiet = { ...lend, tops: [[61.288, '2009-08-14']] };
  assert.ok(!desk.dollarRead({ measures: { AUX_FX_EA: quiet } })[0].includes('stood above'));
  assert.equal(desk.dollarRead({ measures: { AUX_EURUSD: eur } }).length, 1);
});

test('inside each named window: each measure a shortage shows in, at the end it shows in', () => {
  const w = desk.dollarWindows(desk.dollarData(DS, D, null));
  assert.deepEqual(w.cols.map(c => [c.m.id, c.end]), [['FED_SWAPS', 'hi'], ['FED_FIMA', 'hi'], ['AUX_FX_EA', 'hi'], ['USD_DXY', 'hi'], ['USD_BROAD', 'hi'], ['USD_AFE', 'hi'], ['USD_EME', 'hi'],
    ['AUX_EURUSD', 'lo'], ['FED_CUSTODY_13W', 'lo'], ['USD_SOFR_IOR', 'hi'], ['USD_TED', 'hi']]);
  assert.deepEqual(w.groups.map(g => [g.id, g.span]), [['backstop', 3], ['price', 5], ['official', 1], ['funding', 2]]);          // the reserves part has no column
  assert.deepEqual(w.left.map(m => m.id), ['FED_CUSTODY_UST', 'FED_FOREIGN_RRP', 'H8_NET_DUE', 'AUX_FX_NONEA']);
  assert.equal(w.groups.reduce((n, g) => n + g.span, 0), w.cols.length);
  for (const id of w.cols.map(c => c.m.id)) assert.ok(desk.DOLLAR_COL[id], id + ' has a column heading');
  assert.equal(w.rows.length, 19);                                                           // nothing here begins before January 1999: no reading in 1987, 1992 or 1998
  assert.equal(w.rows[0].ep.id, 'sept2001');
  const row = id => w.rows.find(r => r.ep.id === id), col = id => w.cols.findIndex(c => c.m.id === id);
  assert.deepEqual(row('gfc2008').cells[col('FED_SWAPS')], [583.135, '2008-12-17']);
  assert.deepEqual(row('gfc2008').cells[col('AUX_FX_EA')], [248.854, '2008-12-05']);
  assert.deepEqual(row('gfc2008').cells[col('AUX_EURUSD')], [1.246, '2008-10-27']);
  assert.deepEqual(row('gfc2008').cells[col('USD_TED')], [458, '2008-10-10']);
  assert.equal(row('gfc2008').cells[col('FED_FIMA')], null);                                 // the line did not exist yet
  assert.deepEqual(row('covid2020').cells[col('AUX_FX_EA')], [150.104, '2020-04-24']);
  assert.equal(row('covid2020').cells[col('FED_SWAPS')][0], 438.953);
  assert.equal(row('covid2020').cells[col('FED_CUSTODY_13W')][0], DM.FED_CUSTODY_13W.peaks.covid2020.lo[0]);
  assert.deepEqual(row('banks2023').cells[col('FED_FIMA')], [60, '2023-03-22']);
  assert.deepEqual(row('gilts2022').cells[col('AUX_EURUSD')], [0.9565, '2022-09-28']);       // the euro below parity
  assert.deepEqual(w.record[col('AUX_FX_EA')], DM.AUX_FX_EA.high);
  assert.deepEqual(w.record[col('AUX_EURUSD')], DM.AUX_EURUSD.low);
  // how dark a cell is: how far toward the measure's own record in that direction
  assert.equal(desk.dollarShare(DM.FED_SWAPS, 'hi', 583.135), 1);
  assert.equal(desk.dollarShare(DM.FED_SWAPS, 'hi', DM.FED_SWAPS.low[0]), 0);
  assert.equal(desk.dollarShare(DM.AUX_EURUSD, 'lo', 0.8252), 1);
  near(desk.dollarShare(DM.AUX_EURUSD, 'lo', 1.2121), (1.599 - 1.2121) / (1.599 - 0.8252), 1e-9, 'the euro half way');
  assert.equal(desk.dollarShare(DM.FED_SWAPS, 'hi', null), 0);
  // a change or a spread is counted from zero: a rise in custody is no part of the way to its record fall
  near(desk.dollarShare(DM.FED_CUSTODY_13W, 'lo', -33.5), 33.5 / 163.729, 1e-9, 'a fall of a fifth of the record');
  assert.equal(desk.dollarShare(DM.FED_CUSTODY_13W, 'lo', 17.2), 0);
  assert.equal(desk.dollarShare(DM.FED_CUSTODY_13W, 'lo', -163.729), 1);
  near(desk.dollarShare(DM.USD_SOFR_IOR, 'hi', 44), 44 / 315, 1e-9, 'SOFR over the rate on reserves in March 2020');
  assert.equal(desk.dollarShare(DM.USD_SOFR_IOR, 'hi', -10), 0);
  // the old document: the euro's low and lending's high
  const old = desk.dollarWindows(desk.dollarData(DO, D, null));
  assert.deepEqual([old.cols.map(c => [c.m.id, c.end]), old.left.map(m => m.id), old.rows.length], [[['AUX_EURUSD', 'lo'], ['AUX_FX_EA', 'hi']], ['AUX_FX_NONEA'], 19]);
});

test('a Federal Reserve daily series is drawn as one line; the world\'s sparse prints are not joined across their gaps', () => {
  assert.equal(desk.gapOf(DM.USD_DXY), 0);
  assert.equal(desk.gapOf({ source: 'dtcc', freq: 'D' }), desk.GAP);
  assert.equal(desk.gapOf({ source: 'ecb', freq: 'M' }), 0);
});

test('the page has the Dollar shortage tab, and the Dollar desk carries its lines from the same document', () => {
  const html = read('sovereign-desk.html'), js = read('jh-sovereign-desk.js'), pro = read('chart-pro.html'), bundle = read('assets/jh-workspaces.js');
  assert.ok(/<button class="tab" data-view="dollar">Dollar shortage<\/button>/.test(html));
  assert.ok(html.indexOf('data-view="world"') < html.indexOf('data-view="dollar"') && html.indexOf('data-view="dollar"') < html.indexOf('data-view="sources"'));
  for (const hook of ['/data/ciss-dollar-shortage.json', 'data-dchart=', 'data-dyears=', 'data-dtip=', 'function renderDollar(', 'VIEWS.indexOf(tab) >= 0', 'if (floor >= 0 && lo < 0) lo = 0', 'id=sov-dboard', 'id=sov-dwin', 'id=sov-dread',
    'id=sov-dsrc', 'class=dpart', 'https://fred.stlouisfed.org/series/', 'wanted = want; needDollar();']) assert.ok(js.includes(hook), hook);
  for (const css of ['.dcharts{', '.dcv{', 'table.dwin th.mk', 'table.dboard tr.grp td', '.dpart{', '.dph{']) assert.ok(html.includes(css), css);
  assert.ok(html.includes('A cross-currency basis is not shown: no free official source publishes one.'));
  assert.ok(!js.includes('Foreign-currency claims abroad') && !pro.includes('foreign-currency claims abroad'), 'the reserves item has one name');
  const family = ['FED_SWAPS', 'FED_FIMA', 'AUX_FX_EA', 'AUX_EURUSD', 'FED_CUSTODY_UST', 'FED_FOREIGN_RRP', 'USD_TED', 'AUX_FX_NONEA'];
  for (const text of [pro, bundle]) {
    assert.ok(text.includes('"ecb-dollar": { url: "/data/ciss-dollar-shortage.json"'), 'the Dollar desk reads the same document');
    assert.ok(text.includes('"/sovereign-desk.html?view=dollar&m=" + encodeURIComponent(field)'), 'a card opens its chart on the Sovereign desk');
    for (const id of family) assert.ok(text.includes('["flows:ecb-dollar/' + id + '",'), id + ' is on the Dollar desk');
    for (const id of ['FED_CUSTODY_13W', 'USD_SOFR_IOR', 'H8_NET_DUE', 'USD_DXY']) assert.ok(!text.includes('flows:ecb-dollar/' + id), id + ' is not a card: a percentage change of it would mislead, or the desk has it natively');
    assert.ok(text.includes('{ cat: "Dollar shortage · central-bank lending, official dollars and the euro side"'));
    assert.ok(text.includes('["flows:ecb-dollar/AUX_FX_NONEA","Eurosystem foreign reserves other than gold · weekly","EUR bn"]'));
  }
  for (const id of family) assert.ok(DM[id] && DS.series[id], id + ' is in the document the cards read');
  assert.ok(/static feedChart\(symbol\)/.test(pro) && pro.includes('if (this.calcSpec(symbol) || this.feedChart(symbol)) return this.chartSeries(symbol);'));
});

// ---- the CISS page's repairs, on rows of the live warehouse ----
const W = FX.warehouse, row = key => W.series.find(r => r.key === key);

test('the CISS headline is a readable number with its place in history; the exact decimal is kept beside it', () => {
  const h = ciss.headline(row('CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX'), W, NOW);
  assert.equal(h.text, 'ECB euro-area CISS: 0.0095 on 2026-11-19 — lower than 86% of its readings since 1980.');
  assert.ok(h.exact.startsWith('Exact source value 0.009462216765937262'));
  assert.equal(/\d\.\d{6,}/.test(h.text), false);
  const stale = ciss.headline(row('CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX'), W, NOW + 30 * 86400000);
  assert.deepEqual([stale.current, stale.exact], [false, '']);
  assert.equal(ciss.headline(undefined, W, NOW).current, false);
});

test('the reconciliation is said in words', () => {
  const r = ciss.reconciliation(W.headline_reconciliation);
  assert.equal(r.text, 'Matched on 2026-11-19 — the six contributions add up to the composite.');
  assert.equal(r.detail, 'difference 1.25E-17 (tolerance 0.0000000001)');
  assert.ok(ciss.reconciliation({ status: 'mismatch', date: '2026-11-19', residual_decimal: '0.01', tolerance_decimal: '0.0000000001' }).text.startsWith('MISMATCH'));
  assert.equal(ciss.reconciliation({ status: 'unavailable' }).text, 'Not available for this run.');
});

test('a row the ECB printed blank, and a series it stopped, say so and keep their last value', () => {
  const life = key => ciss.lifecycle(row(key), W, NOW);
  assert.equal(life('CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX'), 'fresh');
  assert.equal(life('CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX'), 'blank');             // the 2026-11-20 row has no value; 2026-11-19 has
  assert.equal(life('CISS.M.FR.Z0Z.4F.EC.SOV_CI.IDX'), 'ended');             // the legacy monthly methodology, last row 2025-04
  assert.equal(life('CISS.D.US.Z0Z.4F.EC.SS_CI.IDX'), 'ended');
  assert.equal(life('CLIFS.M.EE._Z.4F.EC.CLIFS_CI.IDX'), 'ended');           // 2010-12
  assert.equal(life('CLIFS.M.AT._Z.4F.EC.CLIFS_CI.IDX'), 'fresh');
  const de = row('CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX');
  assert.deepEqual([de.latest, de.latest_date, ciss.tidy(de.last_observed_value), de.last_observed_date], [null, '2026-11-20', '0.0080', '2026-11-19']);
  assert.deepEqual(W.counts.by_status, { fresh: 58, stale: 28, missing: 2 });
  // a daily series a month late has not ended
  const late = { ...row('CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX'), quality: { ...row('CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX').quality, status: 'stale' } };
  assert.equal(ciss.lifecycle(late, W, NOW), 'stale');
  for (const k of ['fresh', 'blank', 'ended', 'stale']) assert.ok(ciss.LIFE[k]);
});

test('a value is shown to the four decimals the ECB publishes', () => {
  assert.equal(ciss.tidy(0.009462216765937262), '0.0095');
  assert.equal(ciss.tidy(0.17391102315801982), '0.1739');
  assert.equal(ciss.tidy(0), '0.0000');
  assert.equal(ciss.tidy(null), '—');
  assert.equal(ciss.tidy(-0.10758), '-0.1076');
  assert.equal(ciss.tidy(1.26e-17), '1.3 × 10⁻¹⁷');
});

test('an auxiliary is checked against the ECB response it was read from: same bytes, same row, same value', async () => {
  const m = M.AUX_EURUSD, source = m.evidence.source_url;
  const hash = b => createHash('sha256').update(b).digest('hex');
  const filler = Array.from({ length: m.source_row }, (_, i) => `${m.key},1999-01-${String(i % 28 + 1).padStart(2, '0')},1.1,A,USD,0,x`).join('\n');
  const raw = Buffer.from('KEY,TIME_PERIOD,OBS_VALUE,OBS_STATUS,UNIT,UNIT_MULT,TITLE\n' + filler + `\n${m.key},${m.period},${m.exact},A,USD,0,"ECB reference exchange rate, US dollar/Euro"\n`);
  const evidence = { ...m.evidence, sha256: hash(raw), bytes: raw.length, key: 'data/evidence/ecb/' + hash(source) + '/' + hash(raw) + '.bin.gz' };
  assert.equal(m.evidence.key.split('/')[3], hash(source));                   // the real receipt is filed under its request
  const mine = { ...m, evidence }, fetcher = async () => new Response(zlib.gzipSync(raw));
  const ok = await ciss.verifiedMeasure(mine, fetcher);
  assert.deepEqual([ok.source_row, ok.original.TIME_PERIOD, ok.original.OBS_VALUE], [m.source_row, m.period, m.exact]);
  await assert.rejects(ciss.verifiedMeasure({ ...mine, exact: '1.2000' }, fetcher), /row differs/);
  await assert.rejects(ciss.verifiedMeasure(mine, async () => new Response(zlib.gzipSync(Buffer.from('other')))), /hash differs/);
  await assert.rejects(ciss.verifiedMeasure({ ...mine, evidence: { ...evidence, key: 'data/evidence/ecb/x/y.bin.gz' } }, fetcher), /identity differs/);
  await assert.rejects(ciss.verifiedMeasure({ ...m, evidence: null }, fetcher), /receipt unavailable/);
});

test('the CISS page has the new places for the exact value and the reconciliation detail, and reads the auxiliaries with their originals', () => {
  const html = read('ciss.html'), js = read('jh-ciss-research.js');
  for (const id of ['ciss-exact', 'ciss-reconcile-detail', 'ciss-aux', 'ciss-read', 'ciss-reconcile']) assert.ok(html.includes('id="' + id + '"'), id);
  assert.ok(html.includes('href="/sovereign-desk.html"'));
  assert.ok(html.includes('<option value="ended">') && html.includes('<option value="blank">'));
  assert.ok(js.includes("fetch('/data/ciss-sovereign.json'"));
  assert.equal(js.includes('replay for this auxiliary is pending'), false);
  assert.equal(js.includes('measured ${head.latest_decimal}'), false);
  assert.deepEqual(D.auxiliaries, ['AUX_EURUSD', 'AUX_FX_EA', 'AUX_FX_NONEA']);
  for (const id of D.auxiliaries) assert.ok(M[id].evidence.captured === true && Number.isInteger(M[id].source_row) && M[id].exact, id);
});
