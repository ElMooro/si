'use strict';
// The Sovereign stress desk (sovereign-desk.html, jh-sovereign-desk.js), its chart button, and the CISS page's repairs -
// on real numbers: the documents the CISS engine compiled in its dry run of 2026-11-22 (ops 6913) and rows of the live
// warehouse (ops 6912), cut into tests/fixtures/sovereign-desk.json.gz.
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
  assert.equal(Object.keys(M).length, 176);
  assert.equal(D.countries.length, 30);
  assert.equal(D.episodes.length, 16);
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
  assert.ok(b.others.every(r => !r.m.sovciss));
  const spreads = b.others.map(r => r.m.spread ? r.m.spread.value : -Infinity);
  assert.deepEqual(spreads, [...spreads].sort((x, y) => y - x));
  assert.equal(b.systemic.length, 13);
  assert.equal(desk.commonWhen(b.sovereign, 'yield'), 'Oct 2026');
  assert.equal(desk.commonWhen(b.sovereign, 'target'), 'Sep 2026');
});

test('the opening lines are the document\'s own numbers', () => {
  const lines = desk.read(D), rank = D.cross_section.ranking;
  assert.equal(lines.length, 4);
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
  assert.equal(c.cols.length, 12);
  assert.equal(c.rows.length, 16);
  const row = c.rows.find(r => r.ep.id === 'italy2011'), italy = c.cols.findIndex(m => m.id === 'SOV_IT');
  assert.deepEqual([row.cells[italy].v, row.cells[italy].d], M.SOV_IT.peaks.italy2011.hi);
  assert.equal(row.cells[italy].v, 0.99032);
  assert.equal(c.rows.find(r => r.ep.id === 'greece2015').cells[c.cols.findIndex(m => m.id === 'SOV_GR')].v, 0.92855);
  for (const r of c.rows) for (const x of r.cells) assert.ok(x && x.share >= 0 && x.share <= 1);
  assert.ok(c.record.every(x => x.share === 1));
  near(c.now[c.cols.findIndex(m => m.id === 'SOV_FR')].share, M.SOV_FR.value / M.SOV_FR.high[0], 1e-9, 'France now against its record');
  // a central bank that owes on TARGET is read at its lowest, one that is owed at its highest
  assert.equal(desk.side(M.TGT_IT), 'lo');
  assert.equal(desk.side(M.TGT_DE), 'hi');
  const euro = desk.crisis(D, desk.SETS[2].ids), it = euro.cols.findIndex(m => m.id === 'TGT_IT'), de = euro.cols.findIndex(m => m.id === 'TGT_DE');
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
  assert.equal(desk.bands(D.episodes, '2000-01-01', '2026-11-19').length, 16);
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
  assert.ok(used.size >= 9);
  for (const id of used) assert.ok(html.includes('id="' + id + '"'), 'sovereign-desk.html lacks #' + id);
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
