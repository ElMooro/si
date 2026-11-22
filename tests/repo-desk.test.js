// The Repo desk (repo.html + jh-repo-desk.js) against real numbers cut from the live board (ops 6897, engine 3.0, the
// board as rebuilt on 2026-11-21 after the OFR bank was refreshed) and the desk feed of the same day.
// Khalid 2026-11-21: "display an icon for it in the chart.html make sure it cover all the different aspect of the repo
// market and repo rate including their spikes in crisis".
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const zlib = require('node:zlib');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const SRC = fs.readFileSync(path.join(ROOT, 'jh-repo-desk.js'), 'utf8');
const HTML = fs.readFileSync(path.join(ROOT, 'repo.html'), 'utf8');
const FX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures/repo-desk.json.gz'))).toString('utf8'));

// The page's own arithmetic, loaded the way a browser cannot: the file also exports itself to node.
const D = require(path.join(ROOT, 'jh-repo-desk.js'));
const H = (id) => FX.history[id];
const near = (a, b, eps, msg) => assert.ok(Math.abs(a - b) <= (eps || 1e-6), `${msg || ''} ${a} vs ${b}`);
const naiveMedian = (list) => { const a = list.filter((v) => typeof v === 'number' && isFinite(v)).sort((x, y) => x - y); const m = a.length >> 1; return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2; };

test('the fixture is the rebuilt board: engine 3.1, 1,803 series, the venue data current', () => {
  assert.equal(FX.board.engine_v, '3.1');
  assert.equal(FX.board_ids.length, 1803);
  assert.equal(FX.board.groups.length, 21);
  assert.ok(FX.rows['FNYR-SOFR-A'].last >= '2026-11-19' && FX.rows['REPO-TRIV1_AR_OO-P'].last >= '2026-11-18', 'the OFR-fed rows are past their August dates');
  assert.deepEqual(FX.board.groups.filter((g) => /Money market funds|securities borrowed/.test(g.name)).map((g) => [g.name, g.n]),
    [['Money market funds (OFR)', 42], ['Primary dealer securities borrowed & lent', 85]]);
});

test('every series a tile or a crisis measure names is on the real board', () => {
  const ids = new Set(FX.board_ids), named = [];
  D.SECTIONS.forEach((s) => s.tiles.forEach((t) => { if (t.id) named.push(t.id); }));
  D.MEASURES.forEach((m) => named.push(m.id));
  assert.ok(named.length >= 75);
  assert.deepEqual(named.filter((id) => !ids.has(id)), []);
  assert.deepEqual(named.filter((id) => !FX.rows[id]), [], 'and the fixture carries each of their rows');
  assert.equal(new Set(D.SECTIONS.map((s) => s.key)).size, D.SECTIONS.length);
  assert.deepEqual(D.SECTIONS.map((s) => s.key), ['rates', 'spreads', 'venues', 'lenders', 'dealers', 'fed', 'collateral', 'europe']);
  D.SECTIONS.forEach((s) => s.tiles.forEach((t) => {
    assert.ok(!!t.id !== !!t.feed, `${t.label}: one source`);
    assert.ok(['rate', 'bp', 'pct', 'usd', 'usdm', 'usdb', 'num'].includes(t.kind) && ['d', 'w', 'm', 'q'].includes(t.every), t.label);
  }));
});

test('the rolling median behind every spike equals a plain recomputation on the real SOFR tail', () => {
  const { dates, values } = H('D_SOFR_TAIL');
  const ex = D.excessSeries(dates, values, { window: 60, minBase: 20 });
  assert.equal(ex.length, dates.length);
  for (let i = 0; i < 20; i++) assert.equal(ex[i], null, 'no baseline before 20 observations');
  for (const i of [20, 21, 59, 60, 61, 200, 360, 361, 362, 1000, dates.length - 1]) {
    const base = naiveMedian(values.slice(Math.max(0, i - 60), i));
    near(ex[i].base, base, 1e-9, `baseline at ${dates[i]}`);
    near(ex[i].excess, values[i] - base, 1e-9, `excess at ${dates[i]}`);
  }
});

test('the SOFR tail: the data itself finds September 2019, year-end 2018 and March 2020 - and agrees with the desk engine', () => {
  const { dates, values } = H('D_SOFR_TAIL');
  const found = D.spikes(dates, values, { window: 60, minBase: 20, gapDays: 21, top: 5, min: 5 });
  assert.deepEqual(found.slice(0, 3).map((s) => [s.date, s.value]), [['2019-09-17', 375], ['2018-12-31', 325], ['2020-03-16', 174]]);
  assert.equal(found[0].from <= '2019-09-17' && found[0].to >= '2019-09-18', true, 'the episode spans the days that stayed above half its peak');
  // the desk engine (justhodl-repo-market) ranks tail days from the NY Fed API on its own: same days, same basis points
  const engine = FX.desk.episodes.top_tail_days;
  assert.deepEqual([engine[0].date, engine[0].tail_bps, engine[1].date, engine[1].tail_bps], ['2019-09-17', 375, '2018-12-31', 325]);
  const byDate = Object.fromEntries(dates.map((d, i) => [d, values[i]]));
  for (const e of engine) near(byDate[e.date], e.tail_bps, 1e-6, `tail on ${e.date}`);
  // 16, 17 and 18 September 2019 are three of the engine's five days and one episode here
  assert.equal(found.filter((s) => s.date >= '2019-09-13' && s.date <= '2019-10-04').length, 1);
});

test('SOFR against interest on reserves across the 2021 rename: +315 bp on 2019-09-17, as the desk engine measured it', () => {
  const { dates, values } = H('D_SOFR_ADMIN');
  const byDate = Object.fromEntries(dates.map((d, i) => [d, values[i]]));
  assert.equal(byDate['2019-09-17'], 315);
  assert.equal(FX.desk.episodes.top_tail_days[0].sofr_iorb_bps, 315);
  assert.equal(byDate['2025-10-31'], 32);
  assert.ok(dates[0] <= '2018-04-03', 'reaches back before IORB existed (July 2021)');
  const m = D.MEASURES.find((x) => x.id === 'D_SOFR_ADMIN'), read = D.readMeasure(m, H('D_SOFR_ADMIN'));
  assert.deepEqual(read.spikes.slice(0, 3).map((s) => [s.date, D.labelFor(s.date).name]),
    [['2019-09-17', 'September 2019 repo squeeze'], ['2019-01-02', 'Year-end 2018'], ['2020-03-17', 'March 2020 dash for cash']]);
  assert.equal(read.cells.sep2019.date, '2019-09-17');
  assert.equal(read.cells.gfc2008, null, 'no SOFR in 2008: a dash, not a number');
});

test('2008 is read through what has history: fed funds against target, the TED spread, fails, the Fed\'s operations', () => {
  const read = (id) => D.readMeasure(D.MEASURES.find((x) => x.id === id), H(id));
  const ted = read('TEDRATE'), fails = read('NYPD-PD_AFtD_T-A'), target = read('D_EFFR_TARGET'), fed = read('D_FED_REPO'), swaps = read('SWPT'), dw = read('WLCFLPCL');
  assert.deepEqual([ted.cells.gfc2008.date, ted.cells.gfc2008.value], ['2008-10-10', 4.58]);
  assert.equal(D.fmtMeasure(D.MEASURES.find((x) => x.id === 'TEDRATE'), 4.58), '+458 bp');
  assert.deepEqual([fails.cells.gfc2008.date, fails.cells.gfc2008.value], ['2008-10-22', 2683588000000]);
  assert.equal(D.fmtMeasure(D.MEASURES.find((x) => x.id === 'NYPD-PD_AFtD_T-A'), 2683588000000), '$2,684B');
  assert.equal(fails.spikes[0].date, '2008-10-22');
  assert.deepEqual(fails.spikes.slice(0, 3).map((s) => D.labelFor(s.date).name), ['Lehman / global financial crisis', '2003 Treasury fails episode', 'September 11, 2001']);
  assert.deepEqual([target.cells.gfc2008.date, target.cells.gfc2008.value], ['2008-10-07', 97]);
  assert.equal(H('D_EFFR_TARGET').values[H('D_EFFR_TARGET').dates.indexOf('2008-09-15')], 64);
  assert.ok(target.spikes.every((s) => s.value >= 10), 'a jump from far below target to just below it is not listed as a squeeze');
  assert.equal(fed.spikes[0].date, '2020-03-17');
  near(fed.spikes[0].value, 152.75, 1e-6);
  assert.equal(fed.cells.sep2001.date, '2001-09-14');
  assert.equal(swaps.cells.gfc2008.date, '2008-12-17');
  assert.equal(dw.spikes[0].date, '2023-03-15');
  assert.equal(D.labelFor(dw.spikes[0].date).name, 'March 2023 bank failures');
  // a series that had not begun shows nothing for the episode
  assert.equal(read('SRF_TAKEUP').cells.sep2019, null);
  assert.equal(read('SRF_TAKEUP').spikes[0].date, '2025-12-31');
  assert.equal(ted.cells.oct1987.date, '1987-10-20');
  assert.equal(ted.cells.ye2025, null, 'the TED spread ended in January 2022');
});

test('the repo rate before SOFR: the dealer survey rate carries repo against the policy rate back to February 1998', () => {
  const gc = FX.rows.GC_SURVEY, up = H('D_REPO_TARGET'), down = H('D_TARGET_REPO');
  assert.deepEqual([gc.first, gc.last, gc.n_obs, gc.group], ['1998-02-20', '2018-02-28', 4998, 'Reference rates & volumes']);
  assert.equal(up.dates[0], '1998-02-20');
  assert.equal(up.dates.length, FX.rows.D_REPO_TARGET.n_obs);
  const at = (h, day) => h.values[h.dates.indexOf(day)];
  // the days the two series were built for, each against the policy rate of its day
  near(at(up, '2019-09-17'), 312.5, 1e-6, 'SOFR 5.25 against the middle of 2.00-2.25');
  near(at(up, '2008-09-17'), -175, 1e-6, 'survey rate 0.25 against a 2.00 target');
  near(at(down, '2008-09-17'), 175, 1e-6);
  near(at(down, '1999-12-31'), 263, 1e-6, 'survey rate 2.87 against a 5.50 target');
  near(at(down, '2007-08-20'), 104, 1e-6, 'survey rate 4.21 against a 5.25 target');
  near(at(up, '2008-12-31'), -9.5, 1e-6, 'survey rate 0.03 against the middle of 0-0.25');
  // one series is the mirror of the other, day for day
  assert.deepEqual(down.dates, up.dates);
  assert.ok(up.values.every((v, i) => v === -down.values[i]));
  // the survey ended on 2018-02-28 and SOFR began on 2018-04-03: the weeks between are not filled in
  assert.equal(up.dates[up.dates.indexOf('2018-02-28') + 1], '2018-04-03');
  // each crisis prints on the side it belongs to
  const peak = (h, id) => { const e = D.EPISODES.find((x) => x.id === id); return D.windowPeak(h.dates, h.values, e.from, e.to); };
  assert.deepEqual([peak(up, 'sep2019').date, Math.round(peak(up, 'sep2019').value * 10) / 10], ['2019-09-17', 312.5]);
  assert.deepEqual([peak(down, 'aug2007').date, Math.round(peak(down, 'aug2007').value)], ['2007-08-21', 309]);
  assert.deepEqual([peak(down, 'y2k1999').date, Math.round(peak(down, 'y2k1999').value)], ['1999-12-30', 284]);
  assert.deepEqual([peak(down, 'sep2001').date, Math.round(peak(down, 'sep2001').value)], ['2001-09-19', 210]);
  assert.deepEqual([peak(down, 'bear2008').date, Math.round(peak(down, 'bear2008').value)], ['2008-03-19', 191]);
  assert.deepEqual([peak(down, 'gfc2008').date, Math.round(peak(down, 'gfc2008').value)], ['2008-10-03', 189]);
  assert.ok(peak(up, 'gfc2008').value < 60, 'in 2008 repo did not trade far above the policy rate: the stress was the other way');
  // the data finds the same days without being told: the largest spikes of each side
  const mUp = D.MEASURES.find((m) => m.id === 'D_REPO_TARGET'), mDown = D.MEASURES.find((m) => m.id === 'D_TARGET_REPO');
  const topUp = D.readMeasure(mUp, up).spikes, topDown = D.readMeasure(mDown, down).spikes;
  assert.equal(topUp[0].date, '2019-09-17');
  assert.ok(topUp[0].excess > 290 && topUp[0].excess < 315);
  const years = topDown.slice(0, 5).map((s) => s.date.slice(0, 7));
  assert.ok(years.includes('2007-08') && years.includes('1999-12'), years.join(' '));
  assert.ok(topDown.every((s) => s.value >= mDown.min) && topUp.every((s) => s.value >= mUp.min));
  // a return to normal after months far below policy is not a squeeze: January 2009 (+16 bp) is not listed
  assert.ok(!topUp.some((s) => s.date.startsWith('2009-01')), topUp.map((s) => s.date).join(' '));
  assert.deepEqual(topUp.slice(0, 3).map((s) => [s.date, D.labelFor(s.date).name]), [['2019-09-17', 'September 2019 repo squeeze'], ['2019-01-02', 'Year-end 2018'], ['1998-12-15', '']]);
  assert.deepEqual(D.labelFor('1999-12-30'), { kind: 'episode', id: 'y2k1999', name: 'Year-end 1999 (Y2K)' });
});

test('a series\' own chart marks the spikes the Crisis view lists, not every climb back to zero', () => {
  const up = H('D_REPO_TARGET'), m = D.MEASURES.find((x) => x.id === 'D_REPO_TARGET');
  const general = D.spikes(up.dates, up.values, Object.assign({ top: 6 }, D.windowFor(up.dates)));
  // the general rule alone calls the end of 2008 a spike: repo climbing from 85 bp under policy to 2.5 bp under it
  assert.ok(general.some((s) => s.date === '2008-12-16' && s.value < 0), general.map((s) => s.date + ' ' + s.value).join(' | '));
  const marked = D.chartSpikes('D_REPO_TARGET', up);
  assert.deepEqual(marked, D.readMeasure(m, up).spikes.slice(0, 6));
  assert.ok(marked.length === 6 && marked.every((s) => s.value >= m.min) && !marked.some((s) => s.date.startsWith('2008-12') || s.date.startsWith('2009-01')));
  assert.equal(marked[0].date, '2019-09-17');
  // a series that is not a crisis measure keeps the general rule
  const sofr = H('SOFR');
  assert.deepEqual(D.chartSpikes('SOFR', sofr), D.spikes(sofr.dates, sofr.values, Object.assign({ top: 6 }, D.windowFor(sofr.dates))));
  assert.equal(D.chartSpikes('SOFR', sofr)[0].date, '2019-09-17');
});

test('dealers\' Treasury financing: a side the New York Fed withholds is shown at its last published week', () => {
  const real = D.dealerFinancing(FX.dealer).treasury;
  assert.deepEqual(real, { as_of: '2026-11-11', repo_out_b: 3398.38, repo_out_b_as_of: '2026-11-11', reverse_repo_in_b: 3002.182, reverse_repo_in_b_as_of: '2026-10-28' });
  assert.equal(FX.dealer.financing.treasury.reverse_repo_in_b, null, 'the engine publishes no figure for the withheld week');
  assert.equal(D.isLate(real.reverse_repo_in_b_as_of, '2026-11-22', 'w'), true);
  assert.equal(D.isLate(real.repo_out_b_as_of, '2026-11-22', 'w'), false);
  // both sides published: each at the document's week
  const both = D.dealerFinancing({ financing: { treasury: { as_of: '2026-10-28', repo_out_b: 3344.1, reverse_repo_in_b: 3001.4 } }, groups: {} }).treasury;
  assert.deepEqual(both, { as_of: '2026-10-28', repo_out_b: 3344.1, repo_out_b_as_of: '2026-10-28', reverse_repo_in_b: 3001.4, reverse_repo_in_b_as_of: '2026-10-28' });
  // never published and no history: no number and no date, so the tile says "not published"
  const none = D.dealerFinancing({ financing: { treasury: { as_of: '2026-11-11', repo_out_b: null, reverse_repo_in_b: null } }, groups: { treasury_repo: { history: [['2026-11-11', null, 'SBN2024', false]] } } }).treasury;
  assert.deepEqual([none.repo_out_b, none.repo_out_b_as_of, none.reverse_repo_in_b, none.reverse_repo_in_b_as_of], [null, null, null, null]);
  assert.equal(D.dealerFinancing(null), null);
  assert.equal(D.dealerFinancing({}), null);
});

test('spikes(): one episode per cluster, most extreme first, thresholds honoured', () => {
  const dates = [], values = [];
  for (let i = 0; i < 200; i++) { dates.push(new Date(Date.UTC(2024, 0, 1 + i)).toISOString().slice(0, 10)); values.push(2 + (i % 5) * 0.01); }
  values[100] = 5; values[101] = 4.2; values[102] = 3.6; values[150] = 3; values[30] = 2.3;
  const got = D.spikes(dates, values, { window: 60, minBase: 20, gapDays: 21, top: 8, min: 0.2 });
  assert.deepEqual(got.map((s) => s.date), [dates[100], dates[150], dates[30]]);
  assert.deepEqual([got[0].from, got[0].to, got[0].obs], [dates[100], dates[102], 3]);
  near(got[0].base, 2.02, 1e-9);
  near(got[0].excess, 2.98, 1e-9);
  assert.equal(D.spikes(dates, values, { top: 1 }).length, 1);
  assert.deepEqual(D.spikes(dates, values, { min: 5 }), []);
  assert.deepEqual(D.spikes([], [], {}), []);
  // gaps in the data are not observations
  const holes = values.slice(); holes[99] = null; holes[98] = NaN;
  assert.equal(D.spikes(dates, holes, { min: 0.2 })[0].date, dates[100]);
  assert.equal(D.spikes(dates, values, { scale: 100, min: 20 })[0].excess > 297, true);
});

test('a baseline as long as a quarter of the series own time', () => {
  assert.deepEqual(D.windowFor(H('SOFR').dates), { window: 60, minBase: 20, gapDays: 21 });
  assert.deepEqual(D.windowFor(H('NYPD-PD_AFtD_T-A').dates), { window: 26, minBase: 8, gapDays: 60 });
  assert.deepEqual(D.windowFor(['2026-07-31', '2026-08-31', '2026-09-30', '2026-10-31']), { window: 12, minBase: 4, gapDays: 120 });
  assert.equal(D.spacingDays(H('SWPT').dates), 7);
});

test('names for dates: known episodes first, then the turn of a month, a quarter, a year', () => {
  assert.deepEqual(D.labelFor('2019-09-17'), { kind: 'episode', id: 'sep2019', name: 'September 2019 repo squeeze' });
  assert.equal(D.labelFor('2018-12-31').name, 'Year-end 2018');
  assert.equal(D.labelFor('2008-10-10').id, 'gfc2008');
  assert.deepEqual(['2024-09-30', '2024-10-01', '2024-07-02', '2024-12-31', '2025-01-02', '2024-10-31', '2024-11-01', '2024-10-15', '2024-10-27', '2024-10-03'].map(D.calendarTurn),
    ['quarter-end', 'quarter-end', 'quarter-end', 'year-end', 'year-end', 'month-end', 'month-end', '', '', '']);
  assert.deepEqual(D.labelFor('2024-06-28'), { kind: 'turn', id: 'quarter-end', name: 'quarter-end' });
  assert.deepEqual(D.labelFor('2024-06-12'), { kind: '', id: '', name: '' });
  const windows = D.EPISODES.map((e) => [e.from, e.to]);
  assert.ok(windows.every(([a, b]) => a < b));
  assert.ok(D.EPISODES.every((e, i) => i === 0 || e.to < D.EPISODES[i - 1].from), 'newest first, never overlapping');
});

test('the corridor: from the feed\'s policy block, or worked back out of the spreads for a feed that has none', () => {
  const desk = FX.desk, c = D.corridor(desk), sp = desk.spreads, sofr = desk.rates.sofr.value;
  assert.equal(c.sofr, sofr);
  near(c.admin, sofr - sp.sofr_iorb.bps / 100, 0.0051);
  near(c.floor, sofr - sp.sofr_floor.bps / 100, 0.0051);
  near(c.top, sofr + sp.ceiling_room.bps / 100, 0.0051);
  assert.ok(c.floor < c.admin && c.admin < c.top && c.floor <= c.p1 && c.p99 <= c.top + 0.25);
  // the board's own policy rows say the same levels
  near(c.admin, FX.rows.IORB.last_value, 0.0051, 'IORB');
  near(c.floor, FX.rows.RRPONTSYAWARD.last_value, 0.0051, 'ON RRP award');
  near(c.top, FX.rows.DFEDTARU.last_value, 0.0051, 'top of range');
  const withPolicy = D.corridor(Object.assign({}, desk, { policy: { admin_rate: 3.9, on_rrp_award: 3.75, range_top: 4.0 } }));
  assert.deepEqual([withPolicy.floor, withPolicy.admin, withPolicy.top, withPolicy.from_policy_block], [3.75, 3.9, 4.0, true]);
  assert.deepEqual(D.corridor({}), { as_of: null, sofr: null, tgcr: null, bgcr: null, effr: null, obfr: null, floor: null, admin: null, top: null, p1: null, p25: null, p75: null, p99: null, from_policy_block: false });
  const svg = D.corridorSvg(c);
  assert.match(svg, /^<svg /);
  assert.match(svg, /SOFR [0-9.]+%/);
  assert.equal(D.corridorSvg(D.corridor({})), '');
});

test('a rate\'s change is its move in basis points, checked against the series itself', () => {
  // the board gives SOFR's week as a percentage of the level; the page shows basis points
  const row = FX.rows['FNYR-SOFR-A'], sofr = H('SOFR'), last = sofr.values[sofr.values.length - 1];
  assert.equal(row.last_value, last);
  const weekAgo = sofr.values[sofr.dates.indexOf('2026-11-12')];
  near(D.moveFromPct(row.last_value, row.chg.w, 100), (last - weekAgo) * 100, 0.05, 'one week');
  assert.equal(D.tileMove('rate', row.last_value, row.chg.w).text, '+1.0 bp');
  // a spread that crossed zero: -2 bp a month ago, +1 bp now, published as -150%
  const adm = FX.rows.D_SOFR_ADMIN;
  near(D.prevFromPct(adm.last_value, adm.chg.m), -2, 1e-6);
  assert.equal(D.tileMove('bp', adm.last_value, adm.chg.m).text, '+3.0 bp');
  assert.equal(D.tileMove('bp', adm.last_value, adm.chg.pop), null, 'a change from exactly zero has no percentage: nothing is shown');
  assert.equal(D.tileMove('usd', 2.2e12, -3.283).text, '-3.3%');
  assert.equal(D.tileMove('pct', 80.286, 0.5).text.endsWith(' pt'), true);
  assert.equal(D.prevFromPct(5, -100), null);
  assert.equal(D.prevFromPct(null, 3), null);
});

test('money and rates print in the units a desk reads', () => {
  assert.deepEqual([2.235e12, 2.7373e11, 6.114e9, 1.46e8, 5.547e9].map(D.usd), ['$2.24T', '$274B', '$6.1B', '$146M', '$5.5B']);
  assert.equal(D.fmtKind('usdm', FX.rows.WRESBAL.last_value), '$2.98T', 'reserves are published in millions');
  assert.equal(D.fmtKind('usdm', FX.rows.SWPT.last_value), '$146M');
  assert.equal(D.fmtKind('usdb', FX.rows.RRPONTSYD.last_value), '$401M', 'the ON RRP balance is published in billions');
  assert.equal(D.fmtKind('usdb', D.dealerFinancing(FX.dealer).treasury.repo_out_b), '$3.40T');
  assert.equal(D.fmtKind('rate', 3.91), '3.91%');
  assert.equal(D.fmtKind('bp', 1), '+1.0 bp');
  assert.equal(D.fmtKind('bp', -12), '-12.0 bp');
  assert.equal(D.fmtKind('pct', 80.2861), '80.3%');
  assert.equal(D.fmtKind('rate', null), '—');
  assert.equal(D.usd(undefined), '—');
});

test('a number older than its source\'s rhythm allows is marked, with its age', () => {
  assert.equal(D.ageDays('2026-11-19', '2026-11-21'), 2);
  assert.equal(D.isLate('2026-11-19', '2026-11-21', 'd'), false);
  assert.equal(D.isLate('2026-11-13', '2026-11-21', 'd'), true);
  assert.equal(D.isLate('2026-11-11', '2026-11-21', 'w'), false);
  assert.equal(D.isLate('2026-10-28', '2026-11-21', 'w'), true, 'the dealers\' financing block of 28 October is called late');
  assert.equal(D.isLate('2026-10-31', '2026-11-21', 'm'), false);
  assert.equal(D.isLate('2026-07-09', '2026-11-21', 'm'), true, 'the haircuts of July are called late');
  assert.equal(D.isLate(null, '2026-11-21', 'd'), true);
  assert.equal(D.isLate('2026-11-23', '2026-11-21', 'd'), false, 'IORB is published ahead of its date');
});

test('the desk renders every aspect from the real feeds, each tile with its own date', () => {
  const made = {};
  const el = { set innerHTML(v) { made.html = v; }, get innerHTML() { return made.html; }, querySelectorAll() { return []; } };
  const ctx = { rows: FX.rows, desk: FX.desk, pd: D.dealerFinancing(FX.dealer), today: '2026-11-22', open() {}, history() {} };
  D.renderDesk(el, ctx);
  const html = made.html;
  for (const s of D.SECTIONS) assert.ok(html.includes(`data-sec='${s.key}'`), s.key);
  assert.ok(html.includes('17.6') && html.includes('CALM'));
  assert.equal(html.split("class='rd-tile").length - 1, D.SECTIONS.reduce((n, s) => n + s.tiles.length, 0));
  assert.equal(html.split('not published').length - 1, 0, 'no tile without a number on the real board');
  assert.ok(html.includes('>3.91%<') && html.includes('2026-11-19'));
  assert.ok(html.includes('$3.40T') && html.includes('2026-11-11'), 'dealers\' Treasury repo at the newest week');
  assert.ok(html.includes('$3.00T') && html.includes('2026-10-28 · 25 days old'), 'the side the New York Fed withholds is shown at its last published week, with its age');
  assert.ok(html.includes('2026-10-09') && !html.includes('2026-07-09'), 'the haircuts are at the newest monthly snapshot');
  assert.ok(html.includes(FX.rows['DTCC-TREASURY-FAILS'].last) && FX.rows['DTCC-TREASURY-FAILS'].last >= '2026-11-20', 'DTCC daily fails are current');
  // before the page change a withheld side was a blank tile
  D.renderDesk(el, Object.assign({}, ctx, { pd: { treasury: FX.dealer.financing.treasury } }));
  assert.equal(made.html.split('not published').length - 1, 1);
  D.renderDesk(el, ctx);
  assert.ok(html.includes('The corridor') && html.includes('The calendar') && html.includes('What the stress score is made of'));
  // without the overnight feed the board still renders
  D.renderDesk(el, Object.assign({}, ctx, { desk: null }));
  assert.ok(made.html.includes('The overnight desk feed is not available') && made.html.includes("data-sec='rates'"));
  // a row that is not on the board says so instead of showing a blank
  const rows = Object.assign({}, FX.rows); delete rows['FNYR-SOFR-A'];
  D.renderDesk(el, Object.assign({}, ctx, { rows }));
  assert.equal(made.html.split('not published').length - 1, 1);
});

test('the crisis view builds its table from the histories: one row per episode, one column per measure', async () => {
  const made = {};
  const el = { set innerHTML(v) { made.html = v; }, get innerHTML() { return made.html; }, querySelectorAll() { return []; } };
  const asked = [];
  const ctx = { rows: FX.rows, desk: FX.desk, today: '2026-11-21', open() {},
    history(sid) { asked.push(sid); return FX.history[sid] ? Promise.resolve(FX.history[sid]) : Promise.reject(new Error('404')); } };
  const list = await D.renderCrisis(el, ctx);
  assert.equal(list.length, D.MEASURES.length);
  assert.deepEqual(asked.slice().sort(), D.MEASURES.map((m) => m.id).sort());
  const html = made.html;
  for (const e of D.EPISODES) assert.ok(html.includes(e.name), e.name);
  assert.equal(html.split('<tr><td class=\'namecell\'>').length - 1, D.EPISODES.length);
  assert.ok(html.includes('+315 bp') && html.includes('+375 bp') && html.includes('$2,684B') && html.includes('+458 bp') && html.includes('$74.6B'));
  assert.ok(html.includes('September 2019 repo squeeze') && html.includes('Lehman / global financial crisis'));
  // the two measures that reach back to 1998 lead the table, and the episodes before SOFR have numbers in them
  assert.deepEqual(D.MEASURES.slice(0, 2).map((m) => m.id), ['D_REPO_TARGET', 'D_TARGET_REPO']);
  assert.ok(html.includes('+313 bp') && html.includes('+309 bp') && html.includes('+284 bp') && html.includes('Year-end 1999 (Y2K)'));
  assert.ok(html.includes('primary-dealer survey rate') && !html.includes('No public daily repo rate reaches back before August 2014'));
  // a measure whose history cannot be read drops out; the rest still render
  const partial = await D.renderCrisis(el, Object.assign({}, ctx, { history(sid) { return sid === 'TEDRATE' ? Promise.reject(new Error('x')) : Promise.resolve(FX.history[sid]); } }));
  assert.equal(partial.length, D.MEASURES.length - 1);
});

test('repo.html opens on the Desk, keeps the board, and marks spikes on every chart', () => {
  assert.match(HTML, /<script src="\/jh-repo-desk\.js\?v=[0-9a-z]+"><\/script>/);
  assert.ok(HTML.indexOf('/jh-repo-desk.js') < HTML.indexOf('"use strict";\nconst fmt='), 'the desk script loads before the page script');
  assert.match(HTML, /\['desk','Desk',''\],\['crisis','Crisis spikes',''\],\['overview','Overview',rows\.length\]/);
  assert.match(HTML, /return h\|\|'desk';/);
  assert.match(HTML, /optional\('\/data\/repo-market\.json'\),optional\('\/data\/nyfed-primary-dealer\.json'\)/);
  assert.match(HTML, /SPK=window\.JHRepoDesk\?JHRepoDesk\.chartSpikes\(id,h\):\[\];/);
  assert.ok(HTML.includes('<div id="spk"></div>') && HTML.includes('★ Favorites') && HTML.includes("localStorage.getItem('repoFavs')"));
  const order = /const GORDER=(\[[^\]]+\]);/.exec(HTML);
  const groups = JSON.parse(order[1]);
  assert.equal(new Set(groups).size, groups.length, 'no group listed twice in the tab order');
  assert.deepEqual(FX.board.groups.map((g) => g.name).filter((g) => !groups.includes(g)), [], 'every group on the real board has its place in the tab order');
  assert.doesNotMatch(SRC, /\beval\(|new Function\(/);
  assert.doesNotMatch(SRC, /localStorage|document\.cookie/);
});

test('the chart has a Repo button right after CDS, and it opens this desk', () => {
  const engine = fs.readFileSync(path.join(ROOT, 'jh-chart-engine.js'), 'utf8'), rail = fs.readFileSync(path.join(ROOT, 'jh-chart-tvrail.js'), 'utf8');
  const chart = fs.readFileSync(path.join(ROOT, 'chart.html'), 'utf8');
  const iCds = engine.indexOf('id=btn-cds'), iRepo = engine.indexOf('id=btn-repo'), iNext = engine.indexOf('id=btn-usd');
  assert.ok(iCds > 0 && iRepo > iCds && iNext > iRepo);
  assert.equal(engine.slice(iCds, iRepo).split('<button').length, 2, 'nothing but the CDS button\'s own tail before it');
  assert.equal(engine.split('id=btn-repo').length, 2);
  assert.match(engine, /getElementById\("btn-repo"\); if\(brepo\) brepo\.onclick=function\(\)\{ if\(window\.jhOpenWorkspace\) window\.jhOpenWorkspace\("repo"\); \};/);
  assert.match(rail, /repo: \["Repo · rates, venues, Fed facilities, crisis spikes", "\/repo\.html"\]/);
  assert.equal(chart.split('&repo=20261121').length, 3, 'the engine and the rail are both cache-busted so a browser never pairs a new button with an old rail');
  assert.ok(fs.existsSync(path.join(ROOT, 'repo.html')));
});
