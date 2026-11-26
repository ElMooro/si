'use strict';
// "At a glance" (jh-glance.js): the picture at the top of five engine pages, drawn from the page's own documents.
// Khalid 2026-11-26: "liquidity and plumbing need better visuals for easier digestion of data, breadth ... calendar ...
// crypto need improvement and a better iux". The fixture is the real documents of 2026-11-26 (ops 6977).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const ROOT = path.join(__dirname, '..');
const G = require('../jh-glance.js');
const SRC = fs.readFileSync(path.join(ROOT, 'jh-glance.js'), 'utf8');
const F = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures', 'glance-docs-2026-11-26.json.gz'))).toString('utf8'));
const read = (f) => fs.readFileSync(path.join(ROOT, f), 'utf8');
const clean = (html) => assert.ok(!/NaN|undefined|\[object Object\]/.test(html), 'nothing printed as NaN, undefined or an object');
const MOUNTS = { 'plumbing.html': 'plumbing', 'liquidity.html': 'liquidity', 'breadth-thrust.html': 'breadth', 'catalyst-calendar.html': 'calendar', 'btc-cycle.html': 'crypto' };

test('the kit is one plain-ASCII file the colour re-skin leaves alone, and it knows the five pages', () => {
  assert.ok(/^[\x00-\x7f]*$/.test(SRC), 'ASCII only');
  assert.ok(SRC.includes('jh-reskin-skip'));
  assert.deepEqual(Object.keys(G.ADAPTERS).sort(), ['breadth', 'calendar', 'crypto', 'liquidity', 'plumbing']);
  for (const k of Object.keys(G.ADAPTERS)) assert.ok(G.ADAPTERS[k].docs.length >= 1 && typeof G.ADAPTERS[k].title === 'string');
});

test('each of the five pages mounts its glance above its own content and keeps its Engine data section', () => {
  for (const [file, name] of Object.entries(MOUNTS)) {
    const html = read(file);
    assert.equal(html.split('data-glance="' + name + '"').length - 1, 1, file + ': one mount');
    assert.match(html, /<script src="\/jh-glance\.js\?v=\d{8}[a-z]" defer><\/script>/, file + ': the kit is loaded');
    assert.ok(html.includes('data-jed="' + name + '"'), file + ': the Engine data section is still mounted');
    assert.ok(html.indexOf('data-glance="' + name + '"') < html.indexOf('data-jed="' + name + '"'), file + ': the glance comes first');
  }
  const liq = read('liquidity.html');
  assert.ok(liq.indexOf('data-glance="liquidity"') < liq.indexOf('<div class="page-header">'), 'liquidity: at the top of the page, not 1,300 px down');
});

test('plumbing: the gauge is the document\'s score, and the layer weights give the same number', () => {
  const M = G.plumbingModel(F.plumbing);
  assert.equal(M.score, F.plumbing.composite_score);
  assert.equal(M.label, F.plumbing.composite_label);
  assert.equal(M.n, 58);
  assert.equal(M.have, 55);
  assert.equal(M.layers.length, 5);
  assert.ok(Math.abs(M.weighted - M.score) < 0.06, 'the five layers, weighted, give the composite (' + M.weighted + ' vs ' + M.score + ')');
  assert.equal(M.layers.reduce((a, l) => a + l.n, 0), M.n, 'every indicator sits in one layer');
  const html = G.drawPlumbing(M);
  clean(html);
  assert.equal(new Set([...html.matchAll(/data-ind="([^"]+)"/g)].map((m) => m[1])).size, M.n, 'every indicator the engine wrote can be picked');
});

test('liquidity: the three legs add to the net, and the chart is the document\'s 180 days', () => {
  const M = G.liquidityModel(F.flow, F.agent);
  const c = F.flow.current;
  assert.equal(M.net, c.net_liquidity_b);
  assert.ok(M.adds, 'Fed balance sheet less Treasury account less reverse repo equals the net');
  assert.ok(Math.abs(c.fed_balance_sheet_b - c.tga_b - c.rrp_b - M.net) < 0.0015);
  assert.equal(M.asOf, F.flow.generated_at, 'the stamp is when the document was written, not its oldest input');
  assert.equal(M.valued, c.valuation_date);
  assert.equal(M.windows.length, 4);
  assert.equal(M.hist.length, F.flow.history_180d.filter((r) => typeof r.net_liquidity_b === 'number' || typeof r.net === 'number').length || M.hist.length);
  assert.ok(M.lo <= M.net && M.net <= M.hi, 'today sits inside its 180-day range');
  assert.equal(M.groups.reduce((a, g) => a + g.rows.length, 0), M.nSeries);
  assert.equal(M.nSeries, Object.keys(F.agent.series).length, 'every series the liquidity agent keeps is listed in a group');
  const html = G.drawLiquidity(M);
  clean(html);
  assert.equal(html.split('data-chart="').length - 1, 4, 'net liquidity and each of its three legs are drawn');
  assert.ok(html.includes('nothing kept for this series today'), 'a series with no observation says so instead of showing a bare id');
  const none = G.liquidityModel(F.flow, null);
  assert.equal(none.groups.length, 0);
  clean(G.drawLiquidity(none));
});

test('breadth: the 10-session average recomputed here is the engine\'s, and the state is the document\'s', () => {
  const M = G.breadthModel(F.breadth, F.breadth_history);
  assert.equal(M.state, 'ARMED');
  assert.equal(M.n, 60);
  assert.equal(M.agrees, true, 'recomputed from the kept sessions it equals the published average');
  assert.equal(Number(M.ema[M.ema.length - 1].toFixed(4)), F.breadth.current_readings.zweig_10d_ema);
  assert.equal(Number(M.winMin.toFixed(4)), F.breadth.current_readings.zweig_window_min);
  assert.equal(M.episodes.length, F.breadth.historical_episodes.length);
  assert.deepEqual(G.ema([1, 2, 3], 3), [1, 1.5, 2.25], 'seeded with the first value, alpha 2 / (n + 1)');
  assert.equal(M.emaDod, null, 'a document without day-over-day rows draws no second line');
  assert.equal(M.mcclellan, null);
  const html = G.drawBreadth(M);
  clean(html);
  assert.equal(html.split('data-chart="').length - 1, 1);
  assert.equal(html.split('<tr><td>20').length - 1, M.episodes.length, 'every past firing is listed');
});

test('breadth: a v1.1 document adds the day-over-day line, the counts, McClellan and the preliminary reading; the state basis stays', () => {
  const rows = F.breadth_history.history.map((r, i) => Object.assign({}, r, { adv: 3000 + i, dec: 3000 - i, unch: 40, up_vol_share: 0.5, ratio_dod: Math.min(0.9, r.ratio + 0.02), adv_dod: 3100, dec_dod: 2900, unch_dod: 12, set_aside_dod: 3 }));
  const older = Array.from({ length: 70 }, (_, i) => ({ date: '2026-0' + (5 + Math.floor(i / 28)) + '-' + String(1 + (i % 28)).padStart(2, '0'), ratio: 0.9, n_stocks: 6000, ratio_dod: 0.9 }));
  const E = G.ema(rows.map((r) => r.ratio_dod), 10);
  const doc = Object.assign({}, F.breadth, {
    version: '1.1', breadth_basis: 'The state follows an OPEN-TO-CLOSE ratio.',
    day_over_day: { available: true, ema_10d: Number(E[E.length - 1].toFixed(4)), would_read: 'ARMED', n_sessions: 60 },
    mcclellan: { available: true, oscillator: -31.8, previous: -42.2, n_sessions: 130, seasoned: true, basis: 'b', series: rows.slice(-40).map((r, i) => [r.date, i - 20]) },
    preliminary: { status: 'ahead_of_settled', session: '2026-11-27', ratio: 0.61, ema_10d_with_it: 0.44, would_read: 'ARMED' },
  });
  const M = G.breadthModel(doc, { history: older.concat(rows) });
  assert.equal(M.kept, 130);
  assert.equal(M.n, 60, 'the picture and the recomputed average use the 60 sessions the state machine reads');
  assert.equal(M.agrees, true, '70 older sessions in the cache do not move the recomputed average');
  assert.equal(M.emaDod.length, 60);
  assert.equal(M.dodAgrees, true);
  assert.ok(M.mcclellan && M.preliminary);
  const html = G.drawBreadth(M);
  clean(html);
  assert.equal(html.split('data-chart="').length - 1, 2, 'the sessions chart and the McClellan chart');
  for (const s of ['Advanced / declined', 'against the prior close', 'McClellan oscillator', 'Today so far', 'moves no state', 'set aside as likely splits', 'OPEN-TO-CLOSE']) assert.ok(html.includes(s), s);
  const settled = G.breadthModel(Object.assign({}, doc, { preliminary: { status: 'settled', session: '2026-11-25', ratio: 0.5, gap_to_settled: 0.001 } }), { history: rows });
  assert.equal(settled.preliminary, null, 'a session already settled is not shown as "today so far"');
});

test('calendar: five whole weeks from Monday, the counts are the document\'s, the United States leads a day', () => {
  const M = G.calendarModel(F.calendar, '2026-11-26');
  const ahead = F.calendar.events.filter((e) => e.date >= '2026-11-26');
  assert.equal(M.nAhead, ahead.length);
  assert.equal(M.cells.length, 35);
  assert.equal(M.cells[0].date, '2026-11-23');
  assert.equal(new Date(M.cells[0].date + 'T00:00:00Z').getUTCDay(), 1, 'the grid starts on a Monday');
  const today = M.cells.find((c) => c.today);
  assert.equal(today.date, '2026-11-26');
  assert.match(today.holiday, /Thanksgiving/);
  for (const c of M.cells.filter((x) => !x.past)) assert.equal(c.n, F.calendar.events.filter((e) => e.date === c.date).length, c.date + ': the cell counts the document\'s events');
  const high = ahead.filter((e) => e.date <= '2026-12-03' && e.impact === 'HIGH');
  assert.equal(M.nHigh7, high.length);
  assert.equal(M.week.length + M.weekMore, high.length);
  assert.equal(M.week[0].country, 'US', 'within a day the United States comes first');
  for (let i = 1; i < M.week.length; i++) assert.ok(M.week[i - 1].date <= M.week[i].date, 'in date order');
  const html = G.drawCalendar(M);
  clean(html);
  const days = new Set([...html.matchAll(/data-day="([^"]+)"/g)].map((m) => m[1]));
  for (const c of M.cells.filter((x) => !x.past && x.n)) assert.ok(days.has(c.date), c.date + ' can be picked');
  assert.equal(html.split(' disabled').length - 1, M.cells.filter((c) => !c.n).length, 'a day with nothing on it cannot be picked');
});

test('calendar page: ahead / past switch, search, Eastern times and jump-to-day are wired', () => {
  const html = read('catalyst-calendar.html');
  for (const w of ['ahead', 'past', 'all']) assert.ok(html.includes('data-when="' + w + '"'), 'chip ' + w);
  assert.ok(html.includes('id="calSearch"'));
  assert.ok(/window\.jhCalendarShowDay\s*=/.test(html));
  assert.ok(/function etTime\(/.test(html));
  assert.ok(html.includes('data-date='), 'each day group carries its date so the glance can jump to it');
  const scripts = [...html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1]).filter((s) => s.includes('jhCalendarShowDay'));
  assert.equal(scripts.length, 1);
  assert.doesNotThrow(() => new Function(scripts[0]), 'the page script parses');
  assert.ok(/if \(DATA && FILTERS\.impacts\.size < 3 && !applyFilters\(DATA\.events \|\| \[\]\)\.length\) allImpactsOn\(\);/.test(html), 'Past never opens on an empty list because Low is switched off');
  assert.ok(html.includes('id="showAllImpacts"') && html.includes('at the impact levels switched off: show them'), 'an empty list says what is hidden and offers it');
});

test('crypto: seven bands from the engine\'s fit, every on-chain metric placed in a category; the price chart waits for the weekly closes', () => {
  const M = G.cryptoModel(F.onchain, F.cryptoquant);
  assert.equal(M.bands.length, 7);
  assert.equal(M.price, F.onchain.cycle_bands.price);
  assert.equal(M.nMetrics, Object.keys(F.cryptoquant.metrics).length);
  assert.equal(M.cats.reduce((a, c) => a + c.rows.length, 0), M.nMetrics, 'every metric is in one category');
  assert.ok(M.fit && M.fit.a === F.onchain.cycle_bands.rainbow.slope_a);
  const html = G.drawCrypto(M);
  clean(html);
  assert.equal(html.split('data-met="').length - 1 >= M.nMetrics, true, 'a tile for every metric');
  assert.equal(html.split('data-chart="').length - 1, 0, 'no weekly closes in the document of Nov 26: no invented chart');
  // the engine's own formula: ln(price) = a * ln(days since the first observation) + b, bands at k sigma
  const r = F.onchain.cycle_bands.rainbow;
  const t = G.daysBetween(r.history_starts, F.onchain.cycle_bands.as_of) + 1;
  const fair = Math.exp(r.slope_a * Math.log(t) + r.intercept_b);
  assert.ok(Math.abs(fair / r.fair_value - 1) < 0.01, 'the fit redrawn here gives the engine\'s fair value (' + fair.toFixed(0) + ' vs ' + r.fair_value + ')');
  const withHist = JSON.parse(JSON.stringify(F.onchain));
  withHist.cycle_bands.history_weekly = Array.from({ length: 200 }, (_, i) => [G.addDays('2023-01-01', 7 * i), 20000 + 250 * i]);
  const H = G.cryptoModel(withHist, F.cryptoquant);
  const h2 = G.drawCrypto(H);
  clean(h2);
  assert.equal(h2.split('data-chart="').length - 1, 1);
  assert.equal(h2.split('class="jgl-ln s').length - 1, 8, 'the price and the seven bands');
});

test('a missing or misshapen document draws a plain notice, never a broken picture', () => {
  for (const [model, draw, args] of [[G.plumbingModel, G.drawPlumbing, [null]], [G.liquidityModel, G.drawLiquidity, [{}, null]], [G.breadthModel, G.drawBreadth, [{}, {}]], [G.calendarModel, G.drawCalendar, [{}, '2026-11-26']], [G.cryptoModel, G.drawCrypto, [{}, null]]]) {
    let html = '';
    assert.doesNotThrow(() => { html = draw(model.apply(null, args)); });
    assert.ok(!/NaN|undefined/.test(html));
  }
});
