// The stock volatility desk (stock-volatility.html + jh-stock-volatility.js) against a slice of a real engine artifact
// (ops 6851: the engine's own build() on the warehouse, session of 2026-11-18).
// Khalid 2026-11-19: "create an engine that would measure volatility. Most and least volatile stocks. Also it would
// measure the price spreads for all stocks and rank them based on that. Price spread last day, week, month, quarter and
// create an icon for it in chart.html".
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const SRC = fs.readFileSync(path.join(ROOT, 'jh-stock-volatility.js'), 'utf8');
const HTML = fs.readFileSync(path.join(ROOT, 'stock-volatility.html'), 'utf8');
const DOC = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/stock-volatility-desk.json'), 'utf8'));

function load() {
  const ctx = { console };
  ctx.window = ctx;
  vm.runInNewContext(SRC, ctx);
  return ctx.JHStockVol;
}
const V = load();
const J = x => JSON.parse(JSON.stringify(x));          // values built inside the vm realm, compared as plain data
const ROWS = V.unpack(DOC), BY = V.index(ROWS);
const col = id => V.COLS[id];
const filt = st => ROWS.filter(V.rowFilter(Object.assign({ kind: 'stock', size: 'all', sector: '', q: '', show: 'all' }, st)));
const FLOOR = DOC.board_rule.min_mcap;

test('the fixture is the engine\'s own artifact', () => {
  assert.equal(DOC.contract, V.CONTRACT);
  assert.equal(DOC.engine, 'justhodl-stock-volatility');
  assert.equal(V.KEY, 'data/stock-volatility.json');
  assert.ok(DOC.rows.length > 500 && DOC.rows.every(r => r.length === DOC.cols.length));
  assert.deepEqual(J(Object.keys(ROWS[0]).filter(k => k[0] !== '_')), DOC.cols);
});

test('a row keeps every column of the artifact, and its set-aside measures as a list', () => {
  const aapl = BY.AAPL;
  assert.equal(aapl.name, 'Apple Inc.');
  assert.equal(aapl.kind, 'stock');
  for (const k of ['hv21', 'hv63', 'hv252', 'yz21', 'atr', 'sp_d', 'sp_w', 'sp_m', 'sp_q', 'hi_q', 'lo_q', 'rk_hv21', 'rk_q']) assert.equal(typeof aapl[k], 'number', k);
  assert.deepEqual(J(aapl._aside), []);
  const pinned = ROWS.find(r => r.why === 'pinned');
  assert.deepEqual(J(pinned._aside), ['hv21', 'hv63', 'sp_d', 'sp_w', 'sp_m', 'sp_q']);
  assert.ok(V.isAside(pinned, 'hv21') && !V.isAside(aapl, 'hv21') && !V.isAside(pinned, null));
  // a wider window is never narrower, and the close sits inside it
  for (const r of ROWS) {
    const s = [r.sp_d, r.sp_w, r.sp_m, r.sp_q].filter(x => x !== null);
    for (let i = 1; i < s.length; i++) assert.ok(s[i] >= s[i - 1] - 0.011, r.t);
    for (const k of 'dwmq') if (r['pos_' + k] !== null) assert.ok(r['pos_' + k] >= 0 && r['pos_' + k] <= 100, r.t + ' pos_' + k);
  }
});

test('every view, board and CSV column resolves, and every cell renders on real rows', () => {
  for (const v of V.VIEWS) for (const id of v[2]) assert.ok(col(id), `${v[0]} names an unknown column ${id}`);
  for (const b of V.BOARDS) { for (const id of b[4]) assert.ok(col(id), `board ${b[0]} names an unknown column ${id}`); assert.ok(col(b[2]), `board ${b[0]} is ranked by an unknown column`); }
  for (const id of V.CSV_COLS) assert.ok(col(id), `the CSV names an unknown column ${id}`);
  let cells = 0;
  for (const id of Object.keys(V.COLS)) for (const r of ROWS) {
    const html = col(id).cell(r);
    assert.match(html, /^<td[ >]/, `${id} on ${r.t}`);
    assert.doesNotMatch(html, /undefined|NaN|\[object|>null</, `${id} on ${r.t}: ${html.slice(0, 140)}`);
    const v = col(id).get(r);
    assert.ok(v === null || ['number', 'string'].includes(typeof v), `${id} sort value on ${r.t} is ${typeof v}`);
    cells++;
  }
  assert.ok(cells > 30000);
});

test('the boards: the engine\'s own lists, in order, from ranked $1B+ common stocks', () => {
  assert.equal(V.BOARDS.length, 13);
  const key = { hvd: V.hvd };
  for (const b of V.BOARDS) {
    const rows = V.boardRows(DOC, BY, b[0]);
    const want = b[0] === 'pinned' ? DOC.pinned : DOC.boards[b[0]];
    assert.deepEqual(J(rows.map(r => r.t)), want, b[0]);
    assert.ok(rows.length > 0 && (b[0] === 'pinned' || rows.length === DOC.board_rule.n), b[0]);
    if (b[0] === 'pinned') continue;
    const get = key[b[2]] || (r => r[b[2]]);
    for (const r of rows) { assert.equal(r.kind, 'stock'); assert.ok(r.mcap >= FLOOR, `${b[0]} ${r.t} is under the floor`); assert.equal(r.why, null, `${b[0]} ${r.t} is set aside`); }
    for (let i = 1; i < rows.length; i++) assert.ok(b[3] < 0 ? get(rows[i]) <= get(rows[i - 1]) + 1e-9 : get(rows[i]) >= get(rows[i - 1]) - 1e-9, `${b[0]} is not in order at ${rows[i].t}`);
  }
  for (const t of DOC.pinned) { assert.equal(BY[t].why, 'pinned'); assert.ok(BY[t].sp_m <= DOC.windows.pin_spread_m, t); assert.equal(BY[t].rk_hv21, null); }
  // the two ends do not meet, and a deal stub is at neither
  assert.ok(BY[DOC.boards.most_volatile[0]].hv21 > 100 && BY[DOC.boards.least_volatile[0]].hv21 < 20);
  assert.ok(!DOC.boards.least_volatile.some(t => DOC.pinned.includes(t)) && !DOC.boards.narrowest_m.some(t => DOC.pinned.includes(t)));
});

test('filters: kind, size, sector, text, and the kinds of row that are not ranked', () => {
  const all = filt({});
  assert.ok(all.length > 400 && all.every(r => r.kind === 'stock'));
  assert.ok(filt({ kind: 'etf' }).every(r => r.kind === 'etf') && filt({ kind: 'etf' }).length > 100);
  const free = filt({ show: 'free' }), pinned = filt({ show: 'pinned' }), aside = filt({ show: 'aside' });
  assert.equal(free.length + pinned.length + aside.length, all.length);
  assert.ok(free.every(r => r.why === null) && pinned.every(r => r.why === 'pinned') && aside.every(r => r.why === 'jump' || r.why === 'print' || r.why === 'thin') && ['jump', 'print', 'thin'].every(w => aside.some(r => r.why === w)));
  assert.deepEqual(J(pinned.map(r => r.t).sort()), DOC.pinned.slice().sort());
  assert.deepEqual(J(V.counts(ROWS, { kind: 'stock', size: 'all', sector: '', q: '' })), { free: free.length, pinned: pinned.length, aside: aside.length, all: all.length });
  const big = filt({ size: '10b' });
  assert.ok(big.length > 20 && big.every(r => r.mcap >= 1e10) && big.length < filt({ size: '2b' }).length && filt({ size: '2b' }).length < filt({ size: '300m' }).length);
  const util = filt({ sector: 'Utilities' });
  assert.ok(util.length > 3 && util.every(r => r.sector === 'Utilities'));
  assert.deepEqual(J(filt({ q: 'nvda' }).map(r => r.t)), ['NVDA']);
  assert.ok(filt({ q: 'apple' }).some(r => r.t === 'AAPL') && filt({ q: 'semiconductors' }).every(r => /semiconductor/i.test(r.industry + r.name)));
  assert.equal(filt({ q: 'zzzz-no-such' }).length, 0);
  assert.equal(V.showOf(BY.AAPL), 'free');
});

test('any column ranks the market both ways, and a blank is last either way', () => {
  const rows = filt({ show: 'free' });
  for (const id of ['hv21', 'hv63', 'hv252', 'yz21', 'atr', 'hvp', 'hvd', 'sp_d', 'sp_w', 'sp_m', 'sp_q', 'own_q', 'mcap', 'px', 'chg', 'rk_hv21', 'usd_q', 'rng_m', 'name', 't']) {
    for (const d of [-1, 1]) {
      const sorted = V.sortRows(rows, col(id), d), vals = sorted.map(r => col(id).get(r));
      assert.equal(sorted.length, rows.length);
      let blank = false;
      for (let i = 0; i < vals.length; i++) {
        const b = vals[i] === null || vals[i] === '';
        assert.ok(!(blank && !b), `${id} ${d}: a reading after a blank`);
        blank = blank || b;
        if (i && !b && typeof vals[i] === 'number') assert.ok(d < 0 ? vals[i] <= vals[i - 1] : vals[i] >= vals[i - 1], `${id} ${d} out of order at ${sorted[i].t}`);
      }
    }
  }
  // sorted by volatility, the first row on screen is the most volatile freely trading stock: its position is its rank
  const top = V.sortRows(rows, col('hv21'), -1)[0];
  assert.equal(top.hv21, Math.max(...rows.map(r => r.hv21)));
  assert.equal(top.rk_hv21, Math.min(...rows.map(r => r.rk_hv21)));
  assert.deepEqual(J(V.nextSort(null, col('hv21'))), { k: 'hv21', d: -1 });
  assert.deepEqual(J(V.nextSort({ k: 'hv21', d: -1 }, col('hv21'))), { k: 'hv21', d: 1 });
  assert.deepEqual(J(V.nextSort({ k: 'hv21', d: 1 }, col('name'))), { k: 'name', d: 1 });
  assert.equal(V.compare(null, 5, -1), 1); assert.equal(V.compare(null, 5, 1), 1); assert.equal(V.compare(3, 5, -1), 1); assert.equal(V.compare(3, 5, 1), -1);
});

test('cells say what the row is: a level without a sign, a change with one, a figure set aside is marked', () => {
  const f = V.fmt;
  assert.equal(f.lev(57.44), '57.4%'); assert.equal(f.lev(4.513, 2), '4.51%'); assert.equal(f.lev(null), '\u2014');
  assert.equal(f.pct(2.345, 2), '+2.35%'); assert.equal(f.pct(-0.31, 2), '\u22120.31%'); assert.equal(f.pct(0.001, 2), '0.00%'); assert.equal(f.pct(-4.8, 1, ' pt'), '\u22124.8 pt');
  assert.equal(f.usd(5177386894000), '$5.18T'); assert.equal(f.usd(21291916688), '$21.3B'); assert.equal(f.usd(804292106434), '$804B'); assert.equal(f.usd(113000000), '$113M'); assert.equal(f.usd(475000), '$475K');
  assert.equal(f.gap(1.1), '$1.10'); assert.equal(f.gap(0.03), '$0.03'); assert.equal(f.gap(0.0042), '$0.0042'); assert.equal(f.gap(1234.5), '$1235');
  assert.equal(f.floor(1e9), '$1B'); assert.equal(f.floor(3e8), '$300M'); assert.equal(f.floor(2.5e9), '$2.5B');
  assert.equal(f.int(4281), '4,281'); assert.equal(f.px(0.4189), '0.4189'); assert.equal(f.px(195.85), '195.85');
  const pinned = BY[DOC.pinned[0]], jump = ROWS.find(r => r.why === 'jump'), thin = ROWS.find(r => r.why === 'thin'), old = ROWS.find(r => r.jd && !r.why);
  assert.match(col('hv21').cell(pinned), /class="aside"/); assert.doesNotMatch(col('hv21').cell(BY.AAPL), /aside/);
  assert.match(col('flag').cell(pinned), /Pinned/); assert.equal(V.flagText(pinned), 'pinned');
  assert.match(col('flag').cell(jump), /Jump/); assert.match(V.flagText(jump), /^jump; jump \d{4}-\d\d-\d\d x/);
  assert.match(col('flag').cell(thin), /Thin \d+ missed/);
  // ops 6855: MYND's banked bar of 2026-10-19 opens at 0.0001 between closes of 0.26 and 0.22 - a print, said on the row
  const mynd = BY.MYND, oldPrint = ROWS.find(r => r.pd && !r.why);
  assert.equal(mynd.why, 'print'); assert.equal(mynd.pd, '2026-10-19'); assert.ok(mynd.pr < 0.001 && mynd.sp_q > 100000 && mynd.rk_q === null);
  assert.match(col('flag').cell(mynd), /Print/); assert.match(col('flag').cell(mynd), /low 2026-10-19 \u00d70\.0005/); assert.match(V.flagText(mynd), /^print; print 2026-10-19 x0\.0005/);
  assert.match(col('sp_q').cell(mynd), /class="aside"/); assert.equal(V.showOf(mynd), 'aside');
  assert.ok(oldPrint, 'the fixture holds a print older than every ranked window'); assert.match(col('flag').cell(oldPrint), /(high|low) \d{4}-\d\d-\d\d/); assert.doesNotMatch(col('sp_q').cell(oldPrint), /aside/);
  assert.ok(old, 'the fixture holds a jump no ranked window spans any more'); assert.match(V.flagText(old), /^jump \d{4}/); assert.doesNotMatch(col('hv21').cell(old), /aside/);
  assert.equal(col('flag').cell(BY.AAPL), '<td class="l na">\u2014</td>');
  // the low, the high and where the close sits between them
  const cell = col('rng_q').cell(BY.AAPL);
  assert.ok(cell.includes(f.px(BY.AAPL.lo_q)) && cell.includes(f.px(BY.AAPL.hi_q)) && cell.includes('left:' + BY.AAPL.pos_q.toFixed(1) + '%'));
  assert.equal(col('usd_q').get(BY.AAPL), BY.AAPL.hi_q - BY.AAPL.lo_q);
  assert.ok(Math.abs((BY.AAPL.hi_q / BY.AAPL.lo_q - 1) * 100 - BY.AAPL.sp_q) < 0.02, 'the spread is the high over the low');
  assert.equal(V.hvd(BY.AAPL), BY.AAPL.hv21 - BY.AAPL.hv21p);
  assert.match(col('t').cell(BY.AAPL), /href="\/chart\.html\?s=AAPL" target="_top" data-t="AAPL"/);
});

test('the verdict sentence states the artifact\'s own medians', () => {
  const s = V.verdictLine(DOC), m = DOC.market.stock;
  assert.ok(s.startsWith('As of ' + DOC.as_of + ' the median listed stock is moving at ' + m.med.hv21.toFixed(1) + '% a year'));
  assert.ok(s.includes(m.med.hv21p.toFixed(1) + '% a month earlier'));
  assert.ok(s.includes(Math.round(m.rising / (m.rising + m.falling) * 100) + '% of stocks are more volatile'));
  assert.ok(s.includes(m.med.sp_d.toFixed(2) + '% for the day') && s.includes(m.med.sp_q.toFixed(1) + '% for the quarter'));
  assert.ok(s.includes('(SPY) reads ' + DOC.market.benchmark.hv21.toFixed(1) + '%'));
  assert.doesNotMatch(s, /undefined|NaN|null/);
});

test('the market\'s year: one line inside a band, clean ticks, every session a point', () => {
  const g = V.chartOf(DOC.history, 'vol', 720, 240);
  assert.equal(g.pts.length, DOC.history.length);
  assert.equal(g.pts.length, 252);
  assert.equal((g.svg.match(/class="sv-line"/g) || []).length, 1);
  assert.equal((g.svg.match(/class="sv-band"/g) || []).length, 1);
  const last = DOC.history[DOC.history.length - 1];
  assert.ok(g.svg.includes('>' + last.med.toFixed(1) + '%</text>'), 'the last reading is labelled at the end of the line');
  assert.ok(g.lo <= Math.min(...DOC.history.map(h => h.p25)) && g.hi >= Math.max(...DOC.history.map(h => h.p75)));
  assert.ok(g.y(g.hi) < g.y(g.lo) && g.x(0) === g.L && Math.abs(g.x(251) - (720 - g.R)) < 1e-6);
  for (const h of DOC.history) assert.ok(h.p25 <= h.med && h.med <= h.p75 && h.n > 1000);
  assert.deepEqual(J(V.ticks(27, 89, 4)), [40, 60, 80]);
  assert.deepEqual(J(V.ticks(0, 4.2, 4)), [0, 2, 4]);
  const s = V.chartOf(DOC.history, 'spread', 360, 240);
  assert.equal(s.band, false); assert.equal((s.svg.match(/class="sv-band"/g) || []).length, 0);
  assert.ok(s.svg.includes('>' + last.sp_d.toFixed(2) + '%</text>'));
  assert.equal(V.chartOf([], 'vol', 720, 240), null); assert.equal(V.chartOf(DOC.history.slice(0, 1), 'vol', 720, 240), null);
  assert.doesNotMatch(g.svg + s.svg, /NaN|undefined/);
});

test('bands, sectors and sizes add up to the ranked stocks', () => {
  const n = DOC.bands.reduce((a, b) => a + b.n, 0);
  assert.equal(n, DOC.universe.ranked.stock.hv21);
  assert.equal(DOC.by_sector.reduce((a, g) => a + g.n, 0), DOC.universe.scanned_stock);
  assert.equal(DOC.by_size.reduce((a, g) => a + g.n, 0), DOC.universe.scanned_stock);
  assert.equal(DOC.universe.scanned, DOC.universe.scanned_stock + DOC.universe.scanned_etf);
  assert.equal(DOC.universe.pinned.stock, DOC.pinned.length);
  assert.equal(DOC.universe.reclassified.length, DOC.universe.left_out['a note, preferred share or warrant the book lists as common stock']);
});

test('the CSV carries every measure of the rows on screen', () => {
  const rows = V.sortRows(filt({ show: 'free', size: '10b' }), col('hv21'), -1), csv = V.csvOf(V.defs(V.CSV_COLS), rows).split('\n');
  assert.equal(csv.length, rows.length + 1);
  assert.equal(csv[0], V.CSV_COLS.join(','));
  const first = csv[1].split(',');
  assert.equal(first[0], rows[0].t);
  assert.equal(Number(first[V.CSV_COLS.indexOf('hv21')]), rows[0].hv21);
  assert.equal(Number(first[V.CSV_COLS.indexOf('lo_q')]), rows[0].lo_q);
  const comma = filt({}).find(r => /,/.test(r.name));
  assert.ok(V.csvOf(V.defs(['t', 'name']), [comma]).split('\n')[1].includes('"' + comma.name + '"'));
});

test('the page: its sections, its artifact, its scripts in order', () => {
  for (const id of ['verdict-line', 'stamp', 'kpis', 'changes', 'hist', 'hist-mode', 'hist-key', 'hist-table', 'bands', 'board-chips', 'board-meta', 'board', 'rows-kind', 'rows-view', 'rows-size', 'rows-sector', 'rows-q',
    'rows-reset', 'rows-csv', 'rows-show', 'rows', 'rows-more', 'rows-meta', 'sectors', 'sizes', 'rules', 'limits', 'sources', 'ops', 'verdict-meta']) assert.ok(HTML.includes('id="' + id + '"'), id);
  for (const id of ['verdict-line', 'stamp', 'kpis', 'changes', 'hist', 'hist-mode', 'hist-key', 'hist-table', 'bands', 'board-chips', 'board-meta', 'board', 'rows-kind', 'rows-view', 'rows-size', 'rows-sector', 'rows-q',
    'rows-reset', 'rows-csv', 'rows-show', 'rows', 'rows-more', 'rows-meta', 'sectors', 'sizes', 'rules', 'limits', 'sources', 'ops', 'verdict-meta']) assert.ok(SRC.includes('"' + id + '"'), 'the script paints ' + id);
  assert.match(HTML, /<body data-research-context="data\/stock-volatility\.json">/);
  const order = ['/private-artifacts.js', '/jh-nav-drawer.js', '/jh-desk-core.js', '/jh-stock-volatility.js'].map(s => HTML.indexOf('src="' + s));
  assert.ok(order.every(i => i > 0) && order.join() === order.slice().sort((a, b) => a - b).join());
  assert.equal((HTML.match(/data-jh-key="/g) || []).length, 7);
  assert.doesNotMatch(HTML, /<script(?![^>]*\bsrc=)[^>]*>/, 'no inline script: the page runs under the site\'s Content-Security-Policy');
  assert.doesNotMatch(SRC, /\beval\(|new Function\(/);
});

test('the chart has a Volatility button right after Leaders, and it opens this desk', () => {
  const engine = fs.readFileSync(path.join(ROOT, 'jh-chart-engine.js'), 'utf8'), rail = fs.readFileSync(path.join(ROOT, 'jh-chart-tvrail.js'), 'utf8');
  const iLdr = engine.indexOf('id=btn-ldr'), iVol = engine.indexOf('id=btn-svol'), iNext = engine.indexOf('id=btn-wysc');
  assert.ok(iLdr > 0 && iVol > iLdr && iNext > iVol);
  assert.equal(engine.slice(iLdr, iVol).split('<button').length, 2, 'nothing but the Leaders button\'s own tail before it');
  assert.equal(engine.split('id=btn-svol').length, 2);
  assert.match(engine, /getElementById\("btn-svol"\); if\(bsvol\) bsvol\.onclick=function\(\)\{ if\(window\.jhOpenWorkspace\) window\.jhOpenWorkspace\("volatility"\); \};/);
  assert.match(rail, /volatility: \["Volatility \u00b7 most and least volatile stocks, price spread", "\/stock-volatility\.html"\]/);
  assert.ok(fs.existsSync(path.join(ROOT, 'stock-volatility.html')));
});
