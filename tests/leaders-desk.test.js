// The leaders desk (leaders.html + jh-leaders.js) against a slice of a real engine artifact (ops 6747 dry run).
// Khalid 2026-11-04: "... I want to be able to sort them ascending and descending. Make sure i can access it from
// chart.html put an icon for it next to 13f".
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const SRC = fs.readFileSync(path.join(ROOT, 'jh-leaders.js'), 'utf8');
const HTML = fs.readFileSync(path.join(ROOT, 'leaders.html'), 'utf8');
const DOC = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/leaders-desk.json'), 'utf8'));

function load() {
  const ctx = { console };
  ctx.window = ctx;
  vm.runInNewContext(SRC, ctx);
  return ctx.JHLeaders;
}
const L = load();
const by = Object.fromEntries(DOC.rows.map(r => [r.ticker, r]));
const J = x => JSON.parse(JSON.stringify(x));          // values built inside the vm realm, compared as plain data

test('every view, board and industry column resolves, and every cell renders on real rows', () => {
  for (const v of [...L.LEADER_VIEWS, ...L.ETF_VIEWS]) for (const id of v[2]) assert.ok(L.COLS[id], `${v[0]} names an unknown column ${id}`);
  for (const id of L.IND_VIEW) assert.ok(L.ICOLS[id], `industries name an unknown column ${id}`);
  for (const b of L.BOARDS) for (const scope of ['leaders', 'etfs']) for (const id of L.boardCols(b[0], scope)) assert.ok(L.COLS[id], `board ${b[0]} names an unknown column ${id}`);
  let cells = 0;
  for (const [reg, rows] of [[L.COLS, [...DOC.rows, ...DOC.etfs]], [L.ICOLS, DOC.industries]]) {
    for (const id of Object.keys(reg)) for (const r of rows) {
      const html = L.cellOf(reg[id], r);
      assert.match(html, /^<td[ >]/, `${id} on ${r.ticker || r.industry}`);
      assert.doesNotMatch(html, /undefined|NaN|\[object|null/, `${id} on ${r.ticker || r.industry}: ${html.slice(0, 120)}`);
      const v = reg[id].get(r);
      assert.ok(v === null || ['number', 'string'].includes(typeof v), `${id} sort value on ${r.ticker || r.industry} is ${typeof v}`);
      cells++;
    }
  }
  assert.ok(cells > 4000);
  // a view opens sorted by a column it shows
  for (const v of L.LEADER_VIEWS) assert.ok(v[2].includes(L.VIEW_SORT[v[0]][0]), v[0]);
  for (const v of L.ETF_VIEWS) assert.ok(v[2].includes(L.ETF_VIEW_SORT[v[0]][0]), v[0]);
});

test('what Khalid asked for is on the page: vs S&P 500, the five averages, crosses, patterns, climax, tight, A/D, smart money, flows', () => {
  const overview = L.viewCols(L.LEADER_VIEWS, 'overview'), ma = L.viewCols(L.LEADER_VIEWS, 'ma');
  for (const id of ['strength', 'vs_m', 'vs_q', 'ladder', 'cross', 'ad', 'smart', 'mf21', 'signals']) assert.ok(overview.includes(id), id);
  assert.deepEqual(J(ma.filter(c => /^d\d+$/.test(c))), ['d20', 'd50', 'd100', 'd200', 'd250']);
  assert.deepEqual(J(L.viewCols(L.LEADER_VIEWS, 'ad').filter(c => /^mf/.test(c))), ['mf5', 'mf21', 'mf21_in', 'mf21_out', 'mf21_pct', 'mf63', 'mfi']);     // money in, out and net
  assert.deepEqual(J(L.viewCols(L.ETF_VIEWS, 'flows').filter(c => /^flow\d/.test(c))), ['flow1', 'flow5', 'flow21', 'flow63']);
  assert.deepEqual(J(L.viewCols(L.ETF_VIEWS, 'ma').filter(c => /^d\d+$/.test(c))), ['d20', 'd50', 'd100', 'd200', 'd250']);
  const ids = L.BOARDS.map(b => b[0]);
  for (const id of ['golden_cross', 'death_cross', 'double_top', 'double_bottom', 'buying_climax', 'selling_climax', 'tight', 'accumulation', 'distribution', 'smart_buying', 'smart_selling']) assert.ok(ids.includes(id), id);
  for (const id of ids) assert.ok(Array.isArray(DOC.boards.leaders[id]), `the engine publishes no ${id} board`);
  for (const id of ['flow5', 'flow21', 'funds', 'f13_ind', 'above50', 'above200', 'ad', 'smart', 'vs_m']) assert.ok(L.IND_VIEW.includes(id), id);
  // the ladder is the five averages in order
  const nv = L.ladder(by.NVDA.ma);
  assert.equal((nv.match(/<i class="(up|dn)"/g) || []).length, 5);
  assert.match(nv, /20d [+−]\d/); assert.match(nv, /250d [+−]\d/);
  assert.ok(nv.endsWith(by.NVDA.ma.above + '/5'));
});

test('sorting: both directions, blanks last either way, text A-Z first, a second click reverses', () => {
  const c = L.compare;
  assert.ok(c(1, 2, 1) < 0 && c(1, 2, -1) > 0 && c(2, 2, 1) === 0);
  for (const dir of [1, -1]) for (const b of [null, undefined, '', NaN]) { assert.ok(c(5, b, dir) < 0, 'a blank sorts after a value'); assert.ok(c(b, 5, dir) > 0); assert.equal(c(b, null, dir), 0); }
  assert.ok(c('AAPL', 'MSFT', 1) < 0 && c('AAPL', 'MSFT', -1) > 0 && c('BRK.B', 'brk.b', 1) === 0 && c('T2', 'T10', 1) < 0);
  const col = L.COLS.vs_y, rows = DOC.rows;
  const has = rows.filter(r => col.get(r) != null).length;
  assert.ok(has < rows.length && has > 5, 'the fixture needs rows with and without a one-year figure');
  for (const dir of [-1, 1]) {
    const out = L.sortRows(rows, col, dir).map(r => col.get(r));
    assert.ok(out.slice(has).every(v => v == null) && out.slice(0, has).every(v => v != null), 'blanks last');
    for (let i = 1; i < has; i++) assert.ok(dir < 0 ? out[i] <= out[i - 1] : out[i] >= out[i - 1]);
  }
  assert.equal(L.sortRows(rows, col, -1).length, rows.length);
  assert.deepEqual(J(L.sortRows(rows, null, -1)), rows);
  // stable: equal keys keep the order they came in
  const same = L.sortRows(rows, L.COLS.sector, 1).filter(r => r.sector === 'Technology').map(r => r.ticker);
  assert.deepEqual(same, rows.filter(r => r.sector === 'Technology').map(r => r.ticker));
  assert.deepEqual(J(L.nextSort(null, L.COLS.vs_m)), { k: 'vs_m', d: -1 });
  assert.deepEqual(J(L.nextSort({ k: 'vs_m', d: -1 }, L.COLS.vs_m)), { k: 'vs_m', d: 1 });
  assert.deepEqual(J(L.nextSort({ k: 'vs_m', d: 1 }, L.COLS.vs_m)), { k: 'vs_m', d: -1 });
  assert.deepEqual(J(L.nextSort({ k: 'vs_m', d: 1 }, L.COLS.ticker)), { k: 'ticker', d: 1 });
  // every column carries a sort value and a kind
  for (const reg of [L.COLS, L.ICOLS]) for (const id of Object.keys(reg)) { assert.equal(typeof reg[id].get, 'function', id); assert.ok(reg[id].type === 'n' || reg[id].type === 's', id); }
});

test('formatting never invents a reading', () => {
  assert.equal(L.fpct(null), '—'); assert.equal(L.fpct(1.234), '+1.2%'); assert.equal(L.fpct(-1.26), '−1.3%'); assert.equal(L.fpct(0.001), '0.0%'); assert.equal(L.fpct(-0.001), '0.0%');
  assert.equal(L.fusd(null), '—'); assert.equal(L.fusd(4.98e12), '$4.98T'); assert.equal(L.fusd(-3.06e9, true), '−$3.06B'); assert.equal(L.fusd(5.7e9, true), '+$5.70B');
  assert.equal(L.fusd(45.7e6, true), '+$45.7M'); assert.equal(L.fusd(4.33e8), '$433M'); assert.equal(L.fusd(0, true), '$0'); assert.equal(L.fusd(244590, true), '+$245K');
  assert.equal(L.fnum(null), '—'); assert.equal(L.fnum(-0.004, 2), '0.00'); assert.equal(L.fnum(-57.8, 0), '−58'); assert.equal(L.fx(1.4812, 1), '1.5×'); assert.equal(L.fx(undefined), '—');
  assert.equal(L.fpx(766.81), '766.81'); assert.equal(L.fpx(1489.39), '1489'); assert.equal(L.fpx(0.1234), '0.1234'); assert.equal(L.fpx(null), '—');
  assert.equal(L.tone(null), 'flat'); assert.equal(L.tone(2), 'up'); assert.equal(L.tone(-2), 'dn');
  assert.equal(L.at({ a: { b: 0 } }, 'a.b'), 0); assert.equal(L.at({ a: null }, 'a.b.c'), null); assert.equal(L.at(null, 'a'), null);
  assert.equal(L.esc('<a href="x">&\'</a>'), '&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;');
  // a row with a short history: no cross, no one-year figure - dashes, dimmed, never a zero
  const sp = by.SPCX;
  assert.equal(sp.cross, null); assert.equal(sp.h.y, null);
  assert.equal(L.cellOf(L.COLS.vs_y, sp), '<td class="flat na">—</td>');
  assert.equal(L.cellOf(L.COLS.cross, sp), '<td class="l na">—</td>');
  assert.match(L.cellOf(L.COLS.cut, sp), /gap/);
});

test('filters and boards', () => {
  const rows = DOC.rows, n = st => rows.filter(L.leaderFilter(st)).map(r => r.ticker);
  assert.equal(n({}).length, rows.length);
  assert.deepEqual(n({ industry: 'Semiconductors' }).sort(), rows.filter(r => r.industry === 'Semiconductors').map(r => r.ticker).sort());
  assert.ok(n({ sector: 'Technology' }).every(t => by[t].sector === 'Technology'));
  assert.ok(n({ rank: '1' }).every(t => by[t].rank === 1) && n({ rank: '1' }).includes('NVDA'));
  assert.ok(n({ rank: 'us' }).every(t => by[t].rank_us != null && by[t].rank_us <= 5) && !n({ rank: 'us' }).includes('TSM') && !n({ rank: 'us' }).includes('ITUB'));     // ADRs carry no ex-ADR rank
  assert.deepEqual(n({ q: 'nvid' }), ['NVDA']); assert.deepEqual(n({ q: 'BRK.B' }), ['BRK.B']); assert.ok(n({ q: 'semiconductors' }).includes('AMD'));
  assert.deepEqual(n({ sig: ['GOLDEN CROSS'] }).sort(), rows.filter(r => r.signals.includes('GOLDEN CROSS')).map(r => r.ticker).sort());
  assert.ok(n({ sig: ['GOLDEN CROSS', 'BUYING CLIMAX'] }).every(t => by[t].signals.includes('BUYING CLIMAX') && by[t].signals.includes('GOLDEN CROSS')));     // signals combine with AND
  const inds = DOC.industries;
  assert.deepEqual(inds.filter(L.industryFilter({ min: 3 })).map(r => r.industry).sort(), inds.filter(r => r.n_leaders >= 3).map(r => r.industry).sort());
  assert.deepEqual(inds.filter(L.industryFilter({ q: 'nvda' })).map(r => r.industry), ['Semiconductors']);
  assert.deepEqual(DOC.etfs.filter(L.etfFilter({ role: 'sector' })).map(r => r.ticker), DOC.etfs.filter(r => r.role === 'sector').map(r => r.ticker));
  assert.ok(DOC.etfs.filter(L.etfFilter({ current: true })).every(r => r.flow && r.flow.current) && DOC.etfs.some(r => !r.flow));
  // a board is the engine's list, in the engine's order, never re-derived here
  for (const b of L.BOARDS) assert.deepEqual(J(L.boardRows(DOC, b[0], 'leaders').map(r => r.ticker)), DOC.boards.leaders[b[0]]);
  assert.deepEqual(J(L.boardRows(DOC, 'tight', 'etfs').map(r => r.ticker)), DOC.boards.etfs.tight);
  assert.deepEqual(J(L.boardCols('golden_cross', 'leaders').slice(0, 3)), ['ticker', 'name', 'industry']);
  assert.deepEqual(J(L.boardCols('golden_cross', 'etfs').slice(0, 3)), ['ticker', 'role', 'covers']);
  assert.ok(!L.boardCols('tight', 'etfs').includes('smart') && L.boardCols('tight', 'leaders').includes('smart'));      // funds carry no insider / verdict leg
  assert.equal(L.boardDef('nope')[0], L.BOARDS[0][0]);
});

test('patterns, climaxes, history and funds read as what they are', () => {
  const failed = DOC.rows.find(r => r.dtop && r.dtop.state === 'FAILED'), conf = DOC.rows.find(r => r.dtop && r.dtop.state === 'CONFIRMED');
  assert.ok(failed && conf);
  assert.match(L.cellOf(L.COLS.dtop, failed), /ld-chip mute[^>]*>FAILED/); assert.match(L.cellOf(L.COLS.dtop, failed), /neckline was reclaimed/);
  assert.match(L.cellOf(L.COLS.dtop, conf), /ld-chip s-dn[^>]*>CONFIRMED/);
  assert.ok(L.COLS.dtop.get(conf) > L.COLS.dtop.get(failed), 'a confirmed top sorts above a failed one');
  assert.ok(!failed.signals.some(s => s.startsWith('DOUBLE TOP')) && conf.signals.includes('DOUBLE TOP CONFIRMED'));
  const met = DOC.rows.find(r => r.dbot && r.dbot.target_hit);
  assert.match(L.cellOf(L.COLS.dbot, met), /target met/);
  const cl = DOC.rows.find(r => r.climax && r.climax.type === 'SELLING');
  assert.match(L.cellOf(L.COLS.climax, cl), /SELLING · \d+d/); assert.match(L.cellOf(L.COLS.climax, cl), /session [+−]/);
  assert.match(L.cellOf(L.COLS.tight, by.AES), /TIGHT · \d+/);
  // history: a stitched series and a cut one say so
  assert.match(L.historyNote(by.BNY.history), /are BK's - the same security under its previous symbol/);
  assert.match(L.historyNote(by.CTVA.history), /a separation \(VYLR listed that session\)/);
  assert.match(L.historyNote(by.LQDA.history), /one-session move ×0\.4281 on 2026-09-30/);
  assert.equal(L.historyNote(by.NVDA.history), ''); assert.equal(L.historyNote(null), '');
  assert.match(L.cellOf(L.COLS.name, by.BNY), /ld-chip mute[^>]*>hist</); assert.match(L.cellOf(L.COLS.name, by.ITUB), />ADR BR</);
  assert.match(L.cellOf(L.COLS.name, by['BRK.B']), /also BRK\.A/);
  // funds: what a fund covers, and a missing flow is said, not zeroed
  const smh = DOC.etfs.find(r => r.ticker === 'SMH'), xlk = DOC.etfs.find(r => r.ticker === 'XLK'), amlp = DOC.etfs.find(r => r.ticker === 'AMLP');
  assert.equal(L.coversText(smh), 'Semiconductors'); assert.match(L.coversText(xlk), /^sector fund for \d+ industries$/);
  assert.match(L.cellOf(L.COLS.flow21, smh), /\+\$3\.35B <span class="dim">\+4\.98%/);
  assert.equal(amlp.flow, null); assert.equal(L.cellOf(L.COLS.flow21, amlp), '<td class="na">—</td>'); assert.match(L.cellOf(L.COLS.flow_src, amlp), /no flow file yet/);
  assert.equal(L.COLS.flow21.get(amlp), null);
  // ticker links open the chart in the top window (the desk also lives inside the chart's workspace)
  assert.equal(L.cellOf(L.COLS.ticker, by['BRK.B']), '<td class="l"><a class="tk" href="/chart.html?s=BRK.B" target="_top" data-t="BRK.B">BRK.B</a></td>');
});

test('csv and the verdict sentence', () => {
  const cols = L.defs(L.COLS, ['ticker', 'name', 'vs_m', 'cross', 'ad', 'signals']);
  const csv = L.csvOf(cols, [by.NVDA, by.ADT, by.SPCX]).split('\n');
  assert.equal(csv[0], 'Ticker,Name,vs SPY 1M,Cross,Accum / dist,Printing now');
  assert.equal(csv.length, 4);
  assert.match(csv[1], /^NVDA,NVIDIA Corporation,-14\.77,/);
  assert.match(csv[2], /^ADT,ADT Inc\.,57\.24,GOLDEN 2026-11-04,STRONG ACCUMULATION,/);
  assert.match(csv[3], /^SPCX,Space Exploration Technologies Corp\. Class A Common Stock|^SPCX,Space Exploration Technologies Corp\.,-19\.36,,/);          // no cross: an empty cell, never a zero
  assert.equal(L.csvOf(L.defs(L.COLS, ['ticker', 'name']), [{ ticker: 'X', name: 'Acme, "Old" Inc.' }]).split('\n')[1], 'X,"Acme, ""Old"" Inc."');      // commas and quotes are quoted
  assert.equal(L.csvOf(L.defs(L.ICOLS, ['industry', 'leaders']), DOC.industries.filter(r => r.industry === 'Semiconductors')).split('\n')[1], 'Semiconductors,NVDA TSM AVGO SKHY MU');
  const line = L.verdictLine(DOC);
  assert.match(line, /^74% of 672 leaders beat SPY over 21 sessions \(SPY −1\.6%\); 67% sit above their 50-session average and 63% above the 200\./);
  assert.match(line, /accumulation in 256 and distribution in 178; the 13F cohort and insiders are buying 145 and selling 171\.$/);
  assert.equal(L.verdictLine({}), '');
});

test('leaders.html: the page order, its tables and its scripts', () => {
  const order = ['id="verdict"', 'id="printing"', 'id="industries"', 'id="leaders"', 'id="funds"', 'id="rules-sec"', 'id="sources-sec"'].map(s => HTML.indexOf(s));
  assert.ok(order.every(i => i > 0) && order.every((v, i) => i === 0 || v > order[i - 1]), 'verdict -> boards -> industries -> leaders -> funds -> reading rules -> sources');
  for (const id of ['kpis', 'changes', 'board', 'board-chips', 'board-scope', 'ind', 'lead', 'lead-view', 'lead-rank', 'lead-sig', 'lead-sector', 'lead-industry', 'lead-q', 'lead-more', 'etf', 'etf-view', 'etf-role', 'rules', 'limits', 'sources', 'history', 'ops', 'stamp', 'verdict-line'])
    assert.ok(HTML.includes(`id="${id}"`), id);
  assert.match(HTML, /<body data-research-context="data\/leaders\.json">/);
  const scripts = [...HTML.matchAll(/<script src="([^"]+)"/g)].map(m => m[1].split('?')[0]);
  assert.deepEqual(scripts, ['/private-artifacts.js', '/jh-nav-drawer.js', '/jh-desk-core.js', '/jh-leaders.js']);
  assert.doesNotMatch(HTML, /\[object Object\]|undefined%|>—</);            // the page gate's forbidden literals and the dead-dash ratchet
  assert.doesNotMatch(HTML, /jh-wire\.js/);
  assert.match(HTML, /<a href="\/leaders\.html" class="on" aria-current="page">Leaders<\/a>/);
  for (const key of ['verdict', 'printing-now', 'industries', 'leaders', 'industry-funds', 'reading-rules', 'sources']) assert.ok(HTML.includes(`data-jh-key="${key}"`), key);
  // every table the client paints carries its own sort headers, so /jh-sortable.js leaves them alone
  assert.match(SRC, /<th scope="col" tabindex="0" data-sort="/); assert.match(SRC, /aria-sort="/);
  assert.match(SRC, /e\.key === "Enter" \|\| e\.key === " "/);
  assert.equal(L.KEY, 'data/leaders.json'); assert.equal(L.CONTRACT, 'leaders.v1');
  // the reskin rewrites colour literals even inside scripts: the client carries none
  assert.doesNotMatch(SRC, /rgba?\(|hsla?\(|#[0-9a-fA-F]{6}\b|#[0-9a-fA-F]{3}\b/);
  // the strength desk links here
  assert.match(fs.readFileSync(path.join(ROOT, 'strong.html'), 'utf8'), /<a href="\/leaders\.html">Leaders<\/a>/);
});
