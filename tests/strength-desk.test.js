// The Strength desk's whole-market read (strong.html, jh-strong-engine.js, jh-strong-today.js, the "strength" tearsheet).
// Khalid 2026-11-20, the S&P 500 down 2%: "today everything is dumping hard however i cant see which stocks and etfs are
// showing strength and dumping less than sp500".
//
// The fixture is real: a cut of the live table (data/strength-wide.json, justhodl-strength 1.1.0) and of the forming-bar
// file of the same instant (data/warm/polygon-full/today.json.gz), with the reads the ENGINE's own code makes of them
// (tests/helpers/strength-fixture.py -> wide.last_read / wide.live_read / wide.live_join). The page ports that
// arithmetic to the browser; these tests hold the port to the engine's figures digit for digit.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), zlib = require('node:zlib');
const root = path.join(__dirname, '..');
const S = require('../jh-strong-engine.js');
const T = require('../jh-strong-today.js');
const kit = require('../jh-tearsheet.js');
const FX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures/strength-desk.json.gz'))).toString('utf8'));
const clone = x => structuredClone(x);
const READ_KEYS = ['n_counted', 'regime', 'groups', 'mcap_1b', 'by_sector', 'by_class', 'industries', 'by_rank', 'rank_col', 'n_ranked_1b', 'by_record', 'record_cols', 'boards'];
const pick = (o, keys) => Object.fromEntries(keys.map(k => [k, o[k]]));
const view = (today, opts) => S.model(clone(FX.wide), today === undefined ? clone(FX.today) : today, Object.assign({ classLabels: FX.class_labels }, opts || {}));
const sum = (list, f) => list.reduce((a, x) => a + f(x), 0);

test('the fixture is the real table and a session after it', () => {
  assert.equal(FX.wide.contract, 'strength-wide.v1');
  assert.ok(FX.wide.rows.length > 2000 && FX.wide.cols.length === 41 && FX.wide.fixture.rows_published > 7000);
  assert.ok(FX.today.session > FX.wide.as_of && Object.keys(FX.today.rows).length > 2000 && FX.today.rows.SPY);
  assert.ok(FX.expect.live && FX.expect.last && FX.expect.live.regime && Object.keys(FX.expect.join).length >= 20);
});

test('pyRound is Python\'s round and nsum is Python 3.12\'s sum', () => {
  // ties go to even on the double's exact value; everything else rounds on the exact value
  assert.deepEqual([S.pyRound(1.25, 1), S.pyRound(0.125, 2), S.pyRound(2.5, 0), S.pyRound(3.5, 0), S.pyRound(-2.5, 0), S.pyRound(0.5, 0), S.pyRound(1.5, 0)], [1.2, 0.12, 2, 4, -2, 0, 2]);
  assert.deepEqual([S.pyRound(2.675, 2), S.pyRound(1.005, 2), S.pyRound(85.71428571428571, 1), S.pyRound(52.94117647058823, 1), S.pyRound(-0.004, 2), S.pyRound(12345.5, 0)], [2.67, 1, 85.7, 52.9, -0, 12346]);
  assert.equal(S.pyRound(null, 2), null);
  assert.equal(S.pyRound(NaN, 2), null);
  // compensated summation: the naive loop loses the small terms, Python's sum does not
  assert.equal(S.nsum([1e16, 1, -1e16]), 1);
  assert.equal(S.nsum([0.1, 0.2, 0.3]), 0.6);
  assert.equal(S.nsum([]), 0);
  assert.equal(S.median([3, 1, 2]), 2);
  assert.equal(S.median([4, 1, 2, 3]), 2.5);
  assert.equal(S.median([]), null);
});

test('the words: up, flat, down less, down more; the regime; the record buckets', () => {
  assert.deepEqual([0.26, 0.25, -0.25, -0.26, -1.99, -2, -3].map(x => S.outcome(x, -2, 0.25)), ['up', 'flat', 'flat', 'down_less', 'down_less', 'down_more', 'down_more']);
  assert.deepEqual([2, 0.5, 0, -0.5].map(x => S.outcome(x, 1, 0.25)), ['up', 'up', 'flat', 'down_more']);
  const rules = FX.wide.rules;
  assert.deepEqual([-1, -0.99, -0.25, 0.25, 0.26, 1].map(x => S.regimeOf(x, rules)), ['dump', 'down', 'flat', 'flat', 'up', 'rally']);
  assert.deepEqual([[3, 3], [3, 2], [6, 4], [6, 3], [6, 2], [6, 1], [3, 0], [2, 2], [null, null]].map(x => S.recordBucket(x[0], x[1], 3)), ['all', 'most', 'most', 'some', 'some', 'few', 'few', null, null]);
  assert.deepEqual([rules.flat_pct, rules.dump_pct, rules.rally_pct, rules.record_min, rules.board_skip_cls], [0.25, -1, 1, 3, ['volatility']]);
});

test('one bar against the close before it', () => {
  const b = S.barRead([100, 104, 99, 103, 1e6, 101], 100, { ret: -2, fo: -1 });
  assert.deepEqual(['ret', 'fo', 'pos', 'x', 'fox'].map(k => +b[k].toFixed(6)), [3, 3, 80, 5, 4]);
  assert.equal(+b.vwx.toFixed(4), 1.9802);
  assert.equal(S.barRead([100, 100, 100, 100, 0, null], 100).pos, null);
  assert.equal(S.barRead([100, 100, 100, 100, 0, null], 100).vwx, null);
});

test('the last session: the page restates the engine\'s read to the last digit', () => {
  const v = view(null);
  assert.equal(v.mode, 'last');
  assert.equal(v.session, FX.wide.as_of);
  assert.equal(v.rank_as_of, FX.wide.prior_session);
  assert.equal(v.rank_col, 'rsp');
  assert.deepEqual(v.record_cols, ['dnp', 'dhp']);
  assert.deepEqual(pick(v.read, READ_KEYS), pick(FX.expect.last, READ_KEYS));
  assert.equal(S.pyRound(v.spy.ret, 4), FX.expect.last.spy_ret);
  // the engine stores the same read in the table itself
  assert.deepEqual(FX.wide.last_session, FX.expect.last);
  assert.ok(v.read.groups.stocks_1b.n > 500 && v.read.by_rank.length === 10 && v.read.by_record.length === 4 && v.read.by_sector.length >= 10);
});

test('today\'s forming bars over the table: the join and the read are the engine\'s', () => {
  const v = view();
  assert.equal(v.mode, 'live');
  assert.equal(v.session, FX.today.session);
  assert.equal(v.rank_as_of, FX.wide.as_of);
  assert.equal(v.rank_col, 'rs');
  assert.deepEqual(v.record_cols, ['dn', 'dh']);
  assert.equal(v.bars_as_of, FX.today.as_of);
  assert.equal(v.delay_minutes, 15);
  assert.deepEqual(pick(v.read, READ_KEYS), pick(FX.expect.live, READ_KEYS));
  assert.equal(S.pyRound(v.spy.ret, 4), FX.expect.live.spy_ret);
  for (const k of ['ret', 'fo', 'pos', 'vwx']) assert.equal(v.spy[k], FX.expect.spy[k], k);
  assert.equal(v.n_no_print, FX.expect.live.n_no_print);
  assert.deepEqual(v.unconfirmed.map(r => r.t), FX.expect.live.unconfirmed);
  const by = Object.fromEntries(v.rows.map(r => [r.t, r]));
  for (const [t, want] of Object.entries(FX.expect.join)) {
    const r = by[t];
    assert.equal(r.ret, want.ret, t);
    assert.equal(r.unconfirmed, want.unconfirmed, t);
    assert.deepEqual(r.split, want.split, t);
    assert.equal(!!r.bar, want.has_bar, t);
    if (want.live) for (const k of ['ret', 'fo', 'pos', 'vwx', 'x', 'fox']) assert.equal(r.live[k], want.live[k], `${t}.${k}`);
    if (want.prev != null) assert.equal(r.prev, want.prev, t);
  }
  assert.equal(v.gap_sessions, 1);
});

test('a split announced for today divides the last close; a move no split explains is shown and not counted', () => {
  const v = view(), by = Object.fromEntries(v.rows.map(r => [r.t, r])), ahead = FX.wide.splits_ahead;
  const today = Object.keys(ahead).filter(t => ahead[t].date <= FX.today.session && by[t] && by[t].bar);
  assert.ok(today.length >= 1, 'the fixture holds a split in force on the forming session');
  for (const t of today) {
    const r = by[t], sa = ahead[t];
    assert.equal(r.prev, r.c * sa.from / sa.to, t);
    assert.deepEqual(r.split, sa, t);
    if (!r.unconfirmed) assert.ok(Math.abs(r.ret) < 80, `${t}: ${r.ret}`);
  }
  assert.deepEqual(v.splits_today.map(r => r.t).sort(), today.sort());
  // SanDisk split 8-for-1 on 2026-11-20: against the raw close it would read -89%
  if (by.SNDK && by.SNDK.split) { assert.equal(by.SNDK.split.to, 8); assert.ok(by.SNDK.ret > -40 && by.SNDK.bar[3] / by.SNDK.c < 0.2); }
  for (const r of v.unconfirmed) { assert.equal(r.ret, null); assert.equal(r.out, null); assert.equal(r.counted, false); }
  // a made-up collapse with no split on record
  const t2 = clone(FX.today); t2.rows.AAPL = t2.rows.AAPL.slice(); t2.rows.AAPL[3] = by.AAPL.c * 0.1;
  const v2 = view(t2), a2 = v2.rows.find(r => r.t === 'AAPL');
  assert.equal(a2.unconfirmed, true);
  assert.equal(a2.ret, null);
  assert.ok(v2.unconfirmed.some(r => r.t === 'AAPL') && v2.read.groups.sp500.n === v.read.groups.sp500.n - 1);
});

test('no live session when the forming file is the table\'s own session, older, empty or absent', () => {
  for (const today of [Object.assign(clone(FX.today), { session: FX.wide.as_of }), Object.assign(clone(FX.today), { session: '2026-01-02' }), { session: FX.today.session, rows: {} }, null, {}]) {
    const v = view(today);
    assert.equal(v.mode, 'last');
    assert.equal(v.session, FX.wide.as_of);
    assert.equal(v.gap_sessions, null);
  }
  assert.equal(S.liveJoin(clone(FX.wide), null), null);
  assert.equal(S.weekdaysBetween('2026-11-20', '2026-11-23'), 1);       // Friday to Monday
  assert.equal(S.weekdaysBetween('2026-11-19', '2026-11-20'), 1);
  assert.equal(S.weekdaysBetween('2026-11-18', '2026-11-20'), 2);       // a missed table: the page says so
});

test('what each row gains: the rank carried in, the bar\'s five facts, the volume pace', () => {
  const v = view(), R = FX.wide.rules;
  let counted = 0;
  for (const r of v.rows) {
    assert.equal(r.rank, r.rs);
    assert.equal(r.rec_n, r.dn);
    assert.equal(r.rec_h, r.dh);
    assert.equal(r.bucket, S.recordBucket(r.dn, r.dh, R.record_min));
    assert.equal(r.counted, r.ret != null && !r.why);
    if (r.counted) counted++;
    if (r.ret == null) { assert.equal(r.ticks, null); continue; }
    assert.equal(r.out, S.outcome(r.ret, v.spy.ret, R.flat_pct));
    assert.equal(r.x, r.ret - v.spy.ret);
    const facts = [r.ret > R.flat_pct, r.fo == null ? null : r.fo > 0, r.vwx == null ? null : r.vwx > 0, r.pos == null ? null : r.pos >= 200 / 3, r.rv == null ? null : r.rv >= 1.2];
    assert.deepEqual(r.facts, facts);
    assert.equal(r.ticks, facts.filter(f => f === true).length);
    assert.equal(r.tickN, facts.filter(f => f != null).length);
  }
  assert.equal(counted, v.read.n_counted);
  // the pace is the median counted name's volume against its own average: half the counted names trade above 1.0x
  const rv = v.rows.filter(r => r.counted && r.rv != null).map(r => r.rv);
  assert.ok(Math.abs(rv.filter(x => x >= 1).length / rv.length - 0.5) < 0.02 && v.pace > 0);
  // last-session mode: the rank and the record are one session older
  const L = view(null), l = L.rows.find(r => r.t === 'AAPL');
  assert.equal(l.rank, l.rsp);
  assert.equal(l.rec_n, l.dnp);
  assert.deepEqual(l.bar, [l.o, l.h, l.l, l.c, l.v, l.vw]);
  assert.equal(l.prev, l.pc);
});

test('set-aside rows are shown and never counted: deals, geared funds, jumps, thin trading', () => {
  const v = view(), aside = v.rows.filter(r => r.why);
  assert.ok(aside.length > 100);
  for (const w of ['pinned', 'geared', 'jump']) assert.ok(aside.some(r => r.why === w), w);
  assert.ok(aside.every(r => !r.counted && r.rs == null));
  const board = new Set(Object.values(v.read.boards).flat());
  assert.ok(aside.every(r => !board.has(r.t)));
  // a long-volatility fund is counted in its class and kept off the ETF boards
  const vol = v.rows.filter(r => r.cls === 'volatility' && r.counted);
  assert.ok(vol.length >= 1 && vol.every(r => !v.read.boards.etf_up.includes(r.t) && !v.read.boards.etf_down_most.includes(r.t)));
});

test('the sentence at the top says what the session is, in the reader\'s words', () => {
  const v = view(), line = T.verdictLine(v), g = v.read.groups.stocks_1b;
  assert.match(line, /^Today, as of \d{1,2}:\d{2} [AP]M ET: SPY [+−]\d+\.\d{2}%\. /);
  assert.ok(line.includes(g.held_pct.toFixed(0) + '% of stocks worth $1B or more are holding up'), line);
  assert.ok(line.includes('among S&P 500 members ' + v.read.groups.sp500.held_pct.toFixed(0) + '% are holding up'), line);
  assert.ok(line.includes('Holding best: ' + v.read.by_sector[0].sector) && line.includes('breaking most: ' + v.read.by_sector[v.read.by_sector.length - 1].sector), line);
  assert.ok(line.includes('The strongest tenth by rank coming in: ' + v.read.by_rank[9].held_pct.toFixed(0) + '% holding up, against ' + v.read.by_rank[0].held_pct.toFixed(0) + '% of the weakest tenth'), line);
  const rec = v.read.by_record;
  assert.ok(line.includes('Of the ' + rec[0].n.toLocaleString('en-US') + ' that had held up on every dump day of the quarter, ' + rec[0].held_pct.toFixed(0) + '% are holding up again; of the ' + rec[3].n.toLocaleString('en-US') + ' that had held on fewer than a third, ' + rec[3].held_pct.toFixed(0) + '%.'), line);
  assert.match(T.verdictLine(view(null)), /^Last session \(\d{4}-\d{2}-\d{2}\): SPY /);
  assert.ok(/ held up \(up, flat, or down less than SPY\) and \d+% were up/.test(T.verdictLine(view(null))) && !/are holding up|are up/.test(T.verdictLine(view(null))), 'a finished session is told in the past tense');
  assert.doesNotMatch(line + T.verdictLine(view(null)), /undefined|NaN|null/);
  // a session SPY is not down: the headline is the share ahead of it
  const up = clone(FX.today); up.rows.SPY = up.rows.SPY.slice(); up.rows.SPY[3] = FX.wide.benchmark.c * 1.012;
  const vu = view(up);
  assert.equal(vu.read.regime, 'rally');
  assert.match(T.verdictLine(vu), /are ahead of SPY and \d+% are up/);
});

test('the table\'s filters: kind, size, outcome, what was carried in, set-aside rows, search', () => {
  const v = view(), R = v.rules, base = { kind: 'stock', scope: 'b1', out: 'auto', rank: 'all', show: 'counted', sector: '', q: '' };
  const n = st => v.rows.filter(T.rowFilter(v, Object.assign({}, base, st))).length;
  const g = v.read.groups;
  assert.equal(n({}), g.stocks_1b.held);                                   // a red session opens on the names holding up
  assert.equal(n({ out: 'all' }), g.stocks_1b.n);
  assert.equal(n({ out: 'up' }), g.stocks_1b.up);
  assert.equal(n({ out: 'down_more' }), g.stocks_1b.down_more);
  assert.equal(n({ out: 'all', scope: 'sp' }), g.sp500.n);
  assert.equal(n({ out: 'all', scope: 'b10' }), g.stocks_10b.n);
  assert.equal(n({ out: 'all', scope: 'all' }), g.stocks.n);
  assert.equal(n({ kind: 'etf', out: 'all' }), g.etfs_liquid.n);
  assert.equal(n({ kind: 'etf', scope: 'all', out: 'all' }), g.etfs.n);
  assert.equal(n({ kind: 'all', scope: 'all', out: 'all' }), v.read.n_counted);
  assert.equal(n({ out: 'all', rank: 'record' }), v.read.by_record[0].n);
  assert.equal(n({ out: 'all', rank: 'strong' }), v.rows.filter(r => r.counted && r.kind === 'stock' && (r.mcap || 0) >= R.board_min_mcap && r.rank >= 80).length);
  assert.equal(n({ kind: 'all', scope: 'all', out: 'all', show: 'aside' }), v.rows.filter(r => !r.counted && r.bar).length);
  assert.equal(n({ kind: 'all', scope: 'all', out: 'all', show: 'all' }), v.rows.length);
  assert.equal(n({ out: 'all', sector: 'Utilities' }), v.read.by_sector.find(s => s.sector === 'Utilities').n);
  assert.deepEqual(v.rows.filter(T.rowFilter(v, Object.assign({}, base, { out: 'all', scope: 'all', q: 'wmt' }))).map(r => r.t), ['WMT']);
  // a session that is not red opens on every name
  const up = clone(FX.today); up.rows.SPY = up.rows.SPY.slice(); up.rows.SPY[3] = FX.wide.benchmark.c * 1.012;
  const vu = view(up);
  assert.equal(vu.rows.filter(T.rowFilter(vu, base)).length, vu.read.groups.stocks_1b.n);
});

test('the CSV carries every column of every view, the outcome as a word', () => {
  const v = view(), rows = ['AAPL', 'WMT', 'XLP'].map(t => v.rows.find(r => r.t === t)), cols = Object.keys(T.COL).map(k => T.COL[k]);
  for (const id of new Set([].concat(T.VIEWS.today, T.VIEWS.record, T.VIEWS.trend))) assert.ok(T.COL[id], id);
  const lines = T.csvOf(cols, rows, v.class_labels).split('\n'), head = lines[0].split(',');
  assert.equal(lines.length, 4);
  assert.deepEqual(head.slice(0, 4), ['t', 'name', 'where', 'industry']);
  assert.deepEqual(head.slice(-4), ['sector', 'kind', 'set_aside', 'outcome']);
  assert.equal(head.filter(h => h === 'industry').length, 1);
  const cell = (line, id) => line.split(',')[head.indexOf(id)];                // none of these three names holds a comma
  for (const [i, r] of rows.entries()) {
    assert.equal(cell(lines[i + 1], 't'), r.t);
    assert.equal(Number(cell(lines[i + 1], 'ret')), Math.round(r.ret * 10000) / 10000);
    assert.equal(cell(lines[i + 1], 'out'), r.out);
    assert.equal(cell(lines[i + 1], 'dump'), r.rec_h + '/' + r.rec_n);
    assert.equal(cell(lines[i + 1], 'kind'), r.kind);
  }
  assert.equal(cell(lines[3], 'where'), 'Equity ETFs');
  assert.equal(cell(lines[2], 'where'), 'Consumer Defensive');
  assert.doesNotMatch(lines.join('\n'), /undefined|NaN|\[object/);
});

// what a sheet prints: a list's `full` holds the desk's own rows (blanks and all) for the kit to rank; its cells are tested where they are drawn
const shown = (k, v) => (k === 'full' ? undefined : v);

test('the tearsheet: a ring of market value by outcome, tiles that restate the read, lists by what was carried in', () => {
  const v = view(), ad = kit.adapters.strength, m = ad.build([clone(v)], {});
  assert.deepEqual(m.controls.map(c => [c.id, c.label, c.on]), [['b1', '$1B+ STOCKS', true], ['sp', 'S&P 500', false], ['all', 'EVERY STOCK', false], ['etf', 'ETFS', false]]);
  assert.equal(m.tiles.length, 6);
  assert.equal(m.notes.length, 3);
  assert.equal(m.cards.length, 2);
  assert.doesNotMatch(JSON.stringify(m, shown), /undefined|NaN|Infinity|\[object|\bnull\b/);
  const R = v.rules, scope = v.rows.filter(r => r.counted && r.kind === 'stock' && (r.mcap || 0) >= R.board_min_mcap), g = v.read.groups.stocks_1b;
  // the ring is the scope's market value, each name once
  const ring = sum(m.ring.slices, s => s.value);
  assert.ok(Math.abs(ring - sum(scope, r => r.mcap)) < 1);
  assert.deepEqual(m.ring.slices.map(s => s.name), ['Up', 'Flat', 'Down less than SPY', 'Down more than SPY']);
  assert.deepEqual(m.ring.slices.map(s => s.tone), ['pos', 'pos', 'pos', 'oth']);                      // one hue stepping down for the three that hold; the rest stands apart
  assert.deepEqual(m.ring.slices.slice(0, 3).map(s => s.op), [1, 0.62, 0.4]);
  for (const [i, k] of ['up', 'flat', 'down_less', 'down_more'].entries()) assert.ok(m.ring.slices[i].small.startsWith(g[k].toLocaleString('en-US') + ' stock'), m.ring.slices[i].small);
  assert.equal(m.ring.n, g.n);
  // the tiles restate the engine's read of the scope
  assert.equal(m.tiles[1].k, 'Holding up');
  assert.equal(m.tiles[1].v, g.held_pct.toFixed(0) + '%');
  assert.ok(m.tiles[1].s.includes('up ' + g.up.toLocaleString('en-US')) && m.tiles[1].s.includes('down less ' + g.down_less.toLocaleString('en-US')));
  assert.equal(m.tiles[2].v, (100 * g.down_more / g.n).toFixed(0) + '%');
  assert.equal(m.tiles[5].v, v.read.by_record[0].held_pct.toFixed(0) + '%');
  assert.ok(m.title.startsWith('SPY ') && m.title.includes('today') && m.title.includes(g.held_pct.toFixed(0) + '% of stocks worth $1B or more are holding up'));
  assert.ok(m.eyebrow.includes('bars as of') && m.eyebrow.includes('15 minutes delayed') && m.foot.includes('justhodl-polygon-today'));
  // the lists
  const list = key => m.lists.find(l => l.key === key), tick = l => l.rows.map(r => r.name.split(' · ')[0]);
  assert.deepEqual(m.lists.map(l => l.key), ['up', 'down-less', 'strong-holding', 'strong-breaking', 'weak-bid', 'repeat', 'groups', 'industries-holding', 'industries-breaking']);
  assert.equal(list('up').n, g.up);
  assert.deepEqual(tick(list('up')).slice(0, 25), v.read.boards.up);
  assert.deepEqual(tick(list('down-less')).slice(0, 25), v.read.boards.down_less);
  assert.deepEqual(tick(list('strong-holding')).slice(0, 25), v.read.boards.strong_holding);
  assert.deepEqual(tick(list('strong-breaking')).slice(0, 25), v.read.boards.strong_breaking);
  assert.deepEqual(tick(list('weak-bid')).slice(0, 25), v.read.boards.weak_bid);
  assert.deepEqual(tick(list('repeat')).slice(0, 25), v.read.boards.repeat);
  assert.deepEqual(list('groups').rows.map(r => r.name), v.read.by_sector.slice().sort((a, b) => (b.held_pct - a.held_pct) || (b.median_ret - a.median_ret) || (a.sector < b.sector ? -1 : 1)).map(s => s.sector));
  assert.equal(list('industries-holding').n, v.read.industries.n);
  // nothing on the sheet is coloured by direction alone: every row states its move with its sign
  assert.ok(list('up').rows.every(r => /^\+/.test(r.big)) && list('down-less').rows.every(r => /^−/.test(r.big)));
});

test('the tearsheet follows the scope and the session', () => {
  const v = view(), ad = kit.adapters.strength, R = v.rules;
  const sp = ad.build([clone(v)], { fund: 'sp' }), all = ad.build([clone(v)], { fund: 'all' }), etf = ad.build([clone(v)], { fund: 'etf' });
  assert.equal(sp.ring.n, v.read.groups.sp500.n);
  assert.equal(sp.tiles[1].v, v.read.groups.sp500.held_pct.toFixed(0) + '%');
  assert.equal(all.ring.n, v.read.groups.stocks.n);
  assert.equal(etf.ring.n, v.read.groups.etfs_liquid.n);
  assert.equal(etf.tiles[1].v, v.read.groups.etfs_liquid.held_pct.toFixed(0) + '%');
  assert.deepEqual(etf.lists.map(l => l.key), ['up', 'down-less', 'strong-holding', 'strong-breaking', 'weak-bid', 'repeat', 'groups', 'down-most-etf']);
  assert.equal(etf.lists.find(l => l.key === 'groups').title, 'Asset classes');
  assert.ok(Math.abs(sum(etf.ring.slices, s => s.value) - sum(v.rows.filter(r => r.counted && r.kind === 'etf' && (r.adv || 0) >= R.etf_min_adv), r => r.mcap || 0)) < 1);
  // the funds that hold volatility itself are counted in the ring and the tiles and left off the ETF lists of names (the engine's own rule for its
  // boards, rules.board_skip_cls); the list says how many, and the stock scopes are untouched
  assert.deepEqual(R.board_skip_cls, ['volatility']);
  const vol = v.rows.filter(r => r.counted && r.kind === 'etf' && (r.adv || 0) >= R.etf_min_adv && r.cls === 'volatility');
  assert.ok(vol.length >= 2 && vol.some(r => r.out === 'up'), 'the fixture holds liquid volatility funds, one of them up');
  const listed = new Set([].concat(...etf.lists.filter(l => l.key !== 'groups').map(l => l.rows.map(r => r.name.split(' \u00b7 ')[0]))));
  for (const r of vol) assert.ok(!listed.has(r.t), r.t);
  assert.equal(etf.lists[0].n, v.rows.filter(r => r.counted && r.kind === 'etf' && (r.adv || 0) >= R.etf_min_adv && r.cls !== 'volatility' && r.out === 'up').length);
  assert.ok(etf.lists[0].note.startsWith(vol.length + ' funds that hold volatility itself'), etf.lists[0].note);
  assert.ok(!etf.cards.some(c => vol.some(r => c.title.startsWith(r.t + ' '))));
  assert.ok(!sp.lists[0].note && !all.lists[0].note);
  assert.equal(ad.build([clone(v)], { fund: 'zz' }).controls.find(c => c.on).id, 'b1');
  // the market shut: the last session, final, no word about a delay
  const L = ad.build([clone(view(null))], {});
  assert.ok(L.eyebrow.includes('last session ' + FX.wide.as_of + ' (final)') && !L.eyebrow.includes('delayed') && !L.foot.includes('polygon-today'));
  assert.ok(L.title.includes('on ' + FX.wide.as_of) && L.notes[2][1].startsWith('this is the last finished session'));
  assert.equal(L.tiles[1].v, view(null).read.groups.stocks_1b.held_pct.toFixed(0) + '%');
  // a finished session is told in the past tense
  assert.equal(L.tiles[1].k, 'Held up');
  assert.ok(/ held up( |$)/.test(L.title) && !/are holding up|is holding up|holding again|breaking/.test(JSON.stringify([L.title, L.tiles.map(t => t.k), L.lists.map(l => l.title), L.notes, L.ring.caption, L.ring.subs])));
  assert.deepEqual(L.lists.slice(0, 6).map(l => l.title), ['Up while SPY fell', 'Down less than SPY', 'Strong coming in, held up', 'Strong coming in, broke', 'Weak coming in, bid', 'Held every dump day, held again']);
  // a session SPY is not down
  const up = clone(FX.today); up.rows.SPY = up.rows.SPY.slice(); up.rows.SPY[3] = FX.wide.benchmark.c * 1.012;
  const U = ad.build([clone(view(up))], {});
  assert.equal(U.tiles[1].k, 'Ahead of SPY');
  assert.deepEqual(U.lists.slice(0, 2).map(l => l.key), ['up', 'down-most']);
  assert.doesNotMatch(JSON.stringify([sp, all, etf, L, U], shown), /undefined|NaN|Infinity|\[object|\bnull\b/);
});

test('the sheet takes the desk\'s view, on every refresh, and reads the table itself only if none arrives', () => {
  const ad = kit.adapters.strength, html = fs.readFileSync(path.join(root, 'strong.html'), 'utf8');
  assert.deepEqual(ad.share, { global: 'JHStrengthDoc', event: 'jh:strength-doc', ms: 15000 });
  assert.deepEqual(ad.feeds, ['/data/strength-wide.json']);
  assert.ok(html.includes('window.JHStrengthDoc=d.market') && html.includes('new CustomEvent("jh:strength-doc",{detail:d.market})'));
  assert.throws(() => ad.build([clone(FX.wide)], {}), /has not built its view/);            // in a page, window.JHStrong.model builds it
});

test('strong.html: the two market-wide sections come first, every section the desk had is still there under its own number', () => {
  const html = fs.readFileSync(path.join(root, 'strong.html'), 'utf8');
  const at = s => { const i = html.indexOf(s); assert.ok(i >= 0, s); return i; };
  const order = ['id="verdict-line"', 'data-jts="strength"', 'id="today"', 'id="names"', 'data-jh-key="verdict-index-regime-breadth-and-bases"', 'data-jh-key="thesis-the-strongest-when-the-s-p-dumps"', 'data-jh-key="asset-classes"',
    'data-jh-key="long-accumulation-tight-price-spread"', 'data-jh-key="leaders-and-laggards"', 'data-jh-key="focus-holding-up-while-the-index-is-heav"', 'data-jh-key="the-desk"', 'data-jh-key="reading-rules"', 'data-jh-key="sources-and-engine-observability"'];
  const pos = order.map(at);
  assert.deepEqual(pos, pos.slice().sort((a, b) => a - b));
  // the keys the section registry already numbers (config/section-registry.json) are pinned, so no address moves
  const reg = JSON.parse(fs.readFileSync(path.join(root, 'config/section-registry.json'), 'utf8')).pages.strong.sections.map(s => s.key);
  for (const key of reg) assert.ok(html.includes(`data-jh-key="${key}"`), key);
  // The nightly registry crawl (scripts/build_section_registry.py) calls JustHodlSections.canonical(), which DROPS every pinned key and derives
  // them again: a stable id, else the title. So a pin only holds when it equals that derivation - a section with an id is keyed by the id, and
  // one whose heading changed carries its registered title in data-jh-title.
  assert.match(html, /<section class="sx-sec" id="today" data-jh-key="today" data-jh-title="Against the S&P 500"/);
  assert.match(html, /<section class="sx-sec" id="names" data-jh-key="names" data-jh-title="Every stock and ETF"/);
  assert.match(html, /data-jh-key="verdict-index-regime-breadth-and-bases" data-jh-title="Verdict: index regime, breadth and bases"/);
  const regStrong = JSON.parse(fs.readFileSync(path.join(root, 'config/section-registry.json'), 'utf8')).pages.strong.sections;
  assert.deepEqual(regStrong.filter(x => ['today', 'names'].includes(x.key)).map(x => [x.n, x.key, x.sub.map(y => y.n + ' ' + y.key)]),
    [['13', 'today', ['13.1 today/today-kpis', '13.2 today/today-who', '13.3 today/today-carried', '13.4 today/today-groups']], ['14', 'names', []]]);
  for (const id of ['today-kpis', 'today-who', 'today-carried', 'today-groups']) assert.ok(html.includes(`id="${id}"`), id);
  // the two cards are found as panels only while they carry no data-jh-title (a titled child reads as its wrapper's heading), and the control
  // rows and KPI tiles are not panels at any width
  assert.ok(!/class="sx-card"[^>]*data-jh-title/.test(html));
  assert.ok(!html.includes('<div class="sx-row">') && html.split('<div class="sx-row" data-jh-chrome>').length === 6);
  assert.ok(fs.readFileSync(path.join(root, 'jh-strong-today.js'), 'utf8').includes('<div class="jh-kpi" data-jh-chrome>'));
  // a phone gets the same fifteen columns as a desk, the answer first
  assert.deepEqual(T.PHONE_TODAY.slice(0, 5), ['t', 'name', 'ret', 'x', 'out']);
  assert.deepEqual(T.PHONE_TODAY.slice().sort(), T.VIEWS.today.slice().sort());
  for (const id of ['kpis', 'thesis-win', 'thesis-verdict', 'thesis-kpis', 'dumps', 'rallies', 'thesis-pred', 'lastnames', 'classes', 'excluded', 'coil-scope', 'coil', 'lead-scope', 'leaders', 'laggards', 'newhighs', 'focus', 'chips', 'tbl', 'defs', 'sources', 'note', 'stamp',
    'today-h', 'today-meta', 'today-sub', 'today-kpis', 'today-bars', 'today-key', 'carried-h', 'carried-mode', 'carried', 'carried-note', 'today-group', 'today-groups',
    'names-meta', 'names-kind', 'names-scope', 'names-out', 'names-rank', 'names-show', 'names-view', 'names-sector', 'names-q', 'names-reset', 'names-csv', 'names-tbl', 'names-more', 'names-aside']) assert.ok(html.includes(`id="${id}"`), id);
  const scripts = [...html.matchAll(/<script src="([^"?]+)/g)].map(m => m[1]);
  assert.deepEqual(scripts, ['/private-artifacts.js', '/jh-nav-drawer.js', '/jh-desk-core.js', '/jh-strong-engine.js', '/jh-strong-today.js', '/jh-tearsheet.js']);
  assert.doesNotMatch(html, /#26ffaf|#ff5577|rgba\(38,255,175/, 'colours come from the theme tokens');
  assert.match(html, /<a href="\/leaders\.html">Leaders<\/a>/);
  assert.match(html, /STRENGTH vs S/);
});

test('the engine reads three feeds and nothing else; a missing table never costs the desk its sections', async () => {
  const keys = [];
  const desk = { contract: 'strength.v1', as_of: FX.wide.as_of, benchmark: { px: 715.87, h: { d: -1.96 } }, sleeves: { ETF: [] }, classes: { labels: FX.class_labels }, rows: [{ ticker: 'QQQ', scopes: ['desk'], strength: 20, posture: { code: 'LAG' } }] };
  const load = feed => { const D = { quotes: async () => ({}), feed: async key => { keys.push(key); return feed(key); } }, w = { JHDesk: D }; const src = fs.readFileSync(path.join(root, 'jh-strong-engine.js'), 'utf8');
    new Function('window', 'globalThis', 'module', src)(w, w, undefined); return w.JHStrong; };
  const E = load(key => key === 'data/strength.json' ? desk : key === 'data/strength-wide.json' ? clone(FX.wide) : key === 'data/warm/polygon-full/today.json.gz' ? clone(FX.today) : null);
  const d = await E.run();
  assert.deepEqual(keys, ['data/strength.json', 'data/strength-wide.json', 'data/warm/polygon-full/today.json.gz']);
  assert.equal(d.market.mode, 'live');
  assert.equal(d.marketError, null);
  assert.deepEqual(pick(d.market.read, READ_KEYS), pick(FX.expect.live, READ_KEYS));
  assert.equal(d.desk.length, 1);
  // the table is kept for ten minutes; the forming bars are read again on every refresh
  keys.length = 0;
  await E.run();
  assert.deepEqual(keys, ['data/warm/polygon-full/today.json.gz']);
  // no table: the desk still draws, the market sections say why
  const E2 = load(key => key === 'data/strength.json' ? desk : null), d2 = await E2.run();
  assert.equal(d2.market, null);
  assert.match(d2.marketError, /strength-wide\.json is not published yet/);
  assert.equal(d2.rows.length, 1);
  // the forming file fails mid-session: the last one read stands in, the page does not fall back to yesterday
  let fail = false;
  const E3 = load(key => key === 'data/strength.json' ? desk : key === 'data/strength-wide.json' ? clone(FX.wide) : fail ? null : clone(FX.today));
  assert.equal((await E3.run()).market.mode, 'live');
  fail = true;
  assert.equal((await E3.run()).market.mode, 'live');
});

// ---------------------------------------------------------------------------------------------------------------------
// Khalid 2026-11-20 17:46 ET: "you are only displaying the first six of each category - when i click on that category i
// should be able to view the entire list and sort them by market cap and by industry and sort them ascending and
// descending". Every list of the sheet opens in full (kit 1.6.0): all of its rows, ranked by any column either way.
const every = m => m.lists.concat(m.more || []);
const tickers = rows => rows.map(r => r.t);

test('every category opens in full: the whole list behind each card, each ring slice and the ring itself', () => {
  const v = view(), ad = kit.adapters.strength, R = v.rules;
  for (const fund of ['b1', 'sp', 'all', 'etf']) {
    const m = ad.build([clone(v)], { fund }), lists = every(m), by = Object.fromEntries(lists.map(l => [l.key, l]));
    // each card opens, and what opens is the card's own list: the same count, the same first rows in the same order
    for (const l of m.lists) {
      assert.ok(l.full && l.short, fund + ' ' + l.key);
      if (l.key.startsWith('industries')) { assert.ok(l.full.rows.length >= l.rows.length && l.full.rows.length === l.n, l.key); continue; }
      assert.equal(l.full.rows.length, l.n, fund + ' ' + l.key);
      assert.deepEqual(l.full.rows.slice(0, 15).map(r => r.t || r.name), l.rows.slice(0, 15).map(r => r.name.split(' · ')[0]), fund + ' ' + l.key);
    }
    // the ring opens every counted name of the scope; each slice opens the names of its outcome
    assert.equal(m.ring.open, 'all');
    assert.equal(by.all.full.rows.length, m.ring.n);
    assert.ok(by.all.card === false && by.flat.card === false && !m.lists.some(l => l.card === false));
    const skip = fund === 'etf' ? (R.board_skip_cls || []) : [];
    for (const [i, out] of ['up', 'flat', 'down_less', 'down_more'].entries()) {
      const l = by[m.ring.slices[i].open];
      assert.ok(l && l.full, fund + ' ' + out + ' -> ' + m.ring.slices[i].open);
      assert.ok(l.full.rows.every(r => r.out === out), fund + ' ' + out);
      assert.equal(l.full.rows.length, by.all.full.rows.filter(r => r.out === out && !skip.includes(r.cls)).length, fund + ' ' + out);
    }
    // the four outcomes are the whole scope, each name once (the funds that hold volatility are in "all" and on no list of names)
    const named = by.all.full.rows.filter(r => !skip.includes(r.cls));
    assert.equal(sum(['up', 'flat', 'down_less', 'down_more'], (o) => by[m.ring.slices[['up', 'flat', 'down_less', 'down_more'].indexOf(o)].open].full.rows.length), named.length);
    assert.deepEqual(every(m).map(l => l.key).filter(k => !m.order.includes(k)), [], 'every list has its place among the category buttons');
    // market value and (for companies) industry are columns of every list of names
    const cols = by.up.full.cols.map(c => c.id);
    assert.ok(cols.includes('mcap') && cols.includes('sector') && (fund === 'etf' ? cols.includes('adv') && !cols.includes('industry') : cols.includes('industry')), fund + ' ' + cols);
    assert.equal(by.up.full.href(by.all.full.rows[0]), '/chart.html?s=' + encodeURIComponent(by.all.full.rows[0].t));
  }
  // with SPY not falling the cards differ; every outcome still opens
  const U = ad.build([clone(view(Object.assign(clone(FX.today), { rows: Object.assign(clone(FX.today.rows), { SPY: (() => { const b = clone(FX.today.rows.SPY); b[3] = FX.wide.benchmark.c * 1.004; return b; })() }) })))], {});
  const ub = Object.fromEntries(every(U).map(l => [l.key, l]));
  assert.deepEqual(U.ring.slices.map(s => s.open), ['up', 'flat', 'down-less', 'down-most']);
  assert.ok(ub['down-less'].card === false && ub['down-most'].card !== false && ub['down-less'].full && ub['down-most'].full);
});

test('an open list ranks by any column either way: market value, industry, the move; blanks last, ties in the list\'s own order', () => {
  const v = view(), m = kit.adapters.strength.build([clone(v)], {}), up = every(m).find(l => l.key === 'up'), F = kit.full, f = up.full;
  const col = id => f.cols.find(c => c.id === id);
  // unranked = the list's own order, and the heading says which column that is
  assert.deepEqual(tickers(F.rows(f, { sort: null, q: '' })), tickers(f.rows));
  assert.deepEqual(f.sort, { k: 'ret', d: -1 });
  for (let i = 1; i < f.rows.length; i++) assert.ok(f.rows[i - 1].ret >= f.rows[i].ret);
  // market value: largest first, then smallest first
  const big = F.rows(f, { sort: { k: 'mcap', d: -1 } }), small = F.rows(f, { sort: { k: 'mcap', d: 1 } });
  assert.equal(big.length, f.rows.length);
  for (let i = 1; i < big.length; i++) assert.ok(big[i - 1].mcap >= big[i].mcap, big[i - 1].t + ' ' + big[i].t);
  for (let i = 1; i < small.length; i++) assert.ok(small[i - 1].mcap <= small[i].mcap);
  assert.equal(big[0].mcap, Math.max(...f.rows.map(r => r.mcap)));
  assert.equal(small[0].mcap, Math.min(...f.rows.map(r => r.mcap)));
  // industry: A to Z keeps each industry together and, inside one, the list's own order (the move); Z to A is the mirror by industry
  const az = F.rows(f, { sort: { k: 'industry', d: 1 } }), za = F.rows(f, { sort: { k: 'industry', d: -1 } });
  const low = r => String(r.industry || '').toLowerCase(), named = az.filter(r => r.industry);
  for (let i = 1; i < named.length; i++) {
    assert.ok(low(named[i - 1]) <= low(named[i]));
    if (low(named[i - 1]) === low(named[i])) assert.ok(named[i - 1].ret >= named[i].ret, 'inside an industry the list keeps its own order');
  }
  assert.deepEqual([...new Set(za.filter(r => r.industry).map(low))], [...new Set(named.map(low))].reverse());
  assert.ok(new Set(named.map(low)).size > 20);
  // a blank is last whichever way the column runs
  const rows = [{ t: 'A', mcap: 5 }, { t: 'B', mcap: null }, { t: 'C', mcap: 9 }, { t: 'D', mcap: 5 }, { t: 'E' }];
  const g = { cols: [{ id: 't', t: 'Ticker', type: 's', get: r => r.t }, { id: 'mcap', t: 'Mkt value', type: 'n', get: r => r.mcap }], rows };
  assert.deepEqual(tickers(F.rows(g, { sort: { k: 'mcap', d: -1 } })), ['C', 'A', 'D', 'B', 'E']);
  assert.deepEqual(tickers(F.rows(g, { sort: { k: 'mcap', d: 1 } })), ['A', 'D', 'C', 'B', 'E']);
  assert.deepEqual(tickers(F.rows(g, { sort: { k: 't', d: -1 } })), ['E', 'D', 'C', 'B', 'A']);
  assert.deepEqual(tickers(F.rows(g, { sort: { k: 'nope', d: 1 } })), ['A', 'B', 'C', 'D', 'E']);
  // every column of every list ranks without throwing and keeps every row, both ways
  for (const l of every(m)) for (const c of l.full.cols) for (const d of [-1, 1]) assert.equal(F.rows(l.full, { sort: { k: c.id, d } }).length, l.full.rows.length, l.key + ' ' + c.id);
  // the dump-day column ranks by the share held, a longer record ahead of a shorter one at the same share
  const dump = F.rows(f, { sort: { k: 'dump', d: -1 } }).filter(r => r.rec_n);
  for (let i = 1; i < dump.length; i++) assert.ok(dump[i - 1].rec_h / dump[i - 1].rec_n >= dump[i].rec_h / dump[i].rec_n - 1e-3);
  assert.equal(col('dump').fmt(dump[0]), dump[0].rec_h + '/' + dump[0].rec_n);
});

test('an open list is found by ticker, name, sector or industry, drawn 200 rows at a time, and saved as CSV', () => {
  const v = view(), m = kit.adapters.strength.build([clone(v)], {}), by = Object.fromEntries(every(m).map(l => [l.key, l])), F = kit.full, all = by.all;
  // find: a ticker from its first letters, a name or an industry from any part of it
  const some = all.full.rows.find(r => r.industry && r.industry.includes('Banks')), word = 'banks';
  const found = F.rows(all.full, { q: word });
  assert.ok(found.length > 5 && found.every(r => (r.name + ' ' + r.sector + ' ' + r.industry).toLowerCase().includes(word) || r.t.toLowerCase().startsWith(word)));
  assert.ok(found.includes(some));
  const nv = F.rows(all.full, { q: 'NV' });
  assert.ok(nv.length && nv.every(r => r.t.startsWith('NV') || (r.name + ' ' + r.sector + ' ' + r.industry).toLowerCase().includes('nv')));
  assert.ok(nv.some(r => r.t.startsWith('NV')) && !nv.some(r => r.t.includes('NV') && !r.t.startsWith('NV') && !(r.name + ' ' + r.sector + ' ' + r.industry).toLowerCase().includes('nv')));
  assert.equal(F.rows(all.full, { q: 'zzzzqq' }).length, 0);
  // found, then ranked
  const ranked = F.rows(all.full, { q: word, sort: { k: 'mcap', d: -1 } });
  assert.deepEqual(tickers(ranked).sort(), tickers(found).sort());
  for (let i = 1; i < ranked.length; i++) assert.ok(ranked[i - 1].mcap >= ranked[i].mcap);
  // the drawing: a heading that says how the list is ranked, an arrow on that column, 200 rows and a button for the rest
  assert.ok(all.full.rows.length > F.page);
  const html = F.body(all, { key: 'all', sort: { k: 'mcap', d: -1 }, q: '', all: false });
  assert.equal((html.match(/<tr><td class="jts-fi">/g) || []).length, F.page);
  assert.match(html, new RegExp('data-jts-count>' + all.full.rows.length.toLocaleString('en-US') + ' stocks · ranked by Mkt value, highest first'));
  assert.match(html, /data-jts-sort="mcap" tabindex="0" aria-sort="descending"/);
  assert.equal((html.match(/aria-sort=/g) || []).length, 1);
  assert.match(html, new RegExp('data-jts-act="all">SHOW ALL ' + all.full.rows.length.toLocaleString('en-US') + ' ROWS'));
  assert.equal(all.full.cols.every(c => html.includes('data-jts-sort="' + c.id + '"')), true);
  const whole = F.body(all, { key: 'all', sort: { k: 'industry', d: 1 }, q: '', all: true });
  assert.equal((whole.match(/<tr><td class="jts-fi">/g) || []).length, all.full.rows.length);
  assert.match(whole, /ranked by Industry, A to Z/);
  assert.ok(!whole.includes('data-jts-act="all"'));
  assert.doesNotMatch(whole.replace(/<[^>]+>/g, ' '), /undefined|NaN|Infinity|\[object|\bnull\b/);
  assert.match(whole, /<td class="l jts-fk"><a href="\/chart\.html\?s=[A-Z.%0-9]+">/);
  assert.match(F.body(all, { key: 'all', sort: null, q: 'zzzzqq', all: false }), /Nothing in this list matches “zzzzqq”\./);
  // the full view: one button a category in the model's order, the open one pressed, a find box, CSV, the way back
  const page = F.html(m, by.up, { key: 'up', sort: null, q: '', all: false });
  const btn = [...page.matchAll(/data-jts-open="([^"]+)" aria-pressed="(true|false)"/g)].map(x => [x[1], x[2]]);
  assert.deepEqual(btn.map(b => b[0]), ['all', 'up', 'flat', 'down-less', 'down-more', 'strong-holding', 'strong-breaking', 'weak-bid', 'repeat', 'groups', 'industries-holding', 'industries-breaking']);
  assert.deepEqual(btn.filter(b => b[1] === 'true').map(b => b[0]), ['up']);
  assert.ok(page.includes('data-jts-find') && page.includes('data-jts-act="csv"') && page.includes('data-jts-act="close"') && page.includes('ranked by Move, highest first'));
  // a phone gets the same columns, the answer beside the ticker and the long text last
  const phone = F.body(by.up, { key: 'up', sort: null, q: '', all: false }, true), ids = h => [...h.matchAll(/data-jts-sort="([^"]+)"/g)].map(x => x[1]);
  assert.deepEqual(ids(phone).slice(0, 5), ['t', 'ret', 'x', 'mcap', 'industry']);
  assert.deepEqual(ids(phone).slice().sort(), by.up.full.cols.map(c => c.id).sort());
  assert.deepEqual(ids(F.body(by.up, { key: 'up', sort: null, q: '', all: false })), by.up.full.cols.map(c => c.id));
  // CSV: the columns by their headings, every row of the list as ranked, numbers as numbers, the outcome as a word
  const csv = F.csv(by.up.full, F.rows(by.up.full, { sort: { k: 'mcap', d: 1 } })).split('\n');
  assert.equal(csv.length, by.up.full.rows.length + 1);
  assert.equal(csv[0], 'Ticker,Name,Sector,Industry,Mkt value,Last,Move,vs SPY,Against SPY,Rank in,Dump days,From open,Day range,vs VWAP,Vol pace');
  const first = F.rows(by.up.full, { sort: { k: 'mcap', d: 1 } })[0];
  assert.ok(csv[1].startsWith(first.t + ',') && csv[1].includes(',Up,') && csv[1].includes(String(first.mcap)), csv[1]);
  assert.doesNotMatch(csv.join('\n'), /undefined|NaN|\[object/);
  assert.match(by.up.full.file, /^strength-up-b1-\d{4}-\d{2}-\d{2}-live$/);
});

test('sectors and industries open in full too, and a row hands its names to the table below', () => {
  const v = view(), ad = kit.adapters.strength, m = ad.build([clone(v)], {}), by = Object.fromEntries(every(m).map(l => [l.key, l])), F = kit.full, R = v.rules;
  const scope = v.rows.filter(r => r.counted && r.kind === 'stock' && (r.mcap || 0) >= R.board_min_mcap);
  // sectors: every sector of the scope with its counts; industries: every industry with enough names, best first and worst first
  const sec = by.groups.full, ind = by['industries-holding'].full, worst = by['industries-breaking'].full;
  assert.deepEqual(sec.rows.map(g => g.name).sort(), [...new Set(scope.map(r => r.sector || 'Unclassified'))].sort());
  assert.equal(sum(sec.rows, g => g.n), scope.length);
  assert.equal(ind.rows.length, by['industries-holding'].n);
  assert.ok(ind.rows.length > 30 && ind.rows.every(g => g.n >= (R.industry_min || 5) && g.sector));
  assert.deepEqual(worst.rows.map(g => g.name), ind.rows.map(g => g.name).reverse());
  assert.deepEqual([sec.sort, ind.sort, worst.sort], [{ k: 'held', d: -1 }, { k: 'held', d: -1 }, { k: 'held', d: 1 }]);
  assert.deepEqual(ind.cols.map(c => c.id), ['name', 'sector', 'n', 'held', 'up', 'beat', 'more', 'med', 'mcap']);
  const g0 = ind.rows.find(g => g.name === 'Semiconductors') || ind.rows[0], members = scope.filter(r => r.industry === g0.name);
  assert.equal(g0.n, members.length);
  assert.ok(Math.abs(g0.mcap - sum(members, r => r.mcap || 0)) < 1);
  const byCap = F.rows(ind, { sort: { k: 'mcap', d: -1 } });
  for (let i = 1; i < byCap.length; i++) assert.ok(byCap[i - 1].mcap >= byCap[i].mcap);
  assert.deepEqual(F.rows(ind, { sort: { k: 'name', d: 1 } }).map(g => g.name), ind.rows.map(g => g.name).sort((a, b) => (a.toLowerCase() < b.toLowerCase() ? -1 : 1)));
  const html = F.body(by['industries-holding'], { key: 'industries-holding', sort: null, q: '', all: true });
  assert.equal((html.match(/data-jts-pick="/g) || []).length, ind.rows.length);
  assert.match(html, new RegExp('data-jts-count>' + ind.rows.length + ' industries · ranked by Holding up, highest first'));
  assert.doesNotMatch(html.replace(/<[^>]+>/g, ' '), /undefined|NaN|Infinity|\[object|\bnull\b/);
  // a row's hand-over is what the table below needs: the kind, the size scope of the sheet, the sector or the industry
  assert.deepEqual(ind.pick(g0), { desk: 'strength', kind: 'stock', scope: 'b1', industry: g0.name });
  assert.deepEqual(sec.pick(sec.rows[0]), { desk: 'strength', kind: 'stock', scope: 'b1', sector: sec.rows[0].name });
  const sp = ad.build([clone(v)], { fund: 'sp' }), etf = ad.build([clone(v)], { fund: 'etf' });
  assert.equal(every(sp).find(l => l.key === 'groups').full.pick({ name: 'Energy' }).scope, 'sp');
  const cls = every(etf).find(l => l.key === 'groups');
  assert.deepEqual(cls.full.pick(cls.full.rows[0]), { desk: 'strength', kind: 'etf', scope: 'b1', sector: cls.full.rows[0].name });
  // ... and the table lists exactly those names: the industry filter, the Industry column, the full industry list of the page
  const want = { kind: 'stock', scope: 'b1', out: 'all', rank: 'all', show: 'counted', sector: '', industry: g0.name, q: '' };
  assert.deepEqual(tickers(v.rows.filter(T.rowFilter(v, want))).sort(), tickers(members).sort());
  const cut = v.rows.filter(T.rowFilter(v, Object.assign({}, want, { industry: '', sector: cls.full.rows[0].name, kind: 'etf' })));
  assert.equal(cut.length, cls.full.rows[0].n);
  assert.ok(T.VIEWS.today.includes('ind') && T.PHONE_TODAY.includes('ind') && T.COL.ind.t === 'Industry');
  assert.equal(T.COL.ind.get(members[0]), g0.name);
  assert.equal(T.COL.ind.get(v.rows.find(r => r.kind === 'etf')), null);
  assert.equal(v.industries.length, v.read.industries.n);
  assert.deepEqual(v.industries.slice(0, 15), v.read.industries.holding);
  assert.deepEqual(v.industries.slice(-15).reverse(), v.read.industries.breaking);
  assert.ok(!Object.keys(v.read).includes('industries_all') && !JSON.stringify(v.read).includes('industries_all'), 'the read stays the reference\'s');
  assert.equal(typeof T.pick, 'function');
  assert.equal(T.pick({ kind: 'stock', industry: g0.name }), false, 'nothing is drawn before the desk has a view');
});
