// Kit 1.8.0, 2026-11-23: which names come first.
// Khalid: "in all the different engines on my chart.html the leaders and largest market caps should show up first at the top of
// the enegines they get first priority. everything stay im talking view mode and how different stocks are arranged in view".
// Run on the real feeds the other tearsheet tests use (tests/fixtures: leaders, the six Wyckoff feeds, volatility, strength).
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const root = path.join(__dirname, '..'), read = (f) => fs.readFileSync(path.join(root, f), 'utf8');
const kit = require('../jh-tearsheet.js'), P = kit.priority;
const leadersDoc = JSON.parse(read('tests/fixtures/tearsheet-leaders.json')), wy = JSON.parse(read('tests/fixtures/tearsheet-wyckoff.json')), vol = JSON.parse(read('tests/fixtures/stock-volatility-desk.json'));
const IDX = P.index(leadersDoc);
const strengthFx = JSON.parse(require('node:zlib').gunzipSync(fs.readFileSync(path.join(root, 'tests/fixtures/strength-desk.json.gz'))).toString('utf8'));
const strengthView = () => require('../jh-strong-engine.js').model(strengthFx.wide, strengthFx.today, { classLabels: strengthFx.class_labels });
const wyDocs = () => structuredClone({ __wyckoff: 1, bottom: wy['data/bottom.json'], phase: wy['data/phase-detector.json'], katlin: wy['data/katlin.json'], radar: wy['data/accumulation-radar.json'], capit: wy['data/capitulation-desk.json'], scan: wy['data/campaign-scan.json'] });
const lists = (m) => (m.lists || []).concat(m.more || []);
const names = (rows) => rows.map((x) => x.name);
const SHEETS = () => [['leaders', kit.adapters.leaders.build([leadersDoc], {})], ['volatility', kit.adapters.volatility.build([vol], {})], ['strength', kit.adapters.strength.build([strengthView()], {})], ['strength S&P 500', kit.adapters.strength.build([strengthView()], { fund: 'sp' })]]
  .concat(['sc', 'capit', 'bc', 'spread', 'spring', 'accum', 'bottom', 'dist'].map((tab) => ['wyckoff ' + tab, kit.adapters.wyckoff.build([wyDocs()], { tab })]))
  .concat([['capitulation', kit.adapters.capitulation.build([wy['data/capitulation-desk.json']], {})]]);
// the rule, written again here: 0 = an industry leader, 1 = any other name with a market value, 2 = no market value
const rank = (r) => { const t = P.ticker(r), c = P.cap(r), lead = P.isLeader(t, IDX); return { g: lead ? 0 : c != null ? 1 : 2, c: c != null ? c : lead ? IDX.cap[t] : 0 }; };
function holds(rows, what) {
  for (let i = 1; i < rows.length; i++) {
    const a = rank(rows[i - 1]), b = rank(rows[i]);
    assert.ok(a.g < b.g || (a.g === b.g && (a.g === 2 || a.c >= b.c)), what + ': ' + P.ticker(rows[i - 1]) + ' stands before ' + P.ticker(rows[i]));
  }
}

test('the kit is 1.8.0 and the leaders are the Leaders desk\'s own set', () => {
  assert.equal(kit.version, '1.8.0');
  assert.equal(Object.keys(IDX.cap).length, leadersDoc.rows.length);
  assert.equal(IDX.as_of, leadersDoc.as_of);
  assert.equal(P.isLeader('NVDA', IDX), leadersDoc.rows.some((r) => r.ticker === 'NVDA'));
  assert.equal(P.isLeader('NOT-A-TICKER', IDX), false);
});

test('order: leaders first by market value, then every other name by market value, then names with none in their own order', () => {
  const two = leadersDoc.rows.slice().sort((a, b) => b.mcap - a.mcap).slice(0, 2), small = leadersDoc.rows.slice().sort((a, b) => a.mcap - b.mcap)[0];
  const rows = [{ ticker: 'ZZZ1', mcap: 9e13 }, { t: small.ticker }, { ticker: 'NOCAP-A' }, { ticker: two[1].ticker, mcap: two[1].mcap }, { ticker: 'ZZZ2', mcap: 5e6 }, { ticker: 'NOCAP-B' }, { t: two[0].ticker, mcap: two[0].mcap }];
  assert.deepEqual(P.order(rows, null, IDX).map(P.ticker), [two[0].ticker, two[1].ticker, small.ticker, 'ZZZ1', 'ZZZ2', 'NOCAP-A', 'NOCAP-B'],
    'a leader comes before a larger company that is not one; a leader the row gives no value for takes the Leaders desk\'s; no value keeps its place at the end');
  assert.deepEqual(P.order(rows, null, null).map(P.ticker), ['ZZZ1', two[0].ticker, two[1].ticker, 'ZZZ2', small.ticker, 'NOCAP-A', 'NOCAP-B'], 'before the leaders are known: by market value alone');
  assert.equal(P.order(rows, null, IDX).length, rows.length);
  assert.deepEqual(P.order([], null, IDX), []);
  const own = P.order(rows, { ticker: (r) => r.ticker || r.t, cap: () => null }, IDX).map(P.ticker);
  assert.deepEqual(own.slice(0, 3), [two[0].ticker, two[1].ticker, small.ticker], 'a table with its own record shape passes its own readers');
});

test('every sheet, arranged: nothing leaves a list, no figure changes, and the rule holds from the first row to the last', () => {
  let arranged = 0, cards = 0;
  for (const [name, m0] of SHEETS()) {
    const before = lists(m0).map((l) => ({ key: l.key, full: l.full ? l.full.rows.slice() : null, rows: (l.rows || []).slice() }));
    const m = P.model(m0, 'priority', IDX);
    lists(m).forEach((l, i) => {
      const b = before[i];
      if (!l.full || l.full.rows === b.full || !l.full.own) { assert.deepEqual(names(l.rows || []), names(b.rows), name + ' ' + l.key + ': a list of no stocks is untouched'); return; }
      arranged++;
      assert.equal(l.full.rows.length, b.full.length, name + ' ' + l.key);
      assert.deepEqual(new Set(l.full.rows), new Set(b.full), name + ' ' + l.key + ': the same records, each once');
      holds(l.full.rows, name + ' ' + l.key);
      assert.equal(l.full.sort, null, 'no column is marked as the ranking while the list is arranged by size');
      if ((l.rows || []).length) {
        cards++;
        assert.deepEqual(names(l.rows).slice().sort(), names(b.rows).slice().sort(), name + ' ' + l.key + ': the card holds the same rows');
        l.rows.forEach((x) => { const o = b.rows.find((y) => y === x); assert.ok(o, 'the very row the desk built: its name, figure and note are untouched'); });
      }
    });
    assert.equal(m.stockOrder, lists(m).some((l) => l.full && l.full.own) ? 'priority' : undefined, name + ': the switch shows only on a sheet that lists stocks');
  }
  assert.ok(arranged >= 60 && cards >= 30, arranged + ' lists arranged, ' + cards + ' with a card');
});

test('a card with a ranking title keeps its own first fifteen; a set is arranged whole', () => {
  const m0 = kit.adapters.leaders.build([leadersDoc], {}), own = Object.fromEntries(lists(m0).map((l) => [l.key, (l.rows || []).slice()]));
  const m = P.model(m0, 'priority', IDX), by = Object.fromEntries(lists(m).map((l) => [l.key, l]));
  // "Strongest against the S&P 500": the fifteen strongest, largest first - not the largest of everything ahead of the index
  assert.deepEqual(names(by.strongest.rows.slice(0, 15)).sort(), names(own.strongest.slice(0, 15)).sort());
  holds(by.strongest.rows.slice(0, 15).map((x) => x.__r), 'strongest, first fifteen');
  assert.deepEqual(names(by.strongest.rows.slice(15)), names(own.strongest.slice(15)), 'past the fifteenth the desk\'s own ranking runs on');
  assert.notDeepEqual(names(by.strongest.rows.slice(0, 6)), names(by.weakest.rows.slice(0, 6)));
  // "Under accumulation" is a set: its card starts with the largest leader under accumulation
  const acc = by.accumulation.rows.map((x) => x.__r);
  holds(acc, 'under accumulation');
  assert.equal(acc[0].mcap, Math.max(...own.accumulation.map((x) => x.__r.mcap)));
  // two lists that hold the same names are two rankings of one set, whatever their titles say
  const wyM = P.model(kit.adapters.wyckoff.build([wyDocs()], { tab: 'accum' }), 'priority', IDX), cardsWy = lists(wyM).filter((l) => (l.rows || []).length > 15 && l.full && l.full.own);
  const firsts = cardsWy.map((l) => names(l.rows.slice(0, 6)).join('|'));
  assert.ok(new Set(firsts).size >= cardsWy.length - 1, 'the cards of one board do not all open on the same six names: ' + firsts.length + ' cards, ' + new Set(firsts).size + ' openings');
});

test('the desk\'s own order is one switch away and comes back exactly', () => {
  for (const [name, m0] of SHEETS()) {
    const before = lists(m0).map((l) => ({ full: l.full, rows: l.rows, names: names(l.rows || []), all: l.full ? l.full.rows.slice() : null }));
    P.model(m0, 'priority', IDX);
    const m = P.model(m0, 'engine', IDX);
    lists(m).forEach((l, i) => {
      assert.equal(l.full, before[i].full, name + ' ' + l.key + ': the list the desk built, the same object');
      assert.equal(l.rows, before[i].rows);
      assert.deepEqual(names(l.rows || []), before[i].names);
      if (l.full) assert.deepEqual(l.full.rows, before[i].all);
    });
    if (m.stockOrder !== undefined) assert.equal(m.stockOrder, 'engine');
    // and arranged again, it is arranged again
    assert.equal(P.model(m0, 'priority', IDX).stockOrder, m.stockOrder === undefined ? undefined : 'priority');
  }
});

test('the full view opens arranged, and a click on a column still ranks by that column', () => {
  const m = P.model(kit.adapters.leaders.build([leadersDoc], {}), 'priority', IDX), l = lists(m).find((x) => x.key === 'accumulation');
  holds(kit.full.rows(l.full, { key: l.key, sort: null, q: '' }), 'opened, unranked');
  const col = l.full.cols.find((c) => c.type === 'n'), ranked = kit.full.rows(l.full, { key: l.key, sort: { k: col.id, d: -1 }, q: '' }).map((r) => col.get(r)).filter((v) => typeof v === 'number' && isFinite(v));
  for (let i = 1; i < ranked.length; i++) assert.ok(ranked[i - 1] >= ranked[i], 'ranked by ' + col.id);
});

test('the desks\' own tables follow the same switch, and a column the reader clicked wins', () => {
  const scan = read('jh-wyckoff-scan.js'), ld = read('jh-leaders.js'), st = read('jh-strong-today.js'), sv = read('jh-stock-volatility.js'), cap = read('capitulation-desk.html'), k = read('jh-tearsheet.js');
  assert.ok(scan.includes("if (view.sort == null && arranged) rows = P.order(rows);"), 'the all-stock Wyckoff boards');
  assert.ok(scan.includes('data-sc-order="priority"') && scan.includes('data-sc-order="engine"'));
  for (const [name, src] of [['leaders', ld], ['strength', st], ['volatility', sv]]) {
    assert.ok(src.includes('function largestFirst(table)') && src.includes('L && L.on ? L.P.order(rows)'), name + ': arranged until a column is clicked');
    assert.ok(src.includes('S.own = S.own || {}; S.own[table] = true;'), name + ': a clicked column is the reader\'s choice');
    assert.ok(/data-(ld|stock)-order="engine"/.test(src), name + ': the desk\'s own order is a button');
    assert.ok(src.includes('"jh:stock-order"'), name + ': follows the switch on the tearsheet');
  }
  assert.ok(cap.includes('if(LF) rows=LF.order(rows);') && cap.includes('s.own=true;'), 'the capitulation desk\'s table of prints');
  assert.ok(k.includes("PRIO_DESKS = { leaders: 1, wyckoff: 1, capitulation: 1, volatility: 1, strength: 1 }"), 'the five desks the chart opens that list stocks; the other sheets are as they were');
  assert.ok(k.includes("data-jts-order=\"priority\"") && k.includes("data-jts-order=\"engine\""));
  assert.ok(/^[\x00-\x7F]*$/.test(k), 'the kit stays ASCII');
});

test('the strength sheet on a falling day opens each card on the largest names in it', () => {
  const m = P.model(kit.adapters.strength.build([strengthView()], {}), 'priority', IDX), by = Object.fromEntries(lists(m).map((l) => [l.key, l]));
  for (const key of ['up', 'down-less', 'strong-holding', 'repeat']) {
    const l = by[key];
    if (!l || !(l.rows || []).length) continue;
    assert.ok(l.rows.every((x) => x.__r), key + ': each card row knows its record');
    holds(l.rows.map((x) => x.__r), 'strength ' + key);
    assert.equal(l.rows.length, l.full.own.rows.length, key + ': every name is still on the list');
  }
});
