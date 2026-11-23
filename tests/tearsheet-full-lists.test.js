// Kit 1.7.0: every list of every tearsheet opens in full.
// Khalid 2026-11-20 18:55 ET: "now do the same with Volatility - most and least volatile stocks, price spread - and all
// the other engines that arent showing the whole list". A card shows six rows; its heading opens the whole list on
// typed columns over the adapter's own records, ranked by any column either way (the view tests/strength-desk.test.js
// covers on the Strength sheet). These tests walk every sheet the kit can build, on the real feeds of the fixtures.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), zlib = require('node:zlib');
const kit = require('../jh-tearsheet.js');
const fx = name => JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', name), 'utf8'));
const feeds = fx('tearsheet-feeds.json');
feeds['data/leaders.json'] = fx('tearsheet-leaders.json');
Object.assign(feeds, Object.fromEntries(Object.entries(fx('tearsheet-wyckoff.json')).filter(([k]) => k.startsWith('data/'))));
feeds['data/stock-volatility.json'] = fx('stock-volatility-desk.json');
const strengthFx = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures/strength-desk.json.gz'))).toString('utf8'));
feeds['data/strength-wide.json'] = require('../jh-strong-engine.js').model(strengthFx.wide, strengthFx.today, { classLabels: strengthFx.class_labels });
const build = (name, st) => kit.adapters[name].build(kit.adapters[name].feeds.map(f => structuredClone(feeds[f.slice(1)])), st || {});
// every scope a reader can put a sheet in
const SCOPES = { ark: [{}, { fund: 'ARKK' }], stablecoin: [{}], firmbook: [{}], paperbook: [{}], sp500: [{}], insiders: [{}], tic: [{}], capital: [{}], capitulation: [{}],
  leaders: [{ fund: 'w' }, { fund: 'm' }, { fund: 'q' }], volatility: [{ fund: 'd' }, { fund: 'w' }, { fund: 'm' }, { fund: 'q' }], strength: [{ fund: 'b1' }, { fund: 'sp' }, { fund: 'all' }, { fund: 'etf' }],
  wyckoff: ['bottom', 'accum', 'dist'].flatMap(tab => [{ tab, fund: 'stock' }, { tab, fund: 'etf' }]).concat(['sc', 'capit', 'bc', 'spring', 'spread'].map(tab => ({ tab }))) };
const SHEETS = Object.entries(SCOPES).flatMap(([name, scopes]) => scopes.map(st => ({ name, st, id: `${name} ${JSON.stringify(st)}`, m: build(name, st) })));
const every = m => m.lists.concat(m.more || []);
const text = (c, r) => { const v = c.fmt ? c.fmt(r) : c.get(r); return v == null ? '' : String(v); };
const blank = v => v == null || v === '' || (typeof v === 'number' && !isFinite(v));
const keyOf = (c, r) => { const v = c.get(r); return blank(v) ? null : c.type === 's' ? String(v).toLowerCase() : v; };
const lead = name => String(name).split(' · ')[0];
// a card whose rows are a ranking among names large enough to act on, while its count and its whole list are every such name
const WIDER = new Set(['wyckoff/broken', 'wyckoff/reclaimed']);

test('every adapter and every scope is walked', () => {
  assert.deepEqual(Object.keys(SCOPES).sort(), Object.keys(kit.adapters).sort());
  assert.deepEqual(Object.keys(kit.adapters.wyckoff.tabs).sort(), ['accum', 'bc', 'bottom', 'capit', 'dist', 'sc', 'spread', 'spring']);
  assert.equal(kit.version, '1.8.0');
});

test('every list of every sheet opens in full, on typed columns over the adapter\'s own records', () => {
  let lists = 0, rows = 0;
  for (const { id, m } of SHEETS) {
    assert.ok(m.lists.length >= 3, id);
    for (const l of every(m)) {
      const at = `${id} ${l.key}`, f = l.full;
      lists++;
      assert.ok(f, `${at}: no whole list`);
      assert.ok(Array.isArray(f.rows) && f.rows.length >= (l.rows || []).length, `${at}: ${f.rows.length} rows behind a card of ${(l.rows || []).length}`);
      assert.ok(f.cols.length >= 3 && new Set(f.cols.map(c => c.id)).size === f.cols.length, `${at}: columns ${f.cols.map(c => c.id)}`);
      assert.ok(f.cols[0].type === 's', `${at}: the first column names the row`);
      for (const c of f.cols) assert.ok(typeof c.t === 'string' && c.t.length && ['s', 'n'].includes(c.type) && typeof c.get === 'function', `${at}/${c.id}`);
      assert.ok(!f.cols.some(c => ['sub', 'big', 'small'].includes(c.id)), `${at}: the generic columns, where the adapter has records to give`);
      assert.ok(Array.isArray(f.units) && f.units.length === 2 && f.units.every(u => typeof u === 'string' && u.length), `${at}: units`);
      assert.ok(typeof l.title === 'string' && l.title.length, at);
      // every cell of the first and the last rows renders, and a number column holds numbers
      for (const r of f.rows.slice(0, 40).concat(f.rows.slice(-5))) {
        rows++;
        for (const c of f.cols) {
          const v = c.get(r), s = text(c, r);
          assert.doesNotMatch(s, /undefined|NaN|Infinity|\[object|^null$/, `${at}/${c.id}: ${s}`);
          if (c.type === 'n') assert.ok(v == null || typeof v === 'number', `${at}/${c.id}: ${typeof v}`);
          if (c.tone) assert.ok(['', 'up', 'down'].includes(c.tone(r)), `${at}/${c.id} tone`);
        }
        if (f.href) assert.match(f.href(r), /^$|^\/chart\.html\?s=[A-Za-z0-9.%-]+$/, at);
      }
      if (f.rows.length && !/^ark /.test(id)) assert.ok(text(f.cols[0], f.rows[0]) !== '', `${at}: a row with no name`);                  // an ARK line can be cash, with no ticker
    }
  }
  assert.ok(lists >= 300 && rows > 5000, `${lists} lists, ${rows} rows`);
});

test('what opens is the card\'s own list: its first rows, in its order', () => {
  for (const { name, id, m } of SHEETS) {
    for (const l of m.lists.filter(x => x.card !== false)) {
      const at = `${id} ${l.key}`, f = l.full, names = l.rows.map(r => lead(r.name)), whole = f.rows.map(r => text(f.cols[0], r));
      if (name === 'leaders' && l.key === 'printed') {
        // the card groups the prints by signal; the whole list is each print, one row a ticker and a signal
        assert.equal(f.rows.length, l.n, at);
        assert.equal(sumOf(l.rows, r => +String(r.big).replace(/,/g, '')), f.rows.length, at);
        continue;
      }
      if (name === 'strength' && l.key.startsWith('industries')) continue;                 // tests/strength-desk.test.js: fifteen on the card, every industry behind it
      if (WIDER.has(`${name}/${l.key}`)) {
        let from = 0;
        for (const t of names) { const i = whole.indexOf(t, from); assert.ok(i >= 0, `${at}: ${t} is on the card and not in its whole list, in order`); from = i + 1; }
        assert.equal(f.rows.length, l.n, at);
        continue;
      }
      assert.deepEqual(whole.slice(0, Math.min(names.length, 25)).map(t => t || '?'), names.slice(0, 25), at);
    }
  }
});
function sumOf(list, f) { return list.reduce((a, x) => a + f(x), 0); }

test('a list that states its order stands in it, and any column ranks it both ways with blanks last', () => {
  let ranked = 0;
  for (const { id, m } of SHEETS) {
    for (const l of every(m)) {
      const at = `${id} ${l.key}`, f = l.full;
      if (f.sort) {
        assert.ok(f.cols.some(c => c.id === f.sort.k) && [1, -1].includes(f.sort.d), `${at}: ${JSON.stringify(f.sort)}`);
        const again = kit.full.rows(f, { sort: f.sort });
        assert.ok(again.length === f.rows.length && again.every((r, i) => r === f.rows[i]), `${at}: the list says it stands by ${f.sort.k} ${f.sort.d < 0 ? 'highest first' : 'lowest first'} and does not`);
      }
      assert.ok(kit.full.rows(f, {}).every((r, i) => r === f.rows[i]), `${at}: unranked is the list's own order`);
      const cols = f.rows.length > 800 ? f.cols.slice(0, 7) : f.cols;                       // a long list: the name, the lead figures; the short ones every column
      for (const c of cols) {
        for (const d of [1, -1]) {
          const out = kit.full.rows(f, { sort: { k: c.id, d } }), keys = out.map(r => keyOf(c, r)), firstBlank = keys.indexOf(null);
          assert.equal(out.length, f.rows.length, at);
          if (firstBlank >= 0) assert.ok(keys.slice(firstBlank).every(k => k === null), `${at}/${c.id}: a blank is not last`);
          const filled = firstBlank < 0 ? keys : keys.slice(0, firstBlank);
          for (let i = 1; i < filled.length; i++) assert.ok(d > 0 ? filled[i - 1] <= filled[i] : filled[i - 1] >= filled[i], `${at}/${c.id} ${d}: row ${i}`);
          ranked++;
        }
      }
    }
  }
  assert.ok(ranked > 4000, String(ranked));
});

test('the ring and its slices open lists the sheet has; a list without a card is reached from them or from the buttons', () => {
  const opens = {};
  for (const { name, id, m } of SHEETS) {
    const keys = every(m).map(l => l.key);
    assert.equal(new Set(keys).size, keys.length, `${id}: ${keys}`);
    for (const k of [m.ring.open].concat(m.ring.slices.map(s => s.open))) if (k) { assert.ok(keys.includes(k), `${id}: the ring opens ${k}`); opens[name] = (opens[name] || 0) + 1; }
    for (const l of m.more || []) assert.ok(l.card === false || !m.lists.includes(l), id);
    if (m.order) for (const k of m.order) assert.ok(keys.includes(k) || name === 'strength', `${id}: order names ${k}`);
  }
  // the ring's own heading opens the whole it draws
  for (const [name, key] of [['ark', 'positions'], ['stablecoin', 'coins'], ['firmbook', 'book'], ['paperbook', 'positions'], ['sp500', 'sectors'], ['insiders', 'dollars'], ['tic', 'holders'], ['capital', 'year'], ['leaders', 'all'], ['capitulation', 'all'], ['volatility', 'ranked']]) {
    assert.equal(build(name).ring.open, key, name);
  }
  for (const tab of ['bottom', 'accum', 'dist']) assert.equal(build('wyckoff', { tab }).ring.open, 'largest', tab);
  // a slice opens its members: the leaders of a tape label, the prints of a desk, the stocks of a band, the names under a status
  const L = build('leaders'), tape = every(L).filter(l => /^tape-/.test(l.key));
  assert.equal(sumOf(tape, l => l.full.rows.length), every(L).find(l => l.key === 'all').full.rows.length);
  const C = build('capitulation'), d = feeds['data/capitulation-desk.json'];
  for (const [k, rows] of Object.entries(d.boards)) if (rows.length) assert.equal(every(C).find(l => l.key === 'desk-' + k).full.rows.length, rows.length, k);
  assert.equal(every(C).find(l => l.key === 'all').full.rows.length, sumOf(Object.values(d.boards), b => b.length));
  for (const tab of ['bottom', 'accum', 'dist']) for (const fund of ['stock', 'etf']) {
    const m = build('wyckoff', { tab, fund }), by = Object.fromEntries(every(m).map(l => [l.key, l]));
    assert.equal(sumOf([0, 1, 2], i => by['status-' + i].full.rows.length), by.largest.full.rows.length, `${tab} ${fund}`);
    m.ring.slices.forEach((s, i) => assert.equal(s.open, s.value ? 'status-' + i : '', `${tab} ${fund}`));
  }
  const S = build('wyckoff', { tab: 'spring' });
  assert.deepEqual(S.ring.slices.map(s => s.open), S.lists.map(l => (l.rows.length ? l.key : '')));
});

test('Volatility: every list is the engine\'s whole ranking, and its first 25 rows are the engine\'s board', () => {
  const d = feeds['data/stock-volatility.json'], floor = d.board_rule.min_mcap, RK = { hv21: 'rk_hv21', sp_d: 'rk_d', sp_w: 'rk_w', sp_m: 'rk_m', sp_q: 'rk_q' };
  const rows = d.rows.map(r => Object.fromEntries(d.cols.map((c, i) => [c, r[i]]))), stocks = rows.filter(r => r.kind === 'stock');
  for (const k of ['d', 'w', 'm', 'q']) {
    const m = build('volatility', { fund: k }), by = Object.fromEntries(every(m).map(l => [l.key, l])), T = key => by[key].full.rows.map(r => r.t), sp = 'sp_' + k;
    const want = meas => stocks.filter(r => (r.mcap || 0) >= floor && Number.isFinite(r[RK[meas]])).sort((a, b) => a[RK[meas]] - b[RK[meas]]).map(r => r.t);
    assert.deepEqual(T('most-volatile'), want('hv21'), k);
    assert.deepEqual(T('least-volatile'), want('hv21').reverse(), k);
    assert.deepEqual(T('widest'), want(sp), k);
    assert.deepEqual(T('narrowest'), want(sp).reverse(), k);
    // the engine's boards are the two ends of those rankings, and the card still shows exactly them
    for (const [key, board] of [['most-volatile', 'most_volatile'], ['least-volatile', 'least_volatile'], ['widest', 'widest_' + k], ['narrowest', 'narrowest_' + k], ['rising', 'vol_rising'], ['falling', 'vol_falling']]) {
      assert.deepEqual(T(key).slice(0, 25), d.boards[board], `${k} ${key}`);
      assert.deepEqual(by[key].rows.map(r => lead(r.name)), d.boards[board], `${k} ${key}: the card`);
      assert.ok(T(key).length > 25, `${k} ${key}: ${T(key).length} rows - the whole list, not the board`);
    }
    assert.equal(by['most-volatile'].n, d.board_rule.population.hv21); assert.equal(by.widest.n, d.board_rule.population[sp]);
    // rising and falling: the engine's rule - a reading 21 sessions ago, no flagged jump inside the two windows - and the sign of the change
    const chg = r => r.hv21 - r.hv21p, up = by.rising.full.rows, down = by.falling.full.rows;
    assert.ok(up.every(r => chg(r) > 0 && (r.ja == null || r.ja > 42) && Number.isFinite(r.rk_hv21) && r.mcap >= floor) && down.every(r => chg(r) < 0 && (r.ja == null || r.ja > 42)), k);
    for (let i = 1; i < up.length; i++) assert.ok(chg(up[i - 1]) >= chg(up[i]));
    for (let i = 1; i < down.length; i++) assert.ok(chg(down[i - 1]) <= chg(down[i]));
    assert.equal(up.filter(r => down.includes(r)).length, 0);
    // the lists with no card: every ranked stock (the ring's whole), each band (a slice), what is set aside, the ETFs
    const ranked = stocks.filter(r => Number.isFinite(r.rk_hv21));
    assert.deepEqual(T('ranked').slice().sort(), ranked.map(r => r.t).sort(), k);
    assert.equal(m.ring.open, 'ranked');
    const bands = every(m).filter(l => /^band-/.test(l.key));
    assert.equal(bands.length, d.bands.length); assert.equal(sumOf(bands, l => l.full.rows.length), ranked.length);
    bands.forEach((l, i) => { const b = d.bands[i]; assert.ok(l.full.rows.every(r => r.hv21 >= b.from && (b.to == null || r.hv21 < b.to)), l.key); assert.equal(m.ring.slices[i].open, 'band-' + i); assert.equal(l.title, b.band + ' a year'); });
    assert.deepEqual(T('aside').slice().sort(), stocks.filter(r => r.aside).map(r => r.t).sort());
    assert.ok(by.aside.full.rows.every(r => text(by.aside.full.cols.find(c => c.id === 'why'), r).length > 3));
    assert.deepEqual(T('etfs').slice().sort(), rows.filter(r => r.kind === 'etf').map(r => r.t).sort());
    assert.ok(!by.etfs.full.cols.some(c => c.id === 'sector' || c.id === 'industry') && by.etfs.full.cols.find(c => c.id === 'mcap').t === 'Assets');
    assert.deepEqual(m.order.filter(x => !/^band-/.test(x)), ['most-volatile', 'least-volatile', 'widest', 'narrowest', 'rising', 'falling', 'sectors', 'ranked', 'pinned', 'aside', 'etfs']);
    // the column a list is ranked by stands right after the name, so it is on screen; the spread columns follow the window
    assert.deepEqual(by.widest.full.cols.slice(0, 5).map(c => c.id), ['t', 'name', sp, 'lo', 'hi']);
    assert.deepEqual(by['most-volatile'].full.cols.slice(0, 6).map(c => c.id), ['t', 'name', 'hv21', 'hv21p', 'dv', 'hvp']);
    assert.deepEqual(by.widest.full.sort, { k: sp, d: -1 }); assert.deepEqual(by.narrowest.full.sort, { k: sp, d: 1 });
    const one = by.widest.full.rows[0], lo = by.widest.full.cols.find(c => c.id === 'lo'), hi = by.widest.full.cols.find(c => c.id === 'hi');
    assert.equal(lo.get(one), one['lo_' + k]); assert.equal(hi.get(one), one['hi_' + k]);
    // volatility is neither good nor bad: no column of the desk is coloured up or down
    for (const l of every(m)) assert.ok(l.full.cols.every(c => !c.tone), l.key);
    // sectors: every sector the engine measured, on its own medians
    assert.deepEqual(by.sectors.full.rows.map(g => g.sector), d.by_sector.filter(g => g.med && Number.isFinite(g.med.hv21)).sort((a, b) => b.med.hv21 - a.med.hv21).map(g => g.sector));
  }
});

test('lists that were a few names of a longer one now hold all the feed carries', () => {
  // stablecoins: the feed names five minters and three burners; the whole lists are every coin it carries, by its 30-day change
  const sc = feeds['data/stablecoin-flow.json'], S = build('stablecoin'), sBy = Object.fromEntries(S.lists.map(l => [l.key, l]));
  assert.deepEqual(sBy.burn.full.rows.map(c => c.symbol).sort(), sc.top_stablecoins_by_mcap.filter(c => c.delta_30d < 0).map(c => c.symbol).sort());
  assert.ok(sBy.burn.full.rows.length > sBy.burn.rows.length && sBy.mint.full.rows.every(c => c.delta_30d > 0));
  assert.equal(sBy.chains.full.rows.length, sc.aggregate.top_chains.length);
  // capital flows: the card shows twelve prints of the rolling total, the whole list every print the feed holds
  const cap = feeds['data/capital-inflows.json'], C = build('capital').lists.find(l => l.key === 'history');
  assert.equal(C.rows.length, 12); assert.equal(C.full.rows.length, cap.history_12mo_rolling_b.length);
  assert.ok(Math.abs(C.full.rows[0].chg - (cap.history_12mo_rolling_b[0].rolling_12mo_b - cap.history_12mo_rolling_b[1].rolling_12mo_b)) < 1e-9 && C.full.rows.at(-1).chg === null);
  // leaders: a card names eight tickers of a signal and "N more"; the whole list is every print
  const ld = feeds['data/leaders.json'], P = build('leaders').lists.find(l => l.key === 'printed');
  assert.deepEqual(P.full.rows.map(x => x.ticker + '|' + x.signal).sort(), ld.changes.new.map(x => x.ticker + '|' + x.signal).sort());
  assert.ok(P.full.rows.some(x => x.r && x.r.name) && P.full.cols.map(c => c.id).includes('sig'));
  // the S&P 500: every member, with the valuation and growth fields the feed already carries beside the five the card reads
  const sp = feeds['data/sp500.json'], H = build('sp500').lists.find(l => l.key === 'heavy'), i = sp.member_fields.indexOf('pe_fwd'), j = sp.member_fields.indexOf('roe_pct');
  assert.equal(H.full.rows.length, Object.keys(sp.members).length);
  for (const r of H.full.rows.slice(0, 20)) { assert.equal(r.fwd, sp.members[r.t][i]); assert.equal(r.roe, sp.members[r.t][j]); }
  // ARK, the firm book and the paper book: the positions the ring draws six of
  const ark = feeds['data/ark-holdings.json'], A = build('ark');
  assert.equal(A.more[0].key, 'positions'); assert.equal(A.more[0].full.rows.length, new Set(Object.values(ark.holdings_by_fund).flat().map(h => h.ticker || h.company)).size);
  assert.equal(build('ark', { fund: 'ARKK' }).more[0].full.rows.length, ark.holdings_by_fund.ARKK.length);
  assert.equal(build('firmbook').more[0].full.rows.length, feeds['data/firm-book.json'].equity_book.length);
  assert.equal(build('paperbook').more[0].full.rows.length, feeds['data/paper-book.json'].positions.length);
  // the Wyckoff scan: a low that broke and a rally high that was reclaimed are counted at every size, so they are listed at every size
  for (const fund of ['stock', 'etf']) {
    const scan = feeds['data/campaign-scan.json'].boards, kind = fund;
    const B = build('wyckoff', { tab: 'bottom', fund }).lists.find(l => l.key === 'broken'), R = build('wyckoff', { tab: 'dist', fund }).lists.find(l => l.key === 'reclaimed');
    assert.deepEqual(B.full.rows.map(r => r.ticker).sort(), scan.bottom.filter(r => r.kind === kind && r.status === 'BROKEN').map(r => r.ticker).sort(), fund);
    assert.deepEqual(R.full.rows.map(r => r.ticker).sort(), scan.distribution.filter(r => r.kind === kind && r.status === 'RECLAIMED').map(r => r.ticker).sort(), fund);
  }
});

test('a ticker links to the chart out of the desk\'s frame; a token on the crypto desk is not a listed symbol and is no link', () => {
  const C = build('capitulation'), by = Object.fromEntries(every(C).map(l => [l.key, l]));
  const stocks = kit.full.html(C, by['desk-stocks'], { key: 'desk-stocks' }, false), crypto = kit.full.html(C, by['desk-crypto'], { key: 'desk-crypto' }, false);
  assert.match(stocks, /<td class="l jts-fk"><a href="\/chart\.html\?s=[A-Z.]+" target="_top">[A-Z.]+<\/a><\/td>/);
  assert.doesNotMatch(crypto, /<a href/); assert.match(crypto, /<td class="l jts-fk"><span class="jts-fx" title="[A-Z]+">[A-Z]+<\/span><\/td>/);
  // Katlin's picks say what they are: a crypto pick opens as its dollar pair, the way the capitulation desk's own table links one
  const K = build('wyckoff', { tab: 'spread' }).lists[0].full, picks = feeds['data/katlin.json'].picks;
  const coin = picks.find(p => p.asset_class === 'crypto'), stock = picks.find(p => p.asset_class === 'stock');
  assert.equal(K.href(coin), '/chart.html?s=' + coin.ticker + '-USD'); assert.equal(K.href(stock), '/chart.html?s=' + stock.ticker);
  // inside the chart's workspace the kit hands the symbol to the chart behind the desk, as the desks' own tables do
  const src = fs.readFileSync(path.join(__dirname, '..', 'jh-tearsheet.js'), 'utf8');
  assert.match(src, /window\.top\.jhOpenSymbol\(s\)/); assert.match(src, /getElementById\('wsx'\)/);
  // a CSV is named for its desk and its list
  assert.match(src, /l\.full\.file \|\| name \+ '-' \+ l\.key/);
});

test('the open list is drawn from the list\'s columns: headings, the stated order\'s arrow, the count line, 200 rows then the rest', () => {
  const m = build('volatility'), l = m.lists.find(x => x.key === 'most-volatile'), n = l.full.rows.length;
  const html = kit.full.html(m, l, { key: l.key }, false), body = kit.full.body(l, { key: l.key }, false);
  assert.ok(body.includes(`${n.toLocaleString('en-US')} stocks · ranked by Volatility, 21 sessions, highest first`));
  assert.match(body, /<th class="" data-jts-sort="hv21" tabindex="0" aria-sort="descending"/);
  assert.equal((body.match(/<tr><td class="jts-fi">/g) || []).length, Math.min(n, kit.full.page));
  assert.ok(body.includes(`SHOW ALL ${n.toLocaleString('en-US')} ROWS`) === (n > kit.full.page));
  assert.equal((kit.full.body(l, { key: l.key, all: true }, false).match(/<tr><td class="jts-fi">/g) || []).length, n);
  // the buttons above an open list: every list of the sheet, the hidden ones too, in the adapter's order, each with its size
  const nav = [...html.matchAll(/<button type="button" class="jts-btn" data-jts-open="([a-z0-9-]+)" aria-pressed="(true|false)">[^<]*<i>([\d,]+)<\/i>/g)].map(x => [x[1], x[2], +x[3].replace(/,/g, '')]);
  assert.deepEqual(nav.map(x => x[0]), m.order);
  assert.deepEqual(nav.filter(x => x[1] === 'true').map(x => x[0]), ['most-volatile']);
  for (const [k, , size] of nav) assert.equal(size, every(m).find(x => x.key === k).full.rows.length, k);
  // a phone leads with the ticker and the figure the list is ranked by
  const phone = kit.full.body(l, { key: l.key }, true), heads = [...phone.matchAll(/data-jts-sort="([a-z0-9_]+)"/g)].map(x => x[1]);
  assert.deepEqual(heads.slice(0, 5), ['t', 'hv21', 'hv21p', 'dv', 'hvp']);
  // found by ticker from its start, by name, sector or industry anywhere; and saved as CSV with a header and a line a row
  const one = l.full.rows[3], found = kit.full.rows(l.full, { q: one.t.toLowerCase() });
  assert.ok(found.includes(one) && found.every(r => r.t.toLowerCase().startsWith(one.t.toLowerCase()) || [r.name, r.sector, r.industry].some(v => String(v || '').toLowerCase().includes(one.t.toLowerCase()))));
  const csv = kit.full.csv(l.full, l.full.rows).split('\n');
  assert.equal(csv.length, n + 1); assert.equal(csv[0].split(',')[0], 'Ticker'); assert.ok(csv[1].startsWith(l.full.rows[0].t + ','));
});

test('a list written without columns still opens: its name, detail, figure and note, the figure ranked by its value', () => {
  const { finish, figure } = kit.full, D = '—', MINUS = '−';
  assert.deepEqual(['$1.2B', '+$3.4M', MINUS + '$300.0M', '$950', '12.5%', MINUS + '2.1%', '2.1×', '12×+', '+3.4 pts', '1,234', '88'].map(figure), [1.2e9, 3.4e6, -3e8, 950, 12.5, -2.1, 2.1, 12, 3.4, 1234, 88]);
  assert.deepEqual(['p12', 'A 88', D, '', 'held', '2026-11-20'].map(figure), [null, null, null, null, null, null]);
  const m = finish({ lists: [{ key: 'x', title: 'X', rows: [{ name: 'Alpha', sub: 'first', big: '$950', small: 'note' }, { name: 'Beta', big: '$1.2B', tone: 'up' }, { name: 'Gamma', big: D }, { name: 'Delta', big: MINUS + '$300.0M', tone: 'down' }] },
    { key: 'y', title: 'Y', rows: [{ name: 'One', big: 'held' }, { name: 'Two', big: 'broken' }] }, { key: 'z', title: 'Z', rows: [] }, { key: 'kept', title: 'K', rows: [{ name: 'a' }], full: { cols: [], rows: [1], units: ['a', 'b'] } }] });
  const [x, y, z, kept] = m.lists;
  assert.deepEqual(x.full.cols.map(c => [c.id, c.t, c.type]), [['name', 'Name', 's'], ['sub', 'Detail', 's'], ['big', 'Figure', 'n'], ['small', 'Note', 's']]);
  assert.deepEqual(kit.full.rows(x.full, { sort: { k: 'big', d: -1 } }).map(r => r.name), ['Beta', 'Alpha', 'Delta', 'Gamma']);
  assert.deepEqual(kit.full.rows(x.full, { sort: { k: 'big', d: 1 } }).map(r => r.name), ['Delta', 'Alpha', 'Beta', 'Gamma']);
  const big = x.full.cols[2];
  assert.equal(big.fmt(x.rows[1]), '$1.2B'); assert.equal(big.tone(x.rows[3]), 'down'); assert.equal(x.full.sort, null);
  assert.deepEqual(y.full.cols.map(c => [c.id, c.type]), [['name', 's'], ['big', 's']]);
  assert.equal(z.full, undefined, 'a list with nothing in it has nothing to open');
  assert.deepEqual(kept.full.rows, [1], 'a list that brings its own whole keeps it');
  assert.deepEqual(finish({ hide: true }), { hide: true });
});
