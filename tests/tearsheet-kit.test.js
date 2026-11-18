// The tearsheet kit (jh-tearsheet.js): one sheet + one ring per desk, built from the desk's own feed.
// Fixtures are trimmed copies of the live feeds of 2026-11-17 (ops 6828 / 6829) - real values, shortened lists.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const kit = require('../jh-tearsheet.js');
const root = path.join(__dirname, '..');
const feeds = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/tearsheet-feeds.json'), 'utf8'));
const PAGES = { 'ark.html': 'ark', 'stablecoin-flow.html': 'stablecoin', 'firm-book.html': 'firmbook', 'paper-book.html': 'paperbook', 'sp500.html': 'sp500', 'insider-clusters.html': 'insiders', 'foreign-flows.html': 'tic', 'capital-inflows.html': 'capital' };
const build = (name, state) => kit.adapters[name].build(kit.adapters[name].feeds.map(f => structuredClone(feeds[f.slice(1)])), state || {});
const sum = (list, f) => list.reduce((a, x) => a + f(x), 0);
const near = (a, b, tol) => assert.ok(Math.abs(a - b) <= (tol == null ? 1e-6 * Math.max(1, Math.abs(b)) : tol), `${a} vs ${b}`);

test('every desk that mounts the sheet names a real adapter, keeps it out of section numbering and loads the kit once', () => {
  for (const [page, name] of Object.entries(PAGES)) {
    const html = fs.readFileSync(path.join(root, page), 'utf8');
    const mounts = html.match(/<div[^>]*\sdata-jts="[a-z0-9]+"[^>]*>/g) || [];
    assert.equal(mounts.length, 1, page);
    assert.match(mounts[0], new RegExp(`data-jts="${name}"`), page);
    assert.match(mounts[0], /\sid="jh-tearsheet"/, page);
    assert.match(mounts[0], /\sdata-jh-chrome[\s>]/, `${page}: a numbered sheet renumbers existing sections`);
    assert.equal((html.match(/src="\/jh-tearsheet\.js[^"]*"/g) || []).length, 1, page);
    assert.ok(kit.adapters[name] && kit.adapters[name].feeds.every(f => /^\/data\/[a-z0-9-]+\.json$/.test(f)), name);
  }
  assert.deepEqual(Object.keys(kit.adapters).sort(), Object.values(PAGES).sort());
});

test('the kit file is pure ASCII, so it reads the same whatever charset the host page declares', () => {
  const src = fs.readFileSync(path.join(root, 'jh-tearsheet.js'));
  assert.equal(src.findIndex(b => b > 127), -1);
});

test('every sheet has the picture\'s anatomy and prints no broken value', () => {
  for (const name of Object.keys(kit.adapters)) {
    const m = build(name);
    assert.ok(m.eyebrow && m.title && m.sub && m.foot, name);
    assert.equal(m.tiles.length, 6, `${name}: six tiles`);
    assert.ok(m.cards.length >= 1 && m.cards.length <= 2, name);
    assert.equal(m.notes.length, 3, name);
    assert.ok(m.lists.length >= 3, name);
    assert.ok(m.ring && m.ring.slices.length >= 2, name);
    assert.doesNotMatch(JSON.stringify(m), /undefined|NaN|Infinity|\[object|\bnull\b/, name);
    for (const s of m.ring.slices) assert.ok(Number.isFinite(s.value) && s.value > 0 && ['pos', 'up', 'down', 'oth'].includes(s.tone), `${name}: ${s.name}`);
    for (const l of m.lists) for (const r of l.rows) assert.ok(typeof r.name === 'string' && r.name.length, `${name}/${l.key}`);
  }
});

test('each ring adds up to the whole its centre states', () => {
  const ring = n => sum(build(n).ring.slices, s => s.value);
  const ark = feeds['data/ark-holdings.json'];
  near(ring('ark'), sum(Object.values(ark.holdings_by_fund).flat(), h => h.market_value));
  near(sum(build('ark', { fund: 'ARKK' }).ring.slices, s => s.value), sum(ark.holdings_by_fund.ARKK, h => h.market_value));
  near(ring('stablecoin'), feeds['data/stablecoin-flow.json'].aggregate.total_usd);
  near(ring('paperbook'), 100, 0.11);                                             // weights + cash, published to one decimal
  near(ring('sp500'), sum(Object.values(feeds['data/sp500.json'].sectors), s => s.weight_pct), 1e-9);
  near(ring('tic'), feeds['data/tic-flows.json'].total_foreign_holdings.current_b);
  const cap = feeds['data/capital-inflows.json'];
  near(ring('capital'), sum(Object.values(cap.by_asset_class), c => Math.abs(c.latest_month_b)));
  near(sum(Object.values(cap.by_asset_class), c => c.latest_month_b), cap.headline.latest_month_b, 0.0015);
  const book = feeds['data/firm-book.json'].equity_book;
  near(ring('firmbook'), sum(book, p => Math.abs(p.gross_pct)));
});

test('a two-sided ring is solid on one side and hatched on the other, never colour alone', () => {
  const cap = build('capital'), firm = build('firmbook');
  assert.deepEqual([...new Set(cap.ring.slices.map(s => s.tone))].sort(), ['down', 'up']);
  assert.deepEqual([...new Set(firm.ring.slices.map(s => s.tone))].sort(), ['down', 'up']);
  const svg = kit.ring(cap.ring);
  const arcs = svg.match(/<path class="jts-arc[^>]*>/g);
  assert.equal(arcs.length, cap.ring.slices.length);
  assert.equal(arcs.filter(a => /fill="url\(#jts-hatch-\d+\)"/.test(a)).length, cap.ring.slices.filter(s => s.tone === 'down').length);
  assert.equal(arcs.filter(a => /class="jts-arc up"/.test(a) && /fill="url/.test(a)).length, 0);
  near(sum(arcs, a => +/data-share="([\d.]+)"/.exec(a)[1]), 100, 0.01);
  for (const s of cap.ring.slices) assert.match(s.big, /^[+−]\$/, 'every slice carries its sign');
  assert.match(svg, /<pattern id="jts-hatch-\d+"/);
  assert.doesNotMatch(svg, /#[0-9a-fA-F]{3,8}\b|rgb\(/, 'the ring takes its colours from the site tokens, not literals');
});

test('ARK: the fund view narrows positions and listed moves to that fund and says the feed lists only the largest', () => {
  const all = build('ark'), one = build('ark', { fund: 'ARKK' }), d = feeds['data/ark-holdings.json'];
  assert.equal(all.controls.length, d.funds_tracked.length + 1);
  assert.match(all.title, new RegExp(`${d.diff_vs_prev.n_position_adds} added to, ${d.diff_vs_prev.n_position_trims} cut`));
  assert.match(one.title, /^ARKK \(/);
  const adds = one.lists.find(l => l.key === 'adds');
  assert.ok(adds.rows.length && adds.rows.every(r => /^ARKK /.test(r.sub)));
  assert.equal(adds.rows.length, d.diff_vs_prev.highlights.biggest_adds.filter(x => x.fund === 'ARKK').length);
  assert.match(all.lists.find(l => l.key === 'adds').note, /lists the 15 largest of 72/);
  assert.match(all.eyebrow, /snapshot 2026-11-17 vs 2026-11-16/);
});

test('insiders: dollars come from the scored set, one row per company - a raw record the engine does not score stays out', () => {
  const d = feeds['data/insider-clusters.json'], m = build('insiders');
  const raw = d.all_ticker_buys[0];
  assert.equal(raw.ticker, 'LONA');                                               // $9.9B for one holder at a price of shares / 1000
  assert.ok(raw.total_value > 9e9 && !d.clusters.some(c => c.ticker === 'LONA'));
  assert.doesNotMatch(JSON.stringify(m), /LONA/);
  const ciks = new Set(d.clusters.map(c => c.cik));
  assert.ok(d.clusters.filter(c => c.cik === '0001567892').length === 3, 'fixture keeps the company listed under three tickers');
  const dollars = m.lists.find(l => l.key === 'dollars');
  assert.equal(dollars.rows.length, ciks.size);
  assert.equal(dollars.rows.filter(r => /^MNK/.test(r.name)).length, 1);
  const best = {};
  for (const c of d.clusters) if (!best[c.cik] || c.score > best[c.cik].score) best[c.cik] = c;
  near(sum(m.ring.slices, s => s.value), sum(Object.values(best), c => c.total_value));
});

test('holdings are not called buying: the Treasury holders sheet keeps the feed\'s own distinction', () => {
  const m = build('tic');
  assert.match(m.sub, /not the same as buying or selling/);
  assert.deepEqual(m.lists.map(l => l.title).slice(0, 2), ['Holdings up', 'Holdings down']);
  assert.doesNotMatch(JSON.stringify(m.lists), /\bbought\b|\bsold\b/i);
  const t = feeds['data/tic-flows.json'], named = sum(Object.values(t.individual), r => r.current_b);
  const rest = m.ring.slices.find(s => s.name === 'All other holders');
  near(rest.value, t.total_foreign_holdings.current_b - named);
});

test('numbers read the same everywhere: whole dollars under a million, a true minus sign, signed when asked', () => {
  assert.equal(kit.money(98959.76), '$98,960');
  assert.equal(kit.money(8923), '$8,923');
  assert.equal(kit.money(1.84e6), '$1.8M');
  assert.equal(kit.money(-3.746e9, true), '−$3.75B');
  assert.equal(kit.money(300.724e9, true), '+$300.72B');
  assert.equal(kit.money(9.331654e12), '$9.33T');
  assert.equal(kit.money(null), '—');
  assert.equal(kit.pct(-5.633, 1, true), '−5.6%');
  assert.equal(kit.pct(undefined), '—');
});
