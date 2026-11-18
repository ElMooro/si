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

// ---- the two engines after 2026-11-18 (ARK 2.1.0 publishes every move; the insider scanner 2.2.0 cleans its own list)
const arkV2 = () => ({ ...structuredClone(feeds['data/ark-holdings.json']), ...structuredClone(feeds['v2/ark-overlay']) });

test('ARK 2.1.0: every move is listed, a fund view counts its own lines, and a carried or stale day says so', () => {
  const d = arkV2(), all = kit.adapters.ark.build([d], {}), one = kit.adapters.ark.build([d], { fund: 'ARKK' });
  const adds = all.lists.find(l => l.key === 'adds');
  assert.equal(adds.rows.length, 72);
  assert.equal(adds.n, 72);
  assert.equal(adds.note, '', 'nothing is left out, so nothing needs saying');
  assert.match(all.eyebrow, /holdings 2026-11-16 vs 2026-11-13/);
  assert.match(all.title, /72 added to, 1 cut, 0 opened, 0 closed since the 2026-11-13 holdings/);
  assert.match(all.foot, /Every such line is listed\./);
  assert.doesNotMatch(all.foot, /15 largest/);
  const dollars = adds.rows.map(r => +r.big.replace(/[+$,]/g, '').replace(/M$/, 'e6').replace(/B$/, 'e9'));
  assert.deepEqual(dollars, [...dollars].sort((a, b) => b - a), 'ranked by dollars, not by the engine\'s percent order');
  const mine = d.diff_vs_prev.all.position_adds.filter(x => x.fund === 'ARKK').length;
  const oneAdds = one.lists.find(l => l.key === 'adds');
  assert.equal(oneAdds.rows.length, mine);
  assert.equal(oneAdds.n, mine);
  assert.match(one.title, new RegExp(`^ARKK \\(.*${mine} added to, 0 cut, 0 opened, 0 closed`));
  assert.match(one.tiles[3].s, /lines in ARKK/);
  assert.equal(one.tiles[5].v, String(d.holdings_by_fund.ARKK.length - mine), 'unchanged = the fund\'s lines less what moved');
  assert.deepEqual(all.notes[1][0], 'Stale at the source:');
  assert.match(all.notes[1][1], /ARKW’s holdings are dated 2026-10-06, not 2026-11-16/);
  assert.doesNotMatch(all.sub, /nothing newer/);
  const carried = kit.adapters.ark.build([{ ...d, diff_is_carried: true }], {});
  assert.match(carried.sub, /nothing newer has been published since, so these are still that day’s moves/);
  const closed = kit.adapters.ark.build([{ ...d, diff_vs_prev: { ...d.diff_vs_prev, n_closed_positions: 1, all: { ...d.diff_vs_prev.all, closed_positions: [{ fund: 'ARKQ', ticker: 'GONE', company: 'GONE CO', prev_shares: 1200, prev_value: 3400000 }] } } }], {});
  const row = closed.lists.find(l => l.key === 'closed').rows[0];
  assert.deepEqual([row.name, row.big, row.small], ['GONE · GONE CO', '$3.4M', '1,200 sh']);
  assert.doesNotMatch(JSON.stringify([all, one, carried, closed]), /undefined|NaN|Infinity|\[object|\bnull\b/);
});

test('insider scanner 2.2.0: dollars are every company with a purchase; what was set aside is shown, never counted', () => {
  const d = structuredClone(feeds['v2/insider-clusters.json']), m = kit.adapters.insiders.build([d], {});
  assert.ok(d.hygiene.applied && d.version === '2.2.0');
  const buys = d.all_ticker_buys;
  near(sum(m.ring.slices, s => s.value), sum(buys, b => b.total_value));
  assert.equal(m.lists.find(l => l.key === 'dollars').rows.length, buys.length);
  assert.ok(buys.length > d.clusters.length, 'companies with a single purchase are in the dollars now');
  assert.match(m.title, new RegExp(`across ${buys.length} companies in 30 days`));
  const aside = m.lists.find(l => l.key === 'aside');
  assert.deepEqual(aside.rows.map(r => r.name.split(' ')[0]).sort(), ['AASP', 'LONA']);
  assert.ok(aside.rows.every(r => /not counted/.test(r.small) && /worth more than the whole company/.test(r.sub)));
  const counted = JSON.stringify({ ...m, lists: m.lists.filter(l => l.key !== 'aside') });
  assert.doesNotMatch(counted, /LONA|AASP/);
  for (const l of m.lists) if (l.key !== 'aside') assert.ok(l.rows.every(r => !/^(MNKKQ|MNKPF)\b/.test(r.name)), `${l.key}: one row for the filing, under MNK`);
  assert.ok(sum(buys, b => b.total_value) < 1e9, 'the $9.9B and $200B records are not in the total');
  const mnk = m.lists.find(l => l.key === 'dollars').rows.find(r => /^MNK\b/.test(r.name));
  assert.match(mnk.sub, /also filed as MNKKQ, MNKPF/);
  assert.match(m.lists.find(l => l.key === 'dollars').rows.find(r => /^BH\b/.test(r.name)).sub, /2 lines in another class of security/);
  assert.match(m.foot, /175 feed rows were 52 lines\); 2 impossible records \(\$209\.92B as filed\) set aside and listed/);
  assert.match(m.sub, /2 impossible records set aside/);
  assert.equal(m.tiles.length, 6);
  assert.doesNotMatch(JSON.stringify(m), /undefined|NaN|Infinity|\[object|\bnull\b/);
  // an engine whose hygiene did not run falls back to the scored set, as before
  const old = kit.adapters.insiders.build([{ ...d, hygiene: { applied: false } }], {});
  assert.equal(old.lists.find(l => l.key === 'dollars').rows.length, d.clusters.length);
  assert.equal(old.lists.find(l => l.key === 'aside'), undefined);
});
