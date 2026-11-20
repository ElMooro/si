// The tearsheet kit (jh-tearsheet.js): one sheet + one ring per desk, built from the desk's own feed.
// Fixtures are trimmed copies of the live feeds of 2026-11-17 (ops 6828 / 6829) - real values, shortened lists.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const kit = require('../jh-tearsheet.js');
const root = path.join(__dirname, '..');
const feeds = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/tearsheet-feeds.json'), 'utf8'));
// the Leaders feed is 3 MB: its trimmed copy (every leader, industry and fund; only the fields the sheet reads; ops 6838) has its own file
feeds['data/leaders.json'] = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/tearsheet-leaders.json'), 'utf8'));
// the Wyckoff desks' six feeds (real, trimmed; tests/wyckoff-tearsheets.test.js reads them tab by tab)
Object.assign(feeds, Object.fromEntries(Object.entries(JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/tearsheet-wyckoff.json'), 'utf8'))).filter(([k]) => k.startsWith('data/'))));
// the Volatility desk's feed: the slice of the real artifact its own desk test reads (ops 6851 dry run)
feeds['data/stock-volatility.json'] = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/stock-volatility-desk.json'), 'utf8'));
const PAGES = { 'stock-volatility.html': 'volatility', 'ark.html': 'ark', 'stablecoin-flow.html': 'stablecoin', 'firm-book.html': 'firmbook', 'paper-book.html': 'paperbook', 'sp500.html': 'sp500', 'insider-clusters.html': 'insiders', 'foreign-flows.html': 'tic', 'capital-inflows.html': 'capital', 'leaders.html': 'leaders', 'wyckoff-desk.html': 'wyckoff', 'capitulation-desk.html': 'capitulation' };
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


test('Leaders: every count on the sheet is the desk\'s own, and the ring is each leader\'s market cap once', () => {
  const d = feeds['data/leaders.json'], S = d.summary, m = build('leaders');
  const ranked = d.rows.filter(r => r.vs && !r.stale);
  assert.equal(ranked.length, S.n);                                                // the engine's own "ranked" set
  assert.match(m.eyebrow, new RegExp(`${S.n} leaders ranked in ${d.industries.length} industries`));
  // the ring: five labels, in order, adding up to the ranked leaders' market cap
  assert.deepEqual(m.ring.slices.map(s => [s.name, s.tone]), [['Strong accumulation', 'up'], ['Accumulation', 'up'], ['Neutral', 'oth'], ['Distribution', 'down'], ['Strong distribution', 'down']]);
  near(sum(m.ring.slices, s => s.value), sum(ranked, r => r.mcap));
  assert.equal(m.ring.center, kit.money(sum(ranked, r => r.mcap)));
  const counts = m.ring.slices.map(s => +s.small.split(' ')[0]);
  assert.equal(counts[0] + counts[1], S.accumulation); assert.equal(counts[3] + counts[4], S.distribution);
  assert.equal(counts[0], S.strong_accumulation); assert.equal(counts[4], S.strong_distribution);
  assert.equal(sum(counts, c => c), S.n);
  // tiles: the verdict tiles' figures, not a second count
  const tile = k => m.tiles.find(t => t.k === k), text = t => t.v.replace(/<[^>]+>/g, '');
  assert.equal(tile('Ahead of the S&P 500').s.split(' over ')[0], `${S.beating_m} of ${S.n}`);
  assert.equal(text(tile('Ahead of the S&P 500')), `${Math.round(S.pct_beating_m)}%`);
  assert.equal(text(tile('Leaders by tape')), `${S.accumulation} / ${S.distribution}`);
  assert.equal(text(tile('Smart money')), `${S.smart_buying} / ${S.smart_selling}`);
  assert.equal(text(tile('Signed dollar volume')), kit.money(S.mf21_net_usd, true));
  // the lists are whole: nothing is cut before MORE ROWS
  const list = k => m.lists.find(l => l.key === k);
  assert.equal(list('strongest').rows.length + list('weakest').rows.length, S.n);
  assert.equal(list('accumulation').rows.length, S.accumulation); assert.equal(list('distribution').rows.length, S.distribution);
  assert.equal(list('smart-buying').rows.length, S.smart_buying); assert.equal(list('smart-selling').rows.length, S.smart_selling);
  assert.equal(list('industries-leading').rows.length + list('industries-lagging').rows.length, d.industries.length);
  assert.equal(sum(list('printed').rows, r => +r.big), d.changes.n_new);
  assert.match(list('printed').title, new RegExp(d.changes.prev_as_of));
  // most notable = the largest leader under each strong label
  const biggest = label => ranked.filter(r => r.ad.label === label).sort((a, b) => b.mcap - a.mcap)[0];
  assert.ok(m.cards[0].title.startsWith(biggest('STRONG DISTRIBUTION').ticker + ' ') && m.cards[0].tone === 'down');
  assert.ok(m.cards[1].title.startsWith(biggest('STRONG ACCUMULATION').ticker + ' ') && m.cards[1].tone === 'up');
});

test('Leaders: the window buttons move every list that has a horizon, and the month is the default', () => {
  const d = feeds['data/leaders.json'], S = d.summary;
  assert.deepEqual(build('leaders').controls.map(c => [c.id, c.on]), [['w', false], ['m', true], ['q', false]]);
  assert.equal(JSON.stringify(build('leaders', { fund: 'nothing' })), JSON.stringify(build('leaders', { fund: 'm' })));
  for (const [k, word, obs] of [['w', 'the week', 5], ['m', 'the month', 21], ['q', 'the quarter', 63]]) {
    const m = build('leaders', { fund: k }), list = key => m.lists.find(l => l.key === key);
    assert.ok(m.title.includes(`${Math.round(S['pct_beating_' + k])}% of the leaders are ahead of the S&P 500 over ${word}`), m.title);
    assert.equal(list('strongest').n, S['beating_' + k]);
    const top = d.rows.filter(r => r.vs && !r.stale).sort((a, b) => b.vs[k] - a.vs[k])[0];
    assert.ok(list('strongest').rows[0].name.startsWith(top.ticker + ' '), k);
    assert.ok(list('fund-inflows').hint.endsWith(`${obs} reports`));
    assert.ok(m.tiles[0].s.includes('SPY ' + kit.pct(d.benchmark.h[k], 1, true)));
  }
  // the ring is the tape as of the bars' date: it does not move with the window
  assert.equal(JSON.stringify(build('leaders', { fund: 'w' }).ring), JSON.stringify(build('leaders', { fund: 'q' }).ring));
});

test('Leaders: a fund whose shares split inside the window is set aside, never ranked as money in', () => {
  const d = feeds['data/leaders.json'], by = Object.fromEntries(d.etfs.map(f => [f.ticker, f]));
  // the real case: SOXX and ITA split 3-for-1 on 2026-11-05, and their reported creations read +$90.7B and +$24.2B
  for (const t of ['SOXX', 'ITA']) { assert.equal(by[t].splits[0].date, '2026-11-05'); assert.ok(by[t].flow.w21.pct_aum > 200); }
  for (const k of ['m', 'q']) {
    const m = build('leaders', { fund: k }), ins = m.lists.find(l => l.key === 'fund-inflows'), outs = m.lists.find(l => l.key === 'fund-outflows'), flow = k === 'm' ? 'w21' : 'w63';
    assert.ok(![...ins.rows, ...outs.rows].some(r => r.name === 'SOXX' || r.name === 'ITA'), k);
    assert.match(ins.note, /Set aside, not counted: ITA \+\$\d+\.\d+B \(3-for-1 share split on 2026-11-05 inside the window\); SOXX \+\$\d+\.\d+B/);
    const kept = d.etfs.filter(f => f.ticker !== 'SOXX' && f.ticker !== 'ITA' && f.flow && f.flow.current && Number.isFinite((f.flow[flow] || {}).usd));
    const tile = m.tiles.find(t => t.k === 'Industry funds');
    assert.equal(tile.v.replace(/<[^>]+>/g, ''), kit.money(sum(kept, f => f.flow[flow].usd), true));
    assert.ok(tile.s.endsWith('2 set aside'));
    assert.equal(ins.rows.length + outs.rows.length, kept.filter(f => f.flow[flow].usd !== 0).length);
    assert.equal(ins.rows[0].name, 'SMH');                                          // the largest creation that is money
  }
  // the week's window ends after the split: nothing is set aside and SOXX's redemptions are listed as reported
  const w = build('leaders', { fund: 'w' });
  assert.ok(!w.lists.find(l => l.key === 'fund-inflows').note);
  assert.equal(w.lists.find(l => l.key === 'fund-outflows').rows[0].name, 'SOXX');
  assert.ok(!w.tiles.find(t => t.k === 'Industry funds').s.includes('set aside'));
  // a creation larger than the fund's own assets is set aside whatever its cause
  const doc = structuredClone(d), xlk = doc.etfs.find(f => f.ticker === 'XLK');
  xlk.flow.w21 = { usd: 3 * xlk.flow.w21.usd / (xlk.flow.w21.pct_aum / 100), pct_aum: 300, end: xlk.flow.w21.end };
  const odd = kit.adapters.leaders.build([doc], { fund: 'm' }).lists.find(l => l.key === 'fund-inflows');
  assert.ok(!odd.rows.some(r => r.name === 'XLK')); assert.match(odd.note, /XLK [^;]*creations larger than the fund\u2019s own assets/);
});

test('Leaders: the sheet takes the desk\'s copy of the feed, and reads it itself only when none arrives', () => {
  const ad = kit.adapters.leaders;
  assert.deepEqual(ad.share, { global: 'JHLeadersDoc', event: 'jh:leaders-doc', ms: 8000 });
  const desk = fs.readFileSync(path.join(root, 'jh-leaders.js'), 'utf8');
  assert.match(desk, /window\.JHLeadersDoc = d;/); assert.match(desk, /new CustomEvent\("jh:leaders-doc", \{ detail: d \}\)/);
  for (const name of Object.keys(kit.adapters)) if (!['leaders', 'wyckoff', 'capitulation', 'volatility'].includes(name)) assert.equal(kit.adapters[name].share, undefined, name);
  assert.equal(kit.version, '1.4.0');
});


// ---------------------------------------------------------------------------------------------------- Volatility desk
test('Volatility: the ring is the ranked stocks\' market value by band, and every figure is the desk\'s own', () => {
  const d = feeds['data/stock-volatility.json'], m = build('volatility'), med = d.market.stock.med, ix = Object.fromEntries(d.cols.map((c, i) => [c, i]));
  const total = sum(d.bands, b => b.mcap), hot = sum(d.bands.filter(b => b.from >= 40), b => b.mcap);
  near(sum(m.ring.slices, s => s.value), total);
  assert.equal(m.ring.center, kit.money(total));
  assert.equal(m.ring.n, sum(d.bands, b => b.n));
  assert.deepEqual(m.ring.slices.map(s => s.name), d.bands.map(b => `${b.band} a year`));
  assert.deepEqual(m.ring.slices.map(s => s.label), ['under 15', '15-25', '25-40', '40-60', '60-100', '100+']);   // a band's own % beside the slice's share would read as two shares
  assert.deepEqual(m.ring.slices.map(s => s.small), d.bands.map(b => `${b.n.toLocaleString('en-US')} stock${b.n === 1 ? '' : 's'}`));
  const ops = m.ring.slices.map(s => s.op);
  assert.ok(ops.every((o, i) => s_tone(m.ring.slices[i]) && (i === 0 || o > ops[i - 1])), 'one hue, darker with volatility');
  assert.equal(m.tiles[0].v, `${med.hv21.toFixed(1)}%`);
  assert.match(m.tiles[0].s, new RegExp(`^${med.hv21p.toFixed(1)}% a month ago`));
  assert.equal(m.tiles[2].v, `${d.market.benchmark.hv21.toFixed(1)}%`);
  assert.equal(m.tiles[3].v, `${med.sp_m.toFixed(1)}%`);
  assert.equal(m.tiles[4].v, `${Math.round(hot / total * 100)}%`);
  assert.equal(m.tiles[5].v, String(d.universe.set_aside.stock));
  assert.match(m.tiles[5].s, new RegExp(`^${d.universe.pinned.stock} pinned to a deal price`));
  assert.match(m.title, new RegExp(`moving at ${med.hv21.toFixed(1)}% a year, up from ${med.hv21p.toFixed(1)}% a month ago`));
  // the lists are the engine's boards, name for name and in its order
  const tick = key => m.lists.find(l => l.key === key).rows.map(r => r.name.split(' · ')[0]);
  assert.deepEqual(tick('most-volatile'), d.boards.most_volatile);
  assert.deepEqual(tick('least-volatile'), d.boards.least_volatile);
  assert.deepEqual(tick('widest'), d.boards.widest_m);
  assert.deepEqual(tick('narrowest'), d.boards.narrowest_m);
  assert.deepEqual(tick('rising'), d.boards.vol_rising);
  assert.deepEqual(tick('falling'), d.boards.vol_falling);
  assert.deepEqual(tick('pinned').slice().sort(), d.pinned.slice().sort());
  const top = d.rows.find(r => r[ix.t] === d.boards.most_volatile[0]), low = d.rows.find(r => r[ix.t] === d.boards.least_volatile[0]);
  assert.equal(m.lists[0].rows[0].big, `${top[ix.hv21].toFixed(1)}%`);
  assert.equal(m.cards[0].big, `${top[ix.hv21].toFixed(1)}%`); assert.equal(m.cards[1].big, `${low[ix.hv21].toFixed(1)}%`);
  assert.ok(m.cards[0].title.startsWith(top[ix.t]) && m.cards[1].title.startsWith(low[ix.t]));
  assert.equal(m.lists.find(l => l.key === 'sectors').rows.length, d.by_sector.length);
  const sec = m.lists.find(l => l.key === 'sectors').rows.map(r => parseFloat(r.big));
  assert.ok(sec.every((v, i) => i === 0 || v <= sec[i - 1]), 'sectors, most volatile first');
  // volatility is neither good nor bad: nothing on the sheet is coloured up or down
  assert.doesNotMatch(JSON.stringify(m), /"tone":"(up|down)"|class=\\"(up|down)\\"/);
  function s_tone(s) { return s.tone === 'pos'; }
});

test('Volatility: the four buttons move the spread lists and the spread tile; the month is the default', () => {
  const d = feeds['data/stock-volatility.json'], med = d.market.stock.med;
  const dp = v => `${v.toFixed(Math.abs(v) < 10 ? 2 : 1)}%`;
  assert.deepEqual(build('volatility').controls.map(c => [c.id, c.label, c.on]), [['d', '1 DAY', false], ['w', '1 WEEK', false], ['m', '1 MONTH', true], ['q', '1 QUARTER', false]]);
  for (const k of ['d', 'w', 'm', 'q']) {
    const m = build('volatility', { fund: k }), tick = key => m.lists.find(l => l.key === key).rows.map(r => r.name.split(' · ')[0]);
    assert.deepEqual(tick('widest'), d.boards[`widest_${k}`], k);
    assert.deepEqual(tick('narrowest'), d.boards[`narrowest_${k}`], k);
    assert.equal(m.tiles[3].v, dp(med[`sp_${k}`]), k);
    assert.equal(m.controls.find(c => c.on).id, k);
    // what has no window does not move
    assert.deepEqual(tick('most-volatile'), d.boards.most_volatile);
    assert.equal(m.ring.center, build('volatility').ring.center);
  }
  assert.equal(build('volatility', { fund: 'zz' }).controls.find(c => c.on).id, 'm');
});

test('Volatility: the sheet takes the desk\'s copy of the feed and sits above the desk\'s own sections', () => {
  const ad = kit.adapters.volatility, js = fs.readFileSync(path.join(root, 'jh-stock-volatility.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'stock-volatility.html'), 'utf8');
  assert.deepEqual(ad.share, { global: 'JHStockVolDoc', event: 'jh:stock-vol-doc', ms: 8000 });
  assert.match(js, /window\.JHStockVolDoc = d;/); assert.match(js, /new CustomEvent\("jh:stock-vol-doc", \{ detail: d \}\)/);
  assert.ok(html.indexOf('data-jts="volatility"') > html.indexOf('id="stamp"') && html.indexOf('data-jts="volatility"') < html.indexOf('data-jh-key="verdict"'));
  // a feed with no board and no band still gives a sheet that says so, not a broken one
  const bare = structuredClone(feeds['data/stock-volatility.json']); bare.boards = {}; bare.pinned = [];
  const m = ad.build([bare], {});
  assert.equal(m.cards[0].title, 'None'); assert.equal(m.lists[0].rows.length, 0); assert.doesNotMatch(JSON.stringify(m), /undefined|NaN/);
});
