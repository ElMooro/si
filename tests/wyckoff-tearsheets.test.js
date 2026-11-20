// The eight Wyckoff desks behind the chart's icons, in the tearsheet design (Khalid 2026-11-19): Selling climax,
// Capitulation, Buying climax, Candle spread, Spring - on the feeds their tables already read - and Accumulation, Bottom,
// Distribution on data/campaign-scan.json, the chart's own marks over every stock. Every expectation is computed here
// from the real (trimmed) feeds in tests/fixtures/tearsheet-wyckoff.json, not copied from the sheet.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const kit = require('../jh-tearsheet.js');
const root = path.join(__dirname, '..');
const read = (f) => fs.readFileSync(path.join(root, f), 'utf8');
const FX = JSON.parse(read('tests/fixtures/tearsheet-wyckoff.json'));
const docs = () => structuredClone({ __wyckoff: 1, bottom: FX['data/bottom.json'], phase: FX['data/phase-detector.json'], katlin: FX['data/katlin.json'], radar: FX['data/accumulation-radar.json'], capit: FX['data/capitulation-desk.json'], scan: FX['data/campaign-scan.json'] });
const sheet = (tab, fund, d) => kit.adapters.wyckoff.build([d || docs()], { tab, fund });
const list = (m, key) => m.lists.find((l) => l.key === key);
const firstTicker = (row) => row.name.split(' \u00b7 ')[0];
const tile = (m, k) => m.tiles.find((t) => t.k === k);
const text = (html) => String(html).replace(/<[^>]+>/g, '');
const num = (n) => n.toLocaleString('en-US');
const TABS = ['sc', 'capit', 'bc', 'spread', 'spring', 'accum', 'bottom', 'dist'];

// the desk's own row functions, lifted out of wyckoff-desk.html so the sheets are held to the tables' rules
function deskRows(data, tab) {
  const html = read('wyckoff-desk.html');
  const src = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)][0][1];
  const nodes = {}, mk = () => ({ childNodes: [], style: {}, dataset: {}, appendChild() {}, querySelector() { return null; }, insertBefore() {}, removeChild() {}, set innerHTML(v) { this._h = v; }, get innerHTML() { return this._h || ''; }, textContent: '', hidden: false });
  const feeds = { bottom: data.bottom, 'phase-detector': data.phase, katlin: data.katlin, 'accumulation-radar': data.radar };
  const ctx = { window: {}, document: { hidden: false, createElement: mk, getElementById: (id) => nodes[id] || (nodes[id] = mk()), querySelectorAll: () => [], addEventListener() {} }, location: { search: '?tab=' + tab }, history: { replaceState() {} }, URLSearchParams, Date,
    setInterval() {}, fetch: async (u) => ({ ok: true, json: async () => feeds[u.match(/\/([^/]+)\.json/)[1]] }) };
  vm.createContext(ctx);
  return new Promise((resolve) => { vm.runInContext(src, ctx); setImmediate(() => setImmediate(() => resolve([...(nodes.board.innerHTML || '').matchAll(/<tr data-t='([^']+)'/g)].map((m) => m[1])))); });
}

test('the kit carries one sheet for each of the eight desks, and steps aside on a tab that has none', () => {
  assert.equal(kit.version, '1.5.0');
  assert.deepEqual(Object.keys(kit.adapters.wyckoff.tabs).sort(), TABS.slice().sort());
  assert.deepEqual(kit.adapters.wyckoff.build([docs()], { tab: 'pump' }), { hide: true });
  assert.deepEqual(kit.adapters.wyckoff.build([docs()], { tab: 'top' }), { hide: true });
  assert.deepEqual(kit.adapters.wyckoff.feeds, ['/data/bottom.json', '/data/phase-detector.json', '/data/katlin.json', '/data/accumulation-radar.json', '/data/capitulation-desk.json', '/data/campaign-scan.json']);
  assert.equal(kit.adapters.wyckoff.soft, true, 'one feed missing must not blank the other tabs');
  assert.deepEqual(kit.adapters.wyckoff.share, { global: 'JHWyckoffDocs', event: 'jh:wyckoff-docs', ms: 9000 });
  assert.deepEqual(kit.adapters.capitulation.share, { global: 'JHCapitDoc', event: 'jh:capit-doc', ms: 8000 });
  // without the desk's hand-over the kit reads the six feeds itself, in the adapter's order
  const d = docs();
  const own = kit.adapters.wyckoff.build([d.bottom, d.phase, d.katlin, d.radar, d.capit, d.scan], { tab: 'dist' });
  assert.equal(own.title, sheet('dist').title);
});

test('every sheet has the anatomy of the picture and prints no broken value', () => {
  for (const tab of TABS) for (const fund of ['accum', 'bottom', 'dist'].includes(tab) ? ['stock', 'etf'] : [undefined]) {
    const m = sheet(tab, fund), id = tab + (fund ? '/' + fund : '');
    assert.ok(m.eyebrow && m.title && m.sub && m.foot, id);
    assert.equal(m.tiles.length, 6, id);
    assert.equal(m.cards.length, 2, id);
    assert.equal(m.notes.length, 3, id);
    assert.ok(m.lists.length >= 3, id);
    assert.ok(m.ring.slices.length >= 2 && m.ring.caption && m.ring.aria, id);
    assert.doesNotMatch(JSON.stringify(m), /undefined|NaN|Infinity|\[object|\bnull\b|\u22120\.0%/, id);
    for (const s of m.ring.slices) assert.ok(Number.isFinite(s.value) && s.value >= 0 && ['pos', 'oth'].includes(s.tone), `${id}: a status is a part of a whole, never a signed side (${s.name})`);
    for (const l of m.lists) for (const r of l.rows) assert.ok(typeof r.name === 'string' && r.name.length && r.big !== '', `${id}/${l.key}`);
    assert.match(m.eyebrow, / tearsheet \u00b7 /, id);
  }
});

test('a missing feed fails only its own tab, with a sentence a reader can act on', () => {
  const d = docs();
  d.scan = null;
  d.capit = null;
  for (const tab of ['accum', 'bottom', 'dist']) assert.throws(() => sheet(tab, 'stock', d), /data\/campaign-scan\.json\) is not published yet/);
  assert.throws(() => sheet('capit', undefined, d), /data\/capitulation-desk\.json\) is not readable/);
  for (const tab of ['sc', 'bc', 'spread', 'spring']) assert.ok(sheet(tab, undefined, d).title, tab);
});

for (const [tab, board, statuses, markWord] of [['bottom', 'bottom', ['HELD', 'UNDERCUT', 'BROKEN'], 'Bot'], ['accum', 'accumulation', ['ABOVE', 'BASE', 'LOST'], 'Acc'], ['dist', 'distribution', ['BELOW', 'RANGE', 'RECLAIMED'], 'Dist']]) {
  test(`${tab}: the ring is every marked name once, by where its last close stands; stocks and ETFs never mix`, () => {
    const scan = FX['data/campaign-scan.json'];
    for (const kind of ['stock', 'etf']) {
      const rows = scan.boards[board].filter((r) => r.kind === kind), m = sheet(tab, kind);
      assert.ok(rows.length > 10, `${tab}/${kind}: the fixture holds ${rows.length} rows`);
      assert.equal(m.ring.n, rows.length);
      assert.equal(m.ring.center, num(rows.length));
      assert.deepEqual(m.ring.slices.map((s) => s.value), statuses.map((st) => rows.filter((r) => r.status === st).length));
      assert.equal(m.ring.slices.reduce((a, s) => a + s.value, 0), rows.length, 'every row has one of the three statuses');
      assert.deepEqual(m.controls, [{ id: 'stock', label: 'STOCKS', on: kind === 'stock' }, { id: 'etf', label: 'ETFS', on: kind === 'etf' }]);
      const scanned = kind === 'etf' ? scan.universe.scanned_etf : scan.universe.scanned_stock;
      assert.match(m.eyebrow, new RegExp(`${num(scanned)} ${kind === 'etf' ? 'ETFs' : 'stocks'} scanned$`));
      assert.equal(text(tile(m, 'On the board').v), num(rows.length));
      assert.match(m.title, new RegExp(`^${num(rows.length)} ${kind === 'etf' ? 'ETFs' : 'stocks'} `));
      assert.match(tile(m, kind === 'etf' ? 'Assets marked' : 'Market cap marked').v, /^\$[\d.,]+[KMBT]$/);
      // the largest list is the board's own order by size, whole
      const bySize = rows.slice().sort((a, b) => (b.mcap || 0) - (a.mcap || 0));
      assert.deepEqual(list(m, 'largest').rows.slice(0, 8).map(firstTicker), bySize.slice(0, 8).map((r) => r.ticker));
      assert.equal(list(m, 'largest').rows.length, rows.length);
      // percentage rankings are read among names large enough to act on
      const floor = kind === 'etf' ? 1e8 : 1e9, byTicker = Object.fromEntries(rows.map((r) => [r.ticker, r]));
      for (const l of m.lists.filter((x) => /\$1B and larger|\$100M of assets and larger/.test(x.hint))) for (const r of l.rows) assert.ok(byTicker[firstTicker(r)].mcap >= floor, `${tab}/${kind}/${l.key}: ${r.name}`);
      assert.ok(m.lists.some((x) => /larger$/.test(x.hint)), 'at least one ranking carries the size floor');
      assert.equal(!!list(m, 'sectors'), kind === 'stock', 'an ETF has no sector');
      if (kind === 'etf') assert.doesNotMatch(JSON.stringify(m.cards), /Financial Services|the largest company|of market cap/, 'a fund is not a company and has no sector');
      assert.match(m.foot, new RegExp(`as the ${markWord} icon marks`));
    }
    const s = sheet(tab, 'stock'), sectors = list(s, 'sectors').rows, stocks = scan.boards[board].filter((r) => r.kind === 'stock');
    assert.equal(sectors.reduce((a, r) => a + Number(r.big.replace(/,/g, '')), 0), stocks.length, 'the sector list adds up to the board');
  });
}

test('bottom: climaxes and confirmed lows are counted apart, and a low that broke is listed as broken', () => {
  const rows = FX['data/campaign-scan.json'].boards.bottom.filter((r) => r.kind === 'stock'), m = sheet('bottom', 'stock');
  const cl = rows.filter((r) => r.mark === 'CLIMAX'), lo = rows.filter((r) => r.mark === 'BOTTOM');
  assert.equal(cl.length + lo.length, rows.length);
  assert.equal(text(tile(m, 'Live climaxes').v), num(cl.length));
  assert.equal(text(tile(m, 'Confirmed lows').v), num(lo.length));
  assert.equal(list(m, 'new-climax').rows.length, cl.length);
  assert.equal(list(m, 'new-low').rows.length, lo.length);
  assert.ok(cl.every((r) => r.ago <= 40) && lo.every((r) => r.ago <= 126), 'the windows the engine publishes');
  const newest = cl.slice().sort((a, b) => a.ago - b.ago || (b.mcap || 0) - (a.mcap || 0))[0];
  assert.equal(firstTicker(list(m, 'new-climax').rows[0]), newest.ticker);
  const broken = rows.filter((r) => r.status === 'BROKEN');
  assert.equal(list(m, 'broken').n, broken.length);
  for (const r of list(m, 'broken').rows) assert.equal(r.tone, 'down');
  assert.ok(broken.every((r) => r.off_low_pct < 0) && rows.filter((r) => r.status === 'HELD').every((r) => r.off_low_pct >= 0), 'the status is the arithmetic of the last close against the low');
  assert.match(m.notes[2][1], /failed climax/);
  // the chart's scan stops counting climax volume at 12 times: the sheet says so instead of printing a wall of ties
  assert.doesNotMatch(JSON.stringify(list(m, 'volume')), /volume 12\.0\u00d7/);
});

test('accumulation and distribution: a mark that failed is counted beside the ones that held', () => {
  const scan = FX['data/campaign-scan.json'];
  const acc = scan.boards.accumulation.filter((r) => r.kind === 'stock'), a = sheet('accum', 'stock');
  assert.ok(acc.every((r) => r.mark === 'ACCUM' && r.ago <= scan.windows.accumulation_sessions));
  for (const r of acc.filter((x) => x.low_date)) assert.equal(r.status, r.last >= r.px ? 'ABOVE' : r.last >= r.low ? 'BASE' : 'LOST', r.ticker);
  assert.match(a.title, new RegExp(`${acc.filter((r) => r.status === 'ABOVE').length} are above the bar, ${acc.filter((r) => r.status === 'BASE').length} back in the base, ${acc.filter((r) => r.status === 'LOST').length} under its low`));
  const dist = scan.boards.distribution.filter((r) => r.kind === 'stock'), d = sheet('dist', 'stock');
  assert.ok(dist.every((r) => r.mark === 'DIST' && r.ago <= scan.windows.distribution_sessions && r.high_date < r.date), 'the mark prints after the rally high');
  for (const r of dist) assert.equal(r.status, r.last >= r.high ? 'RECLAIMED' : r.last < r.px ? 'BELOW' : 'RANGE', r.ticker);
  const effort = dist.filter((r) => r.why === 'more volume bought less').length;
  assert.equal(text(tile(d, 'Why it printed').v), `${num(effort)} / ${num(dist.length - effort)}`);
  assert.equal(list(d, 'reclaimed').n, dist.filter((r) => r.status === 'RECLAIMED').length);
  assert.match(d.notes[2][1], /reclaimed was wrong/);
});

test('a row whose tape holds a one-session move no split explains stays in the ring and leaves the rankings', () => {
  const d = docs(), all = d.scan.boards.distribution.filter((r) => r.kind === 'stock'), n0 = all.filter((r) => r.jump).length;
  const says = (n) => new RegExp(`${n} row${n === 1 ? '' : 's'} on this board carr${n === 1 ? 'ies' : 'y'} a one-session move beyond 3-to-1 that no split explains; ${n === 1 ? 'it is' : 'they are'} listed in the table and left out of the medians and rankings here\\.`);
  const before = sheet('dist', 'stock', d);
  // the published feed already carries such rows (micro-caps whose reverse split the record does not hold): none of them is ranked
  const flagged = new Set(all.filter((r) => r.jump).map((r) => r.ticker));
  for (const l of before.lists.filter((x) => /larger$/.test(x.hint))) for (const r of l.rows) assert.ok(!flagged.has(firstTicker(r)), `${l.key}: ${r.name}`);
  if (n0) assert.match(before.foot, says(n0)); else assert.doesNotMatch(before.foot, /beyond 3-to-1/);
  // flag the name that leads a ranking: it leaves the ranking, stays in the ring and in the list by size
  const worst = all.filter((r) => !r.jump && r.mcap >= 1e9 && r.below_high_pct < 0).sort((a, b) => a.below_high_pct - b.below_high_pct)[0];
  assert.equal(firstTicker(list(before, 'furthest').rows[0]), worst.ticker);
  worst.jump = { date: '2026-11-17', ratio: 43.3 };
  const after = sheet('dist', 'stock', d);
  assert.equal(after.ring.n, before.ring.n, 'still on the board');
  assert.ok(!list(after, 'furthest').rows.some((r) => firstTicker(r) === worst.ticker), 'out of the rankings');
  assert.ok(list(after, 'largest').rows.some((r) => firstTicker(r) === worst.ticker), 'still listed by size');
  assert.match(after.foot, says(n0 + 1));
  // the bottom board is where most of them sit
  const bottom = d.scan.boards.bottom.filter((r) => r.kind === 'stock' && r.jump);
  assert.ok(bottom.length >= 20, `${bottom.length} flagged rows on the bottom board`);
  assert.ok(bottom.every((r) => r.jump.ratio >= 3 || r.jump.ratio <= 1 / 3), 'the engine flags only moves beyond 3-to-1');
});

test('selling climax, buying climax, candle spread and spring list exactly the names their tables list', async () => {
  const d = docs();
  const sc = await deskRows(d, 'sc'), m = sheet('sc');
  assert.ok(sc.length > 20, `the SC table lists ${sc.length}`);
  assert.equal(m.ring.n, sc.length);
  assert.equal(m.ring.slices.reduce((a, s) => a + s.value, 0), sc.length);
  assert.deepEqual(list(m, 'volume').rows.map(firstTicker), sc, 'the table is the same rows, heaviest climax volume first');
  const spread = await deskRows(d, 'spread'), sp = sheet('spread');
  assert.equal(sp.ring.n, spread.length);
  assert.deepEqual(list(sp, 'tightest').rows.map(firstTicker).sort(), spread.slice().sort());
  const spring = await deskRows(d, 'spring'), sg = sheet('spring');
  assert.deepEqual(['fired', 'forming', 'failed'].flatMap((k) => list(sg, k).rows.map((r) => r.name)).sort(), spring.slice().sort());
  assert.equal(sg.ring.n, spring.length);
  const bc = [...new Set(await deskRows(d, 'bc'))], b = sheet('bc');
  assert.deepEqual(list(b, 'newest').rows.map((r) => r.name).sort(), bc.slice().sort(), 'one row a name, however many climaxes it printed');
  assert.equal(b.ring.n, bc.length);
});

test('capitulation: the ring is every print of the window by desk, as the engine counted them', () => {
  const c = FX['data/capitulation-desk.json'], m = sheet('capit');
  assert.equal(m.ring.n, c.n_prints_60);
  assert.equal(m.ring.slices.reduce((a, s) => a + s.value, 0), c.n_prints_60);
  assert.deepEqual(m.ring.slices.map((s) => s.value), Object.values(c.counts).map((x) => x.prints_60).filter((n) => n > 0).sort((a, b) => b - a));
  assert.equal(text(tile(m, 'Last 5 sessions').v), num(c.breadth.sum_5));
  assert.equal(list(m, 'latest').rows.length, c.latest.length);
  assert.equal(firstTicker(list(m, 'latest').rows[0]), c.latest.slice().sort((a, b) => b.score - a.score)[0].ticker);
  const heaviest = c.breadth.sessions.slice().sort((a, b) => b.total - a.total)[0];
  assert.equal(list(m, 'sessions').rows[0].name, heaviest.date);
  assert.equal(list(m, 'sessions').rows[0].big, num(heaviest.total));
  assert.deepEqual(kit.adapters.capitulation.build([structuredClone(c)], {}).ring, m.ring, 'the capitulation page and the Wyckoff tab show one sheet');
});

test('the desk pages mount the sheet without touching what they already paint', () => {
  const wy = read('wyckoff-desk.html'), cap = read('capitulation-desk.html');
  assert.match(wy, /<div class="tabs" id="tabs"><\/div>\n  <div id="jh-tearsheet" data-jh-chrome data-jts="wyckoff" data-jts-flush><\/div>/);
  const scripts = [...wy.matchAll(/<script src="([^"?]+)[^"]*"( defer)?><\/script>/g)].map((x) => x[1] + (x[2] || ''));
  assert.deepEqual(scripts, ['/capital-view.js', '/jh-wyckoff-scan.js', '/jh-tearsheet.js defer'], 'the scan script is in place before the desk paints; the kit waits for the document');
  const own = [...wy.matchAll(/<script>([\s\S]*?)<\/script>/g)];
  assert.equal(own.length, 1, 'the desk keeps one inline script: the one the permission test runs against its baseline');
  assert.equal((own[0][1].match(/\bhook\(\);/g) || []).length, 2, 'after a painted board and after an empty one');
  assert.match(own[0][1], /function hook\(\) \{ try \{ if \(window\.JHWyckoffHook\) window\.JHWyckoffHook\(tab, DATA\); \} catch \(e\) \{\} \}/);
  assert.equal((own[0][1].match(/fetchOne\(FEEDS\.\w+\)/g) || []).length, 4, 'the desk still reads its four feeds and no more');
  assert.match(cap, /<div id="jh-tearsheet" data-jh-chrome data-jts="capitulation" data-jts-flush><\/div>\n\n  <section class="cp-sec" aria-label="Verdict/);
  assert.match(cap, /window\.JHCapitDoc=DOC; try\{ window\.dispatchEvent\(new CustomEvent\("jh:capit-doc",\{detail:DOC\}\)\); \}catch\(e2\)\{\}/);
  assert.equal((cap.match(/src="\/jh-tearsheet\.js/g) || []).length, 1);
});

test('the kit follows the open tab and takes the desk\'s copy of the feeds', () => {
  const src = read('jh-tearsheet.js');
  assert.match(src, /el\.addEventListener\('jts:set', function \(e\) \{/);
  assert.match(src, /el\.hidden = !!\(model && model\.hide\);/);
  assert.match(src, /ad\.soft \? get\(u\)\.catch\(function \(\) \{ return null; \}\) : get\(u\)/);
  const scan = read('jh-wyckoff-scan.js');
  assert.equal([...scan].findIndex((ch) => ch.charCodeAt(0) > 127), -1, 'pure ASCII');
  assert.doesNotMatch(scan, /#[0-9a-fA-F]{3,8}\b|rgba?\(|hsla?\(/, 'colours come from the page\'s tokens, so the deploy reskin has nothing to rewrite');
  assert.match(scan, /new CustomEvent\('jh:wyckoff-docs', \{ detail: docs \}\)/);
  assert.match(scan, /new CustomEvent\('jts:set', \{ detail: \{ tab: tab \} \}\)/);
});

// ---- the all-stock board under the sheet (jh-wyckoff-scan.js), in a small DOM
function scanPage(tab, top) {
  const made = [], byId = {}, listeners = {}, win = {};
  const el = (tag) => {
    const n = { tag, id: '', children: [], parentNode: null, hidden: false, attrs: {}, _html: '', textContent: '',
      setAttribute(k, v) { this.attrs[k] = v; }, getAttribute(k) { return this.attrs[k]; },
      insertBefore(child, ref) { child.parentNode = this; const i = this.children.indexOf(ref); this.children.splice(i < 0 ? this.children.length : i, 0, child); if (child.id) byId[child.id] = child; return child; },
      appendChild(child) { return this.insertBefore(child, null); }, removeChild(child) { this.children = this.children.filter((c) => c !== child); if (child.id && byId[child.id] === child) delete byId[child.id]; },
      querySelector(sel) { return sel === 'tr[data-t]' ? (/<tr data-t=/.test(this._html) ? {} : null) : sel === '[data-sc-kpi]' ? (/data-sc-kpi/.test(this._html) ? {} : null) : null; },
      insertAdjacentHTML(_, h) { this._html += h; }, dispatchEvent(e) { (this.events = this.events || []).push(e); return true; },
      set innerHTML(v) { this._html = v; }, get innerHTML() { return this._html; } };
    made.push(n);
    return n;
  };
  const wrap = el('div'), own = el('div'), board = el('table'), memo = el('section'), kpi = el('div'), sheet_ = el('div');
  wrap.children = [own, memo]; own.parentNode = wrap; memo.parentNode = wrap;
  Object.assign(byId, { board, memo, kpi, 'jh-tearsheet': sheet_ });
  board._html = tab === 'dist' ? '<tr><td class=empty colspan=9>Distribution does not keep a separate board.</td></tr>' : "<tr data-t='AAA'><td>AAA</td></tr>";
  const document = { head: el('head'), createElement: el, getElementById: (id) => byId[id] || null, querySelector: (sel) => (sel === '.panel' ? own : null),
    addEventListener: (k, f) => { listeners[k] = f; } };
  const feeds = { '/data/campaign-scan.json': FX['data/campaign-scan.json'], '/data/capitulation-desk.json': FX['data/capitulation-desk.json'] };
  const requests = [], events = [];
  Object.assign(win, { top: top || win, dispatchEvent: (e) => { events.push(e); return true; }, location: { href: '' } });
  if (!top) win.top = win;
  const ctx = { window: win, document, location: win.location, encodeURIComponent, Date, Math, Promise, CustomEvent: function (type, init) { this.type = type; this.detail = init && init.detail; },
    fetch: async (u) => { requests.push(u.split('?')[0]); return { ok: true, json: async () => structuredClone(feeds[u.split('?')[0]]) }; } };
  vm.createContext(ctx);
  vm.runInContext(read('jh-wyckoff-scan.js'), ctx);
  return { win, byId, wrap, own, listeners, requests, events, sheet: sheet_, kpi,
    settle: () => new Promise((r) => setImmediate(() => setImmediate(r))),
    panel: () => byId['scan-panel'], rowsOf: () => [...(byId['scan-panel'] ? byId['scan-panel'].innerHTML : '').matchAll(/<tr data-t="([^"]+)">/g)].map((m) => m[1]),
    click: (attrs) => listeners.click({ target: { closest: (sel) => { const hit = Object.entries(attrs).find(([k]) => sel === k); return sel === '#scan-panel' ? {} : hit ? { getAttribute: () => hit[1] } : null; } } }) };
}

test('the all-stock board: every name the icon marks, above the desk\'s own table, sortable both ways', async () => {
  const scan = FX['data/campaign-scan.json'];
  for (const [tab, board] of [['accum', 'accumulation'], ['bottom', 'bottom'], ['dist', 'distribution']]) {
    const p = scanPage(tab);
    await p.settle();
    assert.deepEqual(p.requests.sort(), ['/data/campaign-scan.json', '/data/capitulation-desk.json'], 'the two feeds the desk does not read, once each');
    p.win.JHWyckoffHook(tab, { bottom: {}, phase: {}, katlin: {}, radar: {} });
    const stocks = scan.boards[board].filter((r) => r.kind === 'stock');
    assert.deepEqual(p.rowsOf(), stocks.slice(0, 300).map((r) => r.ticker), `${tab}: the engine's order, stocks first`);
    assert.equal(p.wrap.children[0].id, 'scan-panel', 'drawn above the desk\'s own table');
    assert.match(p.panel().innerHTML, new RegExp(`STOCKS ${num(stocks.length)}<`));
    assert.equal(p.own.hidden, tab === 'dist', 'only the tab whose own table is the "no separate board" line steps aside');
    assert.match(p.kpi.innerHTML, /Chart scan \u00b7 all stocks/);
    // the sheet was told which tab is open and handed every feed
    assert.equal(p.sheet.events.at(-1).type, 'jts:set');
    assert.deepEqual({ ...p.sheet.events.at(-1).detail }, { tab });
    assert.equal(p.win.JHWyckoffDocs.scan.contract, 'campaign-scan.v1');
    assert.ok(p.events.some((e) => e.type === 'jh:wyckoff-docs' && e.detail.capit && e.detail.scan));
    // sort: a number opens largest first, the second click reverses it
    const cols = p.win.JHWyckoffScan.columns[tab](), i = cols.findIndex((c) => c.k === 'Since');
    p.click({ '[data-sc-col]': String(i) });
    const desc = stocks.filter((r) => Number.isFinite(r.since_pct)).sort((a, b) => b.since_pct - a.since_pct).map((r) => r.since_pct);
    const shown = (n) => p.rowsOf().slice(0, n).map((t) => stocks.find((r) => r.ticker === t).since_pct);
    assert.deepEqual(shown(5), desc.slice(0, 5));
    p.click({ '[data-sc-col]': String(i) });
    assert.deepEqual(shown(5), desc.slice(-5).reverse());
    // ETFs, then everything
    p.click({ '[data-sc-kind]': 'etf' });
    assert.ok(p.rowsOf().every((t) => scan.boards[board].find((r) => r.ticker === t).kind === 'etf'));
    p.click({ '[data-sc-kind]': 'all' });
    p.click({ '[data-sc-all]': '1' });
    assert.equal(p.rowsOf().length, scan.boards[board].length, 'SHOW ALL draws every row');
    // another tab: the board goes, the desk's table comes back
    p.win.JHWyckoffHook('sc', { bottom: {}, phase: {}, katlin: {}, radar: {} });
    assert.equal(p.panel(), undefined);
    assert.equal(p.own.hidden, false);
  }
});

test('a row of the all-stock board opens the chart with that icon\'s marks on', async () => {
  // alone in a tab: a link the chart reads
  const solo = scanPage('dist');
  await solo.settle();
  solo.win.JHWyckoffHook('dist', {});
  solo.click({ 'tr[data-t]': 'BRK.B' });
  assert.equal(solo.win.location.href, '/chart.html?s=BRK.B&marks=dist');
  // inside the chart's workspace: the symbol loads behind the overlay, the marks are switched on, the overlay closes
  const calls = [];
  const top = { jhOpenSymbol: (t) => calls.push(['open', t]), jhEnsureMarks: (m) => calls.push(['marks', m]), document: { getElementById: (id) => (id === 'wsx' ? { click: () => calls.push(['close']) } : null) }, location: { href: '' } };
  const inside = scanPage('accum', top);
  await inside.settle();
  inside.win.JHWyckoffHook('accum', {});
  inside.click({ 'tr[data-t]': 'WMT' });
  assert.deepEqual(calls, [['marks', 'accum'], ['open', 'WMT'], ['close']]);
  assert.equal(top.location.href, '', 'no navigation when the chart is right there');
});

// ---- the chart's three marking icons
function chartMarks(search) {
  const opened = [], classes = {};
  const ctx = { console, setTimeout, fetch: undefined, URLSearchParams, location: { search: search || '' },
    document: { readyState: 'complete', getElementById: (id) => ({ classList: { add: () => { classes[id] = true; }, remove: () => { classes[id] = false; } } }), addEventListener() {} } };
  ctx.window = ctx;
  for (const f of ['jh-chart-campaign-marks.js', 'jh-chart-distribution.js']) vm.runInNewContext(read(f).replace(/\(function \(\) \{\s*if \(typeof document === "undefined"\) return;[\s\S]*$/, ''), ctx);
  ctx.jhOpenWorkspace = (k) => opened.push(k);
  const rail = read('jh-chart-tvrail.js'), a = rail.indexOf('  var jhMarkDesk = '), b = rail.indexOf('  window.jhTvChips = function');
  assert.ok(a > 0 && b > a, 'the mark-desk block is in the rail');
  vm.runInNewContext(rail.slice(a, b), ctx);
  return { ctx, opened, classes };
}

test('Acc, Bot and Dist: the click that turns the marks on opens the desk; the next click turns them off as before', () => {
  const { ctx, opened, classes } = chartMarks('');
  assert.deepEqual({ ...ctx.__jhCampLayer }, { bottom: 0, accum: 0 });
  ctx.jhCampToggle('accum');
  assert.deepEqual([ctx.__jhCampLayer.accum, classes['btn-acc'], opened], [1, true, ['accum']]);
  ctx.jhCampToggle('accum');
  assert.deepEqual([ctx.__jhCampLayer.accum, classes['btn-acc'], opened], [0, false, ['accum']], 'off: no desk');
  ctx.jhCampToggle('bottom');
  ctx.jhDistToggle();
  assert.deepEqual([ctx.__jhCampLayer.bottom, ctx.__jhDistOn, opened], [1, 1, ['accum', 'bottom', 'dist']]);
  ctx.jhDistToggle();
  assert.deepEqual([ctx.__jhDistOn, classes['btn-dist'], opened.length], [0, false, 3]);
  // a desk row asks for the marks without the desk, and never switches them off
  ctx.jhEnsureMarks('dist');
  ctx.jhEnsureMarks('dist');
  ctx.jhEnsureMarks('bottom');
  ctx.jhEnsureMarks('nonsense');
  assert.deepEqual([ctx.__jhDistOn, ctx.__jhCampLayer.bottom, opened.length], [1, 1, 3]);
});

test('a link with ?marks= opens the chart with those marks on and no desk; the rail knows the Distribution desk', () => {
  const { ctx, opened, classes } = chartMarks('?s=NVDA&marks=bottom,dist');
  assert.deepEqual([ctx.__jhCampLayer.bottom, ctx.__jhCampLayer.accum, ctx.__jhDistOn, opened], [1, 0, 1, []]);
  assert.deepEqual([classes['btn-bot'], classes['btn-dist']], [true, true]);
  const rail = read('jh-chart-tvrail.js');
  assert.match(rail, /dist: \["Distribution \u00b7 a rally that fails to hold", "\/wyckoff-desk\.html\?tab=dist"\],/);
  assert.match(rail, /accum: \["Accumulation \u00b7 tight band, long range", "\/wyckoff-desk\.html\?tab=accum"\],/);
  assert.match(rail, /bottom: \["Bottom \u00b7 SC \/ ST \/ hinge \u2014 predict the low", "\/wyckoff-desk\.html\?tab=bottom"\],/);
  // the engine's handlers are untouched: they still call the marking functions, which the rail wraps
  const eng = read('jh-chart-engine.js');
  assert.match(eng, /bacc\.onclick=function\(\)\{ if\(window\.jhCampToggle\) window\.jhCampToggle\("accum"\); \};/);
  assert.match(eng, /bdist\.onclick=function\(\)\{ if\(window\.jhDistToggle\) window\.jhDistToggle\(\); \};/);
});
