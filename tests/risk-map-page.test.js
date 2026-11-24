'use strict';
// The Risk Map page (regime-map.html + jh-risk-map.js) on the document the runner built with engine 2.0.0 (ops 6936, 2026-11-24).
// Khalid 2026-11-24: "make sure whats booming is actually booming and whats destroyed is actually destroyed and make it in a big
// heatmap visual like tradingview heatmap". The page measures nothing; these tests hold what it draws to the document, and hold its
// own check (audit) to catching a document that contradicts itself.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const R = path.resolve(__dirname, '..');
const M = require(path.join(R, 'jh-risk-map.js'));
const DOC = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(R, 'tests/fixtures/risk-map-document.json.gz'))));
const HTML = fs.readFileSync(path.join(R, 'regime-map.html'), 'utf8');
const JS = fs.readFileSync(path.join(R, 'jh-risk-map.js'), 'utf8');
const TILES = DOC.map.tiles;
const BY = Object.fromEntries(TILES.map(t => [t.t, t]));
const clone = o => JSON.parse(JSON.stringify(o));
const rgb = h => [1, 3, 5].map(i => parseInt(h.slice(i, i + 2), 16));

test('the fixture is the whole map: nine asset classes, every kind of instrument', () => {
  assert.equal(DOC.contract, 'risk-map.v2');
  assert.equal(TILES.length, DOC.map.n);
  assert.ok(TILES.length >= 280, String(TILES.length));
  assert.deepEqual(DOC.map.classes.map(c => c.id), ['us', 'world', 'bonds', 'fx', 'cmdty', 'crypto', 'realestate', 'vol', 'alts']);
  assert.deepEqual([...new Set(TILES.map(t => t.k))].sort(), ['coin', 'fund', 'fut', 'fx', 'idx']);
});

test('audit: every change is the close over its anchor, every booming and destroyed tile is held to the rule', () => {
  const a = M.audit(DOC);
  assert.deepEqual(a.bad, []);
  assert.equal(a.tiles, TILES.length);
  assert.equal(a.changes, TILES.reduce((s, t) => s + Object.keys(t.a).length, 0));
  assert.ok(a.changes > 1900, String(a.changes));
});

test('audit catches a document that contradicts itself', () => {
  const falling = TILES.find(t => t.state === 'RISK-OFF' && !t.held);
  const cases = [
    [d => { d.map.tiles[0].r.q += 1.5; }, /the closes give/],
    [d => { d.map.tiles.find(t => t.t === falling.t).state = 'BOOMING'; }, /called booming against its own record/],
    [d => { d.map.tiles.find(t => t.state === 'RISK-ON').state = 'DESTROYED'; }, /called destroyed against its own record/],
    [d => { d.map.booming.push(falling.t); }, /booming list is not exactly/],
    [d => { d.map.destroyed.pop(); }, /destroyed list is not exactly/],
    [d => { d.map.tiles.push(clone(d.map.tiles[3])); }, /appears twice/],
    [d => { d.booming = [clone(d.ranked[d.ranked.length - 1])]; }, /first map's booming list in the state/],
    [d => { const t = d.map.tiles.find(x => x.state === 'NEUTRAL'); t.state = 'RISK-ON'; }, /called rising on a score/],
    [d => { d.map.tiles[5].a.m[0] = '2099-01-01'; }, /a day after its last bar/]
  ];
  for (const [spoil, want] of cases) {
    const d = clone(DOC);
    spoil(d);
    const bad = M.audit(d).bad;
    assert.ok(bad.some(b => want.test(b)), `${want}: ${JSON.stringify(bad.slice(0, 3))}`);
  }
  assert.ok(M.audit({}).bad.length && M.audit({ map: { tiles: [] } }).bad.length);
});

test('the rule the page holds a tile to is the engine\'s', () => {
  // score, month, three months, vs 50-day, vs 200-day
  const t = (score, m, q, v50, v200) => ({ score, r: { m, q }, v50, v200 });
  assert.equal(M.isBooming(t(50, 12.8, 9.4, 7, 9)), true);
  assert.equal(M.isBooming(t(46, 2.1, 4.8, 4, 14)), false, 'neither the month nor three months is up 5%');
  assert.equal(M.isBooming(t(80, -3, 35, 5, 20)), false, 'the month is down');
  assert.equal(M.isBooming(t(50, 6, 9, -1, 4)), false, 'under its 50-day');
  assert.equal(M.isBooming(t(44, 12, 12, 5, 5)), false, 'a score under 45');
  assert.equal(M.isDestroyed(t(-56, -24.4, -21.1, -19.5, -15.1)), true);
  assert.equal(M.isDestroyed(t(-60, 1, -30, -1, -1)), false, 'the month is up');
  assert.equal(M.isDestroyed(t(-46, -3, -4.9, -1, -1)), false);
  for (const x of TILES) {
    if (x.state === 'BOOMING') assert.ok(M.isBooming(x), x.t);
    if (x.state === 'DESTROYED') assert.ok(M.isDestroyed(x), x.t);
  }
});

test('the lists are exactly the tiles in that state, strongest and weakest first, and the held-back say why', () => {
  const L = M.lists(DOC);
  assert.deepEqual(L.booming.map(t => t.t).sort(), TILES.filter(t => t.state === 'BOOMING').map(t => t.t).sort());
  assert.deepEqual(L.destroyed.map(t => t.t).sort(), TILES.filter(t => t.state === 'DESTROYED').map(t => t.t).sort());
  assert.deepEqual(L.booming.map(t => t.t).sort(), DOC.map.booming.slice().sort());
  assert.deepEqual(L.destroyed.map(t => t.t).sort(), DOC.map.destroyed.slice().sort());
  for (let i = 1; i < L.booming.length; i++) assert.ok(L.booming[i - 1].score >= L.booming[i].score);
  for (let i = 1; i < L.destroyed.length; i++) assert.ok(L.destroyed[i - 1].score <= L.destroyed[i].score);
  assert.ok(L.held.length >= 1);
  for (const t of L.held) {
    assert.ok(t.state === 'RISK-ON' || t.state === 'RISK-OFF', t.t + ' ' + t.state);
    assert.ok(Math.abs(t.score) >= 45 && /scores as (booming|destroyed)/.test(t.held), t.t);
    assert.match(M.why(t), /^Not called (booming|destroyed): it scores as/);
  }
  assert.match(M.why(L.booming[0]), /^Booming: score \+\d+, one month \+[\d.]+%, three months \+[\d.]+%, [\d.]+% over its 50-day average, [\d.]+% over its 200-day\.$/);
  assert.match(M.why(L.destroyed[0]), /^Destroyed: score −\d+, one month −[\d.]+%, three months −[\d.]+%, [\d.]+% under its 50-day average, [\d.]+% under its 200-day\.$/);
});

test('layout: every tile once, inside the map, none over another, in proportion to its weight', () => {
  for (const [w, h, sizing, cls] of [[1576, 542, 'size', null], [378, 2347, 'size', null], [1000, 700, 'equal', null], [1576, 542, 'size', 'crypto'], [640, 900, 'equal', 'fx']]) {
    const lay = M.layout(DOC, { w, h, sizing, cls });
    const want = TILES.filter(t => !cls || t.cls === cls);
    const rects = lay.classes.flatMap(c => c.groups.flatMap(g => g.tiles.map(r => ({ ...r, cls: c.id, grp: g.id }))));
    assert.equal(rects.length, want.length, `${w}x${h} ${sizing} ${cls}`);
    assert.equal(lay.n, want.length);
    assert.deepEqual(rects.map(r => r.t).sort(), want.map(t => t.t).sort());
    for (const r of rects) {
      assert.ok(r.x >= -0.01 && r.y >= -0.01 && r.x + r.w <= w + 0.01 && r.y + r.h <= h + 0.01 && r.w > 0 && r.h > 0, `${r.t} is outside the map`);
      assert.equal(BY[r.t].cls, r.cls);
      assert.equal(BY[r.t].grp, r.grp);
    }
    for (let i = 0; i < rects.length; i++) {
      for (let j = i + 1; j < rects.length; j++) {
        const a = rects[i], b = rects[j];
        const over = Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x), up = Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y);
        assert.ok(!(over > 0.01 && up > 0.01), `${a.t} lies over ${b.t}`);
      }
    }
    for (const c of lay.classes) {
      assert.ok(c.x >= -0.01 && c.x + c.w <= w + 0.01 && c.y + c.h <= h + 0.01);
      for (const g of c.groups) {
        const per = g.tiles.map(r => r.w * r.h / r.v);
        if (per.length > 1) assert.ok(Math.max(...per) / Math.min(...per) < 1.0001, `${g.id}: areas are not in proportion to weights`);
        for (const r of g.tiles) assert.ok(r.x >= g.x - 0.01 && r.y >= g.y + g.head - 0.01 && r.x + r.w <= g.x + g.w + 0.01 && r.y + r.h <= g.y + g.h + 0.01, `${r.t} leaves its group`);
      }
    }
    if (!cls) assert.equal(lay.classes.length, 9);
  }
});

test('squarify fills the rectangle and keeps the shapes near square', () => {
  const out = M.squarify([6, 6, 4, 3, 2, 2, 1].map(v => ({ v })), 0, 0, 600, 400);
  assert.equal(out.length, 7);
  const area = out.reduce((s, r) => s + r.w * r.h, 0);
  assert.ok(Math.abs(area - 240000) < 1e-6);
  for (const r of out) assert.ok(Math.abs(r.w * r.h - r.node.v * 10000) < 1e-6);
  assert.ok(Math.max(...out.map(r => Math.max(r.w / r.h, r.h / r.w))) < 3);
  assert.deepEqual(M.squarify([], 0, 0, 10, 10), []);
  assert.deepEqual(M.squarify([{ v: 1 }], 0, 0, 0, 10), []);
});

test('tile size: bounded, one size where there is no figure, equal on request', () => {
  const w = M.weights(TILES, 'size');
  for (const t of TILES) {
    assert.ok(w[t.t] >= 0.45 && w[t.t] <= 3.2, t.t);
    if (t.k === 'fx' || t.k === 'fut' || t.k === 'idx') assert.equal(w[t.t], 1, t.t);
  }
  const funds = TILES.filter(t => t.grp === 'us_sector').sort((a, b) => b.dv - a.dv);
  assert.ok(w[funds[0].t] > w[funds[funds.length - 1].t]);
  assert.ok(w.BTC > w.SOL, 'coins are sized by market value');
  assert.ok(Object.values(M.weights(TILES, 'equal')).every(v => v === 1));
});

test('colour: the five states sit in five separate bands, and cannot be mistaken for one another', () => {
  const band = { BOOMING: [0.8, 1], 'RISK-ON': [0.3, 0.52], NEUTRAL: [0, 0], 'RISK-OFF': [-0.52, -0.3], DESTROYED: [-1, -0.8] };
  for (const t of TILES) {
    const v = M.tone(t, 'state');
    assert.ok(v >= band[t.state][0] - 1e-9 && v <= band[t.state][1] + 1e-9, `${t.t} ${t.state} ${v}`);
  }
  const held = TILES.find(t => t.held && t.score >= 45);
  assert.ok(M.tone(held, 'state') <= 0.52, 'a tile held back from booming is not drawn as booming');
  for (const pal of ['rg', 'cb']) {
    const cols = ['DESTROYED', 'RISK-OFF', 'NEUTRAL', 'RISK-ON', 'BOOMING'].map(s => M.fill(M.tone(TILES.find(t => t.state === s), 'state'), 'state', pal));
    assert.equal(new Set(cols).size, 5);
    assert.equal(M.fill(0, 'state', pal), M.PALETTES[pal].mid);
  }
});

test('colour: a diverging scale with a grey middle, brighter the further from nothing, in both palettes', () => {
  for (const pal of Object.keys(M.PALETTES)) {
    const p = M.PALETTES[pal];
    const [r, g, b] = rgb(p.mid);
    assert.ok(Math.max(r, g, b) - Math.min(r, g, b) < 20, 'the middle is a grey');
    for (const side of [-1, 1]) {
      const lum = [0, 0.1, 0.25, 0.5, 0.75, 1].map(a => M.luminance(M.fill(side * a, 'm', pal)));
      for (let i = 1; i < lum.length; i++) assert.ok(lum[i] > lum[i - 1], `${pal} ${side}: step ${i} is not brighter`);
    }
    assert.notEqual(M.fill(1, 'm', pal), M.fill(-1, 'm', pal));
    assert.equal(M.fill(1, 'm', pal), p.pos);
    assert.equal(M.fill(-1, 'm', pal), p.neg);
    assert.equal(M.fill(null, 'm', pal), p.none);
  }
  // red against green is the usual pair; the second palette keeps its two ends apart for red-green colour blindness (blue channel)
  const cb = M.PALETTES.cb;
  assert.ok(rgb(cb.pos)[2] - rgb(cb.neg)[2] > 120);
});

test('colour by a window: the sign and size of the change, the full colour at the window\'s own scale', () => {
  const spy = BY.SPY;
  for (const w of M.WINDOWS) {
    const v = spy.r[w.id], tn = M.tone(spy, w.id, false, null);
    assert.equal(Math.sign(tn), Math.sign(v));
    assert.ok(Math.abs(tn - Math.max(-1, Math.min(1, v / w.scale))) < 1e-12);
    assert.equal(M.valueOf(spy, w.id, null), v);
    assert.equal(M.labelOf(spy, w.id, null), M.fmtPct(v));
  }
  assert.deepEqual(M.WINDOWS.map(w => w.label), ['1D', '1W', '1M', '3M', '6M', 'YTD', '1Y']);
  assert.equal(M.tone({ r: { m: 99 }, u: {} }, 'm'), 1);
  assert.equal(M.tone({ r: { m: -99 }, u: {} }, 'm'), -1);
  assert.equal(M.tone({ r: {}, u: {} }, 'y'), null, 'no figure for the window');
  assert.equal(M.labelOf({ r: {} }, 'y'), '–');
  // against its own normal: a currency's 1% is more than a coin's 3%
  const fx = { r: { m: 1 }, u: { m: 2.0 } }, coin = { r: { m: 3 }, u: { m: 0.4 } };
  assert.ok(M.tone(fx, 'm', true) > M.tone(coin, 'm', true));
  assert.ok(M.tone(fx, 'm', false) < M.tone(coin, 'm', false));
  assert.equal(M.tone({ r: { m: 1 }, u: { m: 9 } }, 'm', true), 1);
  assert.equal(M.tone({ r: { m: 1 }, u: {} }, 'm', true), null);
});

test('labels never rest on colour alone: a sign on every number, an arrow on booming and destroyed', () => {
  assert.match(M.labelOf(TILES.find(t => t.state === 'BOOMING'), 'state'), /^▲ \+\d+$/);
  assert.match(M.labelOf(TILES.find(t => t.state === 'DESTROYED'), 'state'), /^▼ −\d+$/);
  assert.match(M.labelOf(TILES.find(t => t.state === 'RISK-ON'), 'state'), /^\+\d+$/);
  assert.equal(M.fmtPct(1.234), '+1.23%');
  assert.equal(M.fmtPct(-12.34), '−12.3%');
  assert.equal(M.fmtPct(-123.4), '−123%');
  assert.equal(M.fmtPct(0), '0.00%');
  assert.equal(M.fmtPct(-0.001), '0.00%');
  assert.equal(M.fmtPct(null), '–');
  assert.equal(M.fmtPct(-3, 0), '−3%');
  assert.equal(M.fmtScore(-21), '−21');
  assert.equal(M.fmtScore(0), '0');
  assert.equal(M.fmtScore(-0.4), '0');
  assert.equal(M.fmtScore(36.5), '+37');
  assert.equal(M.fmtScore(-57.5), '\u221257');
  assert.equal(M.fmtPrice(61312), '61,312.00');
  assert.equal(M.fmtPrice(714.94), '714.94');
  assert.equal(M.fmtPrice(0.0062899016), '0.006290');
  assert.equal(M.fmtPrice(0.5), '0.5000');
  assert.equal(M.fmtBig(45140318560), '$45.1B');
  assert.equal(M.fmtBig(1230630497881), '$1.23T');
  assert.equal(M.fmtTimes(-1.08), '−1.1×');
  assert.deepEqual(Object.values(M.PLAIN), ['Booming', 'Rising', 'Flat', 'Falling', 'Destroyed']);
});

test('tile text fits its tile or is left off', () => {
  assert.deepEqual(M.tileText(20, 40, 'SPY', '+1.00%'), { fs: 0, vs: 0, two: false, three: false });
  assert.equal(M.tileText(60, 14, 'SPY', '+1.00%').fs, 0);
  for (const [w, h, name, label] of [[60, 60, 'SPY', '▼ −60'], [44, 30, 'RENDER', '−13.7%'], [130, 110, 'KWEB', '−4.46%'], [300, 200, 'BTC', '+0.24%'], [34, 22, 'USFR', '+0.68%']]) {
    const t = M.tileText(w, h, name, label);
    assert.ok(t.fs >= 8.5 && t.fs <= 30);
    if (t.fs > 8.5) assert.ok(t.fs * 0.76 * Math.max(3, name.length) <= w - 8 + 0.1, `${name} is wider than its tile`);
    if (t.two) assert.ok(t.fs * 1.14 + t.vs * 1.2 <= h && t.vs <= t.fs, `${name}: two lines are taller than the tile`);
    if (t.three) assert.ok(t.two);
  }
  assert.ok(M.mapHeight(1576, 542, 300) >= 542);
  assert.ok(M.mapHeight(378, 500, 300) >= 300 * 2500 / 378, 'on a phone the map grows until the average tile can hold its name');
});

test('live prices: the session price over the same anchors, never from an older, split or mismatched quote', () => {
  const spy = BY.SPY, at = Date.parse(spy.d + 'T17:00:00Z');                   // noon in New York on the tile's day
  const q = (dayClose, prevClose, ms) => ({ price: dayClose * 1.01, dayClose, prevClose, updatedTs: ms });
  const same = M.live(spy, q(720, spy.p, at));
  assert.ok(same);
  assert.equal(same.c, 720);
  assert.equal(same.day, spy.d);
  assert.equal(same.r.d, Math.round((720 / spy.p - 1) * 10000) / 100);
  for (const w of ['w', 'm', 'q', 'h', 'ytd', 'y']) assert.equal(same.r[w], Math.round((720 / spy.a[w][1] - 1) * 10000) / 100, w);
  // the next session: the quote's previous close is the day the engine holds (its bar was still forming, so a little apart)
  const next = M.live(spy, q(730, spy.c * 1.004, at + 86400000));
  assert.ok(next && next.r.d === Math.round((730 / (spy.c * 1.004) - 1) * 10000) / 100 && next.r.m === Math.round((730 / spy.a.m[1] - 1) * 10000) / 100);
  assert.equal(M.live(spy, q(720, spy.p, at - 3 * 86400000)), null, 'a quote from before the engine\'s bar');
  assert.equal(M.live(spy, q(spy.c / 4, spy.p / 4, at)), null, 'a split away');
  assert.equal(M.live(spy, q(spy.c * 2, spy.p, at)), null, 'a split away');
  assert.equal(M.live(spy, q(720, spy.p * 1.02, at)), null, 'its previous close is not the bar the engine holds');
  assert.equal(M.live(Object.assign({}, spy, { f: undefined }), q(730, spy.c * 1.02, at + 86400000)), null, 'a final bar must match the quote\'s previous close');
  assert.equal(M.live(spy, q(0, spy.p, at)), null, 'no session price yet');
  assert.equal(M.live(spy, q(720, 0, at)), null);
  assert.equal(M.live(spy, { dayClose: 720, prevClose: spy.p }), null, 'no time on the quote');
  assert.equal(M.live(spy, null), null);
  assert.equal(M.live(BY.BTC, q(BY.BTC.c, BY.BTC.p, at)), null, 'funds only');
  assert.equal(M.nyDate(Date.parse('2026-11-25T03:30:00Z')), '2026-11-24', 'the New York day, not the UTC one');
  assert.equal(M.nyDate(Date.parse('2026-07-01T03:30:00Z')), '2026-06-30');
});

test('the card shows the proof: each change with the day and close it is measured from', () => {
  const html = M.cardHtml(DOC, BY.SPY, null, { pinned: true });
  for (const w of M.WINDOWS) {
    assert.ok(html.includes('<td>' + BY.SPY.a[w.id][0] + '</td>'), w.id + ' date');
    assert.ok(html.includes('<td>' + M.fmtPrice(BY.SPY.a[w.id][1]) + '</td>'), w.id + ' close');
    assert.ok(html.includes(M.fmtPct(BY.SPY.r[w.id])), w.id + ' change');
  }
  assert.ok(html.includes(M.PLAIN[BY.SPY.state]) && html.includes('score ' + M.fmtScore(BY.SPY.score)));
  assert.ok(html.includes("data-chart='SPY'") && html.includes('Open SPY in the chart'));
  assert.ok(html.includes('still forming at'));
  assert.ok(M.cardHtml(DOC, BY.BTC, null, { pinned: true }).includes("data-chart='BTC-USD'"));
  const jpy = M.cardHtml(DOC, BY.JPY, null, { pinned: true });
  assert.ok(jpy.includes('the market quotes USDJPY') && jpy.includes('Green means the currency rose against the dollar') && jpy.includes('A safe haven') && jpy.includes('No chart for this currency yet'));
  assert.ok(M.cardHtml(DOC, BY.VIX, null, {}).includes('A fear gauge: it rises when investors are afraid'));
  const split = TILES.find(t => t.splits);
  assert.match(M.cardHtml(DOC, split, null, {}), /rebased for a share split \((\d+-for-1|1-for-\d+)\)/);
  const late = TILES.find(t => t.late);
  assert.ok(M.cardHtml(DOC, late, null, {}).includes('behind the newest of its kind'));
  const held = TILES.find(t => t.held);
  assert.ok(M.cardHtml(DOC, held, null, {}).includes('Not called '));
  // live: the engine's change stays, the live one stands beside it
  const lv = M.live(BY.SPY, { dayClose: 720, prevClose: BY.SPY.p, updatedTs: Date.parse(BY.SPY.d + 'T17:00:00Z') });
  const live = M.cardHtml(DOC, BY.SPY, lv, {});
  assert.ok(live.includes('>Now</th>') && live.includes(M.fmtPct(BY.SPY.r.m)) && live.includes(M.fmtPct(lv.r.m)) && live.includes('15 minutes delayed') && live.includes("The engine's close"));
  // nothing from the document reaches the page as markup
  const evil = clone(BY.SPY);
  evil.n = '<img src=x onerror=alert(1)>';
  evil.held = '<script>x</script>';
  const out = M.cardHtml(DOC, evil, null, {});
  assert.ok(!out.includes('<img') && !out.includes('<script>') && out.includes('&lt;img'));
});

test('the page: a heat map by default, the previous view one click away, both from the same feed', () => {
  assert.ok(HTML.includes('<script src="/jh-risk-map.js?v='));
  assert.ok(HTML.includes("gj('data/regime-map.json')"));
  assert.ok(HTML.includes('id="rm-v-classic"') && HTML.includes('>Previous view</button>') && HTML.includes('id="rm-v-map"'));
  assert.ok(HTML.includes("qs.get('view')==='classic'"));
  // the first Risk Map's page is still drawn from the first map's fields
  for (const s of ['🔥 Booming · risk-on', '💀 Getting destroyed · risk-off', 'Full map — every sleeve', 'd.sleeves', 'd.booming', 'd.destroyed', 'd.transitions', 'r.equity_breadth_pct', 'r.concentration_spread_3m']) {
    assert.ok(HTML.includes(s), s);
  }
  for (const s of ['/jh-page-ai.js', '/jh-nav-drawer.js', '/sidebar.js']) assert.ok(HTML.includes('<script src="' + s + '" defer></script>'), s);
  assert.ok(HTML.includes('JHRiskMap.mount(app,doc,'));
  // a document without the map (an older publication) falls back to the previous view instead of an empty page
  assert.ok(HTML.includes("if(doc&&doc.ok) view='classic'"));
});

test('the page script: controls answer only their own clicks, a tile opens the chart it names', () => {
  // 2026-11-24: a handler on "[data-pal]" matched the page's own container and redrew the controls on every click.
  for (const sel of ['".rm-seg [data-mode]"', '".rm-seg [data-size]"', '".rm-seg [data-pal]"', '".rm-more[data-more]"']) assert.ok(JS.includes('q(' + sel + ')'), sel);
  assert.ok(!/q\("\[data-(mode|size|pal|more)\]"\)/.test(JS));
  assert.ok(!/host\.dataset\.(pal|mode|size|cls|t|more|sort)\b/.test(JS), 'the container carries no attribute a control is found by');
  assert.ok(JS.includes('root.parent.jhOpenSymbol(sym)') && JS.includes('"/chart.html?s=" + encodeURIComponent(sym)'));
  assert.ok(JS.includes('/quotes?tickers='));
  assert.ok(!/api\.polygon\.io|financialmodelingprep|apiKey|api_key/i.test(JS + HTML), 'no provider and no key in the page');
  // states are never recomputed from live prices
  assert.ok(JS.includes('S.live && S.mode !== "state" && !S.norm'));
});

test('chart.html: a Risk Map button next to Heat opens the map in the chart\'s own workspace', () => {
  const ledger = require('./helpers/chart-macro-line-preservation.cjs');
  const crypto = require('node:crypto');
  const hash = s => crypto.createHash('sha256').update(s).digest('hex');
  const engine = fs.readFileSync(path.join(R, 'jh-chart-engine.js'), 'utf8'), rail = fs.readFileSync(path.join(R, 'jh-chart-tvrail.js'), 'utf8'), chart = fs.readFileSync(path.join(R, 'chart.html'), 'utf8');
  const heat = engine.indexOf('id=btn-heat '), mine = engine.indexOf('id=btn-rmap '), auc = engine.indexOf('id=btn-auc ');
  assert.ok(heat > 0 && mine > heat && auc > mine, 'the button stands between Heat and Auction; nothing that was there moved');
  assert.equal(engine.split('id=btn-rmap ').length, 2);
  assert.ok(engine.includes("<span class=l>Risk Map</span>"));
  assert.ok(engine.includes('var brm=document.getElementById("btn-rmap"); if(brm) brm.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("riskmap"); };'));
  const m = rail.match(/\n {6}riskmap: \["([^"]+)", "([^"]+)"\],\n/);
  assert.ok(m, 'the rail knows the workspace');
  assert.equal(m[2], '/regime-map.html?v=20261124a');
  assert.ok(fs.existsSync(path.join(R, m[2].split('?')[0].slice(1))));
  assert.ok(rail.indexOf('      heat: [') < rail.indexOf('      riskmap: [') && rail.indexOf('      riskmap: [') < rail.indexOf('      auction: ['));
  // a browser holding the old engine or the old rail fetches the new one
  const tags = chart.split('\n').filter(l => /<script src="\/jh-chart-(engine|tvrail)\.js\?/.test(l));
  assert.equal(tags.length, 2);
  for (const t of tags) assert.ok(t.includes('&repo=20261121&rmap=20261124&sov=20261122'), t.slice(0, 60));
  // the page a workspace opens is the page as it is served: same cache tag on its script
  assert.ok(/<script src="\/jh-risk-map\.js\?v=2026112[4-9][a-z]"><\/script>/.test(HTML));
  // the byte ledger takes the change back out
  assert.equal(hash(ledger.peelRiskMapEngine(engine, 'jh-chart-engine.js')), '5708b52846fa67ad33f6b51621a4919efb1dd56cbd618048fde1231aae9eaeff');
  assert.equal(hash(ledger.peelRiskMapRail(rail, 'jh-chart-tvrail.js')), '2f47a463a82cc92badecd64fbc4e2957a26867ef17d90d244b5fc1fc7a38679d');
  assert.equal(hash(ledger.peelRiskMapHtml(chart, 'chart.html')), '07772f8ce5990a0a84ddb60af1cd0ba3ae089a48e4bb7bb692ef6048e2fcc8c5');
  assert.ok(!ledger.peelRiskMapEngine(engine, 'jh-chart-engine.js').includes('btn-rmap'));
});

test('the home dashboard\'s Risk Map card names what is in the state across the whole map, and says so when nothing is', () => {
  const home = fs.readFileSync(path.join(R, 'classic-dashboard.html'), 'utf8');
  const start = home.indexOf('gj("data/regime-map.json").then(function(d){'), end = home.indexOf('});', home.indexOf('DESTROYED </span>', start));
  assert.ok(start > 0 && end > start);
  const body = home.slice(start + 'gj("data/regime-map.json").then('.length, end + 1);
  const el = { innerHTML: '' };
  const draw = d => { el.innerHTML = ''; new Function('document', 'tx', 'RC', '(' + body + ')(arguments[3])')({ getElementById: () => el }, v => (v > 0 ? 'g' : v < 0 ? 'r' : 'n'), {}, d); return el.innerHTML.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' '); };
  const text = draw(DOC);
  const L = M.lists(DOC);
  assert.ok(text.includes('BOOMING ' + L.booming.length + ' of ' + DOC.map.n), text.slice(-400));
  assert.ok(text.includes('DESTROYED ' + L.destroyed.length + ' of ' + DOC.map.n));
  for (const t of L.booming.slice(0, 5)) assert.ok(text.includes(t.t + ' +' + t.score), t.t);
  for (const t of L.destroyed.slice(0, 5)) assert.ok(text.includes(t.t + ' ' + t.score), t.t);
  const none = clone(DOC);
  none.map.booming = [];
  assert.match(draw(none), /BOOMING 0 of \d+ none today/);
  // a publication of the first engine (no whole map) still draws its own lists
  const old = clone(DOC);
  delete old.map;
  old.booming = old.strongest.slice(0, 2);
  const was = draw(old);
  assert.ok(was.includes('BOOMING ' + old.booming[0].ticker) && !/ of \d+/.test(was));
});

test('a colour that carries meaning is never re-skinned by the site build', () => {
  // 2026-11-24, found by the live proof (ops 6938): scripts/reskin_site.py turns every cool hue in a served file into the site's
  // amber, so the first deploy drew rising tiles amber and the blue palette amber. Every data colour lives in the script, which
  // carries the build's opt-out; the page (and with it the previous view) is re-skinned as it always was.
  assert.ok(JS.includes('jh-reskin-skip'));
  assert.ok(!HTML.includes('jh-reskin-skip'));
  for (const pal of Object.values(M.PALETTES)) for (const k of ['neg', 'pos', 'mid', 'none', 'up', 'dn']) assert.match(pal[k], /^#[0-9a-f]{6}$/);
  const heat = HTML.slice(HTML.indexOf('/* ---------- the heat map (2026-11-24) ----------'), HTML.indexOf('/* ---------- the previous view'));
  assert.ok(heat.length > 3000);
  for (const pal of Object.values(M.PALETTES)) for (const k of ['neg', 'pos', 'up', 'dn']) assert.ok(!heat.toLowerCase().includes(pal[k]), 'a data colour is written in the page: ' + pal[k]);
  assert.ok(JS.includes('host.style.setProperty("--rm-up", PALETTES[S.palette].up)') && JS.includes('host.style.setProperty("--rm-dn", PALETTES[S.palette].dn)'));
  assert.ok(!/\[data-palette/.test(HTML));
});
