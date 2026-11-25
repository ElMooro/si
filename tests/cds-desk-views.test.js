// CDS desk 2.0 - the views drawn from the 2.0 document (jh-cds-views.js) and the page that mounts them.
// The fixture is cut from the real document the engine wrote from the whole public tape (ops 6955), not made up.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), zlib = require('node:zlib');
const V = require('../jh-cds-views.js'), ui = require('../jh-cds-desk.js');
const ROOT = path.join(__dirname, '..');
const gz = f => JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures', f))).toString('utf8'));
const packet = () => gz('cds-desk-v200.json.gz'), tape = () => gz('cds-desk-tape-v200.json.gz');
const clean = html => assert.doesNotMatch(html, /undefined|NaN|\[object Object\]/);
const allRows = d => Object.entries(d.groups).flatMap(([g, b]) => ['rows', 'unpriced', 'dormant', 'quiet'].flatMap(k => (b[k] || []).map(r => Object.assign({ group: g, kind: k }, r))));

test('the fixture is the 2.0 document: four groups, the audit\'s two names read right, nobody twice', () => {
  const d = packet();
  assert.equal(d.version, '2.0.0');
  assert.deepEqual(Object.keys(d.groups).sort(), ['global_corp', 'sovereign', 'sub', 'us_corp']);
  const att = V.findRow(d, 'AT & T'), exp = V.findRow(d, 'EXPEDIA');
  assert.ok(att.spread_bp > 60 && att.spread_bp < 200, 'AT&T was published at 7 bp: a coupon written "1" is one percent');
  assert.ok(exp.spread_bp > 50 && exp.spread_bp < 200, 'Expedia was published at 18 bp');
  assert.equal(att.ticker, 'T');
  for (const [g, b] of Object.entries(d.groups)) { const names = b.rows.map(r => r.name); assert.equal(new Set(names).size, names.length, g + ' carries a name twice'); }
  assert.equal(V.findRow(d, 'AT&T'), null, 'the second AT&T key is gone');
});

test('at a glance: every index gets a card, an index that stopped printing is dated and last, and the heat map has a tile a sector', () => {
  const d = packet(), cards = V.indexCards(d);
  for (const i of d.indices) assert.ok(cards.includes(`data-key="IDX:${i.key}"`), i.key);
  const aged = d.indices.filter(i => i.status === 'aged');
  assert.ok(aged.length >= 1, 'the fixture keeps an index with an old last print (iTraxx Japan)');
  for (const i of aged) { assert.match(cards, new RegExp(`last printed ${i.last_date}`)); assert.ok(cards.lastIndexOf('cv-aged') > cards.indexOf(`IDX:${d.indices.find(x => x.status !== 'aged').key}`)); }
  const tiles = V.tilesOf(d, 'all', '1m', 'chg');
  assert.ok(tiles.length >= 5 && tiles.every(t => t.n > 0));
  assert.equal(tiles.reduce((s, t) => s + t.n, 0), V.rowsOf(d, 'all').filter(r => ['liquid', 'thin', 'aged', 'bracketed'].includes(r.status)).length, 'every name with a level is in exactly one tile');
  const html = V.heatHtml(d, { grp: 'all', win: '1m', measure: 'chg', pick: null });
  for (const t of tiles) assert.ok(html.includes(`data-v="${t.key.replace(/&/g, '&amp;')}"`), t.key);
  clean(html); clean(cards);
  // the boxes of a treemap tile the rectangle: areas sum to the whole and none leaves it
  const boxes = V.squarify(tiles.map(t => ({ v: t.n, t })), 0, 0, 1000, 380);
  assert.ok(Math.abs(boxes.reduce((s, b) => s + b.w * b.h, 0) - 380000) < 1);
  assert.ok(boxes.every(b => b.x >= -0.01 && b.y >= -0.01 && b.x + b.w <= 1000.01 && b.y + b.h <= 380.01));
});

test('heat map: every group, window and measure draws; wider is the trouble colour and tighter the safety colour', () => {
  const d = packet();
  for (const grp of Object.keys(V.GROUPS)) for (const win of Object.keys(V.WIN)) for (const measure of Object.keys(V.MEASURES)) clean(V.heatHtml(d, { grp, win, measure, pick: null }));
  const scale = { lo: 0, hi: 20 };
  assert.equal(V.colourOf(20, scale, 'chg').toLowerCase(), V.PAL.bad); assert.equal(V.colourOf(-20, scale, 'chg').toLowerCase(), V.PAL.good);
  assert.equal(V.colourOf(0, scale, 'chg').toLowerCase(), V.PAL.mid); assert.equal(V.colourOf(null, scale, 'chg'), V.PAL.none);
  assert.equal(V.measureOf({ spread_bp: 110, chg_1m_bp: 10 }, '1m', 'pct'), 10);          // 100 -> 110 is ten percent
  assert.equal(V.measureOf({ spread_bp: 110, chg_1m_bp: null }, '1m', 'chg'), null);
  // a tile's list is the tile's names, each one a name the page can open
  const t = V.tilesOf(d, 'all', '1m', 'chg')[0], pick = V.pickHtml(d, { grp: 'all', win: '1m', measure: 'chg', pick: t.key });
  for (const r of t.rows) assert.ok(pick.includes(`data-key="${r.key.replace(/&/g, '&amp;')}"`), r.key);
  clean(pick); clean(V.moversHtml(d, { grp: 'all', win: '1m' })); clean(V.bandsHtml(d));
});

test('every product of the file is counted and drawn, with what the desk measures for each', () => {
  const d = packet(), pr = d.products;
  for (const w of ['1d', '5d', '30d']) {
    const html = V.productsHtml(d, { prod: w });
    for (const c of pr.windows[w].classes) assert.ok(html.includes(V.CAT_WORDS[V.CAT_OF[c.cls]]) || c.n === 0, c.cls);
    clean(html);
  }
  const classes = new Set(pr.windows['30d'].classes.map(c => c.cls));
  for (const c of ['index', 'option', 'tranche', 'corp', 'sov', 'sub']) assert.ok(classes.has(c), c + ' is counted');
  assert.ok(pr.windows['30d'].classes.every(c => c.usd_mm < 5e6), 'one mis-filed notional of 1e14 no longer makes a class worth trillions');
  for (const cls of classes) assert.ok(V.CAT[V.CAT_OF[cls]], cls + ' has a colour');
});

test('options: a ladder a family, struck-wider share, expiries as bars, and what was set aside is listed', () => {
  const d = packet(), html = V.optionsHtml(d);
  assert.ok(d.options.families.length >= 3);
  for (const f of d.options.families) {
    assert.ok(html.includes(f.label)); assert.ok(f.ladder.length >= 3);
    const lvl = f.index_level;
    if (f.strike_kind === 'spread') assert.ok(f.ladder.every(r => r.strike / lvl > 0.5 && r.strike / lvl < 2.7), f.family + ' draws a strike far off its index');
    assert.ok(f.expiries.every(e => e.expiry && e.expiry < '2029'), f.family + ': an expiry years out is the index maturity, not an option expiry');
    for (const x of f.off_ladder || []) assert.ok(html.includes('Strikes off this ladder'));
    if (f.expiry_not_given) assert.match(html, /Expiry not given on/);
  }
  assert.match(html, /struck on the weaker-credit side/); clean(html);
  const svg = V.ladderSvg(d.options.families[0]);
  assert.equal((svg.match(/<rect /g) || []).length, d.options.families[0].ladder.length); clean(svg);
});

test('mortgage indices, quiet names and the repairs panel draw from the document', () => {
  const d = packet();
  const s = V.structuredHtml(d); assert.match(s, /CMBX/); for (const r of d.structured.rows) assert.ok(s.includes(r.label)); clean(s);
  const quiet = V.quietRows(d); assert.ok(quiet.length >= 10);
  const q = V.quietHtml(d, { quiet: true }); for (const r of quiet) assert.ok(q.includes(`data-key="${r.key.replace(/&/g, '&amp;')}"`)); clean(q);
  assert.ok((V.quietHtml(d, { quiet: false }).match(/<tr>/g) || []).length < (q.match(/<tr>/g) || []).length, 'the short view opens to the whole list');
  const ql = V.qualityHtml(d); assert.match(ql, /Coupons written in percent/); assert.match(ql, /Unconfirmed prints now/); assert.match(ql, /Aged levels/); clean(ql);
});

test('all of the engine\'s data is on the page: every key of the document opens, and every field of every row has a label', () => {
  const d = packet(), html = V.dataHtml(d);
  for (const k of Object.keys(d)) { assert.ok(V.DOC_WORDS[k], `the document key "${k}" has no words on the page`); assert.ok(html.includes(`>${k}<`), k + ' is not in the browser'); }
  const fields = new Set(); allRows(d).forEach(r => Object.keys(r).forEach(k => fields.add(k))); d.indices.forEach(i => Object.keys(i).forEach(k => fields.add(k)));
  const skip = new Set(['kind', 'spark', 'curve_last', 'sign_sources_20d', 'candidates', 'unconfirmed', 'listing', 'list']);
  const missing = [...fields].filter(k => !skip.has(k) && !V.FIELDS[k]);
  assert.deepEqual(missing, [], 'a row field the sheet would show without a label');
  // a branch opens whole: nothing is cut once asked for
  const whole = V.nodeHtml(V.atPath(d, ['groups', 'us_corp', 'rows']), ['groups', 'us_corp', 'rows'], 1e9);
  for (const r of d.groups.us_corp.rows) assert.ok(whole.includes(r.name.replace(/&/g, '&amp;').replace(/'/g, '&#39;')), r.name);
  clean(html);
  const csv = V.csvOf(d).split('\n');
  assert.equal(csv.length - 1, allRows(d).length + (d.groups.sovereign.universe || []).filter(u => u.status === 'not_in_tape').length, 'the file carries every name the desk knows');
  assert.match(csv[0], /name/); assert.doesNotMatch(csv.join('\n'), /undefined|\[object Object\]/);
});

test('a name\'s sheet: level, bonds beside it, every figure, and the prints behind the level', () => {
  const d = packet(), t = tape(), r = V.findRow(d, 'AT & T'), html = V.sheetHtml(r, d, t);
  assert.match(html, /Everything the desk has on AT&amp;T/); assert.match(html, /Its bonds/); assert.match(html, /CDS less bonds/);
  assert.ok(html.includes(String(r.bond_bp.toFixed(0))));
  for (const k of ['spread_bp', 'anchor_source', 'pct_rank_1y', 'trades_30d']) assert.ok(html.includes(V.FIELDS[k][0]), k);
  const rows = t.names['AT & T']; assert.ok(rows.length >= 2);
  assert.equal((V.tapeHtml(t, 'AT & T').match(/<tr>/g) || []).length - 1, rows.length, 'every print of the name is listed');
  assert.doesNotMatch(JSON.stringify(t), /isin|cusip|underlier id|red code/i, 'the reference obligation\'s identifier is not kept');
  clean(html);
  // before the tape has arrived the sheet says so instead of an empty table
  assert.match(V.tapeHtml(null, 'AT & T'), /loading|fetch/i);
  for (const row of allRows(d).slice(0, 60)) clean(V.sheetHtml(V.findRow(d, row.key) || row, d, t));
});

test('a distressed name is read in points: past the ceiling the page shows points, not tens of thousands of basis points', () => {
  const d = packet(), top = allRows(d).filter(r => r.kind === 'rows' && r.at_ceiling);
  assert.ok(top.length >= 1, 'the fixture keeps a name carried at the ceiling');
  for (const r of top) {
    assert.equal(r.spread_bp, 10000); assert.ok(r.points_upfront >= 40);
    const html = V.sheetHtml(V.findRow(d, r.key), d, tape());
    assert.match(html, /points upfront/); assert.match(html, /past the 10,000 bp ceiling/);
  }
  assert.ok(allRows(d).every(r => !(r.spread_bp > 10000)), 'no level past the ceiling');
  const table = ui.universeTable(ui.universeRows('global_corp', d.groups.global_corp), ['Sector'], 'none');
  if (d.groups.global_corp.rows.some(r => r.at_ceiling)) assert.match(table, /10,000\+ bp/);
});

test('the 1.6 tables read the 2.0 document: aged, at-coupon and subordinated names, a reference quote on the map', () => {
  const d = packet();
  for (const g of ['sovereign', 'us_corp', 'global_corp', 'sub']) {
    const rows = ui.universeRows(g, d.groups[g]), html = ui.universeTable(rows, [g === 'sovereign' ? 'Region' : 'Sector'], 'none');
    assert.ok(rows.length >= d.groups[g].rows.length); clean(html);
  }
  const st = new Set(allRows(d).filter(r => r.kind === 'rows').map(r => r.status));
  for (const s of ['liquid', 'aged', 'bracketed']) assert.ok(st.has(s), s);
  const us = ui.universeTable(ui.universeRows('us_corp', d.groups.us_corp), ['Sector'], 'none');
  assert.match(us, /aged/);
  const sov = ui.universeRows('sovereign', d.groups.sovereign), ref = sov.filter(r => r.status === 'not_in_tape' && typeof r.ref_bp === 'number');
  assert.ok(ref.length >= 1, 'a sovereign with no print carries a reference quote');
  const world = { w: 960, h: 415, paths: Object.fromEntries(sov.filter(r => r.iso3).map(r => [r.iso3, 'M0,0 10,0 10,10Z'])), names: {}, centroids: Object.fromEntries(sov.filter(r => r.iso3).map(r => [r.iso3, [5, 5]])) };
  const svg = ui.worldMap(world, sov);
  assert.match(svg, /≈/); assert.match(svg, /reference quote, not a print/); assert.doesNotMatch(svg, /undefined|NaN/);
});

test('the page mounts the views: script order, slots, the build leaves measured colours alone, and the 1.6 document still draws', () => {
  const page = fs.readFileSync(path.join(ROOT, 'cds.html'), 'utf8'), desk = fs.readFileSync(path.join(ROOT, 'jh-cds-desk.js'), 'utf8'), views = fs.readFileSync(path.join(ROOT, 'jh-cds-views.js'), 'utf8');
  assert.ok(page.indexOf('/jh-cds-views.js') > 0 && page.indexOf('/jh-cds-views.js') < page.indexOf('/jh-cds-desk.js'), 'the views load before the desk script that calls them');
  for (const id of ['cds2-glance', 'cds2-sub-panel', 'cds2-products', 'cds2-options', 'cds2-structured', 'cds2-quiet', 'cds2-quality', 'cds2-data']) assert.ok(desk.includes(`id="${id}"`), id);
  assert.match(views, /jh-reskin-skip/, 'series colours are measured: the site build must not re-skin them');
  assert.match(desk, /JHCdsViews/);
  // the document the site serves until the 2.0 data is written: every 2.0 view is empty or still sound, none throws
  const old = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/cds-desk-v130.json'), 'utf8'));
  for (const fn of ['glanceHtml', 'productsHtml', 'optionsHtml', 'structuredHtml', 'qualityHtml', 'dataHtml']) clean(V[fn](old, { grp: 'all', win: '1m', measure: 'chg', pick: null, prod: '5d', quiet: false }));
  assert.equal(V.optionsHtml(old), ''); assert.equal(V.structuredHtml(old), ''); assert.equal(V.quietHtml(old, { quiet: false }), '');
});

test('descriptive doctrine: the views promise no direction', () => {
  const d = packet(), text = [V.glanceHtml(d, { grp: 'all', win: '1m', measure: 'chg', pick: null }), V.productsHtml(d, { prod: '5d' }), V.optionsHtml(d), V.structuredHtml(d), V.qualityHtml(d)].join(' ').replace(/<[^>]+>/g, ' ');
  const names = new Set(allRows(d).flatMap(r => [r.name, r.company].filter(Boolean)));
  let stripped = text; names.forEach(n => { stripped = stripped.split(n).join(' '); });
  for (const w of ['buy', 'sell', 'target', 'forecast', 'predict', 'will widen', 'will tighten']) assert.ok(!new RegExp('\\b' + w, 'i').test(stripped), w);
});
