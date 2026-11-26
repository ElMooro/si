'use strict';
// The Credit Before Equity page (credit-before-equity.html + jh-credit-board.js) on the document the runner built with engine
// 3.0.0 from a cut of the real warehouse (ops 6942, 2026-11-25).
// Khalid 2026-11-24: "expand it to cover as many companies as possible and it should have a heatmap that shows industries with
// biggest corp spread widening which signal trouble and ones with lowes which signals safety also ... sort them by industry and
// sort them ascending and descending and also add companies CDS credit default swaps".
// Khalid 2026-11-24, later: "ai companies and oracle are under enormous stress why arent they showing" - they were on the board
// and nothing pointed at them: a lead needs the stock NOT to have followed, and Oracle's stock already fell. Hence the credit
// stress view (it does not look at the stock), the AI stack theme, and the Stress column; nothing that was there was moved.
// Khalid 2026-11-25: "i like it you should add an icon that would sort distrissed. also you should increase the coverage to include
// a lot more companies and think of different ways and addition to improve this engine so that it can predict what would happen to
// the stock of these companies. also make sure all the data from the engines is viewable in the iux" - hence the distressed switch,
// every published column in the table and as a file, the company sheet (every figure, every bond, its history), the board's own
// record of what stocks did next, the chance of a fall that record supports, and the whole document, browsable.
// The page measures nothing; these tests hold what it draws to the document, and hold its evidence status to the truth: a count
// the engine did not publish is never shown as zero, a document that contradicts itself is said to, and the page never claims the
// sources are fresh.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const R = path.resolve(__dirname, '..');
const B = (require(path.join(R, 'jh-credit-board.js')), globalThis.JHCreditBoard);
const S = (require(path.join(R, 'jh-credit-sheet.js')), globalThis.JHCreditSheet);
const SHEET_JS = fs.readFileSync(path.join(R, 'jh-credit-sheet.js'), 'utf8');
const FIX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(R, 'tests/fixtures/credit-before-equity-warehouse.json.gz'))));
const HTML = fs.readFileSync(path.join(R, 'credit-before-equity.html'), 'utf8');
const JS = fs.readFileSync(path.join(R, 'jh-credit-board.js'), 'utf8');
const clone = o => JSON.parse(JSON.stringify(o));
const doc = () => clone(FIX.expected);
const DOC = doc();
const T = B.table(DOC);
const LEADS = ['CREDIT_LEADS_DOWN', 'CREDIT_LEADS_UP'];
const state = over => Object.assign(B.firstState(DOC, {}), over || {});
const rgb = h => [1, 3, 5].map(i => parseInt(h.slice(i, i + 2), 16));
// the n-th cell of every body row of the first table in a piece of HTML (group headings are not rows)
const cells = (html, n) => [...html.slice(html.indexOf('<tbody>')).matchAll(/<tr>(.*?)<\/tr>/g)].map(m => [...m[1].matchAll(/<td[^>]*>(.*?)<\/td>/g)].map(c => c[1].replace(/<[^>]+>/g, ''))[n]);
const figure = s => (s === '–' || s == null) ? null : Number(String(s).replace('−', '-').replace(/[+,%]|bp|\s/g, ''));

// the same document as the 2.0 engine published it: no stress ranks, no themes, the 25 columns of 2.0
const SC = k => B.STRESS_COLUMNS.findIndex(c => c[0] === k);          // a stress-table cell by its column, not by where the column happens to stand
// the same document as the 2.1 engine published it: the 37 columns, no bond facts, no record, no chance, the three dollar books
const doc21 = () => {
  const d = doc();
  for (const k of ['matrix', 'bonds', 'evidence', 'history', 'risk', 'track']) delete d[k];
  d.version = '2.1.0';
  d.issuers.cols = d.issuers.cols.slice(0, 37);
  d.issuers.rows = d.issuers.rows.map(r => r.slice(0, 37));
  d.books = { ig: d.books.ig, hy: d.books.hy, em: d.books.em };
  delete d.coverage.by_book; delete d.coverage.bonds_not_usd; delete d.coverage.by_currency;
  d.names.forEach(x => { for (const k of ['cell', 'pd_1y', 'wam', 'due2y', 'step_bp', 'bond_px', 'curve_bp', 'vol', 'risk_1m', 'risk_3m']) delete x[k]; });
  return d;
};
const doc20 = () => {
  const d = doc21();
  delete d.stress; delete d.themes;
  d.version = '2.0.0';
  d.issuers.cols = d.issuers.cols.slice(0, 25);
  d.issuers.rows = d.issuers.rows.map(r => r.slice(0, 25));
  d.names.forEach(x => { delete x.stress; delete x.theme; delete x.layer; delete x.credit_conflict; });
  d.industries = Object.fromEntries(Object.entries(d.industries).map(([k, rows]) => [k, rows.map(r => { const o = Object.assign({}, r); for (const f of ['stress', 'n_stressed', 'stressed_value_pct', 'largest_stressed']) delete o[f]; return o; })]));
  return d;
};
const AI = DOC.themes[0];
const AT = DOC.stress.stressed_at;

test('the fixture is the engine\'s 3.0 document: companies, industries, windows, CDS, the model names, stress, the AI stack, the record', () => {
  assert.equal(DOC.contract, 'credit-before-equity.v2');
  assert.equal(DOC.version, '3.0.0');
  assert.ok(S && DOC.evidence.readings.length >= 20 && DOC.matrix.n > 30 && DOC.risk.n > 40 && FIX.expected_bonds.rows.length > 1000 && T.some(r => r.book === 'xig'));
  assert.ok(B.isV2(DOC) && B.hasStress(DOC) && B.isV2(doc20()) && !B.hasStress(doc20()));
  assert.equal(AI.key, 'ai');
  assert.ok(T.filter(r => r.theme === 'ai').length >= 12 && T.some(r => r.ticker === 'ORCL' && r.stress >= AT));
  assert.ok(T.length >= 60 && T.filter(r => r.ticker).length >= 40, String(T.length));
  assert.ok(DOC.industries.all.length >= 10);
  assert.ok(DOC.cds.priced.length >= 30 && DOC.cds.no_level.length >= 10);
  assert.ok(DOC.names.filter(x => typeof x.distance_to_default === 'number').length >= 18);
  assert.ok(DOC.names.some(x => LEADS.includes(x.signal)));
  assert.equal(DOC.windows.m1.ref, '2026-10-30');
});

test('the evidence status reconciles the real document and says what it does not verify', () => {
  const e = B.evidence(DOC);
  assert.equal(e.state, 'RECONCILED');
  assert.equal(e.checks.length, 10);
  assert.ok(e.checks.every(c => c.ok === true), JSON.stringify(e.checks.filter(c => c.ok !== true)));
  const html = B.evidenceHtml(DOC);
  assert.match(html, /does not verify that the bond, CDS and price sources are fresh/);
  assert.doesNotMatch(html, /sources are current|data is fresh|up to date/i);
});

test('a document that contradicts itself is said to, whichever count is wrong', () => {
  const cases = [
    d => { d.n_names += 1; }, d => { d.n_leads += 1; }, d => { d.n_awaiting_history += 1; }, d => { d.leads = d.leads.slice().reverse(); },
    d => { d.leads = d.leads.slice(1); }, d => { d.issuers.rows[3] = d.issuers.rows[3].slice(1); }, d => { d.coverage.companies += 1; }, d => { d.coverage.listed -= 1; },
    d => { d.industries.all[0].n += 2; }, d => { const x = d.names.find(n => n.signal === 'NONE'); x.signal = 'CREDIT_LEADS_UP'; },
    d => { d.stress.n_stressed += 1; }, d => { d.themes[0].n -= 1; }, d => { d.themes[0].layers[0].n += 1; },
  ];
  for (const amend of cases) {
    const d = doc();
    amend(d);
    const e = B.evidence(d);
    assert.equal(e.state, 'MISMATCH', amend.toString());
    assert.ok(e.checks.some(c => c.ok === false));
    assert.match(B.evidenceHtml(d), /does not agree with itself/);
  }
});

test('a count the engine did not publish is unknown, never zero', () => {
  for (const value of [undefined, null, '0', false, {}, [], NaN, Infinity, -1, 0.5]) {
    const d = doc();
    d.n_names = d.n_leads = d.n_awaiting_history = value;
    const e = B.evidence(d);
    assert.equal(e.state, 'INCOMPLETE');
    assert.equal(e.checks.filter(c => c.ok === null && c.detail === 'count not published').length, 3);
  }
  const d = doc();
  delete d.coverage;
  assert.equal(B.evidence(d).state, 'INCOMPLETE');
  const hero = B.heroHtml(d, state());
  assert.match(hero, /Unavailable/);
  assert.doesNotMatch(hero, />0</);
  const quiet = doc();
  delete quiet.degraded;
  assert.equal(B.evidence(quiet).state, 'INCOMPLETE');
});

test('a degraded run, an unreadable document and the first engine\'s document each have their own truthful state', () => {
  const d = doc();
  d.degraded = ['The CDS desk\'s document could not be read (not there): no traded CDS on this run.'];
  assert.equal(B.evidence(d).state, 'DEGRADED');
  assert.match(B.evidenceHtml(d), /Degraded: The CDS desk&#39;s document could not be read/);
  for (const bad of [null, undefined, [], 'x', 7]) assert.equal(B.evidence(bad).state, 'UNAVAILABLE');
  assert.equal(B.evidence({ names: 'none' }).state, 'UNAVAILABLE');
  const v1 = { version: '1.0.0', generated_at: '2026-11-23T22:35:07+00:00', n_names: 1, n_leads: 0, n_awaiting_history: 0, leads: [], degraded: [], gaps: [],
               names: [{ ticker: 'F', name: 'Ford Motor', signal: 'NONE', distance_to_default: 1.01, d_distance_to_default: 0, synthetic_cds_bp: 130, d_synthetic_cds_bp: 0, d_price_pct: 4.1, credit_direction: null }] };
  const e = B.evidence(v1);
  assert.equal(B.isV2(v1), false);
  assert.equal(e.state, 'RECONCILED');
  assert.equal(e.checks.length, 4);
  assert.match(e.notes.join(' '), /first document \(26 names, a structural model\)/);
  assert.match(B.previousHtml(v1), /<b>F<\/b>.*Ford Motor.*1\.01.*130\.0.*\+4\.1%.*NONE/);
});

test('whatever the document says is escaped before it is drawn', () => {
  const attack = '<img src=x onerror=alert(1)>';
  const d = doc();
  d.issuers.rows[0][2] = attack; d.issuers.rows[0][3] = attack;
  d.industries.all[0].industry = attack;
  d.cds.priced[0].name = attack; d.cds.priced[0].sector = attack; d.cds.no_level[0].name = attack;
  const lead = d.names.find(x => LEADS.includes(x.signal));
  lead.name = attack; lead.industry = attack; lead.ticker = attack;
  d.gaps = [attack]; d.method = attack; d.rules.spread = attack; d.match.by_rule[0].issuer = attack; d.match.largest_untied[0].issuer = attack;
  d.windows.w1 = { words: attack, ref: null, why: attack };
  const s = B.firstState(d, {});
  const drawn = [B.tableHtml(d, s), B.mapHtml(d, s, 1200).html, B.leadsHtml(d, Object.assign(s, { allLeads: true })), B.cdsHtml(d, Object.assign(s, { cdsAll: true })), B.evidenceHtml(d),
                 B.methodHtml(d), B.coverageHtml(d), B.controlsHtml(d, s), B.previousHtml(d), B.heroHtml(d, s), B.rankHtml(B.industryView(d, s), s, B.scaleOf(B.industryView(d, s), 'bp'))].join('\n');
  assert.doesNotMatch(drawn, /<img/);
  assert.ok(drawn.includes('&lt;img src=x onerror=alert(1)&gt;'));
});

test('every column sorts ascending and descending, with a row that has no figure last either way', () => {
  for (const [key] of B.COLUMNS) {
    const rows = B.withWindow(T, 'm1');
    for (const dir of ['asc', 'desc']) {
      const out = B.sortRows(rows, key, dir);
      assert.equal(out.length, rows.length);
      const vals = out.map(r => r[key]);
      const firstNull = vals.findIndex(v => v == null || v === '');
      const have = firstNull < 0 ? vals : vals.slice(0, firstNull);
      assert.ok(vals.slice(have.length).every(v => v == null || v === ''), key + ' ' + dir + ': a row with no figure is not last');
      for (let i = 1; i < have.length; i++) {
        const c = typeof have[i] === 'string' ? String(have[i - 1]).localeCompare(String(have[i])) : have[i - 1] - have[i];
        assert.ok(dir === 'asc' ? c <= 0 : c >= 0, key + ' ' + dir + ' at ' + i);
      }
    }
  }
  const tie = [{ k: 1, id: 'a' }, { k: 1, id: 'b' }, { k: null, id: 'c' }, { k: 0, id: 'd' }];
  assert.deepEqual(B.sortRows(tie, 'k', 'desc').map(r => r.id), ['a', 'b', 'd', 'c']);
  assert.deepEqual(B.sortRows(tie, 'k', 'asc').map(r => r.id), ['d', 'a', 'b', 'c']);
  assert.deepEqual(tie.map(r => r.id), ['a', 'b', 'c', 'd']);
});

test('the table is sorted by industry, either way, and by any column inside a group', () => {
  const up = B.tableHtml(DOC, state({ sort: { key: 'industry', dir: 'asc' }, limit: 1e9 })), down = B.tableHtml(DOC, state({ sort: { key: 'industry', dir: 'desc' }, limit: 1e9 }));
  const a = cells(up, 1).filter(Boolean), z = cells(down, 1).filter(Boolean);
  assert.deepEqual(a, a.slice().sort((x, y) => x.localeCompare(y)));
  assert.deepEqual(z, z.slice().sort((x, y) => y.localeCompare(x)));
  assert.match(up, /aria-sort="ascending"><button type="button" data-sort="industry">Industry ▲/);
  assert.match(down, /aria-sort="descending"><button type="button" data-sort="industry">Industry ▼/);
  const grouped = B.tableHtml(DOC, state({ group: true, sort: { key: 'd', dir: 'desc' } }));
  const GRP = () => new RegExp('<tr class="cb-grp"><td colspan="' + B.tableColumns(DOC, state()).length + '"><b>(.*?)<\\/b>', 'g');
  const heads = [...grouped.matchAll(GRP())].map(m => m[1]);
  const view = Object.fromEntries(B.industryView(DOC, { win: 'm1', book: 'all', measure: 'bp' }).map(r => [r.industry, r.d_bp]));
  assert.equal(heads.length, new Set(T.map(r => r.industry)).size);
  const order = heads.map(h => view[h.replace(/&amp;/g, '&')]).filter(v => v != null);
  assert.deepEqual(order, order.slice().sort((x, y) => y - x), 'groups run from the industry widening most to the one widening least');
  const flipped = [...B.tableHtml(DOC, state({ group: true, sort: { key: 'd', dir: 'asc' } })).matchAll(GRP())].map(m => view[m[1].replace(/&amp;/g, '&')]).filter(v => v != null);
  assert.deepEqual(flipped, order.slice().reverse());
});

test('the sort controls name every column, the direction, and follow the chosen window', () => {
  const html = B.tableHtml(DOC, state());
  for (const [key] of B.COLUMNS) assert.ok(html.includes('data-sort="' + key + '"') && html.includes('<option value="' + key + '"'), key);
  assert.match(html, /data-dir aria-label="Sort direction">Descending ▼/);
  assert.match(B.tableHtml(DOC, state({ sort: { key: 'spread_bp', dir: 'asc' } })), /data-dir aria-label="Sort direction">Ascending ▲/);
  assert.match(html, /data-sort="d">Δ 1M ▼/);
  assert.match(B.tableHtml(DOC, state({ win: 'ytd' })), /data-sort="d">Δ YTD ▼/);
  const d = cells(html, 6).map(figure).filter(v => v != null);
  assert.deepEqual(d, d.slice().sort((x, y) => y - x));
  const ytd = cells(B.tableHtml(DOC, state({ win: 'ytd', sort: { key: 'd', dir: 'asc' } })), 6).map(figure).filter(v => v != null);
  assert.deepEqual(ytd, ytd.slice().sort((x, y) => x - y));
  assert.match(html, /data-jh-nosort/);
});

test('the filters keep what they say: an industry, a book, listed stocks, leads, a search', () => {
  const ind = T.find(r => r.ticker).industry;
  assert.ok(B.filterRows(T, { industry: ind }).every(r => r.industry === ind));
  assert.equal(B.filterRows(T, { listed: true }).length, T.filter(r => r.ticker).length);
  assert.ok(B.filterRows(T, { book: 'hy' }).every(r => r.book === 'hy'));
  assert.equal(B.filterRows(T, { book: 'all' }).length, T.length);
  assert.equal(B.filterRows(T, { signal: 'leads' }).length, T.filter(r => LEADS.includes(r.signal)).length);
  assert.ok(B.filterRows(T, { signal: 'cds' }).every(r => typeof r.cds_bp === 'number'));
  assert.deepEqual(B.filterRows(T, { q: 'jpm' }).map(r => r.ticker), ['JPM']);
  assert.ok(B.filterRows(T, { q: 'ORACLE' }).some(r => r.ticker === 'ORCL'));
  assert.equal(B.filterRows(T, { q: 'no such company anywhere' }).length, 0);
  assert.match(B.tableHtml(DOC, state({ q: 'no such company anywhere' })), /No company matches/);
  const paged = B.tableHtml(DOC, state({ limit: 10 }));
  assert.equal(cells(paged, 0).length, 10);
  assert.match(paged, /10 of \d+ companies shown/);
  assert.match(paged, /data-more="200"/);
});

test('the heat map draws the document\'s industries: one tile each, sized by value held, coloured by the change', () => {
  for (const book of ['all', 'ig', 'hy', 'em']) {
    for (const win of ['m1', 'ytd']) {
      const view = B.industryView(DOC, { win, book, measure: 'bp' });
      const src = DOC.industries[book];
      assert.equal(view.length, src.length);
      view.forEach((r, i) => {
        assert.equal(r.industry, src[i].industry);
        assert.equal(r.d_bp, (src[i].win[win] || {}).d_bp == null ? null : src[i].win[win].d_bp);
        assert.equal(r.v, r.d_bp);
      });
    }
  }
  const s = state();
  const m = B.mapHtml(DOC, s, 1200);
  const boxes = [...m.html.matchAll(/class="cb-box[^"]*" data-ind="([^"]*)" title="([^"]*)" style="left:([\d.]+)px;top:([\d.]+)px;width:([\d.]+)px;height:([\d.]+)px;background:(#[0-9a-f]{6})"/g)];
  const live = m.view.filter(r => r.value > 0);
  assert.equal(boxes.length, live.length);
  assert.deepEqual(boxes.map(b => b[1]).sort(), live.map(r => B.esc(r.industry)).sort());
  for (const b of boxes) {
    const [x, y, w, h] = b.slice(3, 7).map(Number);
    assert.ok(x >= 0 && y >= 0 && x + w <= 1200 && y + h <= 440, b[1]);          // a sliver is drawn a pixel wide and stays inside the map
    const r = live.find(v => B.esc(v.industry) === b[1]);
    assert.equal(b[7], B.colour(r.v, m.scale, 'bp', 'rg'));
  }
  const area = Object.fromEntries(boxes.map(b => [b[1], (Number(b[5]) + 2) * (Number(b[6]) + 2)]));
  const total = live.reduce((t, r) => t + r.value, 0);
  for (const r of live) assert.ok(Math.abs(area[B.esc(r.industry)] / (1200 * 440) - r.value / total) < 0.002, r.industry);
  assert.doesNotMatch(m.html, /class="cb-map"[^>]*data-jh-title/, 'a titled map gets a section badge drawn on its tiles');
  assert.match(m.html, /Tightening \(safety\)/);
  assert.match(m.html, /Widening \(trouble\)/);
  assert.match(m.html, /One tile an industry\. Size: the market value of its bonds held by the scanned funds\. Colour: its spread change in basis points/);
  assert.match(B.mapHtml(DOC, state({ measure: 'level' }), 1200).html, /Colour: its spread level/);
});

test('a widening spread is the trouble colour, a tightening one the safety colour, louder the further it moved', () => {
  const scale = { lo: 0, hi: 40 };
  for (const palette of ['rg', 'cb']) {
    const p = B.PALETTES[palette], dist = (a, b) => rgb(a).reduce((t, v, i) => t + Math.abs(v - rgb(b)[i]), 0);
    const wide = [5, 15, 30, 40].map(v => B.colour(v, scale, 'bp', palette)), tight = [-5, -15, -30, -40].map(v => B.colour(v, scale, 'bp', palette));
    for (let i = 1; i < 4; i++) {
      assert.ok(dist(wide[i], p.bad) < dist(wide[i - 1], p.bad));
      assert.ok(dist(tight[i], p.good) < dist(tight[i - 1], p.good));
    }
    assert.equal(wide[3], p.bad);
    assert.equal(tight[3], p.good);
    assert.equal(B.colour(400, scale, 'bp', palette), p.bad);
    assert.equal(B.colour(0, scale, 'bp', palette), p.mid);
    assert.equal(B.colour(null, scale, 'bp', palette), p.none);
    assert.equal(B.colour(undefined, scale, 'bp', palette), p.none);
    const level = [60, 150, 400].map(v => B.colour(v, { lo: 60, hi: 400 }, 'level', palette));
    assert.ok(dist(level[2], p.bad) < dist(level[1], p.bad) && dist(level[1], p.bad) < dist(level[0], p.bad));
    assert.equal(B.textColour(12, palette), p.badText);
    assert.equal(B.textColour(-12, palette), p.goodText);
    assert.equal(B.textColour(0, palette), '');
    assert.equal(B.textColour(null, palette), '');
  }
  assert.notEqual(B.PALETTES.rg.bad, B.PALETTES.cb.bad);
  assert.equal(B.scaleOf([{ v: 2 }, { v: -3 }], 'bp').hi, 10);
  assert.equal(B.scaleOf([{ v: 2 }, { v: -3 }], 'pct').hi, 5);
  assert.equal(B.scaleOf([{ v: 22 }, { v: -31 }], 'bp').hi, 31);
});

test('the other measures are the same rows read another way', () => {
  const pct = B.industryView(DOC, { win: 'm1', book: 'all', measure: 'pct' }), lvl = B.industryView(DOC, { win: 'm1', book: 'all', measure: 'level' }), rel = B.industryView(DOC, { win: 'm1', book: 'hy', measure: 'rel' });
  pct.forEach(r => assert.equal(r.v, r.d_pct));
  lvl.forEach(r => assert.equal(r.v, r.spread_bp));
  const hy = T.filter(r => r.book === 'hy' && typeof r.d_m1 === 'number').map(r => [r.d_m1, r.value]).sort((a, b) => a[0] - b[0]);
  let run = 0, mid = null;
  const total = hy.reduce((t, x) => t + x[1], 0);
  for (const [d, v] of hy) { run += v; if (run >= total / 2) { mid = d; break; } }
  assert.equal(B.marketChange(DOC, 'm1', 'hy'), mid);
  assert.equal(mid, DOC.windows.m1.book_median_bp.hy);
  rel.forEach(r => { if (r.d_bp != null) assert.ok(Math.abs(r.v - (r.d_bp - mid)) < 0.051, r.industry); });
  assert.equal(B.weightedMedian([[5, 10], [7, 10], [2000, 1]]), 7);
  assert.equal(B.weightedMedian([]), null);
});

test('the two lists are the industries widening most and widening least, in order', () => {
  const s = state(), view = B.industryView(DOC, s), html = B.rankHtml(view, s, B.scaleOf(view, 'bp'));
  const [top, bottom] = html.split('</div><div class="cb-rank">').map(part => [...part.matchAll(/data-ind="([^"]*)"/g)].map(m => m[1]));
  const have = view.filter(r => typeof r.v === 'number');
  const desc = have.slice().sort((a, b) => b.v - a.v).slice(0, 6).map(r => B.esc(r.industry)), asc = have.slice().sort((a, b) => a.v - b.v).slice(0, 6).map(r => B.esc(r.industry));
  assert.deepEqual(top.map(x => view.find(r => B.esc(r.industry) === x).v), desc.map(x => view.find(r => B.esc(r.industry) === x).v));
  assert.deepEqual(bottom.map(x => view.find(r => B.esc(r.industry) === x).v), asc.map(x => view.find(r => B.esc(r.industry) === x).v));
  assert.match(html, /Widening most — trouble/);
  assert.match(html, /Widening least or tightening — safety/);
  assert.match(B.rankHtml(B.industryView(DOC, state({ measure: 'level' })), state({ measure: 'level' }), B.scaleOf(B.industryView(DOC, state({ measure: 'level' })), 'level')), /Widest spreads — trouble/);
  assert.equal(B.rankHtml([], s, { lo: 0, hi: 10 }), '');
});

test('an industry\'s detail lists its own companies and how many are in the table', () => {
  const ind = DOC.industries.all[0].industry, s = state({ industry: ind }), view = B.industryView(DOC, s);
  const html = B.detailHtml(DOC, s, view);
  assert.ok(html.includes('data-show-ind="' + B.esc(ind) + '"'));
  assert.match(html, /Widened most/);
  assert.match(html, /Tightened most/);
  const mine = T.filter(r => r.industry === ind && typeof r.d_m1 === 'number').sort((a, b) => b.d_m1 - a.d_m1);
  assert.ok(html.includes(B.esc(mine[0].name)));
  assert.equal(B.detailHtml(DOC, state({ industry: '' }), view), '');
});

test('the leads are the lead rows: every one, with what credit did and what the stock did', () => {
  const html = B.leadsHtml(DOC, state({ allLeads: true }));
  const down = DOC.names.filter(x => x.signal === 'CREDIT_LEADS_DOWN'), up = DOC.names.filter(x => x.signal === 'CREDIT_LEADS_UP');
  assert.equal((html.match(/class="cb-lead"/g) || []).length, down.length + up.length);
  assert.ok(html.includes('Credit worse, stock has not fallen (' + down.length + ')'));
  assert.ok(html.includes('Credit better, stock has not risen (' + up.length + ')'));
  for (const x of down.concat(up)) assert.ok(html.includes('<b>' + B.esc(x.ticker) + '</b>'), x.ticker);
  const lead = down.concat(up).find(x => x.signal_basis === 'bond spreads');
  assert.ok(html.includes('Spread ' + B.bp(lead.spread_bp) + ' bp, ' + B.bp(lead.d_spread_bp, true) + ' bp since Oct 30, 2026'));
  assert.ok(html.includes('Stock ' + B.pct(lead.d_price_pct, true)));
  const none = doc();
  none.names.forEach(x => { if (LEADS.includes(x.signal)) x.signal = 'NONE'; });
  assert.equal((B.leadsHtml(none, state()).match(/None on this run\./g) || []).length, 2);
  const few = B.leadsHtml(DOC, state());
  assert.ok((few.match(/class="cb-lead"/g) || []).length <= 16);
});

test('company CDS: every level the desk prints, sortable either way, a points-upfront quote shown as points', () => {
  const s = state({ cdsAll: true });
  const html = B.cdsHtml(DOC, s);
  assert.equal(cells(html, 0).length, DOC.cds.priced.length);
  for (const [key] of B.CDS_COLUMNS) assert.ok(html.includes('data-cds-sort="' + key + '"') && html.includes('<option value="' + key + '"'), key);
  const m = cells(html, 7).map(figure).filter(v => v != null);
  assert.deepEqual(m, m.slice().sort((a, b) => b - a));
  const asc = cells(B.cdsHtml(DOC, state({ cdsAll: true, cdsSort: { key: 'spread_bp', dir: 'asc' } })), 4);
  const levels = asc.filter(x => /bp$/.test(x)).map(figure);
  assert.deepEqual(levels, levels.slice().sort((a, b) => a - b));
  assert.match(B.cdsHtml(DOC, state({ cdsSort: { key: 'spread_bp', dir: 'asc' } })), /data-cds-dir aria-label="CDS sort direction">Ascending ▲/);
  const odd = DOC.cds.priced.find(c => c.believable === false);
  assert.ok(odd, 'the cut carries a points-upfront quote');
  assert.ok(html.includes(B.fixed(odd.points_upfront, 1) + ' pts upfront'));
  assert.ok(!html.includes(B.bp(odd.spread_bp) + ' bp'));
  assert.ok(html.includes(DOC.cds.no_level.length + ' more names trade with no level the desk can print'));
  const listed = B.cdsHtml(DOC, state({ cdsAll: true, cdsListed: true }));
  assert.equal(cells(listed, 0).length, DOC.cds.priced.filter(c => c.ticker).length);
  const liquid = B.cdsHtml(DOC, state({ cdsAll: true, cdsLiquid: true }));
  assert.equal(cells(liquid, 0).length, DOC.cds.priced.filter(c => c.liquid).length);
  assert.match(B.cdsHtml(DOC, state()), /data-cds-all="1">Show all/);
  const empty = doc();
  empty.cds = { priced: [], no_level: [] };
  assert.match(B.cdsHtml(empty, state()), /No reference entity matches/);
});

test('the previous view is the first board, one click away: the model names with the model\'s own columns', () => {
  const html = B.previousHtml(DOC), model = DOC.names.filter(x => typeof x.distance_to_default === 'number' || typeof x.synthetic_cds_bp === 'number');
  assert.equal(cells(html, 0).length, model.length);
  for (const head of ['Ticker', 'Name', 'DD', 'ΔDD', 'Model CDS bp', 'ΔCDS', 'ΔPrice', 'Credit', 'Signal']) assert.ok(html.includes('>' + head + '</th>'), head);
  assert.match(html, /A model, not a market price/);
  const x = model[0];
  assert.ok(html.includes('<b>' + B.esc(x.ticker) + '</b>'));
  assert.ok(cells(html, 8).every(v => ['NONE', 'CREDIT_LEADS_UP', 'CREDIT_LEADS_DOWN', 'INSUFFICIENT_HISTORY'].includes(v)));
  assert.match(HTML, /id="viewtoggle"[^>]*>Previous view</);
  assert.match(HTML, /id="prevview" hidden/);
  assert.match(JS, /toggle\.textContent = state\.view === "new" \? "Previous view" : "Back to the full board"/);
});

test('a comparison the ledger cannot make yet is offered as unavailable, with the reason', () => {
  const d = doc();
  d.windows.w1 = { words: '1 week', ref: null, why: 'The board\'s ledger holds no day near enough yet; it began on 2026-11-23 and adds a day each run.' };
  d.windows.m3 = { words: '3 months', ref: null, why: 'The board\'s ledger holds no day near enough yet.' };
  const wins = B.windowList(d);
  assert.deepEqual(wins.map(w => [w.key, w.ok]), [['w1', false], ['m1', true], ['m3', false], ['ytd', true]]);
  const html = B.controlsHtml(d, B.firstState(d, {}));
  assert.match(html, /data-win="w1" aria-pressed="false" disabled title="The board&#39;s ledger holds no day near enough yet/);
  assert.match(html, /data-win="m1" aria-pressed="true">1M</);
  assert.equal(B.firstState(d, { win: 'w1' }).win, 'm1', 'a remembered window that no longer exists falls back to the month');
  assert.equal(B.firstState(d, { win: 'ytd', palette: 'cb', measure: 'pct', book: 'hy' }).win, 'ytd');
  assert.equal(B.firstState(d, { palette: 'nope', measure: 'nope', book: 'nope' }).palette, 'rg');
  assert.match(B.evidence(d).notes.join(' '), /1 week: not available yet/);
});

test('figures are written with the sign of what is shown, never a minus zero', () => {
  assert.equal(B.bp(-0.04, true), '0.0');
  assert.equal(B.bp(0.04, true), '0.0');
  assert.equal(B.bp(12.34, true), '+12.3');
  assert.equal(B.bp(-12.36, true), '−12.4');
  assert.equal(B.bp(1977.4, true), '+1,977');
  assert.equal(B.bp(-623.2), '−623');
  assert.equal(B.bp(null), '–');
  assert.equal(B.bp(NaN), '–');
  assert.equal(B.pct(-0.04, true), '0.0%');
  assert.equal(B.pct(4.42, true), '+4.4%');
  assert.equal(B.pct('4'), '–');
  assert.equal(B.money(104657904142), '$104.7B');
  assert.equal(B.money(8870840), '$9M');
  assert.equal(B.money(291000), '$291K');
  assert.equal(B.money(null), '–');
  assert.equal(B.day('2026-10-30'), 'Oct 30, 2026');
  assert.equal(B.day(null), 'Unavailable');
});

test('the page keeps what it had and loads the board: the original sentence, the schedule, the site chrome', () => {
  assert.ok(HTML.includes('The bond desk reprices a company\'s risk before the equity desk does'));
  assert.ok(HTML.includes('A signal fires only when credit moves and the equity hasn\'t noticed. Daily 22:35 UTC.'));
  for (const id of ['board', 'evidence', 'hero', 'controls', 'map', 'rank', 'mapdetail', 'leads', 'table', 'cds', 'method', 'coverage', 'gaps', 'newview', 'prevview', 'prev', 'viewtoggle', 'foot', 'stress']) {
    assert.ok(HTML.includes('id="' + id + '"'), id);
  }
  // the stress view was added between two sections; every section that was there is still there, in the order it had
  const order = [...HTML.matchAll(/<section[^>]* id="(s-[a-z]+)"/g)].map(m => m[1]);
  // (3.0) the record was added after the leads it is the record of, and the whole document after the gaps: nothing that was there moved
  assert.deepEqual(order, ['s-glance', 's-industries', 's-stress', 's-leads', 's-next', 's-companies', 's-cds', 's-method', 's-coverage', 's-gaps', 's-data', 's-links']);
  assert.deepEqual(order.filter(x => x !== 's-next' && x !== 's-data'), ['s-glance', 's-industries', 's-stress', 's-leads', 's-companies', 's-cds', 's-method', 's-coverage', 's-gaps', 's-links']);
  assert.ok(HTML.includes('<h2 class="sec">Industries — where spreads are widening and where they are holding</h2>') && HTML.includes('<h2 class="sec">Leads — credit moved, the stock has not</h2>'));
  assert.deepEqual(B.COLUMNS.filter(c => c[0] !== 'stress' && c[0] !== 'risk_3m').map(c => c[0]), ['name', 'industry', 'book', 'n_bonds', 'value', 'spread_bp', 'd', 'x_m1', 'pct_m1', 'd_ytd', 'cds_bp', 'cds_d1m', 'px_m1', 'direction', 'signal']);
  assert.match(HTML, /<script src="\/jh-credit-board\.js\?v=\d+[a-z]"><\/script>/);
  assert.match(HTML, /<script src="\/jh-nav-drawer\.js" defer><\/script>/);
  assert.match(HTML, /<script src="\/sidebar\.js" defer><\/script>/);
  assert.match(HTML, /Evidence status/);
  assert.match(JS, /https:\/\/justhodl\.ai\/data\/credit-before-equity\.json/);
  for (const href of [...HTML.matchAll(/href="\/([a-z0-9-]+\.html)"/g)].map(m => m[1])) assert.ok(fs.existsSync(path.join(R, href)), href + ' is linked and does not exist');
});

test('colours that measure are the script\'s and survive the site build; the page\'s own chrome is the warm palette', () => {
  assert.match(JS, /jh-reskin-skip/);
  const warm = new Set(['#0c0b09', '#12110c', '#17150e', '#2b2820', '#3a3628', '#6a6455', '#8a836f', '#b5ad99', '#e8e2d4', '#f0b429', '#fff']);
  for (const hex of HTML.match(/#[0-9a-fA-F]{3,6}\b/g) || []) assert.ok(warm.has(hex.toLowerCase()), hex + ' in the page would be re-skinned or is not the site\'s palette');
  assert.doesNotMatch(HTML, /jh-reskin-skip/);
  for (const p of Object.values(B.PALETTES)) for (const k of ['bad', 'good', 'mid', 'none', 'badText', 'goodText']) assert.match(p[k], /^#[0-9a-f]{6}$/);
});

test('the treemap fills its box with no tile over another', () => {
  const nodes = [50, 30, 12, 5, 2, 1, 0].map((v, i) => ({ v, i }));
  const out = B.squarify(nodes, 0, 0, 900, 400);
  assert.equal(out.length, 6);
  const area = out.reduce((t, b) => t + b.w * b.h, 0);
  assert.ok(Math.abs(area - 900 * 400) < 1e-6);
  for (const b of out) assert.ok(Math.abs(b.w * b.h / (900 * 400) - b.node.v / 100) < 1e-9);
  for (let i = 0; i < out.length; i++) for (let j = i + 1; j < out.length; j++) {
    const a = out[i], b = out[j];
    assert.ok(a.x + a.w <= b.x + 1e-6 || b.x + b.w <= a.x + 1e-6 || a.y + a.h <= b.y + 1e-6 || b.y + b.h <= a.y + 1e-6, i + ' over ' + j);
  }
  assert.deepEqual(B.squarify([], 0, 0, 10, 10), []);
  assert.deepEqual(B.squarify(nodes, 0, 0, 0, 10), []);
});

// ------------------------------------------------------------------------------------------------ 2.1: stress, the AI stack
const rowsOf = html => [...html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>')).matchAll(/<tr[^>]*>(.*?)<\/tr>/g)].map(m => [...m[1].matchAll(/<td[^>]*>(.*?)<\/td>/g)].map(c => c[1].replace(/<[^>]+>/g, '')));
const stressTable = html => { const at = html.indexOf('class="cb-table cb-stress"'); return rowsOf(html.slice(at)); };

test('the stress view ranks companies by stress in their own book, highest first, whatever the stock did', () => {
  const s = state(), html = B.stressHtml(DOC, s);
  const want = T.filter(r => typeof r.stress === 'number' && r.value >= B.HELD_FIRST).sort((a, b) => b.stress - a.stress);
  const got = stressTable(html);
  assert.equal(got.length, Math.min(25, want.length));
  got.forEach((cells, i) => {
    const r = want[i];
    assert.ok(cells[0].includes(r.name.replace(/&/g, '&amp;')) && cells[0].startsWith(String(i + 1) + ' '), cells[0]);
    assert.equal(figure(cells[1]), Number(r.stress.toFixed(1)));
    assert.equal(cells[SC('spread_bp')], B.bp(r.spread_bp));
    assert.equal(cells[SC('xbook')], (Math.round(r.spread_bp / DOC.stress.book_spread_bp[r.book] * 10) / 10).toFixed(1) + '×');
    assert.equal(cells[SC('d_ytd')], B.bp(r.d_ytd, true));
    assert.equal(cells[SC('grp')], r.industry.replace(/&/g, '&amp;'));
  });
  assert.equal(got.length ? want[0].ticker : 'ORCL', 'ORCL', 'Oracle stands highest of the companies with $50M or more held');
  assert.match(html, /whatever the stock did/);
  assert.match(html, /The leads ask a different question: whether the stock has noticed\./);
  assert.match(html, /since Dec 31, 2025 as a share of where it started/);
  assert.match(html, /× book is the spread as a multiple of the middle company of its book \(IG [\d.]+ bp, HY [\d.]+ bp, EM [\d.]+ bp, Non-\$ IG [\d.]+ bp, Non-\$ HY [\d.]+ bp\)/);
  assert.match(html, /in its own book \(investment grade, high yield, emerging markets, and the two non-dollar books\)/);
  assert.match(html, /<option value="50000000" selected>\$50M or more<\/option>/);
  assert.match(html, /data-st-theme="" aria-pressed="true">Every company</);
  assert.match(html, /class="cb-table cb-stress" data-jh-nosort/);
  assert.match(html, /<div class="cb-ranked" id="stress-ranked"><h3>Every company, most stressed first<\/h3>/);
  assert.match(B.stressHtml(DOC, state({ stTheme: 'ai', stLayer: 'hyperscalers', stHeld: 0 })), /<h3>The AI stack, Hyperscalers: its companies<\/h3>/);
  // a rank's three parts are its title, and a rank at the bar or over it is the trouble colour
  const top = want[0];
  assert.ok(html.includes('title="In its own book: level ' + top.rk_level.toFixed(1) + ', year ' + top.rk_year.toFixed(1) + ', month ' + top.rk_month.toFixed(1) + '"'));
  assert.ok(html.includes('<span style="color:' + B.PALETTES.rg.badText + ';font-weight:700">' + top.stress.toFixed(1) + '</span>'));
});

test('the stress view sorts by every column either way and keeps to its filters', () => {
  for (const [key] of B.STRESS_COLUMNS) {
    const html = B.stressHtml(DOC, state({ stHeld: 0, stLimit: 1e9, stSort: { key, dir: 'asc' } }));
    assert.ok(html.includes('data-st-sort="' + key + '"') && html.includes('<option value="' + key + '"'), key);
    assert.ok(html.includes('aria-sort="ascending"><button type="button" data-st-sort="' + key + '"'), key);
  }
  const all = B.stressRows(DOC, { held: 0 });
  assert.equal(all.length, T.filter(r => typeof r.stress === 'number').length);
  assert.ok(B.stressRows(DOC, { held: 250e6 }).every(r => r.value >= 250e6));
  assert.ok(B.stressRows(DOC, { held: 0, book: 'hy' }).every(r => r.book === 'hy') && B.stressRows(DOC, { held: 0, listed: true }).every(r => r.ticker));
  const up = stressTable(B.stressHtml(DOC, state({ stHeld: 0, stLimit: 1e9, stSort: { key: 'stress', dir: 'asc' } }))).map(c => figure(c[1]));
  assert.deepEqual(up, up.slice().sort((a, b) => a - b));
  const wide = stressTable(B.stressHtml(DOC, state({ stHeld: 0, stLimit: 1e9, stSort: { key: 'pct_ytd', dir: 'desc' } }))).map(c => figure(c[SC('pct_ytd')])).filter(v => v != null);
  assert.deepEqual(wide, wide.slice().sort((a, b) => b - a));
  const paged = B.stressHtml(DOC, state({ stHeld: 0, stLimit: 10 }));
  assert.equal(stressTable(paged).length, 10);
  assert.match(paged, /10 of \d+ companies shown/);
  assert.match(paged, /data-st-more="25"/);
  assert.match(B.stressHtml(DOC, state({ stHeld: 1e9 * 1e3 })), /No company matches\./);
});

test('Oracle: under stress, in the AI stack, not a lead, and the board says why', () => {
  const o = T.find(r => r.ticker === 'ORCL');
  assert.ok(o.stress >= AT && o.theme === 'ai' && o.layer === 'hyperscalers' && o.signal === 'NONE' && o.direction === 'DETERIORATING' && o.px_m1 <= -3);
  assert.equal(B.verdict(o), 'Not a lead: credit is worse and the stock already fell ' + B.pct(Math.abs(o.px_m1)) + ' in the month. A lead is credit moving while the stock has not.');
  assert.doesNotMatch(B.leadsHtml(DOC, state({ allLeads: true })), /<b>ORCL<\/b>/);
  assert.match(B.leadNotesHtml(DOC), /A company whose stock already fell with its credit is not a lead and is not listed here\. It is in the <button type="button" class="cb-link" data-goto="s-stress">credit stress view<\/button>/);
  const s = state({ scope: 'ai', pick: o.id }), html = B.detailHtml(DOC, s, B.mapView(DOC, s));
  assert.ok(html.includes('<b>ORCL</b> Oracle Corporation'));
  assert.ok(html.includes('Spread <b>' + B.bp(o.spread_bp) + ' bp</b>, ' + (o.spread_bp / DOC.stress.book_spread_bp.ig).toFixed(1) + ' times the middle company of its book'));
  assert.ok(html.includes('Traded CDS ' + B.bp(o.cds_bp) + ' bp'));
  assert.ok(html.includes('Stress rank <b>' + o.stress.toFixed(1) + '</b> of 100 in its book: level ' + o.rk_level.toFixed(1) + ', year ' + o.rk_year.toFixed(1) + ', month ' + o.rk_month.toFixed(1)));
  assert.ok(html.includes('stock already fell') && html.includes('data-show-co="ORCL"') && html.includes('data-co=""'));
  assert.ok(html.includes('YTD <span style="color:' + B.PALETTES.rg.badText + '">' + B.bp(o.d_ytd, true) + ' bp</span> (' + B.pct(o.pct_ytd, true) + ')'));
  assert.match(B.verdict(T.find(r => !r.ticker)), /No listed stock is tied to it/);
  const lead = T.find(r => LEADS.includes(r.signal));
  assert.match(B.verdict(lead), /credit moved past its book and the stock has not followed/);
});

test('the AI stack reads as one line, by layer, and opens into its companies', () => {
  const html = B.themeHtml(DOC, state());
  const mine = T.filter(r => r.theme === 'ai');
  assert.ok(html.includes('<h3>AI stack</h3>') && html.includes('<b>' + mine.length + ' companies</b>, ' + mine.filter(r => r.ticker).length + ' with a listed stock'));
  assert.ok(html.includes('middle spread <b>' + B.bp(AI.spread_bp) + ' bp</b>') && html.includes('<b>' + AI.n_stressed + ' under stress</b> holding ' + B.pct(AI.stressed_value_pct) + ' of its bond value'));
  assert.ok(html.includes(B.bp(AI.win.ytd.d_bp, true) + ' bp</span> (' + B.pct(AI.win.ytd.d_pct, true) + ') since Dec 31, 2025, ' + AI.win.ytd.wider + ' of ' + AI.win.ytd.n + ' wider by 5 bp or more'));
  assert.ok(html.includes('data-scope="ai" data-goto="s-industries">See it on the heat map') && html.includes('data-st-theme="ai">Show its ' + mine.length + ' companies below'));
  const layers = rowsOf(html);
  assert.deepEqual(layers.map(c => c[0]), AI.layers.map(l => l.name.replace(/&/g, '&amp;')));
  AI.layers.forEach((l, i) => {
    assert.equal(layers[i][1], String(l.n));
    assert.equal(layers[i][3], B.bp(l.spread_bp));
    assert.equal(layers[i][8], String(l.n_stressed));
    assert.ok(html.includes('data-st-theme="ai" data-st-layer="' + l.key + '"'));
  });
  assert.ok(AI.layers.some(l => l.key === 'hyperscalers') && AI.layers.some(l => l.key === 'compute_vehicles'));
  // opened: its companies and only they, every one whatever its size, with the layer where the industry was
  const open = B.stressHtml(DOC, state({ stTheme: 'ai', stHeld: 0, stLimit: 100 }));
  const got = stressTable(open);
  assert.equal(got.length, mine.length);
  const words = B.layerWords(DOC);
  const byStress = mine.slice().sort((a, b) => (b.stress == null ? -1 : b.stress) - (a.stress == null ? -1 : a.stress));
  got.forEach((c, i) => { assert.ok(c[0].includes(byStress[i].name.replace(/&/g, '&amp;')), c[0]); assert.equal(c[SC('grp')], words[byStress[i].layer].replace(/&/g, '&amp;')); });
  assert.match(open, /data-st-theme="ai" aria-pressed="true">AI stack</);
  assert.match(open, /data-st-sort="grp">Layer</);
  assert.match(open, /<select data-st-layersel><option value="">All layers<\/option>/);
  const one = stressTable(B.stressHtml(DOC, state({ stTheme: 'ai', stLayer: 'hyperscalers', stHeld: 0, stLimit: 100 })));
  assert.deepEqual(one.map(c => c[SC('grp')]), mine.filter(r => r.layer === 'hyperscalers').map(() => 'Hyperscalers'));
  assert.ok(one.some(c => c[0].includes('Oracle')));
  // in the list of every company, a member of the theme is tagged
  assert.match(B.stressHtml(DOC, state({ stHeld: 0, stLimit: 1e9 })), /Oracle Corporation<\/button> <span class="cb-tag">AI stack<\/span>/);
  assert.equal(B.filterRows(T, { signal: 'ai' }).length, mine.length);
  assert.equal(B.filterRows(T, { signal: 'stressed', at: AT }).length, T.filter(r => r.stress >= AT).length);
});

test('the heat map can be drawn one tile a company of the AI stack, by any measure', () => {
  const mine = T.filter(r => r.theme === 'ai' && r.n_bonds && r.value > 0);
  for (const measure of ['bp', 'pct', 'rel', 'level', 'stress']) {
    for (const win of ['m1', 'ytd', 'w1']) {
      const s = state({ scope: 'ai', measure, win }), m = B.mapHtml(DOC, s, 1200);
      const boxes = [...m.html.matchAll(/class="cb-box[^"]*" data-co="([^"]*)" title="([^"]*)" style="left:([\d.]+)px;top:([\d.]+)px;width:([\d.]+)px;height:([\d.]+)px;background:(#[0-9a-f]{6})"/g)];
      assert.equal(boxes.length, mine.length, measure + ' ' + win);
      assert.doesNotMatch(m.html, /data-ind=/);
      for (const b of boxes) {
        const r = m.view.find(v => B.esc(v.key) === b[1]), [x, y, w, h] = b.slice(3, 7).map(Number);
        assert.ok(x >= 0 && y >= 0 && x + w <= 1200 && y + h <= 440, b[1]);
        assert.equal(b[7], B.colour(r.v, m.scale, measure, 'rg'));
        const row = r.row, col = { m1: 'd_m1', ytd: 'd_ytd', w1: 'd_w1' }[win], pcol = { m1: 'pct_m1', ytd: 'pct_ytd', w1: 'pct_w1' }[win];
        if (measure === 'bp') assert.equal(r.v, row[col]);
        if (measure === 'pct') assert.equal(r.v, row[pcol]);
        if (measure === 'level') assert.equal(r.v, row.spread_bp);
        if (measure === 'stress') assert.equal(r.v, row.stress);
        if (measure === 'rel' && r.v != null) assert.ok(Math.abs(r.v - (row[col] - B.marketChange(DOC, win, row.book))) < 0.051);
      }
    }
  }
  const s = state({ scope: 'ai', measure: 'stress' }), m = B.mapHtml(DOC, s, 1200);
  assert.deepEqual([m.scale.lo, m.scale.hi, m.scale.mid], [0, 100, 50]);
  // a rank has a middle: the middle company of a book is neutral, the calmest the safety colour, the most stressed the trouble colour
  const [hot, calm, even] = [B.colour(99.6, m.scale, 'stress', 'rg'), B.colour(12, m.scale, 'stress', 'rg'), B.colour(50, m.scale, 'stress', 'rg')].map(rgb);
  assert.ok(hot[0] > hot[1] + 100 && calm[1] > calm[0] + 40 && B.colour(50, m.scale, 'stress', 'rg') === B.PALETTES.rg.mid, JSON.stringify([hot, calm, even]));
  assert.ok(rgb(B.colour(60, m.scale, 'stress', 'rg'))[0] < rgb(B.colour(85, m.scale, 'stress', 'rg'))[0], 'the further past the middle, the louder');
  assert.match(m.html, /Calmest of its book/);
  // an industry's share under stress has no middle: one hue from none to the most
  const ind = B.scaleOf(B.industryView(DOC, { win: 'm1', book: 'all', measure: 'stress' }), 'stress');
  assert.equal(ind.mid, undefined);
  assert.ok(rgb(B.colour(ind.hi, ind, 'stress', 'rg'))[0] > rgb(B.colour(0, ind, 'stress', 'rg'))[0] + 80);
  assert.match(m.html, /One tile a company of the AI stack\. Size: the market value of its bonds held by the scanned funds\. Colour: its stress rank in its own book, 0 to 100\. Click a tile for the company\./);
  assert.match(m.html, /Most stressed of its book/);
  const o = T.find(r => r.ticker === 'ORCL');
  assert.ok(m.html.includes('title="ORCL Oracle Corporation: stress rank ' + o.stress.toFixed(1) + ' of 100 · spread ' + B.bp(o.spread_bp) + ' bp · Hyperscalers · ' + B.money(o.value) + ' held"'));
  // Oracle's tile is the loudest on the stress map, and the map's first list names it
  const loud = m.view.slice().sort((a, b) => (b.v == null ? -1 : b.v) - (a.v == null ? -1 : a.v))[0];
  assert.equal(loud.ticker, 'ORCL');
  const rank = B.rankHtml(m.view, s, m.scale);
  assert.match(rank, /Most stress — trouble/);
  assert.match(rank, /class="cb-rank-row" data-co="[^"]*"><span class="cb-rank-i">1<\/span><span class="cb-rank-n"><b>ORCL<\/b> Oracle/);
  assert.match(B.rankHtml(B.mapView(DOC, state({ scope: 'ai' })), state({ scope: 'ai' }), B.scaleOf(B.mapView(DOC, state({ scope: 'ai' })), 'bp')), /The companies of the theme whose spreads moved out the most\./);
  // one outlier does not mute the rest: past ten companies the colour is full at the ninth-decile move
  const fake = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 400].map(v => ({ kind: 'company', v }));
  assert.deepEqual(B.scaleOf(fake, 'bp'), { lo: 0, hi: 10, capped: true });
  assert.deepEqual(B.scaleOf(fake.map(r => ({ kind: 'industry', v: r.v })), 'bp'), { lo: 0, hi: 400 });
  // the controls offer the map of the theme, and the industry map is still what the board opens on
  const ctl = B.controlsHtml(DOC, state());
  assert.match(ctl, /data-scope="industries" aria-pressed="true">Industries</);
  assert.match(ctl, /data-scope="ai" aria-pressed="false">AI stack</);
  assert.match(ctl, /data-measure="stress" aria-pressed="false">Stress</);
  assert.equal(B.firstState(DOC, {}).scope, 'industries');
  // the map of the theme opens on the long window in percent, and leaves the industry map's window and measure as they were
  const first = B.firstState(DOC, { win: 'm1', measure: 'bp' });
  assert.deepEqual([first.win, first.measure, first.thWin, first.thMeasure], ['m1', 'bp', 'ytd', 'pct']);
  assert.equal(B.mapState(first), first);
  const onTheme = B.mapState(Object.assign(first, { scope: 'ai' }));
  assert.deepEqual([onTheme.win, onTheme.measure, onTheme.scope, onTheme.palette, first.win, first.measure], ['ytd', 'pct', 'ai', 'rg', 'm1', 'bp']);
  assert.match(B.controlsHtml(DOC, onTheme), /data-win="ytd" aria-pressed="true">YTD</);
  assert.match(B.controlsHtml(DOC, onTheme), /data-measure="pct" aria-pressed="true">Change, %</);
  assert.equal(B.firstState(DOC, { measure: 'stress' }).measure, 'stress');
});

test('on the industry map, Stress is the share of an industry\'s bond value in companies under stress', () => {
  const view = B.industryView(DOC, { win: 'm1', book: 'all', measure: 'stress' });
  view.forEach((r, i) => {
    assert.equal(r.v, DOC.industries.all[i].stressed_value_pct);
    const rs = T.filter(x => x.industry === r.industry && x.n_bonds), hot = rs.filter(x => x.stress >= AT);
    assert.equal(r.n_stressed, hot.length);
    const total = rs.reduce((t, x) => t + x.value, 0);
    if (total > 0) assert.ok(Math.abs(r.v - 100 * hot.reduce((t, x) => t + x.value, 0) / total) < 0.051, r.industry);
  });
  const s = state({ measure: 'stress' }), m = B.mapHtml(DOC, s, 1200);
  assert.match(m.html, /Colour: the share of its bond value that is in companies under stress/);
  assert.match(m.html, /None of it under stress/);
  assert.match(B.rankHtml(m.view, s, m.scale), /The industries with the largest share of their bond value in companies under stress\./);
  const ind = view.find(r => r.n_stressed > 0).industry;
  assert.match(B.detailHtml(DOC, state({ industry: ind }), B.industryView(DOC, state())), /\d+ under stress holding [\d.]+% of its bond value/);
});

test('a lead that was not called because the bonds and a traded CDS disagree is named, with both moves', () => {
  const clash = DOC.names.filter(x => x.credit_conflict === true);
  assert.ok(clash.length >= 1 && clash.every(x => x.signal === 'NONE' && x.cds_says === 'contradicts'));
  const html = B.leadNotesHtml(DOC);
  for (const x of clash) assert.ok(html.includes('<b>' + x.ticker + '</b> (bonds ' + B.bp(x.d_spread_bp, true) + ' bp, CDS ' + B.bp(x.cds_d1m, true) + ' bp in the month)'), x.ticker);
  assert.match(html, /Not called, because the bonds and a liquid traded CDS point opposite ways/);
  const none = doc();
  none.names.forEach(x => { delete x.credit_conflict; });
  assert.doesNotMatch(B.leadNotesHtml(none), /Not called, because/);
  // the notes sit under the two columns, not in their grid: a third child would squeeze the columns to a third of the width
  assert.equal((B.leadsHtml(DOC, state()).match(/class="cb-leadcol"/g) || []).length, 2);
  assert.doesNotMatch(B.leadsHtml(DOC, state()), /cb-note cb-wide|Not called/);
  assert.ok(HTML.indexOf('<div id="leads"></div>') < HTML.indexOf('<div id="leadnotes"></div>') && HTML.indexOf('<div id="leadnotes"></div>') < HTML.indexOf('id="s-companies"'));
});

test('the summary names who is under stress and the AI stack, and the table carries the rank', () => {
  const hero = B.heroHtml(DOC, state());
  assert.ok(hero.includes('<div class="cb-lbl">Under credit stress</div><div class="cb-big">' + DOC.stress.n_stressed_listed + '</div>'));
  assert.ok(hero.includes('<div class="cb-lbl">AI stack, ' + AI.n + ' companies</div><div class="cb-big">' + B.bp(AI.spread_bp) + ' bp</div>'));
  assert.ok(hero.includes('data-st-theme="" data-goto="s-stress">see them</button>') && hero.includes('data-st-theme="ai" data-goto="s-stress">open it</button>'));
  assert.ok(hero.includes('<b>ORCL</b> ' + T.find(r => r.ticker === 'ORCL').stress.toFixed(1)));
  for (const label of ['Companies with bonds read', 'Bonds with a spread', 'Leads', 'Traded CDS levels']) assert.ok(hero.includes('<div class="cb-lbl">' + label + '</div>'), label);
  const html = B.tableHtml(DOC, state({ q: 'ORCL' }));
  assert.match(html, /data-sort="stress">Stress</);
  assert.ok(html.includes('font-weight:700">' + T.find(r => r.ticker === 'ORCL').stress.toFixed(1) + '</span>'));
  assert.match(html, /<option value="stressed">Under stress<\/option><option value="ai">AI stack<\/option>/);
  const hot = rowsOf(B.tableHtml(DOC, state({ signal: 'stressed', limit: 1e9, sort: { key: 'stress', dir: 'desc' } })));
  assert.equal(hot.length, T.filter(r => r.stress >= AT).length);
  const method = B.methodHtml(DOC);
  for (const word of ['Stress rank', 'When credit disagrees', 'Theme']) assert.ok(method.includes('<dt>' + word + '</dt>'), word);
  assert.match(method, /3M – Aug 31, 2026 \(84 days; the board&#39;s own ledger\)/);          // since the month-ends were kept (ops 6950), three months is a real day
  assert.equal(B.shortName('CoreWeave, Inc. Class A Common Stock'), 'CoreWeave');
  assert.equal(B.shortName('Oracle Corporation'), 'Oracle');
  assert.equal(B.shortName('GALAXY HELIOS DATA CENTERS II LLC'), 'GALAXY HELIOS DATA CENTERS II');
});

test('on a 2.0 document the board draws what it has and says the stress view is not published yet', () => {
  const d = doc20();
  const e = B.evidence(d);
  assert.equal(e.state, 'RECONCILED');
  assert.equal(e.checks.length, 8);
  assert.match(e.notes.join(' '), /The stress ranks and the AI stack are not in this document \(version 2\.0\.0\); they appear when the 2\.1 engine has published\./);
  assert.match(B.stressHtml(d, B.firstState(d, {})), /The stress ranks and the AI stack appear when the 2\.1 engine has published\. This document is version 2\.0\.0\./);
  const ctl = B.controlsHtml(d, B.firstState(d, {}));
  assert.doesNotMatch(ctl, /data-scope=/);
  assert.match(ctl, /data-measure="stress" aria-pressed="false" disabled title="Appears when the 2\.1 engine has published">Stress</);
  const hero = B.heroHtml(d, B.firstState(d, {}));
  assert.doesNotMatch(hero, /Under credit stress|AI stack/);
  assert.match(hero, /Traded CDS levels/);
  const table = B.tableHtml(d, B.firstState(d, {}));
  assert.equal(rowsOf(table).length, Math.min(100, B.table(d).length));
  assert.doesNotMatch(table, /option value="stressed"/);
  assert.ok(rowsOf(table).every(c => c[10] === '–'), 'the Stress column is empty, never a zero');
  assert.equal(B.mapHtml(d, B.firstState(d, {}), 1200).view.length, d.industries.all.length);
  assert.equal(B.leadNotesHtml(d), '');
  assert.deepEqual(B.themeView(d, { scope: 'ai', win: 'm1', book: 'all', measure: 'bp' }), []);
  assert.deepEqual(B.stressRows(d, { held: 0 }), []);
});

// ------------------------------------------------------------------------------------------------ 3.0
const BONDS = FIX.expected_bonds;
const EV = DOC.evidence;
const text = html => html.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/\s+/g, ' ');
const ORCL = T.find(r => r.ticker === 'ORCL');

test('the distressed switch: one click shows only the distressed companies, widest spread first', () => {
  const dist = T.filter(r => r.distressed);
  assert.ok(dist.length >= 5 && S.distressed(DOC).length === dist.length);
  const off = B.tableHtml(DOC, state());
  assert.ok(off.includes('<button type="button" class="cb-distress" data-distress="table" aria-pressed="false"'));
  assert.ok(off.includes('<svg class="cb-ico"') && off.includes('<span>Distressed</span><b>' + dist.length + '</b>'), 'the switch carries the icon, the word and the count');
  // what the click sets: the filter, the widest first, every one of them on the page
  const on = B.tableHtml(DOC, state({ distress: true, signal: 'distressed', sort: { key: 'spread_bp', dir: 'desc' }, limit: 1e9 }));
  assert.ok(on.includes('data-distress="table" aria-pressed="true"'));
  const rows = rowsOf(on);
  assert.equal(rows.length, dist.length);
  const spreads = rows.map(c => figure(c[5])).filter(v => v != null);
  assert.deepEqual(spreads, spreads.slice().sort((a, b) => b - a));
  assert.ok(rows.every(c => c[0].includes('distressed')));
  // the same switch in the stress view, where a distressed company with no rank is still shown
  const st = B.stressHtml(DOC, state({ stDistress: true, stHeld: 0, stLimit: 1e9, stSort: { key: 'spread_bp', dir: 'desc' } }));
  assert.ok(st.includes('data-distress="stress" aria-pressed="true"') && st.includes('<h3>The distressed companies, widest spread first</h3>'));
  assert.equal(stressTable(st).length, dist.length);
  assert.equal(B.stressRows(DOC, { held: 0, distressed: true }).length, dist.length);
  // and at the top of the page
  const tile = S.distressTile(DOC);
  assert.ok(tile.includes('<div class="cb-big">' + dist.length + '</div>') && tile.includes('data-distress="table" data-goto="s-companies"') && B.heroHtml(DOC, state()).includes(tile));
  assert.match(text(tile), new RegExp(dist.filter(r => r.ticker).length + ' with a listed stock'));
  // the click itself is in the board's script: it keeps what the table showed, and a second click puts it back
  assert.match(JS, /if \(d\.distress === "table"\)[\s\S]{0,400}state\.kept = \{[\s\S]{0,700}state\.signal = "distressed"; state\.sort = \{ key: "spread_bp", dir: "desc" \}/);
  assert.match(JS, /state\.signal = k\.signal \|\| ""; state\.sort = k\.sort/);
});

test('every column the engine publishes is in the table when asked for, sortable either way, with a label and a description', () => {
  const cols = S.allColumns(DOC);
  assert.deepEqual(cols.map(c => c[0]).sort(), DOC.issuers.cols.slice().sort());
  assert.ok(cols.every(c => S.FIELD[c[0]] && c[1].length > 1 && c[4].length >= 15 && c[4] !== c[1]), 'each has a label and says what it is');
  const html = B.tableHtml(DOC, state({ cols: 'all', sort: { key: 'spread_bp', dir: 'desc' } }));
  assert.equal((html.match(/<th /g) || []).length, DOC.issuers.cols.length);
  assert.match(html, new RegExp('data-cols="all" aria-pressed="true">Every column \\(' + DOC.issuers.cols.length + '\\)<'));
  for (const [key] of cols) {
    assert.ok(html.includes('data-sort="' + key + '"') && html.includes('<option value="' + key + '"'), key);
    const up = B.sortRows(T, key, 'asc').map(r => r[key]).filter(v => v != null && v !== ''), down = B.sortRows(T, key, 'desc').map(r => r[key]).filter(v => v != null && v !== '');
    assert.deepEqual(down, up.slice().reverse().sort((a, b) => (typeof a === 'string' ? String(b).localeCompare(String(a)) : b - a)), key);
  }
  // a row of the wide table is the document's row, figure for figure
  const wide = B.tableHtml(DOC, state({ cols: 'all', q: 'ORCL', sort: { key: 'spread_bp', dir: 'desc' } }));
  const cellsOf = rowsOf(wide)[0];
  cols.forEach((f, i) => {
    if (f[0] === 'name' || f[0] === 'stress' || f[0] === 'industry') return;
    assert.equal(cellsOf[i].replace(/&amp;/g, '&'), S.shown(f, ORCL[f[0]], { palette: 'rg', doc: DOC }).text, f[0]);
  });
  // a column the engine adds later is shown under its own key rather than hidden
  const d = doc();
  d.issuers.cols.push('brand_new');
  d.issuers.rows.forEach(r => r.push(7));
  assert.ok(S.allColumns(d).some(c => c[0] === 'brand_new' && c[1] === 'brand_new') && B.tableHtml(d, state({ cols: 'all' })).includes('data-sort="brand_new"'));
  // the board's own columns are still the table it opens on
  assert.equal(B.tableColumns(DOC, state()).length, B.COLUMNS.length);
  assert.equal(B.tableColumns(doc21(), state()).length, B.COLUMNS.length - 1, 'a document with no chance of a fall gets no column of dashes');
});

test('the table as a file carries the published keys and the published values, untouched', () => {
  const csv = S.csvText(DOC.issuers.cols, T);
  const lines = csv.trim().split('\r\n');
  assert.equal(lines[0], DOC.issuers.cols.join(','));
  assert.equal(lines.length, T.length + 1);
  const at = DOC.issuers.cols.indexOf('spread_bp');
  const row = lines.find(l => l.startsWith('T:ORCL,ORCL,'));
  assert.ok(row && row.split(',').includes(String(ORCL.spread_bp)));
  assert.equal(S.csvText(['a', 'b'], [{ a: 'x,y', b: 'he said "no"' }, { a: null, b: 2 }]), 'a,b\r\n"x,y","he said ""no"""\r\n,2\r\n');
  assert.equal(S.csvText(BONDS.cols, BONDS.rows.slice(0, 2)).split('\r\n')[0], BONDS.cols.join(','));
  assert.ok(B.tableHtml(DOC, state()).includes('data-csv="companies"') && at > 0);
});

test('a company\'s sheet shows every figure the engine holds for it, each one explained', () => {
  const html = S.sheetHtml(DOC, 'T:ORCL', { palette: 'rg', bonds: S.bondsOf(BONDS, 'T:ORCL'), history: null });
  const said = text(html);
  for (const f of S.allColumns(DOC)) {
    assert.ok(html.includes('<span class="cb-key">' + f[0] + '</span>'), f[0] + ' is not on the sheet');
    assert.ok(said.includes(f[4].replace(/\s+/g, ' ')), 'the description of ' + f[0]);
  }
  assert.match(html, /<h3 id="sheet-title"><b>ORCL<\/b> Oracle Corporation<\/h3>/);
  assert.ok(html.includes('data-sheet-close') && html.includes('href="/chart.html?symbol=ORCL"'));
  assert.ok(said.includes(B.verdict(ORCL)));
  assert.match(said, new RegExp('Spread ' + B.bp(ORCL.spread_bp) + ' bp'));
  assert.match(said, /under stress/);
  // its listed-stock row and its CDS row: whatever the engine published beside the table
  const nm = DOC.names.find(x => x.ticker === 'ORCL');
  assert.ok(html.includes('id="sheet-stock"') && said.includes(String(nm.price)) && said.includes(nm.price_date));
  const cds = DOC.cds.priced.find(c => c.ticker === 'ORCL');
  if (cds) assert.ok(html.includes('id="sheet-cds"') && said.includes(String(cds.spread_bp)));
  // a company with no listed stock, and an id that is not on the board
  const loner = T.find(r => !r.ticker && r.spread_bp != null);
  const lone = text(S.sheetHtml(DOC, loner.id, { palette: 'rg', bonds: S.bondsOf(BONDS, loner.id), history: null }));
  assert.match(lone, /no listed stock tied/);
  assert.match(lone, /No listed stock is tied to it, so the board has no stock to measure\./);
  assert.match(S.sheetHtml(DOC, 'T:NOPE', {}), /The document holds no company under T:NOPE\./);
  // every name on the page opens a sheet
  assert.ok(B.tableHtml(DOC, state({ q: 'ORCL' })).includes('data-sheet="T:ORCL"') && B.stressHtml(DOC, state()).includes('data-sheet="T:ORCL"'));
  const lead = DOC.names.find(x => LEADS.includes(x.signal));
  assert.ok(B.leadsHtml(DOC, state({ allLeads: true })).includes('data-sheet="' + S.idOf(DOC, lead.ticker, lead.name) + '"'));
  assert.equal(S.idOf(DOC, 'ORCL'), 'T:ORCL');
  assert.equal(S.idOf(DOC, '', loner.name), loner.id);
});

test('its bonds: every one the engine read, sortable either way, and no identifier of a bond', () => {
  const mine = S.bondsOf(BONDS, 'T:ORCL');
  assert.equal(mine.length, ORCL.n_bonds + (ORCL.n_other || 0));
  const html = S.bondsHtml(mine, { key: 'maturity', dir: 'asc' }, { palette: 'rg' });
  assert.equal(rowsOf(html).length, mine.length);
  const dates = rowsOf(html).map(c => c[2]);
  assert.equal(dates[0], B.day(mine.map(b => b.maturity).sort()[0]));
  for (const [key] of S.BOND_FIELDS) {
    const down = S.bondsHtml(mine, { key, dir: 'desc' }, { palette: 'rg' });
    assert.ok(down.includes('aria-sort="descending"><button type="button" data-bond-sort="' + key + '"'), key);
  }
  const wide = rowsOf(S.bondsHtml(mine, { key: 'spread_bp', dir: 'desc' }, { palette: 'rg' })).map(c => figure(c[7])).filter(v => v != null);
  assert.deepEqual(wide, wide.slice().sort((a, b) => b - a));
  assert.ok(wide.length === ORCL.n_spread);
  // the spread of the company is its bonds' spreads weighted by the value beside them: the sheet can be checked by hand
  const priced = mine.filter(b => b.spread_bp != null);
  const avg = priced.reduce((s, b) => s + b.spread_bp * b.value, 0) / priced.reduce((s, b) => s + b.value, 0);
  assert.ok(Math.abs(avg - ORCL.spread_bp) < 0.06);
  assert.match(html, /The identifiers of the bonds are not republished\./);
  assert.ok(!BONDS.cols.includes('k') && !BONDS.cols.includes('isin') && !BONDS.cols.includes('cusip'));
  assert.match(html, /data-csv="bonds"/);
  // the states it can be in are each said
  assert.match(S.bondsHtml('loading'), /Loading its bonds\./);
  assert.match(S.bondsHtml('unpublished'), /not published yet/);
  assert.match(S.bondsHtml('error'), /could not be loaded\. Nothing is shown rather than something stale\./);
  assert.match(S.bondsHtml([]), /lists no bond under this company/);
  assert.equal(S.bondsOf({ cols: ['x'], rows: [] }, 'T:ORCL'), null);
});

test('its history: the spread at each kept month-end beside its book, closed by today\'s reading', () => {
  const dates = ['2026-08-31', '2026-09-30', '2026-10-30'];
  const history = { dates, companies: { 'T:ORCL': [150, null, 269.4] }, books: { ig: [90, 95, 101] } };
  const pts = S.historyOf(history, ORCL, DOC);
  assert.deepEqual(pts.map(p => [p.d, p.v, p.b]), [['2026-08-31', 150, 90], ['2026-09-30', null, 95], ['2026-10-30', 269.4, 101], [DOC.as_of, ORCL.spread_bp, DOC.stress.book_spread_bp.ig]]);
  const html = S.chartHtml(pts, ORCL, { palette: 'rg' });
  assert.ok(html.includes('stroke="' + S.SERIES.co + '"') && html.includes('stroke="' + S.SERIES.book + '"'), 'two lines, in the two validated colours');
  assert.match(html, /role="img" aria-label="ORCL: spread at each month-end, beside the middle company of its book"/);
  assert.match(text(html), /ORCL, spread in basis points Middle company of investment grade/);
  assert.match(html, new RegExp('>' + B.bp(ORCL.spread_bp) + ' bp</text>'), 'the last point is labelled with its value');
  assert.match(html, /<details class="cb-figures"><summary[^>]*>The 4 month-ends as figures<\/summary>/);
  assert.match(text(html), new RegExp(B.day(DOC.as_of) + " today's holdings"));
  assert.equal((html.match(/M[\d.]+ [\d.]+/g) || []).length >= 3, true, 'a month with no reading breaks the line rather than being drawn through');
  assert.equal(S.historyOf(history, T.find(r => r.ticker === 'JPM'), DOC), null);
  assert.match(S.chartHtml([{ d: '2026-10-30', v: 100, b: null }], ORCL, {}), /fewer than two month-ends/);
  assert.deepEqual(S.niceTicks(60, 290), [0, 100, 200, 300]);
  assert.deepEqual(S.niceTicks(812, 1340), [800, 1000, 1200, 1400]);
  // the two colours are in the script the site build leaves alone, never in the page
  assert.match(SHEET_JS, /jh-reskin-skip/);
  assert.ok(!HTML.includes(S.SERIES.co) && !HTML.includes(S.SERIES.book));
  const sheet = S.sheetHtml(DOC, 'T:ORCL', { palette: 'rg', bonds: 'loading', history: 'loading' });
  assert.match(sheet, /Loading its history\./);
  assert.match(S.sheetHtml(DOC, 'T:ORCL', { bonds: 'loading', history: history }), /class="cb-chart"/);
});

test('what the stock did next: the record is the engine\'s, figure for figure, and says what it does not show', () => {
  const html = B.firstState && S.nextHtml(DOC, state());
  const said = text(html);
  assert.match(said, new RegExp('At each of ' + EV.month_ends + ' month-ends from ' + B.day(EV.first) + ' to ' + B.day(EV.last)));
  assert.ok(said.includes(EV.company_months.toLocaleString('en-US') + ' company-months of ' + EV.companies.toLocaleString('en-US') + ' companies'));
  // direction: the board's own leads, with their record and how sure it is - whatever that record is
  assert.ok(said.includes(EV.direction.words));
  assert.ok(said.includes('A lead down (credit worse, the stock has not fallen): over the next month its stocks did ' + B.pct(EV.direction.lead_down.mean, true)));
  assert.ok(said.includes('How sure: ' + String(EV.direction.lead_down.t.toFixed(1)).replace('-', '−')));
  assert.match(said, /The leads are kept as they were/);
  // the size of the move: the share that fell, band by band, is the evidence's table
  for (const b of EV.risk.bands) {
    const c = EV.risk.h3[String(b.i)];
    if (!c) continue;
    assert.ok(said.includes(b.words), b.words);
    assert.ok(html.includes('<b>' + B.pct(c.fell) + '</b> <span class="cb-dim">of ' + c.n.toLocaleString('en-US') + '</span>'), b.words);
    c.thirds.forEach(t => { if (t) assert.ok(html.includes('title="' + t.n.toLocaleString('en-US') + ' company-months">' + B.pct(t.fell) + '</td>')); });
  }
  assert.match(html, /Too few company-months to say/);
  // the check: said beside happened, as published
  for (const b of EV.check.h3.buckets) assert.ok(html.includes('<td class="r">' + B.pct(b.said) + '</td><td class="r"><b>' + B.pct(b.happened) + '</b></td>'));
  assert.ok(said.includes('the table had given the faller the higher chance ' + B.pct(EV.check.h3.auc) + ' of the time (a coin: 50%)'));
  assert.match(said, /built only on earlier months whose outcome was already known that day/);
  // the record, reading by reading, over either horizon
  for (const g of EV.groups) {
    for (const hk of ['h1', 'h3']) {
      const tbl = S.nextHtml(DOC, state({ evGroup: g.key, evHorizon: hk }));
      const part = tbl.slice(tbl.indexOf('id="next-record"'));
      for (const x of EV.readings.filter(r => r.group === g.key)) {
        assert.ok(part.includes('<td class="l">' + B.esc(x.words) + '</td><td class="r">' + (x[hk].n || 0).toLocaleString('en-US') + '</td>'), x.key + ' ' + hk);
      }
      assert.ok(part.includes('data-ev-group="' + g.key + '" aria-pressed="true"') && part.includes('data-ev-horizon="' + hk + '" aria-pressed="true"'));
    }
  }
  // what it cannot see is on the page, not in a footnote somewhere else
  for (const c of EV.caveats) assert.ok(said.includes(c.replace(/\s+/g, ' ')), c.slice(0, 40));
  assert.match(said, /delisted is not in the history at all/);
  // nothing on it promises
  assert.doesNotMatch(said, /will (fall|rise|outperform|underperform)|guarantee|predicts that|sure to/i);
  // the traded CDS, week by week
  for (const x of EV.cds.readings) assert.ok(html.includes('<td class="l">' + x.words + '</td>'), x.key);
  assert.ok(said.includes(EV.cds.name_weeks.toLocaleString('en-US') + ' name-weeks'));
});

test('a figure in the record is bold only past the bar, and a cell of the nine is coloured only past two', () => {
  const d = doc();
  const x = d.evidence.readings.find(r => r.key === 'cell:worse/flat');
  x.h1.t = -4.2; x.h1.mean = -3.1;
  const html = S.nextHtml(d, state({ evGroup: 'cells', evHorizon: 'h1' }));
  assert.ok(html.includes('<td class="r" style="font-weight:700;color:var(--text)">−4.2</td>'));
  const quiet = S.nextHtml(DOC, state({ evGroup: 'cells', evHorizon: 'h1' }));
  const sure = DOC.evidence.readings.filter(r => r.group === 'cells' && Math.abs(r.h1.t) >= 3).length;
  assert.equal((quiet.slice(quiet.indexOf('id="next-record"'), quiet.indexOf('id="next-cds"')).match(/font-weight:700;color:var\(--text\)">[−\d.]+<\/td>/g) || []).filter((_, i) => i % 1 === 0).length >= sure, true);
  // the nine cells: today's counts are the document's, and a cell with nothing sure behind it is the neutral colour
  const mx = S.matrixHtml(DOC, state(), EV);
  for (const [key, n] of Object.entries(DOC.matrix.cells)) assert.ok(mx.includes('data-cell="' + key + '"') && mx.includes('<span class="cb-mx-v">' + n + '</span>'), key);
  const neutral = B.colour(0, { hi: 2 }, 'bp', 'rg');
  for (const m of mx.matchAll(/data-cell="([^"]+)"[^>]*style="background:(#[0-9a-f]{6})"/g)) {
    const rec = EV.readings.find(r => r.key === 'cell:' + m[1]);
    if (Math.abs(rec.h1.t) < 2) assert.equal(m[2], neutral, m[1] + ' is coloured with nothing sure behind it');
  }
  const loud = S.matrixHtml(d, state(), d.evidence);
  assert.notEqual(loud.match(/data-cell="worse\/flat"[^>]*style="background:(#[0-9a-f]{6})"/)[1], neutral);
  // a cell filters the table to its companies
  assert.equal(B.filterRows(T, { cell: 'flat/down' }).length, T.filter(r => r.cell === 'flat/down').length);
  assert.match(B.tableHtml(DOC, state({ cell: 'flat/down' })), /data-cell="flat\/down" aria-pressed="true"/);
});

test('a company\'s chance of a fall is the record\'s own table for its spread and its volatility, said as a record', () => {
  assert.ok(typeof ORCL.risk_3m === 'number');
  const band = EV.risk.h3[String(ORCL.band)], cuts = DOC.risk.vol_cuts, third = ORCL.vol < cuts[0] ? 0 : ORCL.vol < cuts[1] ? 1 : 2;
  assert.equal(ORCL.risk_3m, (band.thirds[third] || band).fell);
  const said = text(S.outlookHtml(DOC, ORCL, { palette: 'rg' }));
  assert.ok(said.includes('Stocks whose bonds traded this wide (' + EV.risk.bands[ORCL.band].words + ') and whose own volatility stood in the ' + EV.risk.thirds[third] + ': ' + B.pct(ORCL.risk_3m) +
    ' were at some point 20% or more under where they started inside three months (the whole board: ' + B.pct(EV.board.h3.fell_big) + ')'));
  assert.ok(said.includes('one in ten did worse than ' + B.pct(ORCL.lo_3m, true) + ' and one in ten better than ' + B.pct(ORCL.hi_3m, true)));
  const cell = EV.readings.find(r => r.key === 'cell:' + ORCL.cell);
  assert.ok(said.includes('Which way: its reading today, ' + cell.words.toLowerCase() + ', has ' + (Math.abs(cell.h1.t) >= 3 ? 'told' : 'not told') + ' the direction: ' + B.pct(cell.h1.mean, true)));
  for (const key of S.readingsOf(DOC, ORCL)) assert.ok(EV.readings.some(r => r.key === key), key + ' is not a reading the engine measured');
  assert.deepEqual(S.readingsOf(DOC, ORCL).map(k => k.split(':')[0]), ['level', 'cell', 'stress', 'book']);
  // in the tables: the column, bold from one in three up; and the tile at the top
  const row = rowsOf(B.tableHtml(DOC, state({ q: 'ORCL' })))[0];
  assert.equal(row[B.COLUMNS.findIndex(c => c[0] === 'risk_3m')], B.pct(ORCL.risk_3m));
  const top = B.sortRows(T.filter(r => typeof r.risk_3m === 'number'), 'risk_3m', 'desc')[0];
  assert.ok(B.tableHtml(DOC, state({ q: top.ticker })).includes('font-weight:700;color:' + B.PALETTES.rg.badText + '">' + B.pct(top.risk_3m)) === (top.risk_3m >= 100 / 3));
  const tile = S.riskTile(DOC);
  assert.ok(tile.includes('<div class="cb-big">' + DOC.risk.one_in_three + '</div>') && tile.includes('data-risk="1" data-goto="s-companies"') && tile.includes('data-goto="s-next"'));
  assert.equal(DOC.risk.one_in_three, T.filter(r => r.risk_3m >= 100 / 3).length);
  // a company read in another currency is given none, and the sheet says why
  const other = T.find(r => r.book === 'xig' && r.ticker) || T.find(r => r.book === 'xig');
  assert.equal(other.risk_3m, null);
  if (other.ticker) assert.match(text(S.outlookHtml(DOC, other, {})), /Its bonds are not in dollars, and the history the chances come from holds dollar bonds only: no chance is given\./);
  assert.match(text(S.fieldCell(S.FIELD.risk_3m, { risk_3m: null }, {})), /–/);
  assert.match(S.FIELD.risk_3m[4], /A record, not a promise\./);
});

test('the board\'s own calls are kept and scored: said beside happened, or when the first is due', () => {
  const waiting = { calls_kept: 3, first_call: '2026-11-25', last_call: '2026-11-27', horizons: [
    { key: 'h1', words: '1 month', sessions: 21, days_scored: 0, stocks_scored: 0, buckets: [], next_due: { call_day: '2026-11-25', sessions_left: 19 } },
    { key: 'h3', words: '3 months', sessions: 63, days_scored: 0, stocks_scored: 0, buckets: [], next_due: { call_day: '2026-11-25', sessions_left: 61 } }] };
  const d = doc();
  d.track = waiting;
  const a = text(S.trackHtml(d));
  assert.match(a, /3 days of calls kept, from Nov 25, 2026\./);
  assert.match(a, /Over 1 month: none scored yet; the first \(Nov 25, 2026\) is due in 19 sessions\./);
  d.track.horizons[0] = { key: 'h1', words: '1 month', sessions: 21, days_scored: 2, stocks_scored: 2100, buckets: [{ from: 0, to: 8, n: 1900, said: 2.1, happened: 2.6 }, { from: 8, to: 18, n: 200, said: 10.4, happened: 9.0 }], next_due: null };
  const b = S.trackHtml(d);
  assert.match(text(b), /Over 1 month: 2 days scored, 2,100 stocks\./);
  assert.ok(b.includes('<td class="r">2.1%</td><td class="r"><b>2.6%</b></td>') && b.includes("<h4>The board's own calls over 1 month</h4>"));
  const none = doc();
  none.track = { calls_kept: 0, first_call: null, last_call: null, horizons: [] };
  assert.match(text(S.trackHtml(none)), /No day of calls is kept yet; the first is kept the next time the engine publishes\./);
  delete none.track;
  assert.equal(S.trackHtml(none), '');
});

test('the whole document is on the page: every key it publishes, with what it is and where it is drawn', () => {
  const d = doc();
  d.track = { calls_kept: 0, first_call: null, last_call: null, horizons: [] };
  const html = S.dataHtml(d, state());
  for (const key of Object.keys(d)) {
    assert.ok(S.DOC_KEYS[key], key + ' is published with no description on the page');
    assert.ok(html.includes('<summary data-jh-title="' + key + '"><b>' + key + '</b> — ' + B.esc(S.DOC_KEYS[key][0])), key);
  }
  for (const file of ['data/credit-before-equity.json', DOC.bonds.key, DOC.history.key]) assert.ok(html.includes('href="https://justhodl.ai/' + file + '"'), file);
  // every column, with how many companies have it
  for (const f of S.allColumns(d)) assert.ok(html.includes('<span class="cb-key">' + f[0] + '</span>'), f[0]);
  assert.ok(html.includes('<td class="r">' + T.filter(r => r.spread_bp != null).length.toLocaleString('en-US') + '</td>'));
  // a list of rows is a table that opens whole; an object is shown key by key; nothing is cut off silently
  const cds = S.nodeHtml(d.cds.priced, 'cds.priced', state(), 1);
  assert.equal(rowsOf(cds).length, 25);
  assert.ok(cds.includes('data-data-open="cds.priced">Show all ' + d.cds.priced.length + '</button>'));
  const st = state();
  assert.equal(S.click({ dataOpen: 'cds.priced' }, st), true);
  assert.equal(rowsOf(S.nodeHtml(d.cds.priced, 'cds.priced', st, 1)).length, d.cds.priced.length);
  assert.ok(S.dataHtml(d, st).includes('<details open><summary data-jh-title="cds">'));
  assert.match(S.nodeHtml(null, 'x', st, 0), /not published/);
  assert.match(S.nodeHtml({ a: { b: { c: { d: { e: 1 } } } } }, 'x', st, 0), /\{&quot;e&quot;:1\}/);
  // a key the engine adds later is shown, not dropped
  d.something_new = { a: 1 };
  assert.ok(S.dataHtml(d, state()).includes('<b>something_new</b> — Published by the engine; this page has no description for it yet.'));
  assert.equal(S.dataHtml(null, state()), '<p>The document could not be read.</p>');
});

test('the books: a company read in another currency has its own book, on the map, in the table and in the stress view', () => {
  const opts = B.bookOptions(DOC, 'All').map(o => o[0]);
  assert.deepEqual(opts, ['all', 'ig', 'hy', 'em', 'xig', 'xhy']);
  assert.deepEqual(B.bookOptions(doc21(), 'All').map(o => o[0]), ['all', 'ig', 'hy', 'em']);
  const ctl = B.controlsHtml(DOC, state());
  assert.ok(ctl.includes('data-book="xig" aria-pressed="false">Non-dollar IG<') && ctl.includes('data-book="xhy"'));
  const xig = T.filter(r => r.book === 'xig');
  assert.equal(rowsOf(B.tableHtml(DOC, state({ book: 'xig', limit: 1e9 }))).length, xig.length);
  assert.ok(xig.length >= 5 && xig.every(r => r.ccy && r.ccy !== 'USD'));
  assert.equal(B.mapHtml(DOC, state({ book: 'xig' }), 1200).view.length, DOC.industries.xig.length);
  assert.equal(B.stressRows(DOC, { held: 0, book: 'xig' }).length, xig.filter(r => typeof r.stress === 'number').length);
  assert.equal(B.firstState(DOC, { book: 'xig' }).book, 'xig');
  assert.equal(B.firstState(doc21(), { book: 'xig' }).book, 'all');
  // a company with dollar bonds is read in dollars, in the dollar book it always stood in
  assert.ok(T.filter(r => ['ig', 'hy', 'em'].includes(r.book)).every(r => r.ccy === 'USD'));
  assert.match(text(B.coverageHtml(DOC)), new RegExp(DOC.coverage.bonds_not_usd.toLocaleString('en-US') + " of them in euros, sterling, Canadian or Australian dollars, each against its own government's yields"));
  assert.match(text(B.coverageHtml(DOC)), /By book: investment grade \d+, high yield \d+, emerging markets \d+, non-dollar investment grade \d+/);
  const sheet = text(S.sheetHtml(DOC, xig[0].id, { bonds: 'loading', history: null }));
  assert.match(sheet, new RegExp('over ' + xig[0].ccy + ' government yields'));
});

test('on a 2.1 document everything that was there still draws, and what 3.0 adds says it is not published yet', () => {
  const d = doc21();
  assert.ok(B.isV2(d) && B.hasStress(d));
  assert.equal(B.evidence(d).state, 'RECONCILED');
  const st = B.firstState(d, {});
  assert.match(S.nextHtml(d, st), /measured by engine 3\.0 on the board's own month-ends\. This document \(version 2\.1\.0\) does not carry it yet\./);
  const sheet = S.sheetHtml(d, 'T:ORCL', { palette: 'rg', bonds: 'unpublished', history: null });
  assert.match(sheet, /The bond-by-bond document is not published yet/);
  assert.match(sheet, /The month-by-month history is not published yet/);
  assert.match(sheet, /this document does not carry it yet/);
  assert.doesNotMatch(sheet, /Implied default|Bond price|Chance of a 20% fall/);
  assert.equal(S.riskTile(d), '');
  assert.ok(S.distressTile(d).includes('data-distress="table"'));
  assert.equal(S.allColumns(d).length, 37);
  const table = B.tableHtml(d, st);
  assert.doesNotMatch(table, /Fall risk 3M/);
  assert.match(table, /data-distress="table"/);
  assert.doesNotMatch(B.stressHtml(d, st), /Fall risk 3M/);
  assert.ok(Object.keys(d).every(k => S.DOC_KEYS[k]));
  assert.match(S.dataHtml(d, st), /<b>issuers\.cols<\/b> — the 37 columns/);
});

test('the page carries the sheet, the two new sections and the second script, and opens the sheet as a dialog', () => {
  for (const id of ['next', 'data', 'sheet']) assert.ok(HTML.includes('id="' + id + '"'), id);
  assert.match(HTML, /<div id="sheet" class="cb-sheet" role="dialog" aria-modal="true" aria-labelledby="sheet-title" data-jh-chrome hidden><\/div>/);
  assert.match(HTML, /<script src="\/jh-credit-board\.js\?v=\d+[a-z]"><\/script>\s*<script src="\/jh-credit-sheet\.js\?v=\d+[a-z]"><\/script>/);
  assert.ok(HTML.indexOf('/jh-credit-sheet.js') < HTML.indexOf('JHCreditBoard.start'));
  assert.ok(HTML.includes('<h2 class="sec">What the stock did next — the board\'s own record, month-end by month-end</h2>'));
  assert.ok(HTML.includes('<h2 class="sec">Everything the engine published — every key, every column, the files</h2>'));
  // the sheet closes on Escape wherever the focus is, and gives the focus back to what opened it
  assert.match(JS, /D\.addEventListener\("keydown", function \(ev\) \{ if \(ev\.key === "Escape" && state\.sheet\) \{ closeSheet\(\); \} \}\);/);
  assert.match(JS, /if \(opener && opener\.focus && host\.contains\(opener\)\) opener\.focus\(\);/);
  // the documents beside the main one are asked for once, from the engine's own keys, and a failure is said
  assert.match(JS, /root\.fetch\(S\.BASE \+ meta\.key \+ "\?t=" \+ encodeURIComponent\(String\(doc\.generated_at \|\| ""\)\)\)/);
  assert.match(JS, /function \(\) \{ extra\[what\] = false; \}/);
  assert.equal(S.BASE, 'https://justhodl.ai/');
  // without the second script the board is the board it was
  const keep = globalThis.JHCreditSheet;
  try {
    globalThis.JHCreditSheet = null;
    const bare = B.tableHtml(DOC, state({ q: 'ORCL' }));
    assert.ok(!bare.includes('data-sheet=') && !bare.includes('data-distress') && bare.includes('<b>ORCL</b> Oracle Corporation'));
    assert.ok(!B.heroHtml(DOC, state()).includes('Fall risk'));
  } finally { globalThis.JHCreditSheet = keep; }
});

// ---------------------------------------------------------------------------------------------------------------------
// The board on the chart (Khalid 2026-11-25: "now make sure this have an icon on chart.html")
// ---------------------------------------------------------------------------------------------------------------------
const crypto = require('node:crypto');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const CHART_ENGINE = fs.readFileSync(path.join(R, 'jh-chart-engine.js'), 'utf8');
const CHART_RAIL = fs.readFileSync(path.join(R, 'jh-chart-tvrail.js'), 'utf8');
const CHART_HTML = fs.readFileSync(path.join(R, 'chart.html'), 'utf8');
const chartLedger = require('./helpers/chart-macro-line-preservation.cjs');

test('the chart has a Credit button between Bonds and CDS, and it opens this board in the chart\'s workspace', () => {
  const iBnd = CHART_ENGINE.indexOf('id=btn-bnd '), mine = CHART_ENGINE.indexOf('id=btn-cbe '), iCds = CHART_ENGINE.indexOf('id=btn-cds ');
  assert.ok(iBnd > 0 && mine > iBnd && iCds > mine);
  assert.equal(CHART_ENGINE.split('id=btn-cbe ').length, 2, 'one button');
  assert.equal(CHART_ENGINE.slice(iBnd, mine).split('<button').length, 2, 'nothing but the Bonds button\'s own tail before it');
  assert.equal(CHART_ENGINE.slice(mine, iCds).split('<button').length, 2, 'and CDS is the next button');
  assert.ok(CHART_ENGINE.includes("<button class='wsico wsdesk' id=btn-cbe title='Credit Before Equity · company bond spreads, CDS, stress and the fall risk they carry'><span class=g>Cr</span><span class=l>Credit</span></button>"));
  assert.ok(CHART_ENGINE.includes('var bcbe=document.getElementById("btn-cbe"); if(bcbe) bcbe.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("credit"); };'));
  // every desk that was on the bar is still on it, once, in the order it had
  const ids = [...CHART_ENGINE.matchAll(/<button class='wsico wsdesk' id=(btn-[a-z0-9]+) /g)].map(m => m[1]);
  assert.deepEqual(ids.slice(0, 19), ['btn-macro', 'btn-flows', 'btn-mflow', 'btn-heat', 'btn-rmap', 'btn-auc', 'btn-corm', 'btn-etf', 'btn-str', 'btn-bnd', 'btn-cbe', 'btn-cds', 'btn-repo', 'btn-sov', 'btn-usd', 'btn-soma', 'btn-bofa', 'btn-13f', 'btn-ldr']);
  assert.equal(new Set(ids).size, ids.length);
  // the workspace entry: a name, this page, between Bonds and CDS; no other entry was touched
  const m = CHART_RAIL.match(/\n {6}credit: \["([^"]+)", "([^"]+)"\],\n/);
  assert.ok(m, 'the rail knows the workspace');
  assert.equal(m[2], '/credit-before-equity.html');
  assert.match(m[1], /^Credit Before Equity · /);
  assert.ok(fs.existsSync(path.join(R, m[2].slice(1))), 'the page it opens exists');
  assert.ok(CHART_RAIL.indexOf('      bonds: [') < CHART_RAIL.indexOf('      credit: [') && CHART_RAIL.indexOf('      credit: [') < CHART_RAIL.indexOf('      cds: ['));
  assert.ok(CHART_RAIL.includes('      cds: ["CDS · credit default swaps", "/cds.html"],'));
  // a browser holding the old engine or the old rail fetches the new one: both tags carry the mark, and the older marks are where they were
  const tags = CHART_HTML.match(/<script src="\/jh-chart-(?:engine|tvrail)\.js\?[^"]+"><\/script>/g);
  assert.equal(tags.length, 2);
  for (const t of tags) { assert.ok(t.includes('&cds=20261015&cbe=20261125&'), t.slice(0, 60)); assert.ok(t.includes('&repo=20261121&rmap=20261124&sov=20261122'), t.slice(0, 60)); assert.ok(t.endsWith('&ldr=20261105"></script>')); }
  assert.equal(CHART_HTML.split('&cbe=20261125').length, 3);
});

test('the chart\'s byte ledger takes the button back out: engine, rail and chart.html return to the files before it', () => {
  // (2026-11-26: the Money flow button came after it and is peeled first)
  const e0 = chartLedger.peelCreditBoardEngine(chartLedger.peelMoneyFlowEngine(chartLedger.peelChartTabsEngine(CHART_ENGINE, 'jh-chart-engine.js'), 'jh-chart-engine.js'), 'jh-chart-engine.js'), r0 = chartLedger.peelCreditBoardRail(chartLedger.peelMoneyFlowRail(chartLedger.peelChartTabsRail(CHART_RAIL, 'jh-chart-tvrail.js'), 'jh-chart-tvrail.js'), 'jh-chart-tvrail.js'), h0 = chartLedger.peelCreditBoardHtml(chartLedger.peelMoneyFlowHtml(chartLedger.peelChartTabsHtml(CHART_HTML, 'chart.html'), 'chart.html'), 'chart.html');
  assert.equal(sha(e0), 'b0d05901fb3d5da6ca799d18fc9dcfb7eda187d5e7bf9caa6cdea9b100135ee5');
  assert.equal(sha(r0), '75dbbbc868c849e03e4aa48158acd97371af51156b62ea1d648ce80540d793c6');
  assert.equal(sha(h0), '96f17815e4e76284763617ea0572bef73fa019e4198b5c3aa0959b528835f62f');
  assert.ok(!e0.includes('btn-cbe') && !r0.includes('credit-before-equity') && !h0.includes('&cbe='));
  // a file that is not the one after this change is handed back untouched (an older or a newer file is some other peel's business)
  assert.equal(chartLedger.peelCreditBoardEngine(e0, 'jh-chart-engine.js'), e0);
  assert.equal(chartLedger.peelCreditBoardRail(CHART_RAIL, 'jh-chart-engine.js'), CHART_RAIL);
  // and it is peeled before every older peel (only the Money flow button, added the day after, comes off before it), so every older gate still reads its own predecessor
  assert.match(fs.readFileSync(path.join(R, 'tests/helpers/chart-macro-line-preservation.cjs'), 'utf8'), /raw=peelMoneyFlowHtml\(raw,file\);raw=peelCreditBoardEngine\(raw,file\);raw=peelCreditBoardRail\(raw,file\);raw=peelCreditBoardHtml\(raw,file\);raw=peelRiskMapEngine\(raw,file\);/);
});

test('inside the chart\'s workspace a sheet\'s Chart link loads the company in the chart behind it; anywhere else it is a link', () => {
  const sheet = S.sheetHtml(DOC, 'T:ORCL', { palette: 'rg', bonds: 'loading', history: null });
  assert.match(sheet, /<a class="cb-sheet-a" data-t="ORCL" href="\/chart\.html\?symbol=ORCL">Chart<\/a>/);
  const loner = T.find(r => !r.ticker);
  assert.ok(!S.sheetHtml(DOC, loner.id, { bonds: 'loading', history: null }).includes('cb-sheet-a'), 'a company with no listed stock has no chart to open');
  // under the chart: the symbol goes to the chart's own opener (which closes the workspace) and the link's address is not followed
  const opened = [];
  const chart = { jhOpenSymbol: s => opened.push(s) };
  assert.equal(B.chartHandoff({ top: chart }, 'ORCL'), true);
  assert.equal(B.chartHandoff({ top: chart }, 'BRK.B'), true);
  assert.deepEqual(opened, ['ORCL', 'BRK.B']);
  // the page by itself, a frame under some other page, no symbol, a parent it may not read: the link stands
  const alone = {}; alone.top = alone; alone.jhOpenSymbol = () => { throw new Error('the page must not call itself'); };
  assert.equal(B.chartHandoff(alone, 'ORCL'), false);
  assert.equal(B.chartHandoff({ top: {} }, 'ORCL'), false);
  assert.equal(B.chartHandoff({ top: chart }, ''), false);
  assert.equal(B.chartHandoff({ get top() { throw new Error('another origin'); } }, 'ORCL'), false);
  assert.equal(B.chartHandoff(null, 'ORCL'), false);
  assert.deepEqual(opened, ['ORCL', 'BRK.B']);
  // the board's own click handler is where it is used: the default is cancelled only when the chart took the symbol
  assert.match(JS, /var link = ev\.target\.closest \? ev\.target\.closest\("a\.cb-sheet-a"\) : null;\n {6}if \(link && host\.contains\(link\)\) \{ if \(chartHandoff\(root, link\.getAttribute\("data-t"\)\)\) ev\.preventDefault\(\); return; \}/);
});
