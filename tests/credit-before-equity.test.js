'use strict';
// The Credit Before Equity page (credit-before-equity.html + jh-credit-board.js) on the document the runner built with engine
// 2.1.0 from a cut of the real warehouse (ops 6942, 2026-11-24).
// Khalid 2026-11-24: "expand it to cover as many companies as possible and it should have a heatmap that shows industries with
// biggest corp spread widening which signal trouble and ones with lowes which signals safety also ... sort them by industry and
// sort them ascending and descending and also add companies CDS credit default swaps".
// Khalid 2026-11-24, later: "ai companies and oracle are under enormous stress why arent they showing" - they were on the board
// and nothing pointed at them: a lead needs the stock NOT to have followed, and Oracle's stock already fell. Hence the credit
// stress view (it does not look at the stock), the AI stack theme, and the Stress column; nothing that was there was moved.
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
const doc20 = () => {
  const d = doc();
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

test('the fixture is the engine\'s 2.1 document: companies, industries, windows, CDS, the model names, stress and the AI stack', () => {
  assert.equal(DOC.contract, 'credit-before-equity.v2');
  assert.equal(DOC.version, '2.1.0');
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
  const heads = [...grouped.matchAll(/<tr class="cb-grp"><td colspan="16"><b>(.*?)<\/b>/g)].map(m => m[1]);
  const view = Object.fromEntries(B.industryView(DOC, { win: 'm1', book: 'all', measure: 'bp' }).map(r => [r.industry, r.d_bp]));
  assert.equal(heads.length, new Set(T.map(r => r.industry)).size);
  const order = heads.map(h => view[h.replace(/&amp;/g, '&')]).filter(v => v != null);
  assert.deepEqual(order, order.slice().sort((x, y) => y - x), 'groups run from the industry widening most to the one widening least');
  const flipped = [...B.tableHtml(DOC, state({ group: true, sort: { key: 'd', dir: 'asc' } })).matchAll(/<tr class="cb-grp"><td colspan="16"><b>(.*?)<\/b>/g)].map(m => view[m[1].replace(/&amp;/g, '&')]).filter(v => v != null);
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
  assert.deepEqual(order, ['s-glance', 's-industries', 's-stress', 's-leads', 's-companies', 's-cds', 's-method', 's-coverage', 's-gaps', 's-links']);
  assert.ok(HTML.includes('<h2 class="sec">Industries — where spreads are widening and where they are holding</h2>') && HTML.includes('<h2 class="sec">Leads — credit moved, the stock has not</h2>'));
  assert.deepEqual(B.COLUMNS.filter(c => c[0] !== 'stress').map(c => c[0]), ['name', 'industry', 'book', 'n_bonds', 'value', 'spread_bp', 'd', 'x_m1', 'pct_m1', 'd_ytd', 'cds_bp', 'cds_d1m', 'px_m1', 'direction', 'signal']);
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
    assert.equal(cells[4], B.bp(r.spread_bp));
    assert.equal(cells[5], (Math.round(r.spread_bp / DOC.stress.book_spread_bp[r.book] * 10) / 10).toFixed(1) + '×');
    assert.equal(cells[6], B.bp(r.d_ytd, true));
    assert.equal(cells[15], r.industry.replace(/&/g, '&amp;'));
  });
  assert.equal(got.length ? want[0].ticker : 'ORCL', 'ORCL', 'Oracle stands highest of the companies with $50M or more held');
  assert.match(html, /whatever the stock did/);
  assert.match(html, /The leads ask a different question: whether the stock has noticed\./);
  assert.match(html, /since Dec 31, 2025 as a share of where it started/);
  assert.match(html, /× book is the spread as a multiple of the middle company of its book \(IG [\d.]+ bp, HY [\d.]+ bp, EM [\d.]+ bp\)/);
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
  const wide = stressTable(B.stressHtml(DOC, state({ stHeld: 0, stLimit: 1e9, stSort: { key: 'pct_ytd', dir: 'desc' } }))).map(c => figure(c[7])).filter(v => v != null);
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
  got.forEach((c, i) => { assert.ok(c[0].includes(byStress[i].name.replace(/&/g, '&amp;')), c[0]); assert.equal(c[15], words[byStress[i].layer].replace(/&/g, '&amp;')); });
  assert.match(open, /data-st-theme="ai" aria-pressed="true">AI stack</);
  assert.match(open, /data-st-sort="grp">Layer</);
  assert.match(open, /<select data-st-layersel><option value="">All layers<\/option>/);
  const one = stressTable(B.stressHtml(DOC, state({ stTheme: 'ai', stLayer: 'hyperscalers', stHeld: 0, stLimit: 100 })));
  assert.deepEqual(one.map(c => c[15]), mine.filter(r => r.layer === 'hyperscalers').map(() => 'Hyperscalers'));
  assert.ok(one.some(c => c[0].includes('Oracle')));
  // in the list of every company, a member of the theme is tagged
  assert.match(B.stressHtml(DOC, state({ stHeld: 0, stLimit: 1e9 })), /Oracle Corporation <span class="cb-tag">AI stack<\/span>/);
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
  assert.deepEqual([m.scale.lo, m.scale.hi], [0, 100]);
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
  assert.match(method, /3M – not available yet \(The board&#39;s ledger holds no day 91 days back yet, give or take 21; one will be from about 2027-01-08\.\)/);
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
