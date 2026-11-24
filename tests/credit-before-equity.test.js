'use strict';
// The Credit Before Equity page (credit-before-equity.html + jh-credit-board.js) on the document the runner built with engine
// 2.0.0 from a cut of the real warehouse (ops 6942, 2026-11-24).
// Khalid 2026-11-24: "expand it to cover as many companies as possible and it should have a heatmap that shows industries with
// biggest corp spread widening which signal trouble and ones with lowes which signals safety also ... sort them by industry and
// sort them ascending and descending and also add companies CDS credit default swaps".
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

test('the fixture is the engine\'s 2.0 document: companies, industries, windows, CDS, the model names', () => {
  assert.equal(DOC.contract, 'credit-before-equity.v2');
  assert.ok(B.isV2(DOC));
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
  assert.equal(e.checks.length, 8);
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
  const heads = [...grouped.matchAll(/<tr class="cb-grp"><td colspan="15"><b>(.*?)<\/b>/g)].map(m => m[1]);
  const view = Object.fromEntries(B.industryView(DOC, { win: 'm1', book: 'all', measure: 'bp' }).map(r => [r.industry, r.d_bp]));
  assert.equal(heads.length, new Set(T.map(r => r.industry)).size);
  const order = heads.map(h => view[h.replace(/&amp;/g, '&')]).filter(v => v != null);
  assert.deepEqual(order, order.slice().sort((x, y) => y - x), 'groups run from the industry widening most to the one widening least');
  const flipped = [...B.tableHtml(DOC, state({ group: true, sort: { key: 'd', dir: 'asc' } })).matchAll(/<tr class="cb-grp"><td colspan="15"><b>(.*?)<\/b>/g)].map(m => view[m[1].replace(/&amp;/g, '&')]).filter(v => v != null);
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
  assert.match(m.html, /Tile size: market value of the industry&#39;s bonds held|Tile size: market value of the industry's bonds held/);
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
  for (const id of ['board', 'evidence', 'hero', 'controls', 'map', 'rank', 'mapdetail', 'leads', 'table', 'cds', 'method', 'coverage', 'gaps', 'newview', 'prevview', 'prev', 'viewtoggle', 'foot']) {
    assert.ok(HTML.includes('id="' + id + '"'), id);
  }
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
