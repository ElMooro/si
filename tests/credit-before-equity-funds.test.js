'use strict';
// Credit Before Equity, held by funds (jh-credit-funds.js; the section "Held by funds" and a company's sheet) on the document
// the engine builds on the real warehouse with the fund layer, and a cut of its fund document (ops 6990, 2026-11-27).
// Khalid 2026-11-26: "go" - who holds each bond, how far apart funds mark it, and how funds are positioned in each company's
// credit default swaps.
// The page measures nothing; these tests hold what it draws to the two documents: every list opens whole and sorts on every
// column both ways, a figure the engine did not publish is never shown as zero, and a dealer's name written where a
// reference belongs is tied to no company.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const R = path.resolve(__dirname, '..');
const B = (require(path.join(R, 'jh-credit-board.js')), globalThis.JHCreditBoard);
const S = (require(path.join(R, 'jh-credit-sheet.js')), globalThis.JHCreditSheet);
const F = (require(path.join(R, 'jh-credit-funds.js')), globalThis.JHCreditFunds);
const FIX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(R, 'tests/fixtures/credit-before-equity-funds-page.json.gz'))));
const OLD = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(R, 'tests/fixtures/credit-before-equity-warehouse.json.gz')))).expected;   // the board with no fund layer
const HTML = fs.readFileSync(path.join(R, 'credit-before-equity.html'), 'utf8');
const BOARD_JS = fs.readFileSync(path.join(R, 'jh-credit-board.js'), 'utf8');
const SHEET_JS = fs.readFileSync(path.join(R, 'jh-credit-sheet.js'), 'utf8');
const FUNDS_JS = fs.readFileSync(path.join(R, 'jh-credit-funds.js'), 'utf8');
const clone = o => JSON.parse(JSON.stringify(o));
const DOC = FIX.doc, FD = FIX.funds, BLOCK = DOC.funds, CDS = BLOCK.cds;
const state = over => Object.assign(B.firstState(DOC, {}), over || {});
const text = html => html.replace(/<[^>]+>/g, ' ').replace(/&#39;/g, "'").replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim();
const bodyRows = html => [...html.slice(html.indexOf('<tbody>'), html.indexOf('</tbody>')).matchAll(/<tr>(.*?)<\/tr>/g)].map(m => [...m[1].matchAll(/<td[^>]*>(.*?)<\/td>/g)].map(c => text(c[1])));
const row = id => S.rowOf(DOC, id);
const api = (st, fd) => { const log = { draws: 0, needs: [], files: [] }; return Object.assign(log, { doc: DOC, funds: fd, need: w => log.needs.push(w), draw: () => { log.draws++; }, drawSheet: () => { log.draws++; }, download: (n, t) => log.files.push([n, t]) }); };
const clean = html => { assert.ok(!/undefined|NaN|\[object/.test(html), 'something unreadable was drawn'); return html; };

test('money that may be nothing or below zero is written as it is', () => {
  assert.deepEqual([2501086600929, 3739379770, 552000000, 5533849, 950, 0, -35800000, null].map(v => F.usd(v)), ['$2.50T', '$3.7B', '$552M', '$6M', '$950', '$0', '−$36M', '–']);
  assert.equal(F.usd(549924000, true), '+$550M');
  assert.equal(F.usd(-20000000, true), '−$20M');
  assert.equal(S.usd(0), '$0');
  assert.deepEqual([9.25, 7.214, 0, null].map(F.coupon), ['9.25%', '7.214%', '0%', '–']);
});

test('the section says what the engine published: the file, its day, and the five counts', () => {
  const html = clean(F.sectionHtml(DOC, state(), null)), t = text(html);
  assert.ok(t.includes('SEC Form N-PORT, file 2026q3_nport') && t.includes('holdings as of ' + B.day(BLOCK.as_of)) && t.includes(BLOCK.quarter.summary.funds.toLocaleString('en-US') + ' funds'));
  assert.ok(t.includes("the board's own bonds are of " + B.day(DOC.as_of)), 'the fund layer is months older than the board, and the page says both days');
  const tiles = [...html.matchAll(/<div class="cb-tile-h"><div class="cb-lbl">(.*?)<\/div><div class="cb-big">(.*?)<\/div><div class="cb-sub">(.*?)<\/div>/g)].map(m => [text(m[1]), text(m[2]), text(m[3])]);
  assert.equal(tiles.length, 5);
  assert.deepEqual(tiles[0].slice(0, 2), ['Bonds and loans tied to a company', BLOCK.tied.securities.toLocaleString('en-US')]);
  assert.ok(tiles[0][2].includes(F.usd(BLOCK.tied.value)) && tiles[0][2].includes(BLOCK.tied.value_pct.toFixed(1) + '% of the ' + F.usd(BLOCK.read.value)));
  assert.deepEqual(tiles[1].slice(0, 2), ['Companies on the board funds hold', BLOCK.tied.companies.toLocaleString('en-US')]);
  assert.ok(tiles[1][2].includes('of ' + DOC.coverage.companies.toLocaleString('en-US')) && tiles[1][2].includes(BLOCK.tied.listed.toLocaleString('en-US') + ' with a listed stock'));
  const five = BLOCK.gaps.filter(g => g[BLOCK.gap_cols.indexOf('gap')] >= 5).length;
  assert.ok(five >= 1 && five < BLOCK.gaps.length, 'the first hundred gaps reach under five points, so the count of five or more is whole');
  assert.equal(tiles[2][1], String(five));
  assert.equal(tiles[3][1], String(BLOCK.n_flagged));
  assert.equal(tiles[4][1], F.usd(CDS.totals.notional_usd));
  assert.ok(tiles[4][2].includes(CDS.totals.reference_names + ' names, ' + CDS.tied + ' tied to a company') && tiles[4][2].includes(CDS.totals.indices + ' indices'));
  assert.ok(t.includes(BLOCK.reading) && t.includes('Source: ' + BLOCK.source));
});

test('with no fund layer the section says so in the engine\'s own words, and shows no zero', () => {
  const none = clone(OLD);
  assert.ok(!('funds' in none));
  assert.match(text(F.sectionHtml(none, B.firstState(none, {}), null)), /^The engine published no fund layer on this run\. Nothing is shown rather than something stale\.$/);
  none.funds = null;
  none.degraded = (none.degraded || []).concat(['The fund holdings bank has read no quarter yet: the fund columns are empty on this run.']);
  const html = F.sectionHtml(none, B.firstState(none, {}), null);
  assert.equal(text(html), 'The fund holdings bank has read no quarter yet: the fund columns are empty on this run.');
  assert.ok(!/\$0|>0</.test(html));
  assert.ok(!S.sheetHtml(none, S.table ? none.issuers.rows[0][0] : '', { bonds: 'loading', history: null, funds: 'unpublished' }).includes('sheet-funds'), 'a board with no fund layer keeps its sheet as it was');
});

test('every list opens whole: the pager shows the rest, then all of it', () => {
  const sizes = { flagged: BLOCK.n_flagged, names: CDS.names.length, indices: CDS.indices.length, dealers: CDS.counterparties.length, ties_by_rule: FD.ties_by_rule.length, not_on_board: FD.not_on_board.length, untied: FD.untied.length };
  assert.ok(sizes.names === 755 && sizes.flagged > 100 && sizes.ties_by_rule === BLOCK.n_ties_by_rule && sizes.not_on_board === BLOCK.n_not_on_board && sizes.ties_by_rule > 500);
  for (const [id, n] of Object.entries(sizes)) {
    const st = state();
    let html = clean(F.listHtml(id, DOC, FD, st));
    assert.equal(bodyRows(html).length, Math.min(F.FIRST, n), id);
    if (n > F.FIRST) {
      assert.ok(html.includes('data-fu-more="' + id + '"') && html.includes('data-fu-all="' + id + '">Show all ' + n.toLocaleString('en-US')), id);
      const a = api(st, FD);
      assert.equal(F.click({ fuMore: id }, st, a), true);
      assert.equal(bodyRows(F.listHtml(id, DOC, FD, st)).length, Math.min(F.FIRST + F.STEP, n), id);
      F.click({ fuAll: id }, st, a);
      html = F.listHtml(id, DOC, FD, st);
      assert.ok(!html.includes('data-fu-all'), id);
    }
    assert.equal(bodyRows(html).length, n, id + ' is not whole');
    assert.ok(text(html).includes(n.toLocaleString('en-US') + ' of ' + n.toLocaleString('en-US') + ' shown'), id);
  }
});

test('every column of every list sorts both ways, and the header says which way', () => {
  for (const id of Object.keys(F.LISTS)) {
    for (const c of F.LISTS[id].cols) {
      const st = state(), a = api(st, FD);
      F.state(st).shown[id] = 1e9;
      F.state(st).sorts[id] = { key: '(none)', dir: 'desc' };
      F.click({ fuSort: id + ':' + c[0] }, st, a);
      const first = Object.assign({}, F.state(st).sorts[id]);
      assert.equal(first.key, c[0]);
      assert.equal(first.dir, ['text', 'co', 'how', 'kind', 'date', 'year'].includes(c[2]) ? 'asc' : 'desc', id + '.' + c[0] + ': words open A to Z, figures largest first');
      const at = F.LISTS[id].cols.indexOf(c), numeric = !['text', 'co', 'how', 'kind', 'date', 'year'].includes(c[2]);
      const read = () => {
        const html = F.listHtml(id, DOC, FD, st);
        assert.ok(html.includes('aria-sort="' + (F.state(st).sorts[id].dir === 'asc' ? 'ascending' : 'descending') + '"><button type="button" data-fu-sort="' + id + ':' + c[0] + '">'), id + '.' + c[0]);
        return F.listRows(id, DOC, FD, F.state(st)).rows.length ? bodyRows(html).map(r => r[at]) : [];
      };
      const one = read();
      F.click({ fuSort: id + ':' + c[0] }, st, a);
      assert.equal(F.state(st).sorts[id].dir, first.dir === 'asc' ? 'desc' : 'asc', id + '.' + c[0] + ' did not turn round');
      const two = read();
      assert.equal(one.length, two.length);
      if (numeric) {
        // the same figures, largest first then smallest first; what is not published stays at the foot both ways
        const rows = F.listRows(id, DOC, FD, F.state(st)).rows, vals = rows.map(x => x[c[0]]).filter(v => typeof v === 'number');
        const desc = first.dir === 'desc' ? one : two, asc = first.dir === 'desc' ? two : one;
        if (vals.length) {
          assert.equal(desc[0], F.cellText(c[2], Math.max(...vals)), id + '.' + c[0] + ' largest first');
          assert.equal(asc[0], F.cellText(c[2], Math.min(...vals)), id + '.' + c[0] + ' smallest first');
        }
        if (vals.length < rows.length) assert.ok(desc[desc.length - 1] === '–' && asc[asc.length - 1] === '–', id + '.' + c[0] + ': an unpublished figure is not a small one');
      } else if (new Set(one).size > 1) assert.notDeepEqual(one, two, id + '.' + c[0]);
      assert.equal(a.draws, 2);
    }
  }
});

test('marks apart: the first hundred until the fund document arrives, then every security enough funds mark', () => {
  const st = state();
  const before = F.listRows('gaps', DOC, null, F.state(st));
  assert.deepEqual([before.rows.length, before.total, before.whole], [BLOCK.gaps.length, BLOCK.n_gaps, false]);
  assert.ok(BLOCK.n_gaps > 10000 && BLOCK.gaps.length === 100);
  const waiting = F.listHtml('gaps', DOC, null, st);
  assert.ok(text(waiting).includes('These are the first 100 of ' + BLOCK.n_gaps.toLocaleString('en-US') + '; the rest is loading.') && waiting.includes('data-fu-load'));
  const failed = text(F.listHtml('gaps', DOC, false, st));
  assert.ok(failed.includes('The fund document could not be loaded: these are the first 100 of ' + BLOCK.n_gaps.toLocaleString('en-US')) && !failed.includes('is loading'));
  const after = F.listRows('gaps', DOC, FD, F.state(st)), at = k => FD.cols.indexOf(k);
  const want = FD.rows.filter(r => r[at('n_px')] >= FD.min_marks && r[at('px_lo')] != null && r[at('px_hi')] != null);
  assert.ok(after.whole && after.rows.length === want.length && want.length > 300 && want.length < FD.rows.length);
  for (const x of after.rows) assert.equal(x.gap, Math.round((x.px_hi - x.px_lo) * 100) / 100);
  assert.equal(F.gapOf({ px_lo: 40, px_hi: 55, n_px: FD.min_marks - 1 }, FD.min_marks), null, 'a gap is not read off fewer funds than the engine asks for');
  // the widest first, as published; the choice of size keeps only what is worth that much
  assert.equal(bodyRows(F.listHtml('gaps', DOC, null, st))[0][9], F.cellText('mark', BLOCK.gaps[0][BLOCK.gap_cols.indexOf('gap')]));
  const a = api(st, FD);
  F.click({ fuHeld: String(250e6) }, st, a);
  const big = F.listRows('gaps', DOC, FD, F.state(st));
  assert.ok(big.filtered && big.rows.length && big.rows.length < after.rows.length && big.rows.every(x => x.value >= 250e6) && a.needs.includes('funds'));
  assert.ok(text(F.sectionHtml(DOC, st, FD)).includes('among those worth $250m or more'));
});

test('swap names: all of them, a company only where the name is one, and a dealer\'s name tied to none', () => {
  const st = state();
  F.state(st).view = 'names'; F.state(st).shown.names = 1e9;
  const html = clean(F.sectionHtml(DOC, st, FD)), rows = F.listRows('names', DOC, FD, F.state(st)).rows;
  assert.equal(rows.length, CDS.names.length);
  const at = k => CDS.cols.indexOf(k);
  const dealers = CDS.names.filter(n => n[at('dealer')]);
  assert.ok(dealers.length === CDS.dealer_names_not_tied && dealers.length >= 5 && dealers.every(n => n[at('co')] === null), 'no dealer-named reference enters a company');
  assert.ok(html.includes("also a dealer's name: tied to none") && text(html).includes('A name funds also write as a dealer is tied to no company'));
  const gs = rows.find(x => x.reference === 'GOLDMAN SACHS & CO');
  assert.ok(gs && gs.dealer === 1 && gs.co === null && gs.sold > 9e8);
  for (const x of rows) assert.equal(x.gross, (x.sold || 0) + (x.bought || 0) + (x.unclear || 0));
  const orcl = rows.find(x => x.reference === 'ORACLE CORP');
  assert.ok(orcl.co === 'T:ORCL' && html.includes('data-sheet="T:ORCL"'), 'a tied name opens its company');
  F.click({ fuTied: '' }, st, api(st, FD));
  const tied = F.listRows('names', DOC, FD, F.state(st));
  assert.ok(tied.filtered && tied.rows.length === CDS.tied && tied.rows.every(x => x.co) && tied.total === CDS.names.length);
  assert.ok(text(F.sectionHtml(DOC, st, FD)).includes('(' + CDS.names.length + ' before the choice above)'));
});

test('protection sold and bought, company by company: the fourteen with the most written, in the chosen colours', () => {
  const T = B.table(DOC), gross = r => (r.fc_sold || 0) + (r.fc_bought || 0);
  const want = T.filter(r => gross(r) > 0).sort((a, b) => gross(b) - gross(a)).slice(0, 14);
  for (const palette of ['rg', 'cb']) {
    const html = clean(F.butterflyHtml(DOC, state({ palette }))), p = B.PALETTES[palette];
    const rows = [...html.matchAll(/<button type="button" class="cb-fu-row cb-fu-bf" role="listitem" data-sheet="([^"]+)" title="([^"]+)">(.*?)<\/button>/g)];
    assert.deepEqual(rows.map(m => m[1]), want.map(r => r.id));
    const max = Math.max(...want.map(r => Math.max(r.fc_sold || 0, r.fc_bought || 0)));
    rows.forEach((m, i) => {
      const w = [...m[3].matchAll(/style="width:([\d.]+)%;background:(#[0-9a-f]{6})"/g)].map(x => [Number(x[1]), x[2]]);
      assert.deepEqual(w.map(x => x[1]), [p.bad, p.good], 'bought in the colour of trouble, sold in the colour of safety');
      assert.ok(Math.abs(w[0][0] - 100 * (want[i].fc_bought || 0) / max) < 0.06 && Math.abs(w[1][0] - 100 * (want[i].fc_sold || 0) / max) < 0.06);
      assert.ok(m[2].includes('funds sold ' + F.usd(want[i].fc_sold || 0) + ' of protection and bought ' + F.usd(want[i].fc_bought || 0)));
    });
    assert.ok(html.includes('<i style="background:' + p.bad + '"></i>Bought: the fund pays to be protected') && html.includes('<i style="background:' + p.good + '"></i>Sold: the fund carries the credit risk'), 'both colours are named');
  }
  const all = T.filter(r => gross(r) > 0);
  assert.ok(text(F.butterflyHtml(DOC, state())).includes('net sellers of protection on ' + all.filter(r => r.fc_net > 0).length + ' and net buyers on ' + all.filter(r => r.fc_net < 0).length));
  const none = clone(DOC);
  none.issuers.rows.forEach(r => { for (const k of ['fc_sold', 'fc_bought', 'fc_net', 'fc_funds']) r[none.issuers.cols.indexOf(k)] = null; });
  assert.equal(text(F.butterflyHtml(none, B.firstState(none, {}))), 'No credit default swap in the fund file is tied to a company on the board.');
});

test('a security\'s marks are drawn as they stand: from the 10th to the 90th percentile, about its own middle mark', () => {
  const rows = F.gapRows(DOC, null).rows.slice(0, 12);
  const html = clean(F.rangesHtml(rows, DOC)), span = Math.max(...rows.map(x => Math.max(Math.abs(x.px_lo - x.px), Math.abs(x.px_hi - x.px))));
  const drawn = [...html.matchAll(/data-sheet="([^"]+)" class="cb-fu-row cb-fu-rg" role="listitem" title="([^"]+)">.*?<i style="left:([\d.]+)%;width:([\d.]+)%"><\/i><b><\/b><\/span><span class="cb-fu-v">([^<]+)</g)];
  assert.equal(drawn.length, 12);
  drawn.forEach((m, i) => {
    const x = rows[i], left = 50 + 50 * (x.px_lo - x.px) / span, width = Math.max(50 * (x.px_hi - x.px_lo) / span, 0.8);
    assert.equal(m[1], x.co);
    assert.ok(Math.abs(Number(m[3]) - left) < 0.06 && Math.abs(Number(m[4]) - width) < 0.06, 'the bar is where the marks are');
    assert.equal(m[5], F.cellText('mark', x.gap));
    assert.ok(m[2].includes(x.n_px + ' funds mark it') && m[2].includes(F.usd(x.value) + ' held'));
  });
  assert.ok(html.includes('<i>middle mark</i>') && html.includes(' pts</i>'), 'the scale is labelled');
  assert.equal(text(F.rangesHtml([], DOC, { empty: 'None here.' })), 'None here.');
});

test('a company\'s sheet: what funds hold of it, bond by bond, as the two documents have it', () => {
  const r = row('T:CRWV'), mine = F.rowsOf(FD, 'T:CRWV', DOC);
  assert.ok(r && mine.length === r.f_n && r.f_n === 20);
  assert.equal(mine.reduce((s, x) => s + x.value, 0), r.f_value, 'the column and the rows agree to the dollar');
  assert.equal(Math.max(...mine.map(x => x.funds)), r.f_funds);
  assert.equal(mine.filter(x => x.cls === 'LON').length, r.f_loans);
  const st = state({ sheet: 'T:CRWV' });
  const html = clean(F.sheetHtml(DOC, r, { funds: mine, fundSort: { key: 'value', dir: 'desc' } })), t = text(html);
  const kpis = Object.fromEntries([...html.matchAll(/<div class="cb-kpi"><div class="cb-lbl">(.*?)<\/div><div class="cb-big">(.*?)<\/div><div class="cb-sub">(.*?)<\/div>/g)].map(m => [text(m[1]), [text(m[2]), text(m[3])]]));
  assert.deepEqual(kpis['Bonds and loans funds hold'], ['20', '7 loans among them · 333 funds in the most widely held']);
  assert.deepEqual(kpis["Held, at funds' own marks"], [F.usd(r.f_value), 'face ' + F.usd(r.f_face)]);
  assert.equal(kpis['Middle mark'][0], r.f_px.toFixed(2));
  assert.equal(kpis['Widest gap between marks'][0], r.f_gap.toFixed(2) + ' pts');
  assert.equal(kpis['Flagged by most holders'][1], r.f_def + ' in default · ' + r.f_pik + ' paid in kind');
  const table = bodyRows(html.slice(html.indexOf('cb-bonds')));
  assert.equal(table.length, mine.length, 'every fund-held security of the company is listed');
  const col = k => F.ROW_FIELDS.findIndex(c => c[0] === k);
  assert.deepEqual(table.map(x => x[col('value')]), mine.slice().sort((a, b) => b.value - a.value).map(x => F.usd(x.value)), 'largest first');
  assert.ok(table.some(x => x[col('cls')] === 'Loan') && table.some(x => x[col('cls')] === 'Bond'));
  assert.ok(t.includes('20 securities, ' + F.usd(r.f_value) + ' held. The identifiers are not republished.') && html.includes('data-csv="funds"'));
  assert.ok(!F.ROW_FIELDS.concat(...Object.values(F.LISTS).map(l => l.cols)).some(c => /cusip|isin|figi/i.test(c[0] + c[1])), 'no identifier has a column');
  // the header turns the table round, and the picture is of its own securities
  const a = api(st, FD);
  assert.equal(F.click({ fundSort: 'funds' }, st, a), true);
  assert.deepEqual(st.fundSort, { key: 'funds', dir: 'desc' });
  F.click({ fundSort: 'funds' }, st, a);
  assert.equal(st.fundSort.dir, 'asc');
  const asc = bodyRows(F.sheetHtml(DOC, r, { funds: mine, fundSort: st.fundSort }).split('cb-bonds')[1]).map(x => Number(x[col('funds')].replace(/,/g, '')));
  assert.deepEqual(asc, asc.slice().sort((x, y) => x - y));
  assert.ok(html.includes('How far apart funds mark it') && (html.match(/class="cb-fu-row cb-fu-rg"/g) || []).length === Math.min(10, mine.filter(x => x.gap !== null).length));
  // saved as a file: the company's rows, every column
  F.click({ csv: 'funds' }, st, a);
  const [name, csv] = a.files[0];
  assert.match(name, /^credit-before-equity-funds-T-CRWV-2026-06-30\.csv$/);
  assert.equal(csv.trim().split('\r\n').length, mine.length + 1);
  assert.ok(csv.startsWith('co,title,issuer,cls,cpn,mat,funds,rows,face,value,px,px_lo,px_hi,gap,'));
});

test('a company\'s sheet while its rows load, when they cannot be had, and when funds hold nothing of it', () => {
  const r = row('T:ORCL');
  const loading = text(F.sheetHtml(DOC, r, { funds: 'loading' }));
  assert.ok(loading.includes('Loading what funds hold of it.') && loading.includes(F.usd(r.f_value)), 'its counts are in the board\'s own document, so they are shown at once');
  assert.ok(text(F.sheetHtml(DOC, r, { funds: 'error' })).includes("The fund document could not be loaded. The figures above are from the board's own document."));
  assert.ok(text(F.sheetHtml(DOC, r, { funds: [] })).includes('The fund document lists nothing under this company.'));
  const T = B.table(DOC), bare = T.find(x => x.f_n == null && x.fc_funds == null), swapOnly = T.find(x => x.f_n == null && x.fc_funds);
  assert.ok(bare && swapOnly);
  const none = F.sheetHtml(DOC, bare, { funds: [] });
  assert.ok(text(none).endsWith('No bond or loan in the fund file is tied to this company, and no credit default swap is written on its name.') && !none.includes('cb-kpi') && !/\$0/.test(none));
  const only = text(F.sheetHtml(DOC, swapOnly, { funds: [] }));
  assert.ok(only.includes('No bond or loan in the fund file is tied to this company.') && only.includes('Credit default swaps funds hold on its name') && !only.includes('None in the fund file'));
  assert.ok(text(F.sheetHtml(DOC, row('I:MERIDIAN ARC HOLDCO'), { funds: F.rowsOf(FD, 'I:MERIDIAN ARC HOLDCO', DOC) })).endsWith('None in the fund file.'), 'a company no swap is written on says so');
  assert.equal(F.rowsOf(null, 'T:ORCL', DOC), null);
});

test('a company\'s sheet: the credit default swaps funds hold on its name, and which way they lean', () => {
  const r = row('T:ORCL'), names = CDS.names.filter(n => n[CDS.cols.indexOf('co')] === 'T:ORCL');
  assert.equal(names.length, 2);
  const html = clean(F.sheetHtml(DOC, r, { funds: 'loading' })), t = text(html);
  assert.ok(t.includes('Funds have sold ' + F.usd(r.fc_sold) + ' of protection on it and bought ' + F.usd(r.fc_bought) + ': on balance they carry its credit risk.'));
  const rows = bodyRows(html.slice(html.indexOf('sheet-fund-swaps')));
  assert.deepEqual(rows.map(x => x[0]), ['ORACLE CORP', 'ORACLE CORPORATION']);
  assert.deepEqual(rows[0].slice(1, 6), ['77', '56', '$552M', '$13M', '+$538M']);
  // a swap whose side the file does not make clear is not counted as sold or bought, and is not called nothing
  const echo = row('T:ECHO'), at = k => CDS.cols.indexOf(k);
  assert.deepEqual([echo.fc_sold, echo.fc_bought, echo.fc_funds], [0, 0, 1]);
  assert.ok(text(F.sheetHtml(DOC, echo, { funds: 'loading' })).includes('The file does not make clear which side funds are on for the $396K written on it, so nothing is counted as sold or bought.'));
  // positions in another currency are counted and never added up
  const T = B.table(DOC), euro = CDS.names.filter(n => n[at('co')] && n[at('not_in_dollars')] > 0 && T.some(x => x.id === n[at('co')]))[0];
  assert.ok(euro, 'the cut holds a tied name with positions outside dollars');
  const all = CDS.names.filter(n => n[at('co')] === euro[at('co')]), other = all.reduce((s, n) => s + n[at('not_in_dollars')], 0);
  assert.ok(text(F.sheetHtml(DOC, row(euro[at('co')]), { funds: 'loading' })).includes(other + (other === 1 ? ' position is' : ' positions are') + ' in another currency and not added up.'));
  const buyer = T.filter(x => x.fc_bought > (x.fc_sold || 0) * 1.1).sort((a, b) => b.fc_bought - a.fc_bought)[0];
  assert.ok(text(F.sheetHtml(DOC, buyer, { funds: 'loading' })).includes('on balance they are paying to be protected against it.'));
});

test('the sheet the board opens carries the section, and every fund column has its words', () => {
  const html = S.sheetHtml(DOC, 'T:CRWV', { bonds: 'loading', history: null, funds: F.rowsOf(FD, 'T:CRWV', DOC), fundSort: { key: 'value', dir: 'desc' } });
  const order = [...html.matchAll(/<h4 id="(sheet-[a-z-]+)"/g)].map(m => m[1]);
  assert.deepEqual(order.filter(x => x !== 'sheet-stock' && x !== 'sheet-cds'), ['sheet-next', 'sheet-history', 'sheet-bonds', 'sheet-funds', 'sheet-fund-swaps', 'sheet-figures'], 'what funds hold sits after its bonds; nothing that was there moved');
  const fund = DOC.issuers.cols.slice(69);
  assert.deepEqual(fund, ['f_n', 'f_funds', 'f_face', 'f_value', 'f_px', 'f_gap', 'f_def', 'f_pik', 'f_loans', 'fc_sold', 'fc_bought', 'fc_net', 'fc_funds']);
  for (const k of fund) {
    assert.ok(S.FIELD[k] && S.GROUPS[S.FIELD[k][3]] === 'Held by funds' && S.FIELD[k][4].length > 30, k + ' has no words');
  }
  assert.ok(S.allColumns(DOC).length === 82 && S.allColumns(DOC).every(f => f[2] !== 'raw'));
  const r = row('T:ORCL'), shown = k => S.shown(S.FIELD[k], r[k], { doc: DOC }).text;
  assert.deepEqual(['f_n', 'f_value', 'f_px', 'f_gap', 'fc_sold', 'fc_bought', 'fc_net'].map(shown), ['58', '$26.1B', '91.46', '0.43', '$563M', '$13M', '+$550M']);
  assert.equal(S.shown(S.FIELD.fc_bought, 0, {}).text, '$0', 'nothing bought is $0, and not published is a dash');
  assert.equal(S.shown(S.FIELD.fc_bought, null, {}).text, '–');
  assert.ok(S.figuresHtml(DOC, r, { doc: DOC }).includes('<tr class="cb-grp"><td colspan="3">Held by funds</td></tr>'));
  // the whole document view: the key has its words, and the fund document is among the files
  assert.ok(S.DOC_KEYS.funds && S.DOC_KEYS.funds[1].includes('Held by funds'));
  const data = S.dataHtml(DOC, state());
  assert.ok(data.includes('href="https://justhodl.ai/data/credit-before-equity-funds.json"') && data.includes('<summary data-jh-title="funds"><b>funds</b>'));
  assert.equal(BLOCK.key, 'data/credit-before-equity-funds.json');
});

test('how names were tied: the rules with their counts, and the three lists', () => {
  const st = state();
  F.state(st).view = 'ties';
  const html = clean(F.sectionHtml(DOC, st, FD)), t = text(html);
  for (const [k, v] of Object.entries(BLOCK.tied.how)) assert.ok(t.includes(v[0].toLocaleString('en-US') + ' securities, ' + F.usd(v[1])), k);
  assert.ok(t.includes('Tied by a naming rule — ' + BLOCK.n_ties_by_rule + ' issuer names') && t.includes('does not cover — ' + BLOCK.n_not_on_board) && t.includes('Issuers left untied — ' + BLOCK.n_untied.toLocaleString('en-US')));
  assert.ok(t.includes('Abbreviations spelled out') && t.includes('A name more than one company answers to is tied to none.'));
  const rules = new Set(FD.ties_by_rule.map(x => x[1]));
  for (const k of rules) assert.ok(F.HOW_WORDS[k], 'the rule ' + k + ' has no words on the page');
  // until the fund document arrives: the first hundred of each, and the page says how many there are
  const early = F.listRows('untied', DOC, null, F.state(st));
  assert.deepEqual([early.rows.length, early.total, early.whole], [100, BLOCK.n_untied, false]);
  assert.equal(F.listRows('ties_by_rule', DOC, FD, F.state(st)).rows.length, BLOCK.n_ties_by_rule);
});

test('a list saves as a file: every row of it, in the order shown', () => {
  const st = state(), a = api(st, FD);
  F.state(st).sorts.names = { key: 'sold', dir: 'desc' };
  assert.equal(F.click({ fuCsv: 'names' }, st, a), true);
  const [name, csv] = a.files[0], lines = csv.trim().split('\r\n');
  assert.equal(name, 'credit-before-equity-funds-names-2026-06-30.csv');
  assert.equal(lines.length, CDS.names.length + 1);
  assert.equal(lines[0], 'reference,co,positions,funds,sold,bought,net_sold,unclear,gross,not_in_dollars,usual_end_year,ticker,dealer');
  const sold = CDS.names.map(n => n[CDS.cols.indexOf('sold')]).sort((x, y) => y - x);
  assert.equal(Number(lines[1].split(',')[4]), sold[0]);
  F.click({ fuCsv: 'not_on_board' }, st, a);
  assert.equal(a.files[1][0], 'credit-before-equity-funds-not-on-board-2026-06-30.csv');
  assert.equal(a.files[1][1].trim().split('\r\n').length, FD.not_on_board.length + 1);
});

test('clicks that are not this file\'s are left alone; its own ask for the fund document when they need it', () => {
  const st = state(), a = api(st, null);
  assert.equal(F.click({ sheet: 'T:ORCL' }, st, a), false);
  assert.equal(F.click({ csv: 'bonds' }, st, a), false);
  assert.equal(F.click({ csv: 'funds' }, st, a), false, 'nothing to save before the fund document is here');
  assert.equal(F.click({ fuView: 'names' }, st, a), true);
  assert.equal(F.state(st).view, 'names');
  assert.equal(F.click({ fuView: 'nonsense' }, st, a), true);
  assert.equal(F.state(st).view, 'gaps');
  assert.equal(F.click({ fuLoad: '' }, st, a), true);
  assert.ok(a.needs.length >= 3 && a.needs.every(w => w === 'funds'));
  for (const v of F.VIEWS) { F.state(st).view = v; clean(F.sectionHtml(DOC, st, null)); clean(F.sectionHtml(DOC, st, FD)); clean(F.sectionHtml(DOC, st, false)); }
});

test('the page loads the three scripts in order and gives the section its place', () => {
  const tags = [...HTML.matchAll(/<script src="\/(jh-credit-[a-z]+)\.js\?v=(\d+[a-z])"><\/script>/g)].map(m => [m[1], m[2]]);
  assert.deepEqual(tags.map(t => t[0]), ['jh-credit-board', 'jh-credit-sheet', 'jh-credit-funds']);
  assert.equal(new Set(tags.map(t => t[1])).size, 1, 'one version for the three, so a cached script never meets a newer neighbour');
  assert.match(HTML, /<section id="s-funds">\s*<h2 class="sec">Held by funds — [^<]+<\/h2>\s*<p class="sub">[^<]+<\/p>\s*<div id="funds"><\/div>\s*<\/section>/);
  assert.ok(HTML.indexOf('id="s-cds"') < HTML.indexOf('id="s-funds"') && HTML.indexOf('id="s-funds"') < HTML.indexOf('id="s-method"'));
  for (const cls of ['cb-fu-grid', 'cb-fu-card', 'cb-fu-row', 'cb-fu-bf', 'cb-fu-rg', 'cb-fu-range', 'cb-fu-key', 'cb-fu-axis']) assert.ok(HTML.includes('.' + cls), cls + ' is drawn and not styled');
  assert.ok(BOARD_JS.includes('root.JHCreditFunds') && BOARD_JS.includes('FU.click(d, state,') && BOARD_JS.includes('need("bonds"); need("history"); need("funds");') && BOARD_JS.includes('drawFunds();'));
  assert.ok(BOARD_JS.includes('if (what === "funds" && state.view === "new") drawFunds();'), 'the section is drawn again when its whole document arrives');
  assert.ok(SHEET_JS.includes("funds().sheetHtml(doc, r, ctx)"));
  assert.ok(!/fetch\(|XMLHttpRequest/.test(FUNDS_JS), 'the board asks for documents; this file only draws them');
  assert.ok(!/cusip|isin/i.test(FUNDS_JS.replace(/The identifiers are not republished/g, '')), 'no identifier is named, read or drawn');
});
