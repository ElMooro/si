'use strict';
// cost-of-capital.html (jh-cost-of-capital.js): the desk over what justhodl-damodaran publishes, and eva.html's premium line.
// The documents here are made up in the engine's own shapes; none of his figures are kept in the repository.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const A = require(path.join(ROOT, 'jh-cost-of-capital.js'));
const SRC = fs.readFileSync(path.join(ROOT, 'jh-cost-of-capital.js'), 'utf8');
const PAGE = fs.readFileSync(path.join(ROOT, 'cost-of-capital.html'), 'utf8');
const EVA = fs.readFileSync(path.join(ROOT, 'eva.html'), 'utf8');
const text = (html) => html.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim();

const P = {
  source: 'Aswath Damodaran, Damodaran Online (NYU Stern)', attribution: 'Data: Aswath Damodaran, Damodaran Online.', caution: 'Industry averages are computed once a year.',
  us: { as_of: '2026-11-01', measure: 'sustainable_payout', premium: 0.0441, premium_pct: 4.41, tbond_pct: 4.94, sp500: 7580, expected_return_pct: 9.35,
    a_month_before: { date: '2026-10-01', value: 0.042, pct: 4.2 }, a_year_before: { date: '2025-11-01', value: 0.0373, pct: 3.73 },
    measures: [{ key: 'sustainable_payout', label: 'Sustainable payout', his_heading: 'ERP (T12 m with sustainable payout)', value: 0.0441, pct: 4.41 },
      { key: 'cash_yield', label: 'Cash returned, trailing 12 months', his_heading: 'ERP (T12m)', value: 0.0379, pct: 3.79 }] },
  home_page: { page_date: '2026-11-01', file_date: '2026-11-01', page_values_pct: [4.41, 3.79], agree: true },
  monthly: { from: '2008-09-01', to: '2026-11-01', n: 4, months_his_file_skips: ['2024-09'], measure_columns: { sustainable_payout: 'erp_s', cash_yield: 'erp_t12m' },
    cols: [{ key: 'date', label: 'Start of month' }, { key: 's_and_p_500', label: 'S&P 500' }, { key: 't_bond_rate', label: 'T.Bond Rate' }, { key: 'erp_s', label: 'ERP (T12 m with sustainable payout)' }, { key: 'erp_t12m', label: 'ERP (T12m)' }, { key: 'notes', label: 'Notes' }],
    rows: [['2008-09-01', 1252, 0.0372, null, 0.0422, null], ['2016-01-01', 2044, 0.0227, 0.0612, 0.0516, null], ['2026-10-01', 7652, 0.0529, 0.042, 0.037, null], ['2026-11-01', 7580, 0.0494, 0.0441, 0.0379, null]] },
  annual: { from: 1960, cols: [{ key: 'year', label: 'Year' }, { key: 's_and_p_500', label: 'S&P 500' }, { key: 'implied_erp_fcfe', label: 'Implied ERP (FCFE)' }, { key: 'implied_premium_fcfe_with_sustainable_payout', label: 'Implied Premium (FCFE with sustainable Payout)' }, { key: 'erp_riskfree_rate', label: 'ERP/Riskfree Rate' }],
    rows: [[1960, 58.11, 0.0292, null, 1.06], [2024, 5881.63, 0.0433, 0.04, 0.945], [2025, 6845.5, 0.0423, 0.0418, 1.012]] },
  returns: { cols: [{ key: 'year', label: 'Year' }, { key: 'a', label: 'Annual Returns on Investments in: S&P 500 (includes dividends)' }, { key: 'b', label: 'Value of $100 invested at start of 1928 in: S&P 500 (includes dividends)3' }],
    rows: [[1928, 0.4381, 143.81], [2025, 0.1772, 1157009.09]],
    premiums_worked_here: [{ from: 1928, to: 2025, years: 98, stocks: { arithmetic: 0.1185, compounded: 0.1002 }, stocks_minus_bills: { arithmetic: 0.0844, compounded: 0.0665 }, stocks_minus_bonds: { arithmetic: 0.0703, compounded: 0.0548 } }] },
  files: [{ file: 'ERPbymonth.xlsx', url: 'https://pages.stern.nyu.edu/~adamodar/pc/implprem/ERPbymonth.xlsx', last_modified: 'Mon, 02 Nov 2026 16:19:09 GMT', bytes: 47651 }],
};
const K = { file: 'ctrypremJuly26.xlsx', as_of: '2026-07-01', mature_market_premium: 0.042, us_premium: 0.0442, n: 3,
  cols: [['country', 'Country'], ['region', 'Region'], ['moodys_rating', "Moody's rating"], ['default_spread', 'Default spread from the rating'], ['equity_risk_premium', 'Total equity risk premium'], ['country_risk_premium', 'Country risk premium'],
    ['cds_spread', 'Sovereign CDS, net of the US'], ['equity_risk_premium_cds', 'Total equity risk premium, from the CDS'], ['country_risk_premium_cds', 'Country risk premium, from the CDS']].map(([key, label]) => ({ key, label })),
  rows: [['Germany', 'Western Europe', 'Aaa', 0, 0.042, 0, 0.0006, 0.0429, 0.0009], ['Albania', 'Eastern Europe & Russia', 'Ba3', 0.0286, 0.0865, 0.0445, null, null, null], ['Turkey', 'Western Europe', 'Ba3', 0.0286, 0.0865, 0.0445, 0.027, 0.084, 0.042]],
  frontier: { title: 'Frontier Markets (no sovereign ratings)', cols: [{ key: 'country', label: 'Country' }, { key: 'prs', label: 'PRS Composite Risk Score' }, { key: 'erp', label: 'ERP' }], rows: [['Brunei', 80.75, 0.0494]] } };
const I = { as_of: '2026-01-05', n_tables: 2, regions: [{ code: 'US', label: 'United States' }, { code: 'Global', label: 'Global' }],
  benchmarks: { US: { cols: [{ key: 'industry', label: 'Industry' }, { key: 'firms', label: 'Companies' }, { key: 'beta', label: 'Beta' }, { key: 'cost_of_capital', label: 'Cost of capital' }, { key: 'pe_trailing', label: 'PE, trailing' }],
    rows: [['Advertising', 52, 1.2105, 0.0781, 44.43], ['Banks (Money Center)', 15, 0.88, null, 13.1], ['Total Market', 5994, 0.9121, 0.0696, 57.86]] } },
  tables: [{ id: 'wacc-us', topic: 'wacc', region: 'US', title: 'Cost of equity and capital', key: 'data/warm/damodaran/tables/wacc-us.json' }] };

test('a list sorts both ways on any column, and a row without the figure is last both ways', () => {
  const rows = K.rows;
  assert.deepEqual(A.sorted(rows, 4, 'desc').map(r => r[0]), ['Albania', 'Turkey', 'Germany'], 'ties keep their order');
  assert.deepEqual(A.sorted(rows, 4, 'asc').map(r => r[0]), ['Germany', 'Albania', 'Turkey']);
  assert.deepEqual(A.sorted(rows, 6, 'desc').map(r => r[0]), ['Turkey', 'Germany', 'Albania']);
  assert.deepEqual(A.sorted(rows, 6, 'asc').map(r => r[0]), ['Germany', 'Turkey', 'Albania'], 'no CDS: last, whichever way');
  assert.deepEqual(A.sorted(rows, 0, 'asc').map(r => r[0]), ['Albania', 'Germany', 'Turkey']);
  assert.deepEqual(A.sorted(rows, 0, 'desc').map(r => r[0]), ['Turkey', 'Germany', 'Albania']);
  assert.deepEqual(rows.map(r => r[0]), ['Germany', 'Albania', 'Turkey'], 'the document is not reordered');
  assert.deepEqual(A.filtered(rows, 'western').map(r => r[0]), ['Germany', 'Turkey']);
  assert.deepEqual(A.filtered(rows, ' BA3 ').length, 2);
  assert.equal(A.filtered(rows, '').length, 3);
  assert.equal(A.filtered(rows, 'zz').length, 0);
});

test('a cell is written by the kind of its column', () => {
  assert.equal(A.cell('pct', 0.0441), '4.41%');
  assert.equal(A.cell('pct', null), '–');
  assert.equal(A.cell('int', 5994), '5,994');
  assert.equal(A.cell('num2', 1.2105), '1.21');
  assert.equal(A.cell('x1', 44.43), '44.4');
  assert.equal(A.cell('usd0', 1157009.09), '$1,157,009');
  assert.equal(A.cell('month', '2026-11-01'), 'Nov 2026');
  assert.equal(A.cell('year', 1928), '1928');
  assert.equal(A.cell('raw', 0.0935), '0.0935', 'a table kept as his file holds it shows the fraction, not a guess at its unit');
  assert.equal(A.cell('raw', 5994), '5,994');
  assert.equal(A.cell('raw', 0.30000000001), '0.3');
  assert.equal(A.cell('raw', 'NA'), 'NA');
  assert.equal(A.day('2026-07-01'), 'Jul 1, 2026');
  assert.equal(A.day('not a day'), '–');
});

test('a grid carries the whole list, its count, a sort button on every heading and a save button', () => {
  const g = A.countryGrid(K, { q: '', sort: { j: 4, dir: 'desc' } });
  const html = A.gridHtml(g);
  assert.equal((html.match(/<tr>/g) || []).length, 1 + 3, 'a heading row and every country');
  assert.equal((html.match(/data-cc-sort="countries"/g) || []).length, 9, 'every column sorts');
  assert.ok(html.includes('aria-sort="descending"') && html.includes('Total equity risk premium ▼'));
  assert.equal((html.match(/aria-sort="none"/g) || []).length, 8);
  assert.ok(html.includes('data-cc-csv="countries"') && html.includes('data-cc-q="countries"') && html.includes('>3 countries<'));
  assert.ok(text(html).includes('Albania Eastern Europe & Russia Ba3 2.86% 8.65% 4.45% – – –'));
  const few = A.gridHtml(A.countryGrid(K, { q: 'turk', sort: null }));
  assert.ok(few.includes('>1 of 3 countries<') && !few.includes('Germany'));
  assert.ok(A.gridHtml(A.countryGrid(K, { q: 'zz', sort: null })).includes('Nothing matches.'));
  assert.ok(!A.gridHtml(A.returnsGrid(P, { sort: null })).includes('type="search"'), 'a list keyed by a year has no search box');
});

test('the lists save as files of what is shown, under the headings on the page', () => {
  const g = A.countryGrid(K, { q: 'western', sort: { j: 0, dir: 'desc' } });
  const lines = A.csv(g.cols, A.shown(g)).split('\r\n');
  assert.equal(lines[0], "Country,Region,Moody's rating,Default spread from the rating,Total equity risk premium,Country risk premium,\"Sovereign CDS, net of the US\",\"Total equity risk premium, from the CDS\",\"Country risk premium, from the CDS\"");
  assert.deepEqual(lines.slice(1), ['Turkey,Western Europe,Ba3,0.0286,0.0865,0.0445,0.027,0.084,0.042', 'Germany,Western Europe,Aaa,0,0.042,0,0.0006,0.0429,0.0009', '']);
  assert.ok(A.csv([{ label: 'a' }], [['x "quoted", y'], [null]]).startsWith('a\r\n"x ""quoted"", y"\r\n\r\n'));
});

test('the tiles and the measures are the document\'s figures', () => {
  const t = text(A.tiles(P));
  for (const s of ['Implied equity risk premium 4.41% start of Nov 2026 · over the 10-year Treasury', 'A month before 4.20% +0.21 pts since Oct 2026', 'A year before 3.73% +0.68 pts since Nov 2025',
    '10-year Treasury rate 4.94%', 'Expected return on stocks 9.35%', 'S&P 500 7,580']) assert.ok(t.includes(s), s + ' | ' + t);
  const m = text(A.measuresHtml(P));
  assert.ok(m.includes('Sustainable payout · the one the engines use ERP (T12 m with sustainable payout) 4.41% 4.41%') && m.includes('Cash returned, trailing 12 months ERP (T12m) 3.79% 3.79%'));
  assert.ok(m.includes("The file's newest row is his home page's five figures for Nov 1, 2026."));
  assert.ok(text(A.measuresHtml({ us: P.us, home_page: { page_date: '2026-12-01', file_date: '2026-11-01', page_values_pct: [4.5], agree: null } })).includes('His home page is of Dec 1, 2026 and his file of Nov 1, 2026: not compared.'));
  assert.ok(text(A.measuresHtml({ us: P.us, home_page: { page_date: '2026-11-01', page_values_pct: [4.6, 3.8], agree: false } })).includes('disagree'));
  assert.ok(text(A.tiles({})).includes('Implied equity risk premium –'), 'an empty document draws dashes, not NaN');
  assert.ok(!/NaN|undefined/.test(A.tiles({}) + A.measuresHtml({}) + A.workedHtml({}) + A.sourceHtml(null, null, null)));
});

test('the chart draws one line a measure on one axis, and the pointer reads the nearest month', () => {
  const s = A.monthlySeries(P);
  assert.deepEqual(s.map(x => [x.key, x.color, x.pts.length]), [['sustainable_payout', '#3987e5', 3], ['cash_yield', '#d95926', 4]], 'colours in the palette\'s fixed order; a month without the figure is not a point');
  const out = A.chartSvg(s, 900, 320);
  assert.equal((out.svg.match(/<path /g) || []).length, 2);
  assert.ok(out.svg.includes('>4.41%</text>') && out.svg.includes('>3.79%</text>'), 'each line ends in its last value');
  assert.ok(/fill="#e6eaf2" font-size="12">4\.41%/.test(out.svg), 'the value is written in ink, beside a dot of the line\'s colour');
  assert.equal((out.svg.match(/stroke-width="2" stroke-linejoin/g) || []).length, 2);
  assert.ok(out.svg.includes('>3%</text>') && out.svg.includes('>7%</text>') && !out.svg.includes('>0%</text>'), 'the axis runs over the premiums drawn');
  const near = A.nearest(s, Date.parse('2016-01-20T00:00:00Z'));
  assert.deepEqual(near.map(n => [n.label, n.v]), [['Sustainable payout', 0.0612], ['Cash returned, trailing 12 months', 0.0516]]);
  assert.deepEqual(A.nearest(s, Date.parse('2008-09-05T00:00:00Z')).map(n => n.label), ['Cash returned, trailing 12 months'], 'a line with no month near the pointer is left out');
  const an = A.annualSeries(P);
  assert.deepEqual(an.map(x => [x.key, x.pts.length]), [['implied_premium_fcfe_with_sustainable_payout', 2], ['implied_erp_fcfe', 3]]);
  assert.ok(A.chartSvg([], 900, 320).svg.includes('No history to draw.'));
  const legend = A.legendHtml(s, { cash_yield: true });
  assert.ok(legend.includes('data-cc-line="cash_yield" aria-pressed="false"') && legend.includes('data-cc-line="sustainable_payout" aria-pressed="true"'));
});

test('a figure too far off the scale to be a premium is left out of the picture and named, and the axis keeps a few ticks', () => {
  const Q = JSON.parse(JSON.stringify(P));
  Q.monthly.rows.splice(2, 0, ['2026-08-01', 7490, 0.0474, 0.0428, 5.94, null], ['2026-09-01', 7686, 0.0475, -0.2, 0.0409, null]);
  const s = A.monthlySeries(Q);
  assert.deepEqual(s.map(x => [x.pts.length, x.off]), [[4, [{ when: 'Sep 2026', value: -0.2 }]], [5, [{ when: 'Aug 2026', value: 5.94 }]]], 'the lines keep every other month');
  assert.equal(A.offHtml(s), 'Left out of the picture, as too far off the scale to be a premium: Sustainable payout, Sep 2026 (-20.00% in his file); Cash returned, trailing 12 months, Aug 2026 (594.00% in his file). The table below shows his file as it is.');
  assert.equal(A.offHtml(A.monthlySeries(P)), '', 'nothing off the scale: nothing is said');
  const out = A.chartSvg(s, 900, 320);
  assert.ok(!out.svg.includes('>594') && out.svg.includes('>7%</text>') && (out.svg.match(/text-anchor="end"/g) || []).length <= 9, 'the axis is the premiums\' own range');
  assert.ok(text(A.gridHtml(A.monthlyGrid(Q, { sort: null }))).includes('Aug 2026 7,490 4.74% 4.28% 594.00%'), 'the table keeps his figure as it is');
  assert.ok(A.onScale(0.25) && A.onScale(-0.05) && !A.onScale(0.2501) && !A.onScale(null));
  const wide = A.chartSvg([{ key: 'a', label: 'a', color: '#3987e5', pts: [[0, 0.01], [86400000 * 400, 0.24]] }], 900, 320).svg;
  assert.ok((wide.match(/text-anchor="end"/g) || []).length <= 9 && wide.includes('>25%</text>') && wide.includes('>0%</text>'), 'a wide range takes wider steps, never more than nine labels');
  assert.ok(PAGE.includes('<p class="cc-note" id="cc-off"></p>'));
});

test('the other lists use the right kind for each column', () => {
  const b = A.benchGrid(I, 'US', { q: '', sort: null });
  assert.deepEqual(b.cols.map(c => c.kind), ['text', 'int', 'num2', 'pct', 'x1']);
  assert.ok(text(A.gridHtml(b)).includes('Advertising 52 1.21 7.81% 44.4') && text(A.gridHtml(b)).includes('Banks (Money Center) 15 0.88 – 13.1'));
  assert.equal(A.benchGrid(I, 'Japan', { q: '', sort: null }).rows.length, 0);
  assert.deepEqual(A.frontierGrid(K, { q: '', sort: null }).cols.map(c => c.kind), ['text', 'num2', 'pct']);
  assert.deepEqual(A.monthlyGrid(P, { sort: null }).cols.map(c => c.kind), ['month', 'raw', 'pct', 'pct', 'pct', 'text']);
  assert.deepEqual(A.annualGrid(P, { sort: null }).cols.map(c => c.kind), ['year', 'raw', 'pct', 'pct', 'raw'], 'a ratio of the premium to the rate is a ratio');
  assert.deepEqual(A.returnsGrid(P, { sort: null }).cols.map(c => c.kind), ['year', 'pct', 'usd0']);
  const t = A.tableGrid({ cols: [{ key: 'industry', label: 'Industry Name' }, { key: 'n', label: 'Number of firms' }, { key: 'b', label: 'Beta' }], rows: [['Advertising', 52, 1.2105]] }, { q: '', sort: null });
  assert.deepEqual(t.cols.map(c => c.kind), ['text', 'int', 'raw']);
  assert.ok(text(A.workedHtml(P)).includes('1928 to 2025 (98) 11.85% 10.02% 8.44% 6.65% 7.03% 5.48%'));
  const src = text(A.sourceHtml(P, K, I));
  assert.ok(src.includes('Data: Aswath Damodaran') && src.includes('no strings attached') && src.includes('Months his monthly file has no row for: Sep 2024.') && src.includes('ERPbymonth.xlsx Mon, 02 Nov 2026 16:19:09 GMT 47,651 bytes'));
});

test('the page gives every block its own heading and loads one script of its own', () => {
  const sections = PAGE.match(/<section id="s-[a-z]+">\s*<h2>/g) || [];
  assert.equal(sections.length, 7, 'seven sections, each opening with its heading');
  assert.equal((PAGE.match(/<section /g) || []).length, 7);
  for (const id of ['cc-premium', 'cc-measures', 'cc-views', 'cc-legend', 'cc-chart', 'cc-monthly', 'cc-annual', 'cc-country-facts', 'cc-countries', 'cc-frontier', 'cc-regions', 'cc-industries', 'cc-pick', 'cc-table-head', 'cc-table', 'cc-worked', 'cc-returns', 'cc-source', 'cc-host'])
    assert.equal((PAGE.match(new RegExp('id="' + id + '"', 'g')) || []).length, 1, id);
  for (const id of ['cc-monthly', 'cc-annual', 'cc-countries', 'cc-frontier', 'cc-industries', 'cc-table', 'cc-returns']) assert.ok(PAGE.includes('class="cc-grid" id="' + id + '"'), id + ' is a grid host');
  assert.ok(/<script src="\/jh-cost-of-capital\.js\?v=\d{8}[a-z]"><\/script>/.test(PAGE) && PAGE.includes('window.JHCostOfCapital.mount(window)'));
  assert.ok(PAGE.includes("Aswath Damodaran's") && PAGE.includes('not a real-time figure for one company'));
  assert.ok(SRC.includes('jh-reskin-skip') && !/api[_-]?key/i.test(SRC) && !SRC.includes('@'));
  for (const key of ['data/equity-risk-premium.json', 'data/country-risk-premiums.json', 'data/industry-benchmarks.json']) assert.ok(SRC.includes('get("' + key + '")'), key);
  assert.ok(!/stern\.nyu\.edu/.test(SRC), 'the page reads the warehouse, never his site; his links come from the documents');
});

test('eva.html says where its premium was read from, and how it reads a document from before the change', () => {
  assert.ok(EVA.includes('<div class="row" id="erp-source"><span>Premium read from</span>'));
  assert.ok(EVA.includes('w.erp_fixed===false&&w.erp_as_of') && EVA.includes('href="/cost-of-capital.html"') && EVA.includes('Damodaran · start of '));
  assert.ok(EVA.includes('"fixed · no fresh figure in the warehouse":"a fixed figure"'), 'a document without the new fields reads "a fixed figure"');
  assert.equal((EVA.match(/Equity Risk Premium<\/span>/g) || []).length, 1);
});
