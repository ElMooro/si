'use strict';
// The "Engine data" section (jh-engine-data.js): every field an engine wrote, on the engine's own page.
// Khalid 2026-11-26: "all data from the engine is displaying on the page".
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const ROOT = path.join(__dirname, '..');
const J = require('../jh-engine-data.js');
const SRC = fs.readFileSync(path.join(ROOT, 'jh-engine-data.js'), 'utf8');
const FIX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures', 'engine-data-cut.json.gz'))).toString('utf8'));
const MOUNTS = {
  'why.html': 'research', 'insiders.html': 'insiders', 'options-analytics.html': 'options', 'valuations.html': 'valuations',
  'corpbonds.html': 'corpbonds', 'plumbing.html': 'plumbing', 'liquidity.html': 'liquidity', 'canaries.html': 'canaries',
  'breadth-thrust.html': 'breadth', 'trend-reversal.html': 'reversal', 'catalyst-calendar.html': 'calendar', 'btc-cycle.html': 'crypto',
};

// an independent count: every scalar is a value, an empty list or object is one value
function count(node) {
  if (node === null || typeof node !== 'object') return 1;
  const kids = Array.isArray(node) ? node : Object.values(node);
  return kids.length ? kids.reduce((n, k) => n + count(k), 0) : 1;
}

test('the script is plain ASCII and ships as a script that also loads in node', () => {
  assert.equal(/[^\x00-\x7F]/.test(SRC), false);
  assert.equal(typeof J.boot, 'function');
  assert.match(J.VERSION, /^\d+\.\d+\.\d+$/);
});

test('each of the twelve pages mounts the section once, by its registered name, with the script', () => {
  for (const [page, name] of Object.entries(MOUNTS)) {
    const html = fs.readFileSync(path.join(ROOT, page), 'utf8');
    assert.equal(html.split('data-jed="').length - 1, 1, page);
    assert.ok(html.includes(`<div data-jed="${name}"></div>`), page);
    assert.match(html, /<script src="\/jh-engine-data\.js\?v=\d{8}[a-z]" defer><\/script>/, page);
    assert.ok(J.PAGES[name] && J.PAGES[name].docs.length >= 1, name);
  }
  assert.deepEqual(Object.keys(J.PAGES).sort(), Object.values(MOUNTS).sort());
});

test('every registered document names its engine, and no document is listed twice on a page', () => {
  for (const [name, page] of Object.entries(J.PAGES)) {
    const keys = page.docs.map((d) => d.key || 'ticker');
    assert.equal(new Set(keys).size, keys.length, name);
    for (const d of page.docs) {
      assert.match(d.engine, /^justhodl-[a-z0-9-]+$/, name);
      assert.ok(d.name && d.name.length > 3, name);
      if (d.key) assert.match(d.key, /^(data\/)?[a-z0-9-]+\.json$/, name);
    }
  }
});

test('the count of values equals an independent count, on four real documents', () => {
  for (const [key, doc] of Object.entries(FIX)) {
    const c = J.census(doc);
    assert.equal(c.values, count(doc), key);
    assert.ok(c.fields > 10 && c.fields <= c.values, key);
    let walked = 0;
    J.eachLeaf(doc, () => { walked += 1; });
    assert.equal(walked, c.values, key);
    assert.equal(J.leafPage(doc, '', 0, 50).total, c.values, key);
  }
});

test('the Every field view pages through every value exactly once and finds a value by path or by content', () => {
  const doc = FIX['data/options-analytics.json'];
  const total = J.census(doc).values;
  const seen = new Set();
  for (let start = 0; start < total; start += 97) for (const [p] of J.leafPage(doc, '', start, 97).rows) seen.add(p);
  assert.equal(seen.size, total);
  const hit = J.leafPage(doc, 'gamma_flip_strike', 0, 500);
  assert.equal(hit.total, doc.board.length + doc.squeeze_setups.length);
  assert.ok(hit.rows.every(([p]) => p.endsWith('gamma_flip_strike')));
  const byValue = J.leafPage(doc, doc.board[0].ticker, 0, 500);
  assert.ok(byValue.rows.some(([p, v]) => p === 'board[0].ticker' && v === doc.board[0].ticker));
});

test('shapes: rows, a table keyed by name, a series, a short list, figures, a mixed group', () => {
  assert.equal(J.kind([{ a: 1 }, { b: 2 }]), 'rows');
  assert.equal(J.kind({ A: { x: 1, y: 2 }, B: { x: 3, y: 4 }, C: { x: 5 } }), 'table');
  assert.equal(J.kind({ A: { x: 1 }, B: { q: 3 }, C: { z: 5 } }), 'group');
  assert.equal(J.kind([1, 2, 3, 4, 5, 6, 7, 8]), 'series');
  assert.equal(J.kind([1, 2, 3]), 'list');
  assert.equal(J.kind({ a: 1, b: 'x', c: null }), 'pairs');
  assert.equal(J.kind({ a: 1, b: [1] }), 'group');
  assert.equal(J.kind([]), 'empty');
  assert.equal(J.kind({}), 'empty');
  assert.equal(J.kind([[1, 2], [3, 4]]), 'matrix');
  assert.deepEqual(J.rowsOf({ A: { x: 1 }, B: { x: 3 }, C: { x: 5 } })[1], { '(key)': 'B', x: 3 });
  assert.deepEqual(J.columns([{ a: 1 }, { b: 2, a: 3 }, { c: 0 }]), ['a', 'b', 'c']);
  assert.equal(J.kind(FIX['data/options-analytics.json'].board), 'rows');
});

test('a table sorts both ways on any column, with blanks last in both directions', () => {
  const rows = [{ t: 'B', v: 2 }, { t: 'A', v: null }, { t: 'D', v: -5 }, { t: 'C', v: 10 }, { t: 'E' }];
  assert.deepEqual(J.sortRows(rows, 'v', 'desc').map((r) => r.t), ['C', 'B', 'D', 'A', 'E']);
  assert.deepEqual(J.sortRows(rows, 'v', 'asc').map((r) => r.t), ['D', 'B', 'C', 'A', 'E']);
  assert.deepEqual(J.sortRows(rows, 't', 'asc').map((r) => r.t), ['A', 'B', 'C', 'D', 'E']);
  assert.deepEqual(J.sortRows(rows, 't', 'desc').map((r) => r.t), ['E', 'D', 'C', 'B', 'A']);
  assert.equal(rows[0].t, 'B');
  const board = FIX['data/options-analytics.json'].board;
  for (const col of J.columns(board)) {
    const up = J.sortRows(board, col, 'asc'), down = J.sortRows(board, col, 'desc');
    assert.equal(up.length, board.length, col);
    assert.equal(new Set(down).size, board.length, col);
  }
});

test('a figure gets a unit only when the field name carries it', () => {
  assert.equal(J.show('net_premium_usd', -119950000), '−$119.95M');
  assert.equal(J.show('buy_value_usd', 9919161281), '$9.92B');
  assert.equal(J.show('value', 1883465), '1,883,465');
  assert.equal(J.show('net_gex_musd_per_1pct', 12.5), '12.5');
  assert.equal(J.show('aum_benchmarked_usd_bn', 10500), '10,500');
  assert.equal(J.show('top_pct', 9.7), '9.7%');
  assert.equal(J.show('score', 62.5), '62.5');
  assert.equal(J.show('x', 0.3997), '0.3997');
  assert.equal(J.show('x', null), '—');
  assert.equal(J.show('x', false), 'No');
  assert.equal(J.show('generated_at', '2026-11-26T14:00:33.865509+00:00'), '2026-11-26 14:00 UTC');
  assert.equal(J.show('as_of', '2026-11-25'), '2026-11-25');
});

test('names are made readable and the engine’s own name is kept for the hover', () => {
  assert.equal(J.label('atm_iv_front'), 'ATM IV front');
  assert.equal(J.label('n_unusual'), 'N unusual');
  assert.equal(J.label('gamma_flip_strike'), 'Gamma flip strike');
  assert.ok(SRC.includes("title=\"' + esc(f) + '\""));
  assert.ok(SRC.includes("title=\"' + esc(c) + '\""));
});

test('a download keeps every column and cannot start a formula', () => {
  const csv = J.csvOf([{ a: '=SUM(A1)', b: 'x,y', c: -3 }, { a: 'plain', d: [1, 2] }], ['a', 'b', 'c', 'd']);
  assert.equal(csv, 'a,b,c,d\r\n\'=SUM(A1),"x,y",-3,\r\nplain,,,"[1,2]"\r\n');
});

test('age is read from the document’s own stamp', () => {
  assert.equal(J.stampOf({ generated_at: '2026-11-26T14:00:33+00:00' }), '2026-11-26T14:00:33+00:00');
  assert.equal(J.stampOf({ as_of: '2026-11-25' }), '2026-11-25');
  assert.equal(J.stampOf([1]), null);
  assert.equal(Math.round(J.hoursSince('2026-11-26T12:00:00+00:00', Date.parse('2026-11-26T15:00:00Z'))), 3);
  assert.equal(Math.round(J.hoursSince('2026-11-26T12:00:00', Date.parse('2026-11-26T15:00:00Z'))), 3);
  assert.equal(J.ago(0.2), '12 min ago');
  assert.equal(J.ago(17.2), '17 h ago');
  assert.equal(J.ago(1350), '56 days ago');
});

test('only a ticker column links to the chart', () => {
  assert.equal(J.isTickerCell('ticker', 'NVDA'), true);
  assert.equal(J.isTickerCell('symbol', 'BRK.B'), true);
  assert.equal(J.isTickerCell('name', 'NVDA'), false);
  assert.equal(J.isTickerCell('ticker', 'not a ticker'), false);
  assert.ok(SRC.includes('win.top.jhOpenSymbol('));
});
