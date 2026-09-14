const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..');
const engine = fs.readFileSync(path.join(root, 'jh-chart-engine.js'), 'utf8');
const html = fs.readFileSync(path.join(root, 'chart.html'), 'utf8');
const search = fs.readFileSync(path.join(root, 'jh-chart-tvsearch.js'), 'utf8');

test('header has TradingView symbol chip, compare plus, and data-type switch', () => {
  assert.match(html, /id="symchip"/);
  assert.match(html, /id="btn-addcmp"/);
  assert.match(html, /id="btn-dtype"/);
  assert.match(html, /Symbol, ISIN, or CUSIP/);
  assert.match(html, /id="dtype"/);
  assert.match(html, /jh-chart-tvsearch\.js/);
  assert.match(html, /jh-tvux-36/);
});

test('compare overlays raw closes on percent scale, never rebases onto price', () => {
  assert.match(engine, /function comparingOn\(\)/);
  assert.match(engine, /function rightScaleMode\(\)/);
  assert.match(engine, /comparingOn\(\)\?2:scaleMode/);
  assert.match(engine, /lastValueVisible:true/);
  assert.match(engine, /ls\.setData\(cb\.map\(function\(b\)\{ return \{time:b\.time, value:b\.close\}; \}\)/);
  assert.doesNotMatch(engine, /\(p\.b\/c0\)\*t0/);
  assert.match(engine, /v12\.34/);
  assert.match(engine, /openSymSearch\("", "compare"\)/);
});

test('compare toggle stays in the search list and data-type desk exists', () => {
  assert.match(engine, /dest==="compare"/);
  assert.match(engine, /ADDED SYMBOLS/);
  assert.match(engine, /RECENT SYMBOLS/);
  assert.match(search, /jhOpenDataType/);
  assert.match(search, /Valuation \/ ratios/);
  assert.match(search, /function renderVal/);
  assert.match(search, /function renderFin/);
  assert.doesNotMatch(search, /\[object Object\]/);
});
