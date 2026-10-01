// Offline predecessor counterexamples and a proposed measurement contract.
// Passing this suite does NOT mean the production chart is repaired.
const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const pinned = require('./fixtures/chart-volume-calculation-predecessor.json');
const scope = {};
vm.runInNewContext(Object.values(pinned.functions).join('\n'), scope);

const bars = volumes => volumes.map((volume, time) => ({time, close: 100, volume}));
const validVolume = x => typeof x === 'number' && Number.isFinite(x) && x >= 0;

// Small independent window oracle, NOT a production implementation or export.
// Exact preceding n observations; no dropping invalid rows or borrowing older ones.
function expectedRvol(rows, i, n = 20) {
  if (i < n || !validVolume(rows[i].volume)) return null;
  const prior = rows.slice(i - n, i).map(row => row.volume);
  if (!prior.every(validVolume)) return null;
  const total = prior.reduce((sum, value) => sum + value, 0);
  if (!Number.isFinite(total) || total <= 0) return null;
  const ratio = rows[i].volume / (total / n);
  return Number.isFinite(ratio) ? ratio : null;
}

test('predecessor compresses a 10x spike to 6.89655x by including itself', () => {
  const rows = bars([...Array(20).fill(100), 1000]);
  assert.equal(expectedRvol(rows, 20), 10);
  assert.equal(scope.rvolSeries(rows, 20).at(-1).value, 1000 / 145);
});

test('20 preceding observations require the 21st observation', () => {
  const rows = bars(Array(20).fill(100));
  assert.equal(expectedRvol(rows, 19), null);
  assert.equal(scope.rvolSeries(rows, 20).at(-1).value, 1);
});

test('zero denominator stays unavailable, not normal 1x or self-created 20x', () => {
  for (const last of [0, 100]) {
    const rows = bars([...Array(20).fill(0), last]);
    assert.equal(expectedRvol(rows, 20), null);
    assert.equal(scope.rvolSeries(rows, 20).at(-1).value, last ? 20 : 1);
  }
});

test('genuine zero numerator and genuine baseline zeros are retained', () => {
  assert.equal(expectedRvol(bars([...Array(20).fill(100), 0]), 20), 0);
  assert.equal(expectedRvol(bars([0, ...Array(19).fill(100), 190]), 20), 2);
});

test('invalid operands withhold the exact window without dropping or backfilling', () => {
  for (const invalid of [null, undefined, '', '100', true, -1, NaN, Infinity]) {
    for (const index of [1, 20]) {
      const rows = bars(Array(21).fill(100));
      rows[index].volume = invalid;
      assert.equal(expectedRvol(rows, 20), null);
    }
  }
});

test('oracle excludes later data and is invariant to constant volume-unit scaling', () => {
  const rows = bars([...Array(20).fill(100), 1000]);
  const prefixValue = expectedRvol(rows, 20);
  assert.equal(expectedRvol([...rows, ...bars([1e12, 0, NaN])], 20), prefixValue);
  assert.equal(expectedRvol(rows.map(row => ({...row, volume: row.volume * 1000})), 20), prefixValue);
});

test('undefined or overflowing ratios never become measured Infinity', () => {
  assert.equal(expectedRvol(bars([...Array(20).fill(1e308), 100]), 20), null);
  assert.equal(expectedRvol(bars([...Array(20).fill(Number.MIN_VALUE), 1e308]), 20), null);
});

test('separate deferred OBV defect: unchanged closes accumulate volume', () => {
  const rows = bars([100, 100, 100]);
  assert.equal(scope.obv(rows).at(-1).value, 200);
  // Required later repair: both close differences are zero, hence signed sum zero.
  assert.equal(rows.slice(1).reduce((sum, row, j) =>
    sum + Math.sign(row.close - rows[j].close) * row.volume, 0), 0);
});
