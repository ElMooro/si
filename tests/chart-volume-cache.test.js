const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const root = path.join(__dirname, '..');
const current = fs.readFileSync(path.join(root, 'jh-chart-vol-events.js'), 'utf8');
const original = fs.readFileSync(path.join(__dirname, 'fixtures/chart-volume-cache-predecessor.js.txt'), 'utf8');
function boot(source = current) {
  const context = {};
  context.window = context;
  vm.runInNewContext(source, context);
  return context;
}
function rows(n = 100) {
  return Array.from({length: n}, (_, i) => ({time: 1700000000 + i * 86400,
    open: 100, high: 101, low: 99, close: 100, volume: 100}));
}
function crash(d, i = 80) {
  d[i] = {...d[i], open: 100, high: 101, low: 90, close: 90, volume: 300};
  return d;
}
const json = value => JSON.stringify(value);
function equalFresh(context, d) {
  assert.equal(json(context.jhVolumeTape(d)), json(boot(original).jhVolumeTape(d)));
}

test('whole predecessor is pinned; all non-cache source remains byte-identical', () => {
  assert.equal(crypto.createHash('sha256').update(original).digest('hex'),
    '5d524e4197cb3b8790b70766b67d6e08434f07ac2ce08a733b95a7af33ab39d0');
  const normalized = current.replace(
    /  var _tblD = null, _tblOut = null, _tblValues = null;[\s\S]*?(?=  function eventTable\(d\))/,
    '  var _tblD = null, _tblOut = null;\n\n'
  ).replace('if (_tblD === d && _tblOut && sameBarValues(d))',
    'if (_tblD === d && _tblOut)').replace('    _tblValues = barValues(d);\n', '');
  assert.equal(normalized, original);
});

test('predecessor retains obsolete CAPIT after an in-place correction; current removes it', () => {
  for (const [source, obsolete] of [[original, true], [current, false]]) {
    const context = boot(source), d = crash(rows());
    assert.ok(context.jhVolEventTable(d).some(e => e.i === 80 && e.kind === 'capit'));
    Object.assign(d[80], {low: 99, close: 100, volume: 100});
    assert.equal(context.jhVolEventTable(d).some(e => e.i === 80 && e.kind === 'capit'), obsolete);
    if (!obsolete) equalFresh(context, d);
  }
});

test('each scalar input correction invalidates even an old interior bar', () => {
  const edits = {time: 1, open: 0.1, high: 0.1, low: -0.1, close: 0.1, volume: 1};
  for (const [field, delta] of Object.entries(edits)) {
    const context = boot(), d = crash(rows());
    const before = context.jhVolEventTable(d);
    d[25][field] += delta;
    assert.notEqual(context.jhVolEventTable(d), before, field);
    equalFresh(context, d);
  }
});

test('append and truncate on the same array match a fresh classifier at every prefix', () => {
  const context = boot(), all = crash(rows(360)), d = [];
  for (const bar of all) {
    d.push(bar);
    equalFresh(context, d);
  }
  for (const length of [340, 300, 100, 81, 80, 69, 0]) {
    d.length = length;
    equalFresh(context, d);
  }
  // Equality is to the original on each prefix, NOT a promise that historical
  // SC/structure labels never repaint. Their forward confirmation is unchanged.
});

test('same-length splice, order change and distinct arrays do not borrow stale events', () => {
  const context = boot(), d = crash(rows());
  context.jhVolEventTable(d);
  d.splice(80, 1, {...rows()[80]});
  equalFresh(context, d);
  d[70] = {...d[70], close: 90, low: 90, volume: 300};
  context.jhVolEventTable(d);
  [d[70], d[80]] = [d[80], d[70]];
  equalFresh(context, d);
  const other = crash(rows());
  equalFresh(context, other);
  equalFresh(context, d);
});

test('repeated consumers reuse unchanged results and leave input bars intact', () => {
  const context = boot(), d = crash(rows()), input = json(d);
  const first = context.jhVolEventTable(d);
  for (let i = 0; i < 30; i++) {
    assert.equal(context.jhVolEventTable(d), first);
    assert.equal(context.jhVolumeTape(d).events, first);
    assert.equal(json(context.jhVolEvents(d)), json(context.jhVolumeTape(d).markers));
  }
  assert.equal(json(d), input);
  // Non-input presentation metadata does not invalidate the cache.
  d[80].displayLabel = 'corrected';
  assert.equal(context.jhVolEventTable(d), first);
});

test('unchanged NaN remains cacheable without being sanitized into zero', () => {
  const context = boot(), d = rows();
  d[10].volume = NaN;
  const first = context.jhVolEventTable(d);
  assert.equal(context.jhVolEventTable(d), first);
  d[10].volume = 100;
  assert.notEqual(context.jhVolEventTable(d), first);
  equalFresh(context, d);
});

test('real tape consumer drops stale markers after a correction without new requests', () => {
  const context = boot(), d = crash(rows());
  vm.runInNewContext(fs.readFileSync(path.join(root, 'jh-chart-tape.js'), 'utf8'), context);
  assert.ok(context.jhTapeRead(d).wyckoff.markers.some(e => e.text === 'CAPIT'));
  Object.assign(d[80], {low: 99, close: 100, volume: 100});
  assert.ok(!context.jhTapeRead(d).wyckoff.markers.some(e => e.text === 'CAPIT'));
});
