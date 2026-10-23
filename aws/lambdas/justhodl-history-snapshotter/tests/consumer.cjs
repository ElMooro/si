// Run the actual audit page's load/selectFeed functions against both outputs.
// DOM and fetch are invented; no snapshot/archive URL is invoked.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '../../../..');
const audit = fs.readFileSync(path.join(root, 'audit.html'), 'utf8');
const weights = fs.readFileSync(path.join(root, 'weights.html'), 'utf8');
assert(weights.includes('/calibration/history-index.json'));
assert(!weights.includes('/data/history-index.json'));
const start = audit.indexOf('async function load(){');
const end = audit.indexOf('// Cache fetched snapshots', start);
assert(start > 0 && end > start);
const actual = audit.slice(start, end);
async function consume(packet) {
  const elements = {};
  const requests = [];
  const selected = [];
  const element = id => elements[id] ||= {
    innerHTML: '', textContent: '', style: {},
    scrollIntoView() {}, classList: {add() {}, remove() {}}
  };
  const scope = {
    DATA: null, BUCKET: 'https://fixture.invalid',
    Date: {now: () => 0}, discoverApiUrl: async () => {},
    timeAgo: value => value, fmtNumber: value => String(value),
    fetch: async url => {requests.push(url); return {ok: true, json: async () => packet};},
    document: {getElementById: element, querySelectorAll: () => [], querySelector: () => null}
  };
  vm.createContext(scope);
  vm.runInContext(actual, scope);
  await scope.load();
  for (let i = 0; i < packet.feeds.length; i++) {
    scope.selectFeed(i);
    selected.push({feed: element('selected-feed').textContent, timestamps: element('timestamps').innerHTML});
  }
  return {requests, selected, elements: JSON.parse(JSON.stringify(elements))};
}
(async () => {
  const cases = JSON.parse(fs.readFileSync(0, 'utf8'));
  for (const [before, after] of cases) assert.deepEqual(await consume(before), await consume(after));
  console.log(`Actual audit functions and separate weights index: ${cases.length} consumer cases passed`);
})().catch(error => {console.error(error); process.exit(1);});
