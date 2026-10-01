// Offline repeatable cost probe; emits JSON only and never contacts a provider.
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const {performance} = require('node:perf_hooks');
const source = fs.readFileSync(path.join(__dirname, '../jh-chart-vol-events.js'), 'utf8');
function sample(run, count) {
  const elapsed = [];
  for (let i = 0; i < count; i++) {
    const start = performance.now(); run(); elapsed.push(performance.now() - start);
  }
  elapsed.sort((a, b) => a - b);
  return {iterations: count, median_ms: +elapsed[Math.floor(count / 2)].toFixed(3),
    max_ms: +elapsed.at(-1).toFixed(3)};
}
const results = [];
for (const n of [1000, 10000]) {
  const context = {}; context.window = context; vm.runInNewContext(source, context);
  const d = Array.from({length: n}, (_, i) => {
    const close = 100 + 10 * Math.sin(i / 30);
    return {time: 1700000000 + i * 86400, open: close + 0.1,
      high: close + 1, low: close - 1, close, volume: 100 + i % 17};
  });
  context.jhVolEventTable(d);
  const hits = sample(() => context.jhVolEventTable(d), 30);
  const corrections = sample(() => { d[n - 1].volume++; context.jhVolEventTable(d); }, 5);
  results.push({bars: n, retained_scalar_slots: 6 * n, cache_hits: hits, corrections});
}
console.log(JSON.stringify({node: process.version,
  scope: 'Local synthetic timing only; not a mobile/browser performance guarantee', results}, null, 2));
