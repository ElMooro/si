// The campaign-scan engine lists every stock the chart's Acc, Bot and Dist icons would mark. Its scans are a Python
// port (aws/lambdas/justhodl-campaign-scan/source/chart_scans.py) of the three browser files the chart runs. This suite
// runs the browser files and the port over the same real banked bars and requires the same marks, so a rule changed on
// the chart and not in the port (or the reverse) fails here.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { spawnSync } = require('node:child_process');

const root = path.join(__dirname, '..');
const FILES = ['jh-chart-vol-events.js', 'jh-chart-campaign-marks.js', 'jh-chart-distribution.js'];
const bars = JSON.parse(fs.readFileSync(path.join(root, 'tests/fixtures/campaign-scan-bars.json'), 'utf8')).bars;
const stored = JSON.parse(fs.readFileSync(path.join(root, 'tests/fixtures/campaign-scan-marks.json'), 'utf8')).marks;
const iso = (s) => new Date(s * 1000).toISOString().slice(0, 10);

function browserMarks(rows, ticker) {
  const ctx = { console, setTimeout, fetch: undefined };
  ctx.window = ctx;
  for (const f of FILES) vm.runInNewContext(fs.readFileSync(path.join(root, f), 'utf8'), ctx);
  const d = rows.map((r) => ({ time: Math.floor(r[0] / 1000), open: r[1], high: r[2], low: r[3], close: r[4], volume: r[5] }));
  ctx.__jhCampLayer.bottom = 1;
  ctx.__jhCampLayer.accum = 1;
  ctx.__jhDistOn = 1;
  // the arrays come out of the vm's own realm: rebuild them here so a strict comparison is about the marks
  return {
    campaign: Array.from(ctx.jhCampaignMarks(d, ticker, '1d', 'candles'), (m) => [iso(m.time), m.text]),
    distribution: Array.from(ctx.jhDistributionMarks(d, '1d', 'candles'), (m) => [iso(m.time), m.text]),
  };
}

const PY = `
import json, sys
from datetime import datetime, timezone
sys.path.insert(0, sys.argv[1])
import chart_scans as S
bars = json.load(open(sys.argv[2]))["bars"]
out = {}
for t, rows in bars.items():
    T = [datetime.fromtimestamp(r[0] / 1000.0, tz=timezone.utc).date().isoformat() for r in rows]
    st, sc, ds, camp, dist = S.scan(T, [r[1] for r in rows], [r[2] for r in rows], [r[3] for r in rows], [r[4] for r in rows], [r[5] for r in rows])
    out[t] = {"campaign": [[m[0], m[1]] for m in camp], "distribution": [[m[0], m[1]] for m in dist]}
json.dump(out, sys.stdout)
`;

test('the fixture is real, whole and rich enough to mean something', () => {
  const tickers = Object.keys(bars);
  assert.deepEqual(tickers.sort(), ['LULU', 'NVDA', 'PFE', 'QQQ', 'TSLA', 'XOM']);
  for (const t of tickers) {
    assert.ok(bars[t].length >= 1300, `${t} has ${bars[t].length} sessions`);
    for (let i = 1; i < bars[t].length; i++) assert.ok(bars[t][i][0] > bars[t][i - 1][0], `${t} bars are in order`);
    assert.ok(bars[t].every((r) => r.length === 6 && r[1] > 0 && r[2] >= r[3] && r[3] > 0 && r[4] > 0 && r[5] >= 0), `${t} bars are prices`);
  }
  const kinds = new Set();
  for (const t of tickers) for (const k of ['campaign', 'distribution']) for (const m of stored[t][k]) kinds.add(m[1]);
  assert.deepEqual([...kinds].sort(), ['ACCUM', 'BOTTOM', 'CLIMAX', 'D-TOP', 'DIST']);
});

test('the stored marks are what the chart files print today', () => {
  for (const t of Object.keys(bars)) assert.deepEqual(browserMarks(bars[t], t), stored[t], `${t}: regenerate tests/fixtures/campaign-scan-marks.json and move the port with the chart`);
});

test('the Python port prints the same marks as the chart files, bar for bar', () => {
  const run = spawnSync('python3', ['-c', PY, path.join(root, 'aws/lambdas/justhodl-campaign-scan/source'), path.join(root, 'tests/fixtures/campaign-scan-bars.json')], { encoding: 'utf8', maxBuffer: 1 << 26 });
  assert.equal(run.status, 0, run.stderr);
  const port = JSON.parse(run.stdout);
  let n = 0;
  for (const t of Object.keys(bars)) {
    const browser = browserMarks(bars[t], t);
    assert.deepEqual(port[t].campaign, browser.campaign, `${t} campaign marks`);
    assert.deepEqual(port[t].distribution, browser.distribution, `${t} distribution marks`);
    n += browser.campaign.length + browser.distribution.length;
  }
  assert.ok(n >= 90, `${n} marks compared`);
});

test('the port stays equal on shorter histories too (every mark depends on where the tape ends)', () => {
  const cut = {};
  for (const t of ['NVDA', 'TSLA', 'PFE']) for (const n of [320, 700, 1100, 1250]) cut[`${t}@${n}`] = bars[t].slice(0, n);
  const tmp = path.join(require('node:os').tmpdir(), `campaign-scan-cut-${process.pid}.json`);
  fs.writeFileSync(tmp, JSON.stringify({ bars: cut }));
  try {
    const run = spawnSync('python3', ['-c', PY, path.join(root, 'aws/lambdas/justhodl-campaign-scan/source'), tmp], { encoding: 'utf8', maxBuffer: 1 << 26 });
    assert.equal(run.status, 0, run.stderr);
    const port = JSON.parse(run.stdout);
    for (const k of Object.keys(cut)) assert.deepEqual(port[k], browserMarks(cut[k], k.split('@')[0]), k);
  } finally {
    fs.unlinkSync(tmp);
  }
});

test('the engine names the chart files it ports and reads nothing but S3', () => {
  const port = fs.readFileSync(path.join(root, 'aws/lambdas/justhodl-campaign-scan/source/chart_scans.py'), 'utf8');
  for (const f of FILES) assert.ok(port.includes(f), `${f} is named in the port`);
  const engine = fs.readFileSync(path.join(root, 'aws/lambdas/justhodl-campaign-scan/source/lambda_function.py'), 'utf8');
  assert.match(engine, /chain_guard\.begin\(event, context\)/);
  for (const banned of ['urllib', 'requests', '.invoke(', 'put_events', 'https://']) assert.ok(!engine.includes(banned), `${banned} is not in the engine`);
  assert.match(engine, /CLIMAX_SESSIONS = 40\b/);
  const marks = fs.readFileSync(path.join(root, 'jh-chart-campaign-marks.js'), 'utf8');
  assert.match(marks, /last - s\.i > 40/, 'the chart still reads a live climax inside 40 bars');
});
