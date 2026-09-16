const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const fuseSrc = fs.readFileSync(path.join(root, "jh-etf-fuse.js"), "utf8");
const deskSrc = fs.readFileSync(path.join(root, "jh-chart-etf-desk.js"), "utf8");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const lambda = fs.readFileSync(path.join(root, "aws/lambdas/justhodl-etf-global-desk/source/lambda_function.py"), "utf8");
const worker = fs.readFileSync(path.join(root, "cloudflare/workers/justhodl-data-proxy/src/index.js"), "utf8");

function loadFuse() {
  const ctx = { window: {}, Date, Math, Number, isFinite, Infinity, Array, Object, String, JSON, console, fetch: () => Promise.resolve({ ok: false }) };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(fuseSrc, ctx);
  assert.ok(ctx.JHEtfFuse, "JHEtfFuse attached");
  return ctx.JHEtfFuse;
}

test("desk no longer slices fund-flow tape to 40 sessions", () => {
  assert.match(lambda, /VERSION = "1\.7\.0"/);
  assert.match(lambda, /max_pages=50/);
  assert.match(lambda, /etf-flow-hist\/%s\.json/);
  assert.doesNotMatch(lambda, /for r in rows\[:40\]/);
  assert.match(worker, /etf-flow-hist/);
  assert.match(worker, /allFundFlows/);
  assert.match(worker, /no fund_flow tape/);
  assert.doesNotMatch(worker, /out\.results = rows/);
  assert.match(fuseSrc, /function fullHist/);
  assert.match(fuseSrc, /if \(!rows\.length\) \{ delete histCache/);
  assert.match(fuseSrc, /var daily = step > 0 && step < 36 \* 3600/);
  assert.match(deskSrc, /hydrateFullHist/);
  assert.match(deskSrc, /F\.bare\(active\) !== t/);
  assert.match(engine, /nH\.toLocaleString\(\)\+" sess"/);
});

test("alignHist sums weekly buckets and one print per session on 1m", () => {
  const F = loadFuse();
  const mon = Date.UTC(2026, 8, 14) / 1000;
  const tue = Date.UTC(2026, 8, 15) / 1000;
  const wed = Date.UTC(2026, 8, 16) / 1000;
  const next = Date.UTC(2026, 8, 21) / 1000;
  const hist = [
    { d: "2026-09-14", f: 1e9 },
    { d: "2026-09-15", f: 2e9 },
    { d: "2026-09-16", f: -0.5e9 }
  ];
  const daily = F.alignHist(hist, [{ time: mon }, { time: tue }, { time: wed }]);
  assert.equal(daily.length, 3);
  assert.equal(daily[0].raw, 1e9);
  assert.equal(daily[1].raw, 2e9);
  assert.equal(daily[2].raw, -0.5e9);
  const week = F.alignHist(hist, [{ time: mon }, { time: next }]);
  assert.equal(week.length, 1);
  assert.equal(week[0].raw, 2.5e9);
  assert.equal(week[0].n, 3);
  const intra = [];
  for (let h = 13; h < 20; h++) for (let m = 0; m < 60; m += 5) intra.push({ time: Date.UTC(2026, 8, 14, h, m) / 1000 });
  intra.push({ time: Date.UTC(2026, 8, 15, 13, 30) / 1000 });
  const i = F.alignHist(hist, intra);
  assert.equal(i.length, 2);
  assert.equal(i[0].raw, 1e9);
  assert.equal(i[1].raw, 2e9);
  const monthStart = Date.UTC(2026, 8, 1) / 1000;
  const nextMonth = Date.UTC(2026, 9, 1) / 1000;
  const month = F.alignHist(hist, [{ time: monthStart }, { time: nextMonth }]);
  assert.equal(month.length, 1);
  assert.equal(month[0].raw, 2.5e9);
  assert.equal(month[0].n, 3);
  const gapBars = [];
  for (let day = 1; day <= 16; day++) {
    const dt = new Date(Date.UTC(2026, 8, day));
    const wd = dt.getUTCDay();
    if (wd === 0 || wd === 6) continue;
    if (day === 15) continue;
    gapBars.push({ time: dt.getTime() / 1000 });
  }
  const gapped = F.alignHist(hist, gapBars);
  assert.equal(gapped.length, 2);
  assert.equal(gapped[0].raw, 1e9);
  assert.equal(gapped[1].raw, -0.5e9);
  assert.equal(gapped[0].d, "2026-09-14");
  assert.equal(gapped[1].d, "2026-09-16");
});

test("mergeHist unions tapes by date and never drops a later print", () => {
  const F = loadFuse();
  const a = [{ d: "2026-07-20", f: 1 }, { d: "2026-09-14", f: 2 }];
  const b = [{ d: "2012-01-03", f: 9 }, { d: "2026-09-14", f: 3 }];
  const m = F.mergeHist(a, b);
  assert.equal(m[0].d, "2012-01-03");
  assert.equal(m[m.length - 1].d, "2026-09-14");
  assert.equal(m[m.length - 1].f, 3);
  assert.equal(m.length, 3);
});
