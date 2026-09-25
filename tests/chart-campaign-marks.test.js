const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");

test("chart keeps the engine cache token and paints campaign marks", () => {
  const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
  const eng = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
  assert.match(html, /jh-chart-engine\.js\?v=20260922ab-cqind/);
  assert.match(html, /jh-chart-campaign-marks\.js/);
  assert.match(eng, /jhCampaignMarks\(display, active, tf, kind\)/);
  assert.match(eng, /CLIMAX:14,ACCUM:15,BOTTOM:16/);
  const iAcc = eng.indexOf("id=btn-acc");
  const iBot = eng.indexOf("id=btn-bot");
  const iPump = eng.indexOf("id=btn-pump");
  const iAlrt = eng.indexOf("id=btn-alrt");
  assert.ok(iAcc > 0 && iBot > iAcc && iPump > iBot && iAlrt > iPump);
});

test("climax, bottom, and accumulation land on the daily chart, not on intraday noise", () => {
  const ctx = { window: {}, console, fetch: function () { return Promise.reject(new Error("no")); } };
  ctx.window = ctx;
  vm.runInNewContext(fs.readFileSync(path.join(root, "jh-chart-vol-events.js"), "utf8"), ctx);
  vm.runInNewContext(fs.readFileSync(path.join(root, "jh-chart-campaign-marks.js"), "utf8"), ctx);
  const day = 86400;
  const start = Date.parse("2020-01-01T00:00:00Z") / 1000;
  const d = [];
  for (let i = 0; i < 120; i++) {
    d.push({ time: start + i * day, open: 100, high: 101, low: 99, close: 100, volume: 1e6 });
  }
  ctx.jhVolEventTable = function () {
    return [
      { kind: "sc", time: d[10].time, i: 10, score: 3 },
      { kind: "sc", time: d[40].time, i: 40, score: 12 },
      { kind: "sc", time: d[50].time, i: 50, score: 20 },
      { kind: "bottom", time: d[50].time, i: 50, score: 1 },
      { kind: "eoa", time: d[70].time, i: 70, score: 1 }
    ];
  };
  const mk = ctx.jhCampaignMarks(d, "SPY", "1d", "candles");
  assert.ok(mk.some(function (m) { return m.text === "CLIMAX" && m.time === d[40].time; }));
  assert.ok(!mk.some(function (m) { return m.text === "CLIMAX" && m.time === d[50].time; }));
  assert.ok(mk.some(function (m) { return m.text === "BOTTOM" && m.time === d[50].time; }));
  assert.ok(mk.some(function (m) { return m.text === "ACCUM" && m.time === d[70].time; }));
  let called = 0;
  ctx.jhVolEventTable = function () { called++; return []; };
  const intra = d.map(function (b, i) { return { time: start + i * 300, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume }; });
  const noise = ctx.jhCampaignMarks(intra, "QQQ", "5m", "candles");
  assert.equal(called, 0);
  assert.equal(noise.length, 0);
});
