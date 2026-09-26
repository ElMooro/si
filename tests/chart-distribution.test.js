const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");

test("distribution button sits next to Bottom and stays off until clicked", () => {
  const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
  const eng = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
  const src = fs.readFileSync(path.join(root, "jh-chart-distribution.js"), "utf8");
  assert.match(html, /jh-chart-engine\.js\?v=20260926-frame-identity/);
  assert.match(html, /jh-chart-distribution\.js/);
  assert.match(eng, /jhDistributionMarks\(display, tf, kind\)/);
  assert.match(eng, /jhDistToggle\(\)/);
  assert.match(eng, /"D-TOP":17/);
  assert.match(src, /__jhDistOn = 0/);
  const iBot = eng.indexOf("id=btn-bot");
  const iDist = eng.indexOf("id=btn-dist");
  const iPump = eng.indexOf("id=btn-pump");
  assert.ok(iBot > 0 && iDist > iBot && iPump > iDist);
});

test("a failed rally marks DIST on the daily chart only after the button is on", () => {
  const ctx = { window: {}, console };
  ctx.window = ctx;
  vm.runInNewContext(fs.readFileSync(path.join(root, "jh-chart-vol-events.js"), "utf8"), ctx);
  vm.runInNewContext(fs.readFileSync(path.join(root, "jh-chart-distribution.js"), "utf8"), ctx);
  const day = 86400;
  const start = Date.parse("2016-01-04T00:00:00Z") / 1000;
  const d = [];
  let px = 100;
  for (let i = 0; i < 340; i++) {
    if (i < 210) px = 100 * Math.pow(1.0022, i);
    const o = px;
    let h = px * 1.004, l = px * 0.996, c = px, v = 1e6;
    if (i === 220) { h = 168; c = 166; l = 164; v = 1.1e6; px = 166; }
    else if (i > 220 && i < 236) { px = 166 - (i - 220) * 0.45; h = px * 1.004; l = px * 0.996; c = px; }
    else if (i === 248) { h = 171.5; c = 170.2; l = 166; v = 2.4e6; px = 170.2; }
    else if (i > 248 && i < 270) { px = 170 - (i - 248) * 1.15; h = px * 1.01; l = px * 0.99; c = px; v = 1.6e6; }
    else if (i >= 270) { px = 145; h = px * 1.004; l = px * 0.996; c = px; }
    d.push({ time: start + i * day, open: o, high: h, low: l, close: c, volume: v });
  }
  assert.equal(ctx.jhDistributionMarks(d, "1d", "candles").length, 0);
  ctx.__jhDistOn = 1;
  const on = ctx.jhDistributionMarks(d, "1d", "candles");
  assert.ok(on.some(function (m) { return m.text === "DIST"; }));
  const intra = d.map(function (b, i) {
    return { time: start + i * 300, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume };
  });
  assert.equal(ctx.jhDistributionMarks(intra, "5m", "candles").length, 0);
});
