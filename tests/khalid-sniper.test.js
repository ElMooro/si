const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");

test("Khalid sniper sits next to Alert and opens on the chart", () => {
  const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
  const eng = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
  const rail = fs.readFileSync(path.join(root, "jh-chart-tvrail.js"), "utf8");
  const page = fs.readFileSync(path.join(root, "khalid.html"), "utf8");
  assert.match(html, /jh-chart-engine\.js\?v=20260922ab-cqind/);
  assert.match(html, /jh-khalid-sniper\.js/);
  assert.match(page, /id="k-sniper-host"/);
  assert.match(page, /jh-khalid-sniper\.js/);
  assert.match(rail, /kind === "sniper"/);
  assert.match(eng, /jhOpenSymbol/);
  const sniper = fs.readFileSync(path.join(root, "jh-khalid-sniper.js"), "utf8");
  ["Below the 250-day", "At least 50% off the high", "RSI washed out", "Very tight price spread",
    "Tight Bollinger bands", "Shrinking volume", "Flat moving average", "On 3-month support",
    "Higher low", "Selling climax or capitulation", "Double bottom", "PEG under 1",
    "ETH or BTC turned while this is still on its low", "Small caps versus large caps"
  ].forEach((label) => assert.match(sniper, new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))));
  const iPump = eng.indexOf("id=btn-pump");
  const iSnip = eng.indexOf("id=btn-snip");
  const iAlrt = eng.indexOf("id=btn-alrt");
  assert.ok(iPump > 0 && iSnip > iPump && iAlrt > iSnip);
});

function loadScore() {
  const ctx = { console };
  ctx.window = ctx;
  ctx.globalThis = ctx;
  vm.runInNewContext(fs.readFileSync(path.join(root, "jh-khalid-sniper.js"), "utf8"), ctx);
  return ctx.jhSniperScore;
}

test("a higher-low base can pass and an uptrend cannot", () => {
  const score = loadScore();
  const d = [];
  let px = 100;
  for (let i = 0; i < 560; i++) {
    let o = px, h, l, c, v = 1.4e6;
    if (i < 430) {
      px = 100 * Math.pow(0.9974, Math.min(i, 420));
      c = px; h = c * 1.012; l = c * 0.988;
    } else if (i < 470) {
      px = px * 0.994; c = px; h = c * 1.01; l = c * 0.99;
      if (i === 455) { l = px * 0.90; c = px * 0.95; h = px * 0.99; v = 4e6; px = c; }
    } else if (i < 500) {
      px = px * 1.004; c = px; h = px * 1.01; l = px * 0.995; v = 1e6;
    } else if (i < 545) {
      px = px * 0.9985; c = px; h = px * 1.006; l = px * 0.994; v = 0.9e6;
      if (i === 520) { l = px * 0.92; c = px * 0.96; h = px * 0.995; v = 3.5e6; px = c; }
    } else {
      const up = i % 2 === 0;
      o = px; c = px * (up ? 1.0015 : 0.999); h = Math.max(o, c) * 1.001; l = Math.min(o, c) * 0.999; v = 0.3e6; px = c;
    }
    d.push({ time: i, open: o, high: Math.max(h, o, c), low: Math.min(l, o, c), close: c, volume: v });
  }
  const hit = score(d, { assetClass: "COMMODITY", flows: "Persistent flow score" });
  assert.equal(hit.sniper, true);
  const up = [];
  let q = 50;
  for (let i = 0; i < 560; i++) {
    q *= 1.0015;
    up.push({ time: i, open: q, high: q * 1.01, low: q * 0.99, close: q, volume: 1e6 });
  }
  const miss = score(up, { assetClass: "STOCK" });
  assert.equal(miss.sniper, false);
  assert.equal(miss.checks.find((c) => c.id === "offhigh").pass, false);
});
