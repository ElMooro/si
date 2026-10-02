const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const indux = fs.readFileSync(path.join(root, "jh-chart-indux.js"), "utf8");
const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");

function grab(src, name) {
  const key = "function " + name + "(";
  const i = src.indexOf(key);
  if (i < 0) throw new Error("missing " + name);
  let depth = 0, started = false;
  for (let j = i; j < src.length; j++) {
    if (src[j] === "{") { depth++; started = true; }
    else if (src[j] === "}") {
      depth--;
      if (started && depth === 0) return src.slice(i, j + 1);
    }
  }
  throw new Error("unclosed " + name);
}

function loadMath() {
  const src = ["reportedVolume", "rvolAt", "sma", "rvolSeries", "histVol", "alignSpy", "priceSpread", "volDoD", "relVolRatio"]
    .map(function (n) { return grab(engine, n); })
    .join("\n");
  const ctx = { window: {}, Math: Math, Date: Date, lastBenchName: "SPX" };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(src + "\nthis.priceSpread=priceSpread; this.volDoD=volDoD; this.relVolRatio=relVolRatio; this.histVol=histVol;", ctx);
  return ctx;
}

test("Indicators dropdown is on the chart bar and stamps the engine", () => {
  assert.match(html, /jh-chart-engine\.js\?v=20261001aa-ind/);
  assert.match(html, /jh-chart-indux\.js\?v=20261001aa-ind/);
  assert.match(html, /\.menu \.irow button\.gr/);
  assert.match(engine, /Indicators ▾/);
  assert.match(engine, /function openIndMenu/);
  assert.match(engine, /data-gear/);
  assert.match(engine, /data-all>All indicators/);
  assert.match(engine, /id:"pspread"/);
  assert.match(engine, /id:"relvol"/);
  assert.match(engine, /id:"voldd"/);
  assert.match(engine, /id:"bb"/);
  assert.match(engine, /id:"macd"/);
  assert.match(engine, /id:"fibauto"/);
  assert.match(engine, /ind\.c2\|\|"#26c6da"/);
  assert.match(engine, /o\.c2\|\|"#ff6d00"/);
  assert.match(engine, /c2:i\.c2,c3:i\.c3/);
  assert.match(engine, /v12\.34/);
  assert.match(engine, /__jhChartEngineV1239/);
  assert.match(indux, /pspread:/);
  assert.match(indux, /relvol:/);
  assert.match(indux, /voldd:/);
  assert.match(indux, /id=sc2/);
  assert.match(indux, /Surge %/);
  assert.doesNotMatch(engine, /\[object Object\]/);
  assert.doesNotMatch(html, /undefined%/);
});

test("price spread, day-to-day volume, and relative vol match the desk definitions", () => {
  const m = loadMath();
  const bars = [];
  for (let i = 0; i < 25; i++) {
    bars.push({ time: 1700000000 + i * 86400, open: 100, high: 101, low: 99, close: 100, volume: 1000000 });
  }
  bars.push({ time: 1700000000 + 25 * 86400, open: 100, high: 110, low: 100, close: 100, volume: 3000000 });
  const ps = m.priceSpread(bars, 20);
  assert.ok(Math.abs(ps[0].value - 2) < 1e-9);
  const last = ps[ps.length - 1];
  assert.ok(Math.abs(last.value - 10) < 1e-9);
  assert.ok(last.avg != null && last.avg < 4);
  assert.ok(last.value >= last.avg * 1.5);

  const dod = m.volDoD([
    { time: 1, close: 1, high: 1, low: 1, volume: 100 },
    { time: 2, close: 1, high: 1, low: 1, volume: 180 },
    { time: 3, close: 1, high: 1, low: 1, volume: 90 }
  ]);
  assert.ok(Math.abs(dod[0].value - 80) < 1e-9);
  assert.ok(Math.abs(dod[1].value - (-50)) < 1e-9);

  function wave(amp) {
    const o = [];
    let px = 100;
    for (let i = 0; i < 80; i++) {
      px = px * (1 + amp * Math.sin(i / 3));
      o.push({ time: 1700000000 + i * 86400, close: px, high: px, low: px, open: px, volume: 1 });
    }
    return o;
  }
  const bench = wave(0.01);
  const same = bench.map(function (b) { return { time: b.time, close: b.close, high: b.high, low: b.low, open: b.open, volume: 1 }; });
  const ratio = m.relVolRatio(same, bench, 20);
  assert.equal(ratio.bench, "SPX");
  assert.ok(ratio.ratio.length > 10);
  const end = ratio.ratio[ratio.ratio.length - 1].value;
  assert.ok(Math.abs(end - 1) < 0.05, "identical tape should be ~1×, got " + end);
  const hot = m.relVolRatio(wave(0.03), bench, 20);
  const hotEnd = hot.ratio[hot.ratio.length - 1].value;
  assert.ok(hotEnd > 2, "3× amplitude should realize more vol, got " + hotEnd);
  const none = m.relVolRatio(bench, [], 20);
  assert.equal(none.ratio.length, 0);
  assert.ok(none.ownHv.length > 10);
});
