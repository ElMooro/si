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
  const src = ["sma", "rvolAt", "priceSpread", "volDoD", "histVol", "alignSpy", "relVolRatio"]
    .map(function (n) { return grab(engine, n); })
    .join("\n");
  const ctx = { window: {}, Math: Math, Date: Date, lastBenchName: "SPX" };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(src + "\nthis.priceSpread=priceSpread; this.volDoD=volDoD; this.relVolRatio=relVolRatio; this.histVol=histVol;", ctx);
  return ctx;
}

test("Indicators dropdown is on the chart bar and stamps the engine", () => {
  assert.match(html, /jh-chart-engine\.js\?v=20261002ab-spike/);
  assert.match(html, /jh-chart-indux\.js\?v=20261002ab-spike/);
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
  assert.match(engine, /v12\.41/);
  assert.match(engine, /__jhChartEngineV1239/);
  assert.match(indux, /pspread:/);
  assert.match(indux, /relvol:/);
  assert.match(indux, /voldd:/);
  assert.match(indux, /id=sc2/);
  assert.match(indux, /Outlier ×/);
  assert.match(engine, /8,13,21,34,55,100,200,250/);
  assert.match(engine, /function toggleChartFs/);
  assert.match(engine, /function placeSyncedVLine/);
  assert.match(engine, /row\("sr","Support & Resistance"\)/);
  assert.match(engine, /data-fav/);
  assert.match(indux, /jhIndFavToggle/);
  assert.match(indux, /100, 200 and 250/);
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

  const quiet = [];
  for (let i = 0; i < 51; i++) {
    quiet.push({ time: 1700000000 + i * 86400, open: 100, high: 101, low: 99, close: 100, volume: 1000000 });
  }
  const capit = quiet.slice();
  capit.push({ time: 1700000000 + 51 * 86400, open: 100, high: 100, low: 90, close: 92, volume: 3000000 });
  const cap = m.volDoD(capit, 50, 2);
  const capLast = cap[cap.length - 1];
  assert.ok(Math.abs(capLast.value - 3) < 1e-9);
  assert.equal(capLast.tag, "CAPIT");
  const stop = quiet.slice();
  stop.push({ time: 1700000000 + 51 * 86400, open: 92, high: 100, low: 90, close: 98, volume: 3000000 });
  assert.equal(m.volDoD(stop, 50, 2).slice(-1)[0].tag, "STOP");
  const mid = quiet.slice();
  mid[mid.length - 1] = { time: mid[mid.length - 1].time, open: 100, high: 101, low: 90, close: 100, volume: 1000000 };
  mid.push({ time: 1700000000 + 51 * 86400, open: 110, high: 112, low: 108, close: 111, volume: 3000000 });
  assert.equal(m.volDoD(mid, 50, 2).slice(-1)[0].tag, "OUT");
  const normal = quiet.slice();
  normal.push({ time: 1700000000 + 51 * 86400, open: 100, high: 101, low: 99, close: 100, volume: 1100000 });
  assert.equal(m.volDoD(normal, 50, 2).slice(-1)[0].tag, "");
  const seam = [];
  for (let i = 0; i < 50; i++) seam.push({ time: 1800000000 + i * 86400, open: 100, high: 101, low: 99, close: 100, volume: 3e10 });
  seam.push({ time: 1800000000 + 50 * 86400, open: 100, high: 100, low: 90, close: 92, volume: 100 });
  const seamLast = m.volDoD(seam, 50, 2).slice(-1)[0];
  assert.equal(seamLast.value, undefined);
  assert.equal(seamLast.tag, undefined);

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
