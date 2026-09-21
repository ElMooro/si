const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
const indux = fs.readFileSync(path.join(root, "jh-chart-indux.js"), "utf8");
const search = fs.readFileSync(path.join(root, "jh-chart-tvsearch.js"), "utf8");
const fuseSrc = fs.readFileSync(path.join(root, "jh-etf-fuse.js"), "utf8");

function loadQx() {
  const start = engine.indexOf("function tickSize(");
  const end = engine.indexOf("function fmtVol(");
  const slice = engine.slice(start, end);
  const ctx = { Math, Number, isFinite, Infinity, console };
  vm.createContext(ctx);
  vm.runInContext(
    'var UP="#089981", DN="#f23645";\n' + slice +
    "; this.jhQx={tickSize:tickSize,roundTick:roundTick,roundBar:roundBar,crossedAlert:crossedAlert,retCal:retCal,escHtml:escHtml,safeHref:safeHref};",
    ctx
  );
  return ctx.jhQx;
}
function loadFit() {
  const start = engine.indexOf("function medianGap(");
  const end = engine.indexOf("function resampleToTf(");
  const ctx = { Math, Number, isFinite, Infinity, console };
  vm.createContext(ctx);
  vm.runInContext(
    "function spec(tfId){ return [tfId]; }\n" + engine.slice(start, end) +
    "; this.barsFitTf=barsFitTf; this.medianGap=medianGap; this.expectedGap=expectedGap;",
    ctx
  );
  return ctx;
}
function loadFuse() {
  const ctx = { window: {}, Date, Math, console, fetch: function () { return Promise.reject(new Error("no net")); } };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(fuseSrc, ctx);
  return ctx.JHEtfFuse;
}

test("audit stamp and identity/interval/tick helpers are live", () => {
  assert.match(html, /jh-chart-engine\.js\?v=20260916aa-fix/);
  assert.match(html, /jh-chart-inst\.js\?v=20260916aa-fix/);
  assert.match(html, /jh-chart-indux\.js\?v=20260916aa-fix/);
  assert.match(html, /jh-chart-tvsearch\.js\?v=20260921-etf-native1/);
  assert.match(html, /jh-etf-fuse\.js\?v=20260921-native1/);
  assert.match(engine, /__jhChartEngineV1239/);
  assert.match(engine, /function loadTicker/);
  assert.match(engine, /keeping /);
  assert.match(engine, /function barsFitTf/);
  assert.match(engine, /function crossedAlert/);
  assert.match(engine, /function retCal/);
  assert.match(engine, /function syncLivePill/);
  assert.match(engine, /__jhStBound/);
  assert.doesNotMatch(html, /\[object Object\]/);
});

test("failed search does not commit active before bars", () => {
  assert.match(engine, /async function loadTicker/);
  assert.match(engine, /active=s; tf=tryTf; lastGoodTf=tryTf/);
  const go = engine.slice(engine.indexOf("function goSymbol"), engine.indexOf("async function loadTicker"));
  assert.doesNotMatch(go, /active=s; loadDraw/);
  assert.match(go, /loadTicker\(s, dest\)/);
});

test("barsFitTf rejects daily bars labeled as 1m and accepts 1m", () => {
  const fit = loadFit();
  const t0 = 1700000000;
  const daily = [];
  const intra = [];
  for (let i = 0; i < 20; i++) {
    daily.push({ time: t0 + i * 86400, open: 1, high: 1, low: 1, close: 1, volume: 1 });
    intra.push({ time: t0 + i * 60, open: 1, high: 1, low: 1, close: 1, volume: 1 });
  }
  assert.equal(fit.barsFitTf(daily, "1m"), false);
  assert.equal(fit.barsFitTf(daily, "5m"), false);
  assert.equal(fit.barsFitTf(intra, "1m"), true);
  assert.equal(fit.barsFitTf(daily, "1d"), true);
  assert.equal(fit.barsFitTf(daily, "1w"), true);
});

test("crossedAlert fires 99 → 101 at 100 and ignores a 0.2% proximity miss", () => {
  const qx = loadQx();
  assert.equal(qx.crossedAlert(99, 101, 100), true);
  assert.equal(qx.crossedAlert(101, 99, 100), true);
  assert.equal(qx.crossedAlert(99, 99.5, 100), false);
  assert.equal(qx.crossedAlert(99.9, 100.05, 100), true);
  assert.equal(qx.crossedAlert(null, 100, 100), true);
});

test("retCal uses 365 calendar days, not 252 sessions", () => {
  const qx = loadQx();
  const d = [];
  const t1 = Date.parse("2025-09-15T20:00:00Z") / 1000;
  const t2 = Date.parse("2026-09-15T20:00:00Z") / 1000;
  d.push({ time: t1, close: 100 });
  for (let i = 1; i < 252; i++) d.push({ time: t1 + i * 86400, close: 110 });
  d.push({ time: t2, close: 141.56 });
  const cal = qx.retCal(d, 365);
  assert.ok(Math.abs(cal - 41.56) < 0.05, "cal=" + cal);
  assert.match(engine, /retCal\(d,365\)/);
  assert.match(search, /retCal\(bars, 365\)/);
  assert.match(search, /function retCal/);
});

test("indux no longer hides the quote HUD", () => {
  assert.doesNotMatch(indux, /#quote,#desk-intel/);
  assert.match(indux, /#quote \.sell,#quote \.buy,#desk-intel,#intel/);
  assert.match(html, /#symin,#tab-search\{display:flex!important/);
  assert.doesNotMatch(html, /#dock,#mini,#symin\{display:none/);
});

test("alignHist drops missing ETF flow days instead of painting zero", () => {
  const fuse = loadFuse();
  const hist = [{ d: "2026-01-05", f: 1.5e9 }];
  const bars = [
    { time: Date.parse("2026-01-05T20:00:00Z") / 1000 },
    { time: Date.parse("2026-01-06T20:00:00Z") / 1000 },
    { time: Date.parse("2026-01-07T20:00:00Z") / 1000 }
  ];
  const pts = fuse.alignHist(hist, bars);
  assert.equal(pts.length, 1);
  assert.equal(pts[0].raw, 1.5e9);
  assert.ok(pts.every(function (p) { return p.raw != null; }));
});

test("escHtml and safeHref block script and non-http hrefs", () => {
  const qx = loadQx();
  assert.match(qx.escHtml("<img src=x onerror=alert(1)>"), /lt;img/);
  const amp = qx.escHtml("a&b");
  assert.ok(amp.indexOf("amp;") > 0);
  assert.equal(qx.safeHref("javascript:alert(1)"), "");
  assert.equal(qx.safeHref("https://justhodl.ai/x"), "https://justhodl.ai/x");
  assert.match(engine, /rel=noopener/);
});

test("Overview and Statistics fall back to FMP pe / mkt_cap", () => {
  assert.match(search, /fmp && fmp\.mkt_cap/);
  assert.match(search, /fmp && fmp\.pe/);
  assert.match(search, /\["P\/E \(TTM\)"/);
});
