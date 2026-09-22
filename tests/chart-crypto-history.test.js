const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { pathToFileURL } = require("node:url");

const root = path.join(__dirname, "..");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const catalog = fs.readFileSync(path.join(root, "jh-chart-catalog.js"), "utf8");
const search = fs.readFileSync(path.join(root, "jh-chart-tvsearch.js"), "utf8");
const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
const helperURL = pathToFileURL(path.join(root, "cloudflare/workers/justhodl-data-proxy/src/warehouse-ohlc.js"));

test("crypto history + CQ stamps are on chart.html", () => {
  assert.match(html, /jh-chart-engine\.js\?v=20260922aa-cqfull/);
  assert.match(html, /jh-chart-catalog\.js\?v=20260922aa-cqfull/);
  assert.match(html, /jh-chart-tvsearch\.js\?v=20260922aa-cqfull/);
  assert.match(html, /jh-chart-indux\.js\?v=20260922aa-cqfull/);
  assert.match(html, /jh-cq-fuse\.js\?v=20260922aa-cqfull/);
  assert.match(engine, /v12\.34/);
  assert.match(engine, /__jhChartEngineV1239/);
});

test("engine merges Yahoo onto katlin crypto-bars only", () => {
  assert.match(engine, /function isCryptoTape/);
  assert.match(engine, /crypto-bars/);
  assert.match(engine, /nowarehouse=1/);
  assert.match(engine, /mergeByDay\(ydC, d\)/);
  assert.doesNotMatch(engine.slice(engine.indexOf("async function klines"), engine.indexOf("function computeChange")), /nowarehouse=1.*AAPL|AAPL.*nowarehouse=1/);
  const klines = engine.slice(engine.indexOf("async function klines"), engine.indexOf("function computeChange"));
  assert.match(klines, /isCryptoTape/);
  assert.match(klines, /polygon\+yahoo/);
});

test("catalog lists every CryptoQuant harvest id and twins merge", () => {
  const block = catalog.match(/var CQ_META = \[([\s\S]*?)\];/);
  assert.ok(block, "CQ_META present");
  const n = (block[1].match(/\["[a-z0-9_]+"/g) || []).length;
  assert.ok(n >= 54, "CQ_META has " + n + " series, want >= 54");
  assert.match(catalog, /btc_hashrate/);
  assert.match(catalog, /btc_funding_rates/);
  assert.match(catalog, /stablecoin_supply_total/);
  assert.match(catalog, /function cqOscSpecs/);
  assert.match(catalog, /function mergeDv/);
  assert.match(catalog, /twins\+harvest/);
  assert.match(catalog, /function cqRow/);
});

test("on-chain desk charts every harvest series", () => {
  assert.match(search, /data-cq=/);
  assert.match(search, /class=cqtab/);
  assert.match(search, /BTCUSDT: 1/);
  assert.match(search, /twins extend some series to 2010/);
});

test("mergeBarsPrefer keeps warehouse prints on overlap and prepends Yahoo", async () => {
  const { mergeBarsPrefer, yahooChartSymbol, isCryptoWarehouse, utcDay } = await import(helperURL);
  assert.equal(yahooChartSymbol("BTCUSDT"), "BTC-USD");
  assert.equal(isCryptoWarehouse("data/warm/katlin/crypto-bars/BTC.json.gz"), true);
  assert.equal(isCryptoWarehouse("data/warm/tv-bars/universe/US__AAPL.json.gz"), false);
  const older = [
    { time: 1410912000, open: 450, high: 460, low: 440, close: 455, value: 1 },
    { time: 1599264000, open: 9999, high: 9999, low: 9999, close: 8888, value: 1 }
  ];
  const newer = [
    { time: 1599264000, open: 10000, high: 10100, low: 9900, close: 10050, value: 10 },
    { time: 1601856000, open: 11000, high: 11100, low: 10900, close: 11050, value: 10 }
  ];
  const m = mergeBarsPrefer(older, newer);
  assert.equal(m.length, 3);
  assert.equal(m[0].close, 455);
  assert.equal(m[0].value, 0);
  assert.equal(utcDay(m[0].time), 1410912000);
  assert.equal(m[1].close, 10050);
  assert.equal(m[1].value, 10);
  assert.equal(m[2].close, 11050);
});

test("catalog klines prefers harvest on overlap and twins for 2010 history", async () => {
  const fetch = async (url) => {
    if (String(url).indexOf("cryptoquant-series") >= 0) {
      return {
        ok: true,
        json: async () => ({
          generated_at: "2026-09-21T00:00:00Z",
          series: { btc_mvrv: { d: ["2025-07-02", "2025-07-03", "2025-07-04", "2025-07-05", "2025-07-06", "2025-07-07", "2025-07-08", "2025-07-09"], v: [2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8] } },
          twins: { btc_mvrv: { d: ["2010-07-18", "2010-07-25", "2025-07-02"], v: [0.4, 0.5, 1.9] } }
        })
      };
    }
    throw new Error("unexpected " + url);
  };
  const ctx = { window: {}, fetch, Date, Math, isFinite, Number, String, Object, Array, Promise, console };
  ctx.window = ctx;
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(catalog, ctx);
  const r = await ctx.JHChartCatalog.klines("CQ:btc_mvrv");
  assert.ok(r && r.d && r.d.length >= 10);
  assert.equal(r.d[0].close, 0.4);
  assert.equal(r.d[1].close, 0.5);
  const byDay = {};
  r.d.forEach(function (b) { byDay[new Date(b.time * 1000).toISOString().slice(0, 10)] = b.close; });
  assert.equal(byDay["2025-07-02"], 2.1);
  assert.equal(byDay["2025-07-09"], 2.8);
  assert.match(r.src, /twins\+harvest/);
  const specs = ctx.JHChartCatalog.cqOscSpecs();
  assert.ok(specs.length >= 54);
  assert.equal(specs[0].k, "cq");
});
