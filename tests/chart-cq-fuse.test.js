const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const fuseSrc = fs.readFileSync(path.join(root, "jh-cq-fuse.js"), "utf8");
const catalog = fs.readFileSync(path.join(root, "jh-chart-catalog.js"), "utf8");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const search = fs.readFileSync(path.join(root, "jh-chart-tvsearch.js"), "utf8");
const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
const cryptoIdx = fs.readFileSync(path.join(root, "crypto/index.html"), "utf8");
const onchain = fs.readFileSync(path.join(root, "onchain.html"), "utf8");
const cryptoHtml = fs.readFileSync(path.join(root, "crypto.html"), "utf8");

function dates(n, start) {
  const out = [];
  const t0 = Date.parse(start + "T00:00:00Z");
  for (let i = 0; i < n; i++) out.push(new Date(t0 + i * 86400000).toISOString().slice(0, 10));
  return out;
}

const FIX = {
  series: {
    generated_at: "2026-09-21T21:05:07Z",
    series: {
      btc_mvrv: { d: dates(10, "2025-07-02"), v: [2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0] },
      btc_hashrate: { d: dates(10, "2025-07-02"), v: [400, 401, 402, 403, 404, 405, 406, 407, 408, 409] },
      btc_sopr: { d: dates(10, "2025-07-02"), v: [1.01, 1.02, 1.01, 1.00, 0.99, 1.01, 1.02, 1.03, 1.01, 1.02] }
    },
    twins: {
      btc_mvrv: { d: ["2010-07-18", "2010-07-25", "2025-07-02"], v: [0.4, 0.5, 1.9] }
    }
  },
  onchain: {
    generated_at: "2026-09-21T21:05:07Z",
    plan_note: "Professional tier: 1y API window",
    n_metrics: 3,
    composite_onchain_risk_z: -0.01,
    metrics: {
      btc_mvrv: { value: 1.52, z365: 0.17, pctl_1y: 64, label: "MVRV Ratio", category: "market_indicator", unit: "ratio", hist_read: "constructive" },
      btc_hashrate: { value: 409, z365: -0.2, pctl_1y: 40, label: "Hashrate", category: "network_data", unit: "EH/s", hist_read: "steady" },
      btc_sopr: { value: 1.02, z365: 0.4, pctl_1y: 55, label: "SOPR", category: "market_indicator", unit: "ratio", hist_read: "profit taking light" }
    },
    forecasts: { method: "Ensemble median", btc: { h30: { exp_pct: 1.2 }, h90: { exp_pct: 4 }, h180: { exp_pct: 8 }, h365: { exp_pct: 20 } } }
  },
  feed: {
    generated_at: "2026-09-21T10:52:39Z",
    n_metrics: 3,
    metrics: {
      btc_market_indicator_sopr: {
        path: "btc/market-indicator/sopr",
        asof: "2026-09-20",
        fields: { sopr: 1.02, a_sopr: 1.01, sth_sopr: 0.98, lth_sopr: 1.05 },
        prev: { sopr: 1.01, a_sopr: 1.00, sth_sopr: 0.97, lth_sopr: 1.04 }
      },
      "btc_exchange-flows_in-house-flow": {
        path: "btc/exchange-flows/in-house-flow",
        asof: "2026-09-21",
        fields: { flow_total: 13701.7, flow_mean: 29.5, transactions_count_flow: 464 },
        prev: { flow_total: 12000, flow_mean: 25, transactions_count_flow: 400 }
      },
      "btc_network-data_block-interval": {
        path: "btc/network-data/block-interval",
        asof: "2026-09-20",
        fields: { block_interval: 9.8 },
        prev: { block_interval: 9.6 }
      }
    }
  },
  spec: {
    plan_note: "Professional tier: 1y API window",
    metrics: [
      { name: "btc_mvrv", label: "MVRV Ratio", path: "/btc/market-indicator/mvrv", resolved_key: "mvrv", category: "market_indicator", value_keys: ["mvrv"] },
      { name: "btc_hashrate", label: "Hashrate", path: "/btc/network-data/hashrate", resolved_key: "hashrate", category: "network_data", value_keys: ["hashrate"] },
      { name: "btc_sopr", label: "SOPR", path: "/btc/market-indicator/sopr", resolved_key: "sopr", category: "market_indicator", value_keys: ["sopr", "a_sopr", "sth_sopr", "lth_sopr"] }
    ]
  },
  catalog: { n: 3, catalog: {} }
};

function fixtureFetch(url) {
  const u = String(url);
  let body = {};
  if (u.indexOf("cryptoquant-series") >= 0) body = FIX.series;
  else if (u.indexOf("cryptoquant-onchain") >= 0) body = FIX.onchain;
  else if (u.indexOf("cq-feed") >= 0) body = FIX.feed;
  else if (u.indexOf("cq-catalog") >= 0) body = FIX.catalog;
  else if (u.indexOf("cryptoquant-spec") >= 0) body = FIX.spec;
  else throw new Error("unexpected " + u);
  return Promise.resolve({ ok: true, json: async () => JSON.parse(JSON.stringify(body)) });
}

function loadFuse() {
  const ctx = { window: {}, fetch: fixtureFetch, Date, Math, isFinite, Number, String, Object, Array, Promise, console };
  ctx.window = ctx;
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(fuseSrc, ctx);
  return ctx;
}

test("chart.html and crypto desk load the CQ fuse", () => {
  assert.match(html, /jh-cq-fuse\.js\?v=20260921ac-fuse/);
  assert.match(html, /jh-chart-catalog\.js\?v=20260921ac-fuse/);
  assert.match(html, /jh-chart-engine\.js\?v=20260921ac-fuse/);
  assert.match(cryptoIdx, /jh-cq-fuse\.js\?v=20260921ac-fuse/);
  assert.match(cryptoIdx, /JHCqFuse\.paneHTML/);
  assert.match(cryptoIdx, /JHCqFuse\.load/);
  assert.match(cryptoHtml, /location\.replace\("\/crypto\/"\)/);
  assert.match(onchain, /chart\.html\?s=CQ:\$\{k\}/);
  assert.match(engine, /v12\.34/);
  assert.match(engine, /__jhChartEngineV1239/);
  assert.match(engine, /CQSNAP/);
  assert.match(catalog, /JHCqFuse/);
  assert.match(search, /no harvest series/);
});

test("fuse search finds harvest series and extra snapshots, never invents history", async () => {
  const ctx = loadFuse();
  const pack = await ctx.JHCqFuse.load();
  assert.equal(pack.n_series, 3);
  assert.ok(pack.n_snaps >= 6, "sibling fields a_sopr/sth/lth + in-house + block_interval");
  const hashrate = ctx.JHCqFuse.searchHits("hashrate");
  assert.ok(hashrate.some(function (h) { return h.s === "CQ:btc_hashrate" && h.chartable === true; }));
  const asopr = ctx.JHCqFuse.searchHits("a_sopr");
  assert.ok(asopr.some(function (h) { return /a_sopr/.test(h.s) && h.chartable === false; }));
  const house = ctx.JHCqFuse.searchHits("in-house");
  assert.ok(house.some(function (h) { return h.chartable === false && /in-house/.test(h.s + h.name + h.extra); }));
  const snapK = await ctx.JHCqFuse.klines("CQSNAP:btc/market-indicator/sopr:a_sopr");
  assert.equal(snapK, null);
  const pane = ctx.JHCqFuse.paneHTML({});
  assert.match(pane, /Chart CQ:btc_mvrv/);
  assert.match(pane, /chart\.html\?s=CQ:btc_hashrate/);
  assert.match(pane, /a_sopr/);
  assert.match(pane, /NO HARVEST SERIES/);
  assert.match(pane, /id='pane-cq'/);
});

test("fuse klines merges twins on overlap like catalog", async () => {
  const ctx = loadFuse();
  const r = await ctx.JHCqFuse.klines("CQ:btc_mvrv");
  assert.ok(r && r.d && r.d.length >= 10);
  assert.equal(r.d[0].close, 0.4);
  const byDay = {};
  r.d.forEach(function (b) { byDay[new Date(b.time * 1000).toISOString().slice(0, 10)] = b.close; });
  assert.equal(byDay["2025-07-02"], 2.1);
  assert.match(r.src, /twins\+harvest/);
});

test("catalog still plots CQ without fuse and routes snapshots to the crypto desk", async () => {
  const fetch = async (url) => {
    if (String(url).indexOf("cryptoquant-series") >= 0) {
      return { ok: true, json: async () => FIX.series };
    }
    throw new Error("unexpected " + url);
  };
  const ctx = { window: {}, fetch, Date, Math, isFinite, Number, String, Object, Array, Promise, console, location: { href: "" } };
  ctx.window = ctx;
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(catalog, ctx);
  const r = await ctx.JHChartCatalog.klines("CQ:btc_mvrv");
  assert.ok(r && r.d && r.d.length >= 10);
  assert.equal(r.d[0].close, 0.4);
  assert.equal(await ctx.JHChartCatalog.klines("CQSNAP:btc/market-indicator/sopr:a_sopr"), null);
  assert.equal(ctx.JHChartCatalog.go("CQSNAP:btc/market-indicator/sopr:a_sopr"), true);
  assert.match(ctx.location.href, /\/crypto\/\?tab=cq&snap=/);
  const specs = ctx.JHChartCatalog.cqOscSpecs();
  assert.ok(specs.length >= 54);
  assert.equal(specs[0].on, 0);
});

test("catalog + fuse search is harvest-dynamic", async () => {
  const ctx = loadFuse();
  vm.runInContext(catalog, ctx);
  await ctx.JHCqFuse.load();
  const hits = ctx.JHChartCatalog.suggest("a_sopr", 40);
  assert.ok(hits.some(function (h) { return /a_sopr/.test(h.s) && /snapshot/i.test(h.extra); }));
  const h2 = ctx.JHChartCatalog.suggest("hashrate", 40);
  assert.ok(h2.some(function (h) { return h.s === "CQ:btc_hashrate"; }));
});
