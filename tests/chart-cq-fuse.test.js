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
      },
      "btc_network-indicator_cdd": {
        path: "btc/network-indicator/cdd",
        asof: "2026-09-21",
        fields: { cdd: 1234567, sa_cdd: 0.42 },
        prev: { cdd: 1100000, sa_cdd: 0.40 }
      },
      "community_bitcoin-mvrv-z-score": {
        path: "community/bitcoin-mvrv-z-score",
        asof: "2026-09-21",
        fields: { mvrv_ratio_zscore: 1.85 },
        prev: { mvrv_ratio_zscore: 1.70 }
      },
      "eth_eth2_total-value-staked": {
        path: "eth/eth2/total-value-staked",
        asof: "2026-09-21",
        fields: { total_value_staked: 34000000 },
        prev: { total_value_staked: 33900000 }
      },
      "btc_lightning-network_stats": {
        path: "btc/lightning-network/stats",
        asof: "2026-09-21",
        fields: { number_of_nodes: 12000 },
        prev: { number_of_nodes: 11900 }
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
  catalog: { n: 3, catalog: {} },
  universe: {
    generated_from: "docs.cryptoquant.com/catalog/catalog.json",
    n_v1: 242,
    n_v2: 11,
    n_rows: 6,
    n_armed: 4,
    plan_note: "Professional tier: 1y API window",
    rows: [
      { id: "btc_network_indicator_cdd_cdd", name: "Coin Days Destroyed", path: "/btc/network-indicator/cdd", field: "cdd", group: "Bitcoin", category: "BTC Network Indicator", status: "armed", v2: false },
      { id: "btc_network_indicator_dormancy_average_dormancy", name: "Dormancy", path: "/btc/network-indicator/dormancy", field: "average_dormancy", group: "Bitcoin", category: "BTC Network Indicator", status: "armed", v2: false },
      { id: "community_mvrv_ratio_zscore", name: "MVRV Z-score", path: "/community/bitcoin-mvrv-z-score", field: "mvrv_ratio_zscore", group: "v2", category: "v2_community", status: "armed", v2: true },
      { id: "eth_eth2_total_value_staked_total_value_staked", name: "Total Value Staked", path: "/eth/eth2/total-value-staked", field: "total_value_staked", group: "Ethereum", category: "ETH 2.0", status: "armed", v2: false },
      { id: "discovery_path", name: "Endpoints", path: "/discovery/endpoints", field: "path", group: "Discovery", category: "Available Endpoints", status: "catalog", v2: false },
      { id: "btc_name", name: "Entity List", path: "/btc/status/entity-list", field: "name", group: "Bitcoin", category: "BTC Entity Status", status: "catalog", v2: false }
    ]
  }
};

function fixtureFetch(url) {
  const u = String(url);
  let body = {};
  if (u.indexOf("cryptoquant-series") >= 0) body = FIX.series;
  else if (u.indexOf("cryptoquant-onchain") >= 0) body = FIX.onchain;
  else if (u.indexOf("cq-feed") >= 0) body = FIX.feed;
  else if (u.indexOf("cq-catalog") >= 0) body = FIX.catalog;
  else if (u.indexOf("cryptoquant-spec") >= 0) body = FIX.spec;
  else if (u.indexOf("cq-universe") >= 0) body = FIX.universe;
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
  assert.match(html, /jh-cq-fuse\.js\?v=20260922ab-cqind/);
  assert.match(html, /jh-chart-catalog\.js\?v=20260922ab-cqind/);
  assert.match(html, /jh-chart-engine\.js\?v=20260922ab-cqind/);
  assert.match(cryptoIdx, /jh-cq-fuse\.js\?v=20260922ab-cqind/);
  assert.match(cryptoIdx, /JHCqFuse\.paneHTML/);
  assert.match(cryptoIdx, /JHCqFuse\.load/);
  assert.match(cryptoIdx, /data-arm/);
  assert.match(cryptoHtml, /location\.replace\("\/crypto\/"\)/);
  assert.match(onchain, /chart\.html\?s=CQ:\$\{k\}/);
  assert.match(engine, /v12\.34/);
  assert.match(engine, /__jhChartEngineV1239/);
  assert.match(engine, /CQSNAP/);
  assert.match(engine, /CQARM/);
  assert.match(engine, /CQDOC/);
  assert.match(catalog, /JHCqFuse/);
  assert.match(search, /cq-feed live print/);
});

test("fuse search finds harvest series and extra snapshots, never invents history", async () => {
  const ctx = loadFuse();
  const pack = await ctx.JHCqFuse.load();
  assert.equal(pack.n_series, 3);
  assert.ok(pack.n_snaps >= 10, "sibling fields + CDD + MVRV Z + ETH2 + lightning");
  assert.ok(pack.n_armed >= 1, "universe armed dormancy (not in feed fixture)");
  assert.ok(pack.n_docs >= 2, "catalog-only discovery/entity-list");
  const hashrate = ctx.JHCqFuse.searchHits("hashrate");
  assert.ok(hashrate.some(function (h) { return h.s === "CQ:btc_hashrate" && h.chartable === true; }));
  const asopr = ctx.JHCqFuse.searchHits("a_sopr");
  assert.ok(asopr.some(function (h) { return /a_sopr/.test(h.s) && h.chartable === false && /1\.01/.test(h.extra) && /live print/.test(h.extra); }));
  const house = ctx.JHCqFuse.searchHits("in-house");
  assert.ok(house.some(function (h) { return h.chartable === false && /in-house/.test(h.s + h.name + h.extra); }));
  const snapK = await ctx.JHCqFuse.klines("CQSNAP:btc/market-indicator/sopr:a_sopr");
  assert.equal(snapK, null);
  const pane = ctx.JHCqFuse.paneHTML({});
  assert.match(pane, /Chart CQ:btc_mvrv/);
  assert.match(pane, /chart\.html\?s=CQ:btc_hashrate/);
  assert.match(pane, /a_sopr/);
  assert.match(pane, /LIVE CQ-FEED INDICATORS/);
  assert.doesNotMatch(pane, /NO HARVEST SERIES/);
  assert.match(pane, /id='pane-cq'/);
  assert.match(pane, /AWAITING FIRST EOD PULL/);
  assert.match(pane, /CATALOG-ONLY/);
});

test("feed prints CDD / MVRV Z / ETH2 / lightning as live numbers — dormancy stays armed — no fake bars", async () => {
  const ctx = loadFuse();
  await ctx.JHCqFuse.load();
  const cdd = ctx.JHCqFuse.searchHits("cdd");
  assert.ok(cdd.some(function (h) { return /^CQSNAP:/.test(h.s) && /cdd/i.test(h.s + h.name) && h.chartable === false && /live print/.test(h.extra); }));
  const dorm = ctx.JHCqFuse.searchHits("dormancy");
  assert.ok(dorm.some(function (h) { return /^CQARM:/.test(h.s) && h.chartable === false; }));
  const z = ctx.JHCqFuse.searchHits("mvrv z");
  assert.ok(z.some(function (h) { return /^CQSNAP:/.test(h.s) && /zscore|z-score/i.test(h.s + h.name + h.extra) && /1\.85/.test(h.extra); }));
  const eth2 = ctx.JHCqFuse.searchHits("eth2");
  assert.ok(eth2.some(function (h) { return /^CQSNAP:/.test(h.s) && h.chartable === false; }));
  const ln = ctx.JHCqFuse.searchHits("lightning");
  assert.ok(ln.some(function (h) { return /lightning/i.test(h.s + h.name + h.extra + (h.blob || "")) && /live print/.test(h.extra); }));
  assert.equal(await ctx.JHCqFuse.klines("CQSNAP:btc/network-indicator/cdd:cdd"), null);
  assert.equal(await ctx.JHCqFuse.klines("CQARM:btc_network_indicator_cdd_cdd"), null);
  assert.equal(await ctx.JHCqFuse.klines("CQDOC:discovery_path"), null);
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
  ctx.location.href = "";
  assert.equal(await ctx.JHChartCatalog.klines("CQARM:btc_network_indicator_cdd_cdd"), null);
  assert.equal(ctx.JHChartCatalog.go("CQARM:btc_network_indicator_cdd_cdd"), true);
  assert.match(ctx.location.href, /\/crypto\/\?tab=cq&arm=/);
  ctx.location.href = "";
  assert.equal(ctx.JHChartCatalog.go("CQDOC:discovery_path"), true);
  assert.match(ctx.location.href, /\/crypto\/\?tab=cq&doc=/);
  const specs = ctx.JHChartCatalog.cqOscSpecs();
  assert.ok(specs.length >= 54);
  assert.equal(specs[0].on, 0);
});

test("catalog + fuse search is harvest-dynamic", async () => {
  const ctx = loadFuse();
  vm.runInContext(catalog, ctx);
  await ctx.JHCqFuse.load();
  const hits = ctx.JHChartCatalog.suggest("a_sopr", 40);
  assert.ok(hits.some(function (h) { return /a_sopr/.test(h.s) && /live print/i.test(h.extra); }));
  const h2 = ctx.JHChartCatalog.suggest("hashrate", 40);
  assert.ok(h2.some(function (h) { return h.s === "CQ:btc_hashrate"; }));
  const h3 = ctx.JHChartCatalog.suggest("cdd", 40);
  assert.ok(h3.some(function (h) { return /^CQSNAP:/.test(h.s) && /live print/i.test(h.extra); }));
  const h4 = ctx.JHChartCatalog.suggest("cryptoquant", 400);
  assert.ok(h4.length >= 12, "cq catalog query returns the harvest, not 16 stock hits");
});
