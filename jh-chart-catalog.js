/* Chart data catalog — classify JustHodl harvests, search them, plot full history when the warehouse has it.
 * FRED/NY Fed → PROXY /series (warehouse history). CryptoQuant → data/cryptoquant-series.json (harvest points).
 * CISS → data/ciss-stress.json. Snapshots (13F, FMP TTM) open desks, never invented daily history.
 */
(function (global) {
  "use strict";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var CQ = null, CISS = null;
  var CHIPS = [
    ["all", "All"],
    ["stocks", "Stocks"],
    ["etfs", "Funds"],
    ["macro", "Macro"],
    ["flows", "ETF flows"],
    ["inst", "13F"],
    ["chain", "On-chain"],
    ["bonds", "Bonds"],
    ["stress", "Stress"],
    ["energy", "Energy"],
    ["trade", "Trade"],
    ["crypto", "Crypto"],
    ["fx", "Forex"],
    ["lists", "Lists"],
    ["notes", "Notes"]
  ];
  var TAB_CLS = {
    stocks: { stock: 1 },
    etfs: { etf: 1, fund: 1, flows: 1 },
    crypto: { crypto: 1, onchain: 1, chain: 1 },
    fx: { fx: 1, forex: 1 },
    macro: { macro: 1, economy: 1 },
    flows: { flows: 1, etf: 1, fund: 1 },
    inst: { inst: 1, desk: 1, ownership: 1 },
    chain: { onchain: 1, chain: 1, crypto: 1 },
    bonds: { bonds: 1, bond: 1, credit: 1, economy: 1, macro: 1 },
    stress: { stress: 1, liquidity: 1, macro: 1 },
    energy: { energy: 1, commodity: 1 },
    trade: { trade: 1, ports: 1, exports: 1, macro: 1 },
    lists: {},
    notes: {},
    all: {}
  };

  function H(q, s, name, cat, extra, type) {
    return { q: q, s: s, name: name, cat: cat, extra: extra, type: type || cat };
  }

  /* Curated, chartable. extra is the honest history / cadence label. */
  var CURATED = [
    H(["sofr", "secured overnight"], "FRED:SOFR", "SOFR overnight rate", "macro", "FRED daily · warehouse history from 2018-04-03", "economy"),
    H(["effr", "effective federal funds", "fed funds", "dff", "fedfunds"], "FRED:DFF", "Effective Federal Funds Rate", "macro", "FRED daily · full history", "economy"),
    H(["us10y", "10y", "10 year", "dgs10", "treasury 10y"], "FRED:DGS10", "US 10-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["us2y", "2y", "dgs2"], "FRED:DGS2", "US 2-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["us5y", "5y", "dgs5"], "FRED:DGS5", "US 5-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["us30y", "30y", "dgs30"], "FRED:DGS30", "US 30-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["t10y2y", "2s10s", "yield curve"], "FRED:T10Y2Y", "10Y–2Y Treasury spread", "macro", "FRED daily · full history", "economy"),
    H(["t10y3m", "3m10y"], "FRED:T10Y3M", "10Y–3M Treasury spread", "macro", "FRED daily · full history", "economy"),
    H(["tips 10y", "dfii10", "real yield"], "FRED:DFII10", "10Y TIPS real yield", "macro", "FRED daily · full history", "economy"),
    H(["breakeven", "t10yie", "inflation breakeven"], "FRED:T10YIE", "10Y inflation breakeven", "macro", "FRED daily · full history", "economy"),
    H(["walcl", "fed balance sheet"], "FRED:WALCL", "Fed balance sheet (WALCL)", "macro", "FRED weekly · full history", "economy"),
    H(["rrp", "reverse repo", "rrpontsyd"], "FRED:RRPONTSYD", "ON RRP facility", "macro", "FRED daily · full history", "economy"),
    H(["reserve balances", "wresbal"], "FRED:WRESBAL", "Reserve balances", "macro", "FRED weekly · full history", "economy"),
    H(["cpi", "inflation"], "FRED:CPIAUCSL", "US CPI", "macro", "FRED monthly · full history", "economy"),
    H(["unrate", "unemployment"], "FRED:UNRATE", "Unemployment rate", "macro", "FRED monthly · full history since 1948", "economy"),
    H(["nfp", "payrolls", "payems"], "FRED:PAYEMS", "Nonfarm payrolls", "macro", "FRED monthly · full history", "economy"),
    H(["jobless", "icsa", "claims"], "FRED:ICSA", "Initial jobless claims", "macro", "FRED weekly · full history", "economy"),
    H(["m2", "m2sl"], "FRED:M2SL", "M2 money stock", "macro", "FRED monthly · full history", "economy"),
    H(["nfci", "chicago fed"], "FRED:NFCI", "Chicago Fed NFCI", "stress", "FRED weekly · full history", "stress"),
    H(["hy oas", "high yield spread", "bamlh0a0hym2"], "FRED:BAMLH0A0HYM2", "ICE BofA HY OAS", "bonds", "FRED daily · full history", "bonds"),
    H(["ig oas", "bamlc0a0cm"], "FRED:BAMLC0A0CM", "ICE BofA IG OAS", "bonds", "FRED daily · full history", "bonds"),
    H(["move", "move index"], "FRED:MOVE", "MOVE bond vol", "bonds", "FRED daily when present", "bonds"),
    H(["vixcls", "vix close"], "FRED:VIXCLS", "VIX close (FRED)", "stress", "FRED daily · full history", "stress"),
    H(["dollar", "dxy", "dtwexbgs", "broad dollar"], "FRED:DTWEXBGS", "Trade-weighted USD (broad)", "macro", "FRED daily · full history", "economy"),
    H(["wti", "oil", "dcoilwtico"], "FRED:DCOILWTICO", "WTI spot (FRED)", "energy", "FRED daily · full history", "energy"),
    H(["exports", "expgs", "us exports"], "FRED:EXPGS", "US goods & services exports", "trade", "FRED monthly · full history", "trade"),
    H(["imports", "impgs", "us imports"], "FRED:IMPGS", "US goods & services imports", "trade", "FRED monthly · full history", "trade"),
    H(["trade balance", "bopgstb", "bop"], "FRED:BOPGSTB", "US trade balance", "trade", "FRED monthly · full history", "trade"),
    H(["gdp", "gdpc1"], "FRED:GDPC1", "Real GDP", "macro", "FRED quarterly · full history", "economy"),
    H(["indpro", "industrial production"], "FRED:INDPRO", "Industrial production", "macro", "FRED monthly · full history", "economy"),
    H(["housing starts", "houst"], "FRED:HOUST", "Housing starts", "macro", "FRED monthly · full history", "economy"),
    H(["retail sales", "rsafs"], "FRED:RSAFS", "Advance retail sales", "macro", "FRED monthly · full history", "economy"),
    H(["sp500 fred"], "FRED:SP500", "S&P 500 (FRED)", "macro", "FRED daily · full history", "economy"),

    H(["mvrv", "btc mvrv"], "CQ:btc_mvrv", "BTC MVRV", "chain", "CryptoQuant EOD · harvest history (not live)", "onchain"),
    H(["sopr", "btc sopr"], "CQ:btc_sopr", "BTC SOPR", "chain", "CryptoQuant EOD · harvest history", "onchain"),
    H(["nupl"], "CQ:btc_nupl", "BTC NUPL", "chain", "CryptoQuant EOD · harvest history", "onchain"),
    H(["mpi", "miners position"], "CQ:btc_mpi", "BTC miner position index", "chain", "CryptoQuant EOD · harvest history", "onchain"),
    H(["whale ratio"], "CQ:btc_whale_ratio", "BTC whale ratio", "chain", "CryptoQuant EOD · harvest history", "onchain"),
    H(["exchange netflow", "btc netflow"], "CQ:btc_exchange_netflow", "BTC exchange netflow", "chain", "CryptoQuant EOD · harvest history", "onchain"),
    H(["ssr", "stablecoin supply ratio"], "CQ:btc_ssr", "BTC SSR", "chain", "CryptoQuant EOD · harvest history", "onchain"),
    H(["realized price"], "CQ:btc_realized_price", "BTC realized price", "chain", "CryptoQuant EOD · harvest history", "onchain"),
    H(["eth mvrv"], "CQ:eth_mvrv", "ETH MVRV", "chain", "CryptoQuant EOD · harvest history", "onchain"),

    H(["ciss", "euro stress", "sovereign stress"], "CISS:ea", "EA CISS composite", "stress", "ECB CISS · harvest history from 2000", "stress"),

    H(["13f", "13-f", "institutional ownership", "smart money 13f"], "DESK:inst", "13F institutional book", "inst", "SEC 13F · quarterly lagged snapshot", "desk"),
    H(["etf holdings", "holdings book", "constituents"], "DESK:etf", "ETF holdings + vs-SPX ranks", "flows", "Massive ETF Global + holdings-index", "desk"),
    H(["etf flow", "creations", "redemptions", "inflow", "outflow"], "DESK:flow", "ETF creations / redemptions", "flows", "Massive ETF Global · delayed tape", "desk"),
    H(["valuation", "pe ratio", "ttm ratios"], "DESK:val", "Valuation (FMP EOD/TTM)", "stocks", "FMP Ultimate · TTM snapshot, not a live print", "desk"),
    H(["financials", "income statement", "filings"], "DESK:fin", "Financials (FMP filings)", "stocks", "FMP Ultimate · annual/quarterly filings", "desk"),
    H(["identity", "figi", "openfigi", "cusip"], "DESK:ident", "Identity / OpenFIGI", "stocks", "OpenFIGI symbology master", "desk"),
    H(["on-chain desk", "onchain desk"], "DESK:chain", "On-chain desk", "chain", "CryptoQuant EOD", "desk"),
    H(["pressure", "buying pressure"], "DESK:press", "Buying / selling pressure", "flows", "ETF look-through · inferred", "desk"),
    H(["liquidity pulse", "rrp liquidity"], "FRED:RRPONTSYD", "ON RRP (liquidity pulse)", "stress", "FRED daily · full history", "stress")
  ];

  function norm(q) {
    return String(q || "").toLowerCase().replace(/[^a-z0-9:+.\- ]+/g, " ").replace(/\s+/g, " ").trim();
  }

  function chips() { return CHIPS; }

  function tabMatch(tab, cls, type, s) {
    if (!tab || tab === "all") return true;
    if (tab === "lists" || tab === "notes") return true;
    var want = TAB_CLS[tab];
    if (!want || !Object.keys(want).length) return true;
    var c = String(cls || type || "").toLowerCase();
    if (want[c]) return true;
    var hit = CURATED.filter(function (x) { return x.s === s; })[0];
    if (hit && (hit.cat === tab || want[hit.cat] || want[hit.type])) return true;
    if (tab === "macro" && /^FRED:/i.test(s)) return true;
    if (tab === "chain" && /^CQ:/i.test(s)) return true;
    if (tab === "stress" && /^CISS:/i.test(s)) return true;
    if (tab === "inst" && /^DESK:inst/i.test(s)) return true;
    return false;
  }

  function aliasHits(q) {
    var n = norm(q);
    if (!n) return [];
    var out = [], seen = {};
    CURATED.forEach(function (a) {
      var hit = a.q.some(function (k) { return k === n || n.indexOf(k) >= 0 || k.indexOf(n) >= 0; });
      if (hit && !seen[a.s]) {
        seen[a.s] = 1;
        out.push({ s: a.s, name: a.name, extra: a.extra, type: a.type, cat: a.cat });
      }
    });
    return out;
  }

  function search(q) {
    return aliasHits(q);
  }

  function classify(s) {
    s = String(s || "");
    if (/^DESK:/i.test(s)) return "desk";
    if (/^CQ:/i.test(s)) return "onchain";
    if (/^CISS:/i.test(s)) return "stress";
    if (/^FRED:/i.test(s)) return "macro";
    if (/^NYFED:/i.test(s)) return "macro";
    return "";
  }

  function chartId(raw) {
    var s = String(raw || "").trim();
    if (!s) return "";
    var low = s.toLowerCase();
    if (/^desk:/i.test(s)) return "DESK:" + s.split(":")[1];
    if (/^cq:/i.test(s)) return "CQ:" + s.split(":").slice(1).join(":");
    if (/^ciss:/i.test(s)) return "CISS:" + s.split(":").slice(1).join(":");
    if (/^fred:/i.test(s)) return "FRED:" + s.split(":")[1].toUpperCase();
    if (/^nyfed:/i.test(s)) {
      var t = s.split(":")[1] || "";
      var F = { sofr: "SOFR", effr: "EFFR", obfr: "OBFR", tgcr: "TGCR", bgcr: "BGCR", rrp: "RRPONTSYD" };
      return "FRED:" + (F[t.toLowerCase()] || t.toUpperCase());
    }
    if (/^indicator-bus:/i.test(s)) {
      var id = s.split(":").slice(1).join(":");
      if (/mvrv|sopr|nupl|netflow|ssr/i.test(id)) return "CQ:" + id.replace(/^BTC_?/i, "btc_").toLowerCase();
      return "FRED:" + id.split(":").pop().toUpperCase();
    }
    var hit = CURATED.filter(function (x) { return x.s.toLowerCase() === low || x.q.indexOf(low) >= 0; })[0];
    if (hit && hit.s.indexOf(":") >= 0) return hit.s;
    return "";
  }

  function mapRow(row) {
    if (!row) return null;
    var id = row.id || row.symbol || row.ticker || "";
    var kind = String(row.kind || row.type || "");
    var name = row.name || row.title || "";
    var provider = String(row.provider || "");
    var mapped = chartId(id);
    if (row.src && /^cryptoquant:/i.test(row.src) && /mvrv/i.test(id + name)) mapped = mapped || "CQ:btc_mvrv";
    if (row.src && /^yahoo:/i.test(row.src)) mapped = mapped || String(row.src).split(":").slice(1).join(":");
    if (provider === "fred" && (kind === "series" || row.chartable)) {
      mapped = "FRED:" + String(row.symbol || id.split(":").pop()).toUpperCase();
    }
    if (provider === "nyfed" && (kind === "series" || row.chartable)) {
      mapped = chartId(id) || ("FRED:" + String(row.symbol || id.split(":").pop()).toUpperCase());
    }
    if (/13f/i.test(id + " " + name)) {
      return { s: "DESK:inst", name: name || "13F book", extra: "SEC 13F · quarterly lagged", type: "desk" };
    }
    if (/holding|constituent/i.test(name) && /etf/i.test(name + " " + id)) {
      return { s: "DESK:etf", name: name, extra: "ETF holdings book", type: "desk" };
    }
    if (mapped) {
      var extra = [];
      if (row.first && row.last) extra.push("history " + row.first + " → " + row.last);
      else if (row.first) extra.push("from " + row.first);
      if (row.n) extra.push(row.n + " obs");
      if (row.freq) extra.push(row.freq);
      extra.push(row.provider_name || provider || kind);
      if (row.chartable === false && !/^DESK:/i.test(mapped)) extra.push("snapshot");
      return { s: mapped, name: name || mapped, extra: extra.filter(Boolean).join(" · "), type: classify(mapped) || kind || "economy" };
    }
    if (kind === "dataset" && row.chartable === false) return null;
    return null;
  }

  function go(s, dest) {
    s = String(s || "");
    if (!/^DESK:/i.test(s)) return false;
    var tab = s.split(":")[1] || "over";
    if (global.jhOpenDataTypePanel) global.jhOpenDataTypePanel(tab);
    else if (global.jhOpenDataType) global.jhOpenDataType(null);
    return true;
  }

  function dvBars(d, v) {
    var out = [], i;
    if (!d || !v) return out;
    for (i = 0; i < d.length && i < v.length; i++) {
      var t = Math.floor(Date.parse(String(d[i]).length <= 10 ? d[i] + "T00:00:00Z" : d[i]) / 1000);
      var c = +v[i];
      if (!isFinite(t) || t <= 0 || !isFinite(c)) continue;
      out.push({ time: t, open: c, high: c, low: c, close: c, volume: 0 });
    }
    return out;
  }
  function ptsBars(pts) {
    var out = [], i;
    if (!pts) return out;
    for (i = 0; i < pts.length; i++) {
      var p = pts[i];
      var t = Array.isArray(p) ? p[0] : (p.t || p.date);
      var c = Array.isArray(p) ? p[1] : (p.v != null ? p.v : p.value);
      t = Math.floor(Date.parse(String(t).length <= 10 ? t + "T00:00:00Z" : t) / 1000);
      c = +c;
      if (!isFinite(t) || !isFinite(c)) continue;
      out.push({ time: t, open: c, high: c, low: c, close: c, volume: 0 });
    }
    return out;
  }

  function loadJson(url) {
    return fetch(url, { cache: "no-store" }).then(function (r) {
      if (!r.ok) throw new Error("http " + r.status);
      return r.json();
    });
  }

  function loadCQ() {
    if (CQ) return Promise.resolve(CQ);
    return loadJson("/data/cryptoquant-series.json").then(function (j) { CQ = j; return j; });
  }
  function loadCISS() {
    if (CISS) return Promise.resolve(CISS);
    return loadJson("/data/ciss-stress.json").then(function (j) { CISS = j; return j; });
  }

  function cqKey(sym) {
    var k = String(sym || "").replace(/^CQ:/i, "");
    return k;
  }

  async function klines(sym) {
    var s = String(sym || "");
    if (/^CQ:/i.test(s)) {
      var doc = await loadCQ();
      var k = cqKey(s);
      var row = (doc.series && (doc.series[k] || doc.series[k.toLowerCase()])) ||
        (doc.twins && (doc.twins[k] || doc.twins[k.toLowerCase()]));
      if (!row && doc.series) {
        var want = k.toLowerCase().replace(/^btc_/, "");
        Object.keys(doc.series).forEach(function (id) {
          if (!row && id.toLowerCase().indexOf(want) >= 0) row = doc.series[id];
        });
        if (!row && doc.twins) Object.keys(doc.twins).forEach(function (id) {
          if (!row && id.toLowerCase().indexOf(want) >= 0) row = doc.twins[id];
        });
      }
      if (!row) return null;
      var d = dvBars(row.d, row.v);
      if (d.length < 8) return null;
      var src = "CryptoQuant EOD · " + d.length + " pts " + String(row.d[0]).slice(0, 10) + " → " + String(row.d[row.d.length - 1]).slice(0, 10);
      return { d: d, src: src };
    }
    if (/^CISS:/i.test(s)) {
      var ciss = await loadCISS();
      var rows = ciss.series || [];
      var want = s.split(":")[1] || "ea";
      var hit = null;
      for (var i = 0; i < rows.length; i++) {
        var r = rows[i];
        if (want === "ea" && (r.category === "ea_headline" || /headline|composite/i.test(r.label || ""))) { hit = r; break; }
        if (String(r.id) === want || String(r.key) === want || String(r.area).toLowerCase() === want.toLowerCase()) { hit = r; break; }
      }
      if (!hit && rows[0]) hit = rows.filter(function (x) { return x.category === "ea_headline"; })[0] || rows[0];
      var d2 = ptsBars(hit && hit.points);
      if (d2.length < 8) return null;
      var src2 = "ECB CISS · " + d2.length + " pts " + (hit.start_date || "") + " → " + (hit.latest_date || "") + " · " + (hit.label || "");
      return { d: d2, src: src2 };
    }
    return null;
  }

  global.JHChartCatalog = {
    chips: chips,
    tabMatch: tabMatch,
    aliasHits: aliasHits,
    search: search,
    classify: classify,
    chartId: chartId,
    mapRow: mapRow,
    go: go,
    klines: klines,
    curated: CURATED
  };
})(typeof window !== "undefined" ? window : globalThis);
