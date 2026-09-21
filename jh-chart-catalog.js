/* Chart data catalog — classify JustHodl harvests, search them, plot full history when the warehouse has it.
 * FRED/NY Fed → PROXY /series (warehouse history). CryptoQuant → data/cryptoquant-series.json (harvest points).
 * CISS → data/ciss-stress.json. Snapshots (13F, FMP TTM) open desks, never invented daily history.
 */
(function (global) {
  "use strict";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var CQ = null, CISS = null, SYM = null, IND = null, INST = null, PROV = null, IDX_P = null;
  var EXCH = { NASDAQ:1, NYSE:1, AMEX:1, ARCA:1, CBOE:1, TVC:1, BINANCE:1, INDEX:1, FX:1, CRYPTO:1, CME:1, COMEX:1, NYMEX:1, OTC:1, BATS:1, IEX:1, OPRA:1 };
  var SERIES_PROV = { fred:1, nyfed:1, eurostat:1, ecb:1, oecd:1, bis:1, imf:1, boj:1, statcan:1, worldbank:1, ofr:1, "ofr-fsi":1, "ofr-hfm":1, "ofr-bsrm":1, "ofr-site":1, bls:1, census:1, "census-us":1, bea:1, treasury:1, boe:1, eia:1, te:1, "te-mirror":1, "te-feed":1, chicagofed:1, clevelandfed:1, atlantafed:1, cboe:1, cftc:1, dbnomics:1, banxico:1, snb:1, bcb:1, "official-yields":1, tic:1, "kr-ecos":1, "taiwan-moea":1, "peru-copper":1, "cl-datos":1, "hk-data":1, nasa:1, occ:1, dol:1, finra:1, eiopa:1, gleif:1, gdelt:1, "fed-board":1, cryptoquant:1, coinmetrics:1, fmp:1, quiver:1, benzinga:1, "indicator-bus":1, "nyfed-research":1, "sec-edgar":1, "sec-midas":1, "sec-dera":1, "sec-bulk":1 };
  var CHIPS = [
    ["all", "All"],
    ["stocks", "Stocks"],
    ["etfs", "Funds"],
    ["macro", "Macro"],
    ["data", "Datasets"],
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
    data: { dataset: 1 },
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

    H(["ciss", "euro stress", "sovereign stress"], "CISS:ea", "EA CISS composite", "stress", "ECB CISS · harvest history from 2000", "stress"),

    H(["13f", "13-f", "institutional ownership", "smart money 13f"], "DESK:inst", "13F institutional book", "inst", "SEC 13F · quarterly lagged snapshot", "desk"),
    H(["institutional volume", "inst vol", "dark pool", "ats volume", "finra ats"], "DESK:ivol", "Institutional volume (ATS + tape + 13F)", "inst", "FINRA weekly ATS + Polygon week + 13F confirmation — never blended", "desk"),
    H(["etf holdings", "holdings book", "constituents"], "DESK:etf", "ETF holdings + vs-SPX ranks", "flows", "Massive ETF Global + holdings-index", "desk"),
    H(["etf flow", "creations", "redemptions", "inflow", "outflow"], "DESK:flow", "ETF creations / redemptions", "flows", "Massive ETF Global · delayed tape", "desk"),
    H(["valuation", "pe ratio", "ttm ratios"], "DESK:val", "Valuation (FMP EOD/TTM)", "stocks", "FMP Ultimate · TTM snapshot, not a live print", "desk"),
    H(["financials", "income statement", "filings"], "DESK:fin", "Financials (FMP filings)", "stocks", "FMP Ultimate · annual/quarterly filings", "desk"),
    H(["identity", "figi", "openfigi", "cusip"], "DESK:ident", "Identity / OpenFIGI", "stocks", "OpenFIGI symbology master", "desk"),
    H(["on-chain desk", "onchain desk"], "DESK:chain", "On-chain desk", "chain", "CryptoQuant EOD", "desk"),
    H(["pressure", "buying pressure"], "DESK:press", "Buying / selling pressure", "flows", "ETF look-through · inferred", "desk"),
    H(["liquidity pulse", "rrp liquidity"], "FRED:RRPONTSYD", "ON RRP (liquidity pulse)", "stress", "FRED daily · full history", "stress")
  ];

  /* Every harvest series in /data/cryptoquant-series.json. extra is honest:
   * daily EOD from 2025-07; twins (when present) extend to 2010 at coarser spacing. */
  var CQ_META = [
    ["btc_exchange_netflow", "BTC exchange netflow", ["netflow", "exchange netflow", "btc netflow"]],
    ["btc_exchange_inflow", "BTC exchange inflow", ["exchange inflow", "inflow"]],
    ["btc_exchange_outflow", "BTC exchange outflow", ["exchange outflow", "outflow"]],
    ["btc_exchange_reserve", "BTC exchange reserve", ["exchange reserve", "exchange balance"]],
    ["btc_exchange_addr_in", "BTC exchange depositing addresses", ["depositing addresses", "exchange addresses"]],
    ["btc_mpi", "BTC miner position index", ["mpi", "miner position", "miners position"]],
    ["btc_whale_ratio", "BTC whale ratio", ["whale ratio", "whale"]],
    ["btc_fund_flow_ratio", "BTC fund flow ratio", ["fund flow ratio", "fund flow"]],
    ["btc_stablecoins_ratio", "BTC stablecoins ratio", ["stablecoins ratio"]],
    ["btc_exchange_supply_ratio", "BTC exchange supply ratio", ["exchange supply ratio"]],
    ["btc_mvrv", "BTC MVRV", ["mvrv", "btc mvrv"]],
    ["btc_sopr", "BTC SOPR", ["sopr", "btc sopr"]],
    ["btc_sopr_ratio", "BTC SOPR ratio", ["sopr ratio"]],
    ["btc_nupl", "BTC NUPL", ["nupl"]],
    ["btc_realized_price", "BTC realized price", ["realized price"]],
    ["btc_ssr", "BTC SSR", ["ssr", "stablecoin supply ratio"]],
    ["btc_nvt", "BTC NVT", ["nvt"]],
    ["btc_nvt_golden", "BTC NVT golden", ["nvt golden"]],
    ["btc_nvm", "BTC NVM", ["nvm"]],
    ["btc_puell", "BTC Puell multiple", ["puell", "puell multiple"]],
    ["btc_stock_to_flow", "BTC stock-to-flow", ["stock to flow", "s2f", "stock-to-flow"]],
    ["btc_miner_netflow", "BTC miner netflow", ["miner netflow"]],
    ["btc_miner_outflow", "BTC miner outflow", ["miner outflow"]],
    ["btc_miner_reserve", "BTC miner reserve", ["miner reserve"]],
    ["btc_tx_count", "BTC transaction count", ["tx count", "transaction count"]],
    ["btc_addresses_active", "BTC active addresses", ["active addresses", "addresses active"]],
    ["btc_fees_total", "BTC fees total", ["fees total", "btc fees"]],
    ["btc_fees_tx_mean", "BTC mean fee", ["mean fee", "fee per tx"]],
    ["btc_blockreward", "BTC block reward", ["block reward"]],
    ["btc_difficulty", "BTC difficulty", ["difficulty"]],
    ["btc_hashrate", "BTC hashrate", ["hashrate", "hash rate"]],
    ["btc_utxo_count", "BTC UTXO count", ["utxo"]],
    ["btc_velocity", "BTC velocity", ["velocity"]],
    ["btc_tokens_transferred", "BTC tokens transferred", ["tokens transferred"]],
    ["btc_supply_total", "BTC supply", ["btc supply", "circulating supply"]],
    ["btc_open_interest", "BTC open interest", ["open interest", "oi"]],
    ["btc_funding_rates", "BTC funding rates", ["funding", "funding rates"]],
    ["btc_liquidations", "BTC liquidations", ["liquidations"]],
    ["btc_taker_ratio", "BTC taker buy ratio", ["taker ratio", "taker buy"]],
    ["btc_coinbase_premium", "BTC Coinbase premium", ["coinbase premium"]],
    ["eth_exchange_netflow", "ETH exchange netflow", ["eth netflow"]],
    ["eth_exchange_inflow", "ETH exchange inflow", ["eth inflow"]],
    ["eth_exchange_outflow", "ETH exchange outflow", ["eth outflow"]],
    ["eth_exchange_reserve", "ETH exchange reserve", ["eth reserve"]],
    ["eth_addresses_active", "ETH active addresses", ["eth addresses"]],
    ["eth_tx_count", "ETH transaction count", ["eth tx"]],
    ["eth_open_interest", "ETH open interest", ["eth oi", "eth open interest"]],
    ["eth_funding_rates", "ETH funding rates", ["eth funding"]],
    ["eth_mvrv", "ETH MVRV", ["eth mvrv"]],
    ["stablecoin_exchange_reserve", "Stablecoin exchange reserve", ["stablecoin reserve"]],
    ["stablecoin_exchange_netflow", "Stablecoin exchange netflow", ["stablecoin netflow"]],
    ["stablecoin_exchange_inflow", "Stablecoin exchange inflow", ["stablecoin inflow"]],
    ["stablecoin_exchange_outflow", "Stablecoin exchange outflow", ["stablecoin outflow"]],
    ["stablecoin_supply_total", "Stablecoin supply", ["stablecoin supply"]],
    ["usdc_exchange_reserve", "USDC exchange reserve", ["usdc reserve"]]
  ];
  var CQ_COLORS = ["#f0b429", "#2962ff", "#26c6da", "#ab47bc", "#ff6d00", "#089981", "#f23645", "#7e57c2", "#00897b", "#e91e63"];
  CQ_META.forEach(function (row) {
    var q = row[2].concat([row[0], row[0].replace(/_/g, " ")]);
    CURATED.push(H(q, "CQ:" + row[0], row[1], "chain", "CryptoQuant EOD · harvest (not live); twins extend some series to 2010", "onchain"));
  });
  function cqOscSpecs() {
    return CQ_META.map(function (row, i) {
      return {
        id: "cq_" + row[0],
        n: row[1],
        on: 0,
        cat: "On-chain",
        c: CQ_COLORS[i % CQ_COLORS.length],
        k: "cq",
        cq: row[0]
      };
    });
  }

  function norm(q) {
    return String(q || "").toLowerCase().replace(/[^a-z0-9:+.\- ]+/g, " ").replace(/\s+/g, " ").trim();
  }

  function chips() { return CHIPS; }

  function isWarehouse(s) {
    s = String(s || "");
    if (/^(FRED|CQ|CISS|DESK|DATA|NYFED):/i.test(s)) return true;
    var p = s.split(":")[0];
    if (!p || s.indexOf(":") < 0) return false;
    if (EXCH[p.toUpperCase()]) return false;
    return !!SERIES_PROV[p.toLowerCase()];
  }

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
    if (tab === "macro" && (isWarehouse(s) || type === "economy" || c === "macro")) return true;
    if (tab === "data") return type === "dataset" || /^provider:/i.test(s) || c === "dataset";
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
        out.push({ s: a.s, name: a.name, extra: a.extra, type: a.type, cat: a.cat, suggest: true });
      }
    });
    return out;
  }

  function search(q) {
    return suggest(q, 24);
  }

  function keepId(s) {
    s = String(s || "");
    if (isWarehouse(s) || /^(FRED|CQ|CISS|DESK|DATA|NYFED):/i.test(s)) return s;
    if (/^\^/.test(s) || s.indexOf("=") >= 0) return s.toUpperCase();
    if (/^(NASDAQ|NYSE|AMEX|ARCA|CBOE|TVC|BINANCE):/i.test(s)) return s;
    return s;
  }

  function scoreBlob(q, ticker, name, blob) {
    var n = q.toLowerCase();
    var t = String(ticker || "").toLowerCase();
    var nm = String(name || "").toLowerCase();
    var b = String(blob || "").toLowerCase();
    if (t === n) return 100;
    if (t.indexOf(n) === 0) return 92;
    if (nm === n) return 88;
    if (nm.indexOf(n) === 0) return 80;
    if (b.indexOf(n) === 0) return 78;
    if (nm.indexOf(n) >= 0) return 64;
    if (b.indexOf(n) >= 0) return 55;
    return 0;
  }

  function suggest(q, limit) {
    limit = limit || 16;
    var n = norm(q);
    var out = aliasHits(q);
    var seen = {};
    var i, row, sc;
    out.forEach(function (h) { seen[String(h.s).toUpperCase()] = 1; });
    function push(hit, score) {
      var k = String(hit.s).toUpperCase();
      if (!hit.s || seen[k]) return;
      seen[k] = 1;
      hit.suggest = true;
      hit.score = score;
      out.push(hit);
    }
    if (!n) return out.slice(0, limit);
    if (/cq|on.?chain|cryptoquant|mvrv|sopr|nupl|hashrate|puell/i.test(n)) limit = Math.max(limit, 54);
    if (SYM) {
      var i, row, sc;
      for (i = 0; i < SYM.length; i++) {
        row = SYM[i];
        sc = scoreBlob(n, row.s, row.n, row.blob);
        if (n.length === 1 && sc < 90) continue;
        if (sc) push({ s: row.s, name: row.n, extra: row.ids, type: "stock", cat: "stocks" }, sc);
      }
    }
    if (IND && n.length >= 2) {
      for (i = 0; i < IND.length; i++) {
        row = IND[i];
        if (row.s.toLowerCase().indexOf(n) !== 0 && (row.n || "").toLowerCase().indexOf(n) !== 0) continue;
        push({ s: row.chart, name: row.n || row.s, extra: row.extra, type: row.type, cat: row.cat }, 70);
      }
    }
    if (CQ && CQ.series && n.length >= 2) {
      var cqWant = n.replace(/^cq:/, "").replace(/\s+/g, "_");
      Object.keys(CQ.series).forEach(function (id) {
        var blob = id.replace(/_/g, " ");
        if (id.toLowerCase().indexOf(cqWant) < 0 && blob.indexOf(n) < 0) return;
        var meta = CQ_META.filter(function (r) { return r[0] === id; })[0];
        push({ s: "CQ:" + id, name: meta ? meta[1] : id, extra: "CryptoQuant EOD · harvest", type: "onchain", cat: "chain" }, 85);
      });
    }
    if (INST && n.length >= 1) {
      var hits = [];
      for (i = 0; i < INST.length; i++) {
        row = INST[i];
        sc = 0;
        if (row.u === n.toUpperCase()) sc = 100;
        else if (row.u.indexOf(n.toUpperCase()) === 0) sc = 90;
        else if (n.length >= 3 && row.nu.indexOf(n.toUpperCase()) === 0) sc = 70;
        else if (n.length >= 4 && row.nu.indexOf(n.toUpperCase()) >= 0) sc = 50;
        if (!sc) continue;
        sc += (row.pop || 0) * 8;
        hits.push({ sc: sc, row: row });
      }
      hits.sort(function (a, b) { return b.sc - a.sc; });
      for (i = 0; i < hits.length && i < 8; i++) {
        row = hits[i].row;
        push({ s: row.s, name: row.n, extra: (row.ex || row.m || "") + " " + (row.t || "symbol"), type: (row.t || "stock").toLowerCase(), cat: "stocks" }, hits[i].sc);
      }
    }
    if (PROV && n.length >= 2) {
      for (i = 0; i < PROV.length; i++) {
        row = PROV[i];
        if (row.blob.indexOf(n) < 0) continue;
        push({ s: "provider:" + row.slug, name: row.name, extra: (row.n ? Number(row.n).toLocaleString() + " datasets · " : "") + "warehouse provider", type: "dataset", cat: "data" }, row.slug === n ? 95 : 60);
      }
    }
    out.sort(function (a, b) { return (b.score || 0) - (a.score || 0); });
    return out.slice(0, limit);
  }

  function lookupSym(q) {
    if (!SYM || !q) return "";
    var n = String(q).trim().toUpperCase().replace(/[\s-]/g, "");
    var i, r;
    for (i = 0; i < SYM.length; i++) {
      r = SYM[i];
      if (r.s === n) return r.s;
      if (r.cusip && r.cusip.replace(/[\s-]/g, "").toUpperCase() === n) return r.s;
      if (r.isin && r.isin.replace(/[\s-]/g, "").toUpperCase() === n) return r.s;
      if (r.figi && r.figi.replace(/[\s-]/g, "").toUpperCase() === n) return r.s;
    }
    return "";
  }

  function ensureIndex() {
    if (IDX_P) return IDX_P;
    IDX_P = Promise.all([
      loadJson("/data/symbology/master.json").catch(function () { return null; }),
      loadJson("/data/indicator-bus.json").catch(function () { return null; }),
      loadJson("/data/provider-catalog.json").catch(function () { return null; }),
      loadJson(PROXY + "/data/symdir/instruments.json.gz").catch(function () { return loadJson("/data/symdir/instruments.json.gz").catch(function () { return null; }); }),
      loadCQ().catch(function () { return null; })
    ]).then(function (pack) {
      var master = pack[0], bus = pack[1], catalog = pack[2], instr = pack[3];
      if (master && master.by_ticker) {
        SYM = [];
        Object.keys(master.by_ticker).forEach(function (t) {
          var r = master.by_ticker[t] || {};
          var name = r.name || r.figi_name || t;
          var figi = r.figi || "";
          var cusip = r.cusip || "";
          var isin = r.isin || "";
          var ids = [];
          if (figi) ids.push("FIGI " + figi);
          if (cusip) ids.push("CUSIP " + cusip);
          if (isin) ids.push("ISIN " + isin);
          ids.push("OpenFIGI / SEC spine");
          SYM.push({
            s: t,
            n: name,
            figi: figi,
            cusip: cusip,
            isin: isin,
            ids: ids.join(" · "),
            blob: (t + " " + name + " " + figi + " " + cusip + " " + isin).toLowerCase()
          });
        });
      }
      if (bus && bus.indicators) {
        IND = [];
        Object.keys(bus.indicators).forEach(function (k) {
          var row = bus.indicators[k] || {};
          var src = String(row.src || "");
          var chart = k, type = "economy", cat = "macro", extra = "indicator-bus";
          if (/^cryptoquant:/i.test(src) || /mvrv|sopr|nupl|netflow/i.test(k)) {
            chart = "CQ:" + k.replace(/^BTC_?/i, "btc_").toLowerCase();
            type = "onchain"; cat = "chain"; extra = "CryptoQuant EOD";
          } else if (/^yahoo:/i.test(src)) {
            chart = src.split(":").slice(1).join(":");
            type = "index"; cat = "macro"; extra = "Yahoo · " + (row.asof || "");
          } else if (/^[A-Z0-9][A-Z0-9._]{1,15}$/.test(k) && !/!/.test(k)) {
            chart = "FRED:" + k;
            extra = "FRED warehouse · full history when present";
          }
          IND.push({ s: k, n: k, chart: chart, extra: extra, type: type, cat: cat });
        });
      }
      if (catalog && Array.isArray(catalog.providers)) {
        PROV = catalog.providers.map(function (p) {
          return {
            slug: p.slug,
            name: p.name || p.slug,
            n: p.datasets || p.n_keys || 0,
            blob: String(p.slug + " " + (p.name || "")).toLowerCase()
          };
        });
      }
      if (instr && Array.isArray(instr.rows)) {
        INST = instr.rows.map(function (r) {
          return {
            s: r[0],
            n: r[1] || "",
            ex: r[2] || "",
            t: r[3] || "",
            m: r[4] || "",
            pop: r[5] || 0,
            u: String(r[0] || "").toUpperCase(),
            nu: String(r[1] || "").toUpperCase()
          };
        });
      }
      return { n_sym: SYM ? SYM.length : 0, n_ind: IND ? IND.length : 0, n_inst: INST ? INST.length : 0, n_prov: PROV ? PROV.length : 0 };
    });
    return IDX_P;
  }

  function classify(s) {
    s = String(s || "");
    if (/^DESK:/i.test(s)) return "desk";
    if (/^DATA:/i.test(s) || /^provider:/i.test(s)) return "dataset";
    if (/^CQ:/i.test(s)) return "onchain";
    if (/^CISS:/i.test(s)) return "stress";
    if (isWarehouse(s)) return "macro";
    return "";
  }

  function chartId(raw) {
    var s = String(raw || "").trim();
    if (!s) return "";
    var low = s.toLowerCase();
    if (/^desk:/i.test(s)) return "DESK:" + s.split(":")[1];
    if (/^data:/i.test(s)) return "DATA:" + s.split(":")[1];
    if (/^provider:/i.test(s)) return "provider:" + s.split(":")[1];
    if (/^cq:/i.test(s)) return "CQ:" + s.split(":").slice(1).join(":");
    if (/^ciss:/i.test(s)) return "CISS:" + s.split(":").slice(1).join(":");
    if (isWarehouse(s)) return s;
    if (/^indicator-bus:/i.test(s)) {
      var id = s.split(":").slice(1).join(":");
      if (/mvrv|sopr|nupl|netflow|ssr/i.test(id)) return "CQ:" + id.replace(/^BTC_?/i, "btc_").toLowerCase();
      return "fred:" + id.split(":").pop();
    }
    var via = lookupSym(s);
    if (via) return via;
    var hit = CURATED.filter(function (x) { return x.s.toLowerCase() === low || x.q.indexOf(low) >= 0; })[0];
    if (hit && hit.s.indexOf(":") >= 0) return hit.s;
    return "";
  }

  function rowExtra(row) {
    var extra = [];
    if (row.first && row.last) extra.push(String(row.first).slice(0, 10) + " → " + String(row.last).slice(0, 10));
    else if (row.first) extra.push("from " + row.first);
    if (row.n) extra.push(Number(row.n).toLocaleString() + (row.kind === "dataset" ? " series" : " obs"));
    if (row.freq) extra.push(row.freq);
    extra.push(row.provider_name || row.provider || row.kind);
    if (row.chartable === false) extra.push("browse");
    return extra.filter(Boolean).join(" · ");
  }

  function mapRow(row) {
    if (!row) return null;
    var id = row.id || row.symbol || row.ticker || "";
    var kind = String(row.kind || row.type || "");
    var name = row.name || row.title || "";
    var provider = String(row.provider || "");
    if (/13f/i.test(id + " " + name) && kind !== "instrument") {
      return { s: "DESK:inst", name: name || "13F book", extra: "SEC 13F · quarterly lagged", type: "desk" };
    }
    if (kind === "series" || (row.chartable && isWarehouse(id))) {
      return { s: id, name: name || id, extra: rowExtra(row), type: "economy" };
    }
    if (kind === "instrument") {
      return { s: row.symbol || id, name: name, extra: [(row.ex || row.exchange || ""), (row.type || "symbol"), (row.provider_name || provider)].filter(Boolean).join(" · "), type: String(row.type || "stock").toLowerCase() };
    }
    if (kind === "dataset" || kind === "indicator_ref") {
      return { s: id, name: name || id, extra: rowExtra(row), type: "dataset" };
    }
    var viaSym = lookupSym(id) || lookupSym(row.symbol);
    if (viaSym) return { s: viaSym, name: name || viaSym, extra: "OpenFIGI / SEC spine", type: "stock" };
    var mapped = chartId(id);
    if (mapped) return { s: mapped, name: name || mapped, extra: rowExtra(row), type: classify(mapped) || kind || "economy" };
    if (id) return { s: id, name: name || id, extra: rowExtra(row), type: kind || "stock" };
    return null;
  }

  function go(s, dest) {
    s = String(s || "");
    var slug = "";
    if (/^DATA:/i.test(s) || /^provider:/i.test(s)) slug = s.split(":")[1] || "";
    else if (dest === "dataset" && s.indexOf(":") >= 0) slug = /^provider:/i.test(s) ? s.split(":")[1] : s.split(":")[0];
    if (slug && slug !== "search") {
      global.location.href = SERIES_PROV[String(slug).toLowerCase()] ? ("/provider.html?p=" + encodeURIComponent(slug)) : "/data.html";
      return true;
    }
    if (/^DATA:/i.test(s)) {
      global.location.href = "/data.html";
      return true;
    }
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

  function mergeDv(a, b) {
    var m = {}, i, t;
    function put(row, prefer) {
      if (!row || !row.d || !row.v) return;
      for (i = 0; i < row.d.length && i < row.v.length; i++) {
        t = String(row.d[i]).slice(0, 10);
        if (!t) continue;
        if (prefer || !m[t]) m[t] = +row.v[i];
      }
    }
    put(a, false);
    put(b, true);
    var dates = Object.keys(m).sort();
    return { d: dates, v: dates.map(function (k) { return m[k]; }) };
  }
  function cqRow(doc, k) {
    k = String(k || "");
    var ser = (doc.series && (doc.series[k] || doc.series[k.toLowerCase()])) || null;
    var twin = (doc.twins && (doc.twins[k] || doc.twins[k.toLowerCase()])) || null;
    if (!ser && doc.series) {
      var want = k.toLowerCase().replace(/^btc_/, "");
      Object.keys(doc.series).forEach(function (id) {
        if (!ser && id.toLowerCase().indexOf(want) >= 0) ser = doc.series[id];
      });
    }
    if (!twin && doc.twins) {
      var want2 = k.toLowerCase().replace(/^btc_/, "");
      Object.keys(doc.twins).forEach(function (id) {
        if (!twin && id.toLowerCase().indexOf(want2) >= 0) twin = doc.twins[id];
      });
    }
    if (ser && twin) return mergeDv(twin, ser);
    return ser || twin || null;
  }

  async function klines(sym) {
    var s = String(sym || "");
    if (isWarehouse(s) && !/^CQ:|^CISS:|^DESK:|^DATA:/i.test(s)) {
      try {
        var ser = await loadJson(PROXY + "/series?id=" + encodeURIComponent(s));
        var d0 = ptsBars(ser && ser.obs);
        if (d0.length >= 8) {
          var src0 = (ser.provider_name || ser.provider || "warehouse") + " · " + d0.length + " pts " + (ser.first || "") + " → " + (ser.last || "") + " · " + (ser.freq || "series");
          return { d: d0, src: src0 };
        }
      } catch (eWh) {}
    }
    if (/^CQ:/i.test(s)) {
      var doc = await loadCQ();
      var k = cqKey(s);
      var row = cqRow(doc, k);
      if (!row) return null;
      var d = dvBars(row.d, row.v);
      if (d.length < 8) return null;
      var src = "CryptoQuant EOD · " + d.length + " pts " + String(row.d[0]).slice(0, 10) + " → " + String(row.d[row.d.length - 1]).slice(0, 10);
      if (String(row.d[0]).slice(0, 4) < "2025") src += " · twins+harvest";
      else src += " · harvest (not live)";
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
    suggest: suggest,
    ensureIndex: ensureIndex,
    lookupSym: lookupSym,
    keepId: keepId,
    isWarehouse: isWarehouse,
    classify: classify,
    chartId: chartId,
    mapRow: mapRow,
    go: go,
    klines: klines,
    curated: CURATED,
    cqOscSpecs: cqOscSpecs,
    cqMeta: CQ_META
  };
})(typeof window !== "undefined" ? window : globalThis);
