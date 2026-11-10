/* jh-reskin-skip */
/* jh-uni-search — chart.html universal symbol & data search (TradingView-style dialog).
 *
 * NOTE FOR AI MAINTAINERS — how this search is indexed (same index as chart-pro.html, plus chart.html extras)
 * ---------------------------------------------------------------------------------------------------------
 * The browser NEVER scans the 540 GB warehouse. It queries metadata indexes that point at the stored data:
 *   1. /data/symdir/instruments.json.gz   ~56k market instruments, downloaded once, ranked client-side (instant).
 *   2. /symsearch?q=&limit=200[&provider=][&kind=]  justhodl-symdir Lambda. Merges
 *        a) the symbol directory FTS (~1.38M docs: FRED, BLS, BoJ, Census, OFR, Treasury, BoE, NY Fed series,
 *           Eurostat/ECB/OECD/IMF/BIS/StatCan/World Bank datasets, instruments, TradingView symbols), and
 *        b) the provider warehouse index (data/search/provider-shards.json -> sqlite FTS5, every one of the
 *           73 providers on data.html: datasets, stored files, Indicator Bus entities, TradingView Vault).
 *      It returns rows + per-provider facets (counts). Clicking a facet re-queries with &provider=.
 *   3. /browse?ds=<dataset>&q=&limit=&offset=  lists the series inside a dataset (Eurostat/ECB flows,
 *      StatCan cubes, World Bank indicators, BoJ databases...). Eurostat (564M series) and ECB (3.2M) are
 *      hierarchical: you drill provider -> dataset -> series instead of flattening them into the dropdown.
 *   4. /explorer?provider=<slug>&q=&limit=&offset=  pages through every dataset/file of one provider.
 *   5. /tv-search?text=  TradingView universe symbols (exchange-qualified).
 *   6. window.JHChartCatalog.suggest()  chart.html-only catalogs (CryptoQuant full catalog, CISS, indicator
 *      aliases, curated desks). This is the "more data" chart.html has on top of chart-pro.
 * Every chartable row is loaded with window.jhGoSymbol(id, dest) which calls /series?id= (full stored history,
 * e.g. BLS CPI from 1913, StatCan CPI from 1914). Stored raw files open the file itself.
 * Coverage numbers in the footer are read live from provider-shards.json + symdir/manifest.json — never hardcoded.
 */
(function (root) {
  "use strict";
  if (root.JHUniSearch) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var RECENT_KEY = "jh-us-recent-v1";
  var doc = root.document;

  // ------------------------------------------------------------------ utils
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function hl(text, q) {
    text = String(text == null ? "" : text);
    var words = String(q || "").trim().split(/\s+/).filter(function (w) { return w.length > 0; });
    if (!words.length) return esc(text);
    var low = text.toLowerCase(), marks = [];
    words.forEach(function (w) {
      w = w.toLowerCase(); var i = low.indexOf(w);
      if (i >= 0) marks.push([i, i + w.length]);
    });
    if (!marks.length) return esc(text);
    marks.sort(function (a, b) { return a[0] - b[0]; });
    var out = "", at = 0;
    marks.forEach(function (m) {
      if (m[0] < at) return;
      out += esc(text.slice(at, m[0])) + "<mark>" + esc(text.slice(m[0], m[1])) + "</mark>";
      at = m[1];
    });
    return out + esc(text.slice(at));
  }
  function fmtN(n) { return n == null || isNaN(n) ? "" : Number(n).toLocaleString("en-US"); }
  function fmtV(v) {
    if (v == null || isNaN(v)) return "";
    var a = Math.abs(v);
    if (a >= 1e12) return (v / 1e12).toFixed(2) + "T";
    if (a >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return (v / 1e6).toFixed(2) + "M";
    if (a >= 1e4) return Math.round(v).toLocaleString("en-US");
    return (+v.toFixed(a >= 100 ? 2 : 4)).toString();
  }
  function getJSON(url, signal) {
    return root.fetch(url, { signal: signal }).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    });
  }
  function hash(s) { var h = 0; s = String(s); for (var i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0; return Math.abs(h); }
  var LOGO = ["#2962ff", "#089981", "#f23645", "#ff9800", "#9c27b0", "#00bcd4", "#e91e63", "#4caf50", "#795548", "#607d8b"];
  function logo(sym, key) {
    var ch = String(sym || "?").replace(/^[^A-Za-z0-9]+/, "").charAt(0).toUpperCase() || "?";
    return '<i class="us-logo" style="background:' + LOGO[hash(key || sym) % LOGO.length] + '">' + esc(ch) + "</i>";
  }
  function yr(s) { return s ? String(s).slice(0, 4) : ""; }

  // ------------------------------------------------------------------ type taxonomy (TradingView tabs)
  var TABS = [
    ["all", "All"], ["stocks", "Stocks"], ["funds", "Funds"], ["futures", "Futures"], ["forex", "Forex"],
    ["crypto", "Crypto"], ["indices", "Indices"], ["bonds", "Bonds"], ["economy", "Economy"],
    ["datasets", "Datasets"], ["files", "Stored files"]
  ];
  function classify(r) {
    var t = String(r.type || "").toLowerCase(), ac = String(r.asset_class || "").toLowerCase();
    var id = String(r.id || ""), name = String(r.name || "");
    if (r.kind === "file") return "files";
    if (r.kind === "dataset") return "datasets";
    if (r.kind === "series" || r.kind === "catalog") {
      if (/\b(treasury|yield|bond|bill|note|t-bill|gilt|bund|jgb|oat|btp|coupon|maturity)\b/i.test(name)) return "bonds";
      if (r.kind === "catalog" && /chain|onchain|crypto/.test(String(r.cat || "") + t)) return "crypto";
      return "economy";
    }
    if (/crypto|coin|token/.test(t) || ac === "crypto" || /^(X:|COINBASE:|BINANCE:|BITSTAMP:|KRAKEN:|CRYPTOCAP:|BYBIT:|OKX:)/i.test(id)) return "crypto";
    if (/forex|fx|currency/.test(t) || ac === "fx" || /^(C:|FX:|FX_IDC:|OANDA:|FOREXCOM:|FXCM:)/i.test(id)) return "forex";
    if (/futures?/.test(t) || /1!$|2!$/.test(id)) return "futures";
    if (/bond|yield|rate/.test(t)) return "bonds";
    if (/index/.test(t) || ac === "index" || /^(TVC:|INDEX:|CBOE:VIX|\^|SP:|DJ:|NASDAQ:NDX|CRYPTOCAP)/i.test(id)) return "indices";
    if (/etf|fund|etn|trust|mutual/.test(t) || ac === "etf") return "funds";
    if (/econom/.test(t)) return "economy";
    return "stocks";
  }
  var TYPE_LABEL = { stocks: "stock", funds: "fund", futures: "futures", forex: "forex", crypto: "crypto", indices: "index", bonds: "bond", economy: "economic", datasets: "dataset", files: "stored file" };

  // ------------------------------------------------------------------ data sources
  var INSTR = null, instrLoading = null;
  function loadInstruments() {
    if (INSTR) return Promise.resolve(INSTR);
    if (instrLoading) return instrLoading;
    instrLoading = getJSON(PROXY + "/data/symdir/instruments.json.gz").then(function (d) {
      INSTR = ((d && d.rows) || []).map(function (r) {
        return { symbol: r[0], name: r[1] || "", exchange: r[2] || "", type: r[3] || "", market: r[4] || "", pop: r[5] || 0, U: String(r[0] || "").toUpperCase(), N: String(r[1] || "").toUpperCase() };
      });
      return INSTR;
    }).catch(function () { INSTR = []; return INSTR; });
    return instrLoading;
  }
  function localInstruments(q, limit) {
    if (!INSTR) return [];
    var Q = q.toUpperCase().trim(); if (!Q) return [];
    var bare = Q.indexOf(":") >= 0 ? Q.split(":").pop() : Q, out = [];
    for (var i = 0; i < INSTR.length; i++) {
      var r = INSTR[i], sc = 0;
      if (r.U === Q || r.U === bare) sc = 1000;
      else if (r.U.indexOf(bare) === 0) sc = 500 - (r.U.length - bare.length) * 3;
      else if (r.N.indexOf(Q) === 0) sc = 300;
      else if (Q.length >= 3 && r.N.indexOf(Q) >= 0) sc = 150;
      if (!sc) continue;
      sc += r.pop * 100; if (r.market === "stocks") sc += 20; else if (r.market === "otc") sc -= 60;
      out.push({ sc: sc, r: r });
    }
    out.sort(function (a, b) { return b.sc - a.sc; });
    return out.slice(0, limit || 10).map(function (x) {
      var r = x.r;
      return { id: r.symbol, sym: r.symbol, name: r.name, kind: "instrument", type: r.type, src: r.exchange || (r.market === "tv" ? "TradingView" : "Market"), provider: r.market === "tv" ? "tv" : "instrument", exact: x.sc >= 1000 };
    });
  }
  var symCache = new Map();
  function symsearch(q, provider, kind, signal) {
    var key = q.toLowerCase() + "|" + (provider || "") + "|" + (kind || "");
    if (symCache.has(key)) return symCache.get(key);
    var url = PROXY + "/symsearch?q=" + encodeURIComponent(q) + "&limit=200" + (provider ? "&provider=" + encodeURIComponent(provider) : "") + (kind ? "&kind=" + encodeURIComponent(kind) : "");
    var p = getJSON(url, signal).catch(function (e) { symCache.delete(key); if (e && e.name === "AbortError") throw e; return { rows: [], failed: true }; });
    symCache.set(key, p); if (symCache.size > 300) symCache.delete(symCache.keys().next().value);
    return p;
  }
  function tvsearch(q, signal) {
    return getJSON(PROXY + "/tv-search?text=" + encodeURIComponent(q), signal).then(function (d) { return d.symbols || []; }).catch(function () { return []; });
  }
  function normServer(r) {
    var kind = r.kind;
    if (kind === "dataset" && r.raw) kind = "file";
    var out = {
      id: r.id, sym: r.symbol || r.id, name: r.name || "", kind: kind, type: r.type || r.asset_class || "",
      asset_class: r.asset_class, provider: r.provider, src: r.kind === "instrument" ? (r.ex || r.exchange || r.provider_name || "Market") : (r.provider_name || r.provider || ""),
      first: r.first, last: r.last, n: r.n, freq: r.freq, unit: r.unit, pinned: !!r.pinned, browse: !!r.browse,
      browse_provider: r.browse_provider || null, key: r.key || null, lookup_query: r.lookup_query || null,
      chartable: r.chartable !== false, bytes: r.bytes, score: r.score, raw: r
    };
    if (out.unit && typeof out.unit === "object") out.unit = out.unit.name || "";
    return out;
  }
  function normTv(s) {
    var simpleUS = /^[A-Z]{1,5}$/.test(s.symbol || "") && /^(NASDAQ|NYSE|AMEX|ARCA|BATS|CBOE)$/i.test(s.exchange || "");
    return { id: simpleUS ? s.symbol : (s.full || s.symbol), sym: s.symbol, name: s.description || "", kind: "instrument", type: s.type || "", src: s.exchange || "TradingView", provider: "tv", tv: true };
  }
  function catalogHits(q) {
    var C = root.JHChartCatalog;
    if (!C || typeof C.suggest !== "function") return [];
    try {
      return (C.suggest(q, 80) || []).map(function (h) {
        return { id: h.s, sym: String(h.s).replace(/^(CQ|CISS|DESK|DATA):/, ""), name: h.name || h.s, kind: "catalog", type: h.type || "", cat: h.cat || "", src: h.extra || "JustHodl catalog", provider: String(h.s).split(":")[0].toLowerCase() };
      }).filter(function (h) { return h.id; });
    } catch (e) { return []; }
  }


  // ------------------------------------------------------------------ search assist: aliases, ticker variants, watchlist index, typo correction
  // Every data name is reachable under the names people actually type: "cpi" → consumer price index, "fed balance sheet" → WALCL,
  // "btc-usd" / "BTC/USD" / "X:BTCUSD" → BTCUSD, "^VIX" → VIX. Words in a query are expanded independently and the alternates are
  // searched in parallel with the original; results are merged (original first) and de-duplicated by id.
  var ALIAS = {
    "cpi": ["consumer price index"], "consumer price index": ["cpi"], "inflation": ["consumer price index", "cpi"], "hicp": ["harmonised index of consumer prices"],
    "core cpi": ["cpi less food and energy", "CPILFESL"], "pce": ["personal consumption expenditures price index", "PCEPI"], "core pce": ["PCEPILFE"],
    "ppi": ["producer price index"], "gdp": ["gross domestic product"], "gross domestic product": ["gdp"], "gnp": ["gross national product"],
    "unemployment": ["unemployment rate", "UNRATE"], "jobless claims": ["initial claims", "ICSA"], "initial claims": ["ICSA"], "nfp": ["nonfarm payrolls", "PAYEMS"], "payrolls": ["nonfarm payrolls", "PAYEMS"],
    "jolts": ["job openings"], "retail sales": ["RSAFS"], "industrial production": ["INDPRO"], "pmi": ["purchasing managers index"], "ism": ["ism manufacturing"],
    "fed funds": ["federal funds effective rate", "DFF"], "fed funds rate": ["DFF", "EFFR"], "effr": ["effective federal funds rate"], "sofr": ["secured overnight financing rate"],
    "fed balance sheet": ["WALCL", "total assets federal reserve"], "balance sheet": ["total assets"], "walcl": ["fed total assets"], "qe": ["WALCL"], "qt": ["WALCL"],
    "reverse repo": ["RRPONTSYD", "overnight reverse repurchase"], "rrp": ["RRPONTSYD", "reverse repo"], "on rrp": ["RRPONTSYD"], "tga": ["treasury general account", "WTREGEN"],
    "reserves": ["reserve balances", "WRESBAL"], "bank reserves": ["WRESBAL"], "net liquidity": ["WALCL", "WTREGEN", "RRPONTSYD"], "liquidity": ["net liquidity", "global liquidity"],
    "m2": ["money supply m2", "M2SL"], "money supply": ["m2", "M2SL"], "m1": ["M1SL"], "monetary base": ["BOGMBASE"],
    "10y": ["10 year treasury yield", "DGS10", "US10Y"], "10 year": ["DGS10", "US10Y"], "10yr": ["DGS10"], "2y": ["DGS2", "US02Y"], "2 year": ["DGS2"], "30y": ["DGS30", "US30Y"], "3m": ["DGS3MO"],
    "yield curve": ["T10Y2Y", "T10Y3M"], "2s10s": ["T10Y2Y"], "term premium": ["THREEFYTP10", "ACM term premium"], "real yield": ["DFII10"], "tips": ["DFII10", "inflation indexed"],
    "breakeven": ["T10YIE", "breakeven inflation"], "mortgage rate": ["MORTGAGE30US"], "mortgage": ["MORTGAGE30US"],
    "hy spread": ["BAMLH0A0HYM2", "high yield option adjusted spread"], "high yield": ["BAMLH0A0HYM2", "HYG"], "junk": ["high yield", "HYG"], "ig spread": ["BAMLC0A0CM"], "credit spread": ["BAMLH0A0HYM2", "BAMLC0A0CM"],
    "financial conditions": ["NFCI"], "nfci": ["chicago fed national financial conditions"], "stress index": ["STLFSI4", "financial stress"],
    "dxy": ["dollar index", "TVC:DXY"], "dollar index": ["DXY", "DTWEXBGS"], "dollar": ["dollar index", "DXY"], "usd": ["dollar index"],
    "gold": ["XAUUSD", "GLD", "GC1!"], "xau": ["gold"], "silver": ["XAGUSD", "SLV"], "oil": ["crude oil", "USOIL", "CL1!", "WTI"], "crude": ["crude oil", "WTI"], "wti": ["crude oil wti", "DCOILWTICO"], "brent": ["DCOILBRENTEU", "UKOIL"],
    "natgas": ["natural gas", "NG1!"], "nat gas": ["natural gas"], "copper": ["HG1!", "copper"],
    "btc": ["bitcoin", "BTCUSD"], "bitcoin": ["BTCUSD"], "eth": ["ethereum", "ETHUSD"], "ethereum": ["ETHUSD"], "sol": ["solana", "SOLUSD"], "stablecoin": ["stablecoins", "USDT", "USDC"], "crypto": ["bitcoin", "crypto total market cap"],
    "spx": ["S&P 500", "SPY"], "s&p": ["S&P 500"], "s&p 500": ["SPX", "SPY"], "sp500": ["S&P 500", "SPY"], "es": ["ES1!", "S&P 500 futures"], "ndx": ["nasdaq 100", "QQQ"], "nasdaq": ["nasdaq composite", "QQQ"], "nq": ["NQ1!"],
    "dow": ["dow jones industrial average", "DIA"], "djia": ["DIA"], "russell": ["russell 2000", "IWM"], "rut": ["russell 2000", "IWM"], "small caps": ["IWM", "russell 2000"],
    "vix": ["volatility index", "CBOE volatility"], "vol": ["VIX"], "move": ["MOVE index", "treasury volatility"], "skew": ["CBOE SKEW"],
    "cot": ["commitments of traders", "cftc"], "commitments of traders": ["cftc"], "positioning": ["commitments of traders"],
    "housing starts": ["HOUST"], "case shiller": ["CSUSHPINSA", "home price index"], "home prices": ["CSUSHPINSA", "house price index"], "house prices": ["house price index"],
    "consumer sentiment": ["UMCSENT", "michigan sentiment"], "sentiment": ["consumer sentiment"], "ecb": ["european central bank"], "boj": ["bank of japan"], "pboc": ["people's bank of china"], "boe": ["bank of england"],
    "euro": ["EURUSD"], "yen": ["USDJPY"], "pound": ["GBPUSD"], "yuan": ["USDCNY"], "eur usd": ["EURUSD"], "usd jpy": ["USDJPY"],
    "uk": ["united kingdom"], "us": ["united states"], "usa": ["united states"], "eu": ["euro area"], "eurozone": ["euro area"], "ez": ["euro area"], "japan": ["japan"], "china": ["china"]
  };
  var YAHOO = { "^GSPC": "SPX", "^IXIC": "IXIC", "^NDX": "NDX", "^DJI": "DJI", "^RUT": "RUT", "^VIX": "VIX", "^TNX": "US10Y", "DX-Y.NYB": "DXY", "GC=F": "GOLD", "SI=F": "SILVER", "CL=F": "USOIL", "BZ=F": "UKOIL", "NG=F": "NG1!", "HG=F": "HG1!", "ES=F": "ES1!", "NQ=F": "NQ1!" };
  function tickerVariants(q) {
    var Q = String(q || "").trim().toUpperCase(), v = [];
    if (!Q || Q.length > 40) return v;
    function add(x) { x = String(x || "").trim(); if (x && x !== Q && v.indexOf(x) < 0) v.push(x); }
    if (YAHOO[Q]) add(YAHOO[Q]);
    var bare = Q.indexOf(":") > 0 ? Q.split(":").pop() : Q;
    if (bare !== Q) add(bare);
    var m = bare.match(/^([A-Z0-9]{2,6})\s*[-\/ _]\s*([A-Z]{3,4})$/); if (m) add(m[1] + m[2]);           // BTC-USD, BTC/USD, EUR USD
    if (/=X$/.test(bare)) add(bare.replace(/=X$/, ""));                                                 // EURUSD=X
    if (/^\^/.test(bare)) add(bare.slice(1));                                                           // ^VIX
    if (/^[A-Z]{1,5}-[A-Z]$/.test(bare)) add(bare.replace("-", "."));                                    // BRK-B → BRK.B
    if (/^[A-Z]{1,5}\.[A-Z]$/.test(bare)) add(bare.replace(".", "-"));
    if (/^X:/.test(Q)) add(Q.slice(2));
    return v.slice(0, 3);
  }
  function aliasVariants(q) {
    var low = String(q || "").toLowerCase().trim().replace(/\s+/g, " "), out = [];
    if (!low) return out;
    function add(x) { if (x && x.toLowerCase() !== low && out.indexOf(x) < 0) out.push(x); }
    (ALIAS[low] || []).forEach(add);
    // phrase replacement inside longer queries ("euro area cpi" → "euro area consumer price index")
    var keys = Object.keys(ALIAS).filter(function (k) { return k.length >= 2 && low !== k && (" " + low + " ").indexOf(" " + k + " ") >= 0; });
    keys.sort(function (a, b) { return b.length - a.length; });
    keys.slice(0, 2).forEach(function (k) {
      var rep = ALIAS[k].filter(function (x) { return /[a-z]/.test(x) && x.indexOf(" ") > 0 || /^[a-z ]+$/.test(x); })[0];
      if (rep) add((" " + low + " ").replace(" " + k + " ", " " + rep + " ").trim());
    });
    return out.slice(0, 3);
  }
  function expandQuery(q) { return tickerVariants(q).concat(aliasVariants(q)).slice(0, 4); }

  // Watchlist index — every symbol in every list (TradingView imports + lists created here), with its name and the lists it sits in.
  var WLX = null, wlxLoading = null, wlxVer = -1;
  function loadNames() {
    if (root.JH_WL_NAMES) return Promise.resolve(root.JH_WL_NAMES);
    if (loadNames.p) return loadNames.p;
    loadNames.p = new Promise(function (res) {
      var s = doc.createElement("script"); s.src = "/jh-watchlist-names.js?v=20261005-n1"; s.async = true;
      s.onload = function () { res(root.JH_WL_NAMES || {}); }; s.onerror = function () { res({}); };
      doc.head.appendChild(s);
    });
    return loadNames.p;
  }
  function buildWLX() {
    var names = root.JH_WL_NAMES || {}, map = {}, D = null;
    try { D = root.JHTvWatchlist && root.JHTvWatchlist._data ? root.JHTvWatchlist._data() : null; } catch (e) {}
    function put(id, list) {
      if (!id || id.indexOf("###") === 0) return;
      var k = id.toUpperCase(), e = map[k];
      if (!e) {
        var nm = names[id] || names[k] || null, bare = /[()\/*+]/.test(id) ? id.replace(/[A-Z0-9_]+:/gi, "") : (id.indexOf(":") > 0 ? id.split(":").pop() : id);
        e = map[k] = { id: id, U: k, B: bare.toUpperCase(), name: nm ? nm[0] : "", type: nm ? nm[1] : "", lists: [] };
        e.N = e.name.toUpperCase();
      }
      var al = D && D.alias ? D.alias[k] : ""; // the user's preferred name for this symbol (watchlist "Rename")
      if (al) { e.alias = al; e.A = al.toUpperCase(); }
      if (list && e.lists.indexOf(list) < 0) e.lists.push(list);
    }
    if (D && D.lists) Object.keys(D.lists).forEach(function (lid) { var L = D.lists[lid]; (L.items || []).forEach(function (s) { put(s, L.name); }); });
    if (D && D.flags) Object.keys(D.flags).forEach(function (s) { put(s, D.flags[s].charAt(0).toUpperCase() + D.flags[s].slice(1) + " list"); });
    var files = root.JH_FILE_NAMES || {}; // CSV/JSON files the user imported (jh-chart-import.js)
    Object.keys(files).forEach(function (s) { names[s] = files[s]; put(s, "Your files"); });
    Object.keys(names).forEach(function (s) { put(s, null); });
    WLX = Object.keys(map).map(function (k) { return map[k]; });
    return WLX;
  }
  function loadWLX() {
    if (WLX) return Promise.resolve(WLX);
    if (wlxLoading) return wlxLoading;
    wlxLoading = loadNames().then(buildWLX);
    return wlxLoading;
  }
  root.addEventListener && root.addEventListener("jh-tvwl-changed", function () { if (root.JH_WL_NAMES) buildWLX(); });
  function wordsOf(s) { return String(s || "").toUpperCase().split(/[^A-Z0-9&!.]+/).filter(Boolean); }
  function localWatch(q, alts, limit) {
    if (!WLX) return [];
    var qs = [q].concat(alts || []).map(function (x) { return String(x).toUpperCase().trim(); }).filter(Boolean), out = [];
    for (var i = 0; i < WLX.length; i++) {
      var e = WLX[i], best = 0;
      for (var j = 0; j < qs.length; j++) {
        var Q = qs[j], bare = Q.indexOf(":") > 0 ? Q.split(":").pop() : Q, sc = 0, pen = j ? 40 : 0;
        if (e.A && (e.A === Q)) sc = 1200;
        else if (e.A && e.A.indexOf(Q) === 0 && Q.length >= 2) sc = 1050 - (e.A.length - Q.length);
        else if (e.A && Q.length >= 2 && wordsOf(Q).every(function (w) { return e.A.indexOf(w) >= 0; })) sc = 950;
        else if (e.U === Q || e.B === bare) sc = 1000;
        else if (e.B.indexOf(bare) === 0 && bare.length >= 2) sc = 600 - (e.B.length - bare.length) * 4;
        else if (e.U.indexOf(Q) >= 0 && Q.length >= 3) sc = 380;
        else if (e.N) {
          var ws = wordsOf(Q);
          if (ws.length && ws.every(function (w) { return e.N.indexOf(w) >= 0; })) sc = 300 + (e.N.indexOf(ws[0]) === 0 ? 40 : 0) - Math.min(80, e.N.length / 4);
        }
        if (!sc && e.lists.length && Q.length >= 3) { var L = e.lists.join(" | ").toUpperCase(); var w2 = wordsOf(Q); if (w2.every(function (w) { return L.indexOf(w) >= 0; })) sc = 120; }
        sc = sc ? sc - pen : 0; if (sc > best) best = sc;
      }
      if (!best) continue;
      if (e.lists.length) best += 25;
      out.push({ sc: best, e: e });
    }
    out.sort(function (a, b) { return b.sc - a.sc; });
    return out.slice(0, limit || 12).map(function (x) {
      var e = x.e, ex = e.id.indexOf(":") > 0 && !/[()\/*+]/.test(e.id) ? e.id.split(":")[0] : "";
      return { id: e.id, sym: /[()\/*+]/.test(e.id) ? e.id : e.B, name: e.alias ? e.alias + " — " + (e.name || e.id) : e.name || e.id, alias: e.alias || "", pinned: !!(e.A && x.sc >= 940), kind: "instrument", type: e.type, src: ex || (e.type === "spread" ? "Spread" : "Watchlist"), provider: "watchlist", wl: e.lists.slice(0, 4), wlN: e.lists.length, exact: x.sc >= 1000 };
    });
  }
  // Typo correction ("Did you mean"): bounded Damerau-Levenshtein over watchlist/instrument tickers and name words.
  var VOCAB = null;
  function vocab() {
    if (VOCAB) return VOCAB;
    var v = {};
    (WLX || []).forEach(function (e) { v[e.B] = (v[e.B] || 0) + 3; wordsOf(e.name).forEach(function (w) { if (w.length >= 4) v[w] = (v[w] || 0) + 1; }); });
    (INSTR || []).forEach(function (r) { if (r.pop > 0.2) v[r.U] = (v[r.U] || 0) + 2; });
    Object.keys(ALIAS).forEach(function (k) { k.toUpperCase().split(" ").forEach(function (w) { if (w.length >= 3) v[w] = (v[w] || 0) + 4; }); });
    ["CONSUMER", "PRICE", "INDEX", "EMPLOYMENT", "UNEMPLOYMENT", "TREASURY", "YIELD", "INFLATION", "LIQUIDITY", "BALANCE", "SHEET", "RESERVES", "MONEY", "SUPPLY", "PRODUCTION", "MANUFACTURING", "HOUSING", "MORTGAGE", "SPREAD", "VOLATILITY", "BITCOIN", "ETHEREUM", "DOLLAR", "EXCHANGE", "INTEREST", "FEDERAL", "EUROSTAT", "STATCAN", "WORLDBANK"].forEach(function (w) { v[w] = (v[w] || 0) + 5; });
    VOCAB = Object.keys(v).map(function (w) { return [w, v[w]]; });
    return VOCAB;
  }
  function dist(a, b, max) {
    if (Math.abs(a.length - b.length) > max) return max + 1;
    var p = [], c = [], pp = [], i, j;
    for (j = 0; j <= b.length; j++) p[j] = j;
    for (i = 1; i <= a.length; i++) {
      c = [i]; var lo = i;
      for (j = 1; j <= b.length; j++) {
        var cost = a[i - 1] === b[j - 1] ? 0 : 1;
        c[j] = Math.min(p[j] + 1, c[j - 1] + 1, p[j - 1] + cost);
        if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) c[j] = Math.min(c[j], pp[j - 2] + 1);
        if (c[j] < lo) lo = c[j];
      }
      if (lo > max) return max + 1;
      pp = p; p = c;
    }
    return p[b.length];
  }
  function correct(q) {
    var words = String(q || "").toUpperCase().trim().split(/\s+/).filter(Boolean); if (!words.length) return null;
    var V = vocab(), changed = false;
    var fixed = words.map(function (w) {
      if (w.length < 3 || /\d/.test(w) && w.length < 5) return w;
      if (V.some(function (x) { return x[0] === w; })) return w;
      var max = w.length >= 7 ? 2 : 1, best = null, bd = 9, bw = -1;
      for (var i = 0; i < V.length; i++) {
        var t = V[i][0]; if (Math.abs(t.length - w.length) > max || t[0] !== w[0] && max < 2) continue;
        var d = dist(w, t, max); if (d <= max && (d < bd || d === bd && V[i][1] > bw)) { bd = d; best = t; bw = V[i][1]; }
      }
      if (best) { changed = true; return best; }
      return w;
    });
    return changed ? fixed.join(" ").toLowerCase() : null;
  }
  // Query completions shown under the input while typing (alias phrases, watchlist names, tickers).
  function completions(q) {
    var low = String(q || "").toLowerCase().trim(); if (low.length < 2) return [];
    var out = [], seen = {};
    function add(s) { var k = s.toLowerCase(); if (seen[k] || k === low) return; seen[k] = 1; out.push(s); }
    Object.keys(ALIAS).forEach(function (k) { if (k.indexOf(low) === 0) add(k); });
    expandQuery(q).forEach(add);
    if (WLX) {
      var U = low.toUpperCase();
      for (var a = 0; a < WLX.length && out.length < 8; a++) { if (WLX[a].A && WLX[a].A.indexOf(U) === 0) add(WLX[a].alias); }
      for (var i = 0; i < WLX.length && out.length < 14; i++) { var e = WLX[i]; if (e.lists.length && e.N && e.N.indexOf(U) === 0) add(e.name.length > 60 ? e.name.slice(0, 60) : e.name); }
    }
    return out.slice(0, 8);
  }

  // ------------------------------------------------------------------ recents
  function recents() { try { var a = JSON.parse(root.localStorage.getItem(RECENT_KEY) || "[]"); return Array.isArray(a) ? a : []; } catch (e) { return []; } }
  function remember(r) {
    if (!r || !r.id) return;
    var a = recents().filter(function (x) { return x.id !== r.id; });
    a.unshift({ id: r.id, sym: r.sym, name: r.name, kind: r.kind === "file" || r.kind === "dataset" ? "instrument" : r.kind, type: r.type, src: r.src, provider: r.provider, first: r.first, last: r.last, freq: r.freq });
    try { root.localStorage.setItem(RECENT_KEY, JSON.stringify(a.slice(0, 30))); } catch (e) {}
  }

  // ------------------------------------------------------------------ coverage (live, never hardcoded)
  var COVER = null;
  function loadCoverage() {
    if (COVER) return Promise.resolve(COVER);
    return Promise.all([
      getJSON(PROXY + "/data/search/provider-shards.json").catch(function () { return null; }),
      getJSON(PROXY + "/data/symdir/manifest.json").catch(function () { return null; })
    ]).then(function (a) {
      var s = a[0] || {}, m = a[1] || {}, c = s.coverage || {};
      COVER = {
        providers: s.providers, documents: s.documents, datasets: c.catalog_datasets, objects: c.storage_objects,
        bytes: c.storage_bytes, symdocs: m.docs, instruments: m.instruments, generated: s.generated_at, built: m.built_at,
        hier: c.hierarchical_series || {}
      };
      return COVER;
    });
  }

  // ------------------------------------------------------------------ state
  var S = {
    open: false, dest: "chart", q: "", tab: "all", prov: "", view: "search", rows: [], sel: 0, seq: 0,
    facets: [], total: null, more: false, loading: false, ctl: null,
    ds: null, dsName: "", dsOffset: 0, dsTotal: null, dsChip: "", dsFacets: null, dsHint: "",
    provSlug: "", provName: "", provOffset: 0, provTotal: null, added: {}
  };
  var box, inp, list, chipsEl, facEl, crumbEl, footEl, statusEl, debT = 0;

  function css() {
    if (doc.getElementById("jhus-css")) return;
    var st = doc.createElement("style"); st.id = "jhus-css";
    st.textContent = [
      "#symsearch{display:none!important}",
      "#jhus{position:fixed;inset:0;z-index:10050;display:none;align-items:flex-start;justify-content:center;background:rgba(0,0,0,.45);font:13px/1.4 -apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,Ubuntu,sans-serif}",
      "#jhus.on{display:flex}",
      "#jhus .us-dlg{--bg:#1e222d;--bg2:#2a2e39;--bd:#363a45;--fg:#d1d4dc;--mut:#787b86;--blue:#2962ff;--hov:#2a2e39;--sel:#142e61;margin-top:7vh;width:min(820px,calc(100vw - 16px));height:min(78vh,760px);background:var(--bg);color:var(--fg);border:1px solid var(--bd);border-radius:6px;box-shadow:0 12px 48px rgba(0,0,0,.55);display:flex;flex-direction:column;overflow:hidden}",
      "html[data-theme=light] #jhus .us-dlg{--bg:#fff;--bg2:#f0f3fa;--bd:#e0e3eb;--fg:#131722;--mut:#6a6d78;--hov:#f0f3fa;--sel:#e3effd}",
      "#jhus .us-hd{display:flex;align-items:center;padding:14px 20px 8px;gap:12px}",
      "#jhus .us-hd h2{margin:0;font-size:20px;font-weight:600;flex:1}",
      "#jhus .us-x{background:none;border:0;color:var(--mut);font-size:22px;cursor:pointer;line-height:1;padding:4px 6px;border-radius:4px}",
      "#jhus .us-x:hover{background:var(--hov);color:var(--fg)}",
      "#jhus .us-in{display:flex;align-items:center;gap:10px;margin:0 20px;border-bottom:1px solid var(--bd);padding:6px 0 10px}",
      "#jhus .us-in svg{flex:0 0 18px;color:var(--mut)}",
      "#jhus .us-in input{flex:1;min-width:0;background:transparent;border:0;outline:0;color:var(--fg);font-size:17px;text-transform:none;padding:4px 0}",
      "#jhus .us-clear{background:none;border:0;color:var(--mut);cursor:pointer;font-size:16px}",
      "#jhus .us-chips{display:flex;gap:6px;padding:10px 20px 6px;overflow-x:auto;scrollbar-width:none;flex:0 0 auto}",
      "#jhus .us-chips::-webkit-scrollbar{display:none}",
      "#jhus .us-sugg{display:flex;gap:6px;align-items:center;padding:8px 20px 0;overflow-x:auto;scrollbar-width:none;flex:0 0 auto;font-size:12px;color:#787b86;white-space:nowrap}",
      "#jhus .us-sugg::-webkit-scrollbar{display:none}",
      "#jhus .us-sl{flex:0 0 auto}#jhus .us-sl b{color:#d1d4dc}",
      "#jhus .us-sg{flex:0 0 auto;background:none;border:1px solid #434651;color:#b2b5be;border-radius:14px;padding:3px 10px;cursor:pointer;font:inherit}",
      "#jhus .us-sg:hover{border-color:#2962ff;color:#fff}#jhus .us-sg.dym{border-color:#2962ff;color:#90bfff}#jhus .us-sg mark{background:none;color:#2962ff;font-weight:600}",
      "html[data-theme=light] #jhus .us-sg{color:#131722;border-color:#d1d4dc}html[data-theme=light] #jhus .us-sl b{color:#131722}",
      "#jhus .us-chip{flex:0 0 auto;border:1px solid var(--bd);background:transparent;color:var(--fg);border-radius:18px;padding:5px 12px;cursor:pointer;font-size:13px;white-space:nowrap}",
      "#jhus .us-chip:hover{background:var(--hov)}",
      "#jhus .us-chip.on{background:var(--fg);color:var(--bg);border-color:var(--fg)}",
      "#jhus .us-fac{display:flex;gap:6px;padding:2px 20px 8px;overflow-x:auto;scrollbar-width:thin;flex:0 0 auto}",
      "#jhus .us-fac:empty{display:none}",
      "#jhus .us-f{flex:0 0 auto;border:0;background:var(--bg2);color:var(--fg);border-radius:4px;padding:3px 8px;cursor:pointer;font-size:12px;white-space:nowrap}",
      "#jhus .us-f b{color:var(--mut);font-weight:400;margin-left:4px}",
      "#jhus .us-f.on{background:var(--blue);color:#fff}#jhus .us-f.on b{color:#dbe4ff}",
      "#jhus .us-crumb{display:flex;align-items:center;gap:8px;padding:4px 20px 8px;font-size:13px;color:var(--mut);flex-wrap:wrap}",
      "#jhus .us-crumb:empty{display:none}",
      "#jhus .us-crumb button{background:var(--bg2);border:0;color:var(--fg);border-radius:4px;padding:3px 9px;cursor:pointer}",
      "#jhus .us-crumb b{color:var(--fg);font-weight:600}",
      "#jhus .us-status{padding:0 20px 6px;color:var(--mut);font-size:12px;min-height:16px}",
      "#jhus .us-list{flex:1;overflow-y:auto;overscroll-behavior:contain;border-top:1px solid var(--bd)}",
      "#jhus .us-grp{padding:10px 20px 4px;color:var(--mut);font-size:11px;letter-spacing:.06em;text-transform:uppercase}",
      "#jhus .us-grp{display:flex;align-items:center;gap:8px}#jhus .us-ga{margin-left:auto;background:none;border:1px solid var(--bd,#363a45);border-radius:12px;color:var(--fg,#d1d4dc);font:600 11px/20px inherit;padding:0 10px;cursor:pointer;letter-spacing:0;text-transform:none}",
      "#jhus .us-ga:hover{border-color:#2962ff;color:#fff;background:#2962ff}#jhus .us-ga.ok{border-color:#089981;color:#22ab94;background:none}",
      "#jhus .us-row{display:grid;grid-template-columns:28px minmax(120px,190px) 1fr auto auto;align-items:center;gap:12px;padding:7px 20px;cursor:pointer;border-bottom:1px solid transparent;min-height:40px}",
      "#jhus .us-row:hover{background:var(--hov)}",
      "#jhus .us-row.sel{background:var(--sel)}",
      "#jhus .us-logo{width:26px;height:26px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;color:#fff;font-style:normal;font-weight:700;font-size:12px}",
      "#jhus .us-sym{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhus .us-desc{min-width:0;overflow:hidden}",
      "#jhus .us-desc .n{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhus .us-desc small{display:block;color:var(--mut);font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhus mark{background:transparent;color:var(--blue);font-weight:700}",
      "#jhus .us-meta{text-align:right;color:var(--mut);font-size:12px;white-space:nowrap}",
      "#jhus .us-meta .t{display:block;color:var(--fg)}",
      "#jhus .us-act{display:flex;gap:4px;opacity:.0}",
      "#jhus .us-row:hover .us-act,#jhus .us-row.sel .us-act{opacity:1}",
      "#jhus .us-act:has([data-act=addds]){opacity:1}#jhus [data-act=addds]{border-color:#2962ff!important;color:#5b8cff!important}",
      "#jhus .us-act button{width:26px;height:26px;border-radius:4px;border:1px solid var(--bd);background:var(--bg);color:var(--fg);cursor:pointer;font-size:14px;line-height:1}",
      "#jhus .us-act button:hover{background:var(--blue);color:#fff;border-color:var(--blue)}",
      "#jhus .us-act button.ok{background:#089981;color:#fff;border-color:#089981}",
      "#jhus .us-more{display:block;margin:10px auto 16px;background:var(--bg2);border:0;color:var(--fg);border-radius:4px;padding:7px 16px;cursor:pointer}",
      "#jhus .us-empty{padding:40px 20px;text-align:center;color:var(--mut)}",
      "#jhus .us-foot{border-top:1px solid var(--bd);padding:8px 20px;color:var(--mut);font-size:11px;display:flex;gap:14px;flex-wrap:wrap;align-items:center}",
      "#jhus .us-foot b{color:var(--fg);font-weight:600}",
      "#jhus .us-foot .ok{color:#089981}",
      "#jhus .us-foot a{color:var(--blue);cursor:pointer;text-decoration:none}",
      "#jhus .us-dims{padding:0 20px 8px;display:flex;flex-wrap:wrap;gap:6px}",
      "#jhus .us-dims:empty{display:none}",
      "#jhus .us-dims .d{font-size:11px;color:var(--mut);margin-right:2px}",
      "@media(max-width:640px){#jhus .us-dlg{margin-top:0;height:100dvh;width:100vw;border-radius:0}#jhus .us-row{grid-template-columns:26px minmax(80px,110px) 1fr auto;padding:7px 12px}#jhus .us-meta{display:none}#jhus .us-act{opacity:1}#jhus .us-hd,#jhus .us-chips,#jhus .us-fac,#jhus .us-status,#jhus .us-crumb{padding-left:12px;padding-right:12px}#jhus .us-in{margin:0 12px}}"
    ].join("\n");
    doc.head.appendChild(st);
  }

  function build() {
    if (box) return;
    css();
    box = doc.createElement("div"); box.id = "jhus"; box.setAttribute("role", "dialog"); box.setAttribute("aria-modal", "true"); box.setAttribute("aria-label", "Symbol search");
    box.innerHTML =
      '<div class="us-dlg">' +
      '<div class="us-hd"><h2 id="jhus-title">Symbol Search</h2><button class="us-x" type="button" aria-label="Close" data-act="close">×</button></div>' +
      '<div class="us-in"><svg viewBox="0 0 18 18" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.6"><circle cx="7.5" cy="7.5" r="5.5"/><path d="M11.5 11.5L16 16"/></svg>' +
      '<input id="jhus-in" type="text" autocomplete="off" spellcheck="false" placeholder="Symbol, ISIN, CUSIP, series name, dataset, provider…" aria-label="Search symbols and data">' +
      '<button class="us-clear" type="button" data-act="clear" aria-label="Clear">✕</button></div>' +
      '<div class="us-sugg" aria-label="Suggestions"></div>' +
      '<div class="us-chips" role="tablist"></div>' +
      '<div class="us-fac"></div>' +
      '<div class="us-crumb"></div>' +
      '<div class="us-dims"></div>' +
      '<div class="us-status" role="status" aria-live="polite"></div>' +
      '<div class="us-list" role="listbox"></div>' +
      '<div class="us-foot"></div>' +
      "</div>";
    doc.body.appendChild(box);
    inp = box.querySelector("#jhus-in"); list = box.querySelector(".us-list"); chipsEl = box.querySelector(".us-chips");
    facEl = box.querySelector(".us-fac"); crumbEl = box.querySelector(".us-crumb"); footEl = box.querySelector(".us-foot"); statusEl = box.querySelector(".us-status");
    box.addEventListener("mousedown", function (e) { if (e.target === box) close(); });
    box.addEventListener("click", onClick);
    box.addEventListener("keydown", onKey, true);
    box.addEventListener("keyup", function (e) { e.stopPropagation(); }, true);
    box.addEventListener("keypress", function (e) { e.stopPropagation(); }, true);
    inp.addEventListener("input", function () {
      S.q = inp.value; S.sel = 0;
      clearTimeout(debT);
      debT = setTimeout(function () { if (S.view === "browse") loadBrowse(true); else if (S.view === "provider") loadProvider(true); else search(); }, S.view === "search" ? 140 : 260);
      if (S.view === "search") renderLocalFirst();
    });
    list.addEventListener("scroll", function () {
      if (list.scrollTop + list.clientHeight > list.scrollHeight - 120) {
        if (S.view === "browse" && S.more && !S.loading) loadBrowse(false);
        if (S.view === "provider" && S.more && !S.loading) loadProvider(false);
      }
    });
    renderChips();
  }

  function renderChips() {
    chipsEl.innerHTML = TABS.map(function (t) {
      return '<button type="button" role="tab" class="us-chip' + (S.tab === t[0] ? " on" : "") + '" data-tab="' + t[0] + '" aria-selected="' + (S.tab === t[0]) + '">' + t[1] + "</button>";
    }).join("");
    chipsEl.style.display = S.view === "search" ? "" : "none";
  }

  // ------------------------------------------------------------------ search pipeline
  var G = null; // groups for current query
  function renderLocalFirst() {
    var q = S.q.trim();
    if (!q) { G = null; renderHome(); return; }
    if (!INSTR) loadInstruments().then(function () { if (S.q.trim() === q && S.view === "search") renderLocalFirst(); });
    if (!WLX) loadWLX().then(function () { if (S.q.trim() === q && S.view === "search") renderLocalFirst(); });
    var loc = localInstruments(q, 10);
    var cat = catalogHits(q);
    var wl = localWatch(q, expandQuery(q), 12);
    if (G && G.q === q && G.fixed) wl = wl.concat(localWatch(G.fixed, expandQuery(G.fixed), 12));
    if (!G || G.q !== q) G = { q: q, local: loc, server: [], alt: [], tv: [], catalog: cat, wl: wl, done: false };
    else { G.local = loc; G.catalog = cat; G.wl = wl; }
    render();
  }
  function search() {
    var q = S.q.trim();
    if (!q) { renderHome(); return; }
    var seq = ++S.seq;
    if (S.ctl) try { S.ctl.abort(); } catch (e) {}
    var ctl = root.AbortController ? new AbortController() : null; S.ctl = ctl;
    var sig = ctl ? ctl.signal : undefined;
    S.loading = true; status("Searching every ticker, series, dataset and stored file…");
    if (!G || G.q !== q) G = { q: q, local: localInstruments(q, 10), server: [], alt: [], tv: [], catalog: catalogHits(q), wl: localWatch(q, expandQuery(q), 12), done: false };
    var alts = expandQuery(q); G.alts = alts;
    var kind = S.tab === "economy" || S.tab === "bonds" ? "series" : (S.tab === "datasets" || S.tab === "files") ? "dataset" : "";
    var a = symsearch(q, S.prov, kind, sig).then(function (d) {
      if (seq !== S.seq) return;
      G.server = (d.rows || []).map(normServer);
      if (d.series_hits && d.series_hits.rows) G.server = d.series_hits.rows.map(normServer).concat(G.server);
      G.suggest = d.suggest || [];
      if (!S.prov) { S.facets = d.facets || []; S.total = d.total; }
      else { S.provTotalQ = d.total; }
      G.failed = !!d.failed; G.ambiguous = !!d.ambiguous;
      render();
    });
    var b = (S.prov || kind) ? Promise.resolve() : tvsearch(q, sig).then(function (arr) {
      if (seq !== S.seq) return;
      G.tv = arr.map(normTv); render();
    });
    // alternate phrasings / ticker spellings searched in parallel (aliases, BTC-USD → BTCUSD, ^VIX → VIX …)
    var c = Promise.all(alts.map(function (alt) {
      return symsearch(alt, S.prov, kind, sig).then(function (d) {
        if (seq !== S.seq) return;
        var rows = (d.rows || []).map(normServer); if (d.series_hits && d.series_hits.rows) rows = d.series_hits.rows.map(normServer).concat(rows);
        var tick = tickerVariants(q).indexOf(alt) >= 0;
        rows.forEach(function (r) { r.via = alt; r.viaTicker = tick; }); G.alt = G.alt.concat(rows.slice(0, 60)); render();
      }).catch(function () {});
    }));
    function cap(p, ms) { return Promise.race([p, new Promise(function (r) { setTimeout(r, ms); })]); }
    Promise.all([cap(a, 9000), cap(b, 6000), cap(c, 7000)]).then(function () {
      if (seq !== S.seq) return;
      S.loading = false; G.done = true;
      // Nothing useful? Try the closest spelling ("Did you mean"), TradingView-style "Showing results for …".
      var n = merged().length;
      if (n < 3 && !S.prov && S.noFix !== q) {
        var fix = correct(q);
        if (fix && fix !== q.toLowerCase()) {
          G.dym = fix;
          if (n === 0) {
            G.fixed = fix; G.wl = (G.wl || []).concat(localWatch(fix, expandQuery(fix), 12)); G.local = (G.local || []).concat(localInstruments(fix, 10));
            return symsearch(fix, "", kind, sig).then(function (d) { if (seq !== S.seq) return; G.alt = G.alt.concat((d.rows || []).map(normServer)); render(); }).catch(function () { render(); });
          }
        }
      }
      render();
    }).catch(function () { if (seq === S.seq) { S.loading = false; render(); } });
  }

  function merged() {
    if (!G) return [];
    var seen = {}, out = [];
    function key(r) { return String(r.id).toUpperCase(); }
    function push(r, grp) { var k = key(r); if (seen[k]) return; seen[k] = 1; r.grp = grp; r.cls = classify(r); out.push(r); }
    var Q = G.q.toUpperCase();
    var server = G.server || [];
    // 1. exact matches anywhere (symbol or id equals query) — "Best match"
    var exact = [];
    var alt = G.alt || [];
    // a provider named in the query ("statcan cpi") also scopes the alternate phrasings
    var pw = String(G.q).toLowerCase().match(/\b(statcan|eurostat|fred|ecb|bls|bea|boj|boe|bis|imf|oecd|worldbank|census|treasury|ofr|cftc|nyfed|snb|rba|boc|ons|insee|destatis|istat|ine|abs|stats?nz|cryptoquant|glassnode|coingecko|defillama)\b/);
    if (pw) alt = alt.filter(function (r) { return String(r.provider || "").toLowerCase().indexOf(pw[1].replace(/s$/, "")) >= 0 || String(r.id).toLowerCase().indexOf(pw[1] + ":") === 0; });
    // when the literal query already has plenty, alternates go below it instead of interleaving
    var inline = server.length < 8 || G.fixed;
    if (inline) server = server.concat(alt);
    var tickAlt = (G.alt || []).filter(function (r) { return r.viaTicker; });
    server.concat(G.local || [], G.wl || [], inline ? [] : tickAlt).forEach(function (r) {
      var s = String(r.sym || "").toUpperCase(), id = key(r), V = r.viaTicker ? String(r.via).toUpperCase() : Q;
      if (r.pinned || s === Q || id === Q || id.split(":").pop() === Q || r.viaTicker && (s === V || id === V || id.split(":").pop() === V)) exact.push(r);
    });
    exact.sort(function (a, b) { return (b.pinned ? 2 : 0) + (b.kind === "instrument" ? 1 : 0) - ((a.pinned ? 2 : 0) + (a.kind === "instrument" ? 1 : 0)); });
    exact.slice(0, 6).forEach(function (r) { push(r, "Best match"); });
    // 1b. everything already in the user's watchlists (TradingView imports + lists made here)
    (G.wl || []).forEach(function (r) { push(r, "In your watchlists"); });
    // 2. symbols (server order is authoritative; local fills gaps)
    server.filter(function (r) { return r.kind === "instrument"; }).forEach(function (r) { push(r, "Symbols"); });
    (G.local || []).forEach(function (r) { push(r, "Symbols"); });
    // 3. data series across all providers
    server.filter(function (r) { return r.kind === "series"; }).forEach(function (r) { push(r, "Data series"); });
    // 4. chart.html catalogs (CryptoQuant, CISS, indicators, desks)
    (G.catalog || []).forEach(function (r) { push(r, "JustHodl catalogs"); });
    // 5. datasets (drill down into series)
    server.filter(function (r) { return r.kind === "dataset"; }).forEach(function (r) { push(r, "Datasets"); });
    // 6. TradingView universe
    (G.tv || []).forEach(function (r) { push(r, "TradingView universe"); });
    // 7. stored files
    server.filter(function (r) { return r.kind === "file"; }).forEach(function (r) { push(r, "Stored files"); });
    if (!inline) alt.forEach(function (r) { push(r, "Related: " + (r.via || "other names")); });
    if (S.tab !== "all") out = out.filter(function (r) { return r.cls === S.tab; });
    return out;
  }

  function rowHtml(r, i) {
    var cls = r.cls || classify(r), t = TYPE_LABEL[cls] || r.kind;
    var sub = [];
    if (r.kind === "series" || r.kind === "dataset") {
      if (r.first || r.last) sub.push(yr(r.first) + "→" + yr(r.last) + (r.n ? " · " + fmtN(r.n) + " obs" : ""));
      if (r.freq) sub.push(String(r.freq));
      if (r.unit) sub.push(String(r.unit).slice(0, 28));
    }
    if (r.kind === "file") { sub.push(r.key || ""); if (r.bytes) sub.push(fmtV(r.bytes) + "B"); }
    if (r.id !== r.sym && r.kind !== "file") sub.push(r.id);
    if (r.wl && r.wl.length) sub.push("in " + r.wl.join(", ") + (r.wlN > r.wl.length ? " +" + (r.wlN - r.wl.length) + " more" : ""));
    if (r.via) sub.push("matched “" + r.via + "”");
    var action = r.kind === "dataset" ? (r.browse_provider ? "provider ›" : "series ›") : r.kind === "file" ? "open file ↗" : t;
    var acts = "";
    if (r.kind !== "dataset" && r.kind !== "file") {
      acts = '<span class="us-act">' +
        '<button type="button" data-act="compare" title="Compare / overlay on chart" aria-label="Compare ' + esc(r.sym) + '">⇄</button>' +
        '<button type="button" data-act="add" class="' + (S.added[r.id] ? "ok" : "") + '" title="Add to watchlist" aria-label="Add ' + esc(r.sym) + ' to watchlist">' + (S.added[r.id] ? "✓" : "+") + "</button></span>";
    } else if (r.kind === "dataset" && r.browse) {
      acts = '<span class="us-act"><button type="button" data-act="addds" title="Add every series in this dataset' + (r.n ? " (" + fmtN(r.n) + ")" : "") + ' to your watchlist" aria-label="Add every series in ' + esc(r.name || r.sym) + '">+</button></span>';
    } else acts = '<span class="us-act"></span>';
    return '<div class="us-row' + (i === S.sel ? " sel" : "") + '" role="option" data-i="' + i + '" aria-selected="' + (i === S.sel) + '" title="' + esc(r.id) + '">' +
      logo(r.sym, r.provider || r.src) +
      '<span class="us-sym">' + hl(r.sym, S.q) + "</span>" +
      '<span class="us-desc"><span class="n">' + hl(r.name, S.q) + "</span>" + (sub.length ? "<small>" + esc(sub.join(" · ")) + "</small>" : "") + "</span>" +
      '<span class="us-meta"><span class="t">' + esc(action) + "</span>" + esc(r.src || "") + "</span>" + acts + "</div>";
  }

  var ROWS = [];
  function render() {
    if (!S.open || S.view !== "search") return;
    var q = S.q.trim();
    if (!q) { renderHome(); return; }
    ROWS = merged();
    if (S.sel >= ROWS.length) S.sel = Math.max(0, ROWS.length - 1);
    // facets (provider chips)
    if (S.facets && S.facets.length) {
      var all = S.facets.reduce(function (a, f) { return a + (f.n || 0); }, 0);
      facEl.innerHTML = '<button type="button" class="us-f' + (S.prov ? "" : " on") + '" data-prov="">All sources<b>' + fmtN(S.total || all) + "</b></button>" +
        S.facets.map(function (f) { return '<button type="button" class="us-f' + (S.prov === f.provider ? " on" : "") + '" data-prov="' + esc(f.provider) + '">' + esc(f.provider_name || f.provider) + "<b>" + fmtN(f.n) + "</b></button>"; }).join("");
    } else facEl.innerHTML = "";
    crumbEl.innerHTML = ""; box.querySelector(".us-dims").innerHTML = "";
    renderSugg(q);
    var html = "", grp = "";
    ROWS.forEach(function (r, i) {
      if (r.grp !== grp && S.tab === "all") {
        grp = r.grp;
        var nAdd = ROWS.filter(function (x) { return x.grp === grp && addable(x); }).length;
        html += '<div class="us-grp">' + esc(grp) + (nAdd > 1 ? '<button type="button" class="us-ga" data-act="addgrp" data-grp="' + esc(grp) + '" title="Add all ' + nAdd + " " + esc(grp.toLowerCase()) + ' results to your watchlist">+ Add all ' + nAdd + "</button>" : "") + "</div>";
      }
      html += rowHtml(r, i);
    });
    if (!ROWS.length) {
      if (S.loading || !G || !G.done) html = '<div class="us-empty">Searching every ticker, series, dataset and stored file…</div>';
      else {
        var sug = (G.suggest || []).slice(0, 8).map(function (t) { return '<button type="button" class="us-f" data-sug="' + esc(t) + '">' + esc(t) + "</button>"; }).join(" ");
        html = '<div class="us-empty">No symbols match your criteria' + (S.tab !== "all" ? " in " + esc(TABS.filter(function (t) { return t[0] === S.tab; })[0][1]) + " — try All" : "") + (sug ? "<br><br>Did you mean: " + sug : "") + "</div>";
      }
    }
    list.innerHTML = html;
    var tot = S.prov ? S.provTotalQ : S.total;
    status(G && G.failed ? "Directory unreachable — showing instruments and TradingView matches only." :
      (tot != null ? fmtN(tot) + " indexed matches" + (S.prov ? " in " + provLabel(S.prov) : " across " + (S.facets.length || "all") + " sources") + " · showing " + ROWS.length + (tot > 200 ? " best — refine words, pick a source or a tab to go deeper" : "") : (S.loading ? "Searching…" : "")) + (S.loading ? " · searching…" : ""));
    scrollSel();
  }
  function renderSugg(q) {
    var el = box.querySelector(".us-sugg"); if (!el) return;
    var c = completions(q), h = "";
    if (G && G.fixed) h += '<span class="us-sl">Showing results for <b>' + esc(G.fixed) + '</b> · <button type="button" class="us-sg" data-sug="' + esc(q) + '" data-exact="1">search “' + esc(q) + '” only</button></span>';
    else if (G && G.dym) h += '<span class="us-sl">Did you mean</span><button type="button" class="us-sg dym" data-sug="' + esc(G.dym) + '">' + esc(G.dym) + "</button>";
    if (c.length) h += '<span class="us-sl">' + (h ? "Also" : "Suggestions") + "</span>" + c.map(function (t) { return '<button type="button" class="us-sg" data-sug="' + esc(t) + '">' + hl(t, q) + "</button>"; }).join("");
    el.innerHTML = h; el.style.display = h ? "" : "none";
  }
  function provLabel(p) { var f = (S.facets || []).filter(function (x) { return x.provider === p; })[0]; return f ? (f.provider_name || p) : p; }

  function renderHome() {
    var sgEl = box && box.querySelector(".us-sugg"); if (sgEl) { sgEl.innerHTML = ""; sgEl.style.display = "none"; }
    ROWS = recents().map(function (r) { r.grp = "Recent"; r.cls = classify(r); return r; });
    facEl.innerHTML = ""; crumbEl.innerHTML = ""; box.querySelector(".us-dims").innerHTML = "";
    var html = '<div class="us-grp">Browse the whole warehouse</div>' +
      '<div class="us-row" data-act="providers" style="grid-template-columns:28px 1fr auto"><i class="us-logo" style="background:#2962ff">▦</i><span class="us-desc"><span class="n"><b>All data providers</b> — every dataset on the Data page</span><small id="jhus-cov-mini">Loading coverage…</small></span><span class="us-meta"><span class="t">browse ›</span></span></div>';
    if (ROWS.length) {
      html += '<div class="us-grp">Recent</div>';
      ROWS.forEach(function (r, i) { html += rowHtml(r, i); });
    }
    list.innerHTML = html;
    status(S.dest === "add" ? "Type to add symbols or data series to your watchlist." : S.dest === "compare" ? "Type to add a symbol or series to the chart for comparison." : "Start typing — stocks, ETFs, crypto, FX, futures, every macro series, dataset and stored file.");
    loadCoverage().then(function (c) {
      var el = doc.getElementById("jhus-cov-mini");
      if (el && c) el.textContent = fmtN(c.providers) + " providers · " + fmtN(c.datasets) + " datasets · " + fmtN(c.documents) + " warehouse records · " + fmtN(c.symdocs) + " directory series & symbols · " + (c.bytes ? (c.bytes / 1e9).toFixed(1) + " GB" : "");
    });
  }

  // ------------------------------------------------------------------ provider + dataset browsing
  var PROVIDERS = null;
  function openProviders() {
    S.view = "providers"; S.sel = 0; inp.value = ""; S.q = ""; renderChips();
    facEl.innerHTML = ""; box.querySelector(".us-dims").innerHTML = "";
    crumbEl.innerHTML = '<button type="button" data-act="back">‹ Back</button><b>All providers</b>';
    inp.placeholder = "Filter providers…";
    list.innerHTML = '<div class="us-empty">Loading provider catalogue…</div>';
    (PROVIDERS ? Promise.resolve(PROVIDERS) : getJSON(PROXY + "/explorer").then(function (d) { PROVIDERS = d.providers || []; return PROVIDERS; })).then(renderProviders).catch(function (e) { list.innerHTML = '<div class="us-empty">Provider catalogue unavailable: ' + esc(e.message) + "</div>"; });
  }
  function renderProviders() {
    if (S.view !== "providers") return;
    var f = S.q.trim().toLowerCase();
    ROWS = (PROVIDERS || []).filter(function (p) { return !f || (p.name + " " + p.slug).toLowerCase().indexOf(f) >= 0; }).map(function (p) {
      var dir = p.in_directory || {}, n = (dir.series || 0);
      return { id: "provider:" + p.slug, sym: p.slug, name: p.name, kind: "provider", slug: p.slug, src: (p.mb ? (p.mb / 1000).toFixed(p.mb > 10000 ? 0 : 2) + " GB" : ""), datasets: p.datasets, series: p.series_count || n, hier: p.hierarchical_series };
    });
    status(ROWS.length + " providers · select one to page through all of its datasets and series");
    list.innerHTML = ROWS.map(function (r, i) {
      return '<div class="us-row' + (i === S.sel ? " sel" : "") + '" data-i="' + i + '">' + logo(r.sym, r.sym) + '<span class="us-sym">' + hl(r.sym, S.q) + '</span><span class="us-desc"><span class="n">' + hl(r.name, S.q) + "</span><small>" +
        esc(fmtN(r.datasets) + " datasets" + (r.series ? " · " + fmtN(r.series) + " series" : "")) + '</small></span><span class="us-meta"><span class="t">browse ›</span>' + esc(r.src) + '</span><span class="us-act"></span></div>';
    }).join("") || '<div class="us-empty">No provider matches.</div>';
    scrollSel();
  }
  function openProvider(slug, name) {
    S.view = "provider"; S.provSlug = slug; S.provName = name || slug; S.provOffset = 0; S.sel = 0; S.rowsP = [];
    inp.value = ""; S.q = ""; inp.placeholder = "Search inside " + (name || slug) + "…"; renderChips(); facEl.innerHTML = "";
    loadProvider(true);
  }
  function loadProvider(reset) {
    if (reset) { S.provOffset = 0; S.rowsP = []; S.sel = 0; }
    var seq = ++S.seq; S.loading = true;
    crumbEl.innerHTML = '<button type="button" data-act="back">‹ Back</button><span>Providers ›</span> <b>' + esc(S.provName) + "</b>";
    if (reset) list.innerHTML = '<div class="us-empty">Loading datasets…</div>';
    var url = PROXY + "/explorer?provider=" + encodeURIComponent(S.provSlug) + "&q=" + encodeURIComponent(S.q.trim()) + "&limit=200&offset=" + S.provOffset;
    getJSON(url).then(function (d) {
      if (seq !== S.seq || S.view !== "provider") return;
      var rows = (d.rows || []).filter(function (r) { return r.id !== "provider:" + S.provSlug; }).map(normServer);
      S.rowsP = S.rowsP.concat(rows); S.provOffset += (d.rows || []).length; S.provTotal = d.total;
      S.more = (d.rows || []).length >= 200 && (d.total == null || S.provOffset < d.total);
      S.loading = false; renderProviderRows();
    }).catch(function (e) { if (seq === S.seq) { S.loading = false; list.innerHTML = '<div class="us-empty">Could not list provider: ' + esc(e.message) + "</div>"; } });
  }
  function renderProviderRows() {
    ROWS = S.rowsP.map(function (r) { r.cls = classify(r); return r; });
    status((S.provTotal != null ? fmtN(S.provTotal) + " entries" : ROWS.length + " entries") + " in " + S.provName + " · showing " + ROWS.length + (S.more ? " · scroll for more" : ""));
    list.innerHTML = ROWS.map(rowHtml).join("") + (S.more ? '<button type="button" class="us-more" data-act="moreP">Load more</button>' : "") || '<div class="us-empty">Nothing indexed under this filter.</div>';
    scrollSel();
  }
  function openDataset(r) {
    S.view = "browse"; S.ds = r.id; S.dsName = r.name || r.id; S.dsProv = r.src || r.provider; S.dsOffset = 0; S.dsChip = ""; S.sel = 0; S.rowsB = [];
    S.backTo = S.prevView || "search";
    inp.value = ""; S.q = ""; inp.placeholder = "Filter series in this dataset — words or codes (e.g. DE, monthly, USA)…"; renderChips(); facEl.innerHTML = "";
    loadBrowse(true);
  }
  function loadBrowse(reset) {
    if (reset) { S.dsOffset = 0; S.rowsB = []; S.sel = 0; }
    var seq = ++S.seq; S.loading = true;
    crumbEl.innerHTML = '<button type="button" data-act="back">‹ Back</button><span>' + esc(S.dsProv || "") + " ›</span> <b>" + esc(S.dsName) + "</b> <span>" + esc(S.ds) + "</span>";
    if (reset) list.innerHTML = '<div class="us-empty">Loading series…</div>';
    var q = [S.q.trim(), S.dsChip].filter(Boolean).join(" ");
    var url = PROXY + "/browse?ds=" + encodeURIComponent(S.ds) + "&q=" + encodeURIComponent(q) + "&limit=200&offset=" + S.dsOffset;
    getJSON(url).then(function (d) {
      if (seq !== S.seq || S.view !== "browse") return;
      var rows = (d.rows || []).map(function (x) { var n = normServer(x); n.last_value = x.last_value; n.geo = x.geo; return n; });
      S.rowsB = S.rowsB.concat(rows); S.dsOffset += (d.rows || []).length; S.dsTotal = d.total; S.dsMatched = d.matched;
      S.more = (d.rows || []).length >= 200 && (d.matched == null || S.dsOffset < d.matched);
      S.dsHint = d.error ? "⚠ " + d.error : (d.hint || ""); S.dsTrunc = !!d.truncated; S.dsScanned = d.scanned || d.scanned_rows;
      if (reset) renderDims(d.facets || {});
      S.loading = false; renderBrowseRows();
    }).catch(function (e) { if (seq === S.seq) { S.loading = false; list.innerHTML = '<div class="us-empty">Could not list dataset: ' + esc(e.message) + "</div>"; } });
  }
  function renderDims(fac) {
    var el = box.querySelector(".us-dims");
    var parts = Object.keys(fac).filter(function (k) { return Array.isArray(fac[k]) && fac[k].length > 1 && fac[k].length < 400; }).slice(0, 8).map(function (k) {
      return '<span class="d">' + esc(k) + "</span>" + fac[k].slice(0, 10).map(function (v) {
        var val = v[0], lab = v[2] || v[0];
        return '<button type="button" class="us-f' + (S.dsChip === val ? " on" : "") + '" data-dim="' + esc(val) + '" title="' + esc(lab) + " · " + esc(v[1]) + ' series">' + esc(String(lab).length > 26 ? String(lab).slice(0, 25) + "…" : lab) + "</button>";
      }).join("");
    });
    el.innerHTML = parts.join(" ");
  }
  function renderBrowseRows() {
    ROWS = S.rowsB.map(function (r) { r.cls = classify(r); if (r.last_value != null) r.src = (r.src || "") + " · last " + fmtV(r.last_value); return r; });
    var m = S.dsMatched != null ? S.dsMatched : ROWS.length;
    status(fmtN(m) + " matching series" + (S.dsTotal != null ? " of " + fmtN(S.dsTotal) + " in dataset" : "") + (S.dsTrunc ? " (large dataset: type codes or words to narrow)" : "") + " · showing " + ROWS.length + (S.dsHint ? " · " + S.dsHint : ""));
    list.innerHTML = (ROWS.length ? '<div class="us-grp">Series in ' + esc(S.dsName || S.ds) + '<button type="button" class="us-ga" data-act="addbrowse" title="Add every matching series in this dataset to your watchlist">+ Add all ' + fmtN(m) + "</button></div>" : "") + ROWS.map(rowHtml).join("") + (S.more ? '<button type="button" class="us-more" data-act="moreB">Load more</button>' : "") || '<div class="us-empty">No series match — try a country/frequency code or clear the filter.</div>';
    scrollSel();
  }
  function back() {
    S.seq++; box.querySelector(".us-dims").innerHTML = "";
    inp.placeholder = "Symbol, ISIN, CUSIP, series name, dataset, provider…";
    if (S.view === "browse" && S.backTo === "provider") { S.view = "provider"; inp.value = ""; S.q = ""; renderChips(); renderProviderRows(); crumbEl.innerHTML = '<button type="button" data-act="back">‹ Back</button><span>Providers ›</span> <b>' + esc(S.provName) + "</b>"; return; }
    if (S.view === "provider") { S.view = "providers"; renderChips(); openProviders(); return; }
    S.view = "search"; inp.value = S.lastQ || ""; S.q = inp.value; renderChips(); if (S.q.trim()) { renderLocalFirst(); search(); } else renderHome();
  }

  // ------------------------------------------------------------------ actions
  function pick(r, how) {
    if (!r) return;
    if (r.kind === "provider") { openProvider(r.slug, r.name); return; }
    if (r.kind === "dataset") {
      if (r.browse_provider && !r.browse) { openProvider(r.browse_provider, r.src); return; }
      if (r.browse) { S.prevView = S.view; if (S.view === "search") S.lastQ = S.q; openDataset(r); S.backTo = S.prevView; return; }
      // reference rows (Indicator Bus, TradingView Vault): search for the chartable instrument behind them
      var term = String(r.id).split(":").slice(1).join(" ").replace(/_/g, " ");
      S.view = "search"; inp.value = term; S.q = term; renderChips(); renderLocalFirst(); search(); return;
    }
    if (r.kind === "file") { root.open(PROXY + "/" + String(r.key || "").replace(/^\/+/, ""), "_blank", "noopener"); return; }
    var dest = how || S.dest;
    if (dest === "add") { addToWatch(r); return; }
    remember(r);
    if (dest === "compare") {
      if (typeof root.jhAddCompare === "function") root.jhAddCompare(r.id);
      else if (typeof root.jhGoSymbol === "function") root.jhGoSymbol(r.id, "compare");
      toast("Added " + r.sym + " to the chart");
      if (S.dest === "compare") return; // keep dialog open like TradingView's compare dialog
      close(); return;
    }
    close();
    // TradingView-style ids (EXCH:SYMBOL, spreads like AMEX:XLY/AMEX:XLP) use the watchlist's reviewed route
    var tvId = r.provider === "watchlist" || /^[A-Z][A-Z0-9_]*:/.test(String(r.id)) && !/^(X|C|I|CQ|CISS|DESK|DATA):/.test(String(r.id)) || (root.JHChartExpr && root.JHChartExpr.isExpr(r.id));
    if (tvId && root.JHTvWatchlist && typeof root.JHTvWatchlist.route === "function") root.JHTvWatchlist.route(r.id);
    else if (typeof root.jhGoSymbol === "function") root.jhGoSymbol(r.id, "chart");
    else root.location.hash = "s=" + encodeURIComponent(r.id);
  }
  function addToWatch(r) {
    var ok = false;
    if (root.JHTvWatchlist && typeof root.JHTvWatchlist.add === "function") ok = root.JHTvWatchlist.add(r.id, { name: r.name, src: r.src });
    else if (typeof root.jhGoSymbol === "function") { root.jhGoSymbol(r.id, "add"); ok = true; }
    S.added[r.id] = true; remember(r);
    var btn = list.querySelector('.us-row[data-i="' + ROWS.indexOf(r) + '"] [data-act="add"]');
    if (btn) { btn.textContent = "✓"; btn.classList.add("ok"); }
    toast((ok === "dup" ? r.sym + " is already in " : "Added " + r.sym + " to ") + (root.JHTvWatchlist ? root.JHTvWatchlist.activeName() : "watchlist"));
  }
  // ------------------------------------------------------------------ bulk add: a whole result group or every series of a dataset
  function addable(r) { return r && r.id && r.kind !== "dataset" && r.kind !== "file" && r.kind !== "provider" && r.chartable !== false; }
  function addAll(ids, what, btn) {
    ids = ids.filter(Boolean); if (!ids.length) return;
    if (ids.length > 300 && !root.confirm("Add " + ids.length + " symbols to " + (root.JHTvWatchlist ? root.JHTvWatchlist.activeName() : "the watchlist") + "?")) return;
    var res;
    if (root.JHTvWatchlist && typeof root.JHTvWatchlist.addMany === "function") res = root.JHTvWatchlist.addMany(ids);
    else { ids.forEach(function (id) { if (root.JHTvWatchlist) root.JHTvWatchlist.add(id); }); res = { added: ids.length, dup: 0, list: root.JHTvWatchlist ? root.JHTvWatchlist.activeName() : "watchlist" }; }
    ids.forEach(function (id) { S.added[id] = true; });
    list.querySelectorAll('[data-act="add"]').forEach(function (b) { var rw = b.closest(".us-row"), r = rw && ROWS[+rw.getAttribute("data-i")]; if (r && S.added[r.id]) { b.textContent = "✓"; b.classList.add("ok"); } });
    if (btn) { btn.textContent = "✓ Added " + res.added; btn.classList.add("ok"); }
    toast("Added " + res.added + " from " + what + " to " + res.list + (res.dup ? " · " + res.dup + " already there" : ""));
  }
  function addDataset(ds, name, q, btn) {
    if (btn) { btn.disabled = true; btn.textContent = "…"; }
    var ids = [], off = 0, CAP = 5000;
    (function page() {
      getJSON(PROXY + "/browse?ds=" + encodeURIComponent(ds) + "&q=" + encodeURIComponent(q || "") + "&limit=200&offset=" + off).then(function (d) {
        var rows = d && d.rows || [];
        rows.forEach(function (x) { if (x && x.id && x.chartable !== false) ids.push(x.id); });
        off += rows.length;
        var tot = d && (d.matched != null ? d.matched : d.total);
        if (rows.length >= 200 && off < CAP && (tot == null || off < tot)) { if (btn) btn.textContent = off + "…"; page(); return; }
        if (btn) btn.disabled = false;
        if (!ids.length) { if (btn) btn.textContent = "+"; toast("No chartable series found in " + name); return; }
        addAll(ids, name, btn);
        if (btn && btn.textContent.indexOf("Added") < 0) btn.textContent = "✓";
      }).catch(function (e) { if (btn) { btn.disabled = false; btn.textContent = "+"; } toast("Could not list " + name + ": " + (e && e.message || "error")); });
    })();
  }
  function toast(m) {
    var t = doc.getElementById("jhus-toast");
    if (!t) { t = doc.createElement("div"); t.id = "jhus-toast"; t.setAttribute("role", "status"); t.style.cssText = "position:fixed;left:50%;bottom:28px;transform:translateX(-50%);background:#2a2e39;color:#fff;padding:8px 14px;border-radius:4px;font:13px sans-serif;z-index:10060;box-shadow:0 4px 18px rgba(0,0,0,.4);display:none"; doc.body.appendChild(t); }
    t.textContent = m; t.style.display = "block"; clearTimeout(t._t); t._t = setTimeout(function () { t.style.display = "none"; }, 2000);
  }
  function onClick(e) {
    var a = e.target.closest("[data-act],[data-tab],[data-prov],[data-sug],[data-dim],.us-row");
    if (!a || !box.contains(a)) return;
    if (a.hasAttribute("data-tab")) { S.tab = a.getAttribute("data-tab"); S.sel = 0; renderChips(); if (S.tab === "economy" || S.tab === "bonds" || S.tab === "datasets" || S.tab === "files") search(); else render(); inp.focus(); return; }
    if (a.hasAttribute("data-prov")) { S.prov = a.getAttribute("data-prov"); S.sel = 0; search(); render(); inp.focus(); return; }
    if (a.hasAttribute("data-sug")) { S.noFix = a.hasAttribute("data-exact") ? a.getAttribute("data-sug").trim() : ""; inp.value = a.getAttribute("data-sug"); S.q = inp.value; renderLocalFirst(); search(); return; }
    if (a.hasAttribute("data-dim")) { var v = a.getAttribute("data-dim"); S.dsChip = S.dsChip === v ? "" : v; box.querySelectorAll(".us-dims .us-f").forEach(function (b) { b.classList.toggle("on", b.getAttribute("data-dim") === S.dsChip); }); loadBrowse(true); return; }
    var act = a.getAttribute("data-act");
    if (act === "close") { close(); return; }
    if (act === "clear") { inp.value = ""; S.q = ""; if (S.view === "search") renderHome(); else if (S.view === "browse") loadBrowse(true); else if (S.view === "provider") loadProvider(true); else renderProviders(); inp.focus(); return; }
    if (act === "back") { back(); inp.focus(); return; }
    if (act === "providers") { S.lastQ = S.q; openProviders(); inp.focus(); return; }
    if (act === "moreB") { loadBrowse(false); return; }
    if (act === "moreP") { loadProvider(false); return; }
    if (act === "cover") { renderCoverage(); return; }
    if (act === "addgrp") { e.stopPropagation(); var gname = a.getAttribute("data-grp"); addAll(ROWS.filter(function (x) { return x.grp === gname && addable(x); }).map(function (x) { return x.id; }), gname, a); return; }
    if (act === "addbrowse") { e.stopPropagation(); addDataset(S.ds, S.dsName || S.ds, [S.q.trim(), S.dsChip].filter(Boolean).join(" "), a); return; }
    var row = a.classList.contains("us-row") ? a : a.closest(".us-row");
    if (!row) return;
    if (act === "addds") { e.stopPropagation(); var rr = ROWS[+row.getAttribute("data-i")]; if (rr) addDataset(rr.id, rr.name || rr.sym, "", a); return; }
    var r = ROWS[+row.getAttribute("data-i")];
    if (act === "compare") { e.stopPropagation(); pick(r, "compare"); return; }
    if (act === "add") { e.stopPropagation(); addToWatch(r); return; }
    pick(r);
  }
  function onKey(e) {
    e.stopPropagation();
    var k = e.key;
    if (k === "Escape") { e.preventDefault(); if (S.view !== "search") back(); else close(); return; }
    if (k === "ArrowDown" || k === "ArrowUp") {
      e.preventDefault(); if (!ROWS.length) return;
      S.sel = Math.max(0, Math.min(ROWS.length - 1, S.sel + (k === "ArrowDown" ? 1 : -1)));
      list.querySelectorAll(".us-row.sel").forEach(function (x) { x.classList.remove("sel"); x.setAttribute("aria-selected", "false"); });
      var el = list.querySelector('.us-row[data-i="' + S.sel + '"]'); if (el) { el.classList.add("sel"); el.setAttribute("aria-selected", "true"); }
      scrollSel(); return;
    }
    if (k === "Enter") {
      e.preventDefault();
      if (S.view === "search" && S.q.trim() && (!G || !G.done) && !ROWS.length) {
        var q0 = S.q; var wait = setInterval(function () { if (S.q !== q0) { clearInterval(wait); return; } if (G && G.done) { clearInterval(wait); if (ROWS.length) pick(ROWS[0]); } }, 120);
        setTimeout(function () { clearInterval(wait); }, 15000);
        return;
      }
      if (ROWS[S.sel]) pick(ROWS[S.sel], e.shiftKey ? "compare" : (e.altKey ? "add" : null));
      return;
    }
    if (k === "Backspace" && !inp.value && S.view !== "search") { e.preventDefault(); back(); return; }
    if (k === "Tab" && S.view === "search" && doc.activeElement === inp) {
      e.preventDefault();
      var i = TABS.map(function (t) { return t[0]; }).indexOf(S.tab);
      S.tab = TABS[(i + (e.shiftKey ? TABS.length - 1 : 1)) % TABS.length][0]; S.sel = 0; renderChips();
      if (S.tab === "economy" || S.tab === "bonds" || S.tab === "datasets" || S.tab === "files") search(); else render();
    }
  }
  function scrollSel() { var el = list.querySelector(".us-row.sel"); if (el && el.scrollIntoView) el.scrollIntoView({ block: "nearest" }); }
  function status(m) { statusEl.textContent = m || ""; }

  function renderFoot() {
    footEl.innerHTML = "<span>↑↓ navigate</span><span>↵ chart</span><span>⇧↵ compare</span><span>⌥↵ add to watchlist</span><span>Tab switch type</span><span>Esc close</span><span style=\"margin-left:auto\" id=\"jhus-cov\">…</span>";
    loadCoverage().then(function (c) {
      var el = doc.getElementById("jhus-cov"); if (!el || !c) return;
      var age = c.generated ? Math.round((Date.now() - Date.parse(c.generated)) / 3600000) : null;
      el.innerHTML = '<span class="ok">●</span> Index ready · <b>' + fmtN(c.providers) + "</b> providers · <b>" + fmtN(c.documents) + "</b> warehouse records · <b>" + fmtN(c.symdocs) + "</b> directory docs" + (age != null ? " · refreshed " + (age < 1 ? "<1" : age) + "h ago" : "") + ' · <a data-act="cover">details</a>';
    });
  }
  function renderCoverage() {
    loadCoverage().then(function (c) {
      S.view = "providers"; renderChips(); facEl.innerHTML = ""; box.querySelector(".us-dims").innerHTML = "";
      crumbEl.innerHTML = '<button type="button" data-act="back">‹ Back</button><b>Search coverage</b>';
      var h = c.hier || {};
      list.innerHTML = '<div style="padding:16px 20px;line-height:1.7">' +
        "<div><b>" + fmtN(c.providers) + "</b> providers (the full Data page) · <b>" + fmtN(c.datasets) + "</b> catalogued datasets · <b>" + fmtN(c.objects) + "</b> stored objects · <b>" + (c.bytes ? (c.bytes / 1e9).toFixed(1) : "?") + " GB</b></div>" +
        "<div><b>" + fmtN(c.documents) + "</b> warehouse search records (provider-shards FTS5, built " + esc(c.generated || "?") + ")</div>" +
        "<div><b>" + fmtN(c.symdocs) + "</b> symbol-directory documents incl. <b>" + fmtN(c.instruments) + "</b> instruments (built " + esc(c.built || "?") + ")</div>" +
        "<div>Hierarchical (drill provider → dataset → series): Eurostat <b>" + fmtN(h.eurostat) + "</b> series · ECB <b>" + fmtN(h.ecb) + "</b> series</div>" +
        '<div style="margin-top:10px;color:var(--mut)">Every chartable result loads its full stored history through /series. Stored raw files open directly. Counts are read live from data/search/provider-shards.json and data/symdir/manifest.json.</div>' +
        '<div style="margin-top:10px"><button type="button" class="us-chip" data-act="providers">Browse all providers ›</button></div></div>';
      status("");
    });
  }

  // ------------------------------------------------------------------ open / close
  function open(opts) {
    opts = opts || {};
    build();
    S.open = true; S.dest = opts.dest || "chart"; S.view = "search"; S.prov = ""; S.sel = 0; S.added = {}; S.facets = []; S.total = null; G = null;
    if (opts.tab) S.tab = opts.tab; else if (!S.keepTab) S.tab = "all";
    box.querySelector("#jhus-title").textContent = S.dest === "compare" ? "Compare symbol" : S.dest === "add" ? "Add symbol" : "Symbol Search";
    inp.placeholder = "Symbol, ISIN, CUSIP, series name, dataset, provider…";
    inp.value = opts.q || ""; S.q = inp.value;
    box.classList.add("on");
    renderChips(); renderFoot();
    loadInstruments(); loadWLX().then(function () { VOCAB = null; });
    if (S.q.trim()) { renderLocalFirst(); search(); } else renderHome();
    setTimeout(function () { inp.focus(); try { inp.setSelectionRange(inp.value.length, inp.value.length); } catch (e) {} }, 0);
  }
  function close() {
    if (!box) return;
    S.open = false; S.seq++; if (S.ctl) try { S.ctl.abort(); } catch (e) {}
    box.classList.remove("on");
    var t = doc.getElementById("symin"); if (t) t.value = "";
    var w = doc.getElementById("tv-symwrap"); if (w) w.classList.remove("searching");
  }

  // ------------------------------------------------------------------ take over the legacy #symsearch entry points
  function hijack() {
    var legacy = doc.getElementById("symsearch");
    if (!legacy || legacy.__jhus) return !!legacy;
    legacy.__jhus = 1;
    var mo = new MutationObserver(function () {
      if (!/\bon\b/.test(legacy.className)) return;
      var dest = legacy.dataset.dest || "chart", ss = doc.getElementById("ssin"), q = ss ? ss.value : "";
      legacy.className = "";
      if (ss) { ss.value = ""; ss.blur(); }
      open({ q: q, dest: dest });
    });
    mo.observe(legacy, { attributes: true, attributeFilter: ["class"] });
    return true;
  }
  function boot() {
    css();
    if (!hijack()) { var n = 0, t = setInterval(function () { if (hijack() || ++n > 80) clearInterval(t); }, 250); }
    // Prime the instant directory while the user looks at the chart.
    setTimeout(loadInstruments, 1500);
    setTimeout(loadCoverage, 2500);
  }
  root.JHUniSearch = { open: open, close: close, search: function (q) { open({ q: q }); }, classify: classify, _state: S, loadCoverage: loadCoverage,
    expandQuery: expandQuery, correct: correct, completions: completions, localWatch: localWatch, loadWLX: loadWLX, _G: function () { return G; }, merged: function () { return merged(); } };
  root.jhOpenSearch = function (q, dest) { open({ q: q || "", dest: dest || "chart" }); };
  if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", boot); else boot();
})(window);
