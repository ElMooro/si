/* Khalid sniper. A name is a sniper only when every required box passes.
   Missing data is an open box, never a pass. Bonuses never create a pass. */
(function (root) {
  "use strict";
  var NDX = "ADBE,AMD,ABNB,ALNY,GOOGL,GOOG,AMZN,AEP,AMGN,ADI,AAPL,AMAT,APP,ARM,ASML,ALAB,ADSK,ADP,AXON,BKR,BKNG,AVGO,CDNS,CTAS,CSCO,CCEP,CMCSA,CEG,CPRT,CRWV,COST,CRWD,CSX,DDOG,DXCM,FANG,DASH,EXC,FAST,FER,FTNT,GEHC,GILD,HONA,HON,IDXX,INTC,INTU,ISRG,KDP,KLAC,LRCX,LIN,LITE,MAR,MRVL,MELI,META,MCHP,MU,MSFT,MSTR,MDLZ,MPWR,MNST,NBIS,NFLX,NVDA,NXPI,ORLY,ODFL,PCAR,PLTR,PANW,PAYX,PYPL,PDD,PEP,QCOM,REGN,RKLB,ROP,ROST,SNDK,STX,SHOP,SPCX,SBUX,SNPS,TMUS,TTWO,TER,TSLA,TXN,TRI,VRTX,WMT,WBD,WDC,WDAY,XEL";
  var SECTOR_ETF = {
    "Technology": "XLK", "Healthcare": "XLV", "Health Care": "XLV",
    "Financial Services": "XLF", "Financials": "XLF", "Industrials": "XLI",
    "Consumer Cyclical": "XLY", "Consumer Discretionary": "XLY",
    "Consumer Defensive": "XLP", "Consumer Staples": "XLP", "Energy": "XLE",
    "Utilities": "XLU", "Real Estate": "XLRE", "Basic Materials": "XLB",
    "Materials": "XLB", "Communication Services": "XLC", "Telecommunications": "XLC"
  };
  var CURATED = [
    ["GLD", "COMMODITY", "Metals"], ["SLV", "COMMODITY", "Metals"], ["GDX", "COMMODITY", "Metals"],
    ["GDXJ", "COMMODITY", "Metals"], ["PPLT", "COMMODITY", "Metals"], ["CPER", "COMMODITY", "Metals"],
    ["COPX", "COMMODITY", "Metals"], ["DBB", "COMMODITY", "Metals"], ["LIT", "COMMODITY", "Metals"],
    ["URA", "COMMODITY", "Metals"], ["TLT", "BOND", "Bonds"], ["IEF", "BOND", "Bonds"],
    ["TIP", "BOND", "Bonds"], ["HYG", "BOND", "Bonds"], ["LQD", "BOND", "Bonds"],
    ["EMB", "BOND", "Bonds"], ["BTC-USD", "CRYPTO", "Crypto"], ["ETH-USD", "CRYPTO", "Crypto"]
  ];
  var SECTORS = ["XLK", "XLV", "XLF", "XLI", "XLY", "XLP", "XLE", "XLU", "XLRE", "XLB", "XLC"];
  var CRYPTO_LINKED = {
    MSTR: { name: "Strategy", kind: "Bitcoin treasury" },
    XXI: { name: "Twenty One", kind: "Bitcoin treasury" },
    ASST: { name: "Strive", kind: "Bitcoin treasury" },
    NAKA: { name: "Nakamoto", kind: "Bitcoin treasury" },
    BMNR: { name: "Bitmine", kind: "Ether treasury" },
    SBET: { name: "SharpLink", kind: "Ether treasury" },
    BTCS: { name: "BTCS", kind: "Ether treasury" },
    FGNX: { name: "FG Nexus", kind: "Ether treasury" },
    ETHA: { name: "iShares Ether", kind: "Ether ETF", asset: "ETF" },
    BSOL: { name: "Bitwise Solana", kind: "Altcoin ETF", asset: "ETF" },
    XRPC: { name: "Canary XRP", kind: "Altcoin ETF", asset: "ETF" },
    LTCC: { name: "Canary Litecoin", kind: "Altcoin ETF", asset: "ETF" },
    DOJE: { name: "REX-Osprey Doge", kind: "Altcoin ETF", asset: "ETF" },
    THYP: { name: "21Shares Hyperliquid", kind: "Altcoin ETF", asset: "ETF" }
  };

  function num(x) { return typeof x === "number" && isFinite(x) ? x : null; }
  function volOf(b) { return num(b.volume) || num(b.value) || 0; }
  function median(a) {
    var b = a.filter(function (x) { return x != null && isFinite(x); }).sort(function (x, y) { return x - y; });
    if (!b.length) return null;
    return b[(b.length - 1) >> 1];
  }
  function smaAt(c, i, n) {
    if (i < n - 1) return null;
    var s = 0, k;
    for (k = i - n + 1; k <= i; k++) s += c[k];
    return s / n;
  }
  function rsiAt(c, i, n) {
    if (i < n) return null;
    var ag = 0, al = 0, k, ch, g, l;
    for (k = 1; k <= n; k++) {
      ch = c[k] - c[k - 1];
      ag += Math.max(ch, 0); al += Math.max(-ch, 0);
    }
    ag /= n; al /= n;
    for (k = n + 1; k <= i; k++) {
      ch = c[k] - c[k - 1];
      g = Math.max(ch, 0); l = Math.max(-ch, 0);
      ag = (ag * (n - 1) + g) / n;
      al = (al * (n - 1) + l) / n;
    }
    if (al === 0) return 100;
    return 100 - 100 / (1 + ag / al);
  }
  function atrAt(d, i, n) {
    if (i < n) return null;
    var s = 0, k, tr, a;
    for (k = 1; k <= n; k++) {
      tr = Math.max(d[k].high - d[k].low, Math.abs(d[k].high - d[k - 1].close), Math.abs(d[k].low - d[k - 1].close));
      s += tr;
    }
    a = s / n;
    for (k = n + 1; k <= i; k++) {
      tr = Math.max(d[k].high - d[k].low, Math.abs(d[k].high - d[k - 1].close), Math.abs(d[k].low - d[k - 1].close));
      a = (a * (n - 1) + tr) / n;
    }
    return a;
  }
  function bbWidth(c, i) {
    var m = smaAt(c, i, 20);
    if (m == null || m === 0) return null;
    var s = 0, k;
    for (k = i - 19; k <= i; k++) s += (c[k] - m) * (c[k] - m);
    return (4 * Math.sqrt(s / 20)) / m;
  }
  function box(id, label, pass, detail, required) {
    return { id: id, label: label, pass: pass === true, detail: detail, required: required !== false };
  }
  function pct(x) { return x == null || !isFinite(x) ? "—" : (x >= 0 ? "+" : "") + x.toFixed(1) + "%"; }

  function score(raw, meta) {
    meta = meta || {};
    var d = (raw || []).filter(function (b) { return b && num(b.close) && num(b.high) && num(b.low); });
    if (d.length > 560) d = d.slice(-560);
    var checks = [];
    var i = d.length - 1;
    if (i < 320) {
      var short = "Need 320 daily bars to score this. Have " + d.length + ".";
      return {
        checks: [
          box("book", "S&P 500, Nasdaq-100, ETF, metal, bond, or crypto", !!meta.book, meta.book || "Not tagged", true),
          box("ma250", "Below the 250-day", false, short, true),
          box("offhigh", "At least 50% off the high", false, short, true),
          box("rsi", "RSI washed out", false, short, true),
          box("spread", "Very tight price spread", false, short, true),
          box("bb", "Tight Bollinger bands", false, short, true),
          box("volty", "Very low volatility", false, short, true),
          box("volume", "Shrinking volume", false, short, true),
          box("flat", "Flat moving average", false, short, true),
          box("support", "On 3-month support", false, short, true),
          box("higher", "Higher low", false, short, true),
          box("exhaust", "Selling climax or capitulation", false, short, true),
          box("demand", "Demand showing", false, short, true),
          box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", false, short, true),
          box("sector", "Industry ETF under, at, or just through its 200-day", false, short, true),
          box("flows", "ETF inflows, institutions, smart money", false, short, true)
        ].concat(meta.cryptoLinked ? [box("crypto", "ETH or BTC turned while this is still on its low", false, short, true)] : []).concat([
          box("ma300", "Below the 300-day", false, short, false),
          box("double", "Double bottom", false, short, false),
          box("campaign", "Marked bottom or end of accumulation", false, short, false),
          box("catalyst", "Booming industry or other catalyst", false, short, false),
          box("momentum", "Momentum turning up", false, short, false)
        ]),
        sniper: false, drawdown: null, close: i >= 0 ? d[i].close : null, passed: 0, required: 16
      };
    }
    var c = d.map(function (b) { return b.close; });
    var px = c[i];
    var sma250 = smaAt(c, i, 250);
    var sma300 = smaAt(c, i, 300);
    var sma20 = smaAt(c, i, 20);
    var sma20p = smaAt(c, i - 10, 20);
    var rsi = rsiAt(c, i, 14);
    var atr14 = atrAt(d, i, 14);
    var atr100 = atrAt(d, i, 100);
    var hi = -Infinity, k, lo63 = Infinity, loPrior = Infinity;
    var from = Math.max(0, i - 503);
    for (k = from; k <= i; k++) if (d[k].high > hi) hi = d[k].high;
    for (k = i - 62; k <= i; k++) if (d[k].low < lo63) lo63 = d[k].low;
    for (k = i - 140; k < i - 62; k++) if (d[k].low < loPrior) loPrior = d[k].low;
    var dd = hi ? px / hi - 1 : null;
    var under250 = sma250 != null && px < sma250;
    var allowedBook = { STOCK: 1, ETF: 1, COMMODITY: 1, BOND: 1, CRYPTO: 1 };
    checks.push(box("book", "S&P 500, Nasdaq-100, ETF, metal, bond, or crypto", !!allowedBook[meta.assetClass], meta.book || meta.assetClass || "Not in the book", true));
    checks.push(box("ma250", "Below the 250-day", under250,
      sma250 == null ? "250-day average unavailable" : "Close " + px.toFixed(2) + " vs 250-day " + sma250.toFixed(2) + " (" + pct((px / sma250 - 1) * 100) + ")", true));
    checks.push(box("offhigh", "At least 50% off the high", dd != null && dd <= -0.50,
      "Off the 504-day high by " + pct(dd == null ? null : dd * 100) + ". Deeper is better.", true));
    var rsiPass = rsi != null && rsi <= 45;
    checks.push(box("rsi", "RSI washed out", rsiPass,
      rsi == null ? "RSI unavailable" : "RSI(14) " + rsi.toFixed(1) + (rsi <= 30 ? " — oversold" : rsi <= 45 ? " — washed, not deeply oversold" : " — not washed out") + ". Gate is 45 so a finished base is not rejected for leaving the panic print. Deep oversold is under 30.", true));
    var widths = [];
    for (k = Math.max(20, i - 251); k <= i; k++) widths.push(bbWidth(c, k));
    var width = widths[widths.length - 1];
    var below = widths.filter(function (w) { return w != null && width != null && w <= width; }).length;
    var bbp = width == null ? null : 100 * below / widths.filter(function (w) { return w != null; }).length;
    checks.push(box("bb", "Tight Bollinger bands", bbp != null && bbp <= 20,
      bbp == null ? "Band width unavailable" : (bbp <= 1 ? "Bandwidth is the tightest of the past year." : "Bandwidth sits in the lowest " + bbp.toFixed(0) + "% of the past year. Gate is 20%."), true));
    var spreads = [];
    for (k = i - 59; k <= i; k++) spreads.push((d[k].high - d[k].low) / d[k].close);
    var recentSpread = median(spreads.slice(-10));
    var priorSpread = median(spreads.slice(0, 40));
    checks.push(box("spread", "Very tight price spread", recentSpread != null && priorSpread && recentSpread <= priorSpread * 0.75,
      recentSpread == null ? "Spread unavailable" : "Last 10-day spread " + (recentSpread * 100).toFixed(2) + "% vs prior " + (priorSpread * 100).toFixed(2) + "%. Must be at least 25% tighter.", true));
    var vols = [];
    for (k = i - 59; k <= i; k++) vols.push(volOf(d[k]));
    var recentVol = median(vols.slice(-12));
    var priorVol = median(vols.slice(0, 40));
    checks.push(box("volume", "Shrinking volume", recentVol != null && priorVol && priorVol > 0 && recentVol <= priorVol * 0.75,
      priorVol ? "Recent volume is " + (recentVol / priorVol).toFixed(2) + "× the prior 40 days. Gate is 0.75× or lower." : "No volume on the bars", true));
    var rel = atr14 && atr100 ? atr14 / atr100 : null;
    checks.push(box("volty", "Very low volatility", rel != null && rel <= 0.85,
      rel == null ? "Range unavailable" : "14-day range is " + rel.toFixed(2) + "× the 100-day range. Gate is 0.85×.", true));
    var flat = sma20 && sma20p ? Math.abs(sma20 / sma20p - 1) : null;
    checks.push(box("flat", "Flat moving average", flat != null && flat <= 0.012,
      flat == null ? "20-day average unavailable" : "20-day average moved " + pct(flat * 100) + " over 10 sessions. Gate is 1.2%.", true));
    var distSup = lo63 ? px / lo63 - 1 : null;
    checks.push(box("support", "On 3-month support", distSup != null && distSup >= -0.005 && distSup <= 0.035,
      distSup == null ? "Support unavailable" : "Close is " + pct(distSup * 100) + " from the 63-day low (" + lo63.toFixed(2) + "). Must be on it, not through it and not more than 3.5% above.", true));
    var higher = lo63 && loPrior && lo63 > loPrior * 1.005;
    checks.push(box("higher", "Higher low", higher,
      "63-day low " + (lo63 ? lo63.toFixed(2) : "—") + " vs the prior low " + (loPrior && isFinite(loPrior) ? loPrior.toFixed(2) : "—") + ".", true));
    var panic = false, panicDetail = "No capitulation bar in the last 90 sessions";
    for (k = Math.max(30, i - 90); k <= i - 3; k++) {
      var ret = d[k].close / d[k - 1].close - 1;
      var baseV = median(d.slice(k - 20, k).map(volOf));
      var rv = baseV ? volOf(d[k]) / baseV : 0;
      var loc = (d[k].high - d[k].low) ? (d[k].close - d[k].low) / (d[k].high - d[k].low) : 0;
      if (ret <= -0.03 && rv >= 1.7 && loc >= 0.25) { panic = true; panicDetail = "Capitulation on bar " + (i - k) + " sessions ago: " + pct(ret * 100) + " on " + rv.toFixed(1) + "× volume, closed off the low."; break; }
    }
    checks.push(box("exhaust", "Selling climax or capitulation", panic, panicDetail, true));
    var ups = 0;
    for (k = i - 5; k <= i; k++) if (d[k].close > d[k].open) ups++;
    var demand = px > lo63 && ups >= 2 && c[i] >= c[i - 3] * 0.995;
    checks.push(box("demand", "Demand showing", demand,
      ups + " of the last 6 bars closed up, and the close is holding the recent low.", true));
    var klass = meta.assetClass || "STOCK";
    var nonStock = klass === "ETF" || klass === "COMMODITY" || klass === "BOND" || klass === "CRYPTO";
    if (nonStock) {
      checks.push(box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", true, "Not a stock. P/E and PEG are not applied.", true));
    } else if (!meta.valuation) {
      checks.push(box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", false, "No P/E, PEG or P/S on the S&P book for this name.", true));
    } else {
      var v = meta.valuation;
      var pegOk = v.peg != null && v.peg > 0 && v.peg < 1;
      var peOk = v.pe != null && v.pe > 0 && v.sectorPe != null && v.pe <= v.sectorPe * 0.80;
      var psOk = v.ps != null && v.ps > 0 && v.sectorPs != null && v.ps <= v.sectorPs * 0.80;
      checks.push(box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", pegOk || peOk || psOk,
        "PEG " + (v.peg == null ? "—" : v.peg.toFixed(2)) + " (want under 1). P/E " + (v.pe == null ? "—" : v.pe.toFixed(1)) + " vs industry " + (v.sectorPe == null ? "—" : v.sectorPe.toFixed(1)) + ". P/S " + (v.ps == null ? "—" : v.ps.toFixed(2)) + " vs industry " + (v.sectorPs == null ? "—" : v.sectorPs.toFixed(2)) + ".", true));
    }
    if (meta.sectorEtf == null) {
      checks.push(box("sector", "Industry ETF under, at, or just through its 200-day", nonStock, nonStock ? "This is the vehicle, not a single stock." : "No sector ETF mapped.", true));
    } else {
      var sv = meta.sectorEtf.vs200;
      var sectorOk = sv != null && sv >= -15 && sv <= 8;
      checks.push(box("sector", "Industry ETF under, at, or just through its 200-day", sectorOk,
        (meta.sectorEtf.symbol || "ETF") + " is " + pct(sv) + " versus its 200-day. Allowed band is 15% under to 8% over. Not an extended market.", true));
    }
    if (!meta.flows) {
      checks.push(box("flows", "ETF inflows, institutions, smart money", false, "No verified fund-flow or 13F net buy on this name. Missing is not a yes.", true));
    } else {
      checks.push(box("flows", "ETF inflows, institutions, smart money", true, meta.flows, true));
    }
    if (meta.cryptoLinked) {
      checks.push(box("crypto", "ETH or BTC turned while this is still on its low", !!meta.cryptoLead, meta.cryptoNote || "No fresh ether or bitcoin turn while this name is still sitting on its own low.", true));
    }
    checks.push(box("ma300", "Below the 300-day", sma300 != null && px < sma300,
      sma300 == null ? "300-day unavailable" : "Close vs 300-day " + sma300.toFixed(2) + " (" + pct((px / sma300 - 1) * 100) + "). Better, not required.", false));
    var swings = [];
    for (k = i - 130; k <= i - 5; k++) {
      var isLow = true, j;
      for (j = k - 5; j <= k + 5; j++) if (j !== k && d[j] && d[j].low < d[k].low) isLow = false;
      if (isLow) swings.push(k);
    }
    var dbl = false, dblDetail = "No double bottom in the last 130 sessions";
    var a, b;
    for (a = 0; a < swings.length; a++) for (b = a + 1; b < swings.length; b++) {
      var gap = swings[b] - swings[a];
      if (gap < 10 || gap > 90) continue;
      var p1 = d[swings[a]].low, p2 = d[swings[b]].low;
      if (Math.abs(p1 - p2) / Math.max(p1, p2) > 0.04) continue;
      var valley = -Infinity, t;
      for (t = swings[a]; t <= swings[b]; t++) if (d[t].high > valley) valley = d[t].high;
      if (valley < Math.max(p1, p2) * 1.04) continue;
      if (px <= Math.max(p1, p2) * 1.06) { dbl = true; dblDetail = "Two lows " + p1.toFixed(2) + " and " + p2.toFixed(2) + ", " + gap + " sessions apart."; }
    }
    checks.push(box("double", "Double bottom", dbl, dblDetail, false));
    checks.push(box("campaign", "Marked bottom or end of accumulation", !!meta.campaign, meta.campaign || "Not on the live bottom board.", false));
    checks.push(box("catalyst", "Booming industry or other catalyst", !!meta.catalyst, meta.catalyst || "No industry-boom or contract catalyst on the tape.", false));
    var macdUp = false;
    if (i >= 40) {
      var e12 = null, e26 = null, seed12 = 0, seed26 = 0;
      var k12 = 2 / 13, k26 = 2 / 27;
      for (k = 0; k <= i; k++) {
        if (k < 12) seed12 += c[k];
        if (k === 11) e12 = seed12 / 12;
        else if (k > 11) e12 = c[k] * k12 + e12 * (1 - k12);
        if (k < 26) seed26 += c[k];
        if (k === 25) e26 = seed26 / 26;
        else if (k > 25) e26 = c[k] * k26 + e26 * (1 - k26);
      }
      var h0 = e12 - e26;
      var h1p = null, e12b = null, e26b = null, s12 = 0, s26 = 0;
      for (k = 0; k <= i - 3; k++) {
        if (k < 12) s12 += c[k];
        if (k === 11) e12b = s12 / 12;
        else if (k > 11) e12b = c[k] * k12 + e12b * (1 - k12);
        if (k < 26) s26 += c[k];
        if (k === 25) e26b = s26 / 26;
        else if (k > 25) e26b = c[k] * k26 + e26b * (1 - k26);
      }
      h1p = e12b - e26b;
      macdUp = h0 != null && h1p != null && h0 > h1p && rsi != null && rsi < 55;
    }
    checks.push(box("momentum", "Momentum turning up", macdUp, macdUp ? "MACD line is rising and RSI is still under 55." : "MACD is not turning up yet.", false));

    var required = checks.filter(function (x) { return x.required; });
    var sniper = required.every(function (x) { return x.pass; });
    return { checks: checks, sniper: sniper, drawdown: dd, close: px, passed: required.filter(function (x) { return x.pass; }).length, required: required.length };
  }

  root.jhSniperScore = score;
  if (typeof document === "undefined") return;

  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var mem = { promise: null };
  var showMisses = true;
  var category = "ALL";
  var subcat = "ALL";
  var CATS = ["Stocks", "ETFs", "Crypto", "Metals", "Bonds"];
  function groupOf(r) {
    var link = CRYPTO_LINKED[r.ticker];
    if (link) {
      if (link.asset === "ETF") return { cat: "ETFs", sub: link.kind };
      return { cat: "Crypto", sub: link.kind };
    }
    if (r.assetClass === "CRYPTO") return { cat: "Crypto", sub: "Spot" };
    if (r.assetClass === "COMMODITY") return { cat: "Metals", sub: r.industry || "Metal" };
    if (r.assetClass === "BOND") return { cat: "Bonds", sub: r.industry || "Bond" };
    if (r.assetClass === "ETF") return { cat: "ETFs", sub: r.industry || "ETF" };
    return { cat: "Stocks", sub: r.industry || "Unclassified" };
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      if (c === "&") return "\u0026amp;";
      if (c === "<") return "\u0026lt;";
      if (c === ">") return "\u0026gt;";
      return "\u0026quot;";
    });
  }
  function junk(name, ticker) {
    var s = (String(name || "") + " " + String(ticker || "")).toUpperCase();
    return /YIELDMAX|ULTRAPRO|ULTRASHORT|INVERSE|2X|3X|-1X|-2X|-3X/.test(s);
  }
  function flowText(row) {
    if (!row || !row.evidence) return null;
    var hits = [];
    ["flows", "accumulation"].forEach(function (key) {
      var list = row.evidence[key];
      if (!list) return;
      list.forEach(function (item) {
        var label = item && item.label;
        if (label === "Major fund inflow" || label === "Institutional ownership flow" || label === "Persistent flow score") hits.push(label);
      });
    });
    return hits.length ? hits.join(" · ") : null;
  }
  function catalystText(row) {
    if (!row || !row.evidence || !row.evidence.catalysts) return null;
    var hits = row.evidence.catalysts.map(function (item) { return item && item.label; }).filter(Boolean);
    return hits.length ? hits.join(" · ") : null;
  }
  function campaignText(board, ticker) {
    var row = null;
    (board || []).forEach(function (r) { if (r && String(r.ticker).toUpperCase() === ticker) row = r; });
    if (!row) return null;
    var st = String(row.state || "");
    if (/FAILED|STOPPED/.test(st)) return null;
    if (/MARKUP|COMPLETED|TRIGGERED|ST_CONFIRMED|CLIMAX|TESTING|NO_RALLY|NO_TEST/.test(st)) return st + (row.grade ? " · grade " + row.grade : "");
    return null;
  }
  function yahooSym(ticker) {
    if (ticker === "BTC") return "BTC-USD";
    if (ticker === "ETH") return "ETH-USD";
    return ticker;
  }
  function getJSON(url) {
    return fetch(url, { cache: "no-store" }).then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); });
  }
  function barsFor(ticker) {
    var sym = yahooSym(ticker);
    var urls = [
      PROXY + "/yf-ohlc?symbol=" + encodeURIComponent(sym) + "&range=2y&interval=1d",
      "/api/yahoo?ticker=" + encodeURIComponent(sym) + "&range=2y&interval=1d"
    ];
    function hit(i) {
      if (i >= urls.length) return Promise.resolve([]);
      return getJSON(urls[i]).then(function (d) {
        var bars = (d && (d.bars || d.chart)) || [];
        return bars.length >= 8 ? bars : hit(i + 1);
      }).catch(function () { return hit(i + 1); });
    }
    return hit(0);
  }
  function sectorState(cache, symbol) {
    var d = cache[symbol];
    if (!d || d.length < 220) return { symbol: symbol, vs200: null };
    var c = d.map(function (b) { return b.close; });
    var i = c.length - 1;
    var m = smaAt(c, i, 200);
    if (!m) return { symbol: symbol, vs200: null };
    return { symbol: symbol, vs200: (c[i] / m - 1) * 100 };
  }
  function buildList(khalid, sp, board) {
    var members = (sp && sp.members) || {};
    var ndx = {};
    NDX.split(",").forEach(function (t) { ndx[t] = 1; });
    var byTicker = {};
    ((khalid && khalid.opportunity_radar) || []).forEach(function (r) { if (r && r.ticker) byTicker[String(r.ticker).toUpperCase()] = r; });
    var sectorPe = {}, sectorPs = {}, sectorN = {};
    Object.keys(members).forEach(function (t) {
      var m = members[t];
      if (!m || !m.length) return;
      var sector = m[1], pe = num(m[3]), ps = num(m[6]);
      if (!sector) return;
      if (!sectorPe[sector]) { sectorPe[sector] = []; sectorPs[sector] = []; }
      if (pe != null && pe > 0 && pe < 200) sectorPe[sector].push(pe);
      if (ps != null && ps > 0 && ps < 100) sectorPs[sector].push(ps);
    });
    Object.keys(sectorPe).forEach(function (s) { sectorN[s] = { pe: median(sectorPe[s]), ps: median(sectorPs[s]) }; });
    var rows = [];
    var seen = {};
    function add(ticker, assetClass, industry, row) {
      ticker = String(ticker || "").toUpperCase();
      if (!ticker || seen[ticker] || junk((row && row.name), ticker)) return;
      seen[ticker] = 1;
      var member = members[ticker];
      var sector = (member && member[1]) || (row && (row.sector || row.industry)) || industry || null;
      var val = null;
      if (member) {
        var meds = sectorN[member[1]] || {};
        val = { pe: num(member[3]), peg: num(member[5]), ps: num(member[6]), sectorPe: meds.pe || null, sectorPs: meds.ps || null };
      }
      rows.push({
        ticker: ticker,
        name: (row && row.name) || (CRYPTO_LINKED[ticker] && CRYPTO_LINKED[ticker].name) || ticker,
        assetClass: assetClass || (row && row.asset_class) || "STOCK",
        industry: sector || "Unclassified",
        vs250: row && row.technical ? row.technical.vs_250d_pct : null,
        flows: flowText(row),
        catalyst: catalystText(row),
        campaign: campaignText(board, ticker),
        valuation: val,
        sectorSymbol: SECTOR_ETF[sector] || null,
        book: member ? ("S&P 500" + (sector ? " · " + sector : "")) : (ndx[ticker] ? "Nasdaq-100" : assetClass),
        cryptoLinked: !!CRYPTO_LINKED[ticker]
      });
    }
    Object.keys(byTicker).forEach(function (t) {
      var r = byTicker[t];
      var tech = r.technical || {};
      var inBook = members[t] || ndx[t];
      var klass = r.asset_class;
      if (tech.vs_250d_pct == null || tech.vs_250d_pct > 0) return;
      if (!(inBook || klass === "ETF" || klass === "COMMODITY" || klass === "BOND" || klass === "CRYPTO")) return;
      add(t, inBook && klass === "STOCK" ? "STOCK" : klass, r.industry || r.sector, r);
    });
    CURATED.forEach(function (c) { add(c[0], c[1], c[2], byTicker[c[0]] || byTicker[c[0].replace("-USD", "")]); });
    Object.keys(CRYPTO_LINKED).forEach(function (t) {
      var c = CRYPTO_LINKED[t];
      add(t, c.asset || "STOCK", c.kind, byTicker[t]);
    });
    rows.sort(function (a, b) { return (a.vs250 == null ? 0 : a.vs250) - (b.vs250 == null ? 0 : b.vs250); });
    var deep = rows.filter(function (r) { return r.assetClass === "STOCK"; }).slice(0, 60);
    var vehicles = rows.filter(function (r) { return r.assetClass !== "STOCK"; }).slice(0, 30);
    var keep = {};
    deep.concat(vehicles).forEach(function (r) { keep[r.ticker] = r; });
    SECTORS.forEach(function (t) { if (!keep[t]) keep[t] = { ticker: t, name: t, assetClass: "ETF", industry: "Sector ETF", vs250: null, flows: null, catalyst: null, campaign: null, valuation: null, sectorSymbol: t }; });
    return Object.keys(keep).map(function (k) { return keep[k]; });
  }

  function marketHtml(m) {
    if (!m) return "<p class='sn-note'>Early flags load with the bars: ether, bitcoin, small caps versus large caps, microcaps versus large caps.</p>";
    function cell(title, on, text) {
      return "<div class='" + (on ? "ok" : "") + "'><b>" + (on ? "✓ " : "· ") + esc(title) + "</b><span>" + esc(text) + "</span></div>";
    }
    function turnText(name, t) {
      if (!t) return "No " + name + " bars.";
      return name + " is " + (t.off * 100).toFixed(0) + "% off its 6-month low, " + t.age + " sessions later. A fresh turn is a 12% bounce within 30 sessions. Scored only on bitcoin treasuries, ether treasuries, and altcoin ETFs. Not on miners, and not on any other stock. In 2026 ether bottomed June 6, Strategy June 26, Bitmine June 30, bitcoin July 1.";
    }
    function ratioText(label, t, research) {
      if (!t) return "No " + label + " bars.";
      return label + " is " + (t.off * 100).toFixed(1) + "% off its 63-day low. " + research;
    }
    return "<div class='sn-mkt'>" +
      cell("Ether lead", m.eth && m.eth.on, turnText("Ether", m.eth)) +
      cell("Bitcoin lead", m.btc && m.btc.on, turnText("Bitcoin", m.btc)) +
      cell("Small caps versus large caps", m.small && m.small.on, ratioText("IWM versus SPY", m.small, "When small caps turn up off a 63-day low against the S&P, the next 63 sessions of QQQ were about +7% versus +4% on an ordinary day. Both halves of the history agreed. That is a market tilt, not a reason to buy one stock.")) +
      cell("Microcaps versus large caps", m.micro && m.micro.on, ratioText("IWC versus SPY", m.micro, "The microcap version of the same test was weaker than small versus large, and it did not pick crypto treasuries. Credit and copper versus gold did not beat an ordinary quarter, so they are not on this board.")) +
      "</div>";
  }

  function render(host, pack) {
    if (!host) return;
    var rows = (pack.rows || []).slice().sort(function (a, b) {
      if (a.scored.sniper !== b.scored.sniper) return a.scored.sniper ? -1 : 1;
      var ap = a.scored.passed || 0, bp = b.scored.passed || 0;
      if (ap !== bp) return bp - ap;
      return (a.scored.drawdown || 0) - (b.scored.drawdown || 0);
    });
    var snipers = rows.filter(function (r) { return r.scored.sniper; });
    var inCat = rows.filter(function (r) { return category === "ALL" || groupOf(r).cat === category; });
    var subs = [];
    inCat.forEach(function (r) {
      var s = groupOf(r).sub;
      if (s && subs.indexOf(s) < 0) subs.push(s);
    });
    subs.sort();
    if (subcat !== "ALL" && subs.indexOf(subcat) < 0) subcat = "ALL";
    var view = inCat.filter(function (r) {
      if (subcat !== "ALL" && groupOf(r).sub !== subcat) return false;
      if (!showMisses && !r.scored.sniper) return false;
      return true;
    });
    var html = "" +
      "<div class='sn-head'><div><p class='sn-eye'>KHALID SNIPER</p><h2>Every criterion, on every name</h2></div>" +
      "<div class='sn-tools'><label>Category <select id='sn-cat'><option value='ALL'>All</option>" +
      CATS.map(function (x) { return "<option value='" + esc(x) + "'" + (x === category ? " selected" : "") + ">" + esc(x) + "</option>"; }).join("") +
      "</select></label><label>Inside it <select id='sn-sub'><option value='ALL'>All</option>" +
      subs.map(function (x) { return "<option value='" + esc(x) + "'" + (x === subcat ? " selected" : "") + ">" + esc(x) + "</option>"; }).join("") +
      "</select></label><button type='button' id='sn-miss'>" + (showMisses ? "Snipers only" : "Show every name") + "</button></div></div>" +
      "<ol class='sn-legend'>" +
      ["S&P 500, Nasdaq-100, ETF, metal, bond, or crypto",
        "Below the 250-day", "At least 50% off the high", "RSI washed out",
        "Very tight price spread", "Tight Bollinger bands", "Very low volatility", "Shrinking volume",
        "Flat moving average", "On 3-month support", "Higher low", "Selling climax or capitulation", "Demand showing",
        "Cheap: PEG under 1, or P/E and P/S under the industry",
        "Industry ETF under, at, or just through its 200-day",
        "ETF inflows, institutions, smart money",
        "ETH or BTC turned, only on bitcoin treasuries, ether treasuries, and altcoin ETFs",
        "Below the 300-day", "Double bottom", "Marked bottom or end of accumulation",
        "Booming industry or other catalyst", "Momentum turning up"
      ].map(function (label, n) { return "<li><b>" + (n + 1) + "</b> " + esc(label) + "</li>"; }).join("") +
      "</ol>" +
      marketHtml(pack.market) +
      "<p class='sn-note'>" + esc(pack.note) + "</p>" +
      "<p class='sn-count'>" + snipers.length + " sniper" + (snipers.length === 1 ? "" : "s") + " · " + view.length + " shown · " + rows.length + " scored · " + (pack.failed || 0) + " had no usable bars</p>";
    if (pack.rows && pack.rows.length && !view.length) {
      html += "<p class='sn-empty'>Nothing in this category.</p>";
    }
    view.forEach(function (r) {
      var s = r.scored;
      var g = groupOf(r);
      html += "<article class='sn-card" + (s.sniper ? " is-sniper" : "") + "'>" +
        "<header><a class='sn-tick' href='/chart.html?s=" + encodeURIComponent(r.ticker) + "' data-sym='" + esc(r.ticker) + "'>" + esc(r.ticker) + "</a>" +
        "<span class='sn-name'>" + esc(r.name) + "</span>" +
        "<span class='sn-ind'>" + esc(g.cat + " · " + g.sub) + "</span>" +
        "<b class='sn-badge'>" + (s.sniper ? "SNIPER" : (s.passed || 0) + "/" + (s.required || 0)) + "</b></header>" +
        "<div class='sn-boxes'>" + s.checks.map(function (c) {
          return "<div class='sn-box" + (c.pass ? " ok" : "") + (c.required ? "" : " bonus") + "'><i>" + (c.pass ? "✓" : "·") + "</i><div><b>" + esc(c.label) + (c.required ? "" : " · better") + "</b><span>" + esc(c.detail) + "</span></div></div>";
        }).join("") + "</div></article>";
    });
    html += "<section class='sn-memo'><h3>Criteria this engine starts from</h3><ol>" +
      ["Book: S&P 500, Nasdaq-100, an ETF, a metal, a bond, or crypto.",
        "Required: below the 250-day. Below the 300-day is better, not required.",
        "Required: at least 50% off the high.",
        "Required: RSI washed out, at or under 45.",
        "Required: very tight price spread, tight Bollinger bands, very low volatility, shrinking volume, and a flat 20-day average.",
        "Required: on or within about 3.5% of 3-month support, a higher low, a selling-climax or capitulation bar, and demand showing.",
        "Required on stocks: PEG under 1, or both P/E and P/S at least 20% under the industry. Missing valuation stays open.",
        "Required: the industry ETF is between 15% under and 8% over its 200-day, unless the name is itself the vehicle.",
        "Required: a verified ETF inflow or institutional buy. Missing flow data stays open.",
        "Required only on bitcoin treasuries, ether treasuries, and altcoin ETFs: ether or bitcoin has turned off a 6-month low and this name is still within 10% of its own low. Not scored on miners or any other stock.",
        "Better, not required: double bottom, a marked bottom or the end of accumulation, a booming industry, momentum turning up."
      ].map(function (line) { return "<li>" + esc(line) + "</li>"; }).join("") +
      "</ol><p>Left out on purpose: fib 2.618 and 3.618 extensions, a second-peak volume rule, and extra RSI or MACD filters on the other engines. Those were tested and did not beat the rules already here. Small caps turning up versus large caps is a market tilt on this page, not a stock gate.</p></section>";
    host.innerHTML = "<style>" + STYLES + "</style>" + html;
    var catSel = host.querySelector("#sn-cat");
    if (catSel) catSel.onchange = function () { category = catSel.value; subcat = "ALL"; render(host, pack); };
    var subSel = host.querySelector("#sn-sub");
    if (subSel) subSel.onchange = function () { subcat = subSel.value; render(host, pack); };
    var btn = host.querySelector("#sn-miss");
    if (btn) btn.onclick = function () { showMisses = !showMisses; render(host, pack); };
    host.querySelectorAll("[data-sym]").forEach(function (a) {
      a.onclick = function (ev) {
        if (root.jhOpenSymbol) { ev.preventDefault(); root.jhOpenSymbol(a.getAttribute("data-sym")); }
      };
    });
  }

  var STYLES = ""
    + ".sn-head{display:flex;justify-content:space-between;gap:16px;align-items:flex-end;flex-wrap:wrap}"
    + ".sn-eye{letter-spacing:.14em;font-size:11px;color:#f0b429;margin:0}"
    + ".sn-head h2{margin:4px 0 0;font-size:22px}"
    + ".sn-tools{display:flex;gap:8px;align-items:center}"
    + ".sn-tools select,.sn-tools button{background:#1e222d;color:#d1d4dc;border:1px solid #2a2e39;border-radius:6px;padding:8px 10px}"
    + ".sn-note,.sn-count,.sn-empty{color:#9aa1ad;font-size:13px;line-height:1.45}"
    + ".sn-card{border:1px solid #2a2e39;border-radius:10px;padding:12px;margin:10px 0;background:#131722}"
    + ".sn-card.is-sniper{border-color:#089981}"
    + ".sn-card header{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}"
    + ".sn-tick{color:#f0b429;font-weight:700;text-decoration:none;font-size:16px}"
    + ".sn-name{color:#d1d4dc}.sn-ind{color:#787b86;font-size:12px}"
    + ".sn-badge{margin-left:auto;color:#d1d4dc}"
    + ".sn-card.is-sniper .sn-badge{color:#089981}"
    + ".sn-boxes{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:8px;margin-top:10px}"
    + ".sn-box{display:flex;gap:8px;padding:8px;border-radius:8px;background:#0d1118;color:#8b919c;font-size:12px;line-height:1.35}"
    + ".sn-box.ok{color:#d1d4dc}.sn-box.ok i{color:#089981}.sn-box i{font-style:normal;width:14px;color:#5c6370}"
    + ".sn-box b{display:block;font-size:12px}.sn-box.bonus{opacity:1}"
    + ".sn-legend{display:grid;grid-template-columns:1fr 1fr;gap:6px 22px;margin:12px 0;padding:14px 18px 14px 34px;background:#0d1118;border:1px solid #2a2e39;border-radius:10px}"
    + ".sn-legend li{color:#d1d4dc;font-size:13px;line-height:1.35}"
    + ".sn-mkt{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:8px 0}"
    + ".sn-mkt div{background:#1e222d;border-radius:8px;padding:10px;color:#9aa1ad;font-size:12px;line-height:1.4}"
    + ".sn-mkt b{display:block;color:#d1d4dc;margin-bottom:4px}.sn-mkt div.ok b{color:#089981}"
    + ".sn-memo{margin-top:22px;border:1px solid #2a2e39;border-radius:10px;padding:14px 16px;background:#0d1118}"
    + ".sn-memo h3{margin:0 0 8px;font-size:12px;letter-spacing:.12em;color:#f0b429}"
    + ".sn-memo ol{margin:0;padding-left:18px;color:#d1d4dc;font-size:13px;line-height:1.45}"
    + ".sn-memo p{color:#9aa1ad;font-size:12px;line-height:1.45}"
    + "@media(max-width:760px){.sn-boxes,.sn-legend,.sn-mkt{grid-template-columns:1fr}}";

  function freshTurn(bars) {
    var d = bars || [];
    if (d.length > 400) d = d.slice(-400);
    if (d.length < 150) return null;
    var i = d.length - 1, lowI = i - 126, k, sma = 0;
    for (k = i - 126; k <= i; k++) if (d[k].low < d[lowI].low) lowI = k;
    for (k = i - 19; k <= i; k++) sma += d[k].close;
    sma /= 20;
    var age = i - lowI;
    var off = d[i].close / d[lowI].low - 1;
    return { on: age >= 3 && age <= 30 && off >= 0.12 && d[i].close > sma, age: age, off: off };
  }
  function cryptoNoteFor(eth, btc, nearLow, linked) {
    if (!linked) return "";
    function one(name, t) {
      if (!t) return name + " unavailable";
      return name + " is " + (t.off * 100).toFixed(0) + "% off its 6-month low, " + t.age + " sessions ago" + (t.on ? ", and the turn is fresh" : "");
    }
    return one("Ether", eth) + ". " + one("Bitcoin", btc) + ". " + (nearLow ? "This name is still within 10% of its own 63-day low." : "This name has already left its low, so the early window is shut.") + " This box is only for bitcoin treasuries, ether treasuries, and altcoin ETFs. In 2026 ether's low was June 6, Strategy's June 26, Bitmine's June 30, bitcoin's July 1. It is not applied to any other stock.";
  }
  function ratioTurn(a, b) {
    if (!a || !b || a.length < 80 || b.length < 80) return null;
    var map = {};
    b.forEach(function (bar) { map[Math.floor(bar.time / 86400)] = bar; });
    var rows = [];
    a.slice(-180).forEach(function (bar) {
      var o = map[Math.floor(bar.time / 86400)];
      if (o && o.close) rows.push(bar.close / o.close);
    });
    if (rows.length < 70) return null;
    var i = rows.length - 1, lo = rows[i - 63], k;
    for (k = i - 63; k <= i; k++) if (rows[k] < lo) lo = rows[k];
    var off = rows[i] / lo - 1;
    return { on: off >= 0.04 && off <= 0.12, off: off };
  }
  function marketState(cache) {
    var eth = freshTurn(cache["ETH-USD"]);
    var btc = freshTurn(cache["BTC-USD"]);
    var small = ratioTurn(cache.IWM, cache.SPY);
    var micro = ratioTurn(cache.IWC, cache.SPY);
    return { eth: eth, btc: btc, small: small, micro: micro };
  }

  function scanAll() {
    if (mem.promise) return mem.promise;
    mem.promise = Promise.all([
      getJSON("/data/khalid.json").catch(function () { return getJSON(PROXY + "/data/khalid.json"); }),
      getJSON("/data/sp500.json"),
      getJSON("/data/bottom.json").catch(function () { return { board: [] }; })
    ]).then(function (all) {
      var list = buildList(all[0], all[1], (all[2] && all[2].board) || []);
      var barCache = {};
      var queue = list.map(function (r) { return r.ticker; });
      ["ETH-USD", "BTC-USD", "IWM", "IWC", "SPY"].concat(SECTORS).forEach(function (t) { if (queue.indexOf(t) < 0) queue.push(t); });
      var n = 0;
      function pump(worker) {
        if (!queue.length) return Promise.resolve();
        var t = queue.shift();
        return barsFor(t).then(function (bars) { barCache[t] = bars; n++; return pump(worker); });
      }
      var workers = [];
      for (var w = 0; w < 4; w++) workers.push(pump(w));
      return Promise.all(workers).then(function () {
        var failed = 0;
        var scored = list.map(function (r) {
          var etfSym = r.sectorSymbol;
          var sector = etfSym ? sectorState(barCache, etfSym) : null;
          var nearLow = false;
          var stockBars = barCache[r.ticker] || [];
          if (stockBars.length > 70) {
            var tail = stockBars.slice(-70);
            var lo = tail[0].low, last = tail[tail.length - 1].close;
            tail.forEach(function (b) { if (b.low < lo) lo = b.low; });
            nearLow = last <= lo * 1.10;
          }
          var eth = freshTurn(barCache["ETH-USD"]);
          var btc = freshTurn(barCache["BTC-USD"]);
          var lead = r.cryptoLinked && nearLow && ((eth && eth.on) || (btc && btc.on));
          var cryptoNote = cryptoNoteFor(eth, btc, nearLow, r.cryptoLinked);
          var scoredRow = score(stockBars, {
            assetClass: r.assetClass,
            book: r.book,
            valuation: r.valuation,
            sectorEtf: r.assetClass === "STOCK" ? sector : null,
            flows: r.flows,
            catalyst: r.catalyst,
            campaign: r.campaign,
            cryptoLinked: r.cryptoLinked,
            cryptoLead: lead,
            cryptoNote: cryptoNote
          });
          if (!(barCache[r.ticker] || []).length) failed++;
          return { ticker: r.ticker, name: r.name, industry: r.industry, assetClass: r.assetClass, scored: scoredRow };
        }).filter(function (r) { return r.scored.checks && r.scored.checks.length; });
        return {
          rows: scored.filter(function (r) { return SECTORS.indexOf(r.ticker) < 0 || r.scored.sniper || (r.scored.passed || 0) >= 8; }),
          failed: failed,
          market: marketState(barCache),
          note: "Every criterion is listed above. The ether and bitcoin box is scored only on bitcoin treasuries, ether treasuries, and altcoin ETFs. It is not a box on any other stock. A box is checked only when that test passes. Missing flow data stays open."
        };
      });
    });
    return mem.promise;
  }

  function mount(host) {
    if (!host) return;
    render(host, { rows: [], failed: 0, market: null, note: "Scoring daily bars. The list above is every criterion. Nothing is pre-checked." });
    scanAll().then(function (pack) { render(host, pack); }).catch(function (err) {
      host.innerHTML = "<p class='sn-note'>Sniper could not score the book. " + esc(err && err.message) + "</p>";
    });
  }
  root.jhSniperMount = mount;

  function boot() {
    var host = document.getElementById("k-sniper-host");
    if (host) mount(host);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})(typeof window !== "undefined" ? window : globalThis);
