/* jh-etf-engine.js — ETF desk engine: prices, D/W/M/3M vs SPX, Polygon flows, A/D, pattern. */
(function (w) {
  "use strict";
  if (w.JHEtf) return;
  var UNIVERSE = [
    { t: "SPY", c: "US", n: "S&P 500" }, { t: "VOO", c: "US", n: "Vanguard S&P 500" },
    { t: "IVV", c: "US", n: "iShares S&P 500" }, { t: "QQQ", c: "US", n: "Nasdaq 100" },
    { t: "QQQM", c: "US", n: "Nasdaq 100 Mid" }, { t: "IWM", c: "US", n: "Russell 2000" },
    { t: "VTI", c: "US", n: "Total US" }, { t: "DIA", c: "US", n: "Dow 30" }, { t: "RSP", c: "US", n: "Equal-weight S&P" },
    { t: "XLK", c: "SECTOR", n: "Technology" }, { t: "SMH", c: "SECTOR", n: "Semiconductors" },
    { t: "XLF", c: "SECTOR", n: "Financials" }, { t: "XLE", c: "SECTOR", n: "Energy" },
    { t: "XLV", c: "SECTOR", n: "Health Care" }, { t: "XLY", c: "SECTOR", n: "Discretionary" },
    { t: "XLP", c: "SECTOR", n: "Staples" }, { t: "XLI", c: "SECTOR", n: "Industrials" },
    { t: "XLU", c: "SECTOR", n: "Utilities" }, { t: "XLB", c: "SECTOR", n: "Materials" },
    { t: "XLRE", c: "SECTOR", n: "Real Estate" }, { t: "XLC", c: "SECTOR", n: "Communication" },
    { t: "XBI", c: "SECTOR", n: "Biotech" }, { t: "KRE", c: "SECTOR", n: "Regional Banks" },
    { t: "SOXX", c: "SECTOR", n: "PHLX Semi" }, { t: "ARKK", c: "SECTOR", n: "ARK Innovation" },
    { t: "TLT", c: "RATES", n: "20Y+ Treasury" }, { t: "IEF", c: "RATES", n: "7-10Y Treasury" },
    { t: "SHY", c: "RATES", n: "1-3Y Treasury" }, { t: "BIL", c: "RATES", n: "T-Bills" },
    { t: "TIP", c: "RATES", n: "TIPS" }, { t: "GOVT", c: "RATES", n: "US Treasury" },
    { t: "AGG", c: "CREDIT", n: "US Aggregate" }, { t: "BND", c: "CREDIT", n: "Total Bond" },
    { t: "LQD", c: "CREDIT", n: "IG Corporate" }, { t: "HYG", c: "CREDIT", n: "High Yield" },
    { t: "JNK", c: "CREDIT", n: "HY Bond" }, { t: "USHY", c: "CREDIT", n: "Broad HY" },
    { t: "EMB", c: "CREDIT", n: "EM Sovereign $" }, { t: "FALN", c: "CREDIT", n: "Fallen Angels" },
    { t: "GLD", c: "METAL", n: "Gold" }, { t: "IAU", c: "METAL", n: "Gold (low fee)" },
    { t: "SLV", c: "METAL", n: "Silver" }, { t: "GDX", c: "METAL", n: "Gold Miners" },
    { t: "USO", c: "CMDTY", n: "WTI Oil" }, { t: "UNG", c: "CMDTY", n: "Natural Gas" },
    { t: "DBC", c: "CMDTY", n: "Commodities" }, { t: "DBA", c: "CMDTY", n: "Agriculture" },
    { t: "EEM", c: "INTL", n: "Emerging Mkts" }, { t: "VWO", c: "INTL", n: "EM (Vanguard)" },
    { t: "EFA", c: "INTL", n: "EAFE" }, { t: "IEFA", c: "INTL", n: "EAFE (core)" },
    { t: "FXI", c: "INTL", n: "China Large-Cap" }, { t: "EWJ", c: "INTL", n: "Japan" },
    { t: "EWZ", c: "INTL", n: "Brazil" }, { t: "INDA", c: "INTL", n: "India" },
    { t: "IBIT", c: "CRYPTO", n: "Bitcoin" }, { t: "FBTC", c: "CRYPTO", n: "Bitcoin (Fidel.)" },
    { t: "ETHA", c: "CRYPTO", n: "Ether" }, { t: "BITO", c: "CRYPTO", n: "BTC Futures" },
    { t: "MTUM", c: "FACTOR", n: "Momentum" }, { t: "QUAL", c: "FACTOR", n: "Quality" },
    { t: "USMV", c: "FACTOR", n: "Min Vol" }, { t: "MOAT", c: "FACTOR", n: "Wide Moat" }
  ];
  var CATS = ["ALL", "US", "SECTOR", "RATES", "CREDIT", "METAL", "CMDTY", "INTL", "CRYPTO", "FACTOR"];

  function mergeCensus(mx) {
    var map = {};
    if (!mx || !mx.tickers || !mx.cols) return map;
    mx.tickers.forEach(function (t, i) {
      map[t] = {
        aum: (mx.cols.aum_usd_m || [])[i],
        patternDb: (mx.cols.double_bottom || [])[i],
        patternDt: (mx.cols.double_top || [])[i],
        gc: (mx.cols.golden_cross_10_40w || [])[i],
        bo: (mx.cols.breakout_20w || [])[i],
        rs13: (mx.cols.rs_13w_pct || [])[i],
        name: (mx.cols.name || mx.cols.fund_name || [])[i]
      };
    });
    return map;
  }
  function censusPattern(c) {
    if (!c) return null;
    if (c.gc) return "GOLDEN CROSS";
    if (c.bo) return "BREAKOUT";
    if (c.patternDb) return "DOUBLE BOTTOM";
    if (c.patternDt) return "DOUBLE TOP";
    return null;
  }
  function flowLabel(sig, z) {
    if (sig === "HEAVY_INFLOW") return "INFLOW";
    if (sig === "HEAVY_OUTFLOW") return "OUTFLOW";
    if (sig === "ROTATION_IN") return "ROTATION IN";
    if (sig === "ROTATION_OUT") return "ROTATION OUT";
    if (sig === "UNUSUAL_VOL") return "UNUSUAL $VOL";
    if (z != null && z >= 1.5) return "ELEVATED";
    return "QUIET";
  }

  async function run() {
    var D = w.JHDesk;
    var tickers = UNIVERSE.map(function (x) { return x.t; });
    var pack = await Promise.all([
      D.feed("data/etf-desk.json"),
      D.feed("data/etf-flows.json"),
      D.feed("data/etf-census-matrix.json"),
      D.quotes(tickers),
      D.ohlc("SPY", "6mo")
    ]);
    var desk = pack[0], flows = pack[1], census = pack[2], qx = pack[3] || {}, spyBars = pack[4] || [];
    var spyH = D.horizons(D.closesOf(spyBars));
    var spyQ = qx.SPY || {};
    if (spyQ.changePct != null) spyH.d = D.round(spyQ.changePct, 2);
    var byFlow = (desk && desk.by_etf) || (flows && flows.by_etf) || {};
    var cx = mergeCensus(census);

    var barsMap = {};
    await D.pool(tickers, 6, async function (t) {
      try { barsMap[t] = await D.ohlc(t, "6mo"); } catch (e) { barsMap[t] = []; }
    });

    var rows = UNIVERSE.map(function (u) {
      var t = u.t;
      var q = qx[t] || {};
      var f = byFlow[t] || {};
      var bars = barsMap[t] || [];
      var h = D.horizons(D.closesOf(bars));
      if (q.changePct != null) h.d = D.round(q.changePct, 2);
      if (h.w == null && f.return_5d_pct != null) h.w = f.return_5d_pct;
      if (h.m == null && (f.return_21d_pct != null || f.return_20d_pct != null)) h.m = f.return_21d_pct != null ? f.return_21d_pct : f.return_20d_pct;
      if (h.q == null && f.return_63d_pct != null) h.q = f.return_63d_pct;
      var vs = D.vsSpy(h, spyH);
      var ad = D.adPhase(bars, h);
      if (f.ad_phase) ad.phase = f.ad_phase;
      var pat = f.pattern || censusPattern(cx[t]) || D.pattern(bars);
      var px = q.price || f.latest_close || (bars.length ? bars[bars.length - 1].close : null);
      var z = f.dvol_z_score;
      var sig = f.flow_signal || flowLabel(null, z);
      return {
        ticker: t, name: (cx[t] && cx[t].name) || f.name || u.n, cat: u.c,
        px: px, h: h, vs: vs, ad: ad.phase, cmf: ad.cmf, pattern: pat,
        flow: flowLabel(f.flow_signal, z), flowRaw: f.flow_signal || sig,
        z: z, aum: f.aum_b != null ? f.aum_b * 1000 : (cx[t] && cx[t].aum),
        dvol: f.today_dollar_vol_b, spark: D.closesOf(bars).slice(-40)
      };
    });

    var inflows = rows.filter(function (r) { return /INFLOW|ROTATION IN/.test(r.flow); }).sort(function (a, b) { return (b.z || 0) - (a.z || 0); });
    var outflows = rows.filter(function (r) { return /OUTFLOW|ROTATION OUT/.test(r.flow); }).sort(function (a, b) { return (b.z || 0) - (a.z || 0); });
    var adv = rows.filter(function (r) { return r.h.d != null && r.h.d > 0; }).length;
    var dec = rows.filter(function (r) { return r.h.d != null && r.h.d < 0; }).length;

    return {
      generated_at: new Date().toISOString(),
      spy: { h: spyH, px: spyQ.price },
      rows: rows, cats: CATS, inflows: inflows, outflows: outflows,
      breadth: { adv: adv, dec: dec, n: rows.length },
      sources: {
        quotes: "Polygon snapshot via data-proxy /quotes",
        history: "Warehouse / Yahoo daily bars",
        flows: "justhodl-etf-flows · data/etf-flows.json",
        census: "justhodl-etf-census · data/etf-census-matrix.json"
      }
    };
  }

  w.JHEtf = { UNIVERSE: UNIVERSE, CATS: CATS, run: run };
})(window);
