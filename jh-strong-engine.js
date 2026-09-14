/* jh-strong-engine.js — relative strength vs S&P 500 across ETFs, stocks, metals, crypto. */
(function (w) {
  "use strict";
  if (w.JHStrong) return;
  var SLEEVES = {
    ETF: [
      { t: "QQQ", n: "Nasdaq 100" }, { t: "IWM", n: "Russell 2000" }, { t: "DIA", n: "Dow 30" },
      { t: "XLK", n: "Technology" }, { t: "SMH", n: "Semis" }, { t: "XLF", n: "Financials" },
      { t: "XLE", n: "Energy" }, { t: "XLU", n: "Utilities" }, { t: "XLP", n: "Staples" },
      { t: "XLV", n: "Health Care" }, { t: "XLI", n: "Industrials" }, { t: "XLY", n: "Discretionary" },
      { t: "GLD", n: "Gold" }, { t: "TLT", n: "Long Treasury" }, { t: "HYG", n: "High Yield" },
      { t: "EEM", n: "Emerging Mkts" }, { t: "FXI", n: "China" }, { t: "EWJ", n: "Japan" },
      { t: "KRE", n: "Regional Banks" }, { t: "XBI", n: "Biotech" }
    ],
    STOCK: [
      { t: "AAPL", n: "Apple" }, { t: "MSFT", n: "Microsoft" }, { t: "NVDA", n: "NVIDIA" },
      { t: "AMZN", n: "Amazon" }, { t: "META", n: "Meta" }, { t: "GOOGL", n: "Alphabet" },
      { t: "TSLA", n: "Tesla" }, { t: "AVGO", n: "Broadcom" }, { t: "JPM", n: "JPMorgan" },
      { t: "LLY", n: "Eli Lilly" }, { t: "XOM", n: "Exxon" }, { t: "UNH", n: "UnitedHealth" },
      { t: "V", n: "Visa" }, { t: "MA", n: "Mastercard" }, { t: "HD", n: "Home Depot" },
      { t: "COST", n: "Costco" }, { t: "NFLX", n: "Netflix" }, { t: "AMD", n: "AMD" },
      { t: "ORCL", n: "Oracle" }, { t: "JNJ", n: "J&J" }
    ],
    METAL: [
      { t: "GLD", n: "Gold" }, { t: "SLV", n: "Silver" }, { t: "GDX", n: "Gold Miners" },
      { t: "GDXJ", n: "Jr Gold Miners" }, { t: "PPLT", n: "Platinum" }, { t: "CPER", n: "Copper" },
      { t: "IAU", n: "Gold (IAU)" }, { t: "SIVR", n: "Silver (SIVR)" }
    ],
    CRYPTO: [
      { t: "IBIT", n: "Bitcoin ETF" }, { t: "ETHA", n: "Ether ETF" }, { t: "FBTC", n: "Bitcoin (Fidel.)" },
      { t: "BITO", n: "BTC Futures" }
    ]
  };

  function posture(h, spy) {
    var d = h.d, sd = spy.d;
    if (d == null || sd == null) return { code: "NA", label: "—" };
    var rs = d - sd;
    if (sd <= -0.8 && d > 0) return { code: "BID", label: "UP WHILE SPX DUMPS", rs: rs };
    if (sd <= -0.8 && Math.abs(d) < 0.25) return { code: "FLAT", label: "FLAT WHILE SPX DUMPS", rs: rs };
    if (sd <= -0.8 && d < 0 && d > sd * 0.45) return { code: "HOLD", label: "DUMPING LESS THAN SPX", rs: rs };
    if (sd < 0 && d > 0) return { code: "BID", label: "BID VS SPX", rs: rs };
    if (sd < 0 && d > sd) return { code: "HOLD", label: "HOLDING UP", rs: rs };
    if (rs >= 0.4) return { code: "LEAD", label: "LEADING", rs: rs };
    if (rs <= -0.4) return { code: "LAG", label: "LAGGING", rs: rs };
    return { code: "INLINE", label: "IN LINE", rs: rs };
  }
  function score(vs) {
    if (!vs) return null;
    var w = 0, s = 0;
    [["d", 0.35], ["w", 0.30], ["m", 0.20], ["q", 0.15]].forEach(function (p) {
      if (vs[p[0]] != null) { s += vs[p[0]] * p[1]; w += p[1]; }
    });
    return w ? Math.round((s / w) * 100) / 100 : null;
  }

  async function run() {
    var D = w.JHDesk;
    var names = [{ t: "SPY", n: "S&P 500", sleeve: "BENCH" }];
    Object.keys(SLEEVES).forEach(function (sl) {
      SLEEVES[sl].forEach(function (x) { names.push({ t: x.t, n: x.n, sleeve: sl }); });
    });
    var tickers = names.map(function (x) { return x.t; });
    var pack = await Promise.all([D.quotes(tickers), D.ohlc("SPY", "6mo"), D.feed("data/etf-flows.json")]);
    var qx = pack[0] || {}, spyBars = pack[1] || [], flows = (pack[2] && pack[2].by_etf) || {};
    var spyH = D.horizons(D.closesOf(spyBars));
    if (qx.SPY && qx.SPY.changePct != null) spyH.d = D.round(qx.SPY.changePct, 2);

    var barsMap = {};
    await D.pool(tickers, 6, async function (t) {
      try { barsMap[t] = await D.ohlc(t, "6mo"); } catch (e) { barsMap[t] = []; }
    });

    var rows = names.filter(function (x) { return x.t !== "SPY"; }).map(function (u) {
      var q = qx[u.t] || {};
      var bars = barsMap[u.t] || [];
      var h = D.horizons(D.closesOf(bars));
      if (q.changePct != null) h.d = D.round(q.changePct, 2);
      var f = flows[u.t];
      if (h.w == null && f && f.return_5d_pct != null) h.w = f.return_5d_pct;
      if (h.m == null && f && f.return_20d_pct != null) h.m = f.return_20d_pct;
      var vs = D.vsSpy(h, spyH);
      var sc = score(vs);
      var post = posture(h, spyH);
      return {
        ticker: u.t, name: u.n, sleeve: u.sleeve,
        px: q.price || (bars.length ? bars[bars.length - 1].close : null),
        h: h, vs: vs, score: sc, post: post,
        spark: D.closesOf(bars).slice(-40)
      };
    });

    var focus = rows.filter(function (r) {
      return r.post.code === "BID" || r.post.code === "FLAT" || r.post.code === "HOLD";
    }).sort(function (a, b) { return (b.score || -999) - (a.score || -999); });

    return {
      generated_at: new Date().toISOString(),
      spy: { h: spyH, px: (qx.SPY && qx.SPY.price) || null },
      rows: rows, focus: focus, sleeves: ["ETF", "STOCK", "METAL", "CRYPTO"],
      dump: spyH.d != null && spyH.d < -0.4,
      sources: {
        quotes: "Polygon snapshot",
        history: "Warehouse / Yahoo daily",
        definition: "Strength = asset return − S&P 500 return on D / W / M / 3M. Composite 35/30/20/15."
      }
    };
  }

  w.JHStrong = { SLEEVES: SLEEVES, run: run, posture: posture, score: score };
})(window);
