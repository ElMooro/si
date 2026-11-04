/* jh-strong-engine.js — the strength desk's client: reads data/strength.json (justhodl-strength, daily 03:40 UTC from the
 * Polygon EOD bank, the native ETF desk's fund flows and the 13F per-ticker table) and overlays the live quote for the
 * day column and the day posture. Nothing is computed from the provider here; the artifact carries every figure with
 * its as-of date. v2 (2026-11-04): the old client-side 6-month engine (35/30/20/15 on D/W/M/3M, flows only for the SPY-
 * aligned 1-observation window) is retired. */
(function (w) {
  "use strict";
  if (w.JHStrong) return;
  var KEY = "data/strength.json";
  var DOC_TTL_MS = 10 * 60 * 1000;                 // the artifact changes once a day; quotes refresh every minute
  var cache = { doc: null, at: 0 };

  function posture(h, spy) {
    var d = h && h.d, sd = spy && spy.d;
    if (d == null || sd == null) return { code: "NA", label: "—", rs: null };
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

  function overlay(rows, qx, spyD) {
    rows.forEach(function (r) {
      var q = qx[r.ticker];
      r.live = null;
      if (q && q.price != null) {
        var d = q.changePct != null ? Math.round(q.changePct * 100) / 100 : null;
        r.live = { px: q.price, d: d, vs_d: (d != null && spyD != null) ? Math.round((d - spyD) * 100) / 100 : null,
                   posture: posture({ d: d }, { d: spyD }), asOf: q.updated || q.asOf || q.timestamp || null };
      }
    });
  }

  async function run() {
    var D = w.JHDesk;
    var doc = cache.doc && (Date.now() - cache.at) < DOC_TTL_MS ? cache.doc : await D.feed(KEY);
    if (!doc || doc.contract !== "strength.v1") throw new Error("data/strength.json is not published yet (justhodl-strength runs daily 03:40 UTC)");
    if (doc !== cache.doc) { cache.doc = doc; cache.at = Date.now(); }
    var rows = doc.rows || [];
    var desk = rows.filter(function (r) { return (r.scopes || []).indexOf("desk") >= 0; });
    var tickers = ["SPY"].concat(desk.map(function (r) { return r.ticker; }));
    var qx = {};
    try { qx = await D.quotes(tickers) || {}; } catch (e) { qx = {}; }
    var spyLive = qx.SPY && qx.SPY.price != null ? { px: qx.SPY.price, d: qx.SPY.changePct != null ? Math.round(qx.SPY.changePct * 100) / 100 : null } : null;
    overlay(rows, qx, spyLive ? spyLive.d : null);
    var spyD = spyLive && spyLive.d != null ? spyLive.d : (doc.benchmark && doc.benchmark.h ? doc.benchmark.h.d : null);
    var focus = desk.filter(function (r) {
      var p = r.live ? r.live.posture : r.posture;
      return p && (p.code === "BID" || p.code === "FLAT" || p.code === "HOLD");
    }).sort(function (a, b) { return (b.strength || -1) - (a.strength || -1); });
    return {
      doc: doc, rows: rows, desk: desk, focus: focus, spy: { live: spyLive, d: spyD, h: doc.benchmark ? doc.benchmark.h : {}, px: spyLive ? spyLive.px : (doc.benchmark ? doc.benchmark.px : null) },
      dump: spyD != null && spyD < -0.4, quotes_at: new Date().toISOString(), sleeves: Object.keys(doc.sleeves || {})
    };
  }

  w.JHStrong = { KEY: KEY, run: run, posture: posture, overlay: overlay };
})(window);
