/* JustHodl chart transforms (2026-10-05).
 * Pure functions shared by the chart engine for every series (market bars and macro/warehouse observations):
 *   - change(points, mode)  calendar-correct MoM / QoQ / YoY / change-from-year-ago / period change, on any interval
 *   - gapLine(d, evidence)  line data that breaks (whitespace) at rejected source records instead of joining across them
 *   - candles(d)            OHLC for scalar observations: open = prior period's value, close = this period's value,
 *                           high/low = extremes of the period (and the open) — the convention used for economic data
 * No network, no DOM. Values are never invented: a point whose comparison base is missing is omitted, not filled. */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.JHChartTransforms = factory();
})(typeof window === "object" ? window : globalThis, function () {
  "use strict";
  var DAY = 86400;
  // mode → comparison definition. months: calendar months back; days: calendar days back; prev: previous plotted point.
  var DEF = {
    dod: { days: 1, pct: true, label: "DoD %" },
    wow: { days: 7, pct: true, label: "WoW %" },
    mom: { months: 1, pct: true, label: "MoM %" },
    qoq: { months: 3, pct: true, label: "QoQ %" },
    yoy: { months: 12, pct: true, label: "YoY %" },
    yoyd: { months: 12, pct: false, label: "Change from year ago" },
    pch: { prev: true, pct: true, label: "% change" },
    chg: { prev: true, pct: false, label: "Change" }
  };
  function isPct(mode) { return !DEF[mode] || DEF[mode].pct; }
  function medianGapDays(pts) {
    if (!pts || pts.length < 2) return 0;
    var g = [];
    for (var i = 1; i < pts.length; i++) g.push(pts[i].time - pts[i - 1].time);
    g.sort(function (a, b) { return a - b; });
    return g[Math.floor(g.length / 2)] / DAY;
  }
  function monthKey(t) { var d = new Date(t * 1000); return d.getUTCFullYear() * 12 + d.getUTCMonth(); }
  function shiftMonths(t, k) {
    var d = new Date(t * 1000), day = d.getUTCDate();
    d.setUTCDate(1); d.setUTCMonth(d.getUTCMonth() - k);
    var last = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0)).getUTCDate();
    d.setUTCDate(Math.min(day, last));
    return d.getTime() / 1000;
  }
  // last index with pts[i].time <= t (binary search), or -1
  function asOf(pts, t) {
    var lo = 0, hi = pts.length - 1, r = -1;
    while (lo <= hi) { var m = (lo + hi) >> 1; if (pts[m].time <= t) { r = m; lo = m + 1; } else hi = m - 1; }
    return r;
  }
  function delta(v, b, pct) {
    if (!isFinite(v) || !isFinite(b)) return null;
    if (!pct) return v - b;
    if (b === 0) return null;
    return (v - b) / Math.abs(b) * 100;
  }
  /* points: [{time, value}] ascending. Returns [{time, value, base_time}] — only where an honest base exists. */
  function change(points, mode) {
    var def = DEF[mode];
    if (!def || !points || !points.length) return [];
    var pts = points.filter(function (p) { return p && isFinite(p.value) && p.value !== null; });
    var out = [], i, g = medianGapDays(pts);
    if (def.prev) {
      for (i = 1; i < pts.length; i++) {
        var dv = delta(pts[i].value, pts[i - 1].value, def.pct);
        if (dv !== null) out.push({ time: pts[i].time, value: dv, base_time: pts[i - 1].time });
      }
      return out;
    }
    if (def.months && g >= 25) {
      // monthly, quarterly, annual data: compare like calendar periods exactly (Mar vs Feb, Q2 vs Q1, 2024 vs 2023)
      var byKey = {};
      pts.forEach(function (p, ix) { byKey[monthKey(p.time)] = ix; });
      for (i = 0; i < pts.length; i++) {
        var j = byKey[monthKey(pts[i].time) - def.months];
        if (j === undefined) continue;
        var v1 = delta(pts[i].value, pts[j].value, def.pct);
        if (v1 !== null) out.push({ time: pts[i].time, value: v1, base_time: pts[j].time });
      }
      return out;
    }
    if (def.months && g >= 5) {
      // weekly data: the same week a year/quarter/month earlier (52/13/4 weeks back, the FRED convention), nearest
      // observation within half a week; no base inside that window → the point is omitted
      var back = { 12: 364, 3: 91, 1: 28 }[def.months] || Math.round(def.months * 30.44);
      for (i = 0; i < pts.length; i++) {
        var tw = pts[i].time - back * DAY, kw = asOf(pts, tw + 3.5 * DAY);
        if (kw < 0 || kw >= i || Math.abs(pts[kw].time - tw) > 3.5 * DAY) continue;
        var vw = delta(pts[i].value, pts[kw].value, def.pct);
        if (vw !== null) out.push({ time: pts[i].time, value: vw, base_time: pts[kw].time });
      }
      return out;
    }
    // daily / intraday: value as of the same calendar time one period earlier, never older than the tolerance
    var tol = def.days ? Math.max(4, g * 1.5) * DAY : Math.max(7, g * 1.5) * DAY;
    for (i = 0; i < pts.length; i++) {
      var target = def.days ? pts[i].time - def.days * DAY : shiftMonths(pts[i].time, def.months);
      if (target < pts[0].time - DAY) continue;
      var k = asOf(pts, target);
      if (k < 0 || k >= i || target - pts[k].time > tol) continue;
      var v2 = delta(pts[i].value, pts[k].value, def.pct);
      if (v2 !== null) out.push({ time: pts[i].time, value: v2, base_time: pts[k].time });
    }
    return out;
  }
  /* d: plotted bars [{time, close}], evidence: chart-observations.v1 (optional). Whitespace entries ({time}) are placed
   * at rejected source records that fall between plotted points, so the line breaks where the source has no value. */
  function gapLine(d, evidence, valueOf) {
    var val = valueOf || function (b) { return b.close; };
    var pts = d.map(function (b) { return { time: b.time, value: val(b) }; });
    if (!evidence || !Array.isArray(evidence.records) || pts.length < 2) return pts;
    var have = {}, gaps = {}, first = pts[0].time, last = pts[pts.length - 1].time;
    pts.forEach(function (p) { have[p.time] = 1; });
    evidence.records.forEach(function (r) {
      if (!r || r.accepted || !r.coordinate || !isFinite(r.coordinate.time)) return;
      var t = r.coordinate.time;
      if (t > first && t < last && !have[t]) gaps[t] = 1;
    });
    var keys = Object.keys(gaps);
    if (!keys.length) return pts;
    return pts.concat(keys.map(function (t) { return { time: +t }; })).sort(function (a, b) { return a.time - b.time; });
  }
  function candles(d) {
    var out = [];
    for (var i = 0; i < d.length; i++) {
      var b = d[i], o = i ? d[i - 1].close : (isFinite(b.open) ? b.open : b.close);
      var h = Math.max(o, isFinite(b.high) ? b.high : b.close, b.close), l = Math.min(o, isFinite(b.low) ? b.low : b.close, b.close);
      out.push({ time: b.time, open: o, high: h, low: l, close: b.close, volume: null });
    }
    return out;
  }
  return { change: change, gapLine: gapLine, candles: candles, isPct: isPct, DEF: DEF, medianGapDays: medianGapDays, shiftMonths: shiftMonths };
});
