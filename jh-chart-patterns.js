/* jh-reskin-skip */
/* JustHodl Chart Patterns v2 — D+ only, S&P-calibrated.
   Confirmation-only (no look-ahead). Track record: ^GSPC 1980–now + SPY 1993–now,
   next-open after confirm, 10-session horizon vs unconditional drift.

   KEPT as signals
     Double bottom (neckline close, quieter 2nd low): GSPC n=99 hit 65.7% mean +0.72%
       edge +0.30% t=3.1; SPY n=105 hit 64.8% mean +0.55%. Weekly GSPC n=26 hit 80.8%.
     3-touch resistance break: SPY n=39 hit 79.5% mean +1.07% edge +0.68% t=3.6;
       GSPC n=75 hit 76.0% mean +1.31% edge +0.89% t=4.8.

   KEPT as textbook measured-move targets (hit before invalidation, 80 bars)
     DB 0.5× neck-height: SPY 76% / GSPC 75%  — first objective.
     DB 1.0× neck-height (textbook): SPY 54% / GSPC 53%  — full measured move.
     R-BRK 0.382× range: SPY 64%  — first objective. GSPC 43% (cash-index ranges are huge).
     R-BRK 1.0× range (textbook): SPY 38% / GSPC 23%  — drawn as the book target, not a high-odds hit.

   KEPT as cycle events (existing gold list, 100% of S&P panics 1980–now)
     Livermore BOTTOM / TOP / REV-UP / REV-DN. Wyckoff SC/CAPIT · SOS/EOA · SOW/EOD · BC.
     Livermore danger: last pivot high/low as PH / PL rays.

   MOVED to dedicated studies
     Clustered S/R rails → Support & Resistance (volume-confirmed).
     Wyckoff volume demand/supply boxes → Supply & Demand.

   DROPPED on D+ S&P (hit ≈ drift or the short lost money)
     Doji, hammer, engulfing, H&S, double top-as-short, flags, triangles, cup-handle,
     52-week breakdown. Those live in Candle patterns if you still want the wallpaper. */
(function (root) {
  if (root.__jhChartPatternsV2) return;
  root.__jhChartPatternsV2 = true;
  root.__jhChartPatternsV1 = true;
  var UP = "#089981", DN = "#f23645", GOLD = "#f0b429", CYAN = "#26c6da", MUTE = "#787b86";

  function dailyPlus(d) {
    if (!d || d.length < 3) return false;
    var dt = d[d.length - 1].time - d[d.length - 2].time;
    return dt >= 20 * 3600;
  }
  function swings(d, L) {
    L = L || 5;
    var hi = [], lo = [], i, j, n = d.length, h, l, okh, okl;
    for (i = L; i < n - L; i++) {
      h = d[i].high; l = d[i].low; okh = true; okl = true;
      for (j = i - L; j <= i + L; j++) {
        if (j === i) continue;
        if (d[j].high > h) okh = false;
        if (d[j].low < l) okl = false;
        if (!okh && !okl) break;
      }
      if (okh) hi.push({ i: i, t: d[i].time, px: h, vol: d[i].volume || 0, k: "h" });
      if (okl) lo.push({ i: i, t: d[i].time, px: l, vol: d[i].volume || 0, k: "l" });
    }
    var pts = hi.concat(lo).sort(function (a, b) { return a.i - b.i; });
    var alt = [], p;
    for (i = 0; i < pts.length; i++) {
      p = pts[i];
      if (!alt.length) { alt.push(p); continue; }
      if (alt[alt.length - 1].k === p.k) {
        if (p.k === "h" && p.px >= alt[alt.length - 1].px) alt[alt.length - 1] = p;
        else if (p.k === "l" && p.px <= alt[alt.length - 1].px) alt[alt.length - 1] = p;
      } else alt.push(p);
    }
    return { all: alt, hi: alt.filter(function (x) { return x.k === "h"; }), lo: alt.filter(function (x) { return x.k === "l"; }) };
  }
  function cluster(pts, tol, minN) {
    var out = [], used = [], i, j, g, p;
    for (i = 0; i < pts.length; i++) used[i] = 0;
    for (i = 0; i < pts.length; i++) {
      if (used[i]) continue;
      g = [pts[i]]; used[i] = 1;
      for (j = i + 1; j < pts.length; j++) {
        if (used[j]) continue;
        if (Math.abs(pts[j].px - pts[i].px) / pts[i].px <= tol && pts[j].i - pts[i].i <= 200) {
          g.push(pts[j]); used[j] = 1;
        }
      }
      if (g.length >= minN) {
        p = 0;
        for (j = 0; j < g.length; j++) p += g[j].px;
        out.push({
          lvl: p / g.length,
          n: g.length,
          first: g[0].i,
          last: g[g.length - 1].i,
          t0: g[0].t,
          t1: g[g.length - 1].t
        });
      }
    }
    return out;
  }

  function doubleBottoms(d, los) {
    var out = [], used = {}, a, b, i1, i2, gap, p1, p2, mid, neck, depth, k, conf, v1, v2, n = d.length;
    for (a = 0; a < los.length; a++) {
      for (b = a + 1; b < Math.min(a + 8, los.length); b++) {
        i1 = los[a].i; i2 = los[b].i; gap = i2 - i1;
        if (gap < 12 || gap > 90) continue;
        p1 = los[a].px; p2 = los[b].px; mid = (p1 + p2) / 2;
        if (Math.abs(p2 - p1) / mid > 0.025) continue;
        neck = -1e99;
        for (k = i1; k <= i2; k++) if (d[k].high > neck) neck = d[k].high;
        depth = (neck - Math.min(p1, p2)) / neck;
        if (depth < 0.04 || depth > 0.28) continue;
        v1 = los[a].vol; v2 = los[b].vol;
        if (v1 && v2 > v1 * 1.15) continue;
        conf = null;
        for (k = i2 + 1; k < Math.min(i2 + 26, n); k++) {
          if (d[k].close > neck) { conf = k; break; }
        }
        if (conf == null || used[conf]) {
          if (conf == null && n - 1 - i2 <= 25 && d[n - 1].close <= neck && neck > 0 && (neck - d[n - 1].close) / neck <= 0.025) {
            out.push({ kind: "db_form", dir: 1, i: n - 1, t: d[n - 1].time, i1: i1, i2: i2, p1: p1, p2: p2, neck: neck, t1: d[i1].time, t2: d[i2].time, height: neck - Math.min(p1, p2) });
          }
          continue;
        }
        used[conf] = 1;
        out.push({ kind: "db", dir: 1, i: conf, t: d[conf].time, i1: i1, i2: i2, p1: p1, p2: p2, neck: neck, t1: d[i1].time, t2: d[i2].time, tN: d[conf].time, height: neck - Math.min(p1, p2) });
      }
    }
    return out;
  }

  function resBreaks(d, res) {
    var out = [], used = {}, zi, z, k, n = d.length, loRng, j;
    for (zi = 0; zi < res.length; zi++) {
      z = res[zi];
      if (z.n < 3) continue;
      loRng = 1e99;
      for (j = z.first; j <= z.last; j++) if (d[j].low < loRng) loRng = d[j].low;
      for (k = z.last + 1; k < Math.min(z.last + 40, n); k++) {
        if (d[k].close > z.lvl * 1.002) {
          if (!used[k]) {
            used[k] = 1;
            out.push({ kind: "rbrk", dir: 1, i: k, t: d[k].time, lvl: z.lvl, t0: z.t0, n: z.n, depth: z.lvl - loRng, lo: loRng });
          }
          z.broke = k; z.brokeT = d[k].time;
          break;
        }
      }
    }
    return out;
  }

  function tapeMarks(d) {
    var read = root.jhTapeRead || root.__jhTapeReadRaw;
    if (!read) return [];
    var pack = read(d) || {};
    var wantLv = { BOTTOM: 1, TOP: 1, "REV-UP": 1, "REV-DN": 1 };
    var wantWy = { SC: 1, CAPIT: 1, SOS: 1, SOW: 1, BC: 1, EOA: 1, EOD: 1 };
    var src = (pack.livermore && pack.livermore.markers || []).concat(pack.wyckoff && pack.wyckoff.markers || []);
    if (!src.length && pack.markers) src = pack.markers;
    var lv = src.filter(function (m) { return m && wantLv[m.text]; });
    var wy = src.filter(function (m) { return m && wantWy[m.text]; });
    function lastN(a, n) { return a.length <= n ? a : a.slice(-n); }
    return lastN(lv, 8).concat(lastN(wy, 8));
  }

  function tgtLine(t0, t1, px, lastHigh, lab, color) {
    var hit = lastHigh >= px;
    return {
      t0: t0, t1: t1, px: px,
      color: hit ? MUTE : color,
      dash: "4 3",
      lab: hit ? lab + " HIT" : lab,
      kind: "tgt",
      hit: hit ? 1 : 0
    };
  }

  function detect(d) {
    var empty = { markers: [], zones: [], lines: [], shapes: [], note: "need D+", legend: "" };
    if (!dailyPlus(d) || d.length < 80) {
      empty.note = d && d.length >= 80 ? "Chart Patterns is D / W / M only — intraday is noise on this ruleset." : "short";
      return empty;
    }
    var sw = swings(d, 5);
    var last = d[d.length - 1];
    var res = cluster(sw.hi, 0.007, 3);
    var db = doubleBottoms(d, sw.lo);
    var brk = resBreaks(d, res);
    var mk = [], lines = [], zones = [], shapes = [], z;

    var recent = Math.max(0, d.length - 280);
    var dbDrawn = 0;
    db.slice().reverse().forEach(function (p) {
      if (p.kind === "db" && p.i >= recent && dbDrawn < 2) {
        dbDrawn++;
        mk.push({ time: p.t, position: "belowBar", color: UP, shape: "arrowUp", text: "DB" });
        shapes.push({ kind: "db", t1: p.t1, p1: p.p1, t2: p.t2, p2: p.p2, neck: p.neck, tN: p.tN, color: UP });
        lines.push({ t0: p.t1, t1: last.time, px: p.neck, color: CYAN, dash: "4 3", lab: "DB neck", kind: "neck" });
        if (p.height > 0) {
          lines.push(tgtLine(p.tN, last.time, p.neck + 0.5 * p.height, last.high, "DB 0.5×", CYAN));
          lines.push(tgtLine(p.tN, last.time, p.neck + p.height, last.high, "DB 1.0×", GOLD));
        }
      } else if (p.kind === "db_form" && p.i2 >= recent && dbDrawn < 2) {
        mk.push({ time: p.t2, position: "belowBar", color: CYAN, shape: "circle", text: "DB?" });
        shapes.push({ kind: "db", t1: p.t1, p1: p.p1, t2: p.t2, p2: p.p2, neck: p.neck, tN: last.time, color: CYAN });
        lines.push({ t0: p.t1, t1: last.time, px: p.neck, color: CYAN, dash: "4 3", lab: "DB neck", kind: "neck" });
      }
    });
    var brkDrawn = 0;
    brk.slice().reverse().forEach(function (p) {
      if (p.i >= recent && brkDrawn < 2) {
        brkDrawn++;
        mk.push({ time: p.t, position: "belowBar", color: UP, shape: "arrowUp", text: "R-BRK" });
        lines.push({ t0: p.t0, t1: p.t, px: p.lvl, color: UP, dash: "4 3", lab: "R-BRK", kind: "rbrk" });
        if (p.depth > 0 && p.depth / p.lvl < 0.45) {
          lines.push(tgtLine(p.t, last.time, p.lvl + 0.382 * p.depth, last.high, "0.38×", CYAN));
          lines.push(tgtLine(p.t, last.time, p.lvl + p.depth, last.high, "1.0× book", GOLD));
        }
      }
    });

    /* Livermore danger points — last pivot high / low */
    if (sw.hi.length) {
      z = sw.hi[sw.hi.length - 1];
      lines.push({ t0: z.t, t1: last.time, px: z.px, color: DN, dash: "2 4", lab: "PH", kind: "ph" });
    }
    if (sw.lo.length) {
      z = sw.lo[sw.lo.length - 1];
      lines.push({ t0: z.t, t1: last.time, px: z.px, color: UP, dash: "2 4", lab: "PL", kind: "pl" });
    }

    mk = mk.concat(tapeMarks(d));

    var bits = [];
    if (db.some(function (p) { return p.kind === "db" && p.i >= recent; })) bits.push("DB");
    if (brk.some(function (p) { return p.i >= recent; })) bits.push("R-BRK");
    if (lines.some(function (x) { return x.kind === "tgt" && !x.hit; })) bits.push("tgt");
    var near = last.close;

    return {
      markers: mk,
      zones: zones,
      lines: lines,
      shapes: shapes,
      note: bits.join(" · ") || "no active structure",
      legend: bits.join(" · "),
      legendPts: [{ time: last.time, value: near }]
    };
  }

  root.jhChartPatterns = detect;
  if (typeof module !== "undefined" && module.exports) module.exports = { detect: detect, swings: swings, dailyPlus: dailyPlus };
})(typeof window !== "undefined" ? window : globalThis);
