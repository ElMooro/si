/* jh-reskin-skip */
/* JustHodl Chart Patterns v1 — D+ only, S&P-calibrated.
   Confirmation-only (no look-ahead). Track record: ^GSPC 1980–now + SPY 1993–now,
   next-open after confirm, 10-session horizon vs unconditional drift.

   KEPT as signals
     Double bottom (neckline close, quieter 2nd low): GSPC n=99 hit 65.7% mean +0.72%
       edge +0.30% t=3.1; SPY n=105 hit 64.8% mean +0.55%. Weekly GSPC n=26 hit 80.8%.
     3-touch resistance break: SPY n=19 hit 78.9% mean +1.09% edge +0.71% t=3.1;
       GSPC n=25 hit 64.0% mean +1.28% edge +0.87%.

   KEPT as a map (not a 10d trade)
     Clustered S/R (3+ swings within 0.7%): next-visit reaction 54–76% vs ~33–55% random.
     Fresh supply/demand boxes (impulse → base → departure): first touch rarely smashes
       the whole zone (SPY demand hold 98% / supply 95% of first tests). 10d bounce is
       NOT an edge — boxes are location, not a buy.

   KEPT as cycle events (existing gold list, 100% of S&P panics 1980–now)
     Livermore BOTTOM / TOP / REV-UP / REV-DN. Wyckoff SC/CAPIT · SOS/EOA · SOW/EOD · BC.
     Livermore danger: last pivot high/low as PH / PL rays.

   DROPPED on D+ S&P (hit ≈ drift or the short lost money)
     Doji, hammer, engulfing, H&S, double top-as-short, flags, triangles, cup-handle,
     52-week breakdown. Those live in Candle patterns if you still want the wallpaper. */
(function (root) {
  if (root.__jhChartPatternsV1) return;
  root.__jhChartPatternsV1 = true;
  var UP = "#089981", DN = "#f23645";

  function atrAt(d, i, n) {
    n = n || 14;
    var s = 0, c = 0, j, tr;
    for (j = Math.max(1, i - n); j < i; j++) {
      tr = Math.max(d[j].high - d[j].low, Math.abs(d[j].high - d[j - 1].close), Math.abs(d[j].low - d[j - 1].close));
      s += tr; c++;
    }
    return c ? s / c : (d[i] && d[i].close ? d[i].close * 0.01 : 1);
  }
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
          /* forming: 2nd low in, last bar still under neck but within 2.5% */
          if (conf == null && n - 1 - i2 <= 25 && d[n - 1].close <= neck && neck > 0 && (neck - d[n - 1].close) / neck <= 0.025) {
            out.push({ kind: "db_form", dir: 1, i: n - 1, t: d[n - 1].time, i1: i1, i2: i2, p1: p1, p2: p2, neck: neck, t1: d[i1].time, t2: d[i2].time });
          }
          continue;
        }
        used[conf] = 1;
        out.push({ kind: "db", dir: 1, i: conf, t: d[conf].time, i1: i1, i2: i2, p1: p1, p2: p2, neck: neck, t1: d[i1].time, t2: d[i2].time, tN: d[conf].time });
      }
    }
    return out;
  }

  function resBreaks(d, res) {
    var out = [], used = {}, zi, z, k, n = d.length;
    for (zi = 0; zi < res.length; zi++) {
      z = res[zi];
      if (z.n < 3) continue;
      for (k = z.last + 1; k < Math.min(z.last + 40, n); k++) {
        if (d[k].close > z.lvl * 1.002) {
          if (!used[k]) {
            used[k] = 1;
            out.push({ kind: "rbrk", dir: 1, i: k, t: d[k].time, lvl: z.lvl, t0: z.t0, n: z.n });
          }
          z.broke = k; z.brokeT = d[k].time;
          break;
        }
      }
    }
    return out;
  }

  function sdZones(d) {
    var n = d.length, demand = [], supply = [], i, j, k, A, drop, rally, blo, bhi, blen, born;
    for (i = 20; i < n - 16; i++) {
      A = atrAt(d, i, 14);
      drop = d[i].close / d[i - 8].close - 1;
      if (drop <= -0.045 || (d[i - 8].close - d[i].close) >= 2.0 * A) {
        for (blen = 3; blen <= 7; blen++) {
          j = i + blen; if (j >= n - 8) break;
          blo = 1e99; bhi = -1e99;
          for (k = i; k <= j; k++) { if (d[k].low < blo) blo = d[k].low; if (d[k].high > bhi) bhi = d[k].high; }
          if ((bhi - blo) / d[i].close > 0.022) continue;
          if (A && (bhi - blo) > 1.4 * A) continue;
          born = null;
          for (k = j + 2; k < Math.min(j + 16, n); k++) {
            if (d[k].close > d[i - 8].close) { born = k; break; }
          }
          if (born != null) {
            demand.push({ kind: "dem", z0: blo, z1: bhi, t0: d[i].time, i0: i, born: born, tBorn: d[born].time });
            i = born; break;
          }
        }
      }
      rally = d[i].close / d[i - 8].close - 1;
      if (rally >= 0.045 || (d[i].close - d[i - 8].close) >= 2.0 * A) {
        for (blen = 3; blen <= 7; blen++) {
          j = i + blen; if (j >= n - 8) break;
          blo = 1e99; bhi = -1e99;
          for (k = i; k <= j; k++) { if (d[k].low < blo) blo = d[k].low; if (d[k].high > bhi) bhi = d[k].high; }
          if ((bhi - blo) / d[i].close > 0.022) continue;
          if (A && (bhi - blo) > 1.4 * A) continue;
          born = null;
          for (k = j + 2; k < Math.min(j + 16, n); k++) {
            if (d[k].close < d[i - 8].close) { born = k; break; }
          }
          if (born != null) {
            supply.push({ kind: "sup", z0: blo, z1: bhi, t0: d[i].time, i0: i, born: born, tBorn: d[born].time });
            i = born; break;
          }
        }
      }
    }
    function mitigate(zones, bull) {
      var z, k, last = n - 1;
      for (z = 0; z < zones.length; z++) {
        zones[z].fresh = 1; zones[z].t1 = d[last].time; zones[z].i1 = last;
        for (k = zones[z].born + 1; k < n; k++) {
          if (bull) {
            if (d[k].close < zones[z].z0) { zones[z].fresh = 0; zones[z].t1 = d[k].time; zones[z].i1 = k; break; }
          } else {
            if (d[k].close > zones[z].z1) { zones[z].fresh = 0; zones[z].t1 = d[k].time; zones[z].i1 = k; break; }
          }
        }
      }
    }
    mitigate(demand, 1); mitigate(supply, 0);
    return { demand: demand, supply: supply };
  }

  function tapeMarks(d) {
    var read = root.jhTapeRead || root.__jhTapeReadRaw;
    if (!read) return [];
    var pack = read(d) || {};
    var want = { BOTTOM: 1, TOP: 1, "REV-UP": 1, "REV-DN": 1, SC: 1, CAPIT: 1, SOS: 1, SOW: 1, BC: 1, EOA: 1, EOD: 1 };
    var src = (pack.livermore && pack.livermore.markers || []).concat(pack.wyckoff && pack.wyckoff.markers || []);
    if (!src.length && pack.markers) src = pack.markers;
    return src.filter(function (m) { return m && want[m.text]; });
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
    var sup = cluster(sw.lo, 0.007, 3);
    var db = doubleBottoms(d, sw.lo);
    var brk = resBreaks(d, res);
    var sd = sdZones(d);
    var mk = [], lines = [], zones = [], shapes = [], i, z, near;

    function nearPx(px, band) { return Math.abs(px - last.close) / last.close <= (band || 0.10); }

    /* S/R rails — nearest 5 of each, skip broken unless recent */
    res.sort(function (a, b) { return Math.abs(a.lvl - last.close) - Math.abs(b.lvl - last.close); });
    sup.sort(function (a, b) { return Math.abs(a.lvl - last.close) - Math.abs(b.lvl - last.close); });
    var nr = 0, ns = 0;
    for (i = 0; i < res.length && nr < 5; i++) {
      z = res[i];
      if (!nearPx(z.lvl, 0.16) && !(z.broke != null && (d.length - 1 - z.broke) < 30)) continue;
      if (z.broke && z.broke < d.length - 40) continue;
      lines.push({ t0: z.t0, t1: z.brokeT || last.time, px: z.lvl, color: z.broke ? "#787b86" : DN, dash: z.broke ? "5 4" : "0", lab: "R×" + z.n, kind: "res" });
      nr++;
    }
    for (i = 0; i < sup.length && ns < 5; i++) {
      z = sup[i];
      if (!nearPx(z.lvl, 0.18)) continue;
      lines.push({ t0: z.t0, t1: last.time, px: z.lvl, color: UP, dash: "0", lab: "S×" + z.n, kind: "sup" });
      ns++;
    }

    /* Fresh S/D only, nearest 4 each */
    function takeFresh(arr, bull, col, lab) {
      var a = arr.filter(function (x) { return x.fresh; });
      a.sort(function (p, q) {
        var mp = Math.abs((p.z0 + p.z1) / 2 - last.close), mq = Math.abs((q.z0 + q.z1) / 2 - last.close);
        return mp - mq;
      });
      a = a.filter(function (x) { return nearPx((x.z0 + x.z1) / 2, 0.22); }).slice(0, 3);
      a.forEach(function (x) {
        zones.push({ t0: x.t0, t1: last.time, lo: x.z0, hi: x.z1, color: col, lab: lab, kind: bull ? "dem" : "supz" });
      });
    }
    takeFresh(sd.demand, 1, "rgba(8,153,129,.16)", "DEMAND");
    takeFresh(sd.supply, 0, "rgba(242,54,69,.14)", "SUPPLY");

    /* Confirmed double bottoms in the last ~1.5y of bars + forming */
    var recent = Math.max(0, d.length - 280);
    var dbDrawn = 0;
    db.slice().reverse().forEach(function (p) {
      if (p.kind === "db" && p.i >= recent && dbDrawn < 2) {
        dbDrawn++;
        mk.push({ time: p.t, position: "belowBar", color: UP, shape: "arrowUp", text: "DB" });
        shapes.push({ kind: "db", t1: p.t1, p1: p.p1, t2: p.t2, p2: p.p2, neck: p.neck, tN: p.tN, color: UP });
        lines.push({ t0: p.t1, t1: last.time, px: p.neck, color: "#26c6da", dash: "4 3", lab: "DB neck", kind: "neck" });
      } else if (p.kind === "db_form" && p.i2 >= recent && dbDrawn < 2) {
        mk.push({ time: p.t2, position: "belowBar", color: "#26c6da", shape: "circle", text: "DB?" });
        shapes.push({ kind: "db", t1: p.t1, p1: p.p1, t2: p.t2, p2: p.p2, neck: p.neck, tN: last.time, color: "#26c6da" });
        lines.push({ t0: p.t1, t1: last.time, px: p.neck, color: "#26c6da", dash: "4 3", lab: "DB neck", kind: "neck" });
      }
    });
    var brkDrawn = 0;
    brk.slice().reverse().forEach(function (p) {
      if (p.i >= recent && brkDrawn < 2) {
        brkDrawn++;
        mk.push({ time: p.t, position: "belowBar", color: UP, shape: "arrowUp", text: "R-BRK" });
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
    if (nr) bits.push(nr + "R");
    if (ns) bits.push(ns + "S");
    var nd = zones.filter(function (x) { return x.kind === "dem"; }).length;
    var nsu = zones.filter(function (x) { return x.kind === "supz"; }).length;
    if (nd) bits.push(nd + "D");
    if (nsu) bits.push(nsu + "Sply");
    near = res[0] ? res[0].lvl : (sup[0] ? sup[0].lvl : last.close);

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
