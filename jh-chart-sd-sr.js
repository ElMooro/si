/* jh-reskin-skip */
/* JustHodl Supply & Demand (Wyckoff volume) + Support & Resistance.
   D+ only. Calibrated on SPY 1993–now (real ETF volume) and ^GSPC 1980–now.
   Confirmation-only, next-open 10-session vs unconditional drift.

   KEEP — Supply & Demand (Wyckoff volume)
     Demand = selling climax / stopping volume (RVOL ≥ 1.55, wide, down) →
       3–18 bar base → SOS (close above climax high, RVOL ≥ 1.15, SOS vol ≥ 0.85× climax).
     First TEST of that zone on LIGHTER volume that HOLDS:
       SPY n=22  10d hit 72.7%  mean +1.01%  edge +0.63%  t=2.2.  Hold 92%.
     Heavy-volume retest of demand is NOT a buy (SPY n=17 hit 52.9% mean −0.10%).
     Cause-and-effect 1.0× (impulse copied above the creek): SPY n=22 hit 68% in ~16 bars.
     Supply boxes are a MAP (first touch rarely smashes). 10d short from supply lost to S&P drift
       — no short target is drawn.

   KEEP — Support & Resistance (volume-confirmed)
     Clustered 3+ swings within 0.7% (next-visit reaction 54–76% vs ~33–55% random).
     Light-volume bounce at clustered support: GSPC n=12 hit 91.7% mean +3.66% edge +3.24% t=3.5.
     3-touch resistance BREAK is owned by Chart Patterns (SPY n=39 hit 79.5% / GSPC n=75 hit 76.0%).
     This study draws the rails and tags last-visit volume: vol↓ hold vs vol↑ break.

   DROPPED
     Loosened (percentile) Wyckoff demand — edge vanished (SPY light-test hit 48.6%).
     Supply-as-short, resistance-reject-as-short, 1.0× R-BRK range target (hit 23–38%). */
(function (root) {
  if (root.__jhSdSrV1) return;
  root.__jhSdSrV1 = true;
  var UP = "#089981", DN = "#f23645", GOLD = "#f0b429", CYAN = "#26c6da", MUTE = "#787b86";

  function atrAt(d, i, n) {
    n = n || 14;
    var s = 0, c = 0, j, tr;
    for (j = Math.max(1, i - n); j < i; j++) {
      tr = Math.max(d[j].high - d[j].low, Math.abs(d[j].high - d[j - 1].close), Math.abs(d[j].low - d[j - 1].close));
      s += tr; c++;
    }
    return c ? s / c : (d[i] && d[i].close ? d[i].close * 0.01 : 1);
  }
  function rvol(d, i, n) {
    n = n || 20;
    var s = 0, c = 0, j;
    for (j = Math.max(0, i - n); j < i; j++) { s += d[j].volume || 0; c++; }
    var avg = c ? s / c : 0, v = d[i].volume || 0;
    return avg ? v / avg : 0;
  }
  function loc(b) {
    var rng = b.high - b.low || 1e-12;
    return (b.close - b.low) / rng;
  }
  function dailyPlus(d) {
    if (!d || d.length < 3) return false;
    return d[d.length - 1].time - d[d.length - 2].time >= 20 * 3600;
  }
  function empty(note) {
    return { markers: [], zones: [], lines: [], shapes: [], note: note || "need D+", legend: "", legendPts: [] };
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
    return { hi: hi, lo: lo };
  }
  function cluster(pts, tol, minN, d, span) {
    span = span || 220;
    var out = [], used = [], i, j, g, wsum, pv, w, vol, age, rv, nBars;
    nBars = d && d.length ? d.length : 1;
    for (i = 0; i < pts.length; i++) used[i] = 0;
    for (i = 0; i < pts.length; i++) {
      if (used[i]) continue;
      g = [pts[i]]; used[i] = 1;
      for (j = i + 1; j < pts.length; j++) {
        if (used[j]) continue;
        if (Math.abs(pts[j].px - pts[i].px) / (pts[i].px || 1) <= tol && pts[j].i - pts[i].i <= span) {
          g.push(pts[j]); used[j] = 1;
        }
      }
      if (g.length >= minN) {
        wsum = 0; pv = 0; rv = 0;
        for (j = 0; j < g.length; j++) {
          vol = g[j].vol || 0;
          w = 1 + (vol > 0 ? Math.min(1.5, Math.log(1 + vol) / 10) : 0);
          age = (nBars - 1 - g[j].i) / nBars;
          w *= (1.15 - 0.55 * age);
          if (d) rv += rvol(d, g[j].i);
          wsum += w; pv += g[j].px * w;
        }
        out.push({
          lvl: wsum ? pv / wsum : g[0].px, n: g.length, first: g[0].i, last: g[g.length - 1].i,
          t0: g[0].t, t1: g[g.length - 1].t,
          score: g.length * (0.7 + Math.min((rv / g.length) || 1, 2.2) * 0.35)
        });
      }
    }
    return out;
  }

  function wyckoffDemand(d) {
    var n = d.length, zones = [], i = 30, A, b, rng, wide, down, climax, stop, lo, hi, j, blo, bhi, born, rv;
    while (i < n - 20) {
      rv = rvol(d, i); A = atrAt(d, i); b = d[i]; rng = b.high - b.low;
      wide = A && rng >= 1.25 * A;
      down = b.close < b.open || b.close < d[i - 1].close;
      climax = rv >= 1.55 && wide && down && loc(b) <= 0.42;
      stop = rv >= 1.50 && down && loc(b) >= 0.55;
      if (!(climax || stop)) { i++; continue; }
      lo = b.low; hi = b.high; born = null;
      for (j = i + 3; j < Math.min(i + 19, n - 5); j++) {
        blo = 1e99; bhi = -1e99;
        var k;
        for (k = i; k <= j; k++) { if (d[k].low < blo) blo = d[k].low; if (d[k].high > bhi) bhi = d[k].high; }
        if ((bhi - blo) / b.close > 0.09) break;
        lo = Math.min(lo, blo);
        if (d[j].close > b.high && rvol(d, j) >= 1.15 && loc(d[j]) >= 0.58 && (d[j].volume || 0) >= 0.85 * (b.volume || 1)) {
          var creek = Math.min(bhi, blo + 2.2 * (A || (b.close * 0.01)));
          if (creek <= lo) creek = lo + 0.5 * (A || (b.close * 0.01));
          born = j;
          zones.push({
            kind: "dem", i0: i, j: j, z0: lo, z1: creek,
            impulse: d[j].close - lo, t0: d[i].time, tBorn: d[j].time,
            climaxRvol: rv, sosRvol: rvol(d, j)
          });
          break;
        }
      }
      i = born != null ? born + 5 : i + 1;
    }
    return zones;
  }

  function wyckoffSupply(d) {
    var n = d.length, zones = [], i = 30, A, b, rng, wide, up, rv, lo, hi, j, blo, bhi, born, k;
    while (i < n - 20) {
      rv = rvol(d, i); A = atrAt(d, i); b = d[i]; rng = b.high - b.low;
      wide = A && rng >= 1.25 * A;
      up = b.close > b.open || b.close > d[i - 1].close;
      if (!(rv >= 1.55 && wide && up && loc(b) >= 0.58)) { i++; continue; }
      lo = b.low; hi = b.high; born = null;
      for (j = i + 3; j < Math.min(i + 19, n - 5); j++) {
        blo = 1e99; bhi = -1e99;
        for (k = i; k <= j; k++) { if (d[k].low < blo) blo = d[k].low; if (d[k].high > bhi) bhi = d[k].high; }
        if ((bhi - blo) / b.close > 0.09) break;
        hi = Math.max(hi, bhi);
        if (d[j].close < b.low && rvol(d, j) >= 1.15 && loc(d[j]) <= 0.42 && (d[j].volume || 0) >= 0.85 * (b.volume || 1)) {
          var ice = Math.max(blo, bhi - 2.2 * (A || (b.close * 0.01)));
          if (ice >= hi) ice = hi - 0.5 * (A || (b.close * 0.01));
          born = j;
          zones.push({
            kind: "sup", i0: i, j: j, z0: ice, z1: hi,
            impulse: hi - d[j].close, t0: d[i].time, tBorn: d[j].time,
            climaxRvol: rv, sosRvol: rvol(d, j)
          });
          break;
        }
      }
      i = born != null ? born + 5 : i + 1;
    }
    return zones;
  }

  function firstTest(d, z, bull, horizon) {
    var n = d.length, k, rv, light;
    horizon = horizon || 80;
    for (k = z.j + 3; k < Math.min(z.j + horizon, n); k++) {
      rv = rvol(d, k);
      light = rv <= z.climaxRvol * 0.85 || rv <= 1.15;
      if (bull) {
        if (d[k].low <= z.z1) {
          return {
            k: k, t: d[k].time, held: d[k].close >= z.z0,
            spring: d[k].low < z.z0 && d[k].close >= z.z0,
            light: light, heavy: rv >= 1.40, rv: rv
          };
        }
      } else if (d[k].high >= z.z0) {
        return {
          k: k, t: d[k].time, held: d[k].close <= z.z1,
          spring: false, light: light, heavy: rv >= 1.40, rv: rv
        };
      }
    }
    return null;
  }

  function mitigate(d, zones, bull) {
    var z, k, last = d.length - 1;
    for (z = 0; z < zones.length; z++) {
      zones[z].fresh = 1; zones[z].t1 = d[last].time; zones[z].i1 = last;
      zones[z].test = firstTest(d, zones[z], bull);
      for (k = zones[z].j + 1; k < d.length; k++) {
        if (bull) {
          if (d[k].close < zones[z].z0) { zones[z].fresh = 0; zones[z].t1 = d[k].time; zones[z].i1 = k; break; }
        } else {
          if (d[k].close > zones[z].z1) { zones[z].fresh = 0; zones[z].t1 = d[k].time; zones[z].i1 = k; break; }
        }
      }
    }
  }

  function supplyDemand(d) {
    if (!dailyPlus(d) || d.length < 80) {
      return empty(d && d.length >= 80 ? "Supply & Demand is D / W / M only." : "short");
    }
    var last = d[d.length - 1];
    var dem = wyckoffDemand(d);
    var sup = wyckoffSupply(d);
    mitigate(d, dem, 1); mitigate(d, sup, 0);
    var mk = [], zones = [], lines = [], i, z, t, tgt, hit;

    function near(px, band) { return Math.abs(px - last.close) / last.close <= (band || 0.22); }

    function take(arr, bull, col, lab) {
      var fresh = arr.filter(function (x) { return x.fresh; });
      fresh.sort(function (p, q) { return q.j - p.j; });
      var a = fresh.filter(function (x) {
        return near((x.z0 + x.z1) / 2, 0.40) || (d.length - 1 - x.j) < 400;
      }).slice(0, 3);
      a.forEach(function (x) {
        zones.push({
          t0: x.t0, t1: last.time, lo: x.z0, hi: x.z1, color: col,
          lab: lab, kind: bull ? "dem" : "supz"
        });
        /* creek / ice */
        lines.push({
          t0: x.t0, t1: last.time, px: bull ? x.z1 : x.z0,
          color: bull ? UP : DN, dash: "3 3",
          lab: bull ? "creek" : "ice", kind: "creek"
        });
        t = x.test;
        if (bull) {
          /* cause-and-effect 1.0× — calibrated 68% hit on light held tests */
          tgt = x.z1 + (x.z1 - x.z0);
          hit = last.high >= tgt;
          if (!hit || (d.length - 1 - x.j) < 90) {
            lines.push({
              t0: x.tBorn, t1: last.time, px: tgt,
              color: hit ? MUTE : GOLD, dash: "4 3",
              lab: (hit ? "1.0× HIT" : "1.0× C/E"), kind: "tgt", hit: hit ? 1 : 0
            });
          }
        }
        if (t && (d.length - 1 - t.k) < 120) {
          if (bull && t.spring && t.light && t.held) {
            mk.push({ time: t.t, position: "belowBar", color: GOLD, shape: "arrowUp", text: "SPRING" });
          } else if (bull && t.light && t.held) {
            mk.push({ time: t.t, position: "belowBar", color: UP, shape: "circle", text: "TEST" });
          } else if (bull && t.heavy) {
            mk.push({ time: t.t, position: "aboveBar", color: DN, shape: "circle", text: "UT" });
          }
        }
      });
      return a.length;
    }

    var nd = take(dem, 1, "rgba(8,153,129,.16)", "DEMAND");
    var ns = take(sup, 0, "rgba(242,54,69,.14)", "SUPPLY");
    var bits = [];
    if (nd) bits.push(nd + "D");
    if (ns) bits.push(ns + "Sply");
    var tests = mk.filter(function (m) { return m.text === "TEST" || m.text === "SPRING"; }).length;
    if (tests) bits.push(tests === 1 ? "TEST" : tests + " TEST");
    var nearPx = nd || ns ? (zones[0].hi + zones[0].lo) / 2 : last.close;
    return {
      markers: mk, zones: zones, lines: lines, shapes: [],
      note: bits.join(" · ") || "no fresh Wyckoff zone",
      legend: bits.join(" · "),
      legendPts: [{ time: last.time, value: nearPx }]
    };
  }

  function lastVisit(d, lvl, after, kind) {
    var n = d.length, k, rv;
    for (k = after + 1; k < n; k++) {
      rv = rvol(d, k);
      if (kind === "res") {
        if (d[k].high >= lvl * 0.997) {
          return {
            k: k, t: d[k].time,
            thru: d[k].close > lvl * 1.002,
            rev: d[k].close <= lvl && d[k].close < d[k].open,
            light: rv <= 1.10, heavy: rv >= 1.25, rv: rv
          };
        }
      } else if (d[k].low <= lvl * 1.003) {
        return {
          k: k, t: d[k].time,
          thru: d[k].close < lvl * 0.998,
          rev: d[k].close >= lvl && d[k].close > d[k].open,
          light: rv <= 1.10, heavy: rv >= 1.25, rv: rv
        };
      }
    }
    return null;
  }

  function bucketKey(ts, mode) {
    var dt = new Date(ts * 1000);
    if (mode === "week") {
      var day = dt.getUTCDay();
      var monday = new Date(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate()));
      monday.setUTCDate(monday.getUTCDate() - ((day + 6) % 7));
      return monday.toISOString().slice(0, 10);
    }
    if (mode === "month") return dt.getUTCFullYear() + "-" + dt.getUTCMonth();
    return dt.toISOString().slice(0, 10);
  }
  function priorBucket(d, mode) {
    var map = {}, order = [], i, k, b;
    for (i = 0; i < d.length; i++) {
      k = bucketKey(d[i].time, mode);
      b = map[k];
      if (!b) { b = { hi: -1e99, lo: 1e99, close: d[i].close, t: d[i].time }; map[k] = b; order.push(k); }
      if (d[i].high > b.hi) b.hi = d[i].high;
      if (d[i].low < b.lo) b.lo = d[i].low;
      b.close = d[i].close;
      b.t = d[i].time;
    }
    if (order.length < 2) return null;
    return map[order[order.length - 2]];
  }
  function supportResistance(d) {
    if (!d || d.length < 40) return empty("short");
    var intra = !dailyPlus(d);
    var L = intra ? 3 : 5;
    var minN = 3;
    var sw = swings(d, L);
    var last = d[d.length - 1];
    var atr = atrAt(d, d.length - 1, 14);
    var tol = Math.min(0.012, Math.max(intra ? 0.0026 : 0.0034, (atr / (last.close || 1)) * (intra ? 0.18 : 0.28)));
    var span = intra ? 140 : 240;
    var res = cluster(sw.hi, tol, minN, d, span);
    var sup = cluster(sw.lo, tol, minN, d, span);
    if (res.length + sup.length < 2 && d.length >= 50) {
      res = cluster(sw.hi, tol * 1.35, 2, d, span);
      sup = cluster(sw.lo, tol * 1.35, 2, d, span);
    }
    var mk = [], lines = [], i, z, v, nr = 0, ns = 0, loRng, hiRng, depth, tgt, hit;

    var bandR = intra ? 0.07 : 0.16, bandS = intra ? 0.08 : 0.18;
    function nearPx(px, band) { return Math.abs(px - last.close) / (last.close || 1) <= (band || 0.16); }
    function byRank(a, b) {
      var da = Math.abs(a.lvl - last.close) / (last.close || 1);
      var db = Math.abs(b.lvl - last.close) / (last.close || 1);
      return (da - (a.score || 0) * 0.008) - (db - (b.score || 0) * 0.008);
    }
    res.sort(byRank);
    sup.sort(byRank);

    for (i = 0; i < res.length && nr < 5; i++) {
      z = res[i];
      v = lastVisit(d, z.lvl, z.last, "res");
      if (v && v.thru) z.broke = v.k;
      if (!nearPx(z.lvl, bandR) && !(z.broke != null && (d.length - 1 - z.broke) < 30)) continue;
      if (z.broke && z.broke < d.length - 80) continue;
      var lab = "R×" + z.n;
      if (v && v.thru && v.heavy) lab += " vol↑";
      else if (v && v.rev && v.light) lab += " vol↓";
      lines.push({
        t0: z.t0, t1: z.broke != null ? d[z.broke].time : last.time, px: z.lvl,
        color: z.broke ? MUTE : DN, dash: z.broke ? "5 4" : "0",
        lab: lab, kind: "res"
      });
      nr++;
    }

    for (i = 0; i < sup.length && ns < 5; i++) {
      z = sup[i];
      if (!nearPx(z.lvl, bandS)) continue;
      v = lastVisit(d, z.lvl, z.last, "sup");
      lab = "S×" + z.n;
      if (v && v.thru && v.heavy) lab += " vol↑";
      else if (v && v.rev && v.light) lab += " vol↓";
      lines.push({
        t0: z.t0, t1: last.time, px: z.lvl,
        color: v && v.thru ? MUTE : UP, dash: v && v.thru ? "5 4" : "0",
        lab: lab, kind: "sup"
      });
      /* light-volume bounce — GSPC n=12 hit 91.7% / +3.66%. Target 0.5× range (1.0× failed). */
      if (v && v.rev && v.light && !v.thru && (d.length - 1 - v.k) < 80) {
        mk.push({ time: v.t, position: "belowBar", color: UP, shape: "arrowUp", text: "S-B" });
        hiRng = -1e99;
        for (var k = z.first; k <= z.last; k++) if (d[k].high > hiRng) hiRng = d[k].high;
        depth = hiRng - z.lvl;
        if (depth > 0 && depth / z.lvl < 0.35) {
          tgt = z.lvl + 0.5 * depth;
          hit = last.high >= tgt;
          lines.push({
            t0: v.t, t1: last.time, px: tgt,
            color: hit ? MUTE : CYAN, dash: "4 3",
            lab: hit ? "0.5× HIT" : "0.5×", kind: "tgt", hit: hit ? 1 : 0
          });
        }
      }
      ns++;
    }

    function shelfNear(px) {
      var i, z;
      for (i = 0; i < lines.length; i++) {
        z = lines[i];
        if (Math.abs(z.px - px) / (px || 1) <= tol) return true;
      }
      return false;
    }
    function addRef(bkt, pre) {
      if (!bkt) return;
      var refs = [["H", bkt.hi], ["L", bkt.lo], ["C", bkt.close]];
      var i, px;
      for (i = 0; i < refs.length; i++) {
        px = refs[i][1];
        if (!isFinite(px) || px <= 0) continue;
        if (!nearPx(px, intra ? 0.1 : 0.22)) continue;
        if (Math.abs(px - last.close) < atr * 0.12) continue;
        if (shelfNear(px)) continue;
        lines.push({
          t0: bkt.t, t1: last.time, px: px,
          color: GOLD, dash: "3 3", lab: pre + refs[i][0], kind: refs[i][0] === "H" ? "res" : "sup"
        });
        if (vtimes.length < 10) vtimes.push(bkt.t);
      }
    }
    var vtimes = [];
    lines.forEach(function (z) { if (z.t1 && vtimes.length < 10 && z.kind !== "tgt") vtimes.push(z.t1); });
    addRef(priorBucket(d, "day"), intra ? "PD" : "PD");
    addRef(priorBucket(d, "week"), "PW");
    if (!intra) addRef(priorBucket(d, "month"), "PM");
    var bits = [];
    if (nr) bits.push(nr + "R");
    if (ns) bits.push(ns + "S");
    if (mk.length) bits.push("S-B");
    var refsN = lines.filter(function (z) { return /^(PD|PW|PM)/.test(z.lab || ""); }).length;
    if (refsN) bits.push(refsN + " ref");
    return {
      markers: mk, zones: [], lines: lines, shapes: [],
      note: bits.join(" · ") || "no clustered S/R",
      legend: bits.join(" · "),
      legendPts: [{ time: last.time, value: res[0] ? res[0].lvl : (sup[0] ? sup[0].lvl : last.close) }],
      vtimes: vtimes
    };
  }
  function oscRails(pts, opt) {
    opt = opt || {};
    var tol = opt.tol != null ? opt.tol : 2.5;
    var minN = opt.minN || 2;
    var lines = [], i, j, k, n = pts ? pts.length : 0;
    if (n < 24) return { lines: [] };
    var piv = [];
    for (i = 3; i < n - 3; i++) {
      var v = pts[i].value;
      if (v == null || !isFinite(v)) continue;
      var hi = true, lo = true;
      for (j = i - 3; j <= i + 3; j++) {
        if (j === i || pts[j].value == null) continue;
        if (pts[j].value > v) hi = false;
        if (pts[j].value < v) lo = false;
      }
      if (hi) piv.push({ i: i, t: pts[i].time, v: v, k: "res" });
      else if (lo) piv.push({ i: i, t: pts[i].time, v: v, k: "sup" });
    }
    var used = [];
    for (i = 0; i < piv.length; i++) used[i] = 0;
    var groups = [];
    for (i = 0; i < piv.length; i++) {
      if (used[i]) continue;
      var g = [piv[i]]; used[i] = 1;
      for (j = i + 1; j < piv.length; j++) {
        if (used[j]) continue;
        if (piv[j].k !== piv[i].k) continue;
        if (Math.abs(piv[j].v - piv[i].v) <= tol && piv[j].i - piv[i].i <= 180) { g.push(piv[j]); used[j] = 1; }
      }
      if (g.length >= minN) {
        var s = 0;
        for (k = 0; k < g.length; k++) s += g[k].v;
        groups.push({ value: s / g.length, n: g.length, kind: g[0].k, t0: g[0].t, time: g[g.length - 1].t, score: g.length });
      }
    }
    var lastV = null;
    for (i = n - 1; i >= 0; i--) if (pts[i].value != null && isFinite(pts[i].value)) { lastV = pts[i].value; break; }
    groups.sort(function (a, b) { return b.score - a.score; });
    var ns = 0, nr = 0;
    for (i = 0; i < groups.length; i++) {
      var g2 = groups[i];
      if (lastV != null && g2.kind === "sup" && g2.value > lastV + tol) continue;
      if (lastV != null && g2.kind === "res" && g2.value < lastV - tol) continue;
      if (g2.kind === "sup" && ns >= 3) continue;
      if (g2.kind === "res" && nr >= 3) continue;
      if (g2.kind === "sup") ns++; else nr++;
      lines.push(g2);
    }
    return { lines: lines };
  }

  root.jhSupplyDemand = supplyDemand;
  root.jhSupportResistance = supportResistance;
  root.jhOscRails = oscRails;
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { supplyDemand: supplyDemand, supportResistance: supportResistance, oscRails: oscRails, dailyPlus: dailyPlus };
  }
})(typeof window !== "undefined" ? window : globalThis);
