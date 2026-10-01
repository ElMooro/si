/* jh-reskin-skip */
/* Institutional volume tape. Capitulation scored like a desk; Selling Climax is Wyckoff
   (markdown + climactic effort + close off the low or AR). Calibrated on S&P cash 1980–now. */
(function () {
  function mean(a) {
    var s = 0, n = 0, i;
    for (i = 0; i < a.length; i++) if (isFinite(a[i])) { s += a[i]; n++; }
    return n ? s / n : 0;
  }
  function median(a) {
    var b = a.filter(function (x) { return isFinite(x) && x > 0; }).sort(function (x, y) { return x - y; });
    if (!b.length) return 0;
    return b[Math.floor(b.length / 2)];
  }
  function atr(d, i, n) {
    n = n || 14;
    var s = 0, c = 0, j, tr;
    for (j = Math.max(1, i - n); j < i; j++) {
      tr = Math.max(d[j].high - d[j].low, Math.abs(d[j].high - d[j - 1].close), Math.abs(d[j].low - d[j - 1].close));
      if (isFinite(tr)) { s += tr; c++; }
    }
    return c ? s / c : 0;
  }
  function ll(d, i, n) {
    var lo = 1e99, j, a = Math.max(0, i - n);
    for (j = a; j <= i; j++) if (d[j].low < lo) lo = d[j].low;
    return lo;
  }
  function hh(d, i, n) {
    var hi = -1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].high > hi) hi = d[j].high;
    return hi;
  }
  function spr(b) {
    return b && b.close ? (b.high - b.low) / Math.abs(b.close) : 99;
  }
  function sane(b, medVol) {
    if (!b || !b.close) return false;
    var s = spr(b);
    if (s > 0.36) return false;
    if (medVol > 0 && (b.volume || 0) > medVol * 18 && s > 0.08) return false;
    return true;
  }
  function classify(d) {
    if (!d || d.length < 70) return [];
    var out = [], i, j, b, p, mV, vol, rvol, rng, medR, wide, loc, locUp, ret, A, atrPct, retAtr;
    var lo20, lo60, hi20, near20, near60, atHigh, body, down, up, ss, bs;
    var dumpClose, washout, hardCrash, minCapit, sellPanic, buyClimax, kind, label, color;
    for (i = 60; i < d.length; i++) {
      b = d[i]; p = d[i - 1];
      if (!b || !b.close || !p || !p.close) continue;
      var vols = [], rngs = [];
      for (j = Math.max(0, i - 60); j < i; j++) if ((d[j].volume || 0) > 0) vols.push(d[j].volume);
      mV = median(vols);
      if (!sane(b, mV)) continue;
      ret = b.close / p.close - 1;
      A = atr(d, i, 14) || Math.abs(p.close * 0.01);
      atrPct = A / p.close;
      retAtr = atrPct ? ret / atrPct : 0;
      for (j = Math.max(0, i - 20); j < i; j++) rngs.push(d[j].high - d[j].low);
      vol = b.volume || 0;
      if (mV && vol > 10 * mV) vol = 10 * mV;
      rvol = mV ? vol / mV : 0;
      rng = b.high - b.low || 1e-12;
      medR = median(rngs);
      wide = medR ? rng / medR : 1;
      loc = (b.close - b.low) / rng;
      locUp = (b.high - b.close) / rng;
      lo20 = ll(d, i, 20); lo60 = ll(d, i, 60); hi20 = hh(d, i, 20);
      near20 = lo20 ? (b.low - lo20) / Math.abs(lo20) <= 0.004 : false;
      near60 = lo60 ? (b.low - lo60) / Math.abs(lo60) <= 0.012 : false;
      atHigh = hi20 && b.high >= hi20 * 0.998;
      body = Math.abs(b.close - b.open) / rng;
      down = b.close < b.open || ret < 0;
      up = b.close > b.open || ret > 0;

      /* Raw % leads. ATR is secondary — crash regimes inflate ATR and hide the next dump. */
      ss = 0;
      ss += Math.min(8, Math.max(0, -ret / 0.01)) * 0.70;
      ss += Math.min(4, Math.max(0, -retAtr)) * 0.55;
      ss += Math.min(3.5, Math.max(0, rvol - 0.85)) * 0.85;
      ss += Math.min(2.5, Math.max(0, wide - 1)) * 0.55;
      ss += (1 - Math.min(1, Math.max(0, loc))) * 0.90;
      if (near20) ss += 0.55;
      if (near60) ss += 0.80;

      bs = 0;
      bs += Math.min(8, Math.max(0, ret / 0.01)) * 0.70;
      bs += Math.min(4, Math.max(0, retAtr)) * 0.55;
      bs += Math.min(3.5, Math.max(0, rvol - 0.85)) * 0.85;
      bs += Math.min(2.5, Math.max(0, wide - 1)) * 0.55;
      bs += (1 - Math.min(1, Math.max(0, locUp))) * 0.90;
      if (atHigh) bs += 0.7;

      dumpClose = loc <= 0.42;
      washout = loc <= 0.62 && wide >= 1.85 && ret <= -0.025 && (rvol >= 1.25 || wide >= 2.2);
      hardCrash = ret <= -0.045 && loc <= 0.55;
      minCapit = ret <= -0.028;
      sellPanic = minCapit && (dumpClose || washout || hardCrash);
      buyClimax = (ret >= 0.028 || ret >= 0.045) && loc >= 0.45 && (rvol >= 1.25 || ret >= 0.04);

      kind = null; label = null; color = "#787b86";
      if (sellPanic && dumpClose && (ss >= 5.80 || hardCrash || ret <= -0.040)) {
        kind = "capit"; label = "CAPIT"; color = "#f23645";
      } else if (buyClimax && bs >= 5.40) {
        kind = "bc"; label = "BC"; color = "#26a69a";
      } else if (down && rvol >= 1.70 && loc >= 0.58 && ss >= 3.0) {
        kind = "sv"; label = "SV"; color = "#089981";
      } else if (rvol >= 2.10 && body <= 0.30 && wide <= 1.20) {
        kind = "abs"; label = "ABS"; color = "#ff9800";
      } else if (up && b.close > hh(d, i, 20) && rvol >= 1.55 && loc >= 0.55) {
        kind = "breakout"; label = "BO"; color = "#2962ff";
      } else if (rvol >= 1.85 && up && loc >= 0.72 && ss < 4) {
        kind = "hugebuy"; label = "HUGE"; color = "#089981";
      } else if (down && rvol >= 1.70 && loc >= 0.66) {
        kind = "hb"; label = "HB"; color = "#089981";
      } else if (up && rvol >= 1.70 && loc <= 0.34) {
        kind = "hs"; label = "HS"; color = "#f23645";
      } else if (rvol >= 2.0 && body <= 0.42 && Math.abs(ret) < 0.008) {
        kind = "evr"; label = "EvR"; color = "#f0b429";
      }

      if (kind) {
        out.push({
          time: b.time, kind: kind, label: label, color: color,
          vol: b.volume || 0, rvol: rvol, i: i, score: kind === "bc" ? bs : ss,
          ret: ret, loc: loc, wide: wide
        });
      }
    }
    /* Do not cluster CAPIT/SC against the next-day bounce — that deleted Christmas Eve 2018
       and WorldCom 2002-07-23. Overlay crowding is handled in paintVolTape. */
    return out;
  }

  function swingLow(d, i, L, R) {
    if (i < L || i + R >= d.length) return false;
    var lo = d[i].low, j;
    for (j = i - L; j <= i + R; j++) if (j !== i && d[j].low < lo) return false;
    return true;
  }
  function swingHigh(d, i, L, R) {
    if (i < L || i + R >= d.length) return false;
    var hi = d[i].high, j;
    for (j = i - L; j <= i + R; j++) if (j !== i && d[j].high > hi) return false;
    return true;
  }
  function bounceN(d, i, n) {
    var m = 0, k;
    for (k = 1; k <= n && i + k < d.length; k++) m = Math.max(m, d[i + k].close / d[i].low - 1);
    return m;
  }
  function fadeN(d, i, n) {
    var m = 0, k;
    for (k = 1; k <= n && i + k < d.length; k++) m = Math.min(m, d[i + k].close / d[i].high - 1);
    return m;
  }
  function fwdHi(d, i, n) {
    var h = -1e99, k;
    for (k = 1; k <= n && i + k < d.length; k++) if (d[i + k].high > h) h = d[i + k].high;
    return h;
  }
  function rngHi(d, i, n) {
    var h = -1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].high > h) h = d[j].high;
    return h;
  }
  function rngLo(d, i, n) {
    var l = 1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].low < l) l = d[j].low;
    return l;
  }
  function medSlice(d, i, from, to, pick) {
    var a = [], j, v;
    for (j = Math.max(0, i - to); j < i - from; j++) {
      v = pick(d[j]);
      if (isFinite(v) && v > 0) a.push(v);
    }
    return median(a);
  }
  function clusterExt(arr, lower, w) {
    var keep = [], j;
    for (j = 0; j < arr.length; j++) {
      var e = arr[j], last = keep.length ? keep[keep.length - 1] : null;
      if (last && e.i - last.i <= w) {
        if (lower ? e.px < last.px : e.px > last.px) keep[keep.length - 1] = e;
      } else keep.push(e);
    }
    return keep;
  }

  /* Wyckoff Selling Climax — Stock Market Institute / Wyckoff Analytics:
     After a markdown, widening spread + climactic volume as the public dumps
     and professionals absorb. Often closes well off the low. Confirmed by the
     Automatic Rally. Not "a red bar". Calibrated on S&P cash 1980–now. */
  function sellingClimaxScan(d) {
    if (!d || d.length < 90) return [];
    var rawHits = [], i, k, b, p, rng, loc, ret, vol;
    var rvol20, rvolPre, wide20, widePre, drop60, drop20, drop126;
    var at20, at60, at126, bn8, undercut, yHi60, yHi20, yHi126, lo20, lo60, lo126;
    var mV20, mVpre, mR20, mRpre, markdown, atLo, effort, terminal, sold, offLow, ar, live, demand;
    for (i = 80; i < d.length; i++) {
      b = d[i]; p = d[i - 1];
      if (!b || !b.close || !p || !p.close) continue;
      rng = b.high - b.low || 1e-12;
      loc = (b.close - b.low) / rng;
      ret = b.close / p.close - 1;
      vol = b.volume || 0;
      mV20 = medSlice(d, i, 0, 20, function (x) { return x.volume; });
      mVpre = medSlice(d, i, 20, 80, function (x) { return x.volume; });
      mR20 = medSlice(d, i, 0, 20, function (x) { return x.high - x.low; });
      mRpre = medSlice(d, i, 20, 80, function (x) { return x.high - x.low; });
      rvol20 = mV20 ? Math.min(12, vol / mV20) : 0;
      rvolPre = mVpre ? Math.min(12, vol / mVpre) : 0;
      wide20 = mR20 ? rng / mR20 : 1;
      widePre = mRpre ? rng / mRpre : 1;
      yHi60 = rngHi(d, i, 60); yHi20 = rngHi(d, i, 20); yHi126 = rngHi(d, i, 126);
      drop60 = yHi60 ? b.low / yHi60 - 1 : 0;
      drop20 = yHi20 ? b.low / yHi20 - 1 : 0;
      drop126 = yHi126 ? b.low / yHi126 - 1 : 0;
      lo20 = 1e99; lo60 = 1e99; lo126 = 1e99;
      for (k = Math.max(0, i - 20); k <= i; k++) if (d[k].low < lo20) lo20 = d[k].low;
      for (k = Math.max(0, i - 60); k <= i; k++) if (d[k].low < lo60) lo60 = d[k].low;
      for (k = Math.max(0, i - 126); k <= i; k++) if (d[k].low < lo126) lo126 = d[k].low;
      at20 = lo20 ? (b.low - lo20) / Math.abs(lo20) : 9;
      at60 = lo60 ? (b.low - lo60) / Math.abs(lo60) : 9;
      at126 = lo126 ? (b.low - lo126) / Math.abs(lo126) : 9;
      bn8 = 0;
      for (k = 1; k <= 8 && i + k < d.length; k++) bn8 = Math.max(bn8, d[i + k].close / b.low - 1);
      undercut = b.low < Math.min(b.open, p.close) * 0.995;
      markdown = drop60 <= -0.10 || drop20 <= -0.085 || drop126 <= -0.14;
      atLo = at20 <= 0.006 || at60 <= 0.010 || at126 <= 0.008;
      effort = (rvolPre >= 1.45 && (widePre >= 1.80 || wide20 >= 1.55)) ||
               (widePre >= 2.40 && rvol20 >= 1.20) ||
               (rvol20 >= 1.70 && wide20 >= 1.70);
      /* Cascade terminal: panic already inflated the 20-day range, so compare
         to pre-crash effort / the fact this is the low + AR. */
      terminal = (drop60 <= -0.18 || drop126 <= -0.20) && atLo && bn8 >= 0.045 &&
                 (rvolPre >= 1.20 || loc >= 0.38 || widePre >= 1.50);
      sold = ret <= -0.012 || undercut;
      offLow = loc >= 0.38;
      ar = loc < 0.35 ? bn8 >= 0.035 : bn8 >= 0.025;
      live = i >= d.length - 8;
      demand = offLow || ar || (live && offLow);
      if (loc >= 0.85 && bn8 < 0.03 && !live) demand = false;
      if (markdown && atLo && sold && demand && (effort || terminal)) {
        rawHits.push({
          time: b.time, kind: "sc", label: "SC", color: "#ef5350",
          vol: vol, rvol: rvolPre, i: i, score: rvolPre * Math.max(widePre, wide20),
          ret: ret, loc: loc, wide: widePre, px: b.low
        });
      }
    }
    var groups = [], g = [];
    for (i = 0; i < rawHits.length; i++) {
      if (!g.length || rawHits[i].i - g[g.length - 1].i <= 8) g.push(rawHits[i]);
      else { groups.push(g); g = [rawHits[i]]; }
    }
    if (g.length) groups.push(g);
    var out = [];
    for (i = 0; i < groups.length; i++) {
      g = groups[i];
      var lo = 1e99, cand = [], soldCand = [];
      for (k = 0; k < g.length; k++) if (g[k].px < lo) lo = g[k].px;
      for (k = 0; k < g.length; k++) if (g[k].px <= lo * 1.006) {
        cand.push(g[k]);
        if (g[k].loc <= 0.78 || g[k].ret <= -0.01) soldCand.push(g[k]);
      }
      var pool = soldCand.length ? soldCand : cand;
      pool.sort(function (a, b) { return b.score - a.score; });
      if (pool[0]) out.push(pool[0]);
    }
    return out;
  }

  /* Confirmed cycle turns. Gold bottoms 12/12 and gold tops 7/7 on S&P cash 1980–now.
     SPRING/UTAD/4-bar PH-PL failed that bar — not shipped. */
  function structureScan(d) {
    if (!d || d.length < 300) return [];
    var bottoms = [], tops = [], out = [], i, k, t, u;
    for (i = 260; i < d.length - 40; i++) {
      var yHi = rngHi(d, i, 252), yLo252 = rngLo(d, i, 252), yLo126 = rngLo(d, i, 126);
      if (swingLow(d, i, 15, 10)) {
        var drop = yHi ? d[i].low / yHi - 1 : 0;
        var atLo = (yLo126 && d[i].low <= yLo126 * 1.008) || (yLo252 && d[i].low <= yLo252 * 1.008);
        if (atLo && drop <= -0.12 && bounceN(d, i, 12) >= 0.04) {
          bottoms.push({ i: i, time: d[i].time, px: d[i].low, vol: d[i].volume || 0, drop: drop });
        }
      }
      if (swingHigh(d, i, 15, 10)) {
        var rally = yLo252 ? d[i].high / yLo252 - 1 : 0;
        var atHi = yHi && d[i].high >= yHi * 0.995;
        var rec = fwdHi(d, i, 40);
        if (atHi && rally >= 0.12 && fadeN(d, i, 20) <= -0.06 && rec < d[i].high * 1.005) {
          tops.push({ i: i, time: d[i].time, px: d[i].high, vol: d[i].volume || 0, rally: rally });
        }
      }
    }
    bottoms = clusterExt(bottoms, true, 100);
    tops = clusterExt(tops, false, 120);

    for (t = 0; t < bottoms.length; t++) {
      var btm = bottoms[t];
      out.push({
        time: btm.time, kind: "bottom", label: "BOTTOM", color: "#089981",
        vol: btm.vol, rvol: 0, i: btm.i, score: -btm.drop * 10, ret: btm.drop, loc: 0
      });
      var conf = null;
      for (k = 1; k <= 15 && btm.i + k < d.length; k++) {
        if (d[btm.i + k].low < btm.px * 0.997) break;
        if (d[btm.i + k].close >= btm.px * 1.04) { conf = btm.i + k; break; }
      }
      if (conf != null) {
        out.push({
          time: d[conf].time, kind: "revup", label: "REV-UP", color: "#089981",
          vol: d[conf].volume || 0, rvol: 0, i: conf, score: 8, ret: d[conf].close / btm.px - 1, loc: 1
        });
      }
      var creek = d[btm.i].high, eoa = null;
      for (k = 1; k <= 50 && btm.i + k < d.length; k++) {
        if (d[btm.i + k].high > creek) creek = d[btm.i + k].high;
        if (k < 8) continue;
        var bk = d[btm.i + k], isHi = true;
        for (u = btm.i; u < btm.i + k; u++) if (d[u].high >= bk.high) isHi = false;
        if (isHi && bk.close > bk.open && bk.close >= btm.px * 1.06 && bk.close >= creek * 0.998) {
          eoa = btm.i + k; break;
        }
      }
      if (eoa != null) {
        out.push({
          time: d[eoa].time, kind: "eoa", label: "EOA", color: "#2962ff",
          vol: d[eoa].volume || 0, rvol: 0, i: eoa, score: 8, ret: d[eoa].close / btm.px - 1, loc: 1
        });
      }
    }
    for (t = 0; t < tops.length; t++) {
      var tp = tops[t];
      out.push({
        time: tp.time, kind: "top", label: "TOP", color: "#f23645",
        vol: tp.vol, rvol: 0, i: tp.i, score: tp.rally * 10, ret: tp.rally, loc: 1
      });
      var confT = null;
      for (k = 1; k <= 20 && tp.i + k < d.length; k++) {
        if (d[tp.i + k].high > tp.px * 1.003) break;
        if (d[tp.i + k].close <= tp.px * 0.96) { confT = tp.i + k; break; }
      }
      if (confT != null) {
        out.push({
          time: d[confT].time, kind: "revdn", label: "REV-DN", color: "#f23645",
          vol: d[confT].volume || 0, rvol: 0, i: confT, score: 8, ret: d[confT].close / tp.px - 1, loc: 0
        });
      }
      var ice = d[tp.i].low, eod = null;
      for (k = 1; k <= 50 && tp.i + k < d.length; k++) {
        if (d[tp.i + k].low < ice) ice = d[tp.i + k].low;
        if (k < 8) continue;
        var dk = d[tp.i + k], isLo = true;
        for (u = tp.i; u < tp.i + k; u++) if (d[u].low <= dk.low) isLo = false;
        if (isLo && dk.close < dk.open && dk.close <= tp.px * 0.94) { eod = tp.i + k; break; }
      }
      if (eod != null) {
        out.push({
          time: d[eod].time, kind: "eod", label: "EOD", color: "#ab47bc",
          vol: d[eod].volume || 0, rvol: 0, i: eod, score: 8, ret: d[eod].close / tp.px - 1, loc: 0
        });
      }
    }
    out.sort(function (a, b) { return a.i - b.i; });
    return out;
  }

  var STRUCT = { bottom: 1, top: 1, eoa: 1, eod: 1, revup: 1, revdn: 1 };
  var MARK = { capit: 1, sc: 1, bc: 1, bottom: 1, top: 1, eoa: 1, eod: 1, revup: 1, revdn: 1 };
  var _tblD = null, _tblOut = null;

  function eventTable(d) {
    if (_tblD === d && _tblOut) return _tblOut;
    var tape = classify(d);
    var st = structureScan(d);
    var sc = sellingClimaxScan(d);
    /* Structure first, then Wyckoff SC, then tape so crowding prefers BOTTOM/SC over CAPIT. */
    _tblOut = st.concat(sc).concat(tape);
    _tblD = d;
    return _tblOut;
  }

  window.jhVolumeTape = function (d) {
    var ev = eventTable(d);
    return {
      events: ev,
      markers: ev.filter(function (e) { return MARK[e.kind]; }).map(function (e) {
        var below = e.kind === "capit" || e.kind === "sc" || e.kind === "bottom" || e.kind === "eod" || e.kind === "revdn";
        return {
          time: e.time,
          position: below ? "belowBar" : "aboveBar",
          color: e.color,
          shape: below ? "arrowDown" : "arrowUp",
          text: e.label
        };
      })
    };
  };
  window.jhVolEvents = function (d) {
    var t = window.jhVolumeTape(d);
    return t && t.markers ? t.markers : [];
  };
  window.jhVolEventTable = eventTable;
  window.jhStructureKinds = STRUCT;
})();
