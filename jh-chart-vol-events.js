/* Institutional volume tape. Capitulation scored like a desk, calibrated on S&P cash 1980–now.
   Real panics: 87 crash, 89 mini-crash, 97, LTCM, 00, 9/11, 08 cascade, flash 10, 11, 15,
   Brexit, volmageddon, COVID, 22, yen 24, tariff 25. Not "red bar + 1.75× volume". */
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
      } else if ((sellPanic && ss >= 4.00) ||
                 (ret <= -0.022 && dumpClose && (near20 || near60) && ss >= 3.40) ||
                 (ret <= -0.028 && loc <= 0.55 && (near20 || near60 || wide >= 1.8) && ss >= 3.70)) {
        kind = "sc"; label = "SC"; color = "#ef5350";
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

  window.jhVolumeTape = function (d) {
    var ev = classify(d);
    return {
      events: ev,
      markers: ev.filter(function (e) {
        return e.kind === "capit" || e.kind === "sc" || e.kind === "bc";
      }).map(function (e) {
        var below = e.kind === "capit" || e.kind === "sc";
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
  window.jhVolEventTable = classify;
})();
