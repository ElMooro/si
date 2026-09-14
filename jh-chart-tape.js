/* Bloomberg-grade tape: Livermore pivots, full Wyckoff, VSA. Real OHLC only. */
(function () {
  function avg(a) { var s = 0, i; for (i = 0; i < a.length; i++) s += a[i]; return a.length ? s / a.length : 0; }
  function mean(d, i, n, key) {
    var a = Math.max(0, i - n), s = 0, j, c = 0;
    for (j = a; j < i; j++) { s += d[j][key] || 0; c++; }
    return c ? s / c : 0;
  }
  function rngMean(d, i, n) {
    var a = Math.max(0, i - n), s = 0, j, c = 0;
    for (j = a; j < i; j++) { s += (d[j].high - d[j].low); c++; }
    return c ? s / c : 0;
  }
  function hh(d, i, n) {
    var h = -1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].high > h) h = d[j].high;
    return h;
  }
  function ll(d, i, n) {
    var l = 1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].low < l) l = d[j].low;
    return l;
  }
  function tag(t, pos, color, shape, text) {
    return { time: t, position: pos, color: color, shape: shape, text: text };
  }
  function swings(d, n) {
    n = n || 4; var hi = [], lo = [], i, j;
    for (i = n; i < d.length - n; i++) {
      var isH = true, isL = true;
      for (j = i - n; j <= i + n; j++) {
        if (j === i) continue;
        if (d[j].high > d[i].high) isH = false;
        if (d[j].low < d[i].low) isL = false;
      }
      if (isH) hi.push({ i: i, px: d[i].high, t: d[i].time });
      if (isL) lo.push({ i: i, px: d[i].low, t: d[i].time });
    }
    return { hi: hi, lo: lo };
  }
  function metaBar(d, i) {
    var b = d[i], rng = b.high - b.low || 1e-12;
    var vAvg = mean(d, i, 20, "volume");
    return {
      rvol: vAvg ? (b.volume || 0) / vAvg : 0,
      closeLoc: (b.close - b.low) / rng,
      rng: rng,
      body: Math.abs(b.close - b.open),
      spr: b.close ? rng / Math.abs(b.close) : 0,
      up: b.close > b.open,
      down: b.close < b.open,
      vAvg: vAvg
    };
  }

  /* Jesse Livermore: confirmed pivots + break of the last pivotal point. */
  function livermore(d, sw) {
    var marks = [], i;
    var hs = sw.hi, ls = sw.lo;
    var hAt = {}, lAt = {};
    hs.forEach(function (p) { hAt[p.i] = p; });
    ls.forEach(function (p) { lAt[p.i] = p; });
    var lastH = null, prevH = null, lastL = null, prevL = null, trend = 0;
    var ph = [], pl = [];
    for (i = 0; i < d.length; i++) {
      if (hAt[i]) {
        prevH = lastH; lastH = hAt[i];
        ph.push(tag(lastH.t, "aboveBar", "#2962ff", "arrowDown", "PH"));
        if (prevH && lastL && prevL) {
          if (lastH.px > prevH.px && lastL.px > prevL.px && trend !== 1) {
            marks.push(tag(lastH.t, "aboveBar", "#089981", "arrowUp", "REV-UP"));
            trend = 1;
          } else if (lastH.px < prevH.px && lastL.px < prevL.px && trend !== -1) {
            marks.push(tag(lastH.t, "aboveBar", "#f23645", "arrowDown", "LH"));
            trend = -1;
          }
        }
      }
      if (lAt[i]) {
        prevL = lastL; lastL = lAt[i];
        pl.push(tag(lastL.t, "belowBar", "#2962ff", "arrowUp", "PL"));
        if (lastH && prevH && prevL) {
          if (lastL.px > prevL.px && lastH.px > prevH.px && trend !== 1) {
            marks.push(tag(lastL.t, "belowBar", "#089981", "arrowUp", "HL"));
            trend = 1;
          } else if (lastL.px < prevL.px && lastH.px < prevH.px && trend !== -1) {
            marks.push(tag(lastL.t, "belowBar", "#f23645", "arrowDown", "REV-DN"));
            trend = -1;
          }
        }
      }
      if (trend === 1 && lastL && d[i].close < lastL.px) {
        marks.push(tag(d[i].time, "aboveBar", "#f23645", "arrowDown", "REV-DN"));
        trend = -1;
      } else if (trend === -1 && lastH && d[i].close > lastH.px) {
        marks.push(tag(d[i].time, "belowBar", "#089981", "arrowUp", "REV-UP"));
        trend = 1;
      }
    }
    var last = d[d.length - 1];
    var note = trend === 1 ? "HH+HL uptrend" : trend === -1 ? "LH+LL downtrend" : "reaction / test";
    if (trend === 1) marks.push(tag(last.time, "belowBar", "#089981", "circle", "HH+HL"));
    if (trend === -1) marks.push(tag(last.time, "aboveBar", "#f23645", "circle", "LH+LL"));
    return {
      trend: trend === 1 ? "UPTREND" : trend === -1 ? "DOWNTREND" : "TEST",
      note: note,
      markers: ph.slice(-18).concat(pl.slice(-18)).concat(marks)
    };
  }

  /* Wyckoff: scan history for SC→AR→ST→Spring→SOS and BC→UT→SOW. */
  function wyckoffScan(d) {
    var out = [];
    if (d.length < 55) return out;
    var lastSC = null, lastAR = null, lastST = null, lastSpring = null;
    var lastBC = null, lastUT = null, lastSOS = -99, lastSOW = -99, lastLPS = -99, lastLPSY = -99, i, j;
    for (i = 25; i < d.length; i++) {
      var b = d[i], m = metaBar(d, i);
      var rAvg = rngMean(d, i, 20);
      var declined = d[i - 8].close > b.close * 1.004;
      var rallied = d[i - 8].close < b.close * 0.996;
      var isLow = b.low <= ll(d, i, 12), isHigh = b.high >= hh(d, i, 12);

      if (isLow && declined && m.rvol >= 1.45 && m.closeLoc <= 0.40 && m.rng >= rAvg * 1.08) {
        for (j = Math.max(0, i - 5); j < i; j++) {
          var pm = metaBar(d, j);
          if (pm.down && pm.rvol >= 1.1) { out.push(tag(d[j].time, "belowBar", "#ab47bc", "circle", "PS")); break; }
        }
        out.push(tag(b.time, "belowBar", "#f23645", "arrowDown", "SC"));
        lastSC = { i: i, low: b.low, close: b.close, rvol: m.rvol, time: b.time };
        lastAR = lastST = lastSpring = null;
      }
      if (isHigh && rallied && m.rvol >= 1.45 && m.closeLoc >= 0.60 && m.rng >= rAvg * 1.08) {
        for (j = Math.max(0, i - 5); j < i; j++) {
          var qm = metaBar(d, j);
          if (qm.up && qm.rvol >= 1.1) { out.push(tag(d[j].time, "aboveBar", "#ab47bc", "circle", "PSY")); break; }
        }
        out.push(tag(b.time, "aboveBar", "#26a69a", "arrowUp", "BC"));
        lastBC = { i: i, high: b.high, close: b.close, rvol: m.rvol, time: b.time };
        lastUT = null;
      }

      if (lastSC && !lastAR && i > lastSC.i + 1 && i <= lastSC.i + 12 && m.up && b.close > lastSC.close) {
        out.push(tag(b.time, "aboveBar", "#089981", "circle", "AR"));
        lastAR = { i: i, high: b.high, time: b.time };
      }
      if (lastAR && !lastST && i > lastAR.i && i <= lastAR.i + 18) {
        var near = lastSC.low ? Math.abs(b.low - lastSC.low) / lastSC.low <= 0.012 : false;
        if (near && m.rvol < lastSC.rvol && m.closeLoc >= 0.40) {
          out.push(tag(b.time, "belowBar", "#2962ff", "circle", "ST"));
          lastST = { i: i, low: b.low, time: b.time };
        }
      }
      if (lastSC && i > (lastST ? lastST.i : lastSC.i) + 1 && i <= lastSC.i + 55) {
        if (b.low < lastSC.low && b.close > lastSC.low && m.closeLoc >= 0.50) {
          out.push(tag(b.time, "belowBar", "#089981", "square", "SPRING"));
          lastSpring = { i: i, low: b.low, time: b.time };
        }
      }
      var creek = lastAR ? lastAR.high : (lastSC ? lastSC.close * 1.02 : 0);
      if (lastSC && i <= lastSC.i + 70 && i - lastSOS >= 8 && creek && b.close > creek && m.up && m.rvol >= 1.18 && b.close > hh(d, i, 10)) {
        out.push(tag(b.time, "aboveBar", "#089981", "arrowUp", "SOS"));
        lastSOS = i;
      }
      if ((lastSpring || lastST) && i > (lastSpring || lastST).i && i <= (lastSpring || lastST).i + 16 && i - lastLPS >= 6) {
        if (m.rvol <= 0.85 && m.closeLoc >= 0.45 && b.low > (lastSpring ? lastSpring.low : lastSC.low)) {
          out.push(tag(b.time, "belowBar", "#2962ff", "circle", "LPS"));
          lastLPS = i;
        }
      }
      if (lastSC && i - lastSC.i > 80) lastSC = lastAR = lastST = lastSpring = null;

      if (lastBC && i > lastBC.i + 1 && i <= lastBC.i + 20) {
        if (b.high > lastBC.high && b.close < lastBC.high && m.closeLoc <= 0.45) {
          out.push(tag(b.time, "aboveBar", "#f23645", "square", i - lastBC.i > 12 ? "UTAD" : "UT"));
          lastUT = { i: i, time: b.time };
        }
      }
      if (lastBC && i <= lastBC.i + 70 && i - lastSOW >= 8 && m.down && m.rvol >= 1.18 && b.close < ll(d, i, 10)) {
        out.push(tag(b.time, "belowBar", "#f23645", "arrowDown", "SOW"));
        lastSOW = i;
      }
      if (lastUT && i > lastUT.i && i <= lastUT.i + 16 && i - lastLPSY >= 6) {
        if (m.rvol <= 0.85 && m.closeLoc <= 0.55 && b.high < lastBC.high) {
          out.push(tag(b.time, "aboveBar", "#2962ff", "circle", "LPSY"));
          lastLPSY = i;
        }
      }
      if (lastBC && i - lastBC.i > 80) lastBC = lastUT = null;
    }
    return out;
  }

  /* VSA — Tom Williams tape: demand/supply, stopping, absorption, tests, traps. */
  function vsaScan(d) {
    var out = [], i, lastTag = -9, lastKind = "";
    if (d.length < 25) return out;
    for (i = 20; i < d.length; i++) {
      var b = d[i], m = metaBar(d, i);
      var v1 = d[i - 1].volume || 0, v2 = d[i - 2].volume || 0;
      var less2 = (b.volume || 0) < v1 && (b.volume || 0) < v2;
      var rAvg = rngMean(d, i, 20);
      var rise = b.close > d[i - 5].close, fall = b.close < d[i - 5].close;
      var donHi = hh(d, i, 20), donLo = ll(d, i, 20);
      var kind = null, label = null, color = "#ff9800", pos = "aboveBar", shape = "circle";

      if (m.down && m.rvol >= 1.5 && m.closeLoc >= 0.55) {
        kind = "sv"; label = "SV"; color = "#089981"; pos = "belowBar"; shape = "arrowUp";
      } else if (m.rvol >= 1.55 && m.body / m.rng <= 0.34 && m.rng <= rAvg * 1.05) {
        kind = "abs"; label = "ABS"; color = "#ff9800"; pos = m.up ? "aboveBar" : "belowBar"; shape = "square";
      } else if (m.down && m.rvol >= 1.28 && m.closeLoc >= 0.62) {
        kind = "hb"; label = "HB"; color = "#089981"; pos = "belowBar"; shape = "arrowUp";
      } else if (m.up && m.rvol >= 1.28 && m.closeLoc <= 0.38) {
        kind = "hs"; label = "HS"; color = "#f23645"; pos = "aboveBar"; shape = "arrowDown";
      } else if (b.high > donHi && m.closeLoc <= 0.35 && m.rng >= rAvg * 1.05) {
        kind = "trap"; label = "TRAP"; color = "#f23645"; pos = "aboveBar"; shape = "arrowDown";
      } else if (b.low < donLo && m.closeLoc >= 0.65 && m.rng >= rAvg * 0.9) {
        kind = "shk"; label = "SHK"; color = "#089981"; pos = "belowBar"; shape = "arrowUp";
      } else if (fall && m.down && less2 && m.closeLoc >= 0.45 && m.rng <= rAvg * 1.15) {
        kind = "test"; label = "TEST"; color = "#2962ff"; pos = "belowBar"; shape = "circle";
      } else if (m.up && less2 && rise && m.rng <= rAvg * 1.1) {
        kind = "nd"; label = "ND"; color = "#ef9a9a"; pos = "aboveBar"; shape = "circle";
      } else if (m.down && less2 && fall && m.rng <= rAvg * 1.1) {
        kind = "ns"; label = "NS"; color = "#80cbc4"; pos = "belowBar"; shape = "circle";
      }
      if (!kind) continue;
      if ((kind === "nd" || kind === "ns" || kind === "test") && i - lastTag < 4 && (lastKind === kind || lastKind === "nd" || lastKind === "ns")) continue;
      lastTag = i; lastKind = kind;
      out.push(tag(b.time, pos, color, shape, label));
    }
    return out;
  }

  function effort(d) {
    var out = [], i;
    for (i = 10; i < d.length; i++) {
      var vA = 0, pA = 0, j;
      for (j = i - 10; j < i; j++) { vA += d[j].volume || 0; pA += Math.abs(d[j].close - d[j].open); }
      vA /= 10; pA /= 10;
      var body = Math.abs(d[i].close - d[i].open);
      var vol = d[i].volume || 0;
      if (vA && vol > vA * 1.65 && pA && body < pA * 0.55) {
        out.push(tag(d[i].time, d[i].close >= d[i].open ? "aboveBar" : "belowBar", "#f0b429", "circle",
          d[i].close >= d[i].open ? "E↑noR" : "E↓noR"));
      }
    }
    return out;
  }

  function accumDistrib(d, wy) {
    var accum = [], distrib = [], i, look = 20;
    for (i = look; i < d.length; i++) {
      var vA = 0, j, pxCh, volNow, rng, closeLoc, dry;
      for (j = i - look; j < i; j++) vA += d[j].volume || 0;
      vA /= look;
      pxCh = d[i].close - d[i - look].close;
      volNow = d[i].volume || 0;
      dry = vA && volNow < vA * 0.72;
      rng = d[i].high - d[i].low || 1e-9;
      closeLoc = (d[i].close - d[i].low) / rng;
      if (pxCh <= 0 && dry && closeLoc > 0.55) accum.push(tag(d[i].time, "belowBar", "#089981", "circle", "ACC"));
      if (pxCh >= 0 && volNow > vA * 1.35 && closeLoc < 0.45) distrib.push(tag(d[i].time, "aboveBar", "#f23645", "circle", "DIST"));
    }
    (wy || []).forEach(function (m) {
      if (/SC|SPRING|LPS|AR|ST|SOS|PS/.test(m.text)) accum.push(m);
      if (/BC|UT|SOW|LPSY|PSY|UTAD/.test(m.text)) distrib.push(m);
    });
    return { accum: accum, distrib: distrib };
  }

  function keepLast(arr, n) {
    if (!arr || arr.length <= n) return arr || [];
    return arr.slice(-n);
  }

  window.jhTapeRead = function (d) {
    if (!d || d.length < 40) {
      return { markers: [], panel: "tape: short", livermore: { markers: [] }, wyckoff: { markers: [] }, accum: { markers: [] }, distrib: { markers: [] }, vsa: { markers: [] }, tape: { markers: [] } };
    }
    var sw = swings(d, 4);
    var lv = livermore(d, sw);
    var wy = wyckoffScan(d);
    var vs = vsaScan(d);
    var ef = effort(d);
    var ad = accumDistrib(d, wy);
    var wyMarks = keepLast(wy.concat(ef), 80);
    var vsaMarks = keepLast(vs, 90);
    var all = lv.markers.concat(wyMarks).concat(vsaMarks);
    var panel = "LIVERMORE " + lv.trend + " · " + lv.note + " | WYCKOFF PS SC AR ST SPRING SOS LPS · BC UT SOW | VSA ND NS SV ABS HB HS TEST TRAP SHK";
    return {
      markers: all,
      panel: panel,
      livermore: { trend: lv.trend, note: lv.note, markers: lv.markers },
      wyckoff: { markers: wyMarks },
      accum: { markers: keepLast(ad.accum, 60) },
      distrib: { markers: keepLast(ad.distrib, 60) },
      vsa: { markers: vsaMarks },
      tape: { markers: all }
    };
  };
  window.__jhTapeReadRaw = window.jhTapeRead;
})();
