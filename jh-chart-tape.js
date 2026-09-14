/* Livermore + full Wyckoff labels. Effort vs result. No orders. */
(function () {
  function avg(a) { var s = 0, i; for (i = 0; i < a.length; i++) s += a[i]; return a.length ? s / a.length : 0; }
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
  function tag(t, pos, color, shape, text) {
    return { time: t, position: pos, color: color, shape: shape, text: text };
  }
  function livermore(d, sw) {
    var hs = sw.hi.slice(-5), ls = sw.lo.slice(-5);
    var trend = "—", note = "need pivots";
    if (hs.length >= 2 && ls.length >= 2) {
      var hh = hs[hs.length - 1].px > hs[hs.length - 2].px;
      var hl = ls[ls.length - 1].px > ls[ls.length - 2].px;
      var lh = hs[hs.length - 1].px < hs[hs.length - 2].px;
      var ll = ls[ls.length - 1].px < ls[ls.length - 2].px;
      if (hh && hl) { trend = "UPTREND"; note = "HH+HL"; }
      else if (lh && ll) { trend = "DOWNTREND"; note = "LH+LL"; }
      else { trend = "TEST"; note = "failed side"; }
    }
    var last = d[d.length - 1];
    if (ls.length && last.close < ls[ls.length - 1].px && trend !== "DOWNTREND") { trend = "REV-DN"; note = "broke reaction low"; }
    if (hs.length && last.close > hs[hs.length - 1].px && trend !== "UPTREND") { trend = "REV-UP"; note = "broke rally high"; }
    var marks = [];
    hs.slice(-6).forEach(function (p) { marks.push(tag(p.t, "aboveBar", "#2962ff", "arrowDown", "PH")); });
    ls.slice(-6).forEach(function (p) { marks.push(tag(p.t, "belowBar", "#2962ff", "arrowUp", "PL")); });
    if (trend === "REV-UP") marks.push(tag(last.time, "belowBar", "#089981", "arrowUp", "REV-UP"));
    if (trend === "REV-DN") marks.push(tag(last.time, "aboveBar", "#f23645", "arrowDown", "REV-DN"));
    if (trend === "UPTREND") marks.push(tag(last.time, "belowBar", "#089981", "circle", "HH+HL"));
    if (trend === "DOWNTREND") marks.push(tag(last.time, "aboveBar", "#f23645", "circle", "LH+LL"));
    return { trend: trend, note: note, hs: hs, ls: ls, markers: marks };
  }
  function wyckoffFull(d) {
    var out = [];
    if (d.length < 40) return out;
    var win = d.slice(-80);
    var vAvg = avg(win.map(function (b) { return b.volume || 0; }));
    var maxH = -1e99, minL = 1e99, maxI = 0, minI = 0, i;
    for (i = 0; i < win.length; i++) {
      if (win[i].high > maxH) { maxH = win[i].high; maxI = i; }
      if (win[i].low < minL) { minL = win[i].low; minI = i; }
    }
    var mid = (maxH + minL) / 2;
    var last = win[win.length - 1];
    if (minI > 2 && minI < win.length - 2) {
      var sc = win[minI];
      if ((sc.volume || 0) > vAvg * 1.4) out.push(tag(sc.time, "belowBar", "#f23645", "arrowDown", "SC"));
      if (minI >= 3) {
        var ps = win[minI - 2];
        if ((ps.volume || 0) > vAvg && ps.close < ps.open) out.push(tag(ps.time, "belowBar", "#ab47bc", "circle", "PS"));
      }
      if (minI + 2 < win.length) {
        var ar = win[minI + 2];
        if (ar.close > sc.close) out.push(tag(ar.time, "aboveBar", "#089981", "circle", "AR"));
      }
    }
    if (maxI > 2) {
      var bc = win[maxI];
      if ((bc.volume || 0) > vAvg * 1.4 && bc.close > mid) out.push(tag(bc.time, "aboveBar", "#089981", "arrowUp", "BC"));
    }
    if (last.low < minL * 1.004 && last.close > minL && last.close < mid)
      out.push(tag(last.time, "belowBar", "#089981", "square", "SPRING"));
    if (last.high > maxH * 0.996 && last.close < maxH && last.close > mid)
      out.push(tag(last.time, "aboveBar", "#f23645", "square", "UT"));
    if (last.close > mid && (last.volume || 0) > vAvg * 1.2 && last.close > last.open)
      out.push(tag(last.time, "aboveBar", "#089981", "arrowUp", "SOS"));
    if (last.close < mid && (last.volume || 0) > vAvg * 1.2 && last.close < last.open)
      out.push(tag(last.time, "belowBar", "#f23645", "arrowDown", "SOW"));
    var prev = win[win.length - 5];
    if (prev && last.close > mid && last.low > minL && (last.volume || 0) < vAvg)
      out.push(tag(last.time, "belowBar", "#2962ff", "circle", "LPS"));
    if (prev && last.close < mid && last.high < maxH && (last.volume || 0) < vAvg)
      out.push(tag(last.time, "aboveBar", "#2962ff", "circle", "LPSY"));
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
      if (vA && vol > vA * 1.7 && pA && body < pA * 0.55) {
        out.push(tag(d[i].time, d[i].close >= d[i].open ? "aboveBar" : "belowBar", "#ff9800", "circle",
          d[i].close >= d[i].open ? "E↑noR" : "E↓noR"));
      }
    }
    return out.slice(-10);
  }
  function accumDistrib(d) {
    var accum = [], distrib = [];
    if (d.length < 30) return { accum: accum, distrib: distrib };
    var i, look = 20;
    for (i = look; i < d.length; i++) {
      var vA = 0, j, dnVol = 0, upVol = 0, pxCh;
      for (j = i - look; j < i; j++) {
        vA += d[j].volume || 0;
        if (d[j].close >= d[j].open) upVol += d[j].volume || 0;
        else dnVol += d[j].volume || 0;
      }
      vA /= look;
      pxCh = d[i].close - d[i - look].close;
      var volNow = d[i].volume || 0;
      var dry = vA && volNow < vA * 0.7;
      var rng = d[i].high - d[i].low || 1e-9;
      var closeLoc = (d[i].close - d[i].low) / rng;
      if (pxCh <= 0 && dry && closeLoc > 0.55) accum.push(tag(d[i].time, "belowBar", "#089981", "circle", "ACC"));
      if (pxCh >= 0 && volNow > vA * 1.4 && closeLoc < 0.45) distrib.push(tag(d[i].time, "aboveBar", "#f23645", "circle", "DIST"));
    }
    var wy = wyckoffFull(d);
    wy.forEach(function (m) {
      if (/SC|SPRING|LPS|AR/.test(m.text)) accum.push(m);
      if (/BC|UT|SOW|LPSY/.test(m.text)) distrib.push(m);
    });
    return { accum: accum.slice(-14), distrib: distrib.slice(-14) };
  }
  window.jhTapeRead = function (d) {
    if (!d || d.length < 40) return { markers: [], panel: "tape: short", livermore: { markers: [] }, wyckoff: { markers: [] }, accum: { markers: [] }, distrib: { markers: [] } };
    var sw = swings(d);
    var lv = livermore(d, sw);
    var wy = wyckoffFull(d);
    var ef = effort(d);
    var ad = accumDistrib(d);
    var marks = lv.markers.concat(wy).concat(ef);
    if (window.jhVolEvents) marks = (window.jhVolEvents(d) || []).concat(marks);
    var panel = "LIVERMORE " + lv.trend + " " + lv.note + " | WYCKOFF PS SC AR SPRING UT SOS SOW LPS";
    return {
      markers: marks,
      panel: panel,
      livermore: { trend: lv.trend, note: lv.note, markers: lv.markers },
      wyckoff: { markers: wy.concat(ef) },
      accum: { markers: ad.accum },
      distrib: { markers: ad.distrib }
    };
  };
})();
