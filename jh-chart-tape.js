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
    return { trend: trend, note: note, hs: hs, ls: ls };
  }
  function wyckoffFull(d) {
    var out = [];
    if (d.length < 40) return out;
    var win = d.slice(-60);
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
    return out.slice(-8);
  }
  window.jhTapeRead = function (d) {
    if (!d || d.length < 40) return { markers: [], panel: "tape: short" };
    var sw = swings(d);
    var lv = livermore(d, sw);
    var marks = [];
    lv.hs.slice(-3).forEach(function (p) { marks.push(tag(p.t, "aboveBar", "#2962ff", "arrowDown", "PH")); });
    lv.ls.slice(-3).forEach(function (p) { marks.push(tag(p.t, "belowBar", "#2962ff", "arrowUp", "PL")); });
    marks = marks.concat(wyckoffFull(d)).concat(effort(d));
    if (window.jhVolEvents) marks = (window.jhVolEvents(d) || []).concat(marks);
    if (lv.trend === "REV-UP") marks.push(tag(d[d.length - 1].time, "belowBar", "#2962ff", "arrowUp", "REV-UP"));
    if (lv.trend === "REV-DN") marks.push(tag(d[d.length - 1].time, "aboveBar", "#2962ff", "arrowDown", "REV-DN"));
    var panel = "LIVERMORE " + lv.trend + " " + lv.note + " | WYCKOFF PS SC AR SPRING UT SOS SOW LPS | E vs result";
    return { markers: marks, panel: panel, livermore: lv };
  };
})();
