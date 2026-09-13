/* Livermore + Wyckoff + reversal sequences. Labels only. */
(function () {
  function swings(d, left, right) {
    left = left || 4; right = right || 4;
    var hi = [], lo = [], i, j;
    for (i = left; i < d.length - right; i++) {
      var isH = true, isL = true;
      for (j = i - left; j <= i + right; j++) {
        if (j === i) continue;
        if (d[j].high > d[i].high) isH = false;
        if (d[j].low < d[i].low) isL = false;
      }
      if (isH) hi.push({ i: i, px: d[i].high, t: d[i].time });
      if (isL) lo.push({ i: i, px: d[i].low, t: d[i].time });
    }
    return { hi: hi, lo: lo };
  }

  function livermore(d, sw) {
    var hs = sw.hi.slice(-4), ls = sw.lo.slice(-4);
    var trend = "—", note = "need two pivots";
    if (hs.length >= 2 && ls.length >= 2) {
      var hh = hs[hs.length - 1].px > hs[hs.length - 2].px;
      var hl = ls[ls.length - 1].px > ls[ls.length - 2].px;
      var lh = hs[hs.length - 1].px < hs[hs.length - 2].px;
      var ll = ls[ls.length - 1].px < ls[ls.length - 2].px;
      if (hh && hl) { trend = "UPTREND"; note = "HH + HL"; }
      else if (lh && ll) { trend = "DOWNTREND"; note = "LH + LL"; }
      else { trend = "TEST"; note = "one side failed"; }
    }
    var last = d[d.length - 1];
    if (ls.length && (trend === "UPTREND" || trend === "TEST") && last.close < ls[ls.length - 1].px) {
      trend = "REV-DN"; note = "broke last reaction low";
    }
    if (hs.length && (trend === "DOWNTREND" || trend === "TEST") && last.close > hs[hs.length - 1].px) {
      trend = "REV-UP"; note = "broke last rally high";
    }
    return { trend: trend, note: note, pivotsH: hs, pivotsL: ls };
  }

  function wyckoff(d) {
    if (d.length < 30) return { phase: "—", events: [], note: "short" };
    var win = d.slice(-40), maxH = -1e99, minL = 1e99, i;
    for (i = 0; i < win.length; i++) {
      if (win[i].high > maxH) maxH = win[i].high;
      if (win[i].low < minL) minL = win[i].low;
    }
    var mid = (maxH + minL) / 2, last = win[win.length - 1], vAvg = 0;
    for (i = 0; i < win.length; i++) vAvg += win[i].volume || 0;
    vAvg /= win.length;
    var ev = [];
    if (last.low < minL * 1.003 && last.close > minL && last.close < mid)
      ev.push({ t: last.time, text: "SPRING", pos: "belowBar", color: "#089981" });
    if (last.high > maxH * 0.997 && last.close < maxH && last.close > mid)
      ev.push({ t: last.time, text: "UT", pos: "aboveBar", color: "#f23645" });
    return { phase: ev.length ? ev[0].text : "range", events: ev, support: minL, resist: maxH, note: minL.toFixed(2) + "–" + maxH.toFixed(2) };
  }

  function sequences(d, volRows, lv, wy) {
    var mk = [];
    var last = d[d.length - 1];
    var recent = (volRows || []).filter(function (e) { return e._i >= d.length - 25; });
    var hasSC = recent.some(function (e) { return e.text === "SC" || e.text === "CAPIT"; });
    var hasBC = recent.some(function (e) { return e.text === "BC" || e.text === "BC+"; });
    var hasNS = recent.some(function (e) { return e.text === "NS" || e.text === "ACC"; });
    var spring = (wy.events || []).some(function (e) { return e.text === "SPRING"; });
    var ut = (wy.events || []).some(function (e) { return e.text === "UT"; });
    if ((hasSC && (hasNS || spring)) || lv.trend === "REV-UP") {
      mk.push({ time: last.time, position: "belowBar", color: "#089981", shape: "arrowUp", text: "BOTTOM" });
    }
    if ((hasBC && ut) || lv.trend === "REV-DN") {
      mk.push({ time: last.time, position: "aboveBar", color: "#f23645", shape: "arrowDown", text: "TOP" });
    }
    if (lv.trend === "REV-UP")
      mk.push({ time: last.time, position: "belowBar", color: "#2962ff", shape: "arrowUp", text: "REV-UP" });
    if (lv.trend === "REV-DN")
      mk.push({ time: last.time, position: "aboveBar", color: "#2962ff", shape: "arrowDown", text: "REV-DN" });
    return mk;
  }

  window.jhTapeRead = function (d) {
    if (!d || d.length < 55) return { markers: [], panel: "tape: need 55 bars" };
    var sw = swings(d);
    var lv = livermore(d, sw);
    var wy = wyckoff(d);
    var volRows = window.jhVolEventTable ? window.jhVolEventTable(d) : [];
    var mk = [];
    lv.pivotsH.slice(-3).forEach(function (p) {
      mk.push({ time: p.t, position: "aboveBar", color: "#2962ff", shape: "arrowDown", text: "PH" });
    });
    lv.pivotsL.slice(-3).forEach(function (p) {
      mk.push({ time: p.t, position: "belowBar", color: "#2962ff", shape: "arrowUp", text: "PL" });
    });
    wy.events.forEach(function (e) {
      mk.push({ time: e.t, position: e.pos, color: e.color, shape: "square", text: e.text });
    });
    if (window.jhVolEvents) mk = (window.jhVolEvents(d) || []).concat(mk);
    mk = mk.concat(sequences(d, volRows, lv, wy));
    var panel = "LIVERMORE " + lv.trend + " (" + lv.note + ") | WYCKOFF " + wy.phase + " " + wy.note;
    return { markers: mk, panel: panel, livermore: lv, wyckoff: wy };
  };
})();
