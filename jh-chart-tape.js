/* Livermore + Wyckoff tape. Labels only — no orders. Warehouse bars only. */
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
      if (isH) hi.push({ i: i, px: d[i].high, t: d[i].time, v: d[i].volume || 0 });
      if (isL) lo.push({ i: i, px: d[i].low, t: d[i].time, v: d[i].volume || 0 });
    }
    return { hi: hi, lo: lo };
  }

  function livermore(d, sw) {
    var hs = sw.hi.slice(-4), ls = sw.lo.slice(-4);
    var trend = "—", note = "need two confirmed pivots";
    if (hs.length >= 2 && ls.length >= 2) {
      var hh = hs[hs.length - 1].px > hs[hs.length - 2].px;
      var hl = ls[ls.length - 1].px > ls[ls.length - 2].px;
      var lh = hs[hs.length - 1].px < hs[hs.length - 2].px;
      var ll = ls[ls.length - 1].px < ls[ls.length - 2].px;
      if (hh && hl) { trend = "UPTREND"; note = "higher pivot high + higher reaction low (Livermore continuation)"; }
      else if (lh && ll) { trend = "DOWNTREND"; note = "lower pivot high + lower reaction low"; }
      else if (hh && ll) { trend = "WIDENING"; note = "outside swings — wait for a pivotal reversal"; }
      else { trend = "TEST"; note = "one side failed — treat as change-of-trend watch"; }
    }
    var last = d[d.length - 1];
    var danger = false;
    if (ls.length && trend === "UPTREND" && last.close < ls[ls.length - 1].px) {
      danger = true; note = "broke last reaction low — Livermore change of trend"; trend = "BROKEN UP";
    }
    if (hs.length && trend === "DOWNTREND" && last.close > hs[hs.length - 1].px) {
      danger = true; note = "broke last rally high — change of trend up"; trend = "BROKEN DN";
    }
    return { trend: trend, note: note, danger: danger, pivotsH: hs, pivotsL: ls };
  }

  function wyckoff(d) {
    if (d.length < 30) return { phase: "—", events: [], note: "short history" };
    var win = d.slice(-40);
    var maxH = -1e99, minL = 1e99, i, maxI = 0, minI = 0;
    for (i = 0; i < win.length; i++) {
      if (win[i].high > maxH) { maxH = win[i].high; maxI = i; }
      if (win[i].low < minL) { minL = win[i].low; minI = i; }
    }
    var mid = (maxH + minL) / 2;
    var last = win[win.length - 1];
    var vAvg = 0;
    for (i = 0; i < win.length; i++) vAvg += win[i].volume || 0;
    vAvg /= win.length;
    var ev = [];
    var last5 = win.slice(-5);
    var pierce = last.low < minL * 1.002 && last.close > minL && last.close < mid;
    var ut = last.high > maxH * 0.998 && last.close < maxH && last.close > mid;
    if (pierce && (last.volume || 0) > vAvg * 0.8) ev.push({ t: last.time, text: "SPRING", pos: "belowBar", color: "#089981" });
    if (ut && (last.volume || 0) > vAvg) ev.push({ t: last.time, text: "UT", pos: "aboveBar", color: "#f23645" });
    var phase = "B range";
    if (pierce) phase = "C spring test";
    else if (ut) phase = "C upthrust";
    else if (last.close > mid && last5.every(function (b) { return (b.volume || 0) < vAvg * 1.1; })) phase = "D markup watch";
    else if (last.close < mid && last5.every(function (b) { return (b.volume || 0) < vAvg * 1.1; })) phase = "D markdown watch";
    return { phase: phase, events: ev, support: minL, resist: maxH, note: "range " + minL.toFixed(2) + "–" + maxH.toFixed(2) };
  }

  function effort(d) {
    var out = [], i;
    for (i = 8; i < d.length; i++) {
      var vA = 0, pA = 0, j;
      for (j = i - 8; j < i; j++) { vA += d[j].volume || 0; pA += Math.abs(d[j].close - d[j].open); }
      vA /= 8; pA /= 8;
      var body = Math.abs(d[i].close - d[i].open);
      var vol = d[i].volume || 0;
      if (vA && vol > vA * 1.8 && pA && body < pA * 0.6) {
        out.push({
          t: d[i].time,
          text: d[i].close >= d[i].open ? "EFFORT↑no result" : "EFFORT↓no result",
          pos: d[i].close >= d[i].open ? "aboveBar" : "belowBar",
          color: "#ff9800"
        });
      }
    }
    return out.slice(-6);
  }

  window.jhTapeRead = function (d) {
    if (!d || d.length < 25) return { markers: [], panel: "tape: short history" };
    var sw = swings(d);
    var lv = livermore(d, sw);
    var wy = wyckoff(d);
    var ef = effort(d);
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
    ef.forEach(function (e) {
      mk.push({ time: e.t, position: e.pos, color: e.color, shape: "circle", text: e.text });
    });
    if (window.jhVolEvents) mk = (window.jhVolEvents(d) || []).concat(mk);
    var panel = "LIVERMORE " + lv.trend + " — " + lv.note + " | WYCKOFF " + wy.phase + " — " + wy.note;
    return { markers: mk, panel: panel, livermore: lv, wyckoff: wy };
  };
})();
