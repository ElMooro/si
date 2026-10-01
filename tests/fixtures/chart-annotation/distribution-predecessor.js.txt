/* Distribution engine — seven tests from the rally lesson.
   A rally is not the verdict. Distribution prints only when later upward
   effort buys less progress, the gain is not held, the next reaction is
   more effective, follow-through fails to repair, and the bullish case
   (acceptance above the area) does not survive. Off until the Dist button. */
(function () {
  function swingHigh(d, i, L, R) {
    if (i < L || i + R >= d.length) return false;
    var hi = d[i].high, j;
    for (j = i - L; j <= i + R; j++) if (j !== i && d[j].high >= hi) return false;
    return true;
  }
  function swingLow(d, i, L, R) {
    if (i < L || i + R >= d.length) return false;
    var lo = d[i].low, j;
    for (j = i - L; j <= i + R; j++) if (j !== i && d[j].low <= lo) return false;
    return true;
  }
  function hh(d, i, n) {
    var h = -1e99, j, a = Math.max(0, i - n);
    for (j = a; j <= i; j++) if (d[j].high > h) h = d[j].high;
    return h;
  }
  function ll(d, i, n) {
    var l = 1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].low < l) l = d[j].low;
    return l;
  }
  function volSum(d, a, b) {
    var s = 0, n = 0, j;
    for (j = a; j <= b; j++) { s += d[j].volume || 0; n++; }
    return n ? s / n : 0;
  }
  function tag(t, pos, color, shape, text) {
    return { time: t, position: pos, color: color, shape: shape, text: text };
  }

  /* Confirmed distribution sequences. Each event is the bar where the rally
     failed its proof, not the first up day. */
  function distributionScan(d) {
    if (!d || d.length < 280) return [];
    var highs = [], lows = [], i, j;
    for (i = 40; i < d.length - 12; i++) {
      if (swingHigh(d, i, 8, 5)) highs.push(i);
      if (swingLow(d, i, 8, 5)) lows.push(i);
    }
    var out = [], lastEmit = -999, lastHighPx = 0;
    for (i = 0; i < highs.length; i++) {
      var h = highs[i];
      if (h + 20 >= d.length) continue;
      if (h - lastEmit < 40 && d[h].high <= lastHighPx) continue;
      var yHi = hh(d, h, 189);
      var yLo = ll(d, h, 252);
      var yLo2 = ll(d, h, 504);
      if (!(yHi > 0) || d[h].high < yHi * 0.97) continue;
      var adv1 = yLo > 0 && d[h].high / yLo - 1 >= 0.15;
      var adv2 = yLo2 > 0 && d[h].high / yLo2 - 1 >= 0.30;
      if (!adv1 && !adv2) continue;
      var prevH = -1, k;
      for (k = i - 1; k >= 0; k--) if (h - highs[k] <= 80 && h - highs[k] >= 8) { prevH = highs[k]; break; }
      if (prevH < 0) continue;
      var lowB = -1, lowA = -1;
      for (k = lows.length - 1; k >= 0; k--) {
        if (lows[k] < h && lows[k] > prevH && lowB < 0) lowB = lows[k];
        if (lows[k] < prevH && h - lows[k] <= 120 && lowA < 0) lowA = lows[k];
      }
      if (lowB < 0) {
        lowB = prevH;
        for (k = prevH; k < h; k++) if (d[k].low < d[lowB].low) lowB = k;
      }
      if (lowA < 0) {
        lowA = Math.max(0, prevH - 40);
        for (k = lowA; k < prevH; k++) if (d[k].low < d[lowA].low) lowA = k;
      }
      var resB = d[lowB].low ? d[h].high / d[lowB].low - 1 : 0;
      var resA = d[lowA].low ? d[prevH].high / d[lowA].low - 1 : 0;
      if (resB <= 0.005 || resA <= 0.005) continue;
      var effB = volSum(d, lowB, h);
      var effA = volSum(d, lowA, prevH);
      var worseEffort = effA > 0 && effB > effA * 1.1 && resB < resA * 0.85;
      var smallNet = (d[h].high / d[prevH].high - 1) < 0.025 && resB < resA;
      if (!worseEffort && !smallNet) continue;

      var wave = d[h].high - d[lowB].low;
      if (wave <= 0) continue;
      var gave = -1, t;
      for (t = h + 1; t <= Math.min(d.length - 1, h + 18); t++) {
        var offHigh = d[h].high ? (d[h].high - d[t].close) / d[h].high : 0;
        if (d[h].high - d[t].close >= wave * 0.5 && offHigh >= 0.035) { gave = t; break; }
      }
      if (gave < 0) continue;
      var confirm = Math.min(d.length - 1, gave + 6);
      var reclaimed = false;
      for (t = gave; t <= confirm; t++) if (d[t].close >= d[h].high * 0.998) reclaimed = true;
      if (reclaimed) continue;

      var priorDrop = d[prevH].high ? (d[lowB].low / d[prevH].high - 1) : 0;
      var newLow = gave, nl;
      for (nl = h + 1; nl <= confirm; nl++) if (d[nl].low < d[newLow].low) newLow = nl;
      var newDrop = d[h].high ? d[newLow].low / d[h].high - 1 : 0;
      var worseReaction = newDrop < priorDrop - 0.005 || newDrop <= -0.045;
      if (!worseReaction && newDrop > -0.03) continue;

      out.push({
        i: confirm,
        time: d[confirm].time,
        highI: h,
        highTime: d[h].time,
        text: "DIST"
      });
      lastEmit = h;
      lastHighPx = d[h].high;
    }
    return out;
  }

  function isDaily(d, tf) {
    if (tf === "1d") return true;
    if (!d || d.length < 8) return false;
    var g = [], i, n = Math.min(d.length - 1, 60);
    for (i = d.length - n; i < d.length; i++) if (i > 0 && d[i].time > d[i - 1].time) g.push(d[i].time - d[i - 1].time);
    g.sort(function (a, b) { return a - b; });
    var med = g.length ? g[Math.floor(g.length / 2)] : 0;
    return med >= 18 * 3600 && med <= 4 * 86400;
  }

  window.__jhDistOn = 0;
  window.jhDistributionScan = distributionScan;
  window.jhDistributionMarks = function (d, tf, kind) {
    if (!window.__jhDistOn) return [];
    if (kind && kind !== "candles" && kind !== "hollow" && kind !== "volcandle") return [];
    if (!isDaily(d, tf)) return [];
    var seq = distributionScan(d);
    var marks = [], i, e, tops = [];
    if (window.jhVolEventTable) {
      try {
        var rows = window.jhVolEventTable(d) || [];
        for (i = 0; i < rows.length; i++) if (rows[i] && rows[i].kind === "top") tops.push(rows[i]);
      } catch (err) {}
    }
    for (i = 0; i < seq.length; i++) {
      e = seq[i];
      marks.push(tag(e.time, "aboveBar", "#f23645", "arrowDown", "DIST"));
      var top = null, k, best = 1e9;
      for (k = 0; k < tops.length; k++) {
        var di = Math.abs((tops[k].i || 0) - e.highI);
        if (di <= 40 && di < best) { best = di; top = tops[k]; }
      }
      if (top) marks.push(tag(top.time, "aboveBar", "#ab47bc", "arrowDown", "D-TOP"));
    }
    return marks;
  };
  window.jhDistToggle = function () {
    window.__jhDistOn = window.__jhDistOn ? 0 : 1;
    var el = document.getElementById("btn-dist");
    if (el) {
      if (window.__jhDistOn) el.classList.add("on");
      else el.classList.remove("on");
    }
    if (window.paint && window.lastBars && window.lastBars.length) {
      try { window.paint(window.lastBars); } catch (e) {}
    }
  };
})();
