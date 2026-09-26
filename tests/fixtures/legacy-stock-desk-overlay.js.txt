/* JustHodl stock desk overlay — professional tape on the v12 engine.
   Stocks first. Volume confirms patterns. Chart Pro untouched. */
(function () {
  var STOCKS = ["SPY","QQQ","IWM","AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","XLE","XLF","TLT","GLD","SMH"];

  function barsFromChart() {
    if (window.lastBars && window.lastBars.length) return window.lastBars;
    return [];
  }

  function sma(arr, n) {
    var o = [], s = 0, i;
    for (i = 0; i < arr.length; i++) {
      s += arr[i];
      if (i >= n) s -= arr[i - n];
      if (i >= n - 1) o.push(s / n);
      else o.push(null);
    }
    return o;
  }

  function bbWidth(d, n, k) {
    n = n || 20; k = k || 2;
    if (d.length < n) return null;
    var closes = d.map(function (b) { return b.close; });
    var m = sma(closes, n);
    var i = d.length - 1, mean = m[i];
    if (mean == null) return null;
    var ss = 0, j;
    for (j = 0; j < n; j++) ss += Math.pow(d[i - j].close - mean, 2);
    var sd = Math.sqrt(ss / n);
    var width = (2 * k * sd) / mean;
    var hist = [];
    for (var t = n; t < d.length; t++) {
      var mt = m[t]; if (mt == null) continue;
      var s2 = 0;
      for (j = 0; j < n; j++) s2 += Math.pow(d[t - j].close - mt, 2);
      hist.push((2 * k * Math.sqrt(s2 / n)) / mt);
    }
    hist.sort(function (a, b) { return a - b; });
    var pct = hist.length ? hist.indexOf(hist.filter(function (x) { return x >= width; })[0]) / hist.length : 0.5;
    return { width: width, pct: pct, squeeze: pct <= 0.15 };
  }

  function adLine(d) {
    var ad = 0, obv = 0, i;
    for (i = 0; i < d.length; i++) {
      var r = d[i].high - d[i].low;
      var mfm = r ? ((d[i].close - d[i].low) - (d[i].high - d[i].close)) / r : 0;
      ad += mfm * (d[i].volume || 0);
      if (i) {
        if (d[i].close > d[i - 1].close) obv += d[i].volume || 0;
        else if (d[i].close < d[i - 1].close) obv -= d[i].volume || 0;
      }
    }
    var look = Math.min(20, d.length);
    var ad0 = 0, ad1 = ad;
    /* rough slope: last look vs prior look using close-location only */
    return { ad: ad, obv: obv, look: look };
  }

  function localExtrema(d, left, right, kind) {
    var out = [], i, j;
    for (i = left; i < d.length - right; i++) {
      var px = kind === "high" ? d[i].high : d[i].low, ok = true;
      for (j = i - left; j <= i + right; j++) {
        if (j === i) continue;
        var q = kind === "high" ? d[j].high : d[j].low;
        if (kind === "high" ? q > px : q < px) { ok = false; break; }
      }
      if (ok) out.push({ i: i, px: px, vol: d[i].volume || 0, time: d[i].time });
    }
    return out;
  }

  function doublePatterns(d) {
    if (d.length < 40) return [];
    var found = [];
    var highs = localExtrema(d, 5, 5, "high");
    var lows = localExtrema(d, 5, 5, "low");
    var a, b, k;
    for (a = 0; a < highs.length; a++) {
      for (b = a + 1; b < highs.length; b++) {
        var p1 = highs[a], p2 = highs[b];
        if (p2.i - p1.i < 8 || p2.i - p1.i > 90) continue;
        if (Math.abs(p2.px - p1.px) / p1.px > 0.03) continue;
        var neck = 1e99;
        for (k = p1.i; k <= p2.i; k++) neck = Math.min(neck, d[k].low);
        var last = d[d.length - 1];
        var volOk = p2.vol < p1.vol; /* Bulkowski: right peak quieter */
        var confirmed = last.close < neck;
        found.push({
          kind: "double_top",
          p1: p1, p2: p2, neck: neck,
          volume_confirms: volOk,
          status: confirmed && volOk ? "confirmed" : (volOk ? "forming" : "weak_volume"),
          note: volOk ? "right peak volume < left (distribution tell)" : "right peak not quieter — not a clean top"
        });
      }
    }
    for (a = 0; a < lows.length; a++) {
      for (b = a + 1; b < lows.length; b++) {
        var l1 = lows[a], l2 = lows[b];
        if (l2.i - l1.i < 8 || l2.i - l1.i > 90) continue;
        if (Math.abs(l2.px - l1.px) / l1.px > 0.03) continue;
        var neck2 = -1e99;
        for (k = l1.i; k <= l2.i; k++) neck2 = Math.max(neck2, d[k].high);
        var last2 = d[d.length - 1];
        var volOk2 = l2.vol < l1.vol; /* second low on lighter volume */
        var confirmed2 = last2.close > neck2;
        found.push({
          kind: "double_bottom",
          p1: l1, p2: l2, neck: neck2,
          volume_confirms: volOk2,
          status: confirmed2 && volOk2 ? "confirmed" : (volOk2 ? "forming" : "weak_volume"),
          note: volOk2 ? "second low quieter — supply drying (accumulation tell)" : "second low not quieter"
        });
      }
    }
    found.sort(function (x, y) {
      var r = { confirmed: 0, forming: 1, weak_volume: 2 };
      return (r[x.status] - r[y.status]) || (y.p2.i - x.p2.i);
    });
    return found.slice(0, 6);
  }

  function paint() {
    var host = document.getElementById("tech") || document.getElementById("intel");
    if (!host) return;
    var d = barsFromChart();
    if (d.length < 20) {
      host.innerHTML = "<b>STOCK DESK</b><div class=cell>Need real bars — no synth</div>";
      return;
    }
    var bb = bbWidth(d);
    var pats = doublePatterns(d);
    var last = d[d.length - 1];
    var avgV = 0, i;
    for (i = Math.max(0, d.length - 20); i < d.length; i++) avgV += d[i].volume || 0;
    avgV /= Math.min(20, d.length);
    var rvol = avgV ? last.volume / avgV : 0;
    var html = "<b>STOCK DESK</b>";
    html += "<div class=cell><span>RVOL 20</span><span>" + rvol.toFixed(2) + "x</span></div>";
    if (bb) html += "<div class=cell><span>BB width %ile</span><span>" + (bb.pct * 100).toFixed(0) + "%" + (bb.squeeze ? " SQUEEZE" : "") + "</span></div>";
    html += "<div class=cell><span>Last vol</span><span>" + (last.volume || 0).toLocaleString() + "</span></div>";
    pats.forEach(function (p) {
      html += "<div class=cell><span>" + p.kind.replace("_", " ") + " " + p.status + "</span><span>" + p.note + "</span></div>";
    });
    if (!pats.length) html += "<div class=cell><span>double top/bottom</span><span>none in window</span></div>";
    host.innerHTML = html;
  }

  function restyleTabs() {
    try {
      if (window.TABS && Array.isArray(window.TABS)) {
        /* engine TABS is local; we only set default via URL */
      }
    } catch (e) {}
    var q = new URLSearchParams(location.search);
    if (!q.get("s") && /PEPE/i.test(location.hash || "")) {
      history.replaceState({}, "", location.pathname + "?s=SPY");
    }
  }

  restyleTabs();
  setInterval(paint, 2500);
  window.addEventListener("load", paint);
})();
