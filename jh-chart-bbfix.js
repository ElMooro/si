/* Bollinger and BTC volume. No engine edit.
   BB draws after paint when study bb is on. Non-finite closes are skipped.
   BTC volume before the warehouse join is quote dollars; from the join it is
   base coins. Scale the later side by close so one histogram is quote. */
(function () {
  if (window.__jhBbFix) return;
  window.__jhBbFix = 1;
  function finite(v) { return typeof v === "number" && isFinite(v); }
  function isBtc() {
    var el = document.getElementById && document.getElementById("symin");
    var s = String((el && el.value) || window.jhActive || window.active || window.ticker || "").toUpperCase();
    return s.indexOf("BTC") >= 0;
  }
  function align(d) {
    if (!d || d.length < 40 || d.__jhVol || !isBtc()) return d;
    var out = [], i, b;
    for (i = 0; i < d.length; i++) {
      b = d[i];
      out.push({ time: b.time, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume, value: b.value });
    }
    function med(i0, n) {
      var s = [], k, row, q;
      for (k = i0; k < i0 + n && k < out.length; k++) {
        row = out[k];
        if (row && row.close > 0 && row.volume > 0) { q = row.volume / row.close; if (isFinite(q)) s.push(q); }
      }
      if (s.length < 8) return null;
      s.sort(function (a, c) { return a - c; });
      return s[s.length >> 1];
    }
    var cut = -1, before, after;
    for (i = 30; i < out.length - 20; i++) {
      before = med(i - 30, 30);
      after = med(i, 20);
      if (before > 1000 && after != null && after < 100) { cut = i; break; }
    }
    if (cut >= 0) {
      for (i = cut; i < out.length; i++) {
        b = out[i];
        if (!b || !(b.close > 0) || !(b.volume > 0) || b.volume / b.close >= 100) continue;
        b.volume = b.volume * b.close;
      }
    }
    out.__jhVol = 1;
    return out;
  }
  function bbOn() {
    var list = window.INDS || [], i, row;
    for (i = 0; i < list.length; i++) if (list[i] && list[i].id === "bb" && list[i].on) return true;
    row = document.querySelector && document.querySelector("[data-tog='bb']");
    return !!(row && row.parentNode && row.parentNode.classList && row.parentNode.classList.contains("on"));
  }
  function bands(d) {
    var n = 20, k = 2, i, j, s, m, ss, sd, mid = [], up = [], dn = [];
    for (i = n - 1; i < d.length; i++) {
      if (!finite(d[i].close)) continue;
      s = 0;
      for (j = 0; j < n; j++) { if (!finite(d[i - j].close)) { s = NaN; break; } s += d[i - j].close; }
      if (!finite(s)) continue;
      m = s / n;
      ss = 0;
      for (j = 0; j < n; j++) ss += Math.pow(d[i - j].close - m, 2);
      sd = Math.sqrt(ss / n);
      if (!finite(sd)) continue;
      mid.push({ time: d[i].time, value: m });
      up.push({ time: d[i].time, value: m + k * sd });
      dn.push({ time: d[i].time, value: m - k * sd });
    }
    return { mid: mid, up: up, dn: dn };
  }
  function draw(d) {
    var chart = window.jhDeskChart || window.chart;
    if (!chart || !chart.addLineSeries || !bbOn()) return;
    if (chart.__jhBb) {
      chart.__jhBb.forEach(function (s) { try { chart.removeSeries(s); } catch (e) {} });
    }
    chart.__jhBb = [];
    var bb = bands(d);
    function add(pts, color, w, title) {
      if (!pts.length) return;
      try {
        var s = chart.addLineSeries({ color: color, lineWidth: w, title: title, priceLineVisible: false, lastValueVisible: title === "BB" });
        s.setData(pts);
        chart.__jhBb.push(s);
      } catch (e) {}
    }
    add(bb.mid, "#2962ff", 2, "BB");
    add(bb.up, "#26c6da", 1, "BB+");
    add(bb.dn, "#26c6da", 1, "BB-");
  }
  function arm() {
    if (!window.paint || window.paint.__bb) return false;
    var prev = window.paint;
    function wrapped(d) {
      var src = d || window.lastBars;
      var bars = align(src);
      var r;
      try { r = prev.call(this, bars); } catch (e) {
        try { r = prev.call(this, src); } catch (e2) { r = undefined; }
      }
      try { draw(bars); } catch (e3) {}
      return r;
    }
    wrapped.__bb = 1;
    window.paint = wrapped;
    return true;
  }
  var n = 0, t = setInterval(function () { if (arm() || ++n > 80) clearInterval(t); }, 50);
})();
