/* Bollinger safety and BTC volume. No engine edit.
   alignBeforePaint: scale the coin side before paint reads the bars.
   jhBbFrame: arm Bollinger once, then open on the last 160 bars so the band is readable. */
(function () {
  if (typeof window === "undefined" || window.__jhBbFix) return;
  window.__jhBbFix = 1;
  function finite(v) { return typeof v === "number" && isFinite(v); }
  function isBtc() {
    var el = document.getElementById("symin");
    var s = String(window.jhActive || window.active || (el && el.value) || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
    return s.indexOf("BTC") === 0 || s.indexOf("XBTC") >= 0;
  }
  function align(d) {
    if (!d || d.length < 40 || d.__jhVol || !isBtc()) return;
    function med(arr) {
      if (!arr || arr.length < 8) return null;
      var s = arr.slice().sort(function (a, c) { return a - c; });
      return s[s.length >> 1];
    }
    function ratios(i0, n) {
      var s = [], k, row, q;
      for (k = i0; k < i0 + n && k < d.length; k++) {
        row = d[k];
        if (row && row.close > 0 && row.volume > 0) {
          q = row.volume / row.close;
          if (finite(q)) s.push(q);
        }
      }
      return med(s);
    }
    function vols(i0, n) {
      var s = [], k, row;
      for (k = i0; k < i0 + n && k < d.length; k++) {
        row = d[k];
        if (row && row.volume > 0) s.push(row.volume);
      }
      return med(s);
    }
    var dollar = [], i, b, q, cut = -1, before, after, pre, post, factor;
    for (i = 0; i < d.length; i++) {
      b = d[i];
      q = b && b.close > 0 && b.volume > 0 ? b.volume / b.close : 0;
      dollar[i] = q >= 100;
    }
    for (i = 30; i < d.length - 20; i++) {
      before = ratios(i - 30, 30);
      after = ratios(i, 20);
      if (before > 1000 && after != null && after < 100) { cut = i; break; }
    }
    if (cut >= 0) {
      for (i = cut; i < d.length; i++) {
        b = d[i];
        if (dollar[i] || !b || !(b.close > 0) || !(b.volume > 0)) continue;
        b.volume = b.volume * b.close;
      }
      pre = vols(Math.max(0, cut - 40), Math.min(40, cut));
      post = vols(cut, 40);
      if (pre > 0 && post > 0 && pre > post * 3) {
        factor = post / pre;
        for (i = 0; i < d.length; i++) {
          if (!dollar[i] || !(d[i].volume > 0)) continue;
          d[i].volume = d[i].volume * factor;
        }
      }
    }
    d.__jhVol = 1;
  }
  function sane(data) {
    if (!data || !data.length) return data || [];
    var out = [], i, p, last = null;
    for (i = 0; i < data.length; i++) {
      p = data[i];
      if (!p || typeof p.time !== "number" || !finite(p.time)) continue;
      if (p.value != null && !finite(p.value)) continue;
      if (last != null && !(p.time > last)) continue;
      out.push(p);
      last = p.time;
    }
    return out;
  }
  function guardSeries(s) {
    if (!s || !s.setData || s.setData.__jh) return;
    var sd = s.setData.bind(s);
    function setData(data) {
      try { return sd(data); }
      catch (e) { try { return sd(sane(data)); } catch (e2) {} }
    }
    setData.__jh = 1;
    s.setData = setData;
  }
  function hook(chart) {
    if (!chart || chart.__jhBbHook || !chart.addLineSeries) return false;
    chart.__jhBbHook = 1;
    ["addLineSeries", "addHistogramSeries"].forEach(function (name) {
      if (!chart[name]) return;
      var fn = chart[name].bind(chart);
      chart[name] = function () {
        var s = fn.apply(chart, arguments);
        guardSeries(s);
        return s;
      };
    });
    return true;
  }
  function armBb() {
    var inds = window.INDS, i, bb = null, armed = false;
    if (!inds) return;
    for (i = 0; i < inds.length; i++) if (inds[i] && inds[i].id === "bb") bb = inds[i];
    if (!bb) return;
    try { armed = localStorage.getItem("jh-bb-armed") === "1"; } catch (e) {}
    if (armed) return;
    bb.on = true;
    bb.hide = false;
    try { localStorage.setItem("jh-bb-armed", "1"); } catch (e2) {}
    try { if (window.jhSaveLay) window.jhSaveLay(); } catch (e3) {}
  }
  var framedKey = "";
  function frame(chart) {
    var bars = window.lastBars;
    var key = String(window.jhActive || "") + "|" + String(window.tf || "");
    if (!chart || !chart.timeScale || !bars || bars.length < 40 || framedKey === key) return;
    framedKey = key;
    try {
      chart.timeScale().setVisibleLogicalRange({
        from: Math.max(-0.5, bars.length - 160),
        to: bars.length + 5
      });
    } catch (e) {}
  }
  var seen = null;
  setInterval(function () {
    var chart = window.jhDeskChart || window.chart;
    hook(chart);
    var bars = window.lastBars;
    if (!bars || bars === seen || !window.paint) return;
    try { armBb(); } catch (e0) {}
    try { align(bars); } catch (e) {}
    seen = bars;
    var painted = null;
    try { painted = window.paint(bars); } catch (e2) {}
    if (painted && painted.then) painted.then(function () { frame(window.jhDeskChart || window.chart); }, function () {});
    else frame(chart);
  }, 250);
})();
