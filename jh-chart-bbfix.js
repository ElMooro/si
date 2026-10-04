/* Bollinger safety through the engine's identified frame owner.
   Reported volume is preserved; its unit is not inferred here.
   jhBbFrame: arm Bollinger once, then open on the last 160 bars so the band is readable. */
(function () {
  if (typeof window === "undefined" || window.__jhBbFix) return;
  window.__jhBbFix = 1;
  function finite(v) { return typeof v === "number" && isFinite(v); }
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
  function frame(chart, bars, symbol, interval) {
    var key = String(symbol || "") + "|" + String(interval || "");
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
    if (!bars || bars === seen || !window.jhRepaintOwnedBars) return;
    var painted = null;
    try { painted = window.jhRepaintOwnedBars(bars, armBb, frame); } catch (e2) {}
    if (painted) seen = bars;
  }, 250);
})();
