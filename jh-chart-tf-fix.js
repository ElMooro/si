/* jh-chart-tf-fix.js -- TF active + distinct % change modes. No watchlist deletes. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhTfFix) return;
  window.__jhTfFix = true;

  function useNative() {
    if (!window.State) return;
    if (State.chartEngine === "native") return;
    State.chartEngine = "native";
    document.querySelectorAll(".ceng-btn[data-engine]").forEach(function (x) {
      x.classList.toggle("active", x.dataset.engine === "native");
    });
  }
  function markTf(btn) {
    document.querySelectorAll(".tf-btn[data-span]").forEach(function (x) {
      var on = x.dataset.span === btn.dataset.span &&
        x.dataset.mult === btn.dataset.mult &&
        String(x.dataset.days || "") === String(btn.dataset.days || "");
      x.classList.toggle("active", on);
    });
  }
  function markCt() {
    var ct = (window.State && State.chartType) || localStorage.getItem("jh_chart_type") || "candles";
    document.querySelectorAll(".ct-btn").forEach(function (x) {
      x.classList.toggle("active", x.dataset.ct === ct);
    });
  }
  function reload() {
    if (!window.State || !window.ChartController) return;
    var t = State.activeTicker;
    State.activeTicker = "__r";
    ChartController.loadTicker(t);
  }
  function patchChange() {
    if (!window.NativeChart || NativeChart.__jhChangePatched) return !!window.NativeChart;
    NativeChart.__jhChangePatched = true;
    NativeChart.computeChange = function (series, mode) {
      if (!series || series.length < 2) return [];
      if (mode === "ytd") {
        var out = [], yearStart = {};
        for (var i = 0; i < series.length; i++) {
          var p = series[i];
          var yr = new Date(p.time * 1000).getUTCFullYear();
          if (yearStart[yr] == null) yearStart[yr] = p.value;
          var base = yearStart[yr];
          if (base && isFinite(base)) out.push({ time: p.time, value: (p.value / base - 1) * 100 });
        }
        return out;
      }
      if (mode === "fromhigh") {
        var out2 = [], win = 252;
        for (var i2 = 0; i2 < series.length; i2++) {
          var hi = -Infinity;
          for (var j = Math.max(0, i2 - win + 1); j <= i2; j++) if (series[j].value > hi) hi = series[j].value;
          if (hi > 0) out2.push({ time: series[i2].time, value: (series[i2].value / hi - 1) * 100 });
        }
        return out2;
      }
      var days = { dod: 1, wow: 7, mom: 30.44, qoq: 91.31, yoy: 365 }[mode];
      if (!days) return [];
      var spacings = [];
      for (var k = 1; k < series.length; k++) spacings.push(series[k].time - series[k - 1].time);
      spacings.sort(function (a, b) { return a - b; });
      var med = spacings.length ? spacings[Math.floor(spacings.length / 2)] : 86400;
      if (med < 1) med = 86400;
      var n = mode === "dod" ? 1 : Math.max(1, Math.round((days * 86400) / med));
      var out3 = [];
      for (var i3 = n; i3 < series.length; i3++) {
        var then = series[i3 - n].value, now = series[i3].value;
        if (!then || then === 0 || now == null) continue;
        out3.push({ time: series[i3].time, value: (now / then - 1) * 100 });
      }
      return out3;
    };
    return true;
  }
  function wire() {
    patchChange();
    document.querySelectorAll(".tf-btn[data-span]").forEach(function (b) {
      if (b.dataset.jhTf) return;
      b.dataset.jhTf = "1";
      b.addEventListener("click", function (e) {
        e.stopImmediatePropagation();
        State.tf = { mult: parseInt(b.dataset.mult, 10), span: b.dataset.span, days: parseInt(b.dataset.days, 10) };
        markTf(b);
        markCt();
        if (b.dataset.span === "minute" || b.dataset.span === "hour") useNative();
        reload();
      }, true);
    });
    document.querySelectorAll(".chg-btn").forEach(function (b) {
      if (b.dataset.jhChg) return;
      b.dataset.jhChg = "1";
      b.addEventListener("click", function () {
        if (b.dataset.chg && b.dataset.chg !== "price") useNative();
      }, true);
    });
    document.querySelectorAll(".ct-btn").forEach(function (b) {
      if (b.dataset.jhCt) return;
      b.dataset.jhCt = "1";
      b.addEventListener("click", function () {
        setTimeout(markCt, 0);
        useNative();
      }, true);
    });
    markCt();
  }
  var n = 0;
  var t = setInterval(function () {
    if (patchChange() && ++n > 2) { wire(); clearInterval(t); }
    if (++n > 25) { wire(); clearInterval(t); }
  }, 300);
})();
