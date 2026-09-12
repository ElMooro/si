/* jh-chart-tf-fix.js -- TF + distinct % modes + 52w high/low. No watchlist deletes. */
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
  function medSpacing(series) {
    var spacings = [];
    for (var k = 1; k < series.length; k++) spacings.push(series[k].time - series[k - 1].time);
    spacings.sort(function (a, b) { return a - b; });
    var med = spacings.length ? spacings[Math.floor(spacings.length / 2)] : 86400;
    return med < 1 ? 86400 : med;
  }
  function winBars(series, calendarDays) {
    return Math.max(2, Math.round((calendarDays * 86400) / medSpacing(series)));
  }
  function patchChange() {
    if (!window.NativeChart || NativeChart.__jhChangePatched) return !!window.NativeChart;
    NativeChart.__jhChangePatched = true;
    NativeChart.CHANGE_LABELS = NativeChart.CHANGE_LABELS || {};
    NativeChart.CHANGE_LABELS.fromhigh = "Drawdown from 52w high";
    NativeChart.CHANGE_LABELS.fromlow = "Rally from 52w low";
    NativeChart.computeChange = function (series, mode) {
      if (!series || series.length < 2) return [];
      if (mode === "ytd") {
        var out = [], yearStart = {};
        for (var i = 0; i < series.length; i++) {
          var p = series[i];
          var yr = new Date(p.time * 1000).getUTCFullYear();
          if (yearStart[yr] == null) yearStart[yr] = p.value;
          if (yearStart[yr]) out.push({ time: p.time, value: (p.value / yearStart[yr] - 1) * 100 });
        }
        return out;
      }
      if (mode === "fromhigh" || mode === "fromlow") {
        var w = winBars(series, 365);
        var outH = [];
        for (var i2 = 0; i2 < series.length; i2++) {
          var ext = mode === "fromhigh" ? -Infinity : Infinity;
          var from = Math.max(0, i2 - w + 1);
          for (var j = from; j <= i2; j++) {
            var v = series[j].value;
            if (mode === "fromhigh") { if (v > ext) ext = v; }
            else { if (v < ext) ext = v; }
          }
          if (ext && isFinite(ext) && ext !== 0) outH.push({ time: series[i2].time, value: (series[i2].value / ext - 1) * 100 });
        }
        return outH;
      }
      var days = { dod: 1, wow: 7, mom: 30.44, qoq: 91.31, yoy: 365 }[mode];
      if (!days) return [];
      var n = mode === "dod" ? 1 : Math.max(1, Math.round((days * 86400) / medSpacing(series)));
      var out3 = [];
      for (var i3 = n; i3 < series.length; i3++) {
        var then = series[i3 - n].value, now = series[i3].value;
        if (!then) continue;
        out3.push({ time: series[i3].time, value: (now / then - 1) * 100 });
      }
      return out3;
    };
    return true;
  }
  function addFromLow() {
    var fh = document.querySelector('.chg-btn[data-chg="fromhigh"]');
    if (!fh || document.querySelector('.chg-btn[data-chg="fromlow"]')) return;
    var b = document.createElement("button");
    b.className = "chg-btn";
    b.setAttribute("data-chg", "fromlow");
    b.title = "% above the 52-week low";
    b.textContent = "From Low";
    fh.after(b);
    b.addEventListener("click", function () {
      document.querySelectorAll(".chg-btn").forEach(function (x) { x.classList.remove("active"); });
      b.classList.add("active");
      if (window.State) {
        State.changeMode = "fromlow";
        useNative();
        reload();
      }
    });
  }
  function wire() {
    patchChange();
    addFromLow();
    document.querySelectorAll(".tf-btn[data-span]").forEach(function (b) {
      if (b.dataset.jhTf) return;
      b.dataset.jhTf = "1";
      b.addEventListener("click", function (e) {
        e.stopImmediatePropagation();
        State.tf = { mult: parseInt(b.dataset.mult, 10), span: b.dataset.span, days: parseInt(b.dataset.days, 10) };
        markTf(b); markCt();
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
    markCt();
  }
  var n = 0;
  var t = setInterval(function () {
    n++;
    if (patchChange() || n > 20) { wire(); clearInterval(t); }
  }, 300);
})();
