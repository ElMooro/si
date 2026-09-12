/* jh-chart-tf-fix.js -- 1D vs 5Y + change modes on native. No watchlist deletes. */
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
  function wire() {
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
    var cur = window.State && State.tf;
    if (cur) {
      document.querySelectorAll(".tf-btn[data-span]").forEach(function (x) {
        var on = x.dataset.span === cur.span && x.dataset.mult == String(cur.mult) && String(x.dataset.days) === String(cur.days);
        x.classList.toggle("active", on);
      });
    }
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", function () { setTimeout(wire, 800); });
  else setTimeout(wire, 800);
})();
