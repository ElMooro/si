/* jh-chart-tf-fix.js -- 1D/5Y were both active (same span+mult). Do not touch watchlists. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhTfFix) return;
  window.__jhTfFix = true;

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
  function onTfClick(b) {
    if (!b.dataset.span || !window.State) return;
    State.tf = {
      mult: parseInt(b.dataset.mult, 10),
      span: b.dataset.span,
      days: parseInt(b.dataset.days, 10)
    };
    markTf(b);
    markCt();
    if ((b.dataset.span === "minute" || b.dataset.span === "hour") && State.chartEngine === "tv") {
      State.chartEngine = "native";
      document.querySelectorAll(".ceng-btn[data-engine]").forEach(function (x) {
        x.classList.toggle("active", x.dataset.engine === "native");
      });
    }
    var t = State.activeTicker;
    State.activeTicker = "__r";
    if (window.ChartController) ChartController.loadTicker(t);
  }
  function wire() {
    document.querySelectorAll(".tf-btn[data-span]").forEach(function (b) {
      if (b.dataset.jhTf) return;
      b.dataset.jhTf = "1";
      b.addEventListener("click", function (e) {
        e.stopImmediatePropagation();
        onTfClick(b);
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
