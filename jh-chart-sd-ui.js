/* Bind Demand / Supply buttons on the chart rail. Toolbar buttons are
   wired inside renderTf in jh-chart-engine.js; this file covers #rrail
   which is not rebuilt on every paint, and retries until the engine is up. */
(function (root) {
  if (root.__jhSdUiV1) return;
  root.__jhSdUiV1 = true;

  function show() {
    return root.__jhSdShow || { demand: false, supply: false };
  }
  function sync() {
    var s = show();
    ["btn-demand", "btn-demand2", "btn-demand-rail"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.classList.toggle("on", !!s.demand);
    });
    ["btn-supply", "btn-supply2", "btn-supply-rail"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.classList.toggle("on", !!s.supply);
    });
  }
  function bind(id, fn) {
    var el = document.getElementById(id);
    if (!el || el.__jhSdBound) return el;
    el.__jhSdBound = true;
    el.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      fn();
      sync();
    });
    return el;
  }
  function hook() {
    if (typeof root.jhToggleDemand === "function") {
      bind("btn-demand-rail", root.jhToggleDemand);
    }
    if (typeof root.jhToggleSupply === "function") {
      bind("btn-supply-rail", root.jhToggleSupply);
    }
    sync();
  }
  var n = 0;
  function wait() {
    hook();
    if (n++ < 40 && (!root.jhToggleDemand || !document.getElementById("btn-demand-rail"))) {
      setTimeout(wait, 250);
    }
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", wait);
  else wait();
  root.jhSdUiSync = sync;
})(typeof window !== "undefined" ? window : globalThis);
