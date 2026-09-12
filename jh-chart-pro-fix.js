/* jh-chart-pro-fix.js -- Chart Pro first pass: hide maintainer strip, fill macro tape */
(function () {
  if (window.__jhChartProFix) return;
  window.__jhChartProFix = true;
  function hideNote() {
    document.querySelectorAll(".ai-index-strip").forEach(function (el) { el.style.display = "none"; });
  }
  function fillTape(j) {
    var f = (j && j.fields) || {};
    var map = {
      VIX: f.vix, DXY: f.dxy_broad, US10Y: f.us10y, "US CPI": f.us_cpi, CPI: f.us_cpi
    };
    document.querySelectorAll("[data-tape], .tape-item, .mkt-chip, header, .header").forEach(function () {});
    var nodes = document.querySelectorAll("header span, .header span, .ticker, .tape span");
    nodes.forEach(function (n) {
      var t = (n.textContent || "").trim();
      Object.keys(map).forEach(function (k) {
        if (t.indexOf(k) === 0 && map[k] && map[k].value != null) {
          if (n.textContent.indexOf(String(map[k].value)) >= 0) return;
          n.textContent = k + " " + map[k].value;
        }
      });
    });
  }
  hideNote();
  fetch("/data/macro-tape.json?t=" + Date.now(), { cache: "no-store" })
    .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
    .then(fillTape)
    .catch(function () {
      fetch("https://justhodl-data-proxy.raafouis.workers.dev/data/macro-tape.json?t=" + Date.now())
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (j) { if (j) fillTape(j); });
    });
  document.addEventListener("DOMContentLoaded", hideNote);
})();
