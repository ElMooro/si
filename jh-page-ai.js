/* jh-page-ai.js — universal per-page AI panel + chart-pro overlays */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (!document.querySelector('script[src*="jh-chart-pro-dock"]')) {
    var s = document.createElement("script");
    s.src = "/jh-chart-pro-dock.js?t=" + Date.now();
    s.defer = true;
    document.head.appendChild(s);
  }
  function hide() {
    document.querySelectorAll(".ai-index-strip").forEach(function (el) { el.style.display = "none"; });
    var rows = [];
    document.querySelectorAll("div, nav, span").forEach(function (el) {
      var t = (el.textContent || "").replace(/\s+/g, " ").trim();
      if (el.children && el.children.length > 8) return;
      if (/^1D 5D 1M 3M 6M YTD 1Y 5Y All$/.test(t)) rows.push(el);
    });
    if (rows.length) rows[0].style.display = "none";
  }
  hide();
  document.addEventListener("DOMContentLoaded", hide);
  setTimeout(hide, 1200);
  fetch("/data/macro-tape.json?t=" + Date.now(), { cache: "no-store" })
    .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
    .catch(function () {
      return fetch("https://justhodl-data-proxy.raafouis.workers.dev/data/macro-tape.json?t=" + Date.now())
        .then(function (r) { return r.ok ? r.json() : null; });
    })
    .then(function (j) {
      if (!j || !j.fields) return;
      var f = j.fields;
      var want = { VIX: f.vix, DXY: f.dxy_broad, US10Y: f.us10y, "US CPI": f.us_cpi };
      document.querySelectorAll("span, a, div").forEach(function (n) {
        if (n.children && n.children.length) return;
        var t = (n.textContent || "").replace(/\s+/g, " ").trim();
        Object.keys(want).forEach(function (k) {
          if (!want[k] || t.indexOf(k) !== 0) return;
          if (/\d/.test(t) && t.length > k.length + 2) return;
          n.textContent = k + " " + want[k].value;
        });
      });
    });
  fetch("/data/verdict.json?t=" + Date.now(), { cache: "no-store" })
    .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
    .catch(function () {
      return fetch("https://justhodl-data-proxy.raafouis.workers.dev/data/verdict.json?t=" + Date.now())
        .then(function (r) { return r.ok ? r.json() : null; });
    })
    .then(function (v) {
      if (!v) return;
      var label = [v.bias || v.call, v.regime, v.coverage != null ? ("cov " + v.coverage) : ""]
        .filter(Boolean).join(" · ");
      document.querySelectorAll("div, span").forEach(function (n) {
        var t = (n.textContent || "").trim();
        if (t === "Live Signals: No active JustHodl signals" || t === "No active JustHodl signals") {
          n.textContent = "Live Signals: " + label;
        }
      });
    });
})();
