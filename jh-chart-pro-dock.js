/* jh-chart-pro-dock.js -- left rail of warehouse numbers on chart-pro only */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhChartDock) return;
  window.__jhChartDock = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function gj(path) {
    return fetch("/" + path + "?t=" + Date.now(), { cache: "no-store" })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
      .catch(function () {
        return fetch(PROXY + "/" + path + "?t=" + Date.now(), { cache: "no-store" })
          .then(function (r) { return r.ok ? r.json() : null; });
      });
  }
  function cell(k, v) {
    return "<div style=\"padding:6px 0;border-bottom:1px solid #1d2636\"><div style=\"font:10px IBM Plex Mono,monospace;color:#7dd3fc;letter-spacing:1px\">" +
      k + "</div><div style=\"font:14px Inter,sans-serif;color:#e8edf5\">" + (v == null ? "\u2014" : v) + "</div></div>";
  }
  var box = document.createElement("aside");
  box.id = "jh-chart-dock";
  box.style.cssText = "position:fixed;left:10px;top:88px;z-index:40;width:200px;max-height:72vh;overflow:auto;padding:10px 12px;border:1px solid #1d2636;border-radius:10px;background:rgba(10,13,18,.94);color:#a8b3c7;font:12px Inter,sans-serif";
  box.innerHTML = "<div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.4px;margin-bottom:8px\">WAREHOUSE</div>loading\u2026";
  var tog = document.createElement("button");
  tog.textContent = "WH";
  tog.title = "Warehouse dock";
  tog.style.cssText = "position:fixed;left:10px;top:56px;z-index:41;width:32px;height:24px;border:1px solid #1d2636;border-radius:6px;background:#0a0d12;color:#22d3ee;font:10px IBM Plex Mono,monospace;cursor:pointer";
  tog.onclick = function () {
    box.style.display = box.style.display === "none" ? "block" : "none";
  };
  function mount() {
    if (!document.getElementById("jh-chart-dock")) {
      document.body.appendChild(tog);
      document.body.appendChild(box);
    }
  }
  if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);
  Promise.all([
    gj("data/plumbing-brief.json"),
    gj("data/official-stats-brief.json"),
    gj("data/ofr-funding.json"),
    gj("data/verdict.json"),
    gj("data/positioning-brief.json"),
    gj("data/alfred-vintages.json")
  ]).then(function (arr) {
    var p = arr[0] || {}, o = arr[1] || {}, f = arr[2] || {}, v = arr[3] || {}, pos = arr[4] || {}, al = arr[5] || {};
    var pf = p.fields || {}, of = o.fields || {}, ps = pos.fields || {};
    var sofr = pf.ofr_sofr != null ? pf.ofr_sofr : (f.sofr && f.sofr.value);
    var alfredN = (al.series || []).reduce(function (n, s) { return n + (s.n_vintages_banked || 0); }, 0);
    box.innerHTML =
      "<div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.4px;margin-bottom:8px\">WAREHOUSE</div>" +
      cell("PLUMBING", (pf.composite_label || "") + " " + (pf.composite_score != null ? pf.composite_score : "")) +
      cell("SOFR", sofr) +
      cell("TRIPARTY", pf.ofr_triparty_rate) +
      cell("GDPNOW", of.gdpnow) +
      cell("T10Y3M", of.t10y3m) +
      cell("INST BREADTH", [ps.accumulating, ps.distributing, ps.flat].filter(function (x) { return x != null; }).join(" / ")) +
      cell("CFTC ROWS", ps.cftc_rows) +
      cell("ALFRED", alfredN ? (alfredN + " vintages") : null) +
      cell("VERDICT", [v.bias || v.call, v.regime].filter(Boolean).join(" \u00b7 "));
  });
})();
