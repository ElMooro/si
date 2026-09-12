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
      k + "</div><div style=\"font:14px Inter,sans-serif;color:#e8edf5\">" + (v == null ? "—" : v) + "</div></div>";
  }
  var box = document.createElement("aside");
  box.id = "jh-chart-dock";
  box.style.cssText = "position:fixed;left:10px;top:88px;z-index:40;width:196px;max-height:70vh;overflow:auto;padding:10px 12px;border:1px solid #1d2636;border-radius:10px;background:rgba(10,13,18,.92);color:#a8b3c7;font:12px Inter,sans-serif";
  box.innerHTML = "<div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.4px;margin-bottom:8px\">WAREHOUSE</div>loading…";
  function mount() {
    if (!document.getElementById("jh-chart-dock")) document.body.appendChild(box);
  }
  if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);
  Promise.all([
    gj("data/plumbing-brief.json"),
    gj("data/official-stats-brief.json"),
    gj("data/ofr-funding.json"),
    gj("data/verdict.json")
  ]).then(function (arr) {
    var p = arr[0] || {}, o = arr[1] || {}, f = arr[2] || {}, v = arr[3] || {};
    var pf = p.fields || {}, of = o.fields || {};
    var sofr = pf.ofr_sofr != null ? pf.ofr_sofr : (f.sofr && f.sofr.value);
    box.innerHTML =
      "<div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.4px;margin-bottom:8px\">WAREHOUSE</div>" +
      cell("PLUMBING", (pf.composite_label || "") + " " + (pf.composite_score != null ? pf.composite_score : "")) +
      cell("SOFR", sofr) +
      cell("TRIPARTY", pf.ofr_triparty_rate) +
      cell("GDPNOW", of.gdpnow) +
      cell("T10Y3M", of.t10y3m) +
      cell("VERDICT", [v.bias || v.call, v.regime].filter(Boolean).join(" · "));
  });
})();
