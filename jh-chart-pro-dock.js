/* jh-chart-pro-dock.js -- warehouse rail; lists merge-only */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  ["jh-chart-search-guard", "jh-chart-series-fallback", "jh-chart-suggest", "jh-chart-internals", "jh-chart-pro-tvux", "jh-chart-tf-fix", "jh-tv-lists-bridge", "jh-chart-audit-fix"].forEach(function (name) {
    if (document.querySelector('script[src*="' + name + '"]')) return;
    var ux = document.createElement("script");
    ux.src = "/" + name + ".js?t=" + Date.now();
    ux.defer = true;
    (document.head || document.documentElement).appendChild(ux);
  });
  if (window.__jhChartDock) return;
  window.__jhChartDock = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function gj(path) {
    return fetch("/" + path + "?t=" + Date.now(), { cache: "no-store" })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
      .catch(function () {
        return fetch(PROXY + "/" + path + "?t=" + Date.now())
          .then(function (r) { return r.ok ? r.json() : null; });
      });
  }
  function cell(k, v) {
    return "<div style=\"padding:6px 0;border-bottom:1px solid #1d2636\"><div style=\"font:10px IBM Plex Mono,monospace;color:#7dd3fc;letter-spacing:1px\">" +
      k + "</div><div style=\"font:14px Inter,sans-serif;color:#e8edf5\">" + (v == null ? "\u2014" : v) + "</div></div>";
  }
  function pane(title, body) {
    return "<div style=\"margin-top:10px;padding-top:8px;border-top:1px solid #1d2636\"><div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.2px\">" + title + "</div>" + body + "</div>";
  }
  var box = document.createElement("aside");
  box.id = "jh-chart-dock";
  box.style.cssText = "position:fixed;left:8px;top:168px;z-index:20;width:200px;max-height:50vh;overflow:auto;padding:10px 12px;border:1px solid #1d2636;border-radius:10px;background:rgba(10,13,18,.94);color:#a8b3c7;font:12px Inter,sans-serif";
  var tog = document.createElement("button");
  tog.textContent = "WH";
  tog.style.cssText = "position:fixed;left:8px;top:144px;z-index:21;width:32px;height:24px;border:1px solid #1d2636;border-radius:6px;background:#0a0d12;color:#22d3ee;font:10px IBM Plex Mono,monospace;cursor:pointer";
  tog.onclick = function () { box.style.display = box.style.display === "none" ? "block" : "none"; };
  function mount() {
    if (!document.getElementById("jh-chart-dock")) { document.body.appendChild(tog); document.body.appendChild(box); }
  }
  if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);
  Promise.all([
    gj("data/plumbing-brief.json"), gj("data/official-stats-brief.json"), gj("data/verdict.json")
  ]).then(function (arr) {
    var p = arr[0] || {}, o = arr[1] || {}, v = arr[2] || {};
    var pf = p.fields || {}, of = o.fields || {};
    box.innerHTML =
      pane("PLUMBING", cell("SOFR", pf.ofr_sofr) + cell("GDPNOW", of.gdpnow) + cell("VERDICT", [v.bias || v.call, v.regime].filter(Boolean).join(" \u00b7 ")));
  });
})();
