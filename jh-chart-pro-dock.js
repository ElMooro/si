/* jh-chart-pro-dock.js -- warehouse rail + merge-only TV list import */
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
  function pane(title, body) {
    return "<div style=\"margin-top:10px;padding-top:8px;border-top:1px solid #1d2636\"><div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.2px\">" + title + "</div>" + body + "</div>";
  }
  function parseTvList(text) {
    var out = [];
    String(text || "").split(/\r?\n/).forEach(function (line) {
      line = line.trim();
      if (!line || line[0] === "#" || /^watchlist/i.test(line)) return;
      var tok = line.split(/[,;\t ]+/).filter(Boolean);
      tok.forEach(function (t) {
        t = t.replace(/^\"|\"$/g, "");
        if (/^[A-Za-z0-9_.:\-]+$/.test(t) && /[A-Za-z]/.test(t)) out.push(t.toUpperCase());
      });
    });
    return out.filter(function (t, i, a) { return a.indexOf(t) === i; });
  }
  function importTv(text, name) {
    var tickers = parseTvList(text);
    if (!tickers.length || !window.WatchlistManager) return 0;
    var id = WatchlistManager.createCustom(name || ("TV import " + new Date().toISOString().slice(0, 10)));
    tickers.forEach(function (t) { WatchlistManager.addTicker(id, t); });
    if (typeof WatchlistManager.render === "function") WatchlistManager.render();
    return tickers.length;
  }
  var box = document.createElement("aside");
  box.id = "jh-chart-dock";
  box.style.cssText = "position:fixed;left:10px;top:88px;z-index:40;width:200px;max-height:72vh;overflow:auto;padding:10px 12px;border:1px solid #1d2636;border-radius:10px;background:rgba(10,13,18,.94);color:#a8b3c7;font:12px Inter,sans-serif";
  var tog = document.createElement("button");
  tog.textContent = "WH";
  tog.style.cssText = "position:fixed;left:10px;top:56px;z-index:41;width:32px;height:24px;border:1px solid #1d2636;border-radius:6px;background:#0a0d12;color:#22d3ee;font:10px IBM Plex Mono,monospace;cursor:pointer";
  tog.onclick = function () { box.style.display = box.style.display === "none" ? "block" : "none"; };
  function mount() {
    if (!document.getElementById("jh-chart-dock")) { document.body.appendChild(tog); document.body.appendChild(box); }
  }
  if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);
  Promise.all([
    gj("data/plumbing-brief.json"), gj("data/official-stats-brief.json"), gj("data/ofr-funding.json"),
    gj("data/verdict.json"), gj("data/positioning-brief.json"), gj("data/alfred-vintages.json"), gj("data/market-tape-brief.json")
  ]).then(function (arr) {
    var p = arr[0] || {}, o = arr[1] || {}, f = arr[2] || {}, v = arr[3] || {}, pos = arr[4] || {}, al = arr[5] || {}, mt = arr[6] || {};
    var pf = p.fields || {}, of = o.fields || {}, ps = pos.fields || {}, mf = mt.fields || {};
    var sofr = pf.ofr_sofr != null ? pf.ofr_sofr : (f.sofr && f.sofr.value);
    var nCustom = 0;
    try { nCustom = Object.keys((window.State && State.customWatchlists) || {}).length; } catch (e) {}
    box.innerHTML =
      pane("PLUMBING", cell("SOFR", sofr) + cell("GDPNOW", of.gdpnow) + cell("VERDICT", [v.bias || v.call, v.regime].filter(Boolean).join(" \u00b7 "))) +
      pane("FLOW", cell("ETF", [mf.heavy_inflow_n, mf.heavy_outflow_n].join(" / ")) + cell("INST", [ps.accumulating, ps.distributing].join(" / "))) +
      pane("LISTS", cell("CUSTOM LISTS", nCustom) + "<div style=\"font:10px Inter,sans-serif;color:#6b7480;margin:6px 0\">Not deleted. Open right-rail WATCHLIST. Sign in to sync. Import TV .txt merges a new list.</div><input id=\"jh-tv-import\" type=\"file\" accept=\".txt,.csv\" style=\"width:100%;font-size:10px\"/>") +
      "<input id=\"jh-chart-q\" placeholder=\"search / jump\" style=\"width:100%;margin-top:8px;background:#0a0d12;color:#e8edf5;border:1px solid #1d2636;border-radius:6px;padding:4px\"/>";
    var fi = document.getElementById("jh-tv-import");
    if (fi) fi.addEventListener("change", function () {
      var file = fi.files && fi.files[0]; if (!file) return;
      var reader = new FileReader();
      reader.onload = function () {
        var n = importTv(String(reader.result || ""), file.name.replace(/\.[^.]+$/, ""));
        if (window.jhToast) jhToast("imported " + n + " symbols");
      };
      reader.readAsText(file);
    });
    var q = document.getElementById("jh-chart-q");
    if (q) q.addEventListener("input", function () {
      var src = document.querySelector(".universe-search input, #dx-search, input[placeholder*='search' i]");
      if (!src) return;
      src.value = q.value;
      src.dispatchEvent(new Event("input", { bubbles: true }));
      src.focus();
    });
  });
})();
