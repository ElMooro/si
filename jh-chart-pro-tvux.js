/* jh-chart-pro-tvux.js
 * Chart-only chrome, paste-import, object tree (hide/show), row alert, tiny sparks.
 * NEVER deletes State.customWatchlists, localStorage lists, or tv-watchlists.json.
 */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhChartTvux) return;
  window.__jhChartTvux = true;

  function toast(m) { if (window.jhToast) jhToast(m); }

  function parseSymbols(text) {
    var out = [];
    String(text || "").split(/[\s,;]+/).forEach(function (t) {
      t = t.replace(/^\"|\"$/g, "").trim();
      if (t && /^[A-Za-z0-9_.:\-]+$/.test(t) && /[A-Za-z]/.test(t)) out.push(t.toUpperCase());
    });
    return out.filter(function (t, i, a) { return a.indexOf(t) === i; });
  }

  function addListNeverDelete(name, tickers) {
    if (!window.WatchlistManager || !window.State) return 0;
    if (!tickers || !tickers.length) return 0;
    State.customWatchlists = State.customWatchlists || {};
    var id = null;
    Object.keys(State.customWatchlists).forEach(function (k) {
      if (State.customWatchlists[k] && State.customWatchlists[k].name === name) id = k;
    });
    if (!id) id = WatchlistManager.createCustom(name);
    tickers.forEach(function (t) { WatchlistManager.addTicker(id, t); });
    State.activeWatchlistId = id;
    WatchlistManager.saveActiveWl();
    if (window.UI && UI.refreshWatchlist) UI.refreshWatchlist();
    return tickers.length;
  }

  var bar = document.createElement("div");
  bar.id = "jh-tvux";
  bar.style.cssText = "position:fixed;left:48px;top:56px;z-index:42;display:flex;gap:6px;align-items:center;flex-wrap:wrap";
  bar.innerHTML =
    '<button type="button" data-act="chrome" style="background:#0a0d12;color:#22d3ee;border:1px solid #1d2636;border-radius:6px;padding:3px 8px;font:10px IBM Plex Mono,monospace;cursor:pointer">chart only</button>' +
    '<button type="button" data-act="tree" style="background:#0a0d12;color:#22d3ee;border:1px solid #1d2636;border-radius:6px;padding:3px 8px;font:10px IBM Plex Mono,monospace;cursor:pointer">drawings</button>' +
    '<input data-act="paste" placeholder="paste NVDA,AAPL,FRED:DGS10 — new list" style="width:220px;background:#0a0d12;color:#e8edf5;border:1px solid #1d2636;border-radius:6px;padding:3px 6px;font:10px IBM Plex Mono,monospace"/>';
  function mount() { if (!document.getElementById("jh-tvux")) document.body.appendChild(bar); }
  if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);

  var style = document.createElement("style");
  style.textContent =
    "body.jh-chart-only #jh-nav-drawer, body.jh-chart-only .jh-nav-drawer, body.jh-chart-only [data-jh-chrome]," +
    "body.jh-chart-only .cmd-center, body.jh-chart-only #command-center { display:none !important; }" +
    "#jh-obj-tree{position:fixed;right:56px;top:88px;z-index:43;width:220px;max-height:50vh;overflow:auto;" +
    "background:rgba(10,13,18,.94);border:1px solid #1d2636;border-radius:10px;padding:10px;color:#a8b3c7;font:12px Inter,sans-serif;display:none}";
  document.head.appendChild(style);

  var tree = document.createElement("div");
  tree.id = "jh-obj-tree";
  document.addEventListener("DOMContentLoaded", function () { document.body.appendChild(tree); });
  if (document.body) document.body.appendChild(tree);

  function renderTree() {
    var d = (window.State && State.drawings) || {};
    var keys = Object.keys(d);
    tree.innerHTML = '<div style="font:10px IBM Plex Mono,monospace;color:#22d3ee;margin-bottom:6px">DRAWINGS (hide only)</div>';
    if (!keys.length) {
      tree.innerHTML += '<div>none saved</div>';
      return;
    }
    keys.forEach(function (k) {
      var row = document.createElement("label");
      row.style.cssText = "display:block;margin:4px 0;cursor:pointer";
      var on = d[k] && d[k].hidden ? "" : "checked";
      row.innerHTML = '<input type="checkbox" ' + on + ' data-k="' + k.replace(/"/g, "") + '"> ' + k;
      row.querySelector("input").addEventListener("change", function (ev) {
        if (!State.drawings[k]) return;
        State.drawings[k].hidden = !ev.target.checked;
        if (WatchlistManager && WatchlistManager.syncToCloud) WatchlistManager.syncToCloud();
      });
      tree.appendChild(row);
    });
  }

  bar.addEventListener("click", function (e) {
    var act = e.target && e.target.getAttribute("data-act");
    if (act === "chrome") document.body.classList.toggle("jh-chart-only");
    if (act === "tree") {
      tree.style.display = tree.style.display === "block" ? "none" : "block";
      renderTree();
    }
  });
  bar.addEventListener("keydown", function (e) {
    if (e.target.getAttribute("data-act") !== "paste" || e.key !== "Enter") return;
    var syms = parseSymbols(e.target.value);
    if (!syms.length) return;
    var n = addListNeverDelete("Paste " + new Date().toISOString().slice(0, 16), syms);
    toast("added list with " + n + " symbols");
    e.target.value = "";
  });

  function wireRows() {
    var body = document.getElementById("watchlist-body");
    if (!body || body.dataset.jhAlert) return;
    body.dataset.jhAlert = "1";
    body.addEventListener("click", function (ev) {
      var row = ev.target.closest("[data-ticker], .wl-row, .watchlist-row");
      if (!row) return;
      if (ev.target.closest("[data-alert]")) {
        var t = row.getAttribute("data-ticker") || row.dataset.ticker;
        var btn = document.getElementById("alert-builder-btn");
        if (t && window.State) State.activeTicker = t;
        if (btn) btn.click();
      }
    });
  }
  setInterval(wireRows, 1500);
})();
