/* jh-tv-lists-bridge.js -- MY TV WATCHLISTS already loaded; fill the empty ALERT body from the selected list. Merge-only. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhTvListsBridge) return;
  window.__jhTvListsBridge = true;
  function tickersFromUi() {
    return Array.prototype.map.call(document.querySelectorAll("#jhwl-ul .jhwl-s"), function (n) {
      return (n.textContent || "").trim();
    }).filter(Boolean);
  }
  function listName() {
    var sel = document.getElementById("jhwl-sel");
    if (!sel || sel.selectedIndex < 0) return "TV list";
    return (sel.options[sel.selectedIndex].text || "TV list").replace(/\s*\(\d+\)\s*$/, "");
  }
  function promote() {
    if (!window.WatchlistManager || !window.State) return;
    var tickers = tickersFromUi();
    if (!tickers.length) return;
    var name = listName();
    var id = null;
    Object.keys(State.customWatchlists || {}).forEach(function (k) {
      if (State.customWatchlists[k] && State.customWatchlists[k].name === name) id = k;
    });
    if (!id) id = WatchlistManager.createCustom(name);
    tickers.forEach(function (t) { WatchlistManager.addTicker(id, t); });
    State.activeWatchlistId = id;
    WatchlistManager.saveActiveWl();
    if (window.UI && typeof UI.refreshWatchlist === "function") UI.refreshWatchlist();
  }
  function hook() {
    var sel = document.getElementById("jhwl-sel");
    if (!sel) return false;
    if (sel.dataset.jhBridge) return true;
    sel.dataset.jhBridge = "1";
    sel.addEventListener("change", function () { setTimeout(promote, 50); });
    promote();
    return true;
  }
  var n = 0;
  var t = setInterval(function () {
    if (hook() || ++n > 40) clearInterval(t);
  }, 400);
})();
