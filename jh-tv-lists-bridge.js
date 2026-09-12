/* jh-tv-lists-bridge.js -- filter MY TV WATCHLISTS + fill ALERT body. Merge-only. */
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
  function addFilter(sel) {
    if (document.getElementById("jhwl-filter")) return;
    var inp = document.createElement("input");
    inp.id = "jhwl-filter";
    inp.placeholder = "filter 492 lists or symbols…";
    inp.style.cssText = "width:100%;margin:0 0 6px;background:#0f1420;color:#e6edf3;border:1px solid #1c2433;border-radius:6px;padding:6px;font:11px IBM Plex Mono,monospace";
    sel.parentNode.insertBefore(inp, sel);
    var opts = Array.prototype.map.call(sel.options, function (o) {
      return { value: o.value, text: o.text, lower: o.text.toLowerCase() };
    });
    inp.addEventListener("input", function () {
      var q = inp.value.toLowerCase().trim();
      sel.innerHTML = opts.filter(function (o) { return !q || o.lower.indexOf(q) !== -1; })
        .map(function (o) { return "<option value=\"" + o.value + "\">" + o.text + "</option>"; }).join("");
      if (sel.options.length) { sel.selectedIndex = 0; sel.dispatchEvent(new Event("change")); }
    });
  }
  function hook() {
    var sel = document.getElementById("jhwl-sel");
    if (!sel || !sel.options.length) return false;
    if (sel.dataset.jhBridge) return true;
    sel.dataset.jhBridge = "1";
    addFilter(sel);
    sel.addEventListener("change", function () { setTimeout(promote, 50); });
    promote();
    return true;
  }
  var n = 0;
  var t = setInterval(function () {
    if (hook() || ++n > 40) clearInterval(t);
  }, 400);
})();
