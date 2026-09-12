/* jh-chart-audit-fix.js
 * Remaining chart-pro malfunctions. NEVER replaces customWatchlists with {}.
 */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhChartAudit) return;
  window.__jhChartAudit = true;

  function toast(m) { try { if (window.jhToast) jhToast(m); else if (window.__jhToast) __jhToast(m); } catch (e) {} }

  function mergeLists(local, remote) {
    var out = {};
    function eat(src) {
      if (!src || typeof src !== "object") return;
      Object.keys(src).forEach(function (id) {
        var wl = src[id];
        if (!wl || typeof wl !== "object") return;
        if (!out[id]) {
          out[id] = { name: wl.name || id, color: wl.color, tickers: (wl.tickers || []).slice() };
          return;
        }
        var have = {};
        (out[id].tickers || []).forEach(function (t) { have[t] = 1; });
        (wl.tickers || []).forEach(function (t) { if (!have[t]) out[id].tickers.push(t); });
        if (wl.name && !out[id].name) out[id].name = wl.name;
      });
    }
    eat(local); eat(remote);
    return out;
  }

  function patchCloud() {
    if (!window.WatchlistManager || WatchlistManager.__jhMerge) return !!window.WatchlistManager;
    WatchlistManager.__jhMerge = true;
    var orig = WatchlistManager.loadFromCloud.bind(WatchlistManager);
    WatchlistManager.loadFromCloud = async function () {
      var local = {};
      try { local = JSON.parse(JSON.stringify(State.customWatchlists || {})); } catch (e) { local = State.customWatchlists || {}; }
      var ok = await orig();
      var remote = State.customWatchlists || {};
      var remoteEmpty = !remote || !Object.keys(remote).length;
      if (remoteEmpty && Object.keys(local).length) {
        State.customWatchlists = local;
      } else {
        State.customWatchlists = mergeLists(local, remote);
      }
      if (WatchlistManager.saveLocal) WatchlistManager.saveLocal();
      if (window.UI && UI.refreshWatchlist) UI.refreshWatchlist();
      return ok;
    };
    return true;
  }

  function hideDualRange() {
    var bar = document.getElementById("tv-rangebar");
    if (bar) bar.style.display = "none";
  }

  function moveDock() {
    var dock = document.getElementById("jh-chart-dock");
    var tog = document.querySelector("#jh-chart-dock") ? document.querySelector('button[title="Warehouse dock"], button') : null;
    if (dock) {
      dock.style.top = "120px";
      dock.style.left = "8px";
      dock.style.zIndex = "25";
    }
    var tvux = document.getElementById("jh-tvux");
    if (tvux) { tvux.style.top = "auto"; tvux.style.bottom = "16px"; tvux.style.left = "8px"; }
    var wh = document.querySelectorAll("body > button");
    wh.forEach(function (b) {
      if (b.textContent === "WH") { b.style.top = "96px"; b.style.left = "8px"; }
    });
  }

  function labelFredQuotes() {
    document.querySelectorAll("#watchlist-body [data-ticker], #watchlist-body .wl-row").forEach(function (row) {
      var t = row.getAttribute("data-ticker") || row.dataset.ticker || "";
      if (!/^(FRED|ECONOMICS|CBOE|UST):/i.test(t) && t.indexOf(":") < 0) return;
      row.querySelectorAll(".wl-last, .wl-chg, [data-col=\"last\"], [data-col=\"chg\"]").forEach(function (c) {
        if (!c.textContent || c.textContent === "—" || c.textContent === "-") c.textContent = "n/a";
      });
    });
  }

  function warnTf() {
    document.querySelectorAll(".tf-btn[data-span=\"minute\"], .tf-btn[data-span=\"hour\"]").forEach(function (b) {
      if (b.dataset.jhWarn) return;
      b.dataset.jhWarn = "1";
      b.addEventListener("click", function () {
        var t = window.State && State.activeTicker || "";
        if (/FRED:|ECONOMICS:/.test(t) || (State.tf && State.tf.span === "week")) {
          toast("No " + b.textContent + " bars for this series — use 1D+");
        }
        if (State) State.chartEngine = "native";
      }, true);
    });
  }

  function haVsChange() {
    document.querySelectorAll(".ct-btn[data-ct=\"ha\"]").forEach(function (b) {
      b.addEventListener("click", function () {
        if (window.State && State.changeMode && State.changeMode !== "price") {
          State.changeMode = "price";
          document.querySelectorAll(".chg-btn").forEach(function (x) {
            x.classList.toggle("active", x.dataset.chg === "price");
          });
          toast("Heikin-Ashi needs Price mode");
        }
      }, true);
    });
    document.querySelectorAll(".chg-btn").forEach(function (b) {
      b.addEventListener("click", function () {
        if (b.dataset.chg && b.dataset.chg !== "price" && window.State && State.chartType === "ha") {
          State.chartType = "candles";
          toast("% change uses histogram, not Heikin-Ashi");
        }
      }, true);
    });
  }

  function compareHint() {
    var btn = document.getElementById("compare-btn");
    if (!btn || btn.dataset.jhHint) return;
    btn.dataset.jhHint = "1";
    btn.addEventListener("click", function () {
      toast("Compare runs on JustHodl engine");
    }, true);
  }

  var n = 0;
  var id = setInterval(function () {
    n++;
    patchCloud();
    hideDualRange();
    moveDock();
    labelFredQuotes();
    warnTf();
    haVsChange();
    compareHint();
    if (n > 20) clearInterval(id);
  }, 400);
})();
