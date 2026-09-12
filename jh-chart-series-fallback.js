/* Overlay for unmapped TV-list symbols. Do not use the Function URL as the only path. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function tryNative(sym) {
    if (!window.ChartController) return false;
    var bare = String(sym || "");
    var m = /^([A-Z0-9_]+):(.+)$/.exec(bare);
    if (m && m[1] === "FRED") { ChartController.loadTicker("FRED:" + m[2]); return true; }
    if (m && (m[1] === "NASDAQ" || m[1] === "NYSE" || m[1] === "AMEX" || m[1] === "CBOE" || m[1] === "ARCA")) {
      ChartController.loadTicker(m[2]); return true;
    }
    if (/^[A-Z][A-Z0-9.\-]{0,11}$/.test(bare)) { ChartController.loadTicker(bare); return true; }
    if (bare.indexOf("FRED:") === 0) { ChartController.loadTicker(bare); return true; }
    return false;
  }
  document.addEventListener("click", function (e) {
    var row = e.target && e.target.closest && e.target.closest("#jhwl-ul .jhwl-s");
    if (!row) return;
    var sym = (row.textContent || "").trim();
    if (tryNative(sym)) {
      var ov = document.getElementById("jhwl-ov");
      if (ov) ov.style.display = "none";
    }
  }, true);
  var n = 0;
  var id = setInterval(function () {
    n++;
    var ov = document.getElementById("jhwl-ov-c");
    if (ov && /series api unreachable/i.test(ov.textContent || "")) {
      var title = document.getElementById("jhwl-ov-t");
      var sym = title ? (title.textContent || "").split("·").pop().trim() : "";
      if (tryNative(sym)) ov.parentElement.style.display = "none";
      else {
        fetch(PROXY + "/ohlc?ticker=" + encodeURIComponent(sym.replace(/^.*:/, "")) + "&mult=1&span=day&days=400")
          .then(function (r) { return r.ok ? r.json() : null; })
          .then(function (j) {
            if (!j || !(j.bars || []).length) return;
            ov.textContent = "loaded via proxy · " + j.bars.length + " bars — use JustHodl engine";
            if (window.ChartController) ChartController.loadTicker((j.ticker || sym.replace(/^.*:/, "")));
          }).catch(function () {});
      }
    }
    if (n > 40) clearInterval(id);
  }, 500);
})();
