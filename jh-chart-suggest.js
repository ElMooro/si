/* Map TV / FRED / ECONOMICS tokens to ChartController ids. Merge-only. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhSuggest) return;
  window.__jhSuggest = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var smap = {};
  function loadMap() {
    return fetch("/data/symbol-map.json?t=" + Date.now(), { cache: "no-store" })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
      .catch(function () {
        return fetch(PROXY + "/data/symbol-map.json").then(function (r) { return r.ok ? r.json() : {}; });
      })
      .then(function (j) { smap = (j && j.map) || j || {}; });
  }
  function resolve(raw) {
    var s = String(raw || "").trim();
    if (!s) return null;
    if (smap[s] && smap[s].id) {
      var m = smap[s];
      if (m.source === "FRED" || /^FRED/i.test(m.source || "")) return "FRED:" + String(m.id).replace(/^FRED:/i, "");
      return m.id;
    }
    var p = /^([A-Z0-9_]+):(.+)$/.exec(s);
    if (p) {
      var ex = p[1], id = p[2];
      if (ex === "FRED") return "FRED:" + id;
      if (ex === "NASDAQ" || ex === "NYSE" || ex === "AMEX" || ex === "ARCA" || ex === "CBOE" || ex === "BATS") return id;
      if (smap[id] && smap[id].id) return resolve(id);
    }
    if (/^[A-Z][A-Z0-9.\-]{0,11}$/.test(s)) return s;
    if (/^[A-Z0-9]{2,32}$/.test(s) && smap["FRED:" + s]) return "FRED:" + s;
    return s;
  }
  window.jhResolveSymbol = resolve;
  function go(raw) {
    var id = resolve(raw);
    if (!id || !window.ChartController) return false;
    if (window.State) State.chartEngine = "native";
    ChartController.loadTicker(id);
    return true;
  }
  document.addEventListener("click", function (e) {
    var row = e.target && e.target.closest && e.target.closest("#jhwl-ul .jhwl-s, #search-modal-body [data-symbol], .hs-row, [data-ticker]");
    if (!row) return;
    var raw = row.getAttribute("data-symbol") || row.getAttribute("data-ticker") || (row.textContent || "").trim().split(/\s+/)[0];
    if (go(raw)) {
      var ov = document.getElementById("jhwl-ov");
      if (ov) ov.style.display = "none";
    }
  }, true);
  var box = document.getElementById("search-input");
  if (box) {
    box.addEventListener("keydown", function (e) {
      if (e.key !== "Enter") return;
      var v = box.value.trim();
      if (!v) return;
      var id = resolve(v);
      if (id && id !== v) box.value = id;
      go(id || v);
    });
  }
  loadMap();
})();
