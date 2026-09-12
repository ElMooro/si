/* Resolve TV tokens via data/tv-symbol-resolver.json + symbol-map. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhSuggest) return;
  window.__jhSuggest = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var resolver = { prefix: {}, exact: {}, licensed_econ_skip: [] };
  var smap = {};
  function gj(path) {
    return fetch("/" + path + "?t=" + Date.now(), { cache: "no-store" })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
      .catch(function () {
        return fetch(PROXY + "/" + path).then(function (r) { return r.ok ? r.json() : {}; });
      });
  }
  Promise.all([gj("data/tv-symbol-resolver.json"), gj("data/symbol-map.json")]).then(function (a) {
    resolver = a[0] || resolver;
    smap = (a[1] && a[1].map) || {};
  });
  function resolve(raw) {
    var s = String(raw || "").trim();
    if (!s) return null;
    if ((resolver.licensed_econ_skip || []).indexOf(s) >= 0) return { skip: true, reason: "licensed_econ" };
    if (resolver.exact && resolver.exact[s]) {
      var ex = resolver.exact[s];
      if (ex.id === "SKIP") return { skip: true, reason: ex.reason };
      return { id: ex.id, engine: ex.engine };
    }
    var p = /^([A-Z0-9_]+):(.+)$/.exec(s);
    if (p && resolver.prefix && resolver.prefix[p[1]]) {
      var pr = resolver.prefix[p[1]];
      if (pr.engine === "fred") return { id: "FRED:" + p[2].replace(/^FRED:/, ""), engine: "fred" };
      if (pr.strip) return { id: p[2], engine: "equity" };
    }
    if (smap[s] && smap[s].id) {
      var m = smap[s];
      if (m.source === "FRED") return { id: "FRED:" + String(m.id).replace(/^FRED:/i, ""), engine: "fred" };
      return { id: m.id, engine: "equity" };
    }
    if (/^[A-Z][A-Z0-9.\-]{0,11}$/.test(s)) return { id: s, engine: "equity" };
    return { id: s, engine: "unknown" };
  }
  window.jhResolveSymbol = resolve;
  function go(raw) {
    var r = resolve(raw);
    if (!r || r.skip) return false;
    if (!window.ChartController || !r.id || r.id.indexOf("COMPUTE:") === 0) return false;
    if (window.State) State.chartEngine = "native";
    ChartController.loadTicker(r.id);
    return true;
  }
  document.addEventListener("click", function (e) {
    var row = e.target && e.target.closest && e.target.closest("#jhwl-ul .jhwl-s");
    if (!row) return;
    var raw = (row.textContent || "").trim();
    if (go(raw)) {
      var ov = document.getElementById("jhwl-ov");
      if (ov) ov.style.display = "none";
    }
  }, true);
  var box = document.getElementById("search-input");
  if (box) {
    box.addEventListener("keydown", function (e) {
      if (e.key !== "Enter") return;
      go(box.value.trim());
    });
  }
})();
