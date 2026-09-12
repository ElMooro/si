/* jh-chart-pro-dock.js -- warehouse rail; uses Chart Pro layout + ChartSync range */
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
  function clickLayout(id) {
    var btn = document.querySelector('.layout-btn[data-layout="' + id + '"]');
    if (btn) btn.click();
  }
  function applyRange(to) {
    if (!to || !window.ChartSync || !ChartSync.charts) return;
    var toD = new Date(String(to).slice(0, 10) + "T00:00:00Z");
    var from = new Date(toD.getTime() - 365 * 86400000).toISOString().slice(0, 10);
    var toS = String(to).slice(0, 10);
    ChartSync.charts.forEach(function (c) {
      try { c.timeScale().setVisibleRange({ from: from, to: toS }); } catch (e) {}
    });
  }
  var box = document.createElement("aside");
  box.id = "jh-chart-dock";
  box.style.cssText = "position:fixed;left:10px;top:88px;z-index:40;width:200px;max-height:72vh;overflow:auto;padding:10px 12px;border:1px solid #1d2636;border-radius:10px;background:rgba(10,13,18,.94);color:#a8b3c7;font:12px Inter,sans-serif";
  var tog = document.createElement("button");
  tog.textContent = "WH";
  tog.title = "Warehouse dock";
  tog.style.cssText = "position:fixed;left:10px;top:56px;z-index:41;width:32px;height:24px;border:1px solid #1d2636;border-radius:6px;background:#0a0d12;color:#22d3ee;font:10px IBM Plex Mono,monospace;cursor:pointer";
  tog.onclick = function () { box.style.display = box.style.display === "none" ? "block" : "none"; };
  function mount() {
    if (!document.getElementById("jh-chart-dock")) {
      document.body.appendChild(tog);
      document.body.appendChild(box);
    }
  }
  if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);
  window.addEventListener("jh-chart-asof", function (ev) {
    clickLayout("1x2");
    applyRange(ev && ev.detail);
  });
  Promise.all([
    gj("data/plumbing-brief.json"),
    gj("data/official-stats-brief.json"),
    gj("data/ofr-funding.json"),
    gj("data/verdict.json"),
    gj("data/positioning-brief.json"),
    gj("data/alfred-vintages.json"),
    gj("data/market-tape-brief.json")
  ]).then(function (arr) {
    var p = arr[0] || {}, o = arr[1] || {}, f = arr[2] || {}, v = arr[3] || {}, pos = arr[4] || {}, al = arr[5] || {}, mt = arr[6] || {};
    var pf = p.fields || {}, of = o.fields || {}, ps = pos.fields || {}, mf = mt.fields || {};
    var sofr = pf.ofr_sofr != null ? pf.ofr_sofr : (f.sofr && f.sofr.value);
    var alfredN = (al.series || []).reduce(function (n, s) { return n + (s.n_vintages_banked || 0); }, 0);
    var asof = (p.generated_at || o.generated_at || "").slice(0, 10);
    var saved = "";
    try { saved = localStorage.getItem("jh-chart-asof") || ""; } catch (e) {}
    box.innerHTML =
      pane("PLUMBING", cell("LABEL", pf.composite_label) + cell("SOFR", sofr) + cell("TRIPARTY", pf.ofr_triparty_rate) + cell("DVP", pf.ofr_dvp_rate)) +
      pane("STATS", cell("GDPNOW", of.gdpnow) + cell("T10Y3M", of.t10y3m)) +
      pane("FLOW", cell("ETF IN/OUT", [mf.heavy_inflow_n, mf.heavy_outflow_n].join(" / ")) + cell("INST", [ps.accumulating, ps.distributing, ps.flat].join(" / ")) + cell("CFTC", ps.cftc_rows)) +
      pane("TAPE", cell("ALFRED", alfredN || null) + cell("VERDICT", [v.bias || v.call, v.regime].filter(Boolean).join(" \u00b7 "))) +
      "<div style=\"display:flex;gap:4px;margin-top:8px\"><button type=\"button\" data-lay=\"1x2\" style=\"flex:1;background:#0a0d12;color:#22d3ee;border:1px solid #1d2636;border-radius:6px;padding:4px;font:10px IBM Plex Mono,monospace;cursor:pointer\">1x2</button><button type=\"button\" data-lay=\"2x1\" style=\"flex:1;background:#0a0d12;color:#22d3ee;border:1px solid #1d2636;border-radius:6px;padding:4px;font:10px IBM Plex Mono,monospace;cursor:pointer\">2x1</button></div>" +
      "<input id=\"jh-chart-asof\" type=\"date\" value=\"" + (saved || asof) + "\" style=\"width:100%;margin-top:8px;background:#0a0d12;color:#e8edf5;border:1px solid #1d2636;border-radius:6px;padding:4px\"/>";
    box.querySelectorAll("[data-lay]").forEach(function (b) {
      b.addEventListener("click", function () { clickLayout(b.getAttribute("data-lay")); });
    });
    var inp = document.getElementById("jh-chart-asof");
    if (inp) inp.addEventListener("change", function () {
      try { localStorage.setItem("jh-chart-asof", inp.value); } catch (e) {}
      window.dispatchEvent(new CustomEvent("jh-chart-asof", { detail: inp.value }));
    });
  });
})();
