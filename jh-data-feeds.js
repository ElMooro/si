/* jh-data-feeds.js -- warehouse cards on data.html */
(function () {
  if (window.__jhDataFeeds) return;
  window.__jhDataFeeds = true;
  var KEYS = [
    ["data/verdict.json", "System verdict"],
    ["data/jh-internals.json", "JH internals (computed)"],
    ["data/plumbing-brief.json", "Plumbing brief"],
    ["data/market-tape-brief.json", "Market-tape brief"],
    ["data/official-stats-brief.json", "Official-stats brief"],
    ["data/positioning-brief.json", "Positioning brief"],
    ["data/event-brief.json", "Event brief"],
    ["data/alfred-vintages.json", "ALFRED vintages"],
    ["data/ofr-funding.json", "OFR funding"],
    ["data/macro-tape.json", "Macro tape"],
    ["data/inst-public-join.json", "Free institutional feeds"],
    ["data/cftc-join.json", "CFTC join"],
    ["data/finra-surface.json", "FINRA surface"],
    ["data/real-economy-summary.json", "Real economy"],
    ["data/treasury-auctions-composite.json", "Treasury auctions"],
    ["data/dtcc-fails-agency.json", "DTCC fails"],
    ["data/tic-state.json", "TIC state"],
    ["data/fiscaldata-state.json", "FiscalData state"],
    ["data/census-us-state.json", "Census US state"],
    ["data/bls-full-state.json", "BLS full state"],
    ["data/warm-prefix-index.json", "Warm prefix index"],
    ["data/theme-eurostat.json", "Eurostat theme"],
    ["data/theme-gdelt.json", "GDELT theme"],
    ["data/warehouse-use.json", "Warehouse surface"],
    ["data/polygon-snapshot.json", "Polygon snapshot"],
    ["data/polygon-short-interest.json", "Polygon short interest"],
    ["data/polygon-news.json", "Polygon news"],
    ["data/polygon-options.json", "Polygon options"],
    ["data/polygon-ratios.json", "Polygon financials"],
    ["data/finviz-insider.json", "Finviz insider"],
    ["data/finviz-inst-flow.json", "Finviz institutional flow"],
    ["data/fed-nowcast-join.json", "Fed nowcast join"],
    ["data/etf-global.json", "ETF Global"]
  ];
  function el(tag, css, html) {
    var n = document.createElement(tag);
    if (css) n.style.cssText = css;
    if (html != null) n.innerHTML = html;
    return n;
  }
  function load(key) {
    return fetch("/" + key + "?t=" + Date.now(), { cache: "no-store" })
      .then(function (r) {
        if (r.ok) return r.json();
        return fetch("https://justhodl-data-proxy.raafouis.workers.dev/" + key + "?t=" + Date.now(), { cache: "no-store" })
          .then(function (r2) { return r2.ok ? r2.json() : Promise.reject(r2.status); });
      });
  }
  function mount() {
    if (document.getElementById("jh-data-feeds")) return;
    var box = el("section", "margin:16px 0 28px;padding:16px 18px;border:1px solid #1d2636;border-radius:12px;background:#10151f");
    box.id = "jh-data-feeds";
    box.innerHTML = "<div style=\"font:11px 'IBM Plex Mono',monospace;color:#22d3ee;letter-spacing:1.4px;text-transform:uppercase;margin-bottom:10px\">Briefs + paid feeds</div>";
    var grid = el("div", "display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px");
    box.appendChild(grid);
    var host = document.querySelector(".wrap, .shell, main, #app, body");
    if (host && host.firstChild) host.insertBefore(box, host.children[1] || host.firstChild);
    else document.body.appendChild(box);
    KEYS.forEach(function (pair) {
      var key = pair[0], label = pair[1];
      var card = el("div", "padding:12px;border:1px solid #1d2636;border-radius:10px;background:#0a0d12;font:12px Inter,system-ui,sans-serif;color:#a8b3c7");
      card.innerHTML = "<b style=\"color:#e8edf5\">" + label + "</b><div class=\"jh-df-stat\">loading\u2026</div>";
      grid.appendChild(card);
      load(key)
        .then(function (j) {
          var bits = [j.status || j.bias || "", j.source || j.writer || ""];
          if (j.why) bits.push(j.why);
          if (j.fields && j.fields.twos_tens != null) bits.push("2s10s " + j.fields.twos_tens);
          if (j.fields && j.fields.ad_breadth != null) bits.push("A-D " + j.fields.ad_breadth);
          if (j.fields && j.fields.liq_proxy_bn != null) bits.push("liq " + j.fields.liq_proxy_bn);
          if (j.n_indexed != null) bits.push("indexed " + j.n_indexed);
          if (j.n_live != null) bits.push("live " + j.n_live);
          if (j.available_fields != null) bits.push("fields " + j.available_fields);
          if (j.n_fields != null) bits.push("fields " + j.n_fields);
          if (j.n_prefixes != null) bits.push("prefixes " + j.n_prefixes);
          if (j.n_total != null) bits.push("n=" + j.n_total);
          if (j.fields && j.fields.vix) bits.push("vix " + j.fields.vix.value);
          if (j.n != null) bits.push("n=" + j.n);
          card.querySelector(".jh-df-stat").textContent = bits.filter(Boolean).join(" \u00b7 ") || JSON.stringify(j).slice(0, 140);
        })
        .catch(function (e) {
          card.querySelector(".jh-df-stat").textContent = "missing " + e;
        });
    });
  }
  if (document.body) mount();
  else document.addEventListener("DOMContentLoaded", mount);
})();
