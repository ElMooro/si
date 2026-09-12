/* jh-data-feeds.js -- warehouse cards on data.html */
(function () {
  if (window.__jhDataFeeds) return;
  window.__jhDataFeeds = true;
  var KEYS = [
    ["data/verdict.json", "System verdict"],
    ["data/plumbing-brief.json", "Plumbing brief"],
    ["data/market-tape-brief.json", "Market-tape brief"],
    ["data/official-stats-brief.json", "Official-stats brief"],
    ["data/positioning-brief.json", "Positioning brief"],
    ["data/event-brief.json", "Event brief"],
    ["data/ofr-funding.json", "OFR funding"],
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
      card.innerHTML = "<b style=\"color:#e8edf5\">" + label + "</b><div class=\"jh-df-stat\">loading…</div>";
      grid.appendChild(card);
      load(key)
        .then(function (j) {
          var bits = [j.status || j.bias || "", j.source || j.writer || ""];
          if (j.why) bits.push(j.why);
          if (j.score != null) bits.push("score=" + j.score);
          if (j.horizon) bits.push(j.horizon);
          if (j.available_fields != null) bits.push("fields " + j.available_fields);
          if (j.fields && j.fields.composite_label) bits.push(j.fields.composite_label + " " + j.fields.composite_score);
          if (j.fields && j.fields.session) bits.push("session " + j.fields.session);
          if (j.fields && j.fields.n_tickers) bits.push("tickers " + j.fields.n_tickers);
          if (j.n != null) bits.push("n=" + j.n);
          if (j.n_accumulating != null) bits.push("buy=" + j.n_accumulating);
          if (j.series_count != null) bits.push("series " + j.series_count);
          if (j.n_prefixes != null) bits.push("prefixes " + j.n_prefixes);
          if (j.n_hot != null) bits.push("hot " + j.n_hot);
          card.querySelector(".jh-df-stat").textContent = bits.filter(Boolean).join(" · ") || JSON.stringify(j).slice(0, 120);
        })
        .catch(function (e) {
          card.querySelector(".jh-df-stat").textContent = "missing " + e;
        });
    });
  }
  if (document.body) mount();
  else document.addEventListener("DOMContentLoaded", mount);
})();
