/* jh-data-feeds.js -- mount paid-plan warehouse cards on data.html */
(function () {
  if (window.__jhDataFeeds) return;
  window.__jhDataFeeds = true;
  var BUCKET = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
  var KEYS = [
    ["data/polygon-snapshot.json", "Polygon snapshot"],
    ["data/polygon-short-interest.json", "Polygon short interest"],
    ["data/polygon-news.json", "Polygon news"],
    ["data/polygon-options.json", "Polygon options"],
    ["data/polygon-ratios.json", "Polygon financials"],
    ["data/finviz-insider.json", "Finviz insider"],
    ["data/finviz-inst-flow.json", "Finviz institutional flow"],
    ["data/fed-nowcast-join.json", "Fed nowcast join"]
  ];
  function el(tag, css, html) {
    var n = document.createElement(tag);
    if (css) n.style.cssText = css;
    if (html != null) n.innerHTML = html;
    return n;
  }
  function mount() {
    if (document.getElementById("jh-data-feeds")) return;
    var box = el("section", "margin:16px 0 28px;padding:16px 18px;border:1px solid #1d2636;border-radius:12px;background:#10151f");
    box.id = "jh-data-feeds";
    box.innerHTML = "<div style=\"font:11px 'IBM Plex Mono',monospace;color:#22d3ee;letter-spacing:1.4px;text-transform:uppercase;margin-bottom:10px\">Paid feeds live</div>";
    var grid = el("div", "display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px");
    box.appendChild(grid);
    var host = document.querySelector(".shell, main, #app, body");
    if (host && host.firstChild) host.insertBefore(box, host.firstChild === document.body ? host.children[1] : host.firstChild);
    else document.body.appendChild(box);
    KEYS.forEach(function (pair) {
      var key = pair[0], label = pair[1];
      var card = el("div", "padding:12px;border:1px solid #1d2636;border-radius:10px;background:#0a0d12;font:12px Inter,system-ui,sans-serif;color:#a8b3c7");
      card.innerHTML = "<b style=\"color:#e8edf5\">" + label + "</b><div class=\"jh-df-stat\">loading…</div>";
      grid.appendChild(card);
      fetch(BUCKET + "/" + key + "?t=" + Date.now(), { cache: "no-store" })
        .then(function (r) { return r.ok ? r.json() : Promise.reject(r.status); })
        .then(function (j) {
          var bits = [j.status || "", j.source || ""];
          if (j.n != null) bits.push("n=" + j.n);
          if (j.n_ok != null) bits.push("ok=" + j.n_ok);
          if (j.n_screens != null) bits.push("screens=" + j.n_screens);
          if (j.n_with_inst_trans != null) bits.push("inst=" + j.n_with_inst_trans);
          if (j.n_accumulating != null) bits.push("buy=" + j.n_accumulating);
          if (j.n_distributing != null) bits.push("sell=" + j.n_distributing);
          if (j.series && j.series.clevelandfed && j.series.clevelandfed.last)
            bits.push("Cle " + JSON.stringify(j.series.clevelandfed.last));
          if (j.series && j.series.atlantafed && j.series.atlantafed.last)
            bits.push("Atl GDPNow " + (j.series.atlantafed.last.GDPNOW || ""));
          if (j.counts_subset) bits.push(JSON.stringify(j.counts_subset));
          card.querySelector(".jh-df-stat").textContent = bits.filter(Boolean).join(" · ");
        })
        .catch(function (e) {
          card.querySelector(".jh-df-stat").textContent = "missing " + e;
        });
    });
  }
  if (document.body) mount();
  else document.addEventListener("DOMContentLoaded", mount);
})();
