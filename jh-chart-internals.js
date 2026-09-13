/* Internals from warehouse data/jh-internals.json */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function cell(k, v) {
    return "<div style=\"padding:5px 0;border-bottom:1px solid #1d2636\"><div style=\"font:10px IBM Plex Mono,monospace;color:#7dd3fc\">" +
      k + "</div><div style=\"font:13px Inter,sans-serif;color:#e8edf5\">" + (v == null ? "\u2014" : v) + "</div></div>";
  }
  function pct(x) { return x == null ? null : (Number(x) * 100).toFixed(1) + "%"; }
  function paint(j) {
    var host = document.getElementById("jh-chart-dock");
    if (!host || !j || !j.fields) return;
    var f = j.fields;
    var ad = f.ad_breadth;
    if (ad != null) ad = Number(ad).toFixed(3) + (f.n_up != null ? " (" + f.n_up + "/" + f.n_down + ")" : "");
    var box = document.createElement("div");
    box.innerHTML = "<div style=\"margin-top:10px;padding-top:8px;border-top:1px solid #1d2636\">" +
      "<div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.2px\">INTERNALS</div>" +
      cell("2s10s", f.twos_tens != null ? f.twos_tens + "%" : null) +
      cell("LIQ $B", f.liq_proxy_bn) +
      cell("NFCI", f.nfci) +
      cell("A-D", ad) +
      cell("%>50d", pct(f.pct_above_50)) +
      cell("%>200d", pct(f.pct_above_200)) +
      "<div style=\"font:10px Inter,sans-serif;color:#6b7480;margin-top:6px\">" + String(j.generated_at || "").slice(0, 19) + "Z · warehouse</div></div>";
    host.appendChild(box);
  }
  fetch("/data/jh-internals.json?t=" + Date.now(), { cache: "no-store" })
    .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
    .catch(function () {
      return fetch(PROXY + "/data/jh-internals.json?t=" + Date.now()).then(function (r) { return r.ok ? r.json() : null; });
    })
    .then(paint);
})();
