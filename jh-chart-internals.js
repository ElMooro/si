/* JH internals from official series already in the stack. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function lastObs(j) {
    var o = (j && (j.observations || j.obs || j.bars)) || [];
    if (!o.length) return null;
    var x = o[o.length - 1];
    var v = x.value != null ? x.value : x.close != null ? x.close : x.v;
    var t = x.date || x.time || x.observation_date;
    v = parseFloat(v);
    if (!isFinite(v)) return null;
    return { t: t, v: v };
  }
  function fred(id) {
    return fetch(PROXY + "/fred?series=" + encodeURIComponent(id) + "&obs=8", { cache: "no-store" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(lastObs)
      .catch(function () { return null; });
  }
  function cell(k, v) {
    return "<div style=\"padding:5px 0;border-bottom:1px solid #1d2636\"><div style=\"font:10px IBM Plex Mono,monospace;color:#7dd3fc\">" +
      k + "</div><div style=\"font:13px Inter,sans-serif;color:#e8edf5\">" + (v == null ? "—" : v) + "</div></div>";
  }
  Promise.all([
    fred("DGS10"), fred("DGS2"), fred("WALCL"), fred("WTREGEN"), fred("RRPONTSYD")
  ]).then(function (a) {
    var d10 = a[0], d2 = a[1], wal = a[2], tga = a[3], rrp = a[4];
    var spr = (d10 && d2) ? (d10.v - d2.v).toFixed(2) + "%" : null;
    var liq = (wal && tga && rrp) ? ((wal.v - tga.v - rrp.v) / 1000).toFixed(0) + "B" : null;
    var host = document.getElementById("jh-chart-dock");
    if (!host) return;
    var box = document.createElement("div");
    box.innerHTML = "<div style=\"margin-top:10px;padding-top:8px;border-top:1px solid #1d2636\">" +
      "<div style=\"font:10px IBM Plex Mono,monospace;color:#22d3ee;letter-spacing:1.2px\">INTERNALS (computed)</div>" +
      cell("2s10s", spr) +
      cell("LIQ WALCL-TGA-RRP", liq) +
      "<div style=\"font:10px Inter,sans-serif;color:#6b7480;margin-top:6px\">A-D / %>MA need the daily universe job. Licensed PMI skipped.</div></div>";
    host.appendChild(box);
  });
})();
