/* jh-reskin-skip */
/* FR2004 FTD + FTR strip. Does not change WALCL. */
(function () {
  if (window.__jhPdFailsStrip) return;
  window.__jhPdFailsStrip = true;
  function mount(d) {
    var tr = (d && d.treasury) || {};
    var hd = (d && d.headline) || {};
    var ftd = tr.ftd_bn != null ? tr.ftd_bn : hd.ftd_bn;
    var ftr = tr.ftr_bn != null ? tr.ftr_bn : hd.ftr_bn;
    var comb = tr.gross_bn != null ? tr.gross_bn : hd.combined_bn;
    var asof = tr.as_of || hd.as_of || d.as_of;
    var q = (d.quality || tr.quality || hd.quality || {}).status || "";
    var n = document.getElementById("jh-pd-fails-strip");
    if (!n) {
      n = document.createElement("a");
      n.id = "jh-pd-fails-strip";
      n.href = "/fails.html";
      n.style.cssText = "position:fixed;right:12px;bottom:148px;z-index:9996;display:block;padding:7px 10px;background:#14141c;border:1px solid #3a3a48;border-radius:8px;color:#e6ecf5;font:11px/1.35 IBM Plex Mono,ui-monospace,monospace;text-decoration:none;box-shadow:0 4px 14px rgba(0,0,0,.35)";
      document.body.appendChild(n);
    }
    if (ftd == null && ftr == null) {
      n.textContent = "PD fails — unavailable";
      return;
    }
    n.innerHTML = "PD FAILS " + (asof || "") + " " + q +
      "<br>FTD $" + Number(ftd).toFixed(0) + "bn · FTR $" + Number(ftr).toFixed(0) +
      "bn · tot $" + Number(comb).toFixed(0) + "bn";
  }
  fetch("/data/settlement-fails.json", { cache: "no-store" })
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (d) { if (d) mount(d); })
    .catch(function () {});
})();
