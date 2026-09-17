/* jh-reskin-skip */
/* FR2004 FTD + FTR strip. WALCL untouched. */
(function () {
  if (window.__jhPdFailsStrip) return;
  window.__jhPdFailsStrip = true;
  function el() {
    var n = document.getElementById("jh-pd-fails-strip");
    if (n) return n;
    n = document.createElement("a");
    n.id = "jh-pd-fails-strip";
    n.href = "/fails.html";
    n.style.cssText = "position:fixed;right:12px;bottom:148px;z-index:9996;display:block;max-width:280px;padding:7px 10px;background:#14141c;border:1px solid #3a3a48;border-radius:8px;color:#e6ecf5;font:11px/1.4 IBM Plex Mono,ui-monospace,monospace;text-decoration:none;box-shadow:0 4px 14px rgba(0,0,0,.35)";
    document.body.appendChild(n);
    return n;
  }
  function n(v) { return v == null || isNaN(+v) ? "—" : "$" + Number(v).toFixed(0) + "bn"; }
  function mount(sf, flow) {
    var tr = (sf && sf.treasury) || {};
    var hd = (sf && sf.headline) || {};
    var asof = tr.as_of || hd.as_of || (sf && sf.as_of) || "";
    var q = ((sf && sf.quality) || tr.quality || hd.quality || {}).status || "";
    var ftd = hd.ftd_bn != null ? hd.ftd_bn : tr.ftd_bn;
    var ftr = hd.ftr_bn != null ? hd.ftr_bn : tr.ftr_bn;
    var comb = hd.combined_bn != null ? hd.combined_bn : tr.gross_bn;
    var gross = (flow && flow.pd_settlement_fails) || {};
    var box = el();
    box.innerHTML =
      "PD FAILS " + asof + " " + q +
      "<br>UST ex-TIPS FTD " + n(ftd) + " · FTR " + n(ftr) + " · tot " + n(comb) +
      (gross.combined_bn != null ? "<br>Treasury gross tot " + n(gross.combined_bn) : "");
  }
  Promise.all([
    fetch("/data/settlement-fails.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
    fetch("/data/liquidity-flow.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
  ]).then(function (a) {
    if (a[0] || a[1]) mount(a[0], a[1]);
  });
})();
