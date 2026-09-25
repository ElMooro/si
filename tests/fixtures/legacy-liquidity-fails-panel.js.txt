/* jh-reskin-skip */
/* Inline FTD/FTR panel for liquidity.html — not a WALCL term. */
(function () {
  if (window.__jhLiqFailsPanel) return;
  window.__jhLiqFailsPanel = true;
  function n(v) {
    return v == null || isNaN(+v) ? "\u2014" : "$" + Number(v).toFixed(0) + "bn";
  }
  function mount(flow, sf) {
    var host = document.getElementById("jh-liq-fails-panel");
    if (!host) {
      host = document.createElement("section");
      host.id = "jh-liq-fails-panel";
      host.style.cssText = "margin:16px 0;padding:12px 14px;background:#12121a;border:1px solid #2a2a36;border-radius:10px;color:#e6ecf5;font:13px/1.45 IBM Plex Mono,ui-monospace,monospace";
      var anchor = document.querySelector("main, #app, .wrap, body");
      (anchor || document.body).insertBefore(host, (anchor && anchor.firstChild) || null);
    }
    var pd = (flow && flow.pd_settlement_fails) || {};
    var hd = (sf && sf.headline) || {};
    var tr = (sf && sf.treasury) || {};
    host.innerHTML =
      "<div style=\"color:#9aa3b2;font-size:11px;letter-spacing:.04em\">PD SETTLEMENT FAILS \u2014 not in net liquidity (WALCL \u2212 TGA \u2212 RRP)</div>" +
      "<div>UST ex-TIPS FTD " + n(hd.ftd_bn != null ? hd.ftd_bn : tr.ftd_bn) +
      " \u00b7 FTR " + n(hd.ftr_bn != null ? hd.ftr_bn : tr.ftr_bn) +
      " \u00b7 tot " + n(hd.combined_bn != null ? hd.combined_bn : tr.gross_bn) +
      " <span style=\"color:#9aa3b2\">as of " + (hd.as_of || tr.as_of || "") + "</span></div>" +
      "<div>Treasury gross tot " + n(pd.combined_bn) +
      " <span style=\"color:#9aa3b2\">FR2004 weekly \u00b7 settlement stress, not default risk</span></div>";
  }
  Promise.all([
    fetch("/data/liquidity-flow.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }),
    fetch("/data/settlement-fails.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; })
  ]).then(function (a) { mount(a[0], a[1]); }).catch(function () {});
})();
