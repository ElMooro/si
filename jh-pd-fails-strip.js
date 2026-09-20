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
    n.style.cssText = "display:block;box-sizing:border-box;max-width:100%;margin:16px 0;padding:10px 12px;background:var(--jh-panel,#14141c);border:1px solid var(--jh-border,#3a3a48);border-radius:8px;color:var(--jh-ink,#e6ecf5);font:11px/1.5 IBM Plex Mono,ui-monospace,monospace;text-decoration:none;white-space:pre-line;overflow-wrap:anywhere";
    var host = document.querySelector("main") || document.body;
    host.appendChild(n);
    return n;
  }
  function n(v) { return typeof v !== 'number' || !Number.isFinite(v) || v < 0 ? "—" : "$" + v.toLocaleString('en-US', { maximumFractionDigits: 3 }) + "bn"; }
  function dated(row) {
    var value = row.as_of, valid = typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);
    var day = valid ? Date.parse(value + 'T00:00:00Z') : NaN;
    var age = (Date.now() - day) / 86400000;
    return Number.isFinite(day) && age >= 0 ? value + (age > 15 ? ' · old observation' : '') : 'observation date unavailable';
  }
  function mount(sf, flow) {
    var hd = (sf && sf.headline) || {};
    var gross = (flow && flow.pd_settlement_fails) || {};
    // These scopes overlap. Missing headline values must never fall back to gross.
    if ((hd.scope_id || hd.scope) !== 'ust_ex_tips') hd = {};
    if (gross.scope_id !== 'treasury_incl_tips' || gross.unit !== 'usd_bn') gross = {};
    var box = el();
    box.textContent = "PD settlement fails · weekly reported amounts\n" +
      "UST ex-TIPS · " + dated(hd) + " · FTD " + n(hd.ftd_bn) + " · FTR " + n(hd.ftr_bn) + " · total " + n(hd.combined_bn) +
      "\nTreasury including TIPS · " + dated(gross) + " · total " + n(gross.combined_bn) +
      "\nTwo-sided gross reports, not unique defaults or capital flows. Open the source desk for definitions, quality and original records.";
  }
  Promise.all([
    fetch("/data/settlement-fails.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
    fetch("/data/liquidity-flow.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
  ]).then(function (a) {
    if (a[0] || a[1]) mount(a[0], a[1]);
  });
})();
