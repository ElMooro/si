/* jh-reskin-skip */
/* Inline FTD/FTR panel for liquidity.html — not a WALCL term. */
(function () {
  if (window.__jhLiqFailsPanel) return;
  window.__jhLiqFailsPanel = true;
  function n(v) {
    return typeof v !== 'number' || !Number.isFinite(v) || v < 0 ? 'unavailable' : '$' + v.toLocaleString('en-US', {maximumFractionDigits: 3}) + 'bn';
  }
  function date(value) {
    if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
    var ms = Date.parse(value + 'T00:00:00Z');
    return Number.isFinite(ms) && new Date(ms).toISOString().slice(0, 10) === value && ms <= Date.now() ? value : null;
  }
  function scoped(row, scope) {
    return row && row.scope_id === scope && date(row.as_of) && (row.unit === 'usd_bn' || row.field_units) ? row : null;
  }
  function amount(row, field) {
    if (!row || (row.field_units ? row.field_units[field] !== 'usd_bn' : row.unit !== 'usd_bn')) return null;
    return row[field];
  }
  function dated(row) {
    return row ? row.as_of + ((Date.now() - Date.parse(row.as_of + 'T00:00:00Z')) / 86400000 > 15 ? ' · old observation' : '') : 'date unavailable';
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
    // Preserve the two overlapping populations. A missing ex-TIPS value is never
    // replaced with an including-TIPS amount, even if that value is available.
    var hd = scoped(sf && sf.headline, 'ust_ex_tips');
    var canonical = scoped(sf && sf.treasury, 'treasury_incl_tips');
    var tr = canonical || scoped(flow && flow.pd_settlement_fails, 'treasury_incl_tips');
    var totalField = canonical ? 'gross_bn' : 'combined_bn';
    host.style.whiteSpace = 'pre-line';
    host.textContent = 'PD SETTLEMENT FAILS · weekly reported amounts\n' +
      'UST excluding TIPS · ' + dated(hd) + ' · FTD ' + n(amount(hd, 'ftd_bn')) +
      ' · FTR ' + n(amount(hd, 'ftr_bn')) + ' · total ' + n(amount(hd, 'combined_bn')) + '\n' +
      'Treasury including TIPS · ' + dated(tr) + ' · FTD ' + n(amount(tr, 'ftd_bn')) +
      ' · FTR ' + n(amount(tr, 'ftr_bn')) + ' · total ' + n(amount(tr, totalField)) + '\n' +
      'Two-sided gross reports; the scopes overlap. These are not unique defaults or capital flows, and are not part of WALCL − TGA − RRP.';
  }
  Promise.all([
    fetch("/data/liquidity-flow.json?exact=1&nogen=1", { cache: "no-store", credentials: "omit" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
    fetch("/data/settlement-fails.json?exact=1&nogen=1", { cache: "no-store", credentials: "omit" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
  ]).then(function (a) { mount(a[0], a[1]); }).catch(function () {});
})();
