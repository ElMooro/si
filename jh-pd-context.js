/* Separate FR2004 context for engine pages. No score, flow or allocation blend. */
(function () {
  'use strict';
  const script = document.currentScript;
  const packet = script && script.dataset.packet;
  if (!packet || !/^\/data\/[a-z0-9-]+\.json$/.test(packet)) return;
  const escape = v => String(v ?? 'Unavailable').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = v => typeof v === 'number' && Number.isFinite(v) ? v.toFixed(2) : '—';
  function scope(label, row) {
    row = row || {};
    return '<div style="flex:1;min-width:240px"><strong>'+escape(label)+'</strong><br>'+escape(row.as_of)+' · '+escape(row.quality?.status)+
      '<br>FTD '+number(row.ftd_bn)+' · FTR '+number(row.ftr_bn)+' · gross '+number(row.combined_bn)+' USD bn</div>';
  }
  fetch(packet, {cache:'no-store', signal:AbortSignal.timeout(15000)}).then(r => {
    if (!r.ok) throw new Error('Unavailable');
    return r.json();
  }).then(doc => {
    const pd = doc.pd_settlement_fails;
    if (!pd) return;
    const box = document.createElement('section');
    box.id = 'jh-pd-context';
    box.style.cssText='margin:20px auto;padding:18px;max-width:1240px;border:1px solid #596475;border-radius:8px;font:13px/1.6 system-ui;color:inherit;background:rgba(128,128,128,.06)';
    box.innerHTML='<h2 style="margin:0 0 10px;font-size:18px">Primary-dealer settlement context</h2>'+
      '<div style="display:flex;flex-wrap:wrap;gap:20px">'+scope('Treasury including TIPS — gross scope',pd)+
      scope('Treasury excluding TIPS — headline scope',pd.ust_ex_tips)+'</div>'+
      '<p>These scopes overlap. Do not add them together. FTD + FTR measures two-sided reported settlement fails, not unique securities, defaults, new capital flows or central-bank injection.</p>'+
      '<a href="/fails.html">Inspect the dated source series →</a>';
    script.parentNode.insertBefore(box, script);
  }).catch(() => {});
})();
