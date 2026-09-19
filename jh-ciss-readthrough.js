(function(root){
  'use strict';
  const esc=v=>String(v==null?'unavailable':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
  function state(packet,now=Date.now()){
    const q=packet?.macro_stress||packet?.ciss_systemic;
    const sourceAge=now-Date.parse(q?.acquired_at),warehouseAge=now-Date.parse(q?.warehouse_generated_at);
    const observationAge=(Math.floor(now/86400000)*86400000-Date.parse(q?.period_end+'T00:00:00Z'))/86400000;
    const packetAge=now-Date.parse(packet?.generated_at);
    const fresh=q?.contract==='ciss-readthrough.v1'&&q.status==='fresh'&&path(q.source_replay?.manifest_key)&&Number.isFinite(q.value)&&
      [sourceAge,warehouseAge,observationAge,packetAge].every(v=>Number.isFinite(v)&&v>=0)&&
      sourceAge<=q.maximum_acquisition_age_seconds*1000&&warehouseAge<=72*3600000&&packetAge<=72*3600000&&observationAge<=q.maximum_observation_age_days;
    return {q,fresh:!!fresh,status:fresh?'fresh within stated ceilings':q?.status==='fresh'?'expired':q?.status||'unavailable'};
  }
  function render(host,packet){
    if(!host)return;
    const view=state(packet),q=view.q||{},fresh=view.fresh,status=view.status,run=path(q.source_replay?.manifest_key),original=path(q.evidence?.key),coverage=packet?.assessment_coverage;
    host.style.cssText='padding:18px;margin:16px 0;border:1px solid #526b7c;border-radius:8px;background:#101923;color:#e7edf4;line-height:1.6;overflow-wrap:anywhere';
    host.innerHTML='<strong>Research use and source evidence</strong><p>This desk has no validated trade or position-size authorization. Its screen and stress scores are research descriptions; a percentile is not a crisis probability.</p>'+
      (coverage?`<p>Assessment coverage: ${esc(coverage.assessed)} of ${esc(coverage.universe)} names have at least ${esc(coverage.minimum_axes)} populated axes; ${esc(coverage.insufficient_axes)} lack enough inputs. Input coverage does not establish source quality or predictive accuracy.</p>`:'')+
      `<p><strong>ECB CISS: ${fresh?esc(q.value_decimal):'unavailable'}</strong> · ${esc(q.unit)} · observed ${esc(q.observation_date)} · ${esc(status)}.</p>`+
      '<p>CISS supplies no short-book tailwind or headwind, and no numerical floor for the global stress score. The connection to asset returns has not been validated.</p>'+
      `<details><summary>Inspect CISS source and interpretation limits</summary><p>${esc(q.series_id)} · acquired ${esc(q.acquired_at)} · warehouse ${esc(q.warehouse_generated_at)}. Source publication time is not independently available.</p>`+
      (run?`<a href="${esc(run)}" target="_blank" rel="noopener">Retained source run</a> · `:'')+
      (original?`<a href="${esc(original)}" target="_blank" rel="noopener">Original CSV (.gz)</a> · `:'')+
      `<a href="/ciss.html${q.series_id?'?series='+encodeURIComponent(q.series_id):''}">Inspect exact ECB measurement</a><p>Other inputs on this desk require their own source and model validation. This CISS check does not certify them.</p></details>`;
    host.querySelectorAll('a').forEach(a=>a.style.color='#9cdbed');
    host._cissPacket=packet;host._cissState=status;
  }
  if(typeof module==='object'&&module.exports)module.exports={state,path};
  if(!root.document)return;
  root.JHCissReadthrough={render};
  setInterval(()=>{for(const host of root.document.querySelectorAll('[data-ciss-readthrough]')){
    if(host._cissPacket&&state(host._cissPacket).status!==host._cissState)render(host,host._cissPacket);
  }},60000);
})(typeof window!=='undefined'?window:globalThis);
