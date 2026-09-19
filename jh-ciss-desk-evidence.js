(function(root){
  'use strict';
  const esc=v=>String(v==null?'—':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
  function current(q,now=Date.now()){
    const acquisition=now-Date.parse(q?.source_acquired_at),generation=now-Date.parse(q?.warehouse_generated_at);
    const age=(Math.floor(now/86400000)*86400000-Date.parse(q?.period_end+'T00:00:00Z'))/86400000;
    return q?.status==='fresh'&&[acquisition,generation,age].every(Number.isFinite)&&acquisition>=0&&generation>=0&&age>=0&&generation<=72*3600000&&acquisition<=q.maximum_acquisition_age_seconds*1000&&age<=q.maximum_observation_age_days;
  }
  function rows(packet){
    if(packet.systemic_stress_ciss)return Object.entries(packet.systemic_stress_ciss).map(([name,r])=>({name:name+' · CISS',row:r,q:r.quality,value:r.level_decimal,key:r.source_key,evidence:r.source_evidence,comparisons:r.comparisons})).concat(
      Object.entries(packet.sovereign_stress_sovciss||{}).map(([name,r])=>({name:name+' · SovCISS',row:r,q:r.quality,value:r.level_decimal,key:r.source_key,evidence:r.source_evidence,comparisons:r.comparisons})));
    return Object.entries(packet.countries||{}).map(([name,r])=>({name:r.name||name,row:r,q:r.sovciss_quality,value:r.sovciss_decimal,key:r.sovciss_source_key,evidence:r.sovciss_evidence,comparisons:r.sovciss_comparisons}));
  }
  function render(host,packet){
    if(!host)return;
    const ref=packet.ciss_warehouse?.replay,items=rows(packet),verified=path(ref?.manifest_key);
    host.style.cssText='margin:16px 0;padding:18px;border:1px solid #5c7182;border-radius:12px;background:#101923;color:#e7edf4;font:13px/1.6 system-ui,sans-serif';
    host.innerHTML='<strong>Measurement evidence and decision limits</strong><p>Stress scores and percentile labels on this desk are descriptive heuristics. They are not calibrated crisis probabilities, asset-return forecasts or position weights. No trade or size is authorized.</p>'+(!verified?'<p>Canonical source-run evidence is unavailable for this packet.</p>':`<p>ECB warehouse collected ${esc(packet.ciss_warehouse.generated_at)}. <a href="${esc(verified)}" target="_blank" rel="noopener">Retained source run</a> · <a href="/ciss.html">Inspect all original ECB measurements</a>.</p>`)+
      '<p>Other yield, CDS, macro and composite inputs keep their own qualification limits; CISS source verification does not certify the whole desk.</p><details><summary>Inspect dated CISS and SovCISS inputs ('+items.length+')</summary>'+items.map(item=>{
        const live=!!verified&&current(item.q),key=item.key,original=path(item.evidence?.key);
        return `<details style="margin:10px 0;padding:8px;border-top:1px solid #334252"><summary>${esc(item.name.replaceAll('_',' '))} · ${live?esc(item.value):'unavailable'} · ${esc(item.q?.observation_date)} · ${live?'fresh within stated ceilings':esc(item.q?.status==='fresh'?'expired':item.q?.status)}</summary><p>${esc(key)} · dimensionless index · acquired ${esc(item.q?.source_acquired_at)}. Source publication time is not independently available.</p><p>${key?`<a href="/ciss.html?series=${encodeURIComponent(key)}">Open exact series and verify original row</a>`:''} ${original?` · <a href="${esc(original)}" target="_blank" rel="noopener">Original CSV (.gz)</a>`:''}</p><pre style="white-space:pre-wrap;overflow-wrap:anywhere;max-height:320px;overflow:auto">${esc(JSON.stringify({comparisons:item.comparisons,quality:item.q},null,2))}</pre></details>`;
      }).join('')+'</details>';
    host.querySelectorAll('a').forEach(a=>a.style.color='#9cdbed');
    host._cissEvidencePacket=packet;
    host._cissEvidenceState=items.map(item=>current(item.q)).join('|');
  }
  if(typeof module==='object'&&module.exports)module.exports={current,rows,path};
  if(!root.document)return;
  root.JHCissDeskEvidence={render};
  setInterval(()=>{for(const host of root.document.querySelectorAll('[data-ciss-evidence]')){
    if(host._cissEvidencePacket&&host._cissEvidenceState!==rows(host._cissEvidencePacket).map(item=>current(item.q)).join('|'))render(host,host._cissEvidencePacket);
  }},60000);
})(typeof window!=='undefined'?window:globalThis);
