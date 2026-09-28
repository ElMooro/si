(function(root){
  'use strict';
  const FLAGS=['calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const symbol=v=>typeof v==='string'&&/^[A-Z0-9][A-Z0-9.\-^]{0,24}$/.test(v);
  const label=v=>typeof v==='string'&&v.length>0&&v.length<=512&&v.trim()&&!/[\u0000-\u001f]/.test(v);
  const stamp=v=>typeof v==='string'&&/T.*(?:Z|[+-]\d\d:\d\d)$/.test(v)&&Number.isFinite(Date.parse(v))?Date.parse(v):null;
  const identity=r=>r&&Number.isSafeInteger(r.bytes)&&r.bytes>=0&&r.bytes<=2097152&&typeof r.sha256==='string'&&/^[a-f0-9]{64}$/.test(r.sha256)&&r.key==='audit-private/20260909-originals/momentum-leaders-research/classification-context/sources/'+r.sha256+'.bin';
  function render(p,now=Date.now()){
    let html='<section aria-label="Issuer classifications"><h3>Reported issuer classifications</h3><p>Research only · WAIT / abstain. Shared industry does not establish an active theme, co-movement, a forecast or position size.</p>';
    const generated=stamp(p?.generated_at),rows=p?.classification_observations,source=p?.profile_sources,selected=p?.selection?.selected_tickers;
    const ok=p&&p.measurement_contract==='issuer-classification-observations.v1'&&p.status==='research_only'&&p.call==='WAIT'&&p.call_semantics==='abstain'&&FLAGS.every(k=>p[k]===false)&&generated!==null&&generated<=now;
    const shape=Array.isArray(rows)&&Array.isArray(source)&&Array.isArray(selected)&&rows.length<=30&&rows.length===source.length&&rows.length===selected.length&&new Set(selected).size===selected.length&&selected.every(symbol);
    if(!ok||!shape)return html+'<p>Classification evidence unavailable, conflicting or legacy. Earlier active-theme labels are withheld.</p></section>';
    html+='<p>Published '+esc(p.generated_at)+'. Acquisition time is not the classification’s effective date. The first thirty members of the parent research population define this scope; it is not a ranking.</p>';
    if(!rows.length)return html+'<p>The source explicitly reports an empty research population. No active-theme count is inferred.</p></section>';
    html+='<div role="region" tabindex="0" aria-label="Issuer classification evidence table" style="max-width:100%;overflow:auto"><table style="width:100%;min-width:900px"><thead><tr><th>Issuer</th><th>Reported industry</th><th>Reported sector</th><th>Original acquisition</th><th>Source evidence</th></tr></thead><tbody>';
    for(let i=0;i<rows.length;i++){
      const r=rows[i],a=source[i],ticker=selected[i],ref=a?.original_ref,received=stamp(a?.received_at),requested=stamp(a?.requested_at);
      const bound=r&&a&&r.ticker===ticker&&a.ticker===ticker&&a.endpoint==='https://financialmodelingprep.com/stable/profile?symbol='+encodeURIComponent(ticker)&&a.status==='received'&&a.http_status===200&&identity(ref)&&identity(r.original_ref)&&ref.key===r.original_ref.key&&ref.bytes===r.original_ref.bytes&&requested!==null&&received!==null&&requested<=received&&received<=generated&&r.received_at===a.received_at&&r.requested_at===a.requested_at&&r.acquisition===a.acquisition&&((a.acquisition==='provider_request'&&a.network_attempted===true)||(a.acquisition==='retained_profile'&&a.network_attempted===false));
      const classified=bound&&r.issuer_symbol_matched===true&&r.source_pointer==='/0'&&r.status==='reported_classification'&&label(r.industry);
      html+='<tr><th scope="row">'+esc(ticker)+'</th><td>'+(classified?esc(r.industry):'Unavailable / conflicting')+'</td><td>'+(classified&&label(r.sector)?esc(r.sector):'Unavailable')+'</td><td>'+(bound?esc(a.received_at)+'<br>'+((now-received)>604800000?'Older than seven days · refresh needed':a.acquisition==='retained_profile'?'Reused · original clock preserved':'New provider acquisition'):'Unavailable')+'</td><td style="max-width:280px;overflow-wrap:anywhere">';
      html+=bound?'<details><summary>Inspect whole-source identity</summary>'+esc(ref.bytes)+' bytes<br><code>'+esc(ref.sha256)+'</code><p>Industry coordinate: <code>/0/industry</code></p></details>':'Whole-source binding unavailable';
      html+='</td></tr>';
    }
    return html+'</tbody></table></div><p>Classification age is separate from market-data freshness. Missing classifications stay unknown; no industry alias, score or theme promotion is applied.</p></section>';
  }
  const api={render};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHIssuerClassifications=api;
})(typeof globalThis==='object'?globalThis:this);
