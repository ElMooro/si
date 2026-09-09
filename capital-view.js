/* Capital permission checks and complete, lazy evidence inspection. No network. */
(function(root){
  'use strict';
  const criticalSLAs={risk_gate:30,crisis:8,bond_warroom:84,eurodollar_stress:30,credit_composite:30};
  const finite = n => typeof n === 'number' && Number.isFinite(n);
  const iso = s => typeof s === 'string' && /(?:Z|[+-]\d\d:\d\d)$/.test(s) ? Date.parse(s) : NaN;
  function permissionErrors(p, kind, now=Date.now()) {
    const errors=[];
    if (!p || typeof p !== 'object' || Array.isArray(p)) return ['Engine artifact is not an object'];
    const katlin=kind==='katlin', stamp=iso(katlin?p.generated_at:p.as_of), expiry=iso(p.expires_at);
    if(p.engine !== (katlin?'justhodl-katlin':'justhodl-risk-sizer') || (katlin?p.schema!=='1.1':p.schema_version!=='3.0')) errors.push('Unsupported engine contract');
    if(!Number.isFinite(stamp)||stamp>now+300000) errors.push('Missing or future engine timestamp');
    if(!Number.isFinite(expiry)||expiry<=now||expiry>stamp+(katlin?24:1)*3600000+1000) errors.push('Capital permission expired or has no valid deadline');
    const board=katlin?(p.war_room||{}):p, a=board.authority||{};
    if(board.entries_allowed===true) {
      const cap=katlin?board.exposure_cap_pct:p.max_gross_exposure_pct;
      const age=now-iso(a.generated_at), ae=iso(a.expires_at);
      if(a.source!=='justhodl-khalid-risk'||a.schema_version!=='1.0.0'||a.status!=='FRESH'||!['OK','DEGRADED'].includes(a.engine_status)||!finite(a.exposure_cap_pct)||a.exposure_cap_pct<0||a.exposure_cap_pct>100||a.allows_new_entries!==true||!finite(cap)||cap<0||cap>a.exposure_cap_pct) errors.push('Capital authority is invalid or inconsistent');
      if(!Number.isFinite(age)||age< -300000||age>24*3600000||!Number.isFinite(ae)||ae<=now||expiry>ae+1000) errors.push('Capital authority expired');
      if(!Array.isArray(a.hard_vetoes)||a.hard_vetoes.length||!Array.isArray(a.critical_failures)||a.critical_failures.length) errors.push('Capital authority reports unresolved vetoes or failures');
      const critical=Array.isArray(a.source_health)?a.source_health.filter(x=>x&&x.critical===true):[];
      if(Object.keys(criticalSLAs).some(name=>critical.filter(x=>x.name===name).length!==1)||critical.some(x=>{const t=iso(typeof x.as_of==='string'&&x.as_of.length===10?x.as_of+'T00:00:00Z':x.as_of), age=now-t;return x.status!=='FRESH'||!finite(x.max_age_h)||!Number.isFinite(age)||age< -300000||age>Math.min(x.max_age_h,criticalSLAs[x.name]||x.max_age_h)*3600000;})) errors.push('Critical risk evidence is missing or expired');
      if(katlin) {
        const researchAge=now-iso(p.research_generated_at), marketAge=now-iso(String(p.session||'')+'T00:00:00Z');
        if(p.research_status!=='FRESH'||!Number.isFinite(researchAge)||researchAge< -300000||researchAge>36*3600000||!Number.isFinite(marketAge)||marketAge< -300000||marketAge>96*3600000) errors.push('Research observations expired');
        const core=Array.isArray(p.basket?.core)?p.basket.core:[],barbell=Array.isArray(p.basket?.barbell)?p.basket.barbell:[],rows=[...core,...barbell];
        if(!Array.isArray(p.basket?.core)||!Array.isArray(p.basket?.barbell)||rows.some(r=>!r||!finite(r.weight_pct)||r.weight_pct<0)||rows.reduce((s,r)=>s+(finite(r?.weight_pct)?r.weight_pct:0),0)>cap+1e-8||core.some(r=>r&&r.weight_pct>10)) errors.push('Model basket exceeds its capital constraints');
      } else {
        const rows=p.sized_recommendations,available=p.book?.available_gross_pct;
        if(!Array.isArray(rows)||!finite(available)||rows.some(r=>!r||!finite(r.recommended_size_pct)||r.recommended_size_pct<0||r.recommended_size_pct>8)||rows.reduce((s,r)=>s+(finite(r?.recommended_size_pct)?r.recommended_size_pct:0),0)>available+1e-8) errors.push('Recommendation allocations do not reconcile');
      }
      if(!katlin && (p.book?.status!=='READY'||!p.final_constraint_check||['single_name_ok','cluster_ok','gross_ok'].some(k=>p.final_constraint_check[k]!==true))) errors.push('Book or final allocation constraints are not valid');
    } else if(board.entries_allowed!==false) errors.push('Entry permission is not explicit');
    return errors;
  }
  function restrictedCopy(payload,kind,errors){
    const p=payload&&typeof payload==='object'&&!Array.isArray(payload)?JSON.parse(JSON.stringify(payload)):{};
    if(!errors.length)return p;
    const b=kind==='katlin'?(p.war_room=p.war_room||{}):p;
    b.entries_allowed=false;b.hold_reasons=[...(b.hold_reasons||[]),...errors];
    b.status='DATA_HOLD'; b.posture='DATA_HOLD'; b.exposure_cap_pct=0;
    if(kind==='katlin') {p.basket={...(p.basket||{}),core:[],barbell:[],core_pct:0,barbell_pct:0,cash_pct:100,exposure_cap_pct:0,posture:'DATA_HOLD',notes:errors};b.words='Capital permission unavailable — research evidence only';}
    else {p.status='ENTRIES_BLOCKED';p.max_gross_exposure_pct=0;p.constraints_applied={...(p.constraints_applied||{}),max_gross_exposure_pct:0};p.summary={...(p.summary||{}),total_recommended_size_pct:0};p.sized_recommendations=(p.sized_recommendations||[]).map(x=>({...x,recommended_size_pct:0,blocked_reason:errors.join('; ')}));}
    return p;
  }
  function evidenceNode(name,value,doc){
    if(value===null||typeof value!=='object') {const row=doc.createElement('p');row.style.whiteSpace='pre-wrap';row.textContent=name+': '+(value===null?'null':String(value));return row;}
    const details=doc.createElement('details'),summary=doc.createElement('summary'),body=doc.createElement('div');
    summary.textContent=name+' ('+(Array.isArray(value)?value.length+' rows':Object.keys(value).length+' fields')+')';details.append(summary,body);
    details.addEventListener('toggle',function fill(){if(!details.open||body.childNodes.length)return;Object.entries(value).forEach(([k,v])=>body.append(evidenceNode(k,v,doc)));});return details;
  }
  function renderEvidence(id,payload,doc=document){
    const host=doc.getElementById(id);if(!host)return;host.replaceChildren();
    const note=doc.createElement('p');note.textContent='Complete engine evidence. Expand any field; historical or blocked evidence does not grant capital permission.';
    const download=doc.createElement('button');download.type='button';download.textContent='Download complete JSON';
    download.addEventListener('click',()=>{const url=URL.createObjectURL(new Blob([JSON.stringify(payload,null,2)],{type:'application/json'}));const a=doc.createElement('a');a.href=url;a.download=(payload.engine||'engine')+'-evidence.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
    host.append(note,download,evidenceNode('Engine artifact',payload,doc));
  }
  const api={permissionErrors,restrictedCopy,renderEvidence,evidenceNode};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.JHCapitalView=api;
})(typeof window==='object'?window:globalThis);
