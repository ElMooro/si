/* jh-reskin-skip */
(function(root){
 'use strict';
 const PREFIX='data/bond-desk-research/flows/',LIMIT=16*1024*1024;
 const transport=typeof module!=='undefined'&&module.exports?require('./jh-fifx-research.js'):root.JHFIFXResearch;
 const esc=v=>String(v??'Unavailable').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const sort=v=>Array.isArray(v)?v.map(sort):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sort(v[k])])):v;
 const same=(a,b)=>JSON.stringify(sort(a))===JSON.stringify(sort(b));
 function reference(ref,kind){return ref&&/^[a-f0-9]{64}$/.test(ref.sha256)&&ref.key===PREFIX+kind+'/'+ref.sha256+'.json'&&Number.isInteger(ref.bytes)&&ref.bytes>0&&ref.bytes<=LIMIT;}
 const json=raw=>JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
 async function artifact(ref,kind,fetcher,crypto){
  if(!reference(ref,kind))throw Error('Invalid cohort coordinates');
  const raw=await transport.bytes('/'+ref.key+'?exact=1&nogen=1',fetcher,LIMIT,20000);
  if(raw.length!==ref.bytes||await transport.hash(raw,crypto)!==ref.sha256)throw Error('Cohort artifact differs');
  return json(raw);
 }
 function research(p){return p?.contract==='bond-flow-cohorts.v1'&&['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'].every(k=>p[k]===false)&&p.independent_votes===0&&same(p.decision,{verb:'WAIT',meaning:'abstain'})&&p.source?.original_replay_verified_here===false&&p.upstream_binding?.issuer_originals_replayed_here===false;}
 async function verify(p,fetcher=root.fetch,crypto=root.crypto){
  if(!research(p))throw Error('Cohort contract unavailable');
  const key=p.replay?.manifest_key;
  if(typeof key!=='string'||!/^data\/bond-desk-research\/flows\/runs\/[a-f0-9]{64}\.json$/.test(key))throw Error('Cohort run unavailable');
  const raw=await transport.bytes('/'+key+'?exact=1&nogen=1',fetcher,LIMIT,20000);
  if(key!==PREFIX+'runs/'+await transport.hash(raw,crypto)+'.json')throw Error('Cohort run differs');
  const run=json(raw);
  if(run.contract!=='bond-flow-replay.v1'||run.evaluated_at!==p.evaluated_at||run.output?.sha256!==p.replay.output_sha256)throw Error('Cohort binding differs');
  const [out,proof,input]=await Promise.all([artifact(run.output,'outputs',fetcher,crypto),artifact(run.proof,'proofs',fetcher,crypto),artifact(run.input,'inputs',fetcher,crypto)]);
  if(!same(out,Object.fromEntries(Object.entries(p).filter(([k])=>k!=='replay')))||!same(proof,p.arithmetic_checks)||proof.all_checks_passed!==true||p.source.sha256!==run.input.sha256||p.source.bytes!==run.input.bytes||input.contract!=='etf-original-research.v1'||!same(input.replay,p.source.replay)||input.generated_at!==p.source.generated_at)throw Error('Whole source/output/proof differs');
  return run;
 }
 function link(key,label){return typeof key==='string'&&/^data\/(?:etf-research\/(?:histories|runs)\/[a-f0-9]{64}\.json|evidence\/etf_original\/[a-f0-9]{64}\/[a-f0-9]{64}\.bin\.gz|bond-desk-research\/flows\/(?:inputs|runs)\/[a-f0-9]{64}\.json)$/.test(key)?'<a href="/'+key+'?exact=1&amp;nogen=1">'+esc(label)+'</a>':'Source link unavailable';}
 const label=v=>esc(String(v).replace(/_/g,' '));
 function amount(v){if(v==null)return 'Unavailable';const parts=String(v).split('.');parts[0]=parts[0].replace(/\B(?=(\d{3})+(?!\d))/g,',');return esc(parts.join('.'));}
 function render(p,run){
  let rows='',details='';
  for(const [name,cohort] of Object.entries(p.cohorts)){
   for(const [period,w] of Object.entries(cohort.windows).sort((a,b)=>a[1].observations-b[1].observations)){
    rows+='<tr><td>'+label(name)+'</td><td>'+esc(w.observations)+'</td><td>'+esc(w.start_date)+' → '+esc(w.end_date)+'</td><td>'+esc(w.included_count)+' / '+esc(w.configured_count)+'</td><td>'+amount(w.coverage_subtotal_decimal)+'</td><td>'+amount(w.complete_cohort_decimal)+'</td><td>'+label(w.status)+'</td></tr>';
    details+='<details><summary>'+label(name)+' · '+esc(w.observations)+' observations · every configured fund</summary><div style="overflow:auto"><table><thead><tr><th>Fund</th><th>Result</th><th>Exact USD estimate</th><th>Observation interval</th><th>Acquired</th><th>Evidence</th></tr></thead><tbody>';
    for(const m of w.members)details+='<tr><td>'+esc(m.ticker)+'</td><td>Included in subtotal</td><td>'+amount(m.value_decimal)+'</td><td>'+esc(m.start_date)+' → '+esc(m.end_date)+'</td><td>'+esc(m.acquired_at)+'</td><td>'+link(m.history?.key,'Full issuer history')+' · '+link(m.original_evidence?.key,'Retained original')+'</td></tr>';
    for(const m of w.excluded)details+='<tr><td>'+esc(m.ticker)+'</td><td>'+label(m.reason)+'</td><td colspan="4">Unavailable</td></tr>';
    details+='</tbody></table></div></details>';
   }
  }
  return '<h2>DATED ETF NET-ISSUANCE COHORTS</h2><p>Retained snapshot evaluated '+esc(p.evaluated_at)+'. Source published '+esc(p.source.generated_at)+'. These are acquisition/publication clocks; the observation interval is shown separately for every estimate.</p><p>'+esc(p.configured_funds)+' configured funds across '+Object.keys(p.cohorts).length+' cohorts. Partial subtotals cover only listed funds; unavailable members are never zero. USD estimates value reported share changes at issuer NAV. They do not measure investor transfers, underlying trades or future returns.</p><p>Browser verification: complete source packet, output and arithmetic-proof hashes match the run. The browser does not rerun issuer workbooks or certify historical information availability. WAIT remains abstention.</p><p>'+link(run.input.key,'Complete retained source packet')+' · '+link(p.source.replay?.manifest_key,'Upstream issuer replay')+' · '+link(p.replay.manifest_key,'Cohort replay manifest')+'</p><div style="overflow:auto"><table><thead><tr><th>Cohort</th><th>Observations</th><th>Start → end</th><th>Covered / configured</th><th>Coverage subtotal · USD</th><th>Complete cohort · USD</th><th>Status</th></tr></thead><tbody>'+rows+'</tbody></table></div><p>Precision sensitivity is not a confidence interval. The complete packet contains all exact sensitivities and source coordinates. Twenty observations are not relabeled twenty-one days. Shared issuers do not create independent votes.</p>'+details;
 }
 async function mount(node,packet,fetcher=root.fetch,crypto=root.crypto){
  if(!node)return;
  node.textContent='Dated issuer cohort publication pending; no replacement flow total is inferred from the predecessor.';
  if(!packet)return;
  try{const run=await verify(packet,fetcher,crypto);node.innerHTML=render(packet,run);}
  catch(error){node.textContent='Cohort evidence verification unavailable. Estimates are withheld; the complete reported packet remains available below.';}
 }
 const api={reference,research,verify,render,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHBondCohorts=api;
})(typeof globalThis!=='undefined'?globalThis:this);
