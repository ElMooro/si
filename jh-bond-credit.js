/* jh-reskin-skip */
(function(root){
 'use strict';
 const PREFIX='data/bond-desk-research/credit/',MAX=4*1024*1024;
 const io=typeof module!=='undefined'&&module.exports?require('./jh-fifx-research.js'):root.JHFIFXResearch;
 const esc=v=>String(v??'Unavailable').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const sorted=v=>Array.isArray(v)?v.map(sorted):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sorted(v[k])])):v;
 const same=(a,b)=>JSON.stringify(sorted(a))===JSON.stringify(sorted(b));
 const json=raw=>JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
 async function artifact(ref,kind,fetcher,crypto){
  if(!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!Number.isInteger(ref.bytes)||ref.bytes<=0||ref.bytes>MAX)throw Error('Invalid credit coordinates');
  const raw=await io.bytes('/'+ref.key+'?exact=1&nogen=1',fetcher,MAX,20000);
  if(raw.length!==ref.bytes||await io.hash(raw,crypto)!==ref.sha256)throw Error('Credit bytes differ');return json(raw);
 }
 async function verify(p,fetcher=root.fetch,crypto=root.crypto){
  if(p?.contract!=='bond-credit-comparisons.v1'||!['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'].every(k=>p[k]===false)||p.independent_votes!==0||!same(p.decision,{verb:'WAIT',meaning:'abstain'})||p.upstream_binding?.credit_originals_replayed_here!==false)throw Error('Credit research contract unavailable');
  const key=p.replay?.manifest_key;
  if(typeof key!=='string'||!/^data\/bond-desk-research\/credit\/runs\/[a-f0-9]{64}\.json$/.test(key))throw Error('Credit run unavailable');
  const raw=await io.bytes('/'+key+'?exact=1&nogen=1',fetcher,MAX,20000);
  if(key!==PREFIX+'runs/'+await io.hash(raw,crypto)+'.json')throw Error('Credit run differs');
  const run=json(raw);
  if(run.contract!=='bond-credit-replay.v1'||run.evaluated_at!==p.evaluated_at||run.output?.sha256!==p.replay.output_sha256)throw Error('Credit binding differs');
  const [input,out,proof]=await Promise.all([artifact(run.input,'inputs',fetcher,crypto),artifact(run.output,'outputs',fetcher,crypto),artifact(run.proof,'proofs',fetcher,crypto)]);
  if(!same(out,Object.fromEntries(Object.entries(p).filter(([k])=>k!=='replay')))||!same(proof,p.arithmetic_checks)||proof.checks_passed!==true||p.source.sha256!==run.input.sha256||p.source.bytes!==run.input.bytes||input.contract!=='credit-native-research.v1'||input.generated_at!==p.source.generated_at||!same(input.replay,p.source.replay))throw Error('Complete source and credit projection differ');
  return run;
 }
 function link(key,label){return typeof key==='string'&&/^data\/(?:credit-research\/runs|bond-desk-research\/credit\/(?:inputs|runs))\/[a-f0-9]{64}\.json$/.test(key)?'<a href="/'+key+'?exact=1&amp;nogen=1">'+esc(label)+'</a>':'Evidence link unavailable';}
 function render(p,run){
  let rows='',details='';
  for(const [name,r] of Object.entries(p.comparisons)){
   rows+='<tr><td>'+esc(name.replace(/_/g,' '))+'</td><td>'+esc(r.value_decimal)+'</td><td>Basis points</td><td>'+esc(r.observation_date)+'</td><td>'+esc((r.reason||r.status).replace(/_/g,' '))+'</td></tr>';
   details+='<details><summary>'+esc(name.replace(/_/g,' '))+' · components and source coordinates</summary><p>'+esc(r.formula)+'</p><div style="overflow:auto"><table><thead><tr><th>Series</th><th>Exact percent value</th><th>Observation</th><th>Original row index</th><th>Definition</th></tr></thead><tbody>';
   for(const c of r.components){const url=/^[A-Z0-9]+$/.test(c.series_id)?'https://fred.stlouisfed.org/series/'+c.series_id:null;details+='<tr><td>'+esc(c.series_id)+'</td><td>'+esc(c.value_percent_decimal)+'</td><td>'+esc(c.observation_date)+'</td><td>'+esc(c.original_row_index)+' (zero-based)</td><td>'+(url?'<a href="'+url+'">Official series definition</a>':'Unavailable')+'</td></tr>';}
   details+='</tbody></table></div><p>Complete source responses remain in the existing protected archive. The public upstream replay manifest identifies their hashes and the reviewed compiler; original replay requires the authorized AWS runner.</p></details>';
  }
  return '<h2>DATED CREDIT COMPARISONS</h2><p>Snapshot evaluated '+esc(p.evaluated_at)+'; source published '+esc(p.source.generated_at)+'. Publication time is separate from each observation date. These are index option-adjusted-spread differences across different baskets, not executable spreads or default probabilities.</p><p>Browser checks bind the complete source, output and arithmetic proof. Whole protected originals were qualified separately on the runner; this browser does not replay them or establish investment performance. WAIT remains abstention.</p><p>'+link(run.input.key,'Complete retained credit packet')+' · '+link(p.source.replay?.manifest_key,'Upstream original replay')+' · '+link(p.replay.manifest_key,'Comparison replay')+'</p><div style="overflow:auto"><table><thead><tr><th>Comparison</th><th>Exact difference</th><th>Unit</th><th>Observed</th><th>Status</th></tr></thead><tbody>'+rows+'</tbody></table></div>'+details;
 }
 async function mount(node,p,fetcher=root.fetch,crypto=root.crypto){
  if(!node)return;node.textContent='Dated credit projection publication pending; legacy values below remain unqualified.';
  if(!p)return;
  try{const run=await verify(p,fetcher,crypto);node.innerHTML=render(p,run);}
  catch(error){node.textContent='Credit evidence verification unavailable. Comparisons are withheld; the complete reported packet remains available below.';}
 }
 const api={verify,render,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHBondCredit=api;
})(typeof globalThis!=='undefined'?globalThis:this);
