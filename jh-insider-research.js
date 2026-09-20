/* Exact-output verified, bounded insider filing research. */
(function(root){
 'use strict';
 const PREFIX='data/insider-research/',CURRENT='data/insider-aggregate.json';
 const permissions=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=v=>typeof v==='number'&&Number.isFinite(v);
 const fmt=v=>finite(v)?v.toLocaleString('en-US',{maximumFractionDigits:4}):'Unavailable';
 const safe=key=>/^data\/insider-research\/(?:runs|inputs|outputs|compilers)\/[a-f0-9]{64}\.(?:json|py)$/.test(key||'');
 const typed=p=>p?.contract==='insider-native-research.v1'&&p.call===null&&p.regime===null&&p.portfolio_action==='WAIT'&&permissions.every(k=>p[k]===false)&&p.coverage?.population_complete===false&&p.windows&&p.filing_windows;
 function current(p,now=Date.now()){
  const g=Date.parse(p.generated_at),d=Date.parse(p.freshness?.pipeline_check_due_at);
  return Number.isFinite(g)&&Number.isFinite(d)&&g<=now&&now<d&&now<Date.parse(p.freshness?.sample_valid_until)&&d-g<=80*3600000&&p.quality?.status==='partial';
 }
 function windowRows(group){return ['last_7d','last_30d','last_90d'].map(k=>{
  const w=group[k];if(!w)return '';
  return '<tr><th>'+fmt(w.days)+' calendar days<br><small>'+esc(w.from_date)+' to '+esc(w.through_date)+'</small></th><td>'+fmt(w.buy_count)+'</td><td>'+fmt(w.sell_count)+'</td><td>'+fmt(w.buy_sell_ratio_count)+'</td><td>'+fmt(w.unknown_currency_rows)+' / '+fmt(w.representation_count)+'</td></tr>';
 }).join('');}
 function table(group,label){return '<div class="ir-scroll" tabindex="0" role="region" aria-label="'+label+'"><table><thead><tr><th>Inclusive window</th><th>P-coded rows</th><th>S-coded rows</th><th>Count ratio</th><th>Unknown currency</th></tr></thead><tbody>'+windowRows(group)+'</tbody></table></div>';}
 function render(p,now=Date.now()){
  if(!typed(p))return '<p role="alert">Verified insider sample unavailable.</p>';
  const c=p.coverage,live=current(p,now);
  let h='<h2>Insider filing sample</h2><p class="ir-state">'+(live?'Dated partial sample':'Retained sample · current use withheld')+' · Research only · WAIT</p><p>Collected '+esc(p.generated_at)+'. Latest reported filing '+esc(p.as_of||'unavailable')+'. Next scheduled check allowance ends '+esc(p.freshness?.pipeline_check_due_at)+'.</p>';
  h+='<p>'+fmt(c.rows_received)+' rows across '+fmt(c.requested_pages)+' pages; '+fmt(c.distinct_representation_count)+' distinct row representations, '+fmt(c.duplicate_representations)+' duplicates and '+fmt(c.excluded_representations)+' excluded representations. Collection stopped: '+esc(c.stop_reason)+'. This is a bounded vendor sample, not a complete market population.</p>';
  h+='<h3>Transaction-date windows</h3><p>The dates the provider says transactions occurred. Missing or future dates are excluded; a filing date never replaces them.</p>'+table(p.windows,'Transaction-date sample windows');
  h+='<h3>Filing-date windows</h3><p>The dates the provider says filings arrived. An old transaction reported recently belongs here without becoming a new transaction.</p>'+table(p.filing_windows,'Filing-date sample windows');
  h+='<p>P/S include open-market or private transactions and derivative or non-derivative securities. These are counts of vendor representations, not independently reconciled SEC transactions. Zero sales makes the count ratio unavailable.</p>';
  h+='<p>Currency and 10b5-1 plan status are not supplied by the reviewed endpoint. Missing amounts stay unavailable; no aggregate dollar ratio or discretionary-trade claim is inferred.</p>';
  h+='<details><summary>Exclusions, security labels and sample limits</summary><ul>'+Object.entries(c.excluded_reason_counts||{}).map(([k,v])=>'<li>'+esc(k.replaceAll('_',' '))+': '+fmt(v)+'</li>').join('')+'</ul><p>Raw sample filing range '+esc(c.first_filing_date)+' to '+esc(c.latest_filing_date)+'; nonfuture transaction range '+esc(c.first_transaction_date)+' to '+esc(c.latest_transaction_date)+'. Date ranges do not establish complete coverage.</p><ul>'+Object.entries(p.security_labels||{}).map(([k,v])=>'<li>'+esc(k)+': '+fmt(v)+'</li>').join('')+'</ul><p>'+p.limitations.map(esc).join(' ')+'</p></details>';
  h+='<h3>Multiple reporting CIKs in the P-coded sample</h3><p>Grouped by issuer CIK and distinct reporting CIKs over 30 transaction-date days, across reported securities. These groups are research references; they do not rank stocks or validate insider conviction.</p>';
  h+='<ul>'+p.notable_cluster_buys.map(c=>'<li><strong>'+esc(c.reported_symbols.join(' / ')||c.issuer_cik)+'</strong> · issuer '+esc(c.issuer_cik)+' · '+fmt(c.n_buyers)+' reporting CIKs · '+fmt(c.representation_count)+' row representations · '+esc(c.first_transaction_date)+' to '+esc(c.last_transaction_date)+'</li>').join('')+'</ul>';
  if(!p.notable_cluster_buys.length)h+='<p>No multiple-CIK group in this sample; this is not evidence that no such filing exists.</p>';
  h+='<p>'+(safe(p.replay?.manifest_key)?'<a href="/'+p.replay.manifest_key+'">Inspect this retained run</a>':'Retained run unavailable')+' · <a href="/data/insider-research-verification.json">Deployment acceptance</a> · <a href="https://www.sec.gov/edgar/searchedgar/ownershipformcodes.html">SEC code definitions</a> · <a href="/insider-research.html#scenario">Enter a hypothetical exposure</a></p><p>Whole provider originals stay in the protected replay archive. Prior insider history is retained as unverified legacy snapshots and is excluded from these calculations.</p><button type="button" data-insider-refresh>Refresh and verify</button>';
  return h;
 }
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved research path');const r=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!r.ok)throw Error('HTTP '+r.status);
  const raw=await r.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(packet,fetcher,signal){
  const key=packet?.replay?.manifest_key;if(!typed(packet)||!/^data\/insider-research\/runs\/[a-f0-9]{64}\.json$/.test(key||''))throw Error('Native insider research required');
  const run=await load(key,fetcher,signal),m=run.doc;
  if(key!==PREFIX+'runs/'+await sha(run.raw)+'.json'||m.contract!=='insider-native-replay.v1'||m.output_sha256!==packet.replay.output_sha256||m.generated_at!==packet.generated_at)throw Error('Run differs');
  const ref=m.output;if(ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=packet;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current packet differs');return packet;
 }

 let pending;
 async function mount(){
  const host=root.document?.getElementById('insider-research');if(!host)return;
  if(pending)pending.abort();const c=new AbortController();pending=c;const timer=setTimeout(()=>c.abort(),15000);
  host.innerHTML='<p role="status">Verifying insider sample against retained output…</p>';
  try{const p=await verifyPacket((await load(CURRENT,root.fetch.bind(root),c.signal)).doc,root.fetch.bind(root),c.signal);
   if(c!==pending)return;host.innerHTML=render(p);host.querySelector('[data-insider-refresh]').onclick=mount;
  }catch(e){if(c===pending){host.innerHTML='<p role="alert">Verified insider sample unavailable. No substitute ratio or timing signal is shown.</p><button data-insider-refresh>Retry</button>';host.querySelector('[data-insider-refresh]').onclick=mount;}}
  finally{clearTimeout(timer);}
 }
 function scenario(exposure,shock){
  if(typeof exposure!=='string'||typeof shock!=='string'||!exposure.trim()||!shock.trim())throw Error('Enter both assumptions.');
  const x=Number(exposure),s=Number(shock);
  if(!Number.isFinite(x)||!Number.isFinite(s)||Math.abs(x)>1e12||s<-100||s>1000)throw Error('Use a signed USD exposure within 1 trillion and a shock from -100% to +1000%.');
  return x===0||s===0?0:x*s/100;
 }
 function bindScenario(){
  const form=root.document?.getElementById('insider-scenario');if(!form)return;
  form.oninput=()=>{root.document.getElementById('insider-scenario-result').textContent='Assumptions changed; calculate the entered scenario again.';};
  form.onsubmit=e=>{e.preventDefault();const out=root.document.getElementById('insider-scenario-result');try{
   const value=scenario(form.elements.exposure.value,form.elements.shock.value);out.textContent='Entered scenario: '+value.toLocaleString('en-US',{style:'currency',currency:'USD'})+' price P&L. Signed USD exposure × entered price shock. Excludes FX, dividends, fees, financing and hedges; the filing sample does not predict this shock.';
  }catch(e){out.textContent=e.message;}};
 }
 const api={render,typed,current,verifyPacket,mount,scenario,bindScenario};root.JHInsiderResearch=api;if(typeof module!=='undefined')module.exports=api;
 if(root.document){const start=()=>{mount();bindScenario();};if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',start);else start();}
})(typeof globalThis!=='undefined'?globalThis:this);
