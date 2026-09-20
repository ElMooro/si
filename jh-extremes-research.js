(function(root){'use strict';
 const PREFIX='data/extremes-research/',CONTRACT='extremes-native-research.v1';
 const flags=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const esc=x=>String(x??'Unavailable').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const fmt=x=>typeof x==='number'&&Number.isFinite(x)?x.toLocaleString('en-US',{maximumFractionDigits:6}):'Unavailable';
 const engines=['capitulation','market-extremes'];
 function safe(key){return typeof key==='string'&&/^data\/(?:extremes|crisis|breadth|credit|volatility|eurodollar|insider|aaii|fails|vrp|retail)-research\/(?:runs|outputs)\/[a-f0-9]{64}\.json$/.test(key);}
 function typed(p){return p?.contract===CONTRACT&&engines.includes(p.engine)&&flags.every(k=>p[k]===false)&&p.call===null&&p.signal===null&&p.capitulation_score===null&&p.cycle_position===null&&p.posture===null&&Array.isArray(p.measurements)&&p.measurements.every(r=>flags.every(k=>r[k]===false)&&typeof r.value==='number'&&Number.isFinite(r.value)&&typeof r.unit==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(r.observation_date)&&Number.isFinite(Date.parse(r.valid_until)))&&p.quality&&p.eligibility&&p.dependency_graph&&Number.isFinite(Date.parse(p.generated_at))&&Number.isFinite(Date.parse(p.freshness?.pipeline_check_due_at));}
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  const current=engines.some(e=>key==='data/'+e+'.json');if(!current&&!safe(key))throw Error('Unapproved research path');
  const r=await fetcher('/'+key,{cache:current?'no-store':'default',signal});if(!r.ok)throw Error('HTTP '+r.status);
  const raw=await r.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(p,fetcher,signal){
  if(!typed(p)||!/^data\/extremes-research\/runs\/[a-f0-9]{64}\.json$/.test(p.replay?.manifest_key||''))throw Error('Native synthesis required');
  const key=p.replay.manifest_key,run=await load(key,fetcher,signal),m=run.doc;
  if(key!==PREFIX+'runs/'+await sha(run.raw)+'.json'||m.contract!=='extremes-native-replay.v1'||m.engine!==p.engine||m.generated_at!==p.generated_at||m.output_sha256!==p.replay.output_sha256)throw Error('Retained run differs');
  const ref=m.output;if(ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=p;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current body differs');return p;
 }
 function link(ref,label){return safe(ref?.manifest_key)?'<a href="/'+ref.manifest_key+'">'+esc(label)+'</a>':esc(label)+' · evidence unavailable';}
 function current(p,at){return Date.parse(p.generated_at)<=at&&at<Date.parse(p.freshness.pipeline_check_due_at);}
 function render(p,at=Date.now()){
  if(!typed(p))return '<p role="status">Verified native synthesis unavailable. No cycle score or portfolio recommendation.</p>';
  const active=current(p,at),g=p.dependency_graph,conflicts=[...(g.same_series_date_conflicts||[]),...(g.definition_conflicts||[])];
  let h='<h2>'+esc(p.engine==='capitulation'?'Capitulation research':'Market extremes research')+'</h2><p class="xr-state">'+(active?'Dated partial research':'Dated retained research · refresh overdue')+' · WAIT means abstention</p>';
  h+='<p>Compiled '+esc(p.generated_at)+'. Pipeline check due '+esc(p.freshness.pipeline_check_due_at)+'. Each observation has its own date and expiry.</p>';
  h+='<p>No validated market-turn forecast, probability, target allocation or trade instruction. An unavailable input never becomes a neutral score. WAIT does not certify an existing portfolio as safe.</p>';
  h+='<h3>Input eligibility</h3><div class="xr-scroll" role="region" aria-label="Source eligibility" tabindex="0"><table><thead><tr><th>Input</th><th>Research status at compilation</th><th>Observed rows</th><th>Published</th><th>Evidence</th></tr></thead><tbody>';
  for(const [name,e] of Object.entries(p.eligibility))h+='<tr><th scope="row">'+esc(name)+'</th><td>'+esc(e.reason.replaceAll('_',' '))+'</td><td>'+fmt(e.measurement_count)+'</td><td>'+esc(e.generated_at)+'</td><td>'+link(e.upstream_replay,'Retained upstream run')+'</td></tr>';
  h+='</tbody></table></div><p>These counts describe available research, not independent votes. Legacy valuation, retail or VRP labels receive no authority from this wrapper.</p>';
  h+='<h3>Shared sources and disagreements</h3><p>'+fmt(Object.keys(g.series).length)+' recorded series identities · '+fmt(g.same_series_date_overlaps.length)+' repeated series/date groups · '+fmt(conflicts.length)+' conflicts · zero qualified investment votes. A provider family or a series count is not a measure of statistical independence.</p>';
  if(conflicts.length)h+='<ul>'+conflicts.map(c=>'<li>'+esc(c.series_id)+' · '+esc(c.observation_date)+' · '+(c.values?c.values.map(v=>esc(v.source)+': '+fmt(v.value)).join('; '):esc(c.units.join(' / ')))+' · no automatic winner</li>').join('')+'</ul>';
  h+='<details><summary>Inspect repeated inputs and provider families</summary><ul>'+g.same_series_date_overlaps.map(c=>'<li>'+esc(c.series_id)+' · '+esc(c.observation_date)+' · '+esc(c.sources.join(', '))+'</li>').join('')+'</ul><ul>'+Object.entries(g.provider_families).map(([name,ids])=>'<li>'+esc(name)+': '+esc(ids.join(', '))+'</li>').join('')+'</ul><p>'+esc(g.note)+'</p></details>';
  if(p.contexts.capitulation?.note)h+='<p>'+esc(p.contexts.capitulation.note)+' '+link(p.contexts.capitulation.upstream_replay,'Capitulation lineage')+'</p>';
  h+='<h3>Dated measurements</h3><p>Values retain their original unit. Percent is not basis points; survey spreads use percentage points. Repeated series stay visible so the source chain can be inspected.</p><div class="xr-scroll" role="region" aria-label="Dated research measurements" tabindex="0"><table><thead><tr><th>Series / source</th><th>Value</th><th>Unit</th><th>Observed</th><th>Current use</th><th>Trace</th></tr></thead><tbody>';
  for(const r of p.measurements){const usable=active&&at<Date.parse(r.valid_until);h+='<tr><th scope="row">'+esc(r.series_id)+'<br><small>'+esc(r.source_engine)+' · '+esc(r.label)+'</small></th><td>'+fmt(r.value)+'</td><td>'+esc(r.unit)+'</td><td>'+esc(r.observation_date)+'</td><td>'+(usable?'Dated context':'Expired context')+'<br><small>until '+esc(r.valid_until)+'</small></td><td>'+link(r.upstream_replay,'Source run')+'<br><small>'+esc(r.source_path)+'</small></td></tr>';}
  h+='</tbody></table></div>';if(!p.measurements.length)h+='<p>No current verified measurements in this retained synthesis.</p>';
  const scopes=p.pd_settlement_fails?.scopes;
  if(scopes){h+='<h3>Settlement fails · separate scopes</h3><ul>';for(const [name,s] of Object.entries(scopes))h+='<li>'+esc(name)+': FTD '+fmt(s.ftd_bn)+' + FTR '+fmt(s.ftr_bn)+' = '+fmt(s.combined_bn)+' USD billion · '+esc(s.as_of)+'</li>';h+='</ul><p>'+esc(p.pd_settlement_fails.note)+'</p>';}
  if(p.contexts.insider?.coverage)h+='<p>Insider context: '+fmt(p.contexts.insider.coverage.rows_received)+' vendor rows in a bounded filing sample. Currency, plan and transaction identity limits remain. <a href="/insider-research.html">Inspect the filing research</a>.</p>';
  if(p.contexts.retail?.note)h+='<p>Attention context: '+esc(p.contexts.retail.note)+' <a href="/retail/">Inspect dated community and message samples</a>. Valid until '+esc(p.contexts.retail.source_valid_until)+'.</p>';
  h+='<h3>Reproduce this decision</h3><p>'+link(p.replay,'Inspect retained synthesis run')+' · <a href="/data/extremes-research-verification.json">Deployment acceptance</a></p><p>'+esc(p.replay_scope)+'</p><p>Whole upstream snapshots are retained privately. Prior cycle history remains archived as unverified legacy output and is excluded from this calculation.</p><button type="button" data-extremes-refresh>Refresh and verify</button>';
  return h;
 }
 function scenario(exposure,shock){
  if(String(exposure).trim()===''||String(shock).trim()==='')throw Error('Enter both assumptions');
  const n=Number(exposure),s=Number(shock);if(!Number.isFinite(n)||!Number.isFinite(s)||Math.abs(n)>1e12||s< -100||s>1000)throw Error('Use finite exposure within ±1 trillion USD and shock from -100% to +1000%');
  const result=n*s/100;return Object.is(result,-0)?0:result;
 }
 function bindScenario(){const form=root.document?.getElementById('extremes-scenario'),out=root.document?.getElementById('extremes-scenario-output');if(!form||!out)return;
  form.oninput=()=>{out.textContent='Assumptions changed; calculate the entered scenario again.';};
  form.onsubmit=e=>{e.preventDefault();try{out.textContent='Entered scenario: '+scenario(form.elements.exposure.value,form.elements.shock.value).toLocaleString('en-US',{style:'currency',currency:'USD'})+' price P&L. Signed exposure × entered shock; no forecast probability, financing, FX, dividends, fees or hedge response.';}catch(err){out.textContent=err.message;}};
 }
 async function mount(node){const engine=node.dataset.extremesEngine;if(!engines.includes(engine))return;
  let controller,sequence=0;
  async function refresh(){controller?.abort();controller=new AbortController();const seq=++sequence;node.innerHTML='<p role="status">Verifying retained research…</p>';
   try{const p=(await load('data/'+engine+'.json',root.fetch.bind(root),controller.signal)).doc;await verifyPacket(p,root.fetch.bind(root),controller.signal);if(seq!==sequence)return;if(p.engine!==engine)throw Error('Engine differs');node.innerHTML=render(p);node.querySelector('[data-extremes-refresh]').onclick=refresh;}
   catch(err){if(seq!==sequence||err.name==='AbortError')return;node.innerHTML='<p role="status">Verified current research unavailable. No cycle score or portfolio recommendation.</p><button type="button" data-extremes-refresh>Retry verification</button>';node.querySelector('[data-extremes-refresh]').onclick=refresh;}
  }await refresh();
 }
 const api={typed,render,verifyPacket,scenario,bindScenario,load,current};if(typeof module==='object'&&module.exports)module.exports=api;
 root.JHExtremesResearch=api;if(root.document){const start=()=>{root.document.querySelectorAll('[data-extremes-engine]').forEach(mount);bindScenario();};if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',start);else start();}
})(typeof globalThis==='object'?globalThis:this);
