(function(root){
 'use strict';
 const common=root.JHCrisisResearch||(typeof require==='function'?require('./jh-crisis-research.js'):{});
 const {fmt,number,plot}=common;
 const esc=x=>String(x==null?'':x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const names={official_conditions:'Official financial conditions',credit:'Credit spreads',bank_credit:'Bank credit and deposits',funding_rates:'Funding rates',liquidity_stocks:'Liquidity stocks',foreign_exchange:'Exchange rates and dollar indices',real_activity:'Real activity',yield_curves:'Yield curves',ofr_publisher:'OFR financial stress and contributions'};
 const safe=key=>typeof key==='string'&&/^data\/(?:plumbing-research|report-research|funding-research)\/(?:runs|inputs|outputs|compilers|histories)\/[a-f0-9]{64}\.(?:json|py)$/.test(key)||typeof key==='string'&&/^data\/evidence\/(?:fred|funding)\/[a-f0-9]{64}\/[a-f0-9]{64}\.bin\.gz$/.test(key);
 const link=(key,label)=>safe(key)?'<a href="/'+esc(key)+'" target="_blank" rel="noopener">'+esc(label)+'</a>':esc(label+' unavailable');
 const typed=p=>p&&p.contract==='plumbing-research.v1'&&['calls_eligible','sizing_eligible','execution_eligible','forecast_eligible'].every(k=>p[k]===false)&&p.composite?.composite_stress_score===null&&p.decision?.verb==='WAIT'&&p.measurements&&Object.keys(p.measurements).length>=53;
 const clock=s=>typeof s==='string'&&/(?:Z|[+-]\d{2}:\d{2})$/.test(s)?Date.parse(s):NaN;
 function rowCurrent(row,now){
  const q=row.quality||{},limit=q.maximum_acquisition_age_hours,days=q.maximum_observation_age_days;
  if(q.status!=='fresh'||typeof limit!=='number'||typeof days!=='number'||!/^\d{4}-\d{2}-\d{2}$/.test(row.observation_date||''))return false;
  const observed=Date.parse(row.observation_date+'T00:00:00Z'),day=Math.floor(now/86400000)-Math.floor(observed/86400000);
  return day>=0&&day<=days&&[row.acquired_at,row.source_generated_at].every(s=>{const age=now-clock(s);return age>=0&&age<=limit*3600000;});
 }
 function value(row,now){return rowCurrent(row,now)?fmt(row.value_decimal,6):'Withheld';}
 function detail(row){
  const evidence=row.evidence||{},refs=evidence.key?link(evidence.key,'Original OFR CSV'):[['definition','Series definition'],['observations','Original observations']].map(([key,label])=>link(evidence[key]?.key,label)).join(' · ');
  return '<p>'+esc(row.label)+'</p><dl><dt>Series identity</dt><dd>'+esc(row.series_id)+'</dd><dt>Native unit / frequency</dt><dd>'+esc(row.unit||'Unavailable')+' / '+esc(row.frequency||'Unavailable')+'</dd><dt>Observed</dt><dd>'+esc(row.observation_date||'Unavailable')+'</dd><dt>Original row</dt><dd>'+esc(row.source_row_index??'Unavailable')+'</dd><dt>Acquired</dt><dd>'+esc(row.acquired_at||'Unavailable')+'</dd><dt>Source compiled</dt><dd>'+esc(row.source_generated_at||'Unavailable')+'</dd><dt>Provider updated</dt><dd>'+esc(row.provider_updated_at||'Not supplied')+'</dd><dt>First publication</dt><dd>Not independently established</dd><dt>Last observed decimal</dt><dd>'+esc(row.last_observed_value??'Unavailable')+'</dd></dl><p>'+refs+' · '+link(row.source_replay?.manifest_key,'Source replay')+'</p><p>'+esc(row.note||'Publisher definitions and original history govern interpretation.')+'</p>'+(row.reason?'<p class="warn">'+esc(row.reason)+'</p>':'');
 }
 function render(packet,now=Date.now()){
  if(!typed(packet))return '<section class="notice" role="alert"><h2>Plumbing research unavailable</h2><p>The native research contract is required. Legacy scores cannot substitute for source records.</p></section>';
  const q=packet.quality||{},rows=packet.measurements;
  let html='<section class="notice"><div class="eyebrow">DECISION STATUS</div><h2>WAIT <span>· research abstains</span></h2><p>Inspect official conditions, credit, funding and currencies. These measurements do not authorize a crisis forecast or position size.</p><p class="muted">Compiled '+esc(packet.generated_at)+' · '+Object.values(rows).filter(r=>rowCurrent(r,now)).length+' / '+fmt(q.expected_measurements,0)+' measurements within age rules now.</p><p>'+link(packet.replay?.manifest_key,'Reproduce this run')+' · <a href="/data/crisis-plumbing.json">Download current packet</a> · <button data-plumbing-refresh type="button">Refresh</button></p></section>';
  html+='<section><h2>Compare instruments on an exact date</h2><p>The left-hand series supplies the observation date. Missing daily legs stay missing; monthly rates are never carried into daily quotes.</p><div class="measure-grid">'+Object.values(packet.comparisons||{}).map(c=>{
   const current=c.series_ids.every(id=>rowCurrent(rows[id]||{},now));
   return '<article><h3>'+esc(c.label)+'</h3><p class="value">'+(current?fmt(c.value_decimal):'Withheld')+(current&&number(c.value_decimal)!==null?' <small>bp</small>':'')+'</p><p class="muted">Anchor date '+esc(c.observation_date||'Unavailable')+'</p><details><summary>Definition and matched source rows</summary><p>'+esc(c.formula)+'</p><p>'+esc(c.limitation)+'</p>'+(c.reason?'<p class="warn">'+esc(c.reason)+'</p>':'')+'<ul>'+c.series_ids.map(id=>'<li>'+esc(id)+': latest '+esc(c.source_latest_dates?.[id]||'Unavailable')+'; matched original row '+esc(c.source_rows?.[id]??'Unavailable')+'</li>').join('')+'</ul></details></article>';
  }).join('')+'</div></section>';
  html+='<section><h2>Native source desk</h2><p>All 53 former FRED identities remain visible. The nine OFR columns belong to one publisher index, with overlapping category and regional decompositions.</p>'+Object.entries(packet.groups||{}).map(([group,ids])=>'<article><h3>'+esc(names[group]||group)+'</h3><div class="table-wrap" tabindex="0" role="region" aria-label="'+esc(names[group]||group)+' observations"><table><thead><tr><th>Series / original records</th><th>Current native level</th><th>Observed</th><th>Quality at compilation</th></tr></thead><tbody>'+ids.map(id=>{const row=rows[id]||{};return '<tr><td><details><summary>'+esc(id)+'</summary>'+detail(row)+'</details></td><td>'+value(row,now)+'<small>'+esc(row.unit||'Unit unavailable')+'</small></td><td>'+esc(row.observation_date||'Unavailable')+'</td><td>'+esc(row.quality?.status||'unavailable')+'</td></tr>';}).join('')+'</tbody></table></div></article>').join('')+'</section>';
  html+='<section><h2>Official-index historical context</h2><p>Midranks use prior native observations from the current source vintage, with actual span and coverage checks. They are neither crisis probabilities nor independent votes.</p><div class="two-columns">'+Object.entries(packet.official_historical_ranks||{}).map(([id,ranks])=>'<article><h3>'+esc(id)+'</h3>'+Object.values(ranks).map(rank=>'<p>'+esc(rank.years)+' years: '+(rowCurrent(rows[id],now)?fmt(rank.value,6):'Withheld')+' · '+fmt(rank.observations,0)+' prior observations</p><details><summary>'+esc(rank.years)+'-year definition and coverage</summary><p>'+esc(rank.formula)+'</p><p>'+esc(rank.reason||('From '+rank.from_date+' through the day before '+rank.as_of))+'</p>'+(rank.fraction?'<p>Exact fraction: '+esc(rank.fraction.numerator)+' / '+esc(rank.fraction.denominator)+'</p>':'')+'</details>').join('')+'</article>').join('')+'</div></section>';
  html+='<section><h2>Explore original source history</h2><div class="controls"><label for="plumbing-series">Series<select id="plumbing-series">'+Object.entries(rows).map(([id,row])=>'<option value="'+esc(id)+'"'+(id==='SOFR'?' selected':'')+'>'+esc(id)+' · '+esc(row.label)+'</option>').join('')+'</select></label><label for="plumbing-window">Window<select id="plumbing-window"><option value="63">63 native rows</option><option value="252">252 native rows</option><option value="20000">All retained native rows</option></select></label></div><div id="plumbing-history" aria-live="polite"></div></section>';
  html+='<section><h2>Portfolio consequences and limits</h2><p><a href="/position-sizer.html">Open Portfolio scenarios</a> to apply explicit rate, credit and currency shocks to entered exposures. A scenario loss is not a forecast or a target position.</p><ul>'+Object.values(packet.unavailable_claims||{}).map(x=>'<li>'+esc(x)+'</li>').join('')+'</ul><p>Source-family overlap is explicit: '+Object.entries(packet.dependency_graph?.shared_families||{}).map(([name,ids])=>esc(name.replaceAll('_',' '))+': '+esc(ids.join(', '))).join(' · ')+'. Qualified independent votes: 0.</p><details><summary>Methodology and retained context</summary><ul>'+Object.values(packet.methodology||{}).map(x=>'<li>'+esc(x)+'</li>').join('')+'</ul><p>Earlier repo context: <a href="/repo-market.html">Repo market research</a>. Its prior tail score cannot supply forecasting authority here.</p></details><p><a href="/defcon.html">Crisis &amp; DEFCON Research</a> · <a href="/eurodollar.html">Dollar funding</a> · <a href="/global-stress.html">Global Stress</a> · <a href="/fails.html">Settlement fails</a></p></section>';
  return html;
 }
 function stable(value){if(Array.isArray(value))return value.map(stable);if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(k=>[k,stable(value[k])]));return value;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),b=>b.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  if(key!=='data/crisis-plumbing.json'&&!safe(key))throw Error('Unapproved research path');
  const response=await fetcher('/'+key,{cache:key==='data/crisis-plumbing.json'?'no-store':'default',signal});if(!response.ok)throw Error('HTTP '+response.status);
  const raw=await response.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(packet,fetcher,signal){
  if(!typed(packet)||!/^data\/plumbing-research\/runs\/[a-f0-9]{64}\.json$/.test(packet.replay?.manifest_key||''))throw Error('Native research required');
  const run=await load(packet.replay.manifest_key,fetcher,signal),m=run.doc;
  if(packet.replay.manifest_key!=='data/plumbing-research/runs/'+await sha(run.raw)+'.json'||m.contract!=='plumbing-replay.v1'||m.output_sha256!==packet.replay.output_sha256||m.generated_at!==packet.generated_at)throw Error('Run differs');
  const ref=m.output;if(ref?.key!=='data/plumbing-research/outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);
  if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=packet;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current packet differs');return packet;
 }
 async function verifyHistory(row,fetcher,signal){
  const ref=row.history;if(!ref)return [];
  if(ref.key!=='data/plumbing-research/histories/'+ref.sha256+'.json')throw Error('History reference differs');
  const {raw,doc}=await load(ref.key,fetcher,signal);
  if(raw.byteLength!==ref.bytes||await sha(raw)!==ref.sha256||doc.contract!=='plumbing-history.v1'||doc.series_id!==row.id||doc.unit!==row.unit||doc.frequency!==row.frequency||JSON.stringify(doc.columns)!==JSON.stringify(['date','value_decimal','source_row_index'])||!Array.isArray(doc.rows)||doc.rows.length!==ref.observations||doc.rows.length>20000)throw Error('History identity differs');
  let previous='';const rows=doc.rows.map(r=>{if(!Array.isArray(r)||r.length!==3||!/^\d{4}-\d{2}-\d{2}$/.test(r[0])||r[0]<=previous||(r[1]!==null&&number(r[1])===null)||!Number.isInteger(r[2])||r[2]<0)throw Error('History record differs');previous=r[0];return {date:r[0],value_decimal:r[1],source_row_index:r[2]};});
  if(rows[0]?.date!==ref.first||rows.at(-1)?.date!==ref.last)throw Error('History endpoints differ');return rows;
 }
 function historyHTML(row,rows,window){return '<article><h3>'+esc(row.label)+'</h3>'+plot(rows,window,row.unit)+'<p class="muted">Exact hash-checked native history. Current provider vintage; historical first-publication availability is not reconstructed.</p><p>'+link(row.history?.key,'Download all exact rows')+'</p><details><summary>Recent original row indices and decimals</summary><div class="table-wrap"><table><thead><tr><th>Date</th><th>Native decimal</th><th>Original row</th></tr></thead><tbody>'+rows.slice(-16).reverse().map(r=>'<tr><td>'+esc(r.date)+'</td><td>'+esc(r.value_decimal??'Missing')+'</td><td>'+esc(r.source_row_index)+'</td></tr>').join('')+'</tbody></table></div></details></article>';}
 let generation=0,historyGeneration=0,ageTimer;
 async function start(){
  const host=document.getElementById('plumbing-research');if(!host)return;const serial=++generation;clearTimeout(ageTimer);
  const ctl=new AbortController(),timeout=setTimeout(()=>ctl.abort(),30000);
  host.innerHTML='<p class="loading">Loading and verifying original-source Plumbing research…</p>';
  try{
   const current=await load('data/crisis-plumbing.json',fetch,ctl.signal);const packet=await verifyPacket(current.doc,fetch,ctl.signal);if(serial!==generation)return;
   host.innerHTML=render(packet);const cache=new Map();
   async function drawHistory(){
    const request=++historyGeneration,id=document.getElementById('plumbing-series').value,window=Number(document.getElementById('plumbing-window').value),target=document.getElementById('plumbing-history'),row=packet.measurements[id];
    target.innerHTML='<p class="loading">Verifying source history…</p>';
    const hctl=new AbortController(),timer=setTimeout(()=>hctl.abort(),30000);
    try{const rows=cache.has(id)?cache.get(id):await verifyHistory(row,fetch,hctl.signal);cache.set(id,rows);if(serial===generation&&request===historyGeneration)target.innerHTML=historyHTML(row,rows,window);}
    catch(_){if(serial===generation&&request===historyGeneration)target.innerHTML='<p role="alert">Source history could not be verified. <button type="button" data-history-retry>Retry history</button></p>';}
    finally{clearTimeout(timer);}
   }
   host.onchange=event=>{if(['plumbing-series','plumbing-window'].includes(event.target.id))drawHistory();};
   host.onclick=event=>{if(event.target.closest('[data-plumbing-refresh]'))start();else if(event.target.closest('[data-history-retry]'))drawHistory();};
   drawHistory();
   // Re-evaluate at a source expiry, without refetching/resetting the desk each minute.
   function scheduleAgeCheck(){
    const now=Date.now(),deadlines=Object.values(packet.measurements).filter(r=>rowCurrent(r,now)).flatMap(r=>[
     clock(r.acquired_at)+r.quality.maximum_acquisition_age_hours*3600000,
     clock(r.source_generated_at)+r.quality.maximum_acquisition_age_hours*3600000,
     Date.parse(r.observation_date+'T00:00:00Z')+(r.quality.maximum_observation_age_days+1)*86400000]);
    const next=Math.min(...deadlines.filter(x=>Number.isFinite(x)&&x>=now));if(!Number.isFinite(next))return;
    ageTimer=setTimeout(()=>{if(serial!==generation)return;
     const id=document.getElementById('plumbing-series').value,window=document.getElementById('plumbing-window').value;
     host.innerHTML=render(packet);document.getElementById('plumbing-series').value=id;document.getElementById('plumbing-window').value=window;
     drawHistory();scheduleAgeCheck();
    },Math.min(next-now+1000,2147483647));
   }
   scheduleAgeCheck();
  }catch(_){if(serial!==generation)return;host.innerHTML='<section class="notice" role="alert"><h2>Plumbing research temporarily unavailable</h2><p>The current source packet and its retained output could not be verified.</p><button data-plumbing-refresh type="button">Retry</button></section>';host.onclick=event=>{if(event.target.closest('[data-plumbing-refresh]'))start();};}
  finally{clearTimeout(timeout);}
 }
 const api={typed,rowCurrent,render,verifyPacket,verifyHistory,historyHTML,safe};root.JHPlumbingResearch=api;
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 if(typeof document!=='undefined')start();
})(typeof globalThis!=='undefined'?globalThis:this);
