(function(root){
 'use strict';
 const contract='risk-gate-research.v1',limits={D:10,W:21,BW:35,M:100,Q:200,SA:370,A:550};
 const esc=v=>String(v==null?'—':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const path=k=>typeof k==='string'&&/^data\/(evidence|risk-gate-research|report-research)\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(k,label)=>path(k)?`<a href="${esc(path(k))}" target="_blank" rel="noopener">${esc(label)}</a>`:'Evidence unavailable';
 function status(row,packet,now=Date.now()){
  if(row.quality?.status!=='fresh')return row.quality?.status||'unavailable';
  const dates=[Date.parse(row.acquired_at),Date.parse(packet.source_generated_at),Date.parse(packet.generated_at)];
  const observed=Date.parse(row.latest_date+'T00:00:00Z'),day=Math.floor(now/86400000)*86400000;
  if(!dates.every(d=>Number.isFinite(d)&&d<=now)||!Number.isFinite(observed)||observed>day||!limits[row.frequency])return 'invalid_clock';
  if(dates.some(d=>now-d>26*3600000))return 'stale_source';
  if((day-observed)/86400000>limits[row.frequency])return 'stale_observation';
  return row.available&&row.latest_value_decimal!=null&&Number.isFinite(Number(row.latest_value_decimal))?'fresh':'unavailable';
 }
 function chart(rows){
  const points=(rows||[]).filter(r=>Number.isFinite(Date.parse(r.date))).slice().reverse();
  const values=points.filter(r=>Number.isFinite(r.value)),dates=points.map(r=>Date.parse(r.date));
  if(values.length<2)return '<p>Not enough embedded history for a chart.</p>';
  const start=Math.min(...dates),end=Math.max(...dates),min=Math.min(...values.map(r=>r.value)),max=Math.max(...values.map(r=>r.value));
  let d='',open=false;
  for(const row of points){if(!Number.isFinite(row.value)){open=false;continue;}const x=10+580*(Date.parse(row.date)-start)/(end-start||1),y=max===min?60:110-100*(row.value-min)/(max-min);d+=(open?'L':'M')+x.toFixed(2)+' '+y.toFixed(2)+' ';open=true;}
  return `<svg viewBox="0 0 600 120" role="img" aria-label="Dated original observations; missing rows break the line" style="width:100%;max-height:160px"><path d="${d}" fill="none" stroke="#67d8c0" stroke-width="2"/></svg><p class="muted">Embedded history ${esc(points[0].date)} to ${esc(points.at(-1).date)} · range ${esc(min)} to ${esc(max)} in native units. Date-spaced; missing values are gaps.</p>`;
 }
 function rowHTML(row,packet,horizon,now){
  const state=status(row,packet,now),fresh=state==='fresh',change=fresh?row.calendar_comparisons?.[horizon]:null;
  const changeText=change?.change_decimal!=null?`${esc(change.change_decimal)} ${esc(change.change_unit)}`:'Unavailable';
  return `<article class="rg-observation"><div class="rg-row"><div><h3>${esc(row.series_id)} · ${esc(row._label)}</h3><p class="muted">${esc(state.replaceAll('_',' '))} · ${esc(row.frequency)} · ${esc(row.seasonal_adjustment)}</p></div><div><strong>${fresh?esc(row.latest_value_decimal):'Unavailable'}</strong><p>${esc(row._units)}</p></div><div><strong>${changeText}</strong><p>${esc(horizon)} change · baseline ${esc(change?.baseline_date)}</p></div></div>
   <p>Observed ${esc(row.latest_date)} · retrieved ${esc(row.acquired_at)}</p>
   <details data-series="${esc(row.series_id)}"><summary>Inspect definition, calculations and original evidence</summary>
   <p>${esc(row.source_definition?.title||row.error||'Original provider definition unavailable.')}</p><p>Native source units are preserved. A reported percentage-point change is distinct from relative percent growth.</p>
   ${!fresh&&row.last_observed_value!=null?`<p>Last observed ${esc(row.last_observed_value)} ${esc(row._units)}. Historical context only.</p>`:''}
   <p>Calendar target ${esc(change?.target_date)}, baseline ${esc(change?.baseline_date)} = ${esc(change?.baseline_decimal)} ${esc(row._units)}; relative change ${esc(change?.pct_change)}%. ${esc(change?.relative_change_reason||'')}</p>
   <p>${link(row.evidence?.observations?.key,'Original FRED observations (.gz)')} · ${link(row.evidence?.definition?.key,'Official series definition (.gz)')} · ${link(row.source_replay?.manifest_key,'Macro source replay')}</p>
   <p>Original observation row ${esc(row.source_row)} (zero based); provider updated ${esc(row.provider_updated_at)}. Original publication time is not independently verified.</p>
   ${row.evidence?.observations?`<button type="button" data-verify="${esc(row.series_id)}">Verify original row and hash</button><p role="status" data-result="${esc(row.series_id)}"></p>`:''}
   ${chart(row.history)}
   <p>Current-vintage descriptive statistics: ${[1,5].map(y=>{const s=row.statistics?.[y+'y'];return `${y}y z ${fresh&&Number.isFinite(s?.z)?s.z.toFixed(3):'unavailable'} (${esc(s?.status||'unavailable')}; ${esc(s?.numeric_observations)} numeric, ${esc(s?.missing_observations)} missing)`;}).join(' · ')}. Population mean and standard deviation include the latest value. These are not return forecasts.</p>
   </details></article>`;
 }
 function derivedHTML(packet,now){
  const items=Object.values(packet.derived||{});
  return '<section class="method"><h2>Matched-date comparisons</h2><p>Exact arithmetic, with zero independent votes and no sizing authority. Different latest observation dates are never filled to make a comparison.</p>'+items.map(d=>{
   const available=d.status==='descriptive'&&(d.series_ids||[]).every(id=>status(packet.series[id]||{},packet,now)==='fresh');
   return `<details><summary>${esc(d.label)} · ${available?esc(d.value_decimal)+' '+esc(d.unit):'Unavailable'}</summary><p>${esc(d.formula)} · observed ${esc(d.observation_date)}</p><p>${esc(d.limitation)}</p><p>${available?esc(d.reason):'Current matched observations unavailable; see source clocks and gaps below.'}</p>${(d.inputs||[]).map(r=>`<p>${esc(r.series_id)} = ${esc(r.latest_value_decimal)} ${esc(r._units)} on ${esc(r.latest_date)} · original row ${esc(r.source_row)} · ${link(r.evidence?.observations?.key,'Original observations')}</p>`).join('')}</details>`;
  }).join('')+'</section>';
 }
 function contextHTML(packet){
  return '<section class="method"><h2>Dependence and retained context</h2><p>'+esc(packet.dependency_note)+'</p>'+Object.entries(packet.dependency_groups||{}).map(([k,v])=>'<p>'+esc(k.replaceAll('_',' '))+': '+esc(v.join(', '))+'</p>').join('')+'<details><summary>Existing fleet inputs retained for review</summary><p>These inputs remain unqualified and contribute zero votes or sizing authority. Their exact contents are retained in the complete desk replay input.</p>'+Object.entries(packet.fleet_context?.inputs||{}).map(([k,v])=>'<p>'+esc(k)+' · '+esc(v.generated_at)+' · SHA-256 '+esc(v.content_sha256)+'</p>').join('')+'</details></section>';
 }
 function render(packet,selection={},now=Date.now()){
  if(packet?.contract!==contract||!packet.series||!packet.replay?.manifest_key)throw Error('Canonical Risk Gate research unavailable');
  const all=Object.values(packet.series),fresh=all.filter(r=>status(r,packet,now)==='fresh').length;
  const filter=(selection.filter||'').toLowerCase(),horizon=['week','month','quarter','year'].includes(selection.horizon)?selection.horizon:'month';
  const rows=all.filter(r=>(!selection.category||r._category===selection.category)&&[r.series_id,r._label,r._category].join(' ').toLowerCase().includes(filter));
  return `<section class="rg-summary"><h2>${fresh} / ${all.length} measurements current</h2><p>${all.length-fresh} unavailable or outside the stated age ceilings. Missing inputs remain visible.</p><p>Source packet ${esc(packet.source_generated_at)} · desk ${esc(packet.generated_at)}</p><p>${link(packet.replay.manifest_key,'Replay this complete desk')} · <a href="/data/ops/releases/justhodl-risk-gate.json">Deployed runtime</a></p></section>`+
   derivedHTML(packet,now)+rows.map(r=>rowHTML(r,packet,horizon,now)).join('')+(!rows.length?'<p>No matching series.</p>':'')+contextHTML(packet);
 }
 function decimal(value){
  const m=String(value).trim().match(/^([+-]?)(\d*)\.?([0-9]*)(?:[eE]([+-]?\d+))?$/);
  if(!m||!(m[2]+m[3]).length)throw Error('Invalid source decimal');
  const exponent=Number(m[4]||0);if(!Number.isInteger(exponent)||Math.abs(exponent)>500)throw Error('Source decimal exceeds bound');
  let digits=(m[2]+m[3]).replace(/^0+/,'')||'0',scale=m[3].length-exponent;
  if(digits==='0')return '0';while(digits.endsWith('0')){digits=digits.slice(0,-1);scale--;}
  return (m[1]==='-'?'-':'')+digits+'e'+(-scale);
 }
 async function verify(row,fetcher=root.fetch.bind(root)){
  const receipt=row.evidence?.observations,url=path(receipt?.key);
  if(!url||!Number.isInteger(row.source_row)||row.source_row<0||!/^[a-f0-9]{64}$/.test(receipt?.sha256||''))throw Error('Original row evidence unavailable');
  const response=await fetcher(url+'?exact=1',{cache:'no-store'});if(!response.ok)throw Error('Original source HTTP '+response.status);
  let bytes=new Uint8Array(await response.arrayBuffer());const limit=4*1024*1024;
  if(bytes.length>limit)throw Error('Original source exceeds size bound');
  if(bytes[0]===31&&bytes[1]===139){
   const reader=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();let chunks=[],total=0;
   while(true){const {done,value}=await reader.read();if(done)break;total+=value.length;if(total>limit){await reader.cancel();throw Error('Expanded original exceeds size bound');}chunks.push(value);}
   bytes=new Uint8Array(total);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
  }
  const hash=Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
  if(hash!==receipt.sha256||bytes.length!==receipt.bytes)throw Error('Original response hash or byte count differs');
  const doc=JSON.parse(new TextDecoder().decode(bytes)),original=doc.observations?.[row.source_row];
  if(!original||original.date!==row.latest_date)throw Error('Original observation identity differs');
  if(row.last_observed_value==null){if(!['.','',null].includes(original.value))throw Error('Original missing value differs');}
  else if(decimal(original.value)!==decimal(row.last_observed_value))throw Error('Original observation value differs');
  return 'Verified original response SHA-256 and row '+row.source_row+' · '+original.date+'. This verifies the source observation, not a trading model.';
 }
 const api={status,chart,render,verify,decimal,path,derivedHTML};if(typeof module==='object'&&module.exports)module.exports=api;
 if(!root.document)return;root.JHRiskGateResearch=api;
 const host=root.document.getElementById('rg-research'),search=root.document.getElementById('rg-search'),category=root.document.getElementById('rg-category'),horizon=root.document.getElementById('rg-horizon');
 if(!host||!search||!category||!horizon)return;
 let packet=null,signature='';
 const selection=()=>({filter:search.value,category:category.value,horizon:horizon.value});
 function draw(){
  if(!packet)return;
  const opened=new Set(Array.from(host.querySelectorAll('details[open]')).map(d=>d.dataset.series));
  host.innerHTML=render(packet,selection());
  for(const detail of host.querySelectorAll('details[data-series]'))detail.open=opened.has(detail.dataset.series);
  signature=Object.values(packet.series).map(r=>r.series_id+':'+status(r,packet)).join('|');
  root.JHCissReadthrough?.render(root.document.getElementById('ciss-readthrough'),packet);
 }
 async function refresh(){
  try{const response=await root.fetch('/data/risk-gate.json?exact=1',{cache:'no-store'});if(!response.ok)throw Error('HTTP '+response.status);const next=await response.json();render(next);packet=next;draw();}
  catch(_){packet=null;host.innerHTML='<p role="status">Source-backed Risk Gate measurements are unavailable. Earlier readings have been cleared. WAIT means abstention.</p>';root.JHCissReadthrough?.render(root.document.getElementById('ciss-readthrough'),{});}
 }
 host.addEventListener('click',async event=>{const button=event.target.closest('button[data-verify]');if(!button||!packet)return;const sid=button.dataset.verify,row=packet.series[sid],result=Array.from(host.querySelectorAll('[data-result]')).find(n=>n.dataset.result===sid);if(!row||!result)return;button.disabled=true;result.textContent='Checking retained original response…';try{result.textContent=await verify(row);}catch(error){result.textContent='Not verified: '+error.message;}finally{button.disabled=false;}});
 search.addEventListener('input',draw);category.addEventListener('change',draw);horizon.addEventListener('change',draw);
 root.document.getElementById('rg-export')?.addEventListener('click',()=>{if(!packet)return;const url=URL.createObjectURL(new Blob([JSON.stringify(packet,null,2)],{type:'application/json'})),a=root.document.createElement('a');a.href=url;a.download='risk-gate-research.json';a.click();URL.revokeObjectURL(url);});
 refresh();setInterval(refresh,300000);setInterval(()=>{if(packet&&Object.values(packet.series).map(r=>r.series_id+':'+status(r,packet)).join('|')!==signature)draw();},60000);
})(typeof window!=='undefined'?window:globalThis);
