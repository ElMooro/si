(function(root){
 'use strict';
 const CONTRACT='liquidity-reversal-research.v1',PREFIX='data/reversal-research/';
 const esc=value=>value==null?'—':String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=value=>typeof value==='number'&&Number.isFinite(value)?value.toLocaleString('en-US',{maximumFractionDigits:6}):'—';
 const path=key=>typeof key==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(key)&&!key.includes('..')?'/'+key:null;
 const link=(key,label)=>path(key)?`<a href="${esc(path(key))}">${esc(label)}</a>`:'Unavailable';
 const table=(heads,rows)=>`<div class="table-scroll"><table><thead><tr>${heads.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>'<tr>'+row.map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('')}</tbody></table></div>`;
 function recent(stamp,now,hours){const age=now-Date.parse(stamp);return Number.isFinite(age)&&age>=0&&age<=hours*3600000;}
 function boundary(p){if(p?.contract!==CONTRACT||p.call!==null||['calls_eligible','sizing_eligible','execution_eligible'].some(k=>p[k]!==false)||!Array.isArray(p.rows)||p.rows.length>2500)throw Error('Unsupported research authority or schema');}
 function usable(row,now,pinned=false){
  if(row.measurement_eligible!==true)return false;if(pinned)return true;
  const m=row.measurement||{},age=(Date.parse(new Date(now).toISOString().slice(0,10))-Date.parse(m.date))/86400000;
  return recent(m.acquired_at,now,26)&&Number.isFinite(age)&&age>=0&&age<=m.quality?.max_age_days;
 }
 function status(row,now,pinned=false){return row.original_provider_verified?(usable(row,now,pinned)?'Current at snapshot / source policy':row.measurement_eligible?'Source expired; dated context':row.status):row.inventory_entry?'Legacy — originals unverified':'Registered source unavailable';}
 function filtered(packet,query='',scope='verified'){
  boundary(packet);if(!['all','legacy','verified'].includes(scope))throw Error('Unsupported evidence scope');query=query.trim().toLowerCase();
  return packet.rows.map((row,index)=>({row,index})).filter(({row})=>(scope==='all'||(scope==='legacy'?row.inventory_entry&&!row.original_provider_verified:row.original_provider_verified))&&(!query||(row.symbol+' '+row.name).toLowerCase().includes(query)));
 }
 function inventory(packet,query,scope,page,now,pinned){
  const matches=filtered(packet,query,scope),pages=Math.max(1,Math.ceil(matches.length/30));page=Math.min(Math.max(0,page),pages-1);
  return {page,pages,count:matches.length,html:table(['Series / evidence','Latest native value','Observation date','Status','Previous observed change','Signed z / |z|','Slope change'],matches.slice(page*30,page*30+30).map(({row:r,index})=>{
   const t=r.technical||{},c=t.latest_change,current=usable(r,now,pinned),m=r.measurement||{};
   return [`<button type="button" class="inspect" data-index="${index}">${esc(r.symbol)}</button><div class="dim">${esc(r.name)}</div>`,r.original_provider_verified?`${esc(m.current_decimal)}<div class="dim">${esc(r.unit)}</div>`:`${num(r.legacy?.last)}<div class="dim">${r.inventory_entry?'Legacy value; unit unverified':'Registered source unavailable'}</div>`,r.original_provider_verified?esc(r.date):'Observation unavailable<div class="dim">Legacy engine '+esc(r.legacy_generated_at)+'</div>',esc(status(r,now,pinned)),c?`${esc(c.value_decimal)} ${esc(t.change_unit)}<div class="dim">${esc(c.baseline_date)} → ${esc(c.date)}</div>`:'Unavailable',current?num(t.z_signed)+' / '+num(t.z_absolute):'Withheld',current?(t.trend?esc(t.trend.slope_direction_change||'No detected sign change'):'Unavailable'):'Withheld'];
  }))};
 }
 function chart(history,unit,frequency){
  const rows=[...(history||[])].reverse();if(rows.length<2)return '<p>Insufficient native history for a chart.</p>';
  const finite=rows.filter(r=>typeof r.value==='number'&&Number.isFinite(r.value));if(finite.length<2)return '<p>Insufficient numeric observations.</p>';
  const times=rows.map(r=>Date.parse(r.date)),first=Math.min(...times),last=Math.max(...times),values=finite.map(r=>r.value),lo=Math.min(...values),hi=Math.max(...values);
  if(first===last)return '<p>A single date is not a trend.</p>';
  let pen=false,d='',previous=null;const gapLimit={D:4,W:7,BW:14,M:32,Q:93,SA:184,A:366}[frequency];rows.forEach(r=>{if(r.value==null){pen=false;previous=r;return;}if(previous&&gapLimit&&(Date.parse(r.date)-Date.parse(previous.date))/86400000>gapLimit)pen=false;previous=r;const x=40+650*(Date.parse(r.date)-first)/(last-first),y=hi===lo?95:170-150*(r.value-lo)/(hi-lo);d+=(pen?' L':' M')+x.toFixed(2)+' '+y.toFixed(2);pen=true;});
  return `<svg class="history-chart" viewBox="0 0 740 215" role="img" aria-label="Native observations over dated time; null observations break the line"><path d="${d}" fill="none" stroke="#58c8a6" stroke-width="2"/><text x="40" y="200">${esc(rows[0].date)}</text><text x="580" y="200">${esc(rows.at(-1).date)}</text><text x="700" y="28" text-anchor="end">${num(hi)}</text><text x="700" y="175" text-anchor="end">${num(lo)}</text></svg><p class="dim">${rows.length} newest returned observations · ${esc(unit)} · calendar time on the horizontal axis. Missing reported observations and excessive date gaps break the line; a complete exchange calendar is not verified.</p>`;
 }
 function detail(row,now,pinned=false){
  if(!row)return '<p>Select a series to inspect its calculations and original evidence.</p>';
  if(!row.original_provider_verified&&!row.inventory_entry)return `<h2>${esc(row.symbol)}</h2><p>Registered native source unavailable. No retained original definition and observation pair is available in this snapshot. This entry was added explicitly to the source registry; it is not an invented legacy measurement.</p>`;
  if(!row.original_provider_verified)return `<h2>${esc(row.symbol)}</h2><p>Retained legacy inventory entry · engine snapshot ${esc(row.legacy_generated_at)}.</p><p>Its original provider response, unit and instrument equivalence have not been verified. These values do not enter current trend, shock, policy or portfolio decisions.</p>`+table(['Legacy field','Retained value'],Object.entries(row.legacy||{}).map(([k,v])=>[esc(k),esc(v)]));
  const m=row.measurement,t=row.technical,trend=t.trend,r=t.range_1y,eligible=usable(row,now,pinned);
  return `<h2>${esc(row.name)} <span class="dim">${esc(row.symbol)}</span></h2><p><strong>${esc(m.current_decimal)} ${esc(m.unit)}</strong> · observation ${esc(m.date)} · period ends ${esc(m.period_end)} · ${esc(m.definition.frequency||m.frequency)} · ${esc(m.seasonal_adjustment)}.</p><p>${esc(status(row,now,pinned))}. Acquired ${esc(m.acquired_at)}. Provider metadata updated ${esc(m.provider_updated_at)}. Original publication-time knowledge is unverified.</p><p>Source policy: observation age ≤ ${num(m.quality.max_age_days)} days and acquisition age ≤ 26 hours; this is a conservative ceiling, not a verified release calendar.</p>`+
   `<p>Original FRED ${link(m.evidence.definition.key,'definition response')} · ${link(m.evidence.observations.key,'observations response')} · selected original row ${num(m.current_row_index)}. Native unit is retained; no tenor or currency substitution.</p>`+
   chart(m.history,m.unit,m.frequency)+table(['Comparison','Target date','Actual baseline','Current / baseline','Native difference','Unit'],['week','month','quarter','year'].filter(name=>m.changes[name]).map(name=>[name,m.changes[name]]).map(([name,c])=>[esc(name),esc(c.target_date),esc(c.baseline_date),esc(c.current_decimal)+' / '+esc(c.baseline_decimal),esc(c.change_decimal),esc(c.change_unit)]))+
   `<h3>Descriptive shock</h3><p>Actual observed change direction: ${esc(t.change_direction)}. Signed z ${eligible?num(t.z_signed):'withheld'} · absolute z ${eligible?num(t.z_absolute):'withheld'}. A positive z does not necessarily mean the value rose.</p><p>${esc(t.z_basis)}. Prior N ${num(t.z_n_prior)}; mean ${t.z_mean_decimal==null?'—':num(Number(t.z_mean_decimal))}; sample SD ${t.z_sd_decimal==null?'—':num(Number(t.z_sd_decimal))}. Prior window ${esc(t.z_reference_start)} to ${esc(t.z_reference_end)}. ${t.z_reason?'Unavailable reason: '+esc(t.z_reason):''}</p>`+
   `<h3>Dated trend calculation</h3>`+(trend?`<p>${num(trend.short_observations)} short and ${num(trend.long_observations)} long observations. Current slope ${num(trend.slope_now)}, prior slope ${num(trend.slope_previous)} ${esc(trend.slope_unit)}. No division by the mean level.</p><p>Current slope window ${esc(trend.current_window.start)} → ${esc(trend.current_window.end)}; prior ${esc(trend.previous_window.start)} → ${esc(trend.previous_window.end)}. Sign change ${esc(trend.slope_direction_change||'none')}; most recent MA crossing ${esc(trend.most_recent_ma_cross?.direction)} on ${esc(trend.most_recent_ma_cross?.date)}; matching direction ${trend.matching_ma_cross?'yes':'no'}.</p><p>${esc(trend.interpretation)}.${eligible?'':' This is retained stale context.'}</p>`:'<p>Unavailable: insufficient complete compatible observations for both windows, or unsupported published frequency. Nulls and missing periods are not compacted away.</p>')+
   `<h3>Trailing calendar-year range</h3>`+(r?`<p>${esc(r.start_cutoff)} → ${esc(r.end)}. ${num(r.numeric_rows)} numeric and ${num(r.missing_rows)} missing reported rows. Min ${esc(r.min_decimal)}, max ${esc(r.max_decimal)}, observed range position ${r.position_pct==null?'unavailable':num(r.position_pct)+'%'}. ${esc(t.range_reason||'')}</p>`:`<p>Unavailable: ${esc(t.range_reason)}.</p>`)+
   `<p>Evidence family ${esc(row.evidence_family)}; independence is not established. Decision <code>${esc(row.decision_id)}</code>. This measurement has no Calls or sizing permission.</p>`+
   `<details><summary>Compare the retained legacy values</summary>`+table(['Legacy field','Retained value'],Object.entries(row.legacy||{}).map(([k,v])=>[esc(k),esc(v)]))+'</details>';
 }
 function overview(p,now,pinned=false){
  boundary(p);const fresh=p.rows.filter(r=>usable(r,now,pinned)).length;
  const f=p.pd_settlement_fails||{};
  return {
   hero:`<p class="eyebrow">LIQUIDITY REVERSAL · ORIGINAL-SOURCE RESEARCH</p><h1>${pinned?'Pinned research snapshot':'Liquidity conditions and turning points'}</h1><p>Inspect native observations, dated changes and descriptive slope shifts. <strong>WAIT means abstain; no portfolio change is inferred.</strong></p><p>${num(p.inventory.entries)} retained inventory entries · ${num(p.quality.original_verified_series)} series with verified original responses · ${num(fresh)} current under the source policy.</p><p class="warning">Policy direction, crisis probabilities and portfolio sizing remain unqualified. There is no aggregate easing/tightening score.</p>`,
   ts:`${pinned?'Historical snapshot':'Generated'} ${p.generated_at} · source warehouse ${p.source_generated_at}`,
   families:table(['Evidence family','Series (may overlap)','Independence'],p.dependency_map.groups.map(g=>[esc(g.family),esc(g.members.join(', ')),'Not established']))+'<p>No effective independent-input count is claimed. Treasury deposits overlap across monthly/weekly views; credit facilities overlap total/component series.</p>',
   fails:table(['Scope','Observation','FTD / FTR / gross (USD bn)','Status'],[[f,'Treasury including TIPS'],[f.ust_ex_tips||{},'UST excluding TIPS']].map(([s,label])=>[esc(label),esc(s.as_of),s.display_values?s.display_values.map(num).join(' / '):'Withheld',esc(s.status)+' · '+esc((s.reasons||[]).join(', '))]))+`<p>${esc(f.note)} Original FR2004 responses are not verified by this engine.</p>`,
   evidence:`<p>${link(p.replay.manifest_key,'Immutable run manifest')} · ${link(p.replay.input_key,'Retained input snapshot')} · <a class="permanent" href="/liquidity-reversal.html?run=${esc(p.replay.manifest_key.match(/([a-f0-9]{64})\.json$/)?.[1])}">Permanent snapshot link</a> · <a href="/liquidity-reversal.html">Latest research</a></p><p>Every value above belongs to this one immutable snapshot. Its output bytes are checked in the browser. Independent original-response replay is available with <code>scripts/replay_reversal_research.py</code> from the matching release checkout.</p><p>Legacy membership is retained as of ${esc(p.inventory.legacy_generated_at)}. Full original legacy bytes are protected; the public inventory excludes owner list names and free text.</p><p>Portfolio impact is unavailable until a strategy, point-in-time evaluation, costs, exposures and an authorized portfolio snapshot are established. None is substituted with a guessed allocation.</p>`
  };
 }
 async function sha(raw){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',raw))].map(x=>x.toString(16).padStart(2,'0')).join('');}
 async function bytes(fetcher,key,limit){
  if(!path(key))throw Error('Unsupported evidence path');const response=await fetcher(path(key),{cache:'no-store'});if(!response.ok)throw Error('Research artifact unavailable');
  const reader=response.body.getReader(),parts=[];let size=0;
  try{while(true){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>limit){await reader.cancel();throw Error('Research artifact size bound');}parts.push(value);}}finally{reader.releaseLock();}
  const out=new Uint8Array(size);let offset=0;for(const part of parts){out.set(part,offset);offset+=part.length;}return out;
 }
 async function loadSnapshot(id,fetcher=root.fetch.bind(root)){
  if(!/^[a-f0-9]{64}$/.test(id||''))throw Error('Invalid run ID');const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024);
  if(await sha(raw)!==id)throw Error('Run hash differs');const manifest=JSON.parse(new TextDecoder().decode(raw)),ref=manifest.output;
  if(manifest.contract!=='reversal-replay.v1'||ref?.key!==PREFIX+'outputs/'+manifest.output_sha256+'.json'||ref.sha256!==manifest.output_sha256)throw Error('Run identity differs');
  const output=await bytes(fetcher,ref.key,16*1024*1024);if(output.length!==ref.bytes||await sha(output)!==ref.sha256)throw Error('Research output differs');
  const packet=JSON.parse(new TextDecoder().decode(output));boundary(packet);if(packet.generated_at!==manifest.generated_at)throw Error('Snapshot clock differs');
  packet.replay={manifest_key:key,output_sha256:ref.sha256,input_key:manifest.input?.key};return packet;
 }
 const api={CONTRACT,PREFIX,esc,path,recent,usable,filtered,inventory,detail,overview,loadSnapshot,sha};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.ReversalResearch=api;
})(typeof globalThis==='object'?globalThis:this);
