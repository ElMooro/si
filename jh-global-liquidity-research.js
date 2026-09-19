(function(root){
 'use strict';
 const CONTRACT='global-liquidity-research.v1', PREFIX='data/global-liquidity-research/';
 const esc=v=>String(v==null?'—':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>v!==null&&v!==undefined&&v!==''&&typeof v!=='boolean'&&Number.isFinite(Number(v))?Number(v):null;
 const num=v=>number(v)===null?'—':number(v).toLocaleString('en-US',{maximumFractionDigits:3});
 const path=k=>typeof k==='string'&&/^data\/(evidence|global-liquidity-research|report-research)\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(key,label)=>path(key)?`<a href="${esc(path(key))}" target="_blank" rel="noopener">${esc(label)}</a>`:'Unavailable';
 const age=(stamp,now)=>{const value=Date.parse(stamp);return Number.isFinite(value)?(now-value)/86400000:Infinity;};
 const recent=(stamp,now,days)=>age(stamp,now)>=0&&age(stamp,now)<=days;
 const dayFresh=(stamp,now,days)=>{const day=Date.parse(stamp);const elapsed=Math.floor(now/86400000)-Math.floor(day/86400000);return Number.isFinite(day)&&elapsed>=0&&elapsed<=days;};
 function currentFresh(p,now){
  if(!recent(p.generated_at,now,26/24)||!recent(p.source_generated_at,now,26/24)||p.three_bank_subtotal?.status!=='descriptive')return false;
  return ['WALCL','ECBASSETSW','JPNASSETS'].every(sid=>{
   const c=p.three_bank_subtotal.components?.[sid];if(!c)return false;
   return [c.balance,c.fx].filter(Boolean).every(r=>r.status==='descriptive'&&dayFresh(r.selected?.effective_observation_date,now,Number(r.max_carry_age_days)))&&
    [sid,c.fx_series_id].filter(Boolean).every(id=>p.series?.[id]?.available===true&&recent(p.series[id].acquired_at,now,26/24));
  });
 }
 function rowFresh(row,p,now){
  row=row||{};
  const limits={D:10,W:21,BW:35,M:100,Q:200,SA:370,A:550};
  return row.available===true&&recent(p.generated_at,now,26/24)&&recent(p.source_generated_at,now,26/24)&&recent(row.acquired_at,now,26/24)&&dayFresh(row.date,now,limits[row.frequency]||0);
 }
 function table(headers,rows){return `<div class="table"><table><thead><tr>${headers.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>'<tr>'+r.map(c=>'<td>'+c+'</td>').join('')+'</tr>').join('')}</tbody></table></div>`;}
 function components(snapshot,packet){
  return table(['Institution / identity','Native measurement','FX quotation','USD millions','Status'],Object.entries(snapshot?.components||{}).map(([sid,c])=>{
   const b=c.balance||{},f=c.fx||{},r=b.selected||{},q=f.selected||{};
   return [esc(sid),`${esc(r.native_decimal)} ${esc(c.native_unit)}<span class="small">Period label ${esc(r.observation_label)} · measurement date ${esc(r.effective_observation_date)} · age ${esc(b.carry_age_days)}d / max ${esc(b.max_carry_age_days)}d</span><span class="small">${link(packet?.series?.[sid]?.evidence?.observations?.key,'Original balance')} · row ${esc(r.source_row)} · × ${esc(c.native_multiplier_to_millions)} to native millions</span>`,
    c.fx_series_id?`${esc(c.fx_series_id)}: ${esc(q.native_decimal)}<span class="small">Observed ${esc(q.effective_observation_date)} · age ${esc(f.carry_age_days)}d / max ${esc(f.max_carry_age_days)}d</span><span class="small">${link(packet?.series?.[c.fx_series_id]?.evidence?.observations?.key,'Original FX')} · row ${esc(q.source_row)}</span>`:'Native USD',
    num(c.usd_millions_decimal),esc(b.status==='descriptive'&&(!c.fx||f.status==='descriptive')?'descriptive':[b.reason,f.reason].filter(Boolean).join('; '))];
  }));
 }
 function chart(history){
  const points=Object.entries(history||{}).sort(([a],[b])=>a.localeCompare(b)),numeric=points.map(([,p])=>number(p.total_usd_millions_decimal)).filter(v=>v!==null);
  if(!numeric.length)return '<p>No complete dated subtotal.</p>';
  const lo=Math.min(...numeric),hi=Math.max(...numeric),span=hi-lo||1;let drawing='',connected=false;
  points.forEach(([,p],i)=>{const value=number(p.total_usd_millions_decimal);if(value===null){connected=false;return;}
   const x=55+i*825/Math.max(1,points.length-1),y=210-(value-lo)*180/span;drawing+=(connected?' L':' M')+x.toFixed(2)+' '+y.toFixed(2);connected=true;});
  return `<svg viewBox="0 0 940 255" role="img" aria-label="Three-bank USD balance-sheet history; missing observations are gaps"><path d="M55 20V220H890" fill="none" stroke="#52687d"/><path d="${drawing}" fill="none" stroke="#60c6de" stroke-width="2"/><text x="58" y="15">${esc(num(hi))} USD mn</text><text x="58" y="239">${esc(points[0][0])}</text><text x="800" y="239">${esc(points.at(-1)[0])}</text></svg>`;
 }
 function inspect(p,day){
  const row=p.calendar_research?.history?.[day];if(!row)return '<p>Calendar date unavailable.</p>';
  return `<p>Valuation date <b>${esc(day)}</b> · three-bank subtotal ${num(row.total_usd_millions_decimal)} USD millions · ${esc(row.status)}</p>`+components(row,p);
 }
 function change(p,horizon){
  const row=p.calendar_research?.latest_changes?.[horizon];if(!row)return '<p>Comparison unavailable.</p>';
  let html=`<p>${esc(row.start)} → ${esc(row.end)} · exactly ${esc(row.calendar_days)} calendar days.</p>`;
  if(row.status!=='descriptive')return html+`<p class="status">Comparison unavailable: a required endpoint or component is missing. No shorter period or previous numeric observation is substituted.</p>`+
   `<details><summary>Inspect missing baseline</summary>${inspect(p,row.start)}</details><details><summary>Inspect ending date</summary>${inspect(p,row.end)}</details>`;
  html+=`<p>Total change: <b>${num(row.change_usd_millions_decimal)} USD millions</b> (${num(row.relative_percent_decimal)}%).</p>`;
  html+=table(['Component','Balance effect at baseline FX','FX effect on current balance','Rounding residual','Total change'],Object.entries(row.components).map(([sid,c])=>[esc(sid),num(c.balance_effect_usd_millions_decimal),num(c.fx_effect_usd_millions_decimal),esc(c.rounding_residual_usd_millions_decimal),num(c.change_usd_millions_decimal)]));
  return html+`<p>${esc(row.formula)}</p><p>${esc(row.scope)}</p><details><summary>Baseline original observations</summary>${inspect(p,row.start)}</details><details><summary>Ending original observations</summary>${inspect(p,row.end)}</details><details><summary>Exact decimal calculation</summary><pre>${esc(JSON.stringify(row,null,2))}</pre></details>`;
 }
 function render(p,now=Date.now(),pinned=false){
  if(p.contract!==CONTRACT)throw Error('Current research contract is unavailable');
  const live=currentFresh(p,now),dated=pinned,readable=live||dated,current=p.three_bank_subtotal||{},net=p.us_net_liquidity_proxy||{},m2=p.us_m2||{};
  const card=(title,value,unit,detail)=>`<article><div class="eyebrow">${esc(title)}</div><strong>${num(value)}</strong><div>${esc(unit)}</div><p>${esc(detail)}</p></article>`;
  const summary=card(dated?'Snapshot three-bank subtotal':'Three-bank subtotal',readable?current.total_usd_millions_decimal:null,'USD millions','Fed + Eurosystem + BOJ; other central banks excluded.')+
   card('US mixed-basis proxy',dated||['WALCL','WTREGEN','RRPONTSYD'].every(s=>rowFresh(p.series[s],p,now))?net.net:null,'USD millions','WALCL − WTREGEN − RRP; official units, converted once.')+
   card('US M2 year-over-year',dated||rowFresh(p.series.M2SL,p,now)?m2.year_comparison?.pct_change:null,'percent',`${m2.year_comparison?.baseline_date||'—'} → ${m2.observation_period||'—'}; growth is not acceleration.`);
  const sourceRows=Object.entries(p.series||{}).map(([sid,row])=>[`${esc(sid)}<span class="small">${esc(row.name)}</span>`,`${esc((dated||rowFresh(row,p,now))?row.current_decimal:null)}<span class="small">${esc(row.unit)}</span>`,
   `${esc(row.date)}<span class="small">${esc(row.frequency)} · ${esc(row.seasonal_adjustment)}</span>`,
   `${esc(dated?'as published: '+row.quality?.status:rowFresh(row,p,now)?'fresh':'expired / unavailable')}<span class="small">Acquired ${esc(row.acquired_at)}</span>`,
   `${link(row.evidence?.observations?.key,'Original observations')} · ${link(row.evidence?.definition?.key,'Definition')}<span class="small">Original row ${esc(row.current_row_index)}</span><details><summary>Exact calendar comparisons</summary><pre>${esc(JSON.stringify({last_observed:row.last_observed_value,comparisons:row.changes,dated_comparisons:row.historical_changes},null,2))}</pre></details>`]);
  const run=p.replay?.manifest_key,match=typeof run==='string'?run.match(/\/runs\/([a-f0-9]{64})\.json$/):null;
  const evidence=link(run,'Immutable run manifest')+(match?` · <a href="/global-liquidity.html?run=${match[1]}">Permanent snapshot link</a>`:'')+
   ' · '+link(p.source_replay?.manifest_key,'Canonical source run')+Object.entries(p.legacy_context||{}).map(([key,ref])=>' · '+link(ref.key,'Legacy '+key.split('/').at(-1))).join('');
  return {summary,components:components(current,p),series:table(['Identity / official definition','Native value / unit','Observation','Quality / acquisition','Original evidence'],sourceRows),
   chart:chart(p.calendar_research?.history),evidence,
   status:dated?'Pinned snapshot — values are dated as published':live?'Descriptive research — Calls vote: abstain':'Current inputs expired or incomplete — dated research retained',
   stamp:`Generated ${p.generated_at} · canonical source ${p.source_generated_at} · ${p.quality?.fresh_series}/${p.quality?.expected_series} rows fresh at build`,
   history:`${p.calendar_research?.complete_weekly_slots}/${p.calendar_research?.expected_weekly_slots} complete Friday slots. Current-vintage reconstruction; historical release timing and predictive validity are not established.`,
   options:Object.keys(p.calendar_research?.history||{}).sort().reverse().map(day=>`<option value="${esc(day)}">${esc(day)}</option>`).join('')};
 }
 async function bytes(fetcher,key,limit=8*1024*1024){
  if(!path(key))throw Error('Unsupported evidence path');
  const response=await fetcher('/'+key,{cache:'no-store'});if(!response.ok)throw Error('Research artifact unavailable');
  const reader=response.body.getReader(),parts=[];let size=0;
  try{while(true){const item=await reader.read();if(item.done)break;size+=item.value.length;if(size>limit){await reader.cancel();throw Error('Research artifact exceeds size bound');}parts.push(item.value);}}
  finally{reader.releaseLock();}
  const raw=new Uint8Array(size);let offset=0;for(const part of parts){raw.set(part,offset);offset+=part.length;}return raw;
 }
 async function sha(raw){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw))).map(b=>b.toString(16).padStart(2,'0')).join('');}
 async function loadSnapshot(id,fetcher=root.fetch.bind(root)){
  if(!/^[a-f0-9]{64}$/.test(id))throw Error('Invalid snapshot identifier');
  const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024);
  if(await sha(raw)!==id)throw Error('Run manifest hash differs');
  const manifest=JSON.parse(new TextDecoder().decode(raw)),ref=manifest.output;
  if(manifest.contract!=='global-liquidity-replay.v1'||!ref||ref.sha256!==manifest.output_sha256||ref.key!==PREFIX+'outputs/'+ref.sha256+'.json'||!Number.isInteger(ref.bytes))throw Error('Unsupported output identity');
  const output=await bytes(fetcher,ref.key);if(output.length!==ref.bytes||await sha(output)!==ref.sha256)throw Error('Snapshot output differs');
  const p=JSON.parse(new TextDecoder().decode(output));if(p.contract!==CONTRACT||p.generated_at!==manifest.generated_at)throw Error('Snapshot contract differs');
  p.replay={manifest_key:key,output_sha256:ref.sha256,compilers:manifest.compilers};return p;
 }
 const api={CONTRACT,PREFIX,esc,num,path,currentFresh,rowFresh,render,inspect,change,chart,loadSnapshot,sha};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.GlobalLiquidityResearch=api;
})(typeof globalThis==='object'?globalThis:this);
