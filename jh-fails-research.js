(function(root){
 'use strict';
 const CONTRACT='fr2004-fails-research.v1',PREFIX='data/fails-research/';
 const esc=v=>v==null?'—':String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('en-US',{maximumFractionDigits:6}):'—';
 const path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(k,label)=>path(k)?`<a href="${esc(path(k))}">${esc(label)}</a>`:'Unavailable';
 const table=(heads,rows)=>`<div class="table-scroll"><table><thead><tr>${heads.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>'<tr>'+row.map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('')}</tbody></table></div>`;
 function boundary(p){
  if(p?.contract!==CONTRACT||p.call!==null||['calls_eligible','sizing_eligible','execution_eligible'].some(k=>p[k]!==false)||p.classes?.length!==6)throw Error('Unsupported settlement research contract');
  for(const row of scopes(p)){if(row.unit!=='usd_bn'||!Array.isArray(row.history)||row.history.length>5000)throw Error('Unsupported settlement measurement');
   if(row.complete&&(![row.ftd_usd_mn,row.ftr_usd_mn,row.gross_usd_mn].every(Number.isSafeInteger)||row.ftd_usd_mn+row.ftr_usd_mn!==row.gross_usd_mn))throw Error('Reported gross does not reconcile');}
 }
 function scopes(p){return [p.treasury,...p.classes,p.totals];}
 function current(row,at,pinned=false){
  const q=row.quality||{};if(q.status!=='fresh'||!row.complete)return false;if(pinned)return true;
  const age=at-Date.parse(q.acquired_at),expiry=Date.parse(q.next_expected_publication_date)+24*3600000;
  return Number.isFinite(age)&&age>=0&&age<=36*3600000&&Number.isFinite(expiry)&&at<=expiry;
 }
 function status(row,at,pinned){return current(row,at,pinned)?(pinned?'Current under policy at this historical snapshot':'Current under the source policy'):row.quality?.status==='fresh'?'Expired; retained dated observations':row.quality?.status||'Unavailable';}
 function chart(history,count=104){
  const points=count?history.slice(-count):history,valid=points.filter(p=>p.ftd_usd_mn!=null||p.ftr_usd_mn!=null);
  if(valid.length<2)return '<p>Insufficient observations for a chart.</p>';
  const first=Date.parse(points[0].date),last=Date.parse(points.at(-1).date),max=Math.max(...valid.flatMap(p=>[p.ftd_usd_mn??0,p.ftr_usd_mn??0]));
  if(last<=first)return '<p>A single report date is not a trend.</p>';
  const paths=['ftd_usd_mn','ftr_usd_mn'].map((key,i)=>{
   let d='',pen=false,prior=null;for(const p of points){if(p[key]==null){pen=false;prior=p;continue;}
    if(prior&&(Date.parse(p.date)-Date.parse(prior.date)>7*86400000||prior.seriesbreak!==p.seriesbreak))pen=false;
    const x=45+665*(Date.parse(p.date)-first)/(last-first),y=max?185-160*p[key]/max:185;
    d+=(pen?' L':' M')+x.toFixed(2)+' '+y.toFixed(2);pen=true;prior=p;}
   return `<path d="${d}" fill="none" stroke="${i?'#61afd3':'#d9ad65'}" stroke-width="2"/>`;
  }).join('');
  return `<svg class="history-chart" viewBox="0 0 760 235" role="img" aria-label="Dated fails to deliver and receive; missing reports and source-period breaks interrupt the lines">${paths}<text x="45" y="220">${esc(points[0].date)}</text><text x="610" y="220">${esc(points.at(-1).date)}</text><text x="710" y="18" text-anchor="end">${num(max/1000)} USD bn</text><text x="710" y="199" text-anchor="end">0</text></svg><p class="dim"><span class="deliver">FTD — amber</span> · <span class="receive">FTR — blue</span>. ${num(points.length)} reported observations; calendar time on the horizontal axis. Missing reports and changes in source period interrupt each line. These are cumulative amounts reported for each period, not unique securities or daily balances.</p>`;
 }
 function overview(p,at,pinned){
  boundary(p);return {hero:`<p class="eyebrow">FR2004C · ORIGINAL-SOURCE RESEARCH</p><h1>${pinned?'Pinned settlement snapshot':'Settlement fails, with evidence'}</h1><p>Compare fails to deliver and receive across six reported asset classes. Every amount links to its original source row and reporting definition.</p><p class="warning"><strong>WAIT means abstain.</strong> A large reported fail does not establish a funding crisis, default, market top or trade. No portfolio allocation is inferred.</p>`,
   asof:`${pinned?'Historical snapshot':'Generated'} ${p.generated_at} · original data acquired ${p.source_generated_at}`,
   scopes:table(['Scope','Report date','FTD / FTR / gross · USD bn','Availability'],scopes(p).map(row=>[`<button type="button" class="inspect" data-scope="${esc(row.scope_id)}">${esc(row.label)}</button>`,esc(row.as_of),['ftd','ftr','gross'].map(k=>esc(row.exact_usd_bn[k])).join(' / '),esc(status(row,at,pinned))])),
   evidence:`<p>${link(p.replay.manifest_key,'Immutable run manifest')} · ${link(p.replay.input_key,'Retained input snapshot')} · <a class="permanent" href="/fails.html?run=${esc(p.replay.manifest_key.match(/([a-f0-9]{64})\.json$/)?.[1])}">Permanent snapshot link</a> · <a class="permanent" href="/fails.html">Latest research</a></p><p>Browser verification checks the retained output bytes. Run <code>scripts/replay_fails_research.py</code> from the matching release checkout to independently reproduce the calculations from the original responses.</p><p>${num(Object.values(p.series_coverage).reduce((a,r)=>a+r.observations,0))} original observations across ${num(Object.keys(p.series_coverage).length)} series. ${num(p.revisions.known_changed_values)} values changed since the previous retained acquisition. Historical publication-time knowledge and complete provider revision history are unverified.</p><p>Portfolio consequences require a validated strategy, dated exposure data and explicit cost assumptions. Settlement amounts cannot be substituted for portfolio losses or used to invent position sizes.</p>`};
 }
 function detail(p,row,at,pinned,range=104){
  const last=row.history.at(-1),st=row.statistics,q=row.quality,eligible=current(row,at,pinned);
  const components=last?Object.entries(last.components):[];
  const stats=st.status==='available'||st.status==='constant_prior_sample';
  return `<h2>${esc(row.label)}</h2><p><strong>${esc(row.exact_usd_bn.ftd)} + ${esc(row.exact_usd_bn.ftr)} = ${esc(row.exact_usd_bn.gross)} USD bn</strong> · report date ${esc(row.as_of)}.</p><p>${esc(status(row,at,pinned))}. ${esc(p.measurement_note)}</p>`+
   `<p>Exact reported amounts: ${num(row.ftd_usd_mn)} + ${num(row.ftr_usd_mn)} = ${num(row.gross_usd_mn)} USD millions. Only complete same-date components are summed. A suppressed or absent latest component remains unavailable.</p>`+
   chart(row.history,range)+`<h3>Original rows behind this total</h3>`+table(['Series ID','Original row index (zero-based)','Reported USD millions','Status'],components.map(([id,c])=>[esc(id),num(c.row_index),num(c.value_usd_mn),esc(c.status)]))+
   `<p>${link(p.sources.observations.evidence.key,'Complete original observations response')} · ${link(p.sources.catalog.evidence.key,'Original series catalog')} · ${link(p.sources.breaks.evidence.key,'Original source-period boundaries')}.</p>`+
   `<h3>Descriptive comparison, not a forecast</h3><p>${esc(st.baseline.method)}. Prior sample ${num(st.baseline.sample_n)}; ${esc(st.baseline.from_date)} to ${esc(st.baseline.to_date)}; source period ${esc(st.baseline.source_period)}.</p>`+
   (eligible&&stats?`<p>Signed z ${num(st.z)} · prior-sample percentile ${num(st.pctile)}%. Prior mean ${num(st.mean)} USD bn; sample SD ${st.standard_deviation_usd_mn_decimal==null?'unavailable':num(Number(st.standard_deviation_usd_mn_decimal)/1000)} USD bn. ${st.status==='constant_prior_sample'?'The prior sample is constant; z is undefined.':''}</p>`:`<p>Current comparison unavailable: ${eligible?esc(st.status):'source is incomplete or expired'}.</p>`)+
   (row.previous_reported_change?`<p>Previous reported change ${esc(row.previous_reported_change.difference_usd_bn_decimal)} USD bn, ${esc(row.previous_reported_change.from)} → ${esc(row.previous_reported_change.to)} (${num(row.previous_reported_change.elapsed_days)} calendar days). This is a change in reported cumulative amounts, not a measured cash flow.</p>`:'<p>Previous comparable report unavailable.</p>')+
   `<h3>Dates and measurement limits</h3><p>Acquired ${esc(q.acquired_at)}. The normal next release is ${esc(q.next_expected_publication_date)}; the current-data policy allows 24 hours after that time and at most 36 hours since acquisition. Holiday exceptions and the actual publication timestamp are unverified.</p><p>No daily average is calculated from these weekly totals. The reported sums can count the same unresolved fail repeatedly. Statistical windows stop at missing reports or source-period changes; percentile is not a crisis probability.</p>`;
 }
 function history(row,page=0){
  const points=[...row.history].reverse(),pages=Math.max(1,Math.ceil(points.length/52));page=Math.max(0,Math.min(pages-1,page));
  return {page,pages,count:points.length,html:table(['Report date','Source period','FTD · USD mn','FTR · USD mn','Gross · USD mn','Complete'],points.slice(page*52,page*52+52).map(r=>[esc(r.date),esc(r.seriesbreak),num(r.ftd_usd_mn),num(r.ftr_usd_mn),num(r.gross_usd_mn),r.complete?'Yes':'No']))};
 }
 function definitions(p){return table(['Reporting period','Reviewed accounting instructions','Retained original PDF'],Object.entries(p.reporting_periods).map(([id,d])=>[`${esc(id)}<div class="dim">${esc(d.start)} → ${esc(d.end)}</div>`,`FR2004C · PDF page ${num(d.page)}`,link(p.sources[id].evidence.key,'Original instructions')]))+'<p>Definitions reviewed by source period do not establish what was known at the original publication time. Every stored response represents its acquisition vintage.</p>';}
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
  if(manifest.contract!=='fr2004-fails-replay.v1'||ref?.key!==PREFIX+'outputs/'+manifest.output_sha256+'.json'||ref.sha256!==manifest.output_sha256)throw Error('Run identity differs');
  const output=await bytes(fetcher,ref.key,24*1024*1024);if(output.length!==ref.bytes||await sha(output)!==ref.sha256)throw Error('Research output differs');
  const packet=JSON.parse(new TextDecoder().decode(output));boundary(packet);if(packet.generated_at!==manifest.generated_at)throw Error('Snapshot clock differs');
  packet.replay={manifest_key:key,output_sha256:ref.sha256,input_key:manifest.input?.key};return packet;
 }
 const api={CONTRACT,PREFIX,esc,path,current,status,scopes,boundary,chart,overview,detail,history,definitions,loadSnapshot,sha};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.FailsResearch=api;
})(typeof globalThis==='object'?globalThis:this);
