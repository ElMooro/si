(function(root){
 'use strict';
 const CONTRACT='dealer-original-research.v1',PREFIX='data/dealer-research/';
 const esc=v=>v==null?'—':String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('en-US',{maximumFractionDigits:6}):'—';
 const words=v=>esc(typeof v==='string'?v.replaceAll('_',' '):v);
 const path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(k,label)=>path(k)?`<a href="${esc(path(k))}">${esc(label)}</a>`:'Unavailable';
 const table=(heads,rows)=>`<div class="table-scroll"><table><thead><tr>${heads.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>'<tr>'+r.map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('')}</tbody></table></div>`;
 function boundary(p){
  if(p?.contract!==CONTRACT||p.call!==null||['calls_eligible','sizing_eligible','execution_eligible'].some(k=>p[k]!==false)||Object.keys(p.native_series||{}).length!==70)throw Error('Unsupported dealer research contract');
  for(const [key,row] of Object.entries(p.native_series)){if(row.definition?.series_id!==key||!Array.isArray(row.history)||row.history.length>5000)throw Error('Dealer series identity differs');}
  for(const row of Object.values(p.groups||{})){if(row.usd_mn!=null&&(!Number.isSafeInteger(row.usd_mn)||Object.values(row.components).some(v=>!v||!Number.isSafeInteger(v.usd_mn))||Object.values(row.components).reduce((a,v)=>a+v.usd_mn,0)!==row.usd_mn))throw Error('Dealer sum does not reconcile');}
 }
 function current(q,at,pinned=false){
  if(q?.status!=='fresh')return false;if(pinned)return true;
  const age=at-Date.parse(q.acquired_at),expiry=Date.parse(q.next_expected_publication_date)+24*3600000;
  const hours=q.max_acquisition_age_hours;return [36,192].includes(hours)&&Number.isFinite(age)&&age>=0&&age<=hours*3600000&&Number.isFinite(expiry)&&at<=expiry;
 }
 function status(q,at,pinned){return current(q,at,pinned)?(pinned?'Current under policy at the historical snapshot':'Current under the source policy'):q?.status==='fresh'?'Expired; retained dated observation':q?.status||'Unavailable';}
 function selections(p){return [...Object.values(p.groups).map(r=>({id:'group:'+r.id,label:r.label})),...Object.entries(p.native_series).map(([id,r])=>({id:'series:'+id,label:id+' · '+r.definition.family.replaceAll('_',' ')}))];}
 function selected(p,id){
  const [kind,key]=id.split(':');if(kind==='group'&&p.groups[key]){const g=p.groups[key];return {...g,kind,key,display_unit:'USD millions',definition_status:g.quality.status==='definition_unverified'?'unresolved':'reviewed',rows:g.history.map(r=>({date:r[0],value:r[1],period:r[2],status:r[3]?'complete':'incomplete',row_index:null}))};}
  if(kind==='series'&&p.native_series[key]){const r=p.native_series[key],d=r.definition;return {...r,kind,key,label:key,definition_status:d.definition_status,display_unit:d.native_unit==='usd_mn'?'USD millions':'publisher numeric value; unit normalization unverified',
   rows:r.history.map(x=>({date:x[0],value:x[1],row_index:x[2],status:x[3],period:x[4],definition_status:x[4]===r.reviewed_catalog_period?d.definition_status:'historical scope unverified'})),usd_mn:r.reported_value,valuation_basis:d.valuation_basis};}
  throw Error('Unknown dealer measurement');
 }
 function chart(rows,count=104){
  const a=count?rows.slice(-count):rows,valid=a.filter(r=>r.value!=null);if(valid.length<2)return '<p>No complete comparable chart is available.</p>';
  const first=Date.parse(a[0].date),last=Date.parse(a.at(-1).date);if(last<=first)return '<p>A single report is not a trend.</p>';
  let min=Math.min(0,...valid.map(r=>r.value)),max=Math.max(0,...valid.map(r=>r.value));if(max===min)max=min+1;
  const y=v=>185-160*(v-min)/(max-min);let d='',pen=false,previous=null;
  for(const row of a){if(row.value==null){pen=false;previous=row;continue;}if(previous&&(Date.parse(row.date)-Date.parse(previous.date)>7*86400000||row.period!==previous.period))pen=false;
   d+=(pen?' L':' M')+(50+650*(Date.parse(row.date)-first)/(last-first)).toFixed(2)+' '+y(row.value).toFixed(2);pen=true;previous=row;}
  return `<svg viewBox="0 0 760 235" class="history-chart" role="img" aria-label="Reported dealer values across actual dates; gaps and source-period changes break the line"><line x1="50" x2="705" y1="${y(0)}" y2="${y(0)}" stroke="#718087" stroke-dasharray="4 4"/><path d="${d}" fill="none" stroke="#79b9bf" stroke-width="2"/><text x="50" y="220">${esc(a[0].date)}</text><text x="610" y="220">${esc(a.at(-1).date)}</text><text x="710" y="18" text-anchor="end">${number(max)}</text><text x="710" y="202" text-anchor="end">${number(min)}</text></svg><p class="dim">${number(a.length)} reported dates. Calendar time on the horizontal axis; missing values and source-period changes interrupt the line. The zero line is a sign reference, not a risk threshold.</p>`;
 }
 function overview(p,at,pinned){
  boundary(p);const counts=p.quality.counts,conflicts=Object.entries(p.native_series).filter(([,r])=>r.definition.definition_status!=='reviewed');
  return {hero:`<p class="eyebrow">FR2004 · ORIGINAL-SOURCE RESEARCH</p><h1>${pinned?'Pinned dealer snapshot':'Dealer positions, with evidence'}</h1><p>Inspect net positions, financing and the original publisher records. Each total has a declared scope, valuation basis and reproducible source calculation.</p><p class="warning">WAIT means abstain. Dealer net positions do not establish remaining market-making capacity, a short squeeze or a portfolio allocation.</p>`,
   asof:`${pinned?'Historical snapshot':'Generated'} ${p.generated_at} · source acquired ${p.source_generated_at}`,
   coverage:table(['Native identities','Current under policy','Suppressed or missing latest','Definitions unresolved','Original observations'],[[number(p.quality.total_series),number(Object.values(p.native_series).filter(r=>current(r.quality,at,pinned)).length),number(counts.incomplete||0),number(counts.definition_unverified||0),number(Object.values(p.native_series).reduce((a,r)=>a+r.history.length,0))]]),
   review:table(['Series identity','Definition issue','Inspect original'],conflicts.map(([id,r])=>[esc(id),esc(r.definition.reason),`<button type="button" data-select="series:${esc(id)}">Inspect</button>`])),
   evidence:`<p>${link(p.replay.manifest_key,'Immutable run manifest')} · ${link(p.replay.input_key,'Retained inputs')} · <a id="permanent" href="/primary-dealers.html?run=${esc(p.replay.manifest_key.match(/([a-f0-9]{64})\.json$/)?.[1])}">Permanent snapshot link</a> · <a href="/primary-dealers.html">Latest research</a></p><p>Browser checks the retained output hash. Independently reproduce original-response calculations with <code>scripts/replay_dealer_research.py</code> from the matching release checkout.</p><p>${number(p.revisions.known_changed_values)} values changed since the previous retained acquisition. Historical publication-time knowledge and the provider's complete revision history are unverified.</p><p>Previous engine output, histories and discovery specification are preserved for audit. They do not qualify a forecast. Portfolio consequences require dated exposures, a validated strategy and explicit costs; these reported amounts are not portfolio losses.</p>`};
 }
 function detail(p,id,at,pinned,count=104){
  const row=selected(p,id),q=row.quality,st=row.statistics,period=row.rows.at(-1)?.period;
  let html=`<h2>${esc(row.label)}</h2><p><strong>${number(row.usd_mn)} ${esc(row.display_unit)}</strong> · report date ${esc(row.as_of)}.</p><p>${words(status(q,at,pinned))}. Value basis: ${words(row.valuation_basis)}.</p>`;
  if(row.definition_status!=='reviewed')html+='<p class="warning">Definition unresolved. The original reported numbers remain inspectable; they cannot be used as a labeled market total, normalized flow, risk score or trade signal.</p>';
  if(row.kind==='series'){const d=row.definition,section=p.section_pages[period]?.[d.family];html+=`<p>Publisher description: ${esc(d.catalog.description)}</p><p>${esc(d.note)} ${esc(d.reason||'')}</p><p>Accounting basis: ${words(d.accounting_basis)}. Original observation row ${number(row.original_row_index)}, catalog row ${number(row.catalog_row_index)}. ${link(p.sources[period]?.evidence?.key,'Retained reporting instructions')} · PDF page ${number(section)}.</p><p class="warning">Catalog review period: ${esc(row.reviewed_catalog_period)}. ${esc(row.history_scope_note)} Reporting instructions establish accounting rules; they do not verify the historical mapping of every series identity.</p>`;}
  else html+='<p>Derived history is available only in the reviewed SBN2024 catalog period. Earlier component observations remain available in the native series; earlier group totals are withheld until their historical scope mappings are verified.</p>';
  html+=chart(row.rows,count);
  if(row.kind==='group')html+='<h3>Exact components on this report date</h3>'+table(['Identity','Reported USD millions','Original row index','Status'],Object.entries(row.components).map(([k,v])=>[`<button type="button" data-select="series:${esc(k)}">${esc(k)}</button>`,number(v?.usd_mn),number(v?.row_index),esc(v?.status||'missing_observation')]))+'<p>All declared components must be numeric, reviewed and observed on the same date. Suppressed latest observations never fall back to an older complete total.</p>';
  html+=`<p>${link(p.sources.observations.evidence.key,'Complete original observations')} · ${link(p.sources.catalog.evidence.key,'Original series catalog')} · ${link(p.sources.breaks.evidence.key,'Original source-period boundaries')}.</p><h3>Descriptive comparisons</h3>`;
  if(row.definition_status==='reviewed'&&current(q,at,pinned))html+=table(['Window','Exact baseline','Report date','Change · USD millions','Status'],Object.entries(row.comparisons).sort((a,b)=>parseInt(a[0])-parseInt(b[0])).map(([k,v])=>[esc(k),esc(v.from),esc(v.to),number(v.difference_usd_mn),words(v.status)]))+`<p>${esc(st.method)}. Prior sample ${number(st.prior_n)}: ${esc(st.from_date)} → ${esc(st.to_date)}. Signed z ${number(st.z)}; percentile ${number(st.percentile)}%. ${st.status==='constant_prior_sample'?'Prior sample is constant; z is undefined.':words(st.status)} Percentile is not a probability of market stress.</p>`;
  else html+='<p>Current comparisons withheld because the source is expired, incomplete or its definition is unresolved.</p>';
  html+=`<h3>Dates and limits</h3><p>Acquired ${esc(q.acquired_at)}. Next normal release ${esc(q.next_expected_publication_date)}. Current-data policy permits 24 hours after that time and at most eight days since original acquisition. Actual publication time and holiday adjustments are unverified.</p><p>On-the-run issue identities roll. A tenor series is neither a fixed security nor a full Treasury inventory. Net positions omit gross exposures and hedges. Two-sided financing is neither unique collateral nor measured reuse.</p>`;
  return html;
 }
 function history(p,id,page=0){const row=selected(p,id),all=[...row.rows].reverse(),pages=Math.max(1,Math.ceil(all.length/52));page=Math.max(0,Math.min(pages-1,page));return {page,pages,count:all.length,html:table(['Report date',row.display_unit,'Source period','Source status','Definition review','Original row index'],all.slice(page*52,page*52+52).map(v=>[esc(v.date),number(v.value),esc(v.period),words(v.status),words(v.definition_status||(v.period==='SBN2024'?row.definition_status:'historical scope unverified')),number(v.row_index)]))};}
 function reconciliations(p,at,pinned){const c=p.corporate.reconciliation,t=p.financing.treasury,f=p.settlement_fails;
  let html=`<p>Corporate net bonds ${esc(p.groups.corp_bonds.exact_usd_bn)} + commercial paper ${number(p.corporate.cp_b)} = reported total ${number(p.corporate.total_series_b)} USD bn. Reconciliation ${esc(c.status)}; difference ${number(c.difference_usd_mn)} USD millions. Report ${esc(p.corporate.as_of)} · ${esc(status(p.corporate.quality,at,pinned))}.</p>`;
  html+=`<p>Treasury reverse repo ${number(t.reverse_repo_in_b)} + repo ${number(t.repo_out_b)} = ${number(t.gross_two_sided_b)} USD bn. Report ${esc(t.as_of)} · ${esc(status(t.quality,at,pinned))}. A two-sided financing total does not measure unique collateral.</p>`;
  if(f.scopes)html+=table(['Retained fails context','Report date','FTD / FTR / gross · USD bn','Status'],Object.values(f.scopes).map(r=>[esc(r.scope_id),esc(r.as_of),['ftd','ftr','gross'].map(k=>esc(r.exact_usd_bn[k])).join(' / '),esc(status(r.quality,at,pinned))]))+`<p>${link(f.source_replay.manifest_key,'Fails source snapshot')} · <a href="/fails.html?run=${esc(f.source_replay.manifest_key.match(/([a-f0-9]{64})\.json$/)?.[1])}">Inspect this fails snapshot</a>. Output bytes verified; dealer compilation does not independently rerun the fails model. Including-TIPS and excluding-TIPS scopes overlap and must not be added.</p>`;
  return html;
 }
 async function sha(raw){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',raw))].map(x=>x.toString(16).padStart(2,'0')).join('');}
 async function bytes(fetcher,key,limit){
  if(!path(key))throw Error('Unsupported evidence path');const r=await fetcher(path(key),{cache:'no-store'});if(!r.ok)throw Error('Research artifact unavailable');
  const reader=r.body.getReader(),parts=[];let size=0;
  try{while(true){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>limit){await reader.cancel();throw Error('Research artifact size bound');}parts.push(value);}}finally{reader.releaseLock();}
  const out=new Uint8Array(size);let offset=0;for(const part of parts){out.set(part,offset);offset+=part.length;}return out;
 }
 async function loadSnapshot(id,fetcher=root.fetch.bind(root)){
  if(!/^[a-f0-9]{64}$/.test(id||''))throw Error('Invalid run ID');const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024);
  if(await sha(raw)!==id)throw Error('Run hash differs');const m=JSON.parse(new TextDecoder().decode(raw)),ref=m.output;
  if(m.contract!=='dealer-original-replay.v1'||ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Run identity differs');
  const output=await bytes(fetcher,ref.key,24*1024*1024);if(output.length!==ref.bytes||await sha(output)!==ref.sha256)throw Error('Research output differs');
  const p=JSON.parse(new TextDecoder().decode(output));boundary(p);if(p.generated_at!==m.generated_at)throw Error('Snapshot clock differs');p.replay={manifest_key:key,output_sha256:ref.sha256,input_key:m.input?.key};return p;
 }
 const api={CONTRACT,PREFIX,esc,number,path,boundary,current,status,selections,selected,chart,overview,detail,history,reconciliations,sha,loadSnapshot};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.DealerResearch=api;
})(typeof globalThis==='object'?globalThis:this);
