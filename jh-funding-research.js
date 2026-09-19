(function(root){
 'use strict';
 const CONTRACT='funding-original-research.v1',PREFIX='data/funding-research/';
 const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>v===null||v===undefined||v===''||!Number.isFinite(Number(v))?'—':Number(v).toLocaleString('en-US',{maximumFractionDigits:5});
 const words=v=>esc(v).replaceAll('_',' '),path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(k,label)=>path(k)?`<a href="${esc(path(k))}">${esc(label)}</a>`:'Evidence unavailable';
 const table=(headers,rows)=>`<div class="table-scroll"><table><thead><tr>${headers.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>'<tr>'+r.map(v=>`<td>${v}</td>`).join('')+'</tr>').join('')}</tbody></table></div>`;
 function boundary(p){
  if(p?.contract!==CONTRACT||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false||p.execution_eligible!==false||p.portfolio_action!=='WAIT'||p.plumbing_health!==null||!p.measurements||!p.comparisons)throw Error('Unsupported funding research contract');
  for(const [id,r] of Object.entries({...p.measurements,...p.comparisons})){
   if(r.id!==id||typeof r.unit!=='string'||r.calls_eligible!==false||r.sizing_eligible!==false)throw Error('Measurement identity or authority differs');
   if(r.value!==null&&(!Number.isFinite(r.value)||Number(r.value_decimal)!==r.value))throw Error('Measurement amount differs');
   if(r.history&&(r.history.key!==PREFIX+'histories/'+r.history.sha256+'.json'||!Number.isInteger(r.history.observations)||r.history.observations<1))throw Error('History identity differs');
  }
  return p;
 }
 function status(row,at=Date.now(),pinned=false){
  const q=row.quality;if(!q)return row.status||'unavailable';
  if(q.status!=='fresh')return q.status;
  if(pinned)return 'fresh_at_snapshot';
  const acquired=Date.parse(q.acquired_at),observed=Date.parse(q.observation_date+'T00:00:00Z'),age=at-acquired;
  if(!Number.isFinite(age)||age<0||age>q.max_acquisition_age_hours*3600000)return 'expired_source';
  if(q.next_expected_publication_date&&at>Date.parse(q.next_expected_publication_date)+86400000)return 'overdue_release';
  if(q.max_observation_age_days!==undefined&&(Math.floor(at/86400000)-Math.floor(observed/86400000)>q.max_observation_age_days||observed>at))return 'stale_observation';
  return 'fresh';
 }
 const current=(r,at,pinned)=>['fresh','fresh_at_snapshot'].includes(status(r,at,pinned));
 function selections(p){return [...Object.values(p.comparisons).map(r=>({id:r.id,label:'Comparison · '+r.label})),...Object.values(p.measurements).map(r=>({id:r.id,label:r.layer+' · '+r.label}))];}
 function selected(p,id){const r=p.measurements[id]||p.comparisons[id];if(!r)throw Error('Unknown measurement');return r;}
 function summary(p,at,pinned){
  const values=Object.values(p.measurements),fresh=values.filter(r=>current(r,at,pinned)).length,final=values.filter(r=>r.role==='final_vintage_reference').length;
  return {hero:`<div class="eyebrow">FUNDING RESEARCH · ORIGINAL EVIDENCE</div><h1>Follow the funding, date by date.</h1><p>Inspect reported rates, balances and settlement conditions. Each comparison names its inputs, units and limitations.</p><p class="warning"><strong>WAIT · no qualified directional vote.</strong> These measurements do not establish a crisis probability or authorize position size.</p>`,
    asof:`${pinned?'Archived snapshot — not current market data. ':''}Compiled ${p.generated_at}. Each measurement has its own observation and acquisition dates.`,
    coverage:table(['Retained measurements','Current under policy','Historical final references','Unavailable source requests'],[[number(values.length),number(fresh),number(final),number(Object.keys(p.errors).length)]]),
    board:table(['Dated comparison','Value','Observation date','Status','Interpretation limit'],Object.values(p.comparisons).map(r=>[`<button data-select="${esc(r.id)}" type="button">${esc(r.label)}</button>`,`${number(current(r,at,pinned)?r.value:null)} ${esc(r.unit)}`,esc(r.as_of),words(status(r,at,pinned)),esc(r.limitation)])),
    gaps:table(['Source request','Acquisition / validation result'],Object.entries(p.errors).map(([k,v])=>[esc(k),words(v)]))+
      table(['Capability','Still required'],Object.entries(p.missing_capabilities).map(([k,v])=>[words(k),esc(v)])),
    evidence:`<p>${link(p.replay.manifest_key,'Immutable run manifest')} · ${link(p.replay.input_key,'Retained inputs')} · <a id="permanent" href="/eurodollar.html?run=${esc(p.replay.manifest_key.match(/([a-f0-9]{64})\.json$/)?.[1])}">Permanent snapshot link</a> · <a href="/eurodollar.html">Latest research</a></p><p>Browser verifies the output and selected history hashes. Reproduce all original-response calculations and history shards with <code>scripts/replay_funding_research.py</code> from the matching release checkout. Current acquisition histories do not establish what was known at each past publication.</p>`};
 }
 function evidence(row){
  const refs=row.evidence;if(!refs)return '';
  return refs.key?link(refs.key,'Retained original response'):Object.entries(refs).map(([k,v])=>link(v.key,'Original '+k)).join(' · ');
 }
 function detail(p,id,at,pinned){
  const r=selected(p,id),s=status(r,at,pinned),q=r.quality,stats=r.statistics;
  let html=`<h2>${esc(r.label)}</h2><p class="measurement"><strong>${number(r.value)} ${esc(r.unit)}</strong></p><p>Observation ${esc(r.as_of)} · <strong>${words(s)}</strong>${r.source_vintage?' · '+words(r.source_vintage):''}.</p>`;
  if(!current(r,at,pinned))html+='<p class="warning">The displayed observation is retained evidence. It is not qualified as current data for a decision.</p>';
  html+=`<p>${esc(r.limitation||r.definition?.basis||'Native source definition is retained with the original response.')}</p>`;
  if(r.formula)html+=`<p><strong>Calculation:</strong> ${esc(r.formula)}. Anchor ${esc(r.anchor_series||'matched quote times')}.</p>`+
    table(['Input','Exact value','Date / quote timestamp','Original row'],Object.entries(r.components||{}).map(([key,v])=>[`<button type="button" data-select="${esc(key)}">${esc(key)}</button>`,esc(v?.value_decimal),esc(v?.date||v?.quoted_at),number(v?.row_index)]));
  html+=`<p>${evidence(r)}${r.history?' · '+link(r.history.key,'Complete verified history'):''}</p>`;
  if(q)html+=`<p class="dim">Acquired ${esc(q.acquired_at)}. Maximum acquisition age ${number(q.max_acquisition_age_hours)} hours; maximum observation age ${number(q.max_observation_age_days)} calendar days. Actual publication time and holiday-adjusted release calendar are unverified.</p>`;
  if(r.original_row_index!==undefined)html+=`<p class="dim">Original response row ${number(r.original_row_index)}. Series ${esc(r.id)}. ${esc(r.quoted_at||'')}</p>`;
  if(current(r,at,pinned)&&r.changes)html+=table(['Window','Exact baseline','End date','Difference · native unit','Availability'],Object.entries(r.changes).sort((a,b)=>parseInt(a[0])-parseInt(b[0])).map(([k,v])=>[esc(k),esc(v.from),esc(v.to),esc(v.difference_decimal),words(v.status)]));
  if(current(r,at,pinned)&&stats)html+=`<p>Prior sample ${number(stats.prior_n)} / ${number(stats.requested_prior)} observations, ${esc(stats.from)} → ${esc(stats.to)}. Signed z ${number(stats.z)}; percentile ${number(stats.percentile)}%. ${words(stats.status)}. Current observation is excluded. ${number(stats.excluded_missing_observations||0)} missing prior daily records are excluded; gaps beyond six calendar days break the comparable sample. Percentile is a descriptive rank, not a crisis probability.</p>`;
  return html;
 }
 function chart(h,range=260){
  if(!h)return '<p>No retained history for this comparison.</p>';
  const di=h.columns.indexOf('date'),vi=h.columns.indexOf('value_decimal'),all=h.rows.map(r=>({date:r[di],value:r[vi]===null?null:Number(r[vi])})),rows=range?all.slice(-range):all,valid=rows.filter(r=>r.value!==null&&Number.isFinite(r.value));
  if(!valid.length)return '<p>No numeric observations in this range.</p>';
  const W=1050,H=250,pad=60,min=Math.min(...valid.map(r=>r.value)),max=Math.max(...valid.map(r=>r.value)),span=max-min||1,from=Date.parse(rows[0].date),to=Date.parse(rows.at(-1).date),xs=d=>pad+(Date.parse(d)-from)/Math.max(86400000,to-from)*(W-2*pad),ys=v=>H-pad-(v-min)/span*(H-2*pad);
  let d='',prior=null;const maxGap={D:6,W:8,M:35,quote:1}[h.frequency]||1;
  for(const r of rows){if(r.value===null){prior=null;continue;}const gap=prior?(Date.parse(r.date)-Date.parse(prior.date))/86400000:Infinity;d+=(gap<=maxGap?'L':'M')+xs(r.date).toFixed(2)+','+ys(r.value).toFixed(2)+' ';prior=r;}
  return `<svg class="history-chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(h.id)} dated history"><text x="12" y="25">${number(max)}</text><text x="12" y="${H-pad+4}">${number(min)}</text><text x="${pad}" y="${H-15}">${esc(rows[0].date)}</text><text x="${W-pad}" text-anchor="end" y="${H-15}">${esc(rows.at(-1).date)}</text><path d="${d}" stroke="#d5b372" fill="none" stroke-width="2"/>${valid.length===1?`<circle cx="${xs(valid[0].date)}" cy="${ys(valid[0].value)}" r="4" fill="#d5b372"/>`:''}</svg><p class="dim">${number(rows.length)} retained dates · ${esc(h.unit)} · linear calendar axis. Missing values break the line; no interpolation or resampling. Exact values are below.</p>`;
 }
 function history(h,page=0){
  if(!h)return {page:0,pages:1,count:0,html:'No retained history.'};
  const pages=Math.max(1,Math.ceil(h.rows.length/52));page=Math.max(0,Math.min(pages-1,page));
  return {page,pages,count:h.rows.length,html:table(h.columns,[...h.rows].reverse().slice(page*52,page*52+52).map(r=>r.map(v=>esc(v))))};
 }
 function fails(p,at,pinned){const f=p.settlement_fails;
  if(!f.scopes||!Object.keys(f.scopes).length)return '<p>Original settlement-fails context unavailable.</p>';
  return table(['Separate scope','Report date','FTD / FTR / two-sided gross · USD bn','Status'],Object.values(f.scopes).map(r=>[esc(r.scope_id),esc(r.as_of),['ftd','ftr','gross'].map(k=>esc(r.exact_usd_bn[k])).join(' / '),words(status(r,at,pinned))]))+`<p>${esc(f.note)}</p><p>${link(f.source_replay.manifest_key,'Retained fails snapshot')}. Output identity verified; the funding compiler does not independently rerun the fails model.</p>`;
 }
 function vintages(p){return table(['Final series','Final observation','Preliminary observation','Different common dates'],Object.entries(p.ofr_vintage_comparisons).map(([k,r])=>[esc(k),esc(r.final_as_of),esc(r.preliminary_as_of),number(r.different_dates)]))+'<p>Final and preliminary OFR histories stay separate. Differences compare the two retained responses; this is not a complete publication-time revision history.</p>';}
 function retention(p){return `<p>The prior engine packet is preserved in a protected audit backup. ${link(p.legacy_retention.inventory?.key,'Typed legacy measurement inventory')} keeps prior inputs visible without qualifying their claims.</p>`+
   table(['Retained engine context','Generated','Qualification'],Object.entries(p.context_snapshots).map(([k,v])=>[esc(k),esc(v.source_generated_at),words(v.status)]));}
 function scenario(notional,shock,days,basis){
  const values=[notional,shock,days,basis];if(values.some(v=>typeof v!=='number'||!Number.isFinite(v)))throw Error('Enter all scenario assumptions');
  if(notional<0||notional>1e12||Math.abs(shock)>10000||days<=0||days>3660||!Number.isInteger(days)||![360,365].includes(basis))throw Error('Scenario assumptions are outside supported bounds');
  return {incremental_usd:notional*shock/10000*days/basis,notional_usd:notional,shock_bp:shock,days,day_basis:basis,formula:'USD liability × shock bp / 10,000 × days / day basis',forecast:false,position_size:null};
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
  if(m.contract!=='funding-original-replay.v1'||ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Run identity differs');
  const output=await bytes(fetcher,ref.key,4*1024*1024);if(output.length!==ref.bytes||await sha(output)!==ref.sha256)throw Error('Research output differs');
  const p=JSON.parse(new TextDecoder().decode(output));boundary(p);if(p.generated_at!==m.generated_at)throw Error('Snapshot clock differs');p.replay={manifest_key:key,output_sha256:ref.sha256,input_key:m.input?.key};return p;
 }
 async function loadHistory(row,fetcher=root.fetch.bind(root)){
  const ref=row.history;if(!ref)return null;
  if(ref.key!==PREFIX+'histories/'+ref.sha256+'.json')throw Error('History identity differs');
  const raw=await bytes(fetcher,ref.key,8*1024*1024);if(raw.length!==ref.bytes||await sha(raw)!==ref.sha256)throw Error('History bytes differ');
  const h=JSON.parse(new TextDecoder().decode(raw));if(h.contract!=='funding-history.v1'||h.id!==row.id||h.unit!==row.unit||h.rows.length!==ref.observations||h.columns.slice(0,4).join(',')!=='date,value_decimal,row_index,status')throw Error('History contract differs');
  if(h.rows.some((r,i)=>r.length!==h.columns.length||!/^\d{4}-\d{2}-\d{2}$/.test(r[0])||(i&&r[0]<=h.rows[i-1][0])||(r[1]!==null&&!Number.isFinite(Number(r[1])))))throw Error('History rows differ');
  if(h.rows[0][0]!==ref.first||h.rows.at(-1)[0]!==ref.last)throw Error('History endpoint differs');return h;
 }
 const api={CONTRACT,PREFIX,esc,number,words,path,boundary,status,current,selections,selected,summary,detail,chart,history,fails,vintages,retention,scenario,sha,loadSnapshot,loadHistory};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.FundingResearch=api;
})(typeof globalThis==='object'?globalThis:this);
