(function(root){
 'use strict';const CONTRACT='yen-original-research.v1',PREFIX='data/yen-research/';
 const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>v===null||v===undefined||v===''||!Number.isFinite(Number(v))?'—':Number(v).toLocaleString('en-US',{maximumFractionDigits:5});
 const words=v=>esc(v).replaceAll('_',' '),path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(k,label)=>path(k)?`<a href="${esc(path(k))}">${esc(label)}</a>`:'Evidence unavailable';
 const table=(headers,rows)=>`<div class="table-scroll"><table><thead><tr>${headers.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>'<tr>'+r.map(v=>`<td>${v}</td>`).join('')+'</tr>').join('')}</tbody></table></div>`;
 const selected=(p,id)=>p.measurements[id]||p.comparisons[id]||(p.positioning?.id===id?p.positioning:null);
 const choices=p=>[...Object.values(p.measurements),...Object.values(p.comparisons),...(p.positioning?[p.positioning]:[])];
 function boundary(p){
  if(p?.contract!==CONTRACT||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false||p.execution_eligible!==false||p.unwind_risk_score!==null||p.portfolio_action!=='WAIT'||!p.measurements||!p.comparisons)throw Error('Unsupported yen research contract');
  for(const r of choices(p)){
   if(typeof r.id!=='string'||typeof r.unit!=='string')throw Error('Measurement identity differs');
   if(Object.hasOwn(r,'value')&&r.value!==null&&(!Number.isFinite(r.value)||Number(r.value_decimal)!==r.value))throw Error('Measurement amount differs');
   if(r.history&&(r.history.key!==PREFIX+'histories/'+r.history.sha256+'.json'||!Number.isInteger(r.history.observations)||r.history.observations<1))throw Error('History identity differs');
  }return p;
 }
 function h10Expected(at){
  const parts=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(at)).map(p=>[p.type,p.value]));
  const day=Date.UTC(+parts.year,+parts.month-1,+parts.day),weekday=(new Date(day).getUTCDay()+6)%7;
  let monday=day-weekday*86400000;if(weekday===0&&(+parts.hour*60+ +parts.minute)<975)monday-=7*86400000;
  return new Date(monday-3*86400000).toISOString().slice(0,10);
 }
 function status(r,at=Date.now(),pinned=false){
  if(!r)return 'unavailable';const q=r.quality;if(!q)return r.status||'unavailable';
  if(q.status!=='fresh')return q.status;if(pinned)return 'fresh_at_snapshot';
  const age=at-Date.parse(q.acquired_at);if(!Number.isFinite(age)||age<0||age>26*3600000)return 'expired_source';
  if(r.id==='DEXJPUS'&&r.as_of<h10Expected(at))return 'release_due_unverified';
  if(q.max_observation_age_days!==null&&Number.isFinite(q.max_observation_age_days)&&Math.floor(at/86400000)-Math.floor(Date.parse(r.as_of)/86400000)>q.max_observation_age_days)return 'stale_observation';
  return 'fresh';
 }
 const current=(r,at,pinned)=>['fresh','fresh_at_snapshot'].includes(status(r,at,pinned));
 function summary(p,at=Date.now(),pinned=false){
  const eligible=Object.values(p.measurements).filter(r=>current(r,at,pinned)).length;
  const board=table(['Series','Reported value · native unit','Observation','Source state'],Object.values(p.measurements).map(r=>[
   `<button type="button" class="inspect" data-select="${esc(r.id)}">${esc(r.name)}<br><small>${esc(r.id)}</small></button>`,
   `${current(r,at,pinned)?number(r.value):'—'} ${esc(r.unit)}`,esc(r.as_of),words(status(r,at,pinned))]));
  const comparisons=table(['Comparison','Latest reported month','Difference · pp','Qualification'],Object.values(p.comparisons).map(r=>{
   const live=current(p.measurements[r.left],at,pinned)&&current(p.measurements[r.right],at,pinned);
   return [`<button type="button" class="inspect" data-select="${esc(r.id)}">${esc(r.left)} − ${esc(r.right)}</button>`,esc(r.as_of),live?number(r.value):'—',words(live?r.status:'source_not_current')];}));
  const evidence=`<p>${esc(p.history_basis)}</p><p>${esc(p.decision_boundary)}</p><p>${esc(p.portfolio_consequence)}</p><p>${link(p.replay.manifest_key,'Immutable replay manifest')} · ${link(p.replay.input_key,'Retained input references')}</p><p>Output SHA-256: <code>${esc(p.replay.output_sha256)}</code></p><p><code>python scripts/replay_yen_research.py --run ${esc(p.replay.manifest_key.split('/').at(-1).replace('.json',''))}</code></p><p>The preceding engine packet is preserved in a protected audit backup: <code>${esc(p.legacy.sha256)}</code>. No historical publication-time knowledge is invented.</p><p>Source errors: ${esc(JSON.stringify(p.errors))}</p>`;
  return {board,comparisons,evidence,state:`${pinned?'Archived snapshot':'Current publication'} · ${eligible}/7 original FRED series within their displayed source rules. Research only; no qualified Calls or sizing vote.`,asof:`Compiled ${p.generated_at}. Each observation retains its own date. ${pinned?'Freshness labels refer to this snapshot.':'Expired sources are withheld from the current board.'}`};
 }
 function positioning(p,at,pinned){
  const r=p.positioning;if(!r)return '<p>Original CFTC positioning unavailable. No crowding or unwind inference is available.</p>';
  const live=current(r,at,pinned),rows=Object.entries(r.current.categories).map(([k,v])=>[words(k),number(v.long),number(v.short),number(v.spreading),number(v.net_contracts),number(v.net_pct_oi_decimal),number(r.statistics[k].z),number(r.statistics[k].percentile)]);
  return `<p>Positions at ${esc(r.as_of)} · ${words(status(r,at,pinned))} · ${number(r.current.open_interest)} open contracts.</p>${!live?'<p class="warning">Historical positioning retained below; it is not current evidence.</p>':''}`+
   table(['Classification','Long','Short','Spreading','Net','Net / OI · %','Prior 260 z','Prior percentile'],rows)+
   `<p>${esc(r.category_reconciliation)}</p><p>${esc(r.scope)}</p><p>${esc(r.quote_orientation)}</p><p>Spreading contributes to both reported sides. Never add both sides to estimate unique open interest. A missing nonreportable spreading field is not zero.</p><button type="button" data-select="${esc(r.id)}">Inspect all ${number(r.history.observations)} reports</button>`;
 }
 function detail(p,id,at,pinned){
  const r=selected(p,id);if(!r)throw Error('Unknown series');const originals=r.originals||{};
  const refs=[];function walk(v){if(v?.evidence?.key){refs.push(v);return;}if(v&&typeof v==='object')for(const x of Object.values(v))walk(x);}walk(originals);
  const links=refs.map(v=>`<li>${link(v.evidence.key,'Retained original response')} · <code>${esc(v.url)}</code> · acquired ${esc(v.acquired_at)} · SHA-256 <code>${esc(v.evidence.sha256)}</code></li>`).join('');
  return `<h2>${esc(r.name||r.id)}</h2><p class="measurement"><strong>${Object.hasOwn(r,'value_decimal')?esc(r.value_decimal):'Reported positions'} ${esc(r.unit)}</strong></p><p>Observation ${esc(r.as_of)} · ${words(status(r,at,pinned))}. This panel preserves the reported snapshot value even when the current board withholds it.</p><p>${esc(r.limitation||r.scope)}</p>`+
   (r.quality?`<p>Acquired ${esc(r.quality.acquired_at)}. Observation age at compilation: ${number(r.quality.observation_age_days)} days. ${esc(r.quality.publication_cadence||'Observation-age limit; historical publication timestamp is not verified.')}</p>`:'')+
   (r.components?`<details><summary>Exact comparison components and source rows</summary><pre>${esc(JSON.stringify(r.components,null,2))}</pre></details><p>Inspect original input series: <button type="button" data-select="${esc(r.left)}">${esc(r.left)}</button> <button type="button" data-select="${esc(r.right)}">${esc(r.right)}</button></p>`:'')+
   `<p>${r.history?`${number(r.history.observations)} retained observations from ${esc(r.history.from)} to ${esc(r.history.to)}. ${link(r.history.key,'Full verified history')}`:'History unavailable.'}</p>`+
   (id==='JPNASSETS'?`<p>${esc(p.boj_assets_trillion_jpy_decimal)} trillion JPY. ${esc(p.boj_asset_conversion)}</p>`:'')+
   (p.monthly_changes[id]?`<details><summary>Calendar six- and twelve-month changes</summary><pre>${esc(JSON.stringify(p.monthly_changes[id],null,2))}</pre></details>`:'')+
   (id==='DEXJPUS'?`<details><summary>FX endpoint changes and realized volatility</summary><pre>${esc(JSON.stringify(p.fx_measurement,null,2))}</pre></details>`:'')+
   `<details><summary>Inspect original responses and definitions</summary><ul>${links||'<li>Comparison originals are attached to its two input series.</li>'}</ul><pre>${esc(JSON.stringify(r.definition||{},null,2))}</pre></details>`;
 }
 function history(h,page=0){
  if(!h)return {page:0,pages:1,count:0,html:'No retained history.'};const pages=Math.max(1,Math.ceil(h.rows.length/40));page=Math.max(0,Math.min(pages-1,page));
  const cols=Object.keys(h.rows[0]);return {page,pages,count:h.rows.length,html:table(cols,[...h.rows].reverse().slice(page*40,page*40+40).map(r=>cols.map(k=>esc(typeof r[k]==='object'&&r[k]!==null?JSON.stringify(r[k]):r[k]))))};
 }
 function chart(h,range=260){
  if(!h)return '<p>No retained history.</p>';if(h.unit==='contracts')return '<p>The complete five-category positions and open-interest reconciliation are available in the table below.</p>';
  const rows=(range?h.rows.slice(-range):h.rows).map(r=>({...r,value:r.value_decimal===null?null:Number(r.value_decimal)})),valid=rows.filter(r=>r.value!==null&&Number.isFinite(r.value));
  if(!valid.length)return '<p>No numeric observations in this range.</p>';
  const W=1050,H=250,pad=60,min=Math.min(...valid.map(r=>r.value)),max=Math.max(...valid.map(r=>r.value)),span=max-min||1,from=Date.parse(rows[0].date),to=Date.parse(rows.at(-1).date),xs=d=>pad+(Date.parse(d)-from)/Math.max(86400000,to-from)*(W-2*pad),ys=v=>H-pad-(v-min)/span*(H-2*pad);
  let d='',prior=null;const maxGap={D:6,W:10,M:35}[h.frequency]||1;
  for(const r of rows){if(r.value===null){prior=null;continue;}const gap=prior?(Date.parse(r.date)-Date.parse(prior.date))/86400000:Infinity;d+=(gap<=maxGap?'L':'M')+xs(r.date).toFixed(2)+','+ys(r.value).toFixed(2)+' ';prior=r;}
  return `<svg class="history-chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(h.id)} dated history"><text x="12" y="25">${number(max)}</text><text x="12" y="${H-pad+4}">${number(min)}</text><text x="${pad}" y="${H-15}">${esc(rows[0].date)}</text><text x="${W-pad}" text-anchor="end" y="${H-15}">${esc(rows.at(-1).date)}</text><path d="${d}" stroke="#d5b372" fill="none" stroke-width="2"/></svg><p class="dim">Linear calendar axis · ${esc(h.unit)}. Missing observations break the line; no interpolation.</p>`;
 }
 function scenario(notional,start,end,usdRate,jpyRate,days,usdBasis,jpyBasis,fees){
  const vals=[notional,start,end,usdRate,jpyRate,days,usdBasis,jpyBasis,fees];if(vals.some(v=>typeof v!=='number'||!Number.isFinite(v)))throw Error('Enter every scenario assumption');
  if(notional<0||notional>1e12||start<=0||end<=0||start>1e6||end>1e6||Math.min(usdRate,jpyRate)<-20||Math.max(usdRate,jpyRate)>100||!Number.isInteger(days)||days<1||days>3660||![360,365].includes(usdBasis)||![360,365].includes(jpyBasis)||fees<0||fees>1e15)throw Error('Scenario assumptions outside supported bounds');
  const usdInterest=notional*usdRate/100*days/usdBasis,jpyBorrowed=notional*start,jpyInterest=jpyBorrowed*jpyRate/100*days/jpyBasis,usdEnd=notional+usdInterest;
  if(usdEnd<0||jpyBorrowed+jpyInterest<0)throw Error('Simple-interest assumptions imply negative principal repayment');
  const fx=notional*(end-start),investment=usdInterest*end,net=fx+investment-jpyInterest-fees;
  return {contract:'yen-entered-scenario.v1',assumptions:{notional_usd:notional,starting_jpy_per_usd:start,ending_jpy_per_usd:end,usd_rate_pct:usdRate,jpy_rate_pct:jpyRate,days,usd_day_basis:usdBasis,jpy_day_basis:jpyBasis,fees_jpy:fees},
   jpy_borrowed:jpyBorrowed,usd_ending_asset:usdEnd,jpy_repayment:jpyBorrowed+jpyInterest,fx_principal_pnl_jpy:fx,usd_interest_converted_jpy:investment,jpy_funding_interest:jpyInterest,net_pnl_jpy:net,
   break_even_jpy_per_usd:usdEnd>0?(jpyBorrowed+jpyInterest+fees)/usdEnd:null,
   formula:'N*(S1-S0) + N*rUSD/100*days/USD_basis*S1 - N*S0*rJPY/100*days/JPY_basis - fees_JPY',
   limitations:'Unhedged, simple interest, constant entered rates and fixed JPY fees. No price/default/margin/refinancing or portfolio exposure model.',forecast:false,position_size:null};
 }
 async function sha(raw){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',raw))].map(x=>x.toString(16).padStart(2,'0')).join('');}
 async function bytes(fetcher,key,limit){
  if(!path(key))throw Error('Unsupported evidence path');const r=await fetcher(path(key),{cache:'no-store'});if(!r.ok)throw Error('Research artifact unavailable');
  const reader=r.body.getReader(),parts=[];let size=0;try{while(true){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>limit){await reader.cancel();throw Error('Artifact size bound');}parts.push(value);}}finally{reader.releaseLock();}
  const out=new Uint8Array(size);let offset=0;for(const part of parts){out.set(part,offset);offset+=part.length;}return out;
 }
 async function loadSnapshot(id,fetcher=root.fetch.bind(root)){
  if(!/^[a-f0-9]{64}$/.test(id||''))throw Error('Invalid run ID');const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024);
  if(await sha(raw)!==id)throw Error('Run hash differs');const m=JSON.parse(new TextDecoder().decode(raw)),ref=m.output;
  if(m.contract!=='yen-original-replay.v1'||ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Run identity differs');
  const output=await bytes(fetcher,ref.key,4*1024*1024);if(output.length!==ref.bytes||await sha(output)!==ref.sha256)throw Error('Research output differs');
  const p=JSON.parse(new TextDecoder().decode(output));boundary(p);if(p.generated_at!==m.generated_at)throw Error('Snapshot clock differs');p.replay={manifest_key:key,output_sha256:ref.sha256,input_key:m.input?.key};return p;
 }
 async function loadHistory(row,fetcher=root.fetch.bind(root)){
  const ref=row.history;if(!ref)return null;if(ref.key!==PREFIX+'histories/'+ref.sha256+'.json')throw Error('History identity differs');
  const raw=await bytes(fetcher,ref.key,8*1024*1024);if(raw.length!==ref.bytes||await sha(raw)!==ref.sha256)throw Error('History bytes differ');
  const h=JSON.parse(new TextDecoder().decode(raw));if(h.contract!=='yen-history.v1'||h.id!==row.id||h.unit!==row.unit||h.frequency!==row.frequency||h.rows.length!==ref.observations)throw Error('History contract differs');
  if(h.rows.some((r,i)=>!/^\d{4}-\d{2}-\d{2}$/.test(r.date)||(i&&r.date<=h.rows[i-1].date)||(Object.hasOwn(r,'value_decimal')&&r.value_decimal!==null&&!Number.isFinite(Number(r.value_decimal)))))throw Error('History rows differ');
  if(h.rows[0].date!==ref.from||h.rows.at(-1).date!==ref.to)throw Error('History endpoints differ');return h;
 }
 const api={CONTRACT,PREFIX,esc,number,words,path,table,boundary,h10Expected,status,current,choices,selected,summary,positioning,detail,history,chart,scenario,sha,loadSnapshot,loadHistory};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.YenResearch=api;
})(typeof globalThis==='object'?globalThis:this);
