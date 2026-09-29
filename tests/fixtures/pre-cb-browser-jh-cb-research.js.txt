(function(root){
 'use strict';
 const CONTRACT='cb-research.v1',PREFIX='data/cb-research/';
 const esc=value=>String(value??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=value=>value===null||value===undefined||value===''||typeof value==='boolean'||!Number.isFinite(Number(value))?null:Number(value);
 const num=value=>number(value)===null?'Unavailable':number(value).toLocaleString('en-US',{maximumFractionDigits:4});
 const unit=value=>({'USD_bn':'USD billions','EUR_bn':'EUR billions','JPY_bn':'JPY billions','CHF_bn':'CHF billions','JPY_per_USD':'JPY per USD','CHF_per_USD':'CHF per USD','USD_per_EUR':'USD per EUR','percent_per_annum':'percent per year'}[value]||value||'—');
 const path=key=>typeof key==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(key)&&!key.includes('..')?'/'+key:null;
 const link=(key,label)=>path(key)?`<a href="${path(key)}">${esc(label)}</a>`:esc(label+' unavailable');
 function recent(stamp,now,hours=26){const age=now-Date.parse(stamp);return Number.isFinite(age)&&age>=0&&age<=hours*3600000;}
 function observed(day,now,days){const age=Math.floor(now/86400000)-Math.floor(Date.parse(day)/86400000);return Number.isFinite(age)&&Number.isFinite(days)&&age>=0&&age<=days;}
 function fresh(m,p,now){const q=m?.quality||{};return q.status==='fresh'&&recent(p.generated_at,now)&&recent(q.source_generated_at,now)&&recent(q.acquired_at,now)&&observed(q.observation_date,now,q.max_age_days);}
 function table(headers,rows){return `<div class="table"><table><thead><tr>${headers.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>'<tr>'+row.map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('')}</tbody></table></div>`;}
 function rowEvidence(row,label){if(!row)return `<p>${esc(label)} unavailable.</p>`;
  return `<p><b>${esc(label)}</b> · ${esc(row.date)} (provider period ${esc(row.period)})<br>Native value ${esc(row.native_decimal)} · source status ${esc(row.source_status)} · original row ${esc(row.original_row_index)}<br>${link(row.original?.key,'Original response')}</p>`;
 }
 function evidence(m,horizon){const c=m.changes?.[horizon]||{};
  return `<details><summary>Measurement and comparison evidence</summary><p>${esc(m.source_id)} · original unit ${esc(m.source_unit)} · multiplier ${esc(m.scale_decimal??m.selected?.scale_decimal)} to ${esc(unit(m.unit))}<br>${esc(m.period_basis)}</p>`+
   rowEvidence(m.selected,'Latest observation')+rowEvidence(c.baseline,'Calendar baseline')+
   `<p>Target ${esc(c.target_date)} · ${esc(c.start_date)} → ${esc(c.end_date)}<br>${link(m.definition?.key,'Original definition')}<br>Acquired ${esc(m.quality?.acquired_at)}<br>Source packet ${esc(m.quality?.source_generated_at)}</p></details>`;
 }
 function bank(b,p,now,pinned,horizon){
  const m=b.balance_sheet||{},r=b.rate||{},ma=pinned||fresh(m,p,now),ra=pinned||fresh(r,p,now),change=r.changes?.[horizon];
  return `<article class="cb"><h3>${esc(b.cb)}</h3><div class="stance">${num(ma?m.latest:null)} ${esc(unit(m.unit))}</div>`+
   `<p>${esc(m.quality?.observation_date)} · ${esc(m.source_id)}<br>${esc(ma?'As measured: '+m.quality?.status:'Unavailable or expired')}</p><p>${esc(horizon)} calendar-month stock change: ${num(ma?m.changes?.[horizon]?.level_change:null)} ${esc(unit(m.unit))}.</p>${evidence(m,horizon)}`+
   `<hr><p>${esc(b.rate_definition)}</p><strong>${num(ra?r.latest:null)} ${ra&&number(r.latest)!==null?'%':''}</strong>`+
   `<p>${esc(r.quality?.observation_date)} · ${esc(r.source_id)}<br>${esc(horizon)} calendar-month change: ${num(ra?change?.level_change:null)} percentage points.</p>${evidence(r,horizon)}</article>`;
 }
 function decomposition(b,p,now,pinned){
  const d=b.decomposition||{},valid=d.status==='partial_attribution'&&(pinned||(recent(p.generated_at,now)&&observed(d.observation_date,now,21)&&Object.values(d.components||{}).every(m=>fresh(m,p,now))));
  if(!Object.keys(d.components||{}).length)return `<section class="panel"><h3>${esc(b.cb)} · component history unavailable</h3><p>Missing accounting inputs: ${esc((d.missing||[]).join('; '))}.</p><p>Net policy injection remains unidentified.</p></section>`;
  const rows=Object.entries(d.components||{}).map(([name,m])=>[`${esc(name.replaceAll('_',' '))}<br><small>${esc(m.source_id)}</small>`,num(valid?m.aligned_value:null),num(valid?m.aligned_change_1m:null),
    `<details><summary>Matched source rows</summary>${rowEvidence(m.aligned_current,'Ending component')}${rowEvidence(m.aligned_baseline,'Baseline component')}</details>`]);
  if(rows.length)rows.push(['Other assets and adjustments',num(valid?d.other_assets_and_adjustments_level:null),num(valid?d.other_assets_and_adjustments_change_1m:null),'Accounting remainder; transaction and valuation attribution unavailable.']);
  return `<section class="panel"><h3>${esc(b.cb)} · component stocks</h3><p>${esc(String(d.status).replaceAll('_',' '))} · ${esc(unit(d.unit))}<br>${esc(d.start_date)} → ${esc(d.observation_date)} · calendar-month target ${esc(d.calendar_target_date)}</p>`+
   (rows.length?table(['Component','Aligned level','One-month stock change','Evidence'],rows):`<p>${esc((d.missing||[]).join('; '))}</p>`)+
   `<p>Total stock change: ${num(valid?d.stock_change_1m:null)} ${esc(unit(d.unit))}. Arithmetic residual: ${num(valid?d.reconciliation_residual:null)}.</p><p>Net policy injection: <b>not identified</b>. Purchases, maturities and valuation require separate accounting.</p></section>`;
 }
 function settlement(p,now,pinned){const f=p.pd_settlement_fails||{};
  const rows=[f,f.ust_ex_tips||{}].map(r=>{const usable=r.quality?.status==='fresh'&&r.reconciliation?.consistent===true&&(pinned||(recent(p.generated_at,now)&&recent(f.source_generated_at,now)&&observed(r.as_of,now,14)));
   return [esc(r.label||r.scope_id),esc(r.as_of),num(usable?r.ftd_bn:null),num(usable?r.ftr_bn:null),num(usable?r.combined_bn:null),esc(r.quality?.status)];});
  return `<p>Retained engine context. Original FR2004 provider responses have not been verified in this research run. Two-sided gross is not unique securities, defaults, cash inflow or central-bank injection.</p>`+
   table(['Separate scope','Observation','FTD, USD bn','FTR, USD bn','Combined, USD bn','Status'],rows)+`<p>${esc(f.scope_note)}</p>`;
 }
 function render(p,now=Date.now(),pinned=false,horizon='1'){
  if(p.contract!==CONTRACT||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false)throw Error('Supported descriptive research contract required');
  if(!['1','6','12'].includes(horizon))throw Error('Unsupported calendar horizon');
  const available=Object.values(p.measurements||{}).filter(m=>fresh(m,p,now)).length;
  const ecbAvailable=Object.values(p.ecb_components||{}).filter(m=>fresh(m,p,now)).length;
  const id=p.replay?.manifest_key?.match(/^data\/cb-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];
  return {hero:`<div class="hero"><div class="lab">Source-backed research · Calls: abstain</div><div class="big">Stocks &amp; rates</div><p>${esc(pinned?'Pinned snapshot — values are dated as published':available+' of '+p.quality?.expected_native_series+' native measurements and '+ecbAvailable+' of '+(p.quality?.expected_ecb_series??0)+' original ECB component series currently usable')}</p><p>${esc(p.decision?.reason)}</p></div>`,
   cbs:(p.central_banks||[]).map(b=>bank(b,p,now,pinned,horizon)).join(''),
   carry:(p.central_banks||[]).map(b=>decomposition(b,p,now,pinned)).join(''),
   edollar:table(['FX quotation','Value','Observation','Evidence'],Object.entries(p.fx_context||{}).map(([name,m])=>[esc(unit(m.unit||name)),num(pinned||fresh(m,p,now)?m.latest:null),esc(m.quality?.observation_date),evidence(m,horizon)])),
   fails:settlement(p,now,pinned),
   evidence:link(p.replay?.manifest_key,'Immutable run manifest')+(id?` · <a href="/cb-injection.html?run=${id}">Permanent snapshot link</a>`:'')+' · '+link(p.source_replay?.manifest_key,'Canonical FRED source run')+
     `<details><summary>Duplicate sources and dependencies</summary><pre>${esc(JSON.stringify({source_comparisons:p.source_comparisons,dependencies:p.dependency_groups},null,2))}</pre></details>`,
   note:p.note,ts:`Published ${p.generated_at} · source packet ${p.source_generated_at} · ${p.methodology_version}`};
 }
 async function bytes(fetcher,key,limit=8*1024*1024){
  if(!path(key))throw Error('Unsupported evidence path');const response=await fetcher(path(key),{cache:'no-store'});
  if(!response.ok)throw Error('Research artifact unavailable');const reader=response.body.getReader(),parts=[];let size=0;
  try{while(true){const row=await reader.read();if(row.done)break;size+=row.value.length;if(size>limit){await reader.cancel();throw Error('Research artifact exceeds size bound');}parts.push(row.value);}}
  finally{reader.releaseLock();}
  const out=new Uint8Array(size);let offset=0;for(const part of parts){out.set(part,offset);offset+=part.length;}return out;
 }
 async function sha(raw){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw))).map(b=>b.toString(16).padStart(2,'0')).join('');}
 async function loadSnapshot(id,fetcher=root.fetch.bind(root)){
  if(!/^[a-f0-9]{64}$/.test(id))throw Error('Invalid snapshot identifier');const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024);
  if(await sha(raw)!==id)throw Error('Run manifest hash differs');const m=JSON.parse(new TextDecoder().decode(raw)),ref=m.output;
  if(m.contract!=='cb-replay.v1'||!ref||ref.sha256!==m.output_sha256||ref.key!==PREFIX+'outputs/'+ref.sha256+'.json'||!Number.isInteger(ref.bytes))throw Error('Unsupported output identity');
  const body=await bytes(fetcher,ref.key);if(body.length!==ref.bytes||await sha(body)!==ref.sha256)throw Error('Snapshot output differs');
  const p=JSON.parse(new TextDecoder().decode(body));if(p.contract!==CONTRACT||p.generated_at!==m.generated_at)throw Error('Snapshot contract differs');
  p.replay={manifest_key:key,output_sha256:ref.sha256,compilers:m.compilers};return p;
 }
 const api={CONTRACT,PREFIX,esc,num,path,fresh,render,loadSnapshot,sha};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.CBResearch=api;
})(typeof globalThis==='object'?globalThis:this);
