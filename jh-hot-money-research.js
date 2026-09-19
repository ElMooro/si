(function(root){
 'use strict';
 const CONTRACT='hot-money-research.v1',PREFIX='data/hot-money-research/';
 const esc=value=>String(value??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('en-US',{maximumFractionDigits:3}):'Unavailable';
 const path=key=>typeof key==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(key)&&!key.includes('..')?'/'+key:null;
 const link=(key,label)=>path(key)?`<a href="${path(key)}">${esc(label)}</a>`:esc(label+' unavailable');
 const recent=(date,now,hours=26)=>{const age=now-Date.parse(date);return Number.isFinite(age)&&age>=0&&age<=hours*3600000;};
 function fresh(board,p,now){const q=board.quality||{},day=Math.floor((now+8*3600000)/86400000),observed=Math.floor(Date.parse(q.observation_date)/86400000);
  return q.status==='fresh'&&recent(p.generated_at,now)&&recent(q.acquired_at,now)&&Number.isFinite(observed)&&day>=observed&&day-observed<=5;}
 const table=(headers,rows)=>`<div class="table-scroll"><table><thead><tr>${headers.map(v=>`<th>${esc(v)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>`<tr>${r.map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
 function chart(board){
  const rows=(board.history||[]).slice(-60).filter(r=>/^\d{8}$/.test(r.date)&&/^-?\d+$/.test(r.net_twd));
  if(rows.length<2)return '<p>At least two original-verified dates are needed for the trading tape.</p>';
  const date=d=>Date.parse(d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6)),first=date(rows[0].date),last=date(rows.at(-1).date);
  const values=rows.map(r=>Number(r.net_twd)/1e9),scale=Math.max(...values.map(Math.abs),1),x=d=>62+(date(d)-first)/Math.max(1,last-first)*610;
  const bars=rows.map((r,i)=>{const v=values[i],height=Math.abs(v)/scale*68,y=v>=0?86-height:86;
   return `<rect x="${(x(r.date)-2).toFixed(2)}" y="${y.toFixed(2)}" width="4" height="${height.toFixed(2)}" fill="${v>=0?'#22d3ee':'#fb7185'}"><title>${esc(r.date)}: ${esc(r.net_twd)} TWD</title></rect>`;}).join('');
  return `<figure class="exchange-chart"><figcaption>Last ${rows.length} verified reported dates · net purchases, TWD billions</figcaption><svg viewBox="0 0 720 196" role="img" aria-label="Dated exchange net purchases; missing calendar sessions are not zero-filled"><line x1="60" y1="86" x2="680" y2="86" stroke="currentColor" opacity=".3"/>${bars}`+
   `<g fill="currentColor" font-size="11"><text x="4" y="22">${num(scale)}</text><text x="40" y="90">0</text><text x="4" y="156">−${num(scale)}</text><text x="62" y="184">${esc(rows[0].date)}</text><text x="610" y="184">${esc(rows.at(-1).date)}</text></g></svg><p class="note">The horizontal axis uses calendar dates. Gaps are not zero observations; this chart does not certify the exchange calendar. Exact amounts and original rows appear below.</p></figure>`;
 }
 function evidence(row){if(!row)return 'No verified original';
  const categories=Object.values(row.categories||{}).map(r=>[esc(r.label),esc(r.original_row_index),esc(r.buy_twd),esc(r.sell_twd),esc(r.net_twd)]);
  return `<details><summary>Original rows and exact TWD</summary><p>${esc(row.date)} · acquired ${esc(row.acquired_at)}<br>${link(row.original?.key,'Original exchange response')}</p>`+
   table(['Category','Original row','Buy, TWD','Sell, TWD','Net, TWD'],categories)+
   `<p>Net purchases: ${esc(row.net_twd)} TWD. Display conversion: divide by 1,000,000,000.</p><p>Definition: ${esc(row.definition?.unit_source)} · ${esc(row.definition?.category_rule)}</p>`+
   `<details><summary>Exchange scope and notes</summary><ul>${(row.notes||[]).map(n=>`<li>${esc(n)}</li>`).join('')}</ul><p>Requested: ${esc(row.request?.url)}</p></details>`+
   `<p>${(row.prior_captures||[]).length} earlier captured versions. Newly downloaded historical rows do not establish their original publication-time availability.</p>`+
   (row.legacy_comparison?.status==='differs'?`<p>Original differs from the preserved unverified legacy value ${esc(row.legacy_comparison.legacy_value_twd)} TWD by ${esc(row.legacy_comparison.difference_twd)} TWD. This comparison does not qualify the old ledger as source evidence.</p>`:'')+
   (row.prior_captures||[]).map(r=>`<p>${esc(r.acquired_at)} · ${esc(r.net_twd)} TWD · ${link(r.original?.key,'Prior original')}</p>`).join('')+'</details>';
 }
 function boardPanel(name,b,p,now,pinned){const usable=b.status==='LIVE'&&(pinned||fresh(b,p,now));
  const cards=[['Latest net purchases',num(usable?b.latest_bn:null),b.latest_day||'No verified date'],...[5,20,60].map(n=>{
   const w=b.windows?.[n]||{};return [n+' reported observations',num(usable?w.sum_bn:null),`${w.start||'—'} → ${w.end||'—'} · ${w.observations??0} verified dates`];})];
  return `<section class="sec"><h2>${esc(name)}</h2><p>${esc(b.scope)}. Unit: TWD billions.</p><div class="cards">`+
   cards.map(([label,value,note])=>`<article class="card"><div class="k">${esc(label)}</div><div class="v">${value}</div><p class="s">${esc(note)}</p></article>`).join('')+'</div>'+
   `<p>${esc(b.quality?.status)} as measured · ${b.verified_observations??0} original-verified dates · ${b.legacy_unverified_observations??0} legacy dates awaiting originals.</p>`+
   `<p>Observation ${esc(b.quality?.observation_date)} · acquired ${esc(b.quality?.acquired_at)}. No exchange-calendar completeness claim.</p>${evidence(b.latest)}`+
   `<details><summary>Window evidence and descriptive z-score</summary><p>z against 60 earlier reported observations: ${num(usable?b.z_60_observations:null)}. Sample standard deviation; current date excluded; no clipping or return-forecast claim.</p>`+
   table(['Window','Status','Known dates lacking originals','Exact net, TWD'],[5,20,60].map(n=>{const w=b.windows?.[n]||{};return [esc(n),esc(w.status),esc((w.known_unverified_dates||[]).join(', ')||'none known'),esc(w.sum_twd)];}))+
   `<pre>${esc(JSON.stringify({windows:b.windows,z_definition:b.z_definition},null,2))}</pre></details></section>`;
 }
 function render(p,now=Date.now(),pinned=false,selected='twse'){
  if(p.contract!==CONTRACT||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false||p.execution_eligible!==false)throw Error('Descriptive exchange research contract required');
  if(!['twse','tpex'].includes(selected))throw Error('Unknown exchange board');
  const tw=p.countries?.taiwan||{},otc=tw.otc||{},both=[tw,otc].every(b=>b.status==='LIVE'&&(pinned||fresh(b,p,now))),c=tw.combined||{};
  const chosen=selected==='twse'?tw:otc,id=p.replay?.manifest_key?.match(/^data\/hot-money-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];
  const history=[...(chosen.history||[])].reverse().map(r=>[esc(r.date),esc(r.buy_twd),esc(r.sell_twd),esc(r.net_twd),evidence(r)]);
  const f=p.pd_settlement_fails||{};
  const fails=[f,f.ust_ex_tips||{}].map(r=>{const good=r.quality?.status==='fresh'&&r.reconciliation?.consistent===true&&(pinned||(recent(p.generated_at,now)&&recent(f.source_generated_at,now)));
   return [esc(r.label||r.scope_id),esc(r.as_of),num(good?r.ftd_bn:null),num(good?r.ftr_bn:null),num(good?r.combined_bn:null),esc(r.quality?.status)];});
  return {hero:`<p class="research-label">SOURCE-BACKED EXCHANGE RESEARCH · CALLS: ABSTAIN</p><h2>${pinned?'Pinned historical snapshot':both?'Both exchange boards current':'Exchange coverage degraded or unavailable'}</h2>`+
    `<p>Foreign net purchases are exchange trades. They do not measure cross-border cash settlement, new investment capital or expected returns.</p><p>${esc(p.decision?.reason)}</p>`,
   countries:boardPanel('TWSE · listed board',tw,p,now,pinned)+boardPanel('TPEx · OTC mainboard stocks',otc,p,now,pinned),
   combined:`<p>Same-date sum: <strong>${num(both&&c.status==='LIVE'?c.latest_bn:null)} TWD billions</strong> · ${esc(c.latest_day)}.</p><p>${esc(c.scope)}.</p>`+
    table(['Reported observations','Status','Combined, TWD bn'],[5,20,60].map(n=>[esc(n),esc(c.windows?.[n]?.status),num(both?c.windows?.[n]?.sum_bn:null)]))+
    '<p>Combined windows require at least 60 verified dates on each board and exactly matching window dates. They do not certify every scheduled session.</p>',
   history:chart(chosen)+table(['Observed date','Buy, exact TWD','Sell, exact TWD','Net, exact TWD','Evidence'],history),
   fails:'<p>Separate U.S. settlement context. Original FR2004 responses have not been verified here; these retained engine values never enter Taiwan trading totals. A sum that fails the stated 0.02 USD billion reconciliation tolerance is withheld.</p>'+table(['Scope','Observation','FTD, USD bn','FTR, USD bn','Two-sided gross, USD bn','Status'],fails),
   evidence:link(p.replay?.manifest_key,'Immutable run manifest')+(id?` · <a href="/hot-money.html?run=${id}">Permanent snapshot link</a>`:'')+
    `<details><summary>Source copies, acquisition errors and retained legacy evidence</summary><pre>${esc(JSON.stringify({source_comparisons:p.source_comparisons,acquisition_errors:p.acquisition_errors,backfill_errors:p.backfill_errors},null,2))}</pre>`+
    Object.entries(p.legacy_context||{}).map(([key,ref])=>`<p>${esc(key)} · ${link(ref.key,'Preserved unverified legacy')}</p>`).join('')+'</details>',
   sub:`Published ${p.generated_at} · ledger captured ${p.source_generated_at} · ${p.methodology_version}${pinned?' · values as published':''}`};
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
  if(m.contract!=='hot-money-replay.v1'||!ref||ref.sha256!==m.output_sha256||ref.key!==PREFIX+'outputs/'+ref.sha256+'.json'||!Number.isInteger(ref.bytes))throw Error('Unsupported output identity');
  const body=await bytes(fetcher,ref.key);if(body.length!==ref.bytes||await sha(body)!==ref.sha256)throw Error('Snapshot output differs');
  const p=JSON.parse(new TextDecoder().decode(body));if(p.contract!==CONTRACT||p.generated_at!==m.generated_at)throw Error('Snapshot contract differs');
  p.replay={manifest_key:key,output_sha256:ref.sha256,compilers:m.compilers};return p;
 }
 const api={CONTRACT,PREFIX,esc,num,path,fresh,render,loadSnapshot,sha};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.HotMoneyResearch=api;
})(typeof globalThis==='object'?globalThis:this);
