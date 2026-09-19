(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.TicResearch=api;})(typeof window!=='undefined'?window:this,function(){
 'use strict';
 const CONTRACT='tic-original-research.v1',PREFIX='data/tic-research/';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=v=>(typeof v==='number'||typeof v==='string'&&v.trim()!=='')&&Number.isFinite(Number(v))?Number(v):null;
 const fmt=(v,d=3)=>num(v)===null?'—':Number(v).toLocaleString('en-US',{maximumFractionDigits:d});
 const signed=v=>num(v)===null?'—':(Number(v)>0?'+':'')+fmt(v);
 function boundary(p){if(p?.contract!==CONTRACT||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false||p.execution_eligible!==false||!p.measurements)throw Error('TIC research contract differs');return p;}
 function status(p,at=Date.now(),pinned=false){
  if(pinned)return 'historical snapshot';const q=p.quality||{};
  const acquired=Date.parse(q.original_archive_acquired_at),calendar=Date.parse(q.calendar_acquired_at),expiry=Date.parse(q.calendar_expires_at);
  if(!Number.isFinite(acquired)||!Number.isFinite(calendar)||at<acquired-300000||at-acquired>26*3600000||at-calendar>26*3600000)return 'source check expired';
  if(!Number.isFinite(expiry)||at>=expiry)return 'release calendar expired';
  return q.status==='fresh'?'within displayed source rules':'incomplete or conflicting evidence';
 }
 function table(headers,rows){return `<div class="table-scroll"><table><thead><tr>${headers.map(v=>`<th>${esc(v)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;}
 function summary(p,at,pinned){
  boundary(p);const state=status(p,at,pinned),show=pinned||state==='within displayed source rules',h=p.headline;
  const cards=[['Foreign purchases of U.S. LT securities · 12 months',h.foreign_net_into_us_lt_12mo_b],['Net LT securities transactions · 12 months',h.net_cross_border_lt_12mo_b],['Foreign purchases of U.S. LT securities · latest month',h.latest_month_b],['Foreign purchases of Treasury bills · 12 months',h.short_term_treasury_12mo_b]];
  return {state,cards:cards.map(([label,value])=>`<article class="metric"><div>${esc(label)}</div><strong>${show?signed(value):'—'}</strong><span>USD billions · ${esc(p.data_asof.slice(0,7))}${show?'':' · dated values remain below'}</span></article>`).join(''),
   assets:table(['Security type','Latest month · USD bn','Twelve months · USD bn','Evidence'],Object.entries(p.by_asset_class).map(([name,row])=>[esc(name.replaceAll('_',' ')),show?signed(row.latest_month_b):'—',show?signed(row.rolling_12mo_b):'—',`<button class="text-button" data-series="${esc(row.series_id)}">Inspect</button>`]))};
 }
 function detail(p,id){
  const v=p.measurements[id];if(!v)throw Error('TIC series unavailable');const checked=p.cross_source_checks[id],q=v.quality;
  const totals=v.current_vintage_totals;
  return `<h2>${esc(v.name)}</h2><p class="dim">Observation month ${esc(v.as_of.slice(0,7))} · ${esc(q.status)} in the selected snapshot. Current-vintage historical estimates; original publication-time knowledge is unverified.</p>`+
   table(['Reported item','Amount / definition'],[['Latest monthly net purchases · USD millions',fmt(v.value_decimal,0)],['Latest monthly net purchases · USD billions',signed(v.value_usd_bn_decimal)],['Three complete calendar months · USD bn',fmt(totals['3'].usd_bn_decimal)],['Twelve complete calendar months · USD bn',fmt(totals['12'].usd_bn_decimal)],['Reported history',`${esc(v.coverage.first_observation)} → ${esc(v.coverage.last_observation)} · ${fmt(v.coverage.observations,0)} months`],['FRED distribution check',esc(checked.status)],['Source series / member index',`${esc(v.native_source_id)} / ${esc(v.series_index)}`]])+
   `<p>Positive means net purchases by the named investor group; negative means net sales. U.S. investors’ purchases of foreign securities are the outflow leg. A bill purchase does not establish the investor’s motive.</p>`+
   `<p class="dim">Source acquired ${esc(v.original.acquired_at)}. Native unit ${esc(v.source_unit)}; monthly, not seasonally adjusted. Source integer millions convert to billions by dividing by 1,000.</p>`+
   `<details><summary>Definition and original evidence</summary><p>${esc(v.definition.notes||'Definition retained in the original archive.').replaceAll('####','</p><p>')}</p><p><a href="/${esc(v.original.evidence.key)}">Retained complete Treasury archive</a> · <a href="https://fred.stlouisfed.org/series/${encodeURIComponent(id)}" target="_blank" rel="noopener">FRED series definition</a></p><code>Archive SHA-256 ${esc(v.original.evidence.sha256)}</code></details>`;
 }
 function historyTable(doc,page=0){
  if(!Array.isArray(doc.rows))throw Error('TIC history rows missing');const rows=[...doc.rows].reverse(),size=24,start=Math.max(0,page)*size;
  if(doc.kind==='component_reconciliation')return {pages:Math.ceil(rows.length/size),count:rows.length,html:'<p class="dim">Total minus reported components, in USD millions. Integer-million reporting gives an arithmetic rounding bound of 2.5 million for four asset components and 1.5 million for two holder components. A larger gap is unreconciled.</p>'+table(['Month','Asset sum check','Asset gap · USD m','Holder sum check','Holder gap · USD m'],rows.slice(start,start+size).map(r=>[esc(r.date),esc(r.asset_classes.status),fmt(r.asset_classes.gap_usd_million_decimal),esc(r.official_private.status),fmt(r.official_private.gap_usd_million_decimal)]))};
  if(doc.kind==='rolling_twelve_months')return {pages:Math.ceil(rows.length/size),count:rows.length,html:table(['Ending month','Foreign → U.S. LT · USD bn','Net LT · USD bn','Complete source months'],rows.slice(start,start+size).map(r=>[esc(r.date),signed(r.rolling_12mo_b),signed(r.net_cross_border_lt_12mo_b),esc(r.totals.total.status)]))};
  return {pages:Math.ceil(rows.length/size),count:rows.length,html:table(['Observation month','Net purchases · USD millions','Net purchases · USD billions','Original array row'],rows.slice(start,start+size).map(r=>[esc(r.date),fmt(r.value_decimal,0),signed(num(r.value_decimal)===null?null:Number(r.value_decimal)/1000),esc(r.row_index)]))};
 }
 function historyTitle(p,id,kind){
  if(kind==='rolling_twelve_months')return 'Foreign purchases of U.S. LT securities · twelve-month totals';
  if(kind==='component_reconciliation')return 'Foreign purchases of U.S. LT securities · component reconciliation';
  const labels={total:'Foreign purchases of U.S. LT securities',treasuries:'Foreign purchases of U.S. Treasuries',agency_bonds:'Foreign purchases of U.S. agency bonds',corporate_bonds:'Foreign purchases of U.S. corporate bonds',equities:'Foreign purchases of U.S. equities',short_treasury:'Foreign purchases of Treasury bills',official:'Official purchases of U.S. LT securities',private:'Non-official purchases of U.S. LT securities',us_abroad:'U.S. purchases of foreign LT securities'};
  return (labels[p.measurements[id]?.role]||id)+(kind==='fred_comparison'?' · FRED comparison':' · monthly');
 }
 function chart(doc,range=120,width=1040){
  if(!Array.isArray(doc.rows))throw Error('TIC history rows missing');if(doc.kind==='component_reconciliation')return '<p>Reconciliation is shown in the exact table: arithmetic residuals are not flows or returns.</p>';
  let rows=doc.rows.map(r=>({date:r.date,value:doc.kind==='rolling_twelve_months'?num(r.rolling_12mo_b):num(r.value_decimal)===null?null:Number(r.value_decimal)/1000}));if(range)rows=rows.slice(-range);
  const valid=rows.filter(r=>r.value!==null);if(!valid.length)return '<p>No complete reported values in this range.</p>';
  const min=Math.min(0,...valid.map(r=>r.value)),max=Math.max(0,...valid.map(r=>r.value)),span=max-min||1,W=Math.max(320,Math.min(1040,Number(width)||1040)),H=240,left=72,right=14,top=20,bottom=35;
  const start=Date.parse(rows[0].date),end=Date.parse(rows.at(-1).date),x=d=>left+(Date.parse(d)-start)/(end-start||1)*(W-left-right),y=v=>top+(max-v)/span*(H-top-bottom);
  let paths=[],path='',prior=null;for(const r of rows){const d=new Date(r.date),month=d.getUTCFullYear()*12+d.getUTCMonth();if(r.value===null||prior!==null&&month-prior!==1){if(path)paths.push(path);path='';}if(r.value!==null)path+=(path?' L':'M')+x(r.date).toFixed(2)+' '+y(r.value).toFixed(2);prior=month;}if(path)paths.push(path);
  return `<svg role="img" aria-label="Dated transaction history in USD billions" viewBox="0 0 ${W} ${H}"><line x1="${left}" x2="${W-right}" y1="${y(0)}" y2="${y(0)}" stroke="var(--bd)"/>${[min,max].map(v=>`<text x="${left-8}" y="${y(v)+4}" text-anchor="end">${esc(fmt(v,1))}</text>`).join('')}${paths.map(d=>`<path d="${d}" stroke="var(--cyan)" stroke-width="2" fill="none"/>`).join('')}<text x="${left}" y="${H-5}">${esc(rows[0].date)}</text><text x="${W-right}" y="${H-5}" text-anchor="end">${esc(rows.at(-1).date)}</text></svg><p class="dim">USD billions · calendar axis. Missing months and values break the line. ${doc.kind==='rolling_twelve_months'?'Each point sums twelve calendar months.':'Each point is one monthly transaction amount.'}</p>`;
 }
 function scenario(x){
  for(const key of ['exposure','priceMove','income','costs','days'])if(typeof x[key]!=='number'||!Number.isFinite(x[key]))throw Error('Enter every scenario input.');
  if(x.exposure<=0||x.exposure>1e12||x.priceMove< -100||x.priceMove>1000||x.income<0||x.income>1e12||x.costs<0||x.costs>1e12||!Number.isInteger(x.days)||x.days<1||x.days>3650)throw Error('Scenario input is outside its displayed bounds.');
  const pricePnl=x.exposure*x.priceMove/100,net=pricePnl+x.income-x.costs;
  return {currency:'USD',inputs:{...x},price_pnl:pricePnl,cash_income:x.income,costs:x.costs,net_pnl:net,return_on_exposure_pct:100*net/x.exposure,
   break_even_price_move_pct:100*(x.costs-x.income)/x.exposure,formula:'exposure * entered price move / 100 + entered cash income - entered fees and funding costs',
   forecast:false,position_size:null,tic_flow_to_return_coefficient:null,note:'All price movement, cash income and costs are user-entered for the same horizon. TIC transactions supply context, not the assumed return.'};
 }
 async function verified(fetcher,ref){
  if(!ref||!new RegExp('^'+PREFIX+'(?:runs|outputs|histories)/[a-f0-9]{64}\\.json$').test(ref.key)||!Number.isInteger(ref.bytes)||ref.bytes<=0||ref.bytes>16*1024*1024||!/^[a-f0-9]{64}$/.test(ref.sha256))throw Error('TIC artifact identity differs');
  const response=await fetcher('/'+ref.key,{cache:'no-store'});if(!response.ok)throw Error('Retained TIC artifact unavailable');const raw=await response.arrayBuffer();
  const sha=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');
  if(raw.byteLength!==ref.bytes||sha!==ref.sha256)throw Error('Retained TIC artifact hash differs');return JSON.parse(new TextDecoder().decode(raw));
 }
 return {CONTRACT,PREFIX,esc,fmt,signed,num,boundary,status,summary,detail,historyTable,historyTitle,chart,scenario,verified};
});
