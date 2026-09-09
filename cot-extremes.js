/* Dedicated COT output and every returned calculation-history row. */
(function(global){
'use strict';
const DAY=86400000,ENGINE='justhodl-cot-extremes-scanner';
const esc=value=>String(value??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const finite=value=>typeof value==='number'&&Number.isFinite(value);
function fresh(data,now=Date.now()){
 const stamp=data&&data.generated_at;
 const time=typeof stamp==='string'&&/(Z|[+-]\d\d:\d\d)$/.test(stamp)?Date.parse(stamp):NaN;
 return data&&data.engine===ENGINE&&data.schema_version==='cot-extremes.v2'&&data.execution_eligible===false&&Number.isFinite(time)&&now-time>=-300000&&now-time<=8*DAY;
}
function rankAvailable(data,row,now=Date.now()){
 const stamp=typeof row.report_date==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(row.report_date)?Date.parse(row.report_date+'T00:00:00Z'):NaN;
 const today=Math.floor(now/DAY)*DAY;
 return fresh(data,now)&&row.status==='ok'&&row.execution_eligible===false&&finite(row.percentile)&&row.percentile>=0&&row.percentile<=100&&Number.isFinite(stamp)&&new Date(stamp).toISOString().slice(0,10)===row.report_date&&today-stamp>=0&&today-stamp<=10*DAY;
}
function rowOf(data,row,now){
 const ranked=rankAvailable(data,row,now),pct=ranked?row.percentile:null;
 const state=ranked?'ok':row.status==='ok'?'stale_or_unverified':row.status;
 const fmt=value=>finite(value)?value.toLocaleString():'—';
 return '<tr><td><strong>'+esc(row.contract)+'</strong></td><td>'+esc(row.name)+'</td><td>'+esc(row.category)+'</td>'+
  '<td>'+(pct===null?'—':pct.toFixed(1)+'%')+'</td><td class="right">'+fmt(row.spec_net)+'</td><td class="right">'+fmt(row.open_int)+'</td>'+
  '<td class="right">'+(ranked&&finite(row.trend_4w)?row.trend_4w.toFixed(5):'—')+'</td><td>'+esc(row.report_date)+'</td><td>'+esc(state)+'</td></tr>';
}
const api={esc,fresh,rankAvailable,rowOf};
if(typeof module!=='undefined'&&module.exports)module.exports=api;
if(typeof document==='undefined')return;
let data=null,filter='all',selection=null,availability='';
const availabilityKey=()=>data?JSON.stringify([fresh(data),...(data.contracts||[]).map(row=>rankAvailable(data,row))]):'';
const content=document.getElementById('content');
function showHistory(){
 const target=document.getElementById('cot-history-data');if(!target)return;
 target.replaceChildren();
 const value=data&&data.histories&&data.histories[selection];
 if(!value){target.textContent='No calculation history returned for this contract.';return;}
 if(global.JHDataInspector)global.JHDataInspector.inspect(target,value,'Complete history returned by '+ENGINE+' for '+selection+'. Current provider vintage; original publication times are not certified.');
 else {const pre=document.createElement('pre');pre.style.whiteSpace='pre-wrap';pre.textContent=JSON.stringify(value,null,2);target.append(pre);}
}
function render(){
 if(!data)return;
 availability=availabilityKey();
 const s=data.summary||{},rows=Array.isArray(data.contracts)?data.contracts:[],now=Date.now();
 const categories=[...new Set(rows.map(row=>row.category).filter(value=>typeof value==='string'))].sort();
 const valid=rows.filter(row=>rankAvailable(data,row,now));
 const visible=rows.filter(row=>filter==='all'||filter==='unavailable'&&!rankAvailable(data,row,now)||filter==='extreme'&&rankAvailable(data,row,now)&&row.extreme||row.category===filter);
 content.innerHTML='<p>Descriptive positioning percentiles. These are not calibrated turning-point probabilities or trading permissions.</p>'+
  (!fresh(data,now)?'<p class="error">This publication is stale or its producer schema is unverified. Ranks are withheld; returned data remains inspectable.</p>':'')+
  '<div class="hero"><div class="stat-card"><div class="stat-label">Contracts returned / requested</div><div class="stat-value">'+rows.length+' / '+esc(s.n_requested)+'</div><div class="stat-sub">Universe: '+esc(data.universe_status)+'</div></div>'+
  '<div class="stat-card"><div class="stat-label">Valid current ranks</div><div class="stat-value">'+valid.length+'</div><div class="stat-sub">'+(rows.length-valid.length)+' withheld</div></div>'+
  '<div class="stat-card"><div class="stat-label">Positioning extremes</div><div class="stat-value">'+valid.filter(row=>row.extreme).length+'</div><div class="stat-sub">≥95th or ≤5th percentile</div></div>'+
  '<div class="stat-card"><div class="stat-label">Generated</div><div class="stat-value" style="font-size:13px">'+esc(data.generated_at)+'</div><div class="stat-sub">Weekly observations; report dates below</div></div></div>'+
  '<div class="section"><div class="section-header"><h3>Contract positioning and availability</h3></div><div id="cot-filters" class="filters"></div><div style="overflow:auto"><table><thead><tr>'+['Contract','Name','Category','Percentile','Spec net','Open interest','Change over 4 weeks','Report date','Status'].map(label=>'<th>'+label+'</th>').join('')+'</tr></thead><tbody>'+visible.map(row=>rowOf(data,row,now)).join('')+'</tbody></table></div></div>'+
  '<div class="section"><div class="section-header"><h3>Complete calculation histories</h3></div><p style="padding:0 18px">'+esc(data.history_coverage)+'</p><label style="padding:18px">Contract <select id="cot-history-select" aria-label="Contract history"></select></label><div id="cot-history-data" class="jdi-panel"></div></div>';
 const filters=document.getElementById('cot-filters');
 for(const name of ['all','extreme','unavailable',...categories]){
  const button=document.createElement('button');button.type='button';button.className='filter-btn'+(name===filter?' on':'');button.textContent=name;
  button.onclick=()=>{filter=name;render();};filters.append(button);
 }
 const select=document.getElementById('cot-history-select'),histories=data.histories&&typeof data.histories==='object'?data.histories:{};
 const names=Object.keys(histories).sort();if(!names.includes(selection))selection=names[0]||null;
 for(const name of names){const option=document.createElement('option');option.value=name;option.textContent=name;option.selected=name===selection;select.append(option);}
 select.disabled=!names.length;select.onchange=()=>{selection=select.value;showHistory();};showHistory();
}
async function load(){
 try{
  const response=await fetch('https://justhodl-data-proxy.raafouis.workers.dev/cot/extremes/current.json?exact=1&nogen=1',{cache:'no-store',credentials:'omit'});
  if(!response.ok)throw new Error('unavailable');
  if(response.headers.get('X-JH-Artifact-Key')!=='cot/extremes/current.json')throw new Error('unverified route');
  const result=await response.json();
  if(!result||result.engine!==ENGINE||result.schema_version!=='cot-extremes.v2'||!Array.isArray(result.contracts))throw new Error('invalid');
  data=result;render();
 }catch{data=null;content.replaceChildren();const message=document.createElement('p');message.className='error';message.textContent='COT publication unavailable or awaiting the v2 producer. No previous ranks are presented as current.';content.append(message);}
}
load();setInterval(load,5*60*1000);
function expire(){if(data&&availabilityKey()!==availability)render();}
setInterval(expire,30000);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)expire();});
})(typeof globalThis!=='undefined'?globalThis:this);
