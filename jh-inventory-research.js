/* Inventory observations, with explicit legacy provenance and no shortage inference. */
(function(root){
 'use strict';
 const V=typeof module!=='undefined'&&module.exports?require('./jh-table-values.js'):root.JHTableValues;
 const CONTRACT='inventory-observation-measurements.v1';
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x),text=x=>typeof x==='string'?x:'';
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function records(packet){
  const sectors=[],stocks=[],issues=[],measured=packet?.measurement_contract===CONTRACT;
  function rows(key,append){if(!Array.isArray(packet?.[key])){issues.push(key+' unavailable');return;}packet[key].forEach((r,i)=>{
   if(!object(r)){issues.push(key+'['+i+'] malformed; retained in original');return;}append(r,key+'['+i+']');
  });}
  rows('sector_drawdown',(r,ref)=>sectors.push({series:text(r.series),sector:text(r.sector),theme:text(r.theme_etf),ratio:r.latest_ratio,
   c3:measured?r.ratio_changes_pct?.['3m']?.value:r.chg_3m,c6:measured?r.ratio_changes_pct?.['6m']?.value:r.chg_6m,
   c12:measured?r.ratio_changes_pct?.['12m']?.value:r.chg_12m,percentile:r.percentile_5y,
   date:text(r.as_of),age:r.observation_age_days,sample:r.percentile_sample_n,
   status:measured?text(r.status):'Legacy values: dates and comparisons unverified',ref}));
  rows('stock_drawdown_board',(r,ref)=>stocks.push({ticker:text(r.ticker),industry:text(r.industry),dio:r.dio_latest,
   prior:r.dio_4q_ago,change:r.dio_chg_pct,delta:r.dio_change_days,rps:measured?r.revenue_per_share_change_pct:null,
   date:text(r.as_of),age:r.observation_age_days,prior_date:text(r.comparison?.prior_date),
   status:measured?text(r.status):'Legacy values: dates and comparisons unverified',
   comparison:measured?text(r.comparison?.status):'Unavailable',ref}));
  return {sectors,stocks,issues,measured};
 }
 const SECTORS=[['sector','Sector','text'],['series','FRED series','text'],['ratio','Reported I/S ratio','number'],['c3','3m change %','percent'],['c6','6m change %','percent'],['c12','12m change %','percent'],['percentile','5y percentile','number'],['sample','Monthly sample','number'],['date','Observation month','text'],['age','Age days','number'],['status','Measurement status','text'],['ref','Source record','text']];
 const STOCKS=[['ticker','Ticker','text'],['industry','Source industry','text'],['dio','Reported DIO days','number'],['prior','Prior DIO days','number'],['change','Annual change %','percent'],['delta','Change days','number'],['rps','RPS change %, unadjusted','percent'],['date','Observation end','text'],['prior_date','Annual prior end','text'],['age','Age days','number'],['status','Measurement status','text'],['comparison','Calendar comparison','text'],['ref','Source record','text']];
 function format(v,type){return type==='text'?text(v):type==='percent'?V.percent(v,2):V.format(v,2);}
 function cell(row,key,type){const value=format(row[key],type);if(key==='ticker'&&value)return '<a href="/ticker.html?symbol='+encodeURIComponent(value)+'">'+esc(value)+'</a>';return esc(value);}
 function paint(doc,id,columns,rows,key,direction,onSort){
  const kind=columns.find(c=>c[0]===key)[2];const sorted=rows.slice().sort((a,b)=>V.compare(a,b,key,direction,kind==='text'?'text':'number'));
  doc.getElementById(id).innerHTML='<table><thead><tr>'+columns.map(([k,label])=>'<th data-k="'+k+'">'+esc(label)+'</th>').join('')+'</tr></thead><tbody>'+sorted.map(r=>'<tr>'+columns.map(([k,,t])=>'<td>'+cell(r,k,t)+'</td>').join('')+'</tr>').join('')+'</tbody></table>';
  V.bindSort(doc.querySelectorAll('#'+id+' th[data-k]'),key,direction,onSort);
 }
 async function start(){
  const doc=root.document;let packet;
  try{const source=await V.load('/data/inventory-drawdown.json');packet=source.packet;doc.getElementById('original').textContent=source.raw;}
  catch(e){doc.getElementById('original').textContent=typeof e.original_text==='string'?e.original_text:'Unavailable: '+e.message;doc.getElementById('status').textContent='Source unavailable. No fallback or new calculation was requested.';return;}
  const data=records(packet),plan=packet.universe_plan,filter=doc.getElementById('q'),sorts={sectors:['sector',1],stocks:['ticker',1]};
  doc.getElementById('status').textContent=(data.measured?'Dated observation research':'Legacy publication; calendar comparisons unverified')+' · generated '+(text(packet.generated_at)||'unavailable')+' · '+data.sectors.length+' sector records · '+data.stocks.length+' company records. Forecast and position-sizing authority unavailable.';
  doc.getElementById('coverage').textContent=object(plan)?'Requested '+V.count(plan.requested?.length)+' names; '+V.count(plan.not_attempted?.length)+' outside the existing 130-name cap. Full universe and acquisition details are in the original.':'The legacy publication contains only its selected board. Full acquisition population and source periods were not published.';
  function render(id,columns,rows){const[key,dir]=sorts[id];paint(doc,id,columns,rows,key,dir,k=>{sorts[id]=[k,k===key?-dir:columns.find(c=>c[0]===k)[2]==='text'?1:-1];refresh();});}
  function refresh(){const q=filter.value.trim().toUpperCase(),rows=data.stocks.filter(r=>[r.ticker,r.industry,r.ref].join(' ').toUpperCase().includes(q));render('sectors',SECTORS,data.sectors);render('stocks',STOCKS,rows);doc.getElementById('rows').textContent=rows.length+' of '+data.stocks.length+' company records. '+data.issues.join('; ');}
  filter.oninput=refresh;refresh();
 }
 const api={records,format,esc,start};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHInventoryResearch=api;
})(typeof globalThis!=='undefined'?globalThis:this);
