/* Source-separated materials research. No cross-period issuer or ETF trade signal. */
(function(root){
 'use strict';
 const V=typeof module!=='undefined'&&module.exports?require('./jh-table-values.js'):root.JHTableValues;
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 const text=x=>typeof x==='string'?x:'';
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function day(x){if(typeof x!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(x))return '';const n=Date.parse(x+'T00:00:00Z');return Number.isFinite(n)&&new Date(n).toISOString().slice(0,10)===x?x:'';}
 function nextMonth(x){if(!day(x)||!x.endsWith('-01'))return '';const year=Number(x.slice(0,4)),month=Number(x.slice(5,7));return String(year+(month===12?1:0)).padStart(4,'0')+'-'+String(month===12?1:month+1).padStart(2,'0')+'-01';}
 function official(packet,today){
  const n=object(packet?.NEWORDER)?packet.NEWORDER:{},date=day(n.as_of),previous=day(n.prev_date);
  const valid=n.source?.series_id==='NEWORDER'&&n.series==='NEWORDER'&&n.unit==='Millions of U.S. dollars'&&n.frequency==='M'&&n.seasonal_adjustment==='SA'&&n.data_unavailable===false&&date&&date<=today&&(!n.observation_period||n.observation_period===date);
  const value=valid?V.number(n.value):null,base=valid?V.number(n.prev):null;
  const pair=Boolean(valid&&date.endsWith('-01')&&nextMonth(previous)===date&&value!==null&&value>=0&&base!==null&&base>0);
  const growth=pair?V.number(100*(value/base-1)):null;
  const definitions=[['AMTMNO','Total manufacturing new orders','USD millions'],['AMTMUO','Total manufacturing unfilled orders','USD millions'],['UMTMTI','Manufacturing inventories','USD millions'],['DGORDER','Durable-goods new orders','USD millions'],['NEWORDER','Nondefense capital-goods new orders excluding aircraft','USD millions'],['ISM_NO_INV','ISM new orders minus inventories','index points']];
  return definitions.map(([id,label,unit])=>({id,label,unit,level:id==='NEWORDER'?value:null,mom:id==='NEWORDER'?growth:null,asof:id==='NEWORDER'?date:'',previous:id==='NEWORDER'?previous:'',status:id==='NEWORDER'?(valid?(pair?'Adjacent reported monthly pair; original not replayed':'Reported level; adjacent monthly pair unavailable'):'Definition or source observation unavailable'):'Not supplied by these publications'}));
 }
 function records(packets){
  const sectors=[],firms=[],issues=[];
  function array(packet,key,source,append){
   if(!object(packet)||!Array.isArray(packet[key])){issues.push(source+' '+key+' missing or malformed');return;}
   packet[key].forEach((row,i)=>{const ref=source+':'+key+'['+i+']';if(object(row))append(row,ref,packet);else issues.push(ref+' malformed; retained in original');});
  }
  array(packets.inventory,'sector_drawdown','inventory-drawdown',(r,ref,p)=>sectors.push({...r,source_record:ref,observation_date:day(r.as_of||r.asof),publication_time:text(p.generated_at)}));
  array(packets.inventory,'stock_drawdown_board','inventory-drawdown',(r,ref,p)=>firms.push({ticker:text(r.ticker),sector:text(r.sector),source:'Inventory / DIO',source_record:ref,
    dio:r.dio_latest,dio_change:r.dio_chg_pct,revenue_change:r.rev_growth_yoy,observation_date:day(r.asof||r.as_of),publication_time:text(p.generated_at),status:'Source tags; period, units and issuer join unverified',original:r}));
  const book=packets.backlog;
  if(!object(book)||!object(book.by_ticker)&&!Array.isArray(book.by_ticker))issues.push('backlog by_ticker missing or malformed');
  else for(const [key,r] of Object.entries(book.by_ticker)){
   const ref='backlog:by_ticker['+JSON.stringify(key)+']';
   if(!object(r)){issues.push(ref+' malformed; retained in original');continue;}
   firms.push({ticker:text(r.ticker),sector:text(r.sector),source:'Backlog / RPO',source_record:ref,rpo:r.rpo,rpo_change:r.rpo_yoy,
      observation_date:day(r.rpo_asof),publication_time:text(book.generated_at),status:'Source record; issuer/currency/period join unverified',original:r});
  }
  array(packets.capex,'rows','capex-pulse',(r,ref,p)=>firms.push({ticker:text(r.ticker),sector:text(r.sector),source:'Capex',source_record:ref,
    capex_usd_b:r.capex_ttm_b,capex_change:r.yoy_pct,local_amount:r.reported_window_amount,currency:text(r.reported_currency),
    window_start:day(r.current_window?.start_date),observation_date:day(r.current_window?.end_date||r.asof),publication_time:text(p.generated_at),
    status:text(r.current_window?.status)||'Legacy amount/currency/window unverified',original:r}));
  return {sectors,firms,issues};
 }
 const SECTORS=[['sector','Sector','text'],['theme_etf','Source theme tag','text'],['series','FRED series','text'],['latest_ratio','Source I/S ratio','number'],['chg_3m','Source 3m %','percent'],['chg_6m','Source 6m %','percent'],['chg_12m','Source 12m %','percent'],['observation_date','Observation date','text'],['publication_time','Packet generated','text'],['flag','Source interpretation, unverified','text'],['source_record','Source record','text']];
 const ORDERS=[['id','Identity','text'],['label','Definition','text'],['unit','Defined unit','text'],['level','Reported level','number'],['mom','Calculated MoM %','percent'],['asof','Observation date','text'],['previous','Prior date','text'],['status','Evidence status','text']];
 const FIRMS=[['ticker','Ticker','text'],['sector','Sector','text'],['source','Source','text'],['dio','Source DIO','number'],['dio_change','Source DIO change %','percent'],['revenue_change','Source revenue growth %','percent'],['rpo','Source RPO amount','number'],['rpo_change','Source RPO growth %','percent'],['capex_usd_b','Source USD capex $B','number'],['capex_change','Source capex growth %','percent'],['local_amount','Reported local amount','number'],['currency','Reported currency','text'],['window_start','Window start','text'],['observation_date','Period / observation end','text'],['publication_time','Packet generated','text'],['po','Purchase obligation','number'],['status','Measurement status','text'],['source_record','Source record','text']];
 function formatted(value,kind){if(kind==='text')return text(value);if(kind==='percent')return V.percent(value,2);const n=V.number(value);return n===null?'':n.toLocaleString('en-US',{maximumFractionDigits:3});}
 function cell(row,key,kind){const value=formatted(row[key],kind);return key==='ticker'&&value?'<a href="/ticker.html?symbol='+encodeURIComponent(value)+'">'+esc(value)+'</a>':esc(value);}
 function paint(doc,id,columns,rows,key,direction,onSort){
  const kind=columns.find(c=>c[0]===key)[2];
  const sorted=rows.slice().sort((a,b)=>V.compare(a,b,key,direction,kind==='text'?'text':'number'));
  doc.getElementById(id).innerHTML='<table><thead><tr>'+columns.map(([k,label])=>'<th data-k="'+k+'">'+esc(label)+(k===key?(direction>0?' ↑':' ↓'):'')+'</th>').join('')+'</tr></thead><tbody>'+sorted.map(r=>'<tr>'+columns.map(([k,,type])=>'<td>'+cell(r,k,type)+'</td>').join('')+'</tr>').join('')+'</tbody></table>';
  V.bindSort(doc.querySelectorAll('#'+id+' th[data-k]'),key,direction,onSort);
 }
 async function start(){
  const doc=root.document,packets={},paths={inventory:'/data/inventory-drawdown.json',backlog:'/data/backlog.json',capex:'/data/capex-pulse.json',macro:'/data/canary-macro.json'};
  const states=await Promise.all(Object.entries(paths).map(async([name,path])=>{
   try{const received=await V.load(path);packets[name]=received.packet;doc.getElementById('original-'+name).textContent=received.raw;return path+' generated '+(text(received.packet.generated_at)||'not supplied');}
   catch(error){doc.getElementById('original-'+name).textContent=typeof error.original_text==='string'?error.original_text:'Unavailable: '+error.message;return path+' unavailable';}
  }));
  const data=records(packets),orders=official(packets.macro,new Date().toISOString().slice(0,10));
  doc.getElementById('kpis').textContent=data.sectors.length+' sector records · '+data.firms.length+' company source occurrences · '+data.issues.length+' source issues. '+states.join(' · ');
  doc.getElementById('secStatus').textContent='Every received sector row is shown. An I/S ratio can fall when sales rise; the ratio alone cannot prove physical inventory depletion. Source theme tags are not verified ETF exposures.';
  doc.getElementById('m3Status').textContent='NEWORDER requires its explicit monthly, seasonally adjusted USD definition. MoM also requires adjacent observation months and a positive prior value. Other identities are unavailable in these packets.';
  const filter=doc.getElementById('q'),sorts={secBoard:['sector',1],m3Board:['id',1],firmBoard:['ticker',1]};
  function render(id,columns,rows){const [key,dir]=sorts[id];paint(doc,id,columns,rows,key,dir,k=>{sorts[id]=[k,k===key?-dir:columns.find(c=>c[0]===k)[2]==='text'?1:-1];refresh();});}
  function refresh(){
   const query=filter.value.trim().toUpperCase(),rows=data.firms.filter(r=>!query||[r.ticker,r.sector,r.source,r.source_record].join(' ').toUpperCase().includes(query));
   render('secBoard',SECTORS,data.sectors);render('m3Board',ORDERS,orders);render('firmBoard',FIRMS,rows);
   doc.getElementById('firmStatus').textContent=rows.length+' of '+data.firms.length+' source occurrences. Repeated tickers retain separate dates and sources; no cross-source issuer/period join is asserted. Purchase obligations are unavailable. '+data.issues.join('; ');
  }
  filter.oninput=refresh;refresh();
 }
 const api={official,records,day,nextMonth,formatted,esc,start};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHMaterialsOrders=api;
})(typeof globalThis!=='undefined'?globalThis:this);
