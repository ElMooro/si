/* Source-labelled accounting tables. No calendar, currency or model qualification. */
(function(root){
 'use strict';
 const V=typeof module!=='undefined'&&module.exports?require('./jh-table-values.js'):root.JHTableValues;
 const BAGS=['dilution_offset_warnings','net_shrinkers','fresh_authorizations','high_shareholder_yield','cheap_repurchasers','high_conviction_pumps'];
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 const text=x=>typeof x==='string'?x:'';
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function amount(value){const n=V.number(value);if(n===null)return '';const a=Math.abs(n);return a>=1e12?(n/1e12).toFixed(2)+'T':a>=1e9?(n/1e9).toFixed(2)+'B':a>=1e6?(n/1e6).toFixed(1)+'M':n.toLocaleString('en-US',{maximumFractionDigits:2});}
 function flag(value){return value===true?'Yes':value===false?'No':'';}
 function date(value){if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value))return '';const n=Date.parse(value+'T00:00:00Z');return Number.isFinite(n)&&new Date(n).toISOString().slice(0,10)===value?value:'';}
 function population(packet,mode){
  if(!object(packet))throw Error('Publication object required');
  const rows=[],issues=[];let malformed=0;
  function add(row,source){if(object(row))rows.push({record:row,source});else{malformed++;issues.push(source+' is a malformed record');}}
  if(mode==='deferred'){
   if(!object(packet.by_ticker))throw Error('Received by_ticker population is missing or malformed');
   for(const [key,row] of Object.entries(packet.by_ticker))add(row,'by_ticker['+JSON.stringify(key)+']');
  }else if(mode==='buyback'){
   if(!object(packet.tickers))throw Error('Received ticker population is missing or malformed');
   for(const [key,row] of Object.entries(packet.tickers))add(row,'tickers['+JSON.stringify(key)+']');
  }else if(mode==='dilution'){
   if(object(packet.tickers))for(const [key,row] of Object.entries(packet.tickers))add(row,'tickers['+JSON.stringify(key)+']');
   for(const name of BAGS){
    if(!Object.prototype.hasOwnProperty.call(packet,name)){issues.push(name+' was not supplied');continue;}
    if(!Array.isArray(packet[name])){issues.push(name+' is a malformed population');continue;}
    packet[name].forEach((row,index)=>add(row,name+'['+index+']'));
   }
  }else throw Error('Unreviewed statement desk');
  return{rows,issues,malformed};
 }
 const COLUMNS={
  buyback:[['symbol','Ticker','text'],['company_name','Name','text'],['gross_repurchases_ttm','Source gross amount¹','number'],
   ['net_buyback_ttm','Source net common amount¹','number'],['window_unit','Window currency','text'],['window_start','Window start','date'],['window_end','Window end','date'],
   ['share_count_reduction_yoy','Source share reduction %²','number'],['shares_now','Reported shares','number'],['measurement_status','Measurement status','text'],['source','Source record','text']],
  deferred:[['ticker','Ticker','text'],['source','Source record','text'],['sector','Sector','text'],['deferred_rev','Deferred amount¹','number'],
   ['deferred_yoy','Source “YoY” tag %²','number'],['deferred_qoq','Source “QoQ” tag %²','number'],['rev_yoy','Source revenue growth %²','number'],
   ['deferred_asof','Period end','date'],['deferred_filed','Filed','date'],['deferred_accelerating','Source acceleration','boolean']],
  dilution:[['symbol','Ticker','text'],['company_name','Name','text'],['issuance_ttm','Issuance: source TTM¹','number'],['sbc_ttm','SBC: source TTM¹','number'],
   ['net_buyback_ttm','Net buyback: source TTM¹','number'],['share_count_reduction_yoy','Share reduction tag %²','number'],['shares_now','Source share count','number'],
   ['class','Source class','text'],['net_issuer','Source net-issuer flag','boolean'],['source','Source record','text']]
 };
 function value(row,key){
  if(key==='source')return row.source;
  const window=row.record.measurements?.cashflow_window;
  if(key==='window_unit')return window?.unit;
  if(key==='window_start')return window?.start_date;
  if(key==='window_end')return window?.end_date;
  if(key==='measurement_status')return window?.status||'Legacy measurement unverified';
  return row.record[key];
 }
 function cell(row,key,kind){
  const v=value(row,key);
  if(kind==='text')return text(v);if(kind==='date')return date(v);if(kind==='boolean')return flag(v);
  return ['deferred_yoy','deferred_qoq','rev_yoy','share_count_reduction_yoy'].includes(key)?V.percent(v,2):amount(v);
 }
 function compare(a,b,key,direction,kind){
  const av=value(a,key),bv=value(b,key);
  return V.compare({value:kind==='date'?date(av):av},{value:kind==='date'?date(bv):bv},'value',direction,kind==='date'?'text':kind);
 }
 async function start(options){
  const doc=root.document,board=doc.getElementById('board'),original=doc.getElementById('original'),status=doc.getElementById('status'),filter=doc.getElementById('q');
  let received,data;
  try{received=await V.load(options.source);original.textContent=received.raw;data=population(received.packet,options.mode);}
  catch(error){if(typeof error.original_text==='string')original.textContent=error.original_text;board.textContent='Publication unavailable: '+error.message;return;}
  const columns=COLUMNS[options.mode];let sort=options.mode==='deferred'?'ticker':'symbol',direction=1;
  status.textContent=data.issues.length?data.issues.length+' source issues: '+data.issues.join('; ')+'. Complete values remain in the original.':'All declared record populations parsed; measurement definitions remain unverified.';
  function paint(){
   const q=filter.value.trim().toUpperCase(),kind=columns.find(c=>c[0]===sort)[2];
   const rows=data.rows.filter(row=>!q||[row.source,...['ticker','symbol','sector','company_name','class'].map(k=>text(row.record[k]))].join(' ').toUpperCase().includes(q))
    .slice().sort((a,b)=>compare(a,b,sort,direction,kind));
   let html='<table><thead><tr>'+columns.map(([key,label,type])=>'<th data-k="'+key+'" class="'+(type==='text'||type==='date'?'l ':'')+(sort===key?'on':'')+'">'+esc(label)+(sort===key?(direction>0?' ↑':' ↓'):'')+'</th>').join('')+'</tr></thead><tbody>';
   for(const row of rows){html+='<tr>'+columns.map(([key,,type])=>{
    const value=cell(row,key,type),cls=type==='text'||type==='date'?'l':'';
    return '<td class="'+cls+'">'+(key==='ticker'||key==='symbol'?(value?'<a href="/ticker.html?symbol='+encodeURIComponent(value)+'">'+esc(value)+'</a>':'Unavailable'):esc(value))+'</td>';
   }).join('')+'</tr>';}
   board.innerHTML=html+'</tbody></table><p class="sub">'+rows.length+' of '+data.rows.length+' received source records; '+data.malformed+' malformed records retained in the original. Repeated tickers are separate source occurrences, not independent company evidence. Publication time: '+esc(text(received.packet.generated_at)||'Unavailable')+'.</p>';
   V.bindSort(doc.querySelectorAll('#board th[data-k]'),sort,direction,key=>{if(sort===key)direction=-direction;else{sort=key;direction=columns.find(c=>c[0]===key)[2]==='number'?-1:1;}paint();});
  }
  filter.oninput=paint;paint();
 }
 const api={amount,flag,date,population,cell,compare,start,COLUMNS};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHStatementDesk=api;
})(typeof globalThis!=='undefined'?globalThis:this);
