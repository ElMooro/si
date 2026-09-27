/* Research presentation only. Source occurrences remain separate; no issuer verdicts. */
(function(root){
 'use strict';
 const V=typeof module!=='undefined'&&module.exports?require('./jh-table-values.js'):root.JHTableValues;
 const KEEP=new Set(['going_concern','restatement','material_weakness','auditor_change','investigation','bankruptcy']);
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 const text=x=>typeof x==='string'?x:typeof x==='number'&&Number.isSafeInteger(x)?String(x):'';
 const esc=x=>text(x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function date(value){
  if(typeof value!=='string')return null;
  const m=/^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,9})?)?(Z|[+-]\d{2}:\d{2}))?$/.exec(value);
  if(!m)return null;
  const year=+m[1],month=+m[2],day=+m[3];
  const days=month===2?(year%4===0&&(year%100!==0||year%400===0)?29:28):[4,6,9,11].includes(month)?30:31;
  if(year<1||month<1||month>12||day<1||day>days||+(m[4]||0)>23||+(m[5]||0)>59||+(m[6]||0)>59)return null;
  if(m[7]&&m[7]!=='Z'&&(Number(m[7].slice(1,3))>23||Number(m[7].slice(4))>59))return null;
  const n=Date.parse(value);return Number.isFinite(n)?n:null;
 }
 function dateLabel(value){return date(value)===null?(value==null||value===''?'Unavailable':'Invalid date; see original'):value;}
 function secURL(value){
  if(typeof value!=='string'||!/^https:\/\/(?:www\.)?sec\.gov\/Archives\/edgar\/data\/\d+\/[A-Za-z0-9_./-]+$/.test(value))return '';
  return value;
 }
 function documentLink(row){
  const href=secURL(row.filing_url||row.url||'');
  const label=esc(row.company||row.name||row.ticker||row.accession||'SEC document');
  return href?'<a href="'+esc(href)+'" target="_blank" rel="noopener">'+label+'</a>':label;
 }
 function model(packet,mode){
  const rows=[],warnings=[];let malformed=0,primaryAvailable=false;
  const d=object(packet)?packet:{};
  function population(value,path,required,callback){
   if(!Array.isArray(value)){if(required||value!==undefined)warnings.push(path+' is missing or malformed.');return;}
   if(required)primaryAvailable=true;
   value.forEach((row,i)=>{if(!object(row)){malformed++;return;}callback(row,path+'['+i+']');});
  }
  function add(row,path,parent){
   const view={...row};
   if(mode==='10kq')view.cik=text(row.cik);
   if(mode==='flags'){
    if(typeof row.signal_id!=='string'||!KEEP.has(row.signal_id))return;
    if(!text(view.ticker)&&text(parent?.ticker))view.ticker=parent.ticker;
    if(!text(view.name)&&text(parent?.name))view.name=parent.name;
    view.signal_label=text(row.signal_label)||row.signal_id;
   }
   view.filed_time=date(row.filed_at);view.items_text=Array.isArray(row.items)?row.items.map(text).join(', '):null;
   rows.push({view,source:row,sourcePath:path});
  }
  if(mode==='8k')population(d.filings,'filings',true,add);
  else if(mode==='10kq'){
   population(d.filings,'filings',true,add);population(d.amended,'amended',true,add);
  }else if(mode==='flags'){
   function groups(value,path,required){population(value,path,required,(row,p)=>{
    if(Array.isArray(row.events))population(row.events,p+'.events',false,(event,e)=>add(event,e,row));
    else if(row.signal_id)add(row,p);
    else warnings.push(p+'.events is missing or malformed.');
   });}
   groups(d.all_tickers,'all_tickers',true);
   if(d.highlights!==undefined&&!object(d.highlights))warnings.push('highlights is malformed.');
   for(const key of ['critical','risks','opportunities']){
    groups(d.highlights?.[key],'highlights.'+key,false);
    groups(d[key],key,false);
   }
  }else throw Error('Unknown filing desk');
  return{packet:d,rows,warnings,malformed,primaryAvailable};
 }
 function compare(a,b,key,dir){return V.compare(a.view,b.view,key,dir,key==='filed_time'||key==='weight'?'number':'text');}
 function start(options){
  const doc=root.document,get=id=>doc.getElementById(id),mode=options.mode;
  let data,sort='filed_time',direction=-1,item=typeof options.item==='string'?options.item:'';
  const itemSet=Array.isArray(options.items)?options.items.filter(function(x){return typeof x==='string'&&x;}):null;
  const columns=mode==='8k'?[['filed_time','Filed'],['company','Company / SEC document'],['items_text','Items'],['accession','Accession']]:mode==='10kq'?[['filed_time','Filed'],['form','Form'],['company','Company / SEC document'],['cik','CIK'],['accession','Accession']]:[['filed_time','Filed'],['ticker','Ticker / company'],['signal_label','Matched signal / SEC document'],['form','Form'],['severity','Source severity'],['weight','Source weight'],['accession','Accession']];
  function paint(){
   if(!data)return;
   const q=get('q').value.trim().toLocaleUpperCase();
   const rows=data.rows.filter(row=>{
    const r=row.view;
    if(itemSet&&itemSet.length){if(!(Array.isArray(r.items)&&r.items.some(function(x){return itemSet.indexOf(x)>=0;})))return false;}
    else if(item&&!(Array.isArray(r.items)&&r.items.includes(item)))return false;
    return !q||[r.company,r.name,r.ticker,r.cik,r.form,r.accession,r.signal_id,r.signal_label,r.items_text,row.sourcePath].map(text).join(' ').toLocaleUpperCase().includes(q);
   }).sort((a,b)=>compare(a,b,sort,direction));
   const labels=object(data.packet.item_labels)?data.packet.item_labels:{};
   let html='<table><caption>Received filing records</caption><thead><tr>'+columns.map(([key,label])=>'<th data-k="'+key+'" class="'+(sort===key?'on':'')+'">'+label+(sort===key?(direction<0?' \u2193':' \u2191'):'')+'</th>').join('')+'</tr></thead><tbody>';
   rows.forEach(row=>{
    const r=row.view;
    html+='<tr>';
    columns.forEach(([key])=>{
     if(key==='company'||key==='signal_label')html+='<td>'+documentLink(r)+'</td>';
     else if(key==='filed_time')html+='<td>'+esc(dateLabel(r.filed_at))+'</td>';
     else if(key==='items_text')html+='<td>'+esc(r.items_text||'')+(Array.isArray(r.items)?r.items.map(it=>'<span class="item">'+esc(it)+(labels[it]?' '+esc(String(labels[it]).slice(0,24)):'')+'</span>').join(''):'')+'</td>';
     else html+='<td>'+esc(r[key]==null?'':r[key])+'</td>';
    });
    html+='</tr>';
   });
   get('board').innerHTML=html+'</tbody></table>';
   get('status').textContent=rows.length+' rows after filter. Generated '+(data.packet.generated_at||'')+'. Click headers to sort.';
   doc.querySelectorAll('#board th[data-k]').forEach(el=>{el.onclick=()=>{const k=el.getAttribute('data-k');if(sort===k)direction=-direction;else{sort=k;direction=k==='filed_time'?-1:1;}paint();};});
  }
  function details(){
   const d=data.packet||{};
   const st=d.stats||{};
   get('kpis').innerHTML='<span class="chip"><b>'+(st.total_filings!=null?st.total_filings:(d.filings||[]).length)+'</b> filings</span><span class="chip"><b>'+(data.rows.length)+'</b> modeled rows</span><span class="chip"><b>'+(data.malformed||0)+'</b> malformed</span>';
   if(mode==='8k'&&get('items')){
    const counts=object(d.by_item_counts)?d.by_item_counts:{},labels=object(d.item_labels)?d.item_labels:{};
    get('items').innerHTML='<button type="button" class="chip'+(item||(itemSet&&itemSet.length)?'':' on')+'" aria-pressed="'+String(!(item||(itemSet&&itemSet.length)))+'" data-item="">All items</button>'+Object.keys(counts).sort().map(key=>'<button type="button" class="chip'+(item===key||(itemSet&&itemSet.indexOf(key)>=0)?' on':'')+'" aria-pressed="'+String(item===key||(itemSet&&itemSet.indexOf(key)>=0))+'" data-item="'+esc(key)+'">'+esc(key)+' '+esc(labels[key])+' ('+esc(V.count(counts[key]))+')</button>').join('');
    get('items').onclick=e=>{const button=e.target.closest('button[data-item]');if(!button||!get('items').contains(button))return;item=button.dataset.item;for(const b of doc.querySelectorAll('#items button')){b.classList.toggle('on',b===button);b.setAttribute('aria-pressed',String(b===button));}paint();};
   }
  }
  get('q').oninput=paint;
  const ready=(async()=>{
   try{
    const received=await V.load(options.source);
    get('original').textContent=received.raw;
    data=model(received.packet,mode);details();paint();
   }catch(error){
    get('original').textContent=typeof error.original_text==='string'?error.original_text:'Unavailable';
    get('board').textContent='Publication unavailable: '+error.message;
    get('status').textContent='No filing conclusion is available from this failed read.';
   }
  })();
  return{ready};
 }
 const api={text,esc,date,dateLabel,secURL,documentLink,model,compare,start};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHFilingDesk=api;
})(typeof globalThis!=='undefined'?globalThis:this);
