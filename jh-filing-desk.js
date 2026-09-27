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
  if(typeof value!=='string'||!/^https:\/\/(?:www\.)?sec\.gov\/Archives\/edgar\/data\/\d+\/[A-Za-z0-9_./-]+$/.test(value))return null;
  try{
   const u=new URL(value);
   if(u.protocol!=='https:'||!['sec.gov','www.sec.gov'].includes(u.hostname)||u.username||u.password||u.port||u.search||u.hash)return null;
   if(!/^\/Archives\/edgar\/data\/\d+\/[A-Za-z0-9_./-]+$/.test(u.pathname)||/\/(?:\.|\.\.)\//.test(value))return null;
   return u.href;
  }catch{return null;}
 }
 function documentLink(value,label){const url=secURL(value);return url?'<a href="'+esc(url)+'" target="_blank" rel="noopener noreferrer">'+esc(label||'SEC document')+'</a>':esc(label||'Unavailable')+' <span class="muted">(document link unavailable)</span>';}
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
    // Preserve packets using the older top-level populations, too.
    groups(d[key],key,false);
   }
  }else throw Error('Unknown filing desk');
  return{packet:d,rows,warnings,malformed,primaryAvailable};
 }
 function compare(a,b,key,dir){return V.compare(a.view,b.view,key,dir,key==='filed_time'||key==='weight'?'number':'text');}
 function start(options){
  const doc=root.document,get=id=>doc.getElementById(id),mode=options.mode;
  if(options.item!==undefined&&(mode!=='8k'||typeof options.item!=='string'||!/^\d\.\d{2}$/.test(options.item)))throw Error('A scoped filing desk requires one valid 8-K item');
  const fixedItem=options.item||'';
  let data,sort='filed_time',direction=-1,item='';
  const columns=mode==='8k'?[['filed_time','Filed'],['company','Company / SEC document'],['items_text','Items'],['accession','Accession']]:mode==='10kq'?[['filed_time','Filed'],['form','Form'],['company','Company / SEC document'],['cik','CIK'],['accession','Accession']]:[['filed_time','Filed'],['ticker','Ticker / company'],['signal_label','Matched signal / SEC document'],['form','Form'],['severity','Source severity'],['weight','Source weight'],['accession','Accession']];
  function paint(){
   if(!data)return;
   const q=get('q').value.trim().toLocaleUpperCase();
   const rows=data.rows.filter(row=>{
    const r=row.view;
    if((fixedItem||item)&&!(Array.isArray(r.items)&&r.items.includes(fixedItem||item)))return false;
    return !q||[r.company,r.name,r.ticker,r.cik,r.form,r.accession,r.signal_id,r.signal_label,r.items_text,row.sourcePath].map(text).join(' ').toLocaleUpperCase().includes(q);
   }).sort((a,b)=>compare(a,b,sort,direction));
   const labels=object(data.packet.item_labels)?data.packet.item_labels:{};
   let html='<table><caption>Received filing records; repeated source occurrences are retained</caption><thead><tr>'+columns.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Source path</th></tr></thead><tbody>';
   for(const row of rows){
    const r=row.view;
    html+='<tr><td>'+esc(dateLabel(r.filed_at))+'</td>';
    if(mode==='flags'){
     const ticker=text(r.ticker),tickerCell=/^[A-Za-z0-9][A-Za-z0-9.^-]{0,19}$/.test(ticker)?'<a href="/ticker.html?symbol='+encodeURIComponent(ticker)+'">'+esc(ticker)+'</a>':esc(ticker||'Unavailable');
     html+='<td>'+tickerCell+'<br>'+esc(r.name)+'</td><td>'+documentLink(r.filing_url,text(r.signal_label)||text(r.signal_id))+'</td><td>'+esc(r.form)+'</td><td>'+esc(r.severity)+'</td><td>'+esc(V.format(r.weight,2))+'</td><td>'+esc(r.accession)+'</td>';
    }else{
     if(mode==='10kq')html+='<td>'+esc(r.form)+'</td>';
     html+='<td>'+documentLink(r.filing_url,text(r.company))+'</td>';
     if(mode==='8k')html+='<td>'+(Array.isArray(r.items)?r.items.map(it=>'<span class="item">'+esc(it)+(text(labels[it])?' '+esc(labels[it]):'')+'</span>').join(''):'Unavailable; see original')+'</td>';
     else html+='<td>'+esc(r.cik)+'</td>';
     html+='<td>'+esc(r.accession)+'</td>';
    }
    html+='<td class="source-path">'+esc(row.sourcePath)+'</td></tr>';
   }
   get('board').innerHTML=html+'</tbody></table>';
   V.bindSort(doc.querySelectorAll('#board th[data-k]'),sort,direction,key=>{direction=key===sort?-direction:key==='filed_time'||key==='weight'?-1:1;sort=key;paint();});
   get('status').textContent=rows.length+' of '+data.rows.length+' received source occurrences shown. '+(data.primaryAvailable?'':'Primary population unavailable. ')+(data.rows.length===0&&data.primaryAvailable?'The received population contains no matching rows. ':'')+data.malformed+' malformed records retained in original text. '+data.warnings.join(' ');
  }
  function details(){
   const d=data.packet,st=object(d.stats)?d.stats:{};
   const counts=mode==='8k'?[['Source filings',st.total_filings],['Source red flags',st.red_flag_filings],['Source high impact',st.high_impact_filings],['Window days',d.window_days]]:mode==='10kq'?[['Source total',st.total],['Source 10-K',st.total_10k],['Source 10-Q',st.total_10q],['Source 10-K/A',st.total_10k_amended],['Source 10-Q/A',st.total_10q_amended]]:[['Names on source tape',d.n_tickers_with_signals],['Source event total',d.n_events_total],['Lookback days',d.lookback_days]];
   get('kpis').textContent=counts.map(([label,v])=>label+': '+V.count(v)).join(' · ');
   get('publication').textContent='Publication time (source reported): '+dateLabel(d.generated_at)+'. Filing dates are separate observations. Publication time does not establish source freshness or completeness.';
   if(mode==='8k'){
    const counts=object(d.by_item_counts)?d.by_item_counts:{},labels=object(d.item_labels)?d.item_labels:{};
    if(fixedItem){get('items').textContent='Item '+fixedItem+(text(labels[fixedItem])?' — '+text(labels[fixedItem]):'')+'. This desk keeps its declared item filter; every source record remains in the original.';return;}
    get('items').innerHTML='<button type="button" class="chip on" aria-pressed="true" data-item="">All items</button>'+Object.keys(counts).sort().map(key=>'<button type="button" class="chip" aria-pressed="false" data-item="'+esc(key)+'">'+esc(key)+' '+esc(labels[key])+' ('+esc(V.count(counts[key]))+')</button>').join('');
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
