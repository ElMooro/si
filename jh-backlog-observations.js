/* Published Backlog measurements, complete retained populations and explicit limits. */
(function(root){
 'use strict';
 const CONTRACT='backlog-measurements.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:null,text=v=>typeof v==='string'?v:'';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=v=>typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER?v:null;
 const ptr=v=>String(v).replace(/~/g,'~0').replace(/\//g,'~1');
 function permissions(v){if(v.call!==undefined&&v.call!==null)throw Error('Research call must abstain');for(const k of ['calls_eligible','forecast_qualified','sizing_eligible'])if(v[k]!==false)throw Error('Research permissions invalid');}
 function model(p){
  if(!obj(p)||!obj(p.by_ticker))throw Error('Complete issuer ledger required');
  const native=p.measurement_contract===CONTRACT,out={native,issuers:[],measurements:[],comparisons:[],periods:[],observations:[]};
  if(native){permissions(p);if(!Number.isSafeInteger(p.ledger_size)||p.ledger_size!==Object.keys(p.by_ticker).length)throw Error('Complete ledger count differs');}
  function add(mode,v,raw,pointer){out[mode].push({index:out[mode].length,ticker:'',cik:'',family:'',tag:'',value:null,unit:'',start:'',end:'',filed:'',accession:'',status:'',...v,raw,pointer});}
  for(const [ticker,row] of Object.entries(p.by_ticker)){
   if(!obj(row)||row.ticker!==ticker)throw Error('Issuer ledger identity differs');
   const path='/by_ticker/'+ptr(ticker),base={ticker,cik:text(row.cik)||String(row.cik??'')};
   const qualified=native&&row.measurement_contract===CONTRACT;
   if(native)permissions(row);
   if(!qualified){
    add('issuers',{...base,status:'Legacy record retained; measurements unverified'},row,path);
    add('measurements',{...base,family:'Legacy',status:'No verified measurement; inspect the whole retained record'},row,path);continue;
   }
   if(!obj(row.measurements))throw Error('Complete concept population required');
   const familyNames=Object.keys(row.measurements);if(['rpo','deferred','eps'].some(k=>!familyNames.includes(k)))throw Error('Original concept families required');
   add('issuers',{...base,value:familyNames.length,unit:'reported concept families',status:'Reported concept observations; filing dates and units must be inspected'},row,path);
   function concept(c,family,pointer,depth=0){
    if(!obj(c)||depth>16||!Array.isArray(c.observations))throw Error('Complete concept observations required');
    if(c.alternate_concepts!==undefined&&!Array.isArray(c.alternate_concepts))throw Error('Complete alternate concepts required');
    const reviewed=c.contract===CONTRACT;if(reviewed)permissions(c);
    const b={...base,family,tag:text(c.tag)},latest=obj(c.latest)||{};
    add('measurements',{...b,value:reviewed?num(latest.value):null,unit:reviewed?text(latest.unit)||text(c.unit):'',start:text(latest.start),end:text(latest.end),filed:text(latest.filed),accession:Array.isArray(latest.accessions)?latest.accessions.join(', '):'',status:text(c.status)||'Unavailable concept; inspect acquisition context'},c,pointer);
    if(reviewed&&!Array.isArray(c.periods))throw Error('Complete derived period population required');
    for(const [i,v]of (c.periods||[]).entries()){
     if(!obj(v))throw Error('Period record required');
     add('periods',{...b,value:num(v.value),unit:text(v.unit),start:text(v.start),end:text(v.end),filed:text(v.filed),accession:Array.isArray(v.accessions)?v.accessions.join(', '):'',status:text(v.status)+'; '+text(v.period_kind)},v,pointer+'/periods/'+i);
    }
    for(const [key,v]of Object.entries(c.comparisons||{})){
     if(!obj(v))throw Error('Comparison record required');
     const current=obj(v.current)||{},prior=obj(v.prior)||{};
     const valid=reviewed&&v.status==='exact_calendar_pair'&&num(v.value_pct)!==null;
     add('comparisons',{...b,family:family+' '+key,value:valid?num(v.value_pct):null,unit:'percent change',start:text(prior.end),end:text(current.end),filed:text(current.filed),status:text(v.status)+'; dates are prior → current endpoints; inspect full duration, units and denominator'},v,pointer+'/comparisons/'+ptr(key));
    }
    c.observations.forEach((v,i)=>{
     if(!obj(v)||!Number.isSafeInteger(v.source_index)||v.source_index<0||typeof v.source_unit!=='string')throw Error('Observation source coordinate required');
     const source=obj(v.source)||{};
     add('observations',{...b,value:num(source.val),unit:v.source_unit,start:text(source.start),end:text(source.end),filed:text(source.filed),accession:text(source.accn),status:v.eligible===true?'Eligible under the producer definition; reported '+text(source.form):'Excluded: '+(Array.isArray(v.reasons)?v.reasons.join(', '):'unresolved observation')},v,pointer+'/observations/'+i);
    });
    (c.alternate_concepts||[]).forEach((v,i)=>concept(v,family+' alternative',pointer+'/alternate_concepts/'+i,depth+1));
   }
   for(const [family,c]of Object.entries(row.measurements))concept(c,family,path+'/measurements/'+ptr(family));
  }
  return out;
 }
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={issuers:[],measurements:[],comparisons:[],periods:[],observations:[]},page=0,key='index',direction=1;
  const labels={issuers:'retained issuer records',measurements:'concept and legacy records',comparisons:'reported comparison records',periods:'derived period records',observations:'retained source observation records'};
  function render(){
   const mode=$('mode').value||'measurements',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const rows=all.filter(r=>[r.ticker,r.cik,r.family,r.tag,r.unit,r.start,r.end,r.filed,r.accession,r.status,r.pointer].join(' ').toLowerCase().includes(query));
   rows.sort((a,b)=>api.compare(a,b,key,direction,['index','value'].includes(key)?'number':'text')||a.index-b.index);
   const pages=Math.max(1,Math.ceil(rows.length/100));page=Math.min(Math.max(0,page),pages-1);const shown=rows.slice(page*100,(page+1)*100);
   $('rows').textContent=rows.length+' matching / '+all.length+' '+labels[mode]+'. Page '+(page+1)+' of '+pages+'.';
   $('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const cols=[['index','Occurrence'],['ticker','Ticker'],['cik','Reported CIK'],['family','Concept family'],['tag','XBRL tag'],['value','Reported value'],['unit','Unit'],['start',mode==='comparisons'?'Prior endpoint':'Period start'],['end',mode==='comparisons'?'Current endpoint':'Period end'],['filed','Filing date'],['accession','Accession'],['status','Definition and limits']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Complete retained evidence</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='index'?r.index+1:k==='value'?r.value===null?'Unavailable':r.value:r[k]||'Unavailable')+'</td>').join('')+'<td><details><summary>Inspect '+esc(r.pointer)+'</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{
   const received=await api.load('/data/backlog.json');$('original').textContent=received.raw;data=model(received.packet);
   $('status').textContent=data.issuers.length+' retained issuers; '+data.observations.length+' source observation records, including retained alternative concepts. Publication '+(text(received.packet.generated_at)||'unavailable')+'. Each measurement has its own period and filing date.';
   $('coverage').textContent='The retained ledger is not the complete provider universe. A recent publication does not refresh old filings. RPO, deferred revenue and EPS have distinct definitions and units. Legacy records remain unqualified. Forecasts, demand acceleration and EV/RPO are not established by this packet.';render();
  }catch(e){data={issuers:[],measurements:[],comparisons:[],periods:[],observations:[]};$('status').textContent='Backlog observations unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('coverage').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,start,escape:esc};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHBacklogObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
