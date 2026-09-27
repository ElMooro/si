/* EDGAR search observations, not issuer events, severity ratings or investment signals. */
(function(root){
 'use strict';
 const object=x=>x&&typeof x==='object'&&!Array.isArray(x)?x:{};
 const text=x=>typeof x==='string'?x:'';
 const CONTRACT='sec-search-research.v1',RISK=new Set(['going_concern','restatement','material_weakness','auditor_change','investigation','bankruptcy']);
 const F=typeof module!=='undefined'&&module.exports?require('./jh-filing-desk.js'):root.JHFilingDesk;
 const esc=F.esc;
 function model(packet,riskOnly=false){
  const rows=[],native=packet.contract===CONTRACT;
  if(native){
   if(!Array.isArray(packet.search_matches)||!Array.isArray(packet.source_responses))throw Error('Complete search-hit and query-response populations required');
   const links=new Map();
   for(const company of packet.all_tickers||[])for(const event of object(company).events||[]){const e=object(event);if(typeof e.match_id==='string'&&F.secURL(e.filing_url)){const values=links.get(e.match_id)||[];if(!values.includes(e.filing_url))values.push(e.filing_url);links.set(e.match_id,values);}}
   packet.search_matches.forEach((value,i)=>{
    const r=object(value),e=object(r.source_evidence),hit=object(object(r.source_hit)._source);
    if(riskOnly&&!RISK.has(e.query_id))return;
    const entities=Array.isArray(r.entity_associations)?r.entity_associations:[];
    rows.push({index:i,query:text(e.query_id),filed:text(hit.file_date),filed_time:F.date(hit.file_date),form:text(hit.form),accession:text(hit.adsh),
     tickers:entities.map(x=>text(object(x).ticker)).filter(Boolean).join('; '),names:entities.map(x=>text(object(x).name)||text(object(x).raw)).filter(Boolean).join('; '),
     ciks:entities.map(x=>text(object(x).cik)).filter(Boolean).join('; '),received:text(e.received_at),
     issues:Array.isArray(r.issues)?r.issues.filter(x=>typeof x==='string').join('; '):'Unverified record',
     coordinate:'/search_matches/'+i,links:links.get(r.match_id)||[],raw:value});
   });
  }else{
   const add=(value,path,parent)=>{const r=object(value),query=text(r.signal_id);if(riskOnly&&!RISK.has(query))return;
    rows.push({index:rows.length,query,filed:text(r.filed_at),filed_time:F.date(r.filed_at),form:text(r.form),accession:text(r.accession),
     tickers:text(r.ticker)||text(parent.ticker),names:text(r.name)||text(parent.name),ciks:text(r.cik)||text(parent.cik),received:'',
     issues:'Legacy issuer event, original source identity and timing unverified',coordinate:path,links:F.secURL(r.filing_url)?[r.filing_url]:[],raw:value});};
   for(const[key,values]of [['all_tickers',packet.all_tickers],...['critical','risks','opportunities'].flatMap(k=>[[k,packet[k]],['highlights.'+k,object(packet.highlights)[k]]])]){
    if(values!==undefined&&!Array.isArray(values))throw Error('Malformed legacy population: '+key);
    (values||[]).forEach((value,i)=>{const r=object(value),path=key+'['+i+']';
     if(r.events!==undefined&&!Array.isArray(r.events))throw Error('Malformed legacy event population');
     if(Array.isArray(r.events)&&r.events.length)r.events.forEach((event,j)=>add(event,path+'.events['+j+']',r));else add(value,path,r);
    });
   }
  }
  return{native,rows,responses:native?packet.source_responses.filter(r=>!riskOnly||RISK.has(object(r).query_id)):[]};
 }
 function start(options={}){
  const V=root.JHTableValues,$=id=>root.document.getElementById(id);let data,sort='filed_time',direction=-1;
  function paint(){
   if(!data)return;const q=$('q').value.trim().toLowerCase();
   const rows=data.rows.filter(r=>[r.tickers,r.names,r.ciks,r.query,r.accession,r.coordinate,r.form,r.issues].join(' ').toLowerCase().includes(q)).sort((a,b)=>V.compare(a,b,sort,direction,sort==='filed_time'||sort==='index'?'number':'text'));
   $('status').textContent=rows.length+' shown / '+data.rows.length+' received '+(data.native?'search-hit':'legacy')+' occurrences in this desk. A repeated hit or co-filer is not an independent investment vote.';
   const cols=[['filed_time','Filed'],['query','Search query'],['tickers','Reported tickers'],['names','Reported entities'],['ciks','Reported CIKs'],['form','Reported form'],['accession','Accession'],['received','Response received']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Source evidence and limits</th></tr></thead><tbody>'+rows.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='filed_time'?F.dateLabel(r.filed):r[k])+'</td>').join('')+'<td>'+esc(r.coordinate)+'<br>'+esc(r.issues)+(r.links.length?'<p>'+r.links.map((u,i)=>F.documentLink(u,'SEC index '+(i+1))).join(' · ')+'</p>':'<p>Verified document link unavailable</p>')+'<details><summary>Inspect complete source occurrence</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   V.bindSort($('board').querySelectorAll('th[data-k]'),sort,direction,k=>{direction=sort===k?-direction:1;sort=k;paint();});
  }
  $('q').oninput=paint;$('original').textContent='Unavailable';
  return(async()=>{try{
   const{packet:p,raw}=await V.load('/data/sec-filings-intel.json');$('original').textContent=raw;data=model(p,options.riskOnly===true);
   $('publication').textContent='Packet generated: '+F.dateLabel(p.generated_at)+'. Filing date, reporting period and response receipt are different clocks.';
   const quality=object(p.quality);
   $('coverage').textContent=data.native?'All-query coverage: '+V.count(quality.queries_parsed)+' parsed / '+V.count(quality.queries_requested)+' requested; '+V.count(quality.complete_query_populations)+' complete query populations. '+V.count(quality.returned_hit_occurrences)+' returned hit occurrences. Provider errors, first-page limits and unresolved entities remain explicit.':'Legacy publication: source acquisition completeness and original response identities are unavailable. Every published occurrence in this desk remains inspectable.';
   $('queries').innerHTML=data.native?'<table><thead><tr><th scope="col">Query</th><th scope="col">HTTP / acquisition</th><th scope="col">Returned hits</th><th scope="col">Reported total</th><th scope="col">Population complete</th><th scope="col">Original evidence</th></tr></thead><tbody>'+data.responses.map(value=>{const r=object(value);return '<tr><td>'+esc(r.query_id)+'</td><td>'+esc(r.http_status)+' / '+esc(r.parse_status||r.status)+'</td><td>'+esc(V.count(r.returned_hits))+'</td><td>'+esc(V.count(r.reported_total))+'</td><td>'+(r.query_population_complete===true?'Yes, as reported':r.query_population_complete===false?'No':'Unavailable')+'</td><td><details><summary>Inspect query response</summary><pre>'+esc(JSON.stringify(value,null,2))+'</pre></details></td></tr>';}).join('')+'</tbody></table>':'Original acquisition coverage unavailable';
   paint();
  }catch(e){data=null;$('status').textContent='Stored filing research unavailable: '+e.message;$('board').textContent='Publication unavailable: '+e.message;$('coverage').textContent='Unavailable';if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,start};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHSecSearchDesk=api;
})(typeof globalThis!=='undefined'?globalThis:this);
