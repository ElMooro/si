/* Dated workforce research: complete populations, explicit lineage and no score authority. */
(function(root){
 'use strict';
 const CONTRACT='hiring-statement-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function rows(packet){
  if(packet.measurement_contract===CONTRACT){
   if(!Array.isArray(packet.request_records))throw Error('Whole workforce request population required');
   return packet.request_records.map((value,index)=>{
    const r=obj(value),stock=obj(r.universe_record);
    if(!Array.isArray(r.employee_observations)||!Array.isArray(r.annual_comparisons)||!Array.isArray(r.income_observations)||!Array.isArray(r.acquisitions))throw Error('Complete statement populations required');
    const employees=r.employee_observations.map(obj),dated=employees.filter(e=>typeof e.report_period_end==='string').sort((a,b)=>b.report_period_end.localeCompare(a.report_period_end));
    const latest=dated[0]||{},sameDate=dated.filter(e=>e.report_period_end===latest.report_period_end),valid=latest.measurement_status==='reported_count'&&sameDate.length===1;
    const comparison=r.annual_comparisons.find(c=>obj(c).source_index===latest.source_index)||{};
    const ratios=r.income_observations.filter(c=>obj(c).employee_source_index===latest.source_index&&obj(c).status==='aligned_descriptive_ratio');
    const ratio=ratios.length===1?ratios[0]:{};
    return {index,ticker:text(r.symbol),name:text(stock.name),sector:text(stock.sector),period:text(latest.report_period_end),
     headcount:valid?latest.employee_count:null,change:valid?comparison.annual_interval_change_pct:null,
     revenue:valid?ratio.annual_revenue_per_ending_employee:null,currency:text(ratio.reported_currency),
     acquisition:r.acquisitions.map(a=>text(obj(a).endpoint)+': '+text(obj(a).status)).join(' / '),
     status:valid?'Reported count; organic hiring unverified':sameDate.length>1?'Conflicting or repeated report period':'No qualified reported count',
     source:'/request_records/'+index,original:value};
   });
  }
  const out=[];
  for(const key of ['top_50','expansion_inflections','double_confirmed']){
   if(packet[key]!==undefined&&!Array.isArray(packet[key]))throw Error('Malformed legacy subset: '+key);
   (packet[key]||[]).forEach((value,i)=>{const r=obj(value);out.push({index:out.length,ticker:text(r.symbol),name:text(r.name),sector:text(r.sector),
    period:'Unverified',headcount:r.headcount_latest,change:null,revenue:null,currency:'',
    acquisition:'Legacy '+key,status:'Legacy growth, ratio and expansion claims unverified',source:'/'+key+'/'+i,original:value});});
  }
  return out;
 }
 function pageRows(all,query,key,direction,page,size,api){
  const filtered=all.filter(r=>[r.ticker,r.name,r.sector,r.status,r.acquisition].join(' ').toLowerCase().includes(query.trim().toLowerCase()));
  filtered.sort((a,b)=>api.compare(a,b,key,direction,['index','headcount','change','revenue'].includes(key)?'number':'text'));
  const pages=Math.max(1,Math.ceil(filtered.length/size)),current=Math.min(Math.max(0,page),pages-1);
  return {total:filtered.length,pages,page:current,rows:filtered.slice(current*size,(current+1)*size)};
 }
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let all=[],key='index',direction=1,page=0;
  function render(){
   const view=pageRows(all,$('q').value,key,direction,page,100,api);page=view.page;
   $('rows').textContent=view.total+' matching / '+all.length+' received occurrences. Page '+(page+1)+' of '+view.pages+'; up to 100 rows per page. Duplicate occurrences remain separate.';
   $('previous').disabled=page===0;$('next').disabled=page+1===view.pages;
   const cols=[['index','Occurrence'],['ticker','Ticker'],['name','Name'],['sector','Sector'],['period','Report period'],['headcount','Reported employees'],['change','Annual interval change'],['revenue','Annual revenue / ending employee']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Acquisition, qualifications and original records</th></tr></thead><tbody>'+view.rows.map(r=>'<tr>'+cols.map(([k])=>{
    const v=k==='index'?r.index+1:k==='headcount'?api.count(r.headcount):k==='change'?(api.percent(r.change,2)||'Unavailable'):k==='revenue'?(api.number(r.revenue)===null?'Unavailable':api.format(r.revenue,2)+' '+r.currency):r[k];
    return '<td>'+escape(v)+'</td>';
   }).join('')+'<td>'+escape(r.status)+'<p>'+escape(r.acquisition)+'</p><details><summary>Source '+escape(r.source)+'</summary><pre>'+escape(JSON.stringify(r.original,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{
   const received=await api.load('/data/hiring-velocity.json'),p=received.packet;$('original').textContent=received.raw;all=rows(p);
   const native=p.measurement_contract===CONTRACT;
   $('status').textContent=(native?'Reported workforce research. Investment and organic-hiring claims are unqualified. ':'Legacy packet: annual growth, productivity and investment scores remain unverified. ')+
    'Generated '+(text(p.generated_at)||'unavailable')+'. Generation is not the reporting period or filing date.';
   $('coverage').textContent=native?all.length+' selected universe occurrences; '+api.count(p.universe_occurrences)+' universe occurrences. Every selected company is retained, including failed and unattempted requests. Full universe and bagger context are retained in the packet. Provider-history completeness remains unverified.':
    'All published top-50, inflection and double-confirmed subset occurrences are retained here. The old packet does not contain every scanned company or original provider response. Legacy claims remain available in each original record.';
   render();
  }catch(e){all=[];$('status').textContent='Stored workforce research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,rows,pageRows,start,escape};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHHiringObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
