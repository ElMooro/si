/* Complete reported statement populations; source amounts are not investment tiers. */
(function(root){
 'use strict';
 const CONTRACT='revenue-statement-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const numeric=v=>typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER?v:null;
 function model(p){
  const out={native:p.measurement_contract===CONTRACT,statements:[],requests:[]};
  if(out.native){
   if(!Array.isArray(p.request_records))throw Error('Complete request population required');
   p.request_records.forEach((value,requestIndex)=>{
    const r=obj(value);for(const key of ['acquisitions','statement_observations','period_comparisons','quote_records'])if(!Array.isArray(r[key]))throw Error('Complete source population required: '+key);
    const acquisitions=r.acquisitions.map(a=>{const{original_base64,...metadata}=obj(a);return metadata;});
    out.requests.push({index:requestIndex,ticker:text(r.ticker),end:'',start:'',period:'',currency:'',revenue:r.statement_observations.length,
     status:r.acquisitions.map(a=>text(obj(a).endpoint)+': '+text(obj(a).status)).join(' / '),
     raw:{universe_member:r.universe_member,quote_records:r.quote_records,source_population_status:r.source_population_status,acquisitions},pointer:'/request_records/'+requestIndex});
    r.statement_observations.forEach((value,i)=>{
     const s=obj(value),v=obj(s.values),comparisons=r.period_comparisons.filter(c=>obj(c).source_index===s.source_index),c=comparisons.length===1?obj(comparisons[0]):{};
     const valid=s.status==='reported_statement_amounts',year=obj(obj(obj(c.yoy).changes).revenue);
     out.statements.push({index:out.statements.length,ticker:text(r.ticker),reported:text(obj(s.raw).symbol),period:text(s.reported_period),
      start:text(s.period_start),end:text(s.period_end),currency:text(s.reported_currency)||'Unreported',revenue:valid?numeric(v.revenue):null,
      growth:valid?numeric(year.pct_positive_base):null,acceleration:valid?numeric(c.revenue_growth_acceleration_pp):null,
      gross_margin:valid?numeric(s.gross_margin_pct):null,ttm:valid?numeric(c.ttm_revenue):null,
      status:text(s.status)+' / '+text(c.status),raw:{statement:value,comparison:c,acquisition_metadata:acquisitions},pointer:'/request_records/'+requestIndex+'/statement_observations/'+i});
    });
   });return out;
  }
  for(const[key,values]of [['all_qualifying',p.all_qualifying],['summary/top_25_overall',obj(p.summary).top_25_overall],['summary/tier_s',obj(p.summary).tier_s],['summary/microcap_picks',obj(p.summary).microcap_picks]]){
   if(values!==undefined&&!Array.isArray(values))throw Error('Malformed legacy population: '+key);
   (values||[]).forEach((value,i)=>{const r=obj(value);out.statements.push({index:out.statements.length,ticker:typeof value==='string'?value:text(r.symbol),
    start:'',end:'',period:'Unverified legacy period',currency:'Unreported',revenue:null,growth:null,acceleration:null,gross_margin:null,ttm:null,
    status:'Legacy '+key+'; growth and tier claims unverified',raw:value,pointer:'/'+key+'/'+i});});
  }return out;
 }
 function summary(p){const m=model(p);return {native:m.native,message:m.native?m.requests.length+' request records and '+m.statements.length+' reported statements. No qualified investment signal.':m.statements.length+' published legacy occurrences. Period comparisons and investment tiers remain unverified.',generated_at:text(p.generated_at)||'unavailable'};}
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={statements:[],requests:[]},page=0,key='index',direction=1;
  function render(){
   const mode=$('mode').value||'statements',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const rows=all.filter(r=>[r.ticker,r.reported,r.start,r.end,r.period,r.status,r.pointer].join(' ').toLowerCase().includes(query));
   const numbers=['index','revenue','growth','acceleration','gross_margin','ttm'];rows.sort((a,b)=>api.compare(a,b,key,direction,numbers.includes(key)?'number':'text'));
   const pages=Math.max(1,Math.ceil(rows.length/100));page=Math.min(Math.max(page,0),pages-1);const shown=rows.slice(page*100,(page+1)*100);
   $('rows').textContent=rows.length+' matching / '+all.length+' received '+mode+' occurrences. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const cols=mode==='requests'?[['index','Occurrence'],['ticker','Requested ticker'],['revenue','Statement records'],['status','Acquisition outcomes']]:
    [['index','Occurrence'],['ticker','Requested ticker'],['period','Reported period'],['start','Period start'],['end','Period end'],['currency','Reported currency'],['revenue','Revenue amount'],['growth','Annual revenue change %'],['acceleration','Growth change pp'],['gross_margin','Gross margin %'],['ttm','Trailing-year revenue'],['status','Qualification']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Complete source and calculation</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='index'?r.index+1:numbers.includes(k)?(api.format(r[k],mode==='requests'?0:2)||'Unavailable'):r[k])+'</td>').join('')+'<td><details><summary>Inspect '+esc(r.pointer)+'</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{
   const received=await api.load('/data/revenue-acceleration.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);const s=summary(p);
   $('status').textContent=s.message+' Generated '+s.generated_at+'. Collection time does not establish filing availability.';
   $('coverage').textContent='Every published occurrence is retained. Inspect failed and unattempted requests, original statement records and calculation coordinates. Repeated income-statement sources are shared evidence, not independent investment votes.';render();
  }catch(e){$('status').textContent='Stored revenue research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,summary,start,escape:esc};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHRevenueObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
