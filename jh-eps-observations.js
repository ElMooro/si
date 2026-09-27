/* Forecast targets, snapshot comparisons and rating opinions are distinct populations. */
(function(root){
 'use strict';
 const CONTRACT='eps-target-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function model(p){
  const out={targets:[],ratings:[],requests:[],native:p.measurement_contract===CONTRACT};
  if(out.native){
   if(!Array.isArray(p.request_records))throw Error('Complete request population required');
   p.request_records.forEach((value,requestIndex)=>{
    const r=obj(value);
    for(const k of ['acquisitions','quote_records','estimate_observations','same_target_comparisons','rating_observations'])if(!Array.isArray(r[k]))throw Error('Complete source population required: '+k);
    const acquisitions=r.acquisitions.map(a=>{const{original_base64,...metadata}=obj(a);return metadata;});
    out.requests.push({ticker:text(r.ticker),index:requestIndex,target:'',value:r.estimate_observations.length,unit:'forecast records',change:r.rating_observations.length,
     action:r.acquisitions.map(a=>text(obj(a).endpoint)+': '+text(obj(a).status)).join(' / '),status:'Research acquisition; no investment authority',
     raw:{request_index:requestIndex,source_population_status:r.source_population_status,quote_records:r.quote_records,acquisitions},pointer:'/request_records/'+requestIndex});
    r.estimate_observations.forEach((row,i)=>{
     const e=obj(row),v=obj(e.values),valid=e.measurement_status==='reported_forecast_observation',comparison=r.same_target_comparisons.find(c=>obj(c).source_index===e.source_index)||{};
     out.targets.push({ticker:text(r.ticker),reported:text(obj(e.raw).symbol),index:out.targets.length,target:text(e.target_period_end),
      value:valid?v.epsAvg:null,unit:text(e.reported_currency)||'Unreported',change:valid?comparison.eps_change:null,
      action:text(e.target_status),status:text(e.measurement_status),raw:{observation:row,comparison,acquisition_metadata:acquisitions},
      pointer:'/request_records/'+requestIndex+'/estimate_observations/'+i});
    });
    r.rating_observations.forEach((row,i)=>{const g=obj(row);out.ratings.push({ticker:text(r.ticker),reported:text(obj(g.raw).symbol),index:out.ratings.length,
     target:text(g.reported_date),value:text(g.previous_grade),unit:text(g.new_grade),change:text(g.grading_company),action:text(g.reported_action),
     status:text(g.status),raw:row,pointer:'/request_records/'+requestIndex+'/rating_observations/'+i});});
   });
   return out;
  }
  const sources=[['all_qualifying',p.all_qualifying],['summary/top_25_overall',obj(p.summary).top_25_overall],['summary/tier_a',obj(p.summary).tier_a],['summary/tier_b_symbols',obj(p.summary).tier_b_symbols]];
  for(const[key,values]of sources){
   if(values!==undefined&&!Array.isArray(values))throw Error('Malformed legacy population: '+key);
   (values||[]).forEach((value,i)=>{const r=obj(value);out.targets.push({ticker:typeof value==='string'?value:text(r.symbol),index:out.targets.length,
    target:'Unverified legacy period',value:null,unit:'Unreported',change:null,action:'Legacy '+key,status:'Revision, tier and return claims unverified',raw:value,pointer:'/'+key+'/'+i});});
  }
  return out;
 }
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={targets:[],ratings:[],requests:[]},page=0,key='index',direction=1;
  function render(){
   const mode=$('mode').value||'targets',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const list=all.filter(r=>[r.ticker,r.reported,r.target,r.action,r.status,r.pointer].join(' ').toLowerCase().includes(query));
   const numeric=mode==='ratings'?['index']:['index','value','change'];list.sort((a,b)=>api.compare(a,b,key,direction,numeric.includes(key)?'number':'text'));
   const pages=Math.max(1,Math.ceil(list.length/100));page=Math.min(Math.max(page,0),pages-1);const shown=list.slice(page*100,(page+1)*100);
   $('rows').textContent=list.length+' matching / '+all.length+' received '+mode+' occurrences. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const fields=mode==='ratings'?[['target','Rating date'],['value','Previous grade'],['unit','New grade'],['change','Grading company'],['action','Reported action']]:
    mode==='requests'?[['value','Forecast records'],['change','Rating records'],['action','Acquisition outcomes']]:[['target','Annual target end'],['value','Reported EPS estimate'],['unit','Reported currency'],['change','Same-target EPS change'],['action','Target timing']];
   const cols=[['index','Occurrence'],['ticker','Requested ticker'],...fields,['status','Qualification']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Source record and evidence</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+escape(k==='index'?r.index+1:numeric.includes(k)?(api.format(r[k],mode==='requests'?0:3)||'Unavailable'):r[k])+'</td>').join('')+'<td><details><summary>Inspect '+escape(r.pointer)+'</summary><pre>'+escape(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{
   const received=await api.load('/data/eps-revision-velocity.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);
   $('status').textContent=(data.native?'Source observations; no qualified investment signal. ':'Legacy output: revision, tier and outperformance claims are unverified. ')+
    'Generated '+(text(p.generated_at)||'unavailable')+'. This is a collection clock, not an estimate target or original release date.';
   $('coverage').textContent=data.native?data.requests.length+' request records, '+data.targets.length+' annual forecast records and '+data.ratings.length+' rating records. Inspect acquisition coverage separately; unattempted and failed requests remain explicit. Original response bytes and full universe membership are in the complete packet.':
    'Every published company and summary occurrence is retained. Legacy scores and forecasts are available in source details; missing acquisition history cannot be reconstructed. The legacy packet does not establish a complete company or analyst population.';
   render();
  }catch(e){$('status').textContent='Stored EPS research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,start,escape};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHEPSObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
