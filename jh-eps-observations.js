/* Forecast targets, snapshot comparisons and rating opinions are distinct populations. */
(function(root){
 'use strict';
 const CONTRACT='eps-target-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function source(ref){
  if(ref===undefined||ref===null)return null;
  const r=obj(ref);if(Object.keys(r).sort().join(',')!=='bytes,key,sha256'||typeof r.sha256!=='string'||!/^[a-f0-9]{64}$/.test(r.sha256)||!Number.isInteger(r.bytes)||r.bytes<1||r.bytes>256*1024||r.key!=='data/eps-revision-velocity/sources/'+r.sha256+'.json')throw Error('Invalid owned estimate source identity');
  return {href:'/'+r.key,bytes:r.bytes,sha256:r.sha256,ref:r};
 }
 function comparisonSource(evidence,ticker){
  if(evidence===undefined)return null;
  const e=obj(evidence),d=e.baseline;
  if(!['received','unavailable','not_read_runtime_reserve','not_needed_no_current_estimate','no_retained_baseline'].includes(e.status))throw Error('Unknown comparison source status');
  if(e.status==='no_retained_baseline'){if(d!==null)throw Error('Absent comparison source differs');return null;}
  if(!d||Object.keys(d).sort().join(',')!=='endpoint,original_ref,received_at,ticker'||d.ticker!==ticker||d.endpoint!=='analyst-estimates'||typeof d.received_at!=='string'||!/^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:\d{2})$/.test(d.received_at)||!Number.isFinite(Date.parse(d.received_at)))throw Error('Complete retained estimate descriptor required');
  const retained=source(d.original_ref);if(!retained)throw Error('Complete retained estimate source required');
  return {...retained,received_at:d.received_at,status:e.status};
 }
 async function loadSource(ref,options={}){
  const declared=source(ref);if(!declared)throw Error('Original source reference required');
  const controller=new AbortController();let reader,timer;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Original source timed out'));},options.timeout??15000);});
  try{return await Promise.race([deadline,(async()=>{
   const response=await(options.fetcher||root.fetch.bind(root))(declared.href+'?exact=1&nogen=1',{cache:'no-store',credentials:'omit',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Whole original unavailable (HTTP '+response.status+')');
   reader=response.body.getReader();const chunks=[];let count=0;
   for(;;){const{done,value}=await reader.read();if(done)break;count+=value.byteLength;if(count>declared.bytes)throw Error('Original source byte count differs');chunks.push(value);}
   if(count!==declared.bytes)throw Error('Original source byte count differs');
   const bytes=new Uint8Array(count);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
   const digest=Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),v=>v.toString(16).padStart(2,'0')).join('');
   if(digest!==declared.sha256)throw Error('Original source SHA-256 differs');
   return {raw:new TextDecoder('utf-8',{fatal:true}).decode(bytes),...declared};
  })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 function coverage(p){
  const g=p.acquisition_progress;if(g===undefined){if(p.version==='1.3.0')throw Error('Acquisition progress required');return null;}
  const selected=obj(p.universe_membership).selected_symbols,records=p.request_records;
  if(!g||g.contract!=='eps-acquisition-progress.v1'||!Array.isArray(selected)||selected.length>500||new Set(selected).size!==selected.length||!selected.every(v=>typeof v==='string'&&/^[A-Z0-9][A-Z0-9.\-]{0,19}$/.test(v))||!Array.isArray(records)||records.length!==selected.length)throw Error('Complete selected acquisition population required');
  const planned=g.planned_symbols,indices=g.planned_request_indices,visited=g.visited_request_indices,pending=g.remaining_symbols;
  if(!Array.isArray(planned)||!Array.isArray(indices)||!Array.isArray(visited)||!Array.isArray(pending)||new Set(planned).size!==planned.length||!planned.every(v=>selected.includes(v))||indices.length!==planned.length||!indices.every((n,i)=>Number.isInteger(n)&&selected[n]===planned[i])||visited.length>indices.length||!visited.every((n,i)=>Number.isInteger(n)&&n===indices[i])||JSON.stringify(pending)!==JSON.stringify(planned.slice(visited.length)))throw Error('Exact acquisition progress partition required');
  if(g.visited_occurrences!==visited.length||g.pending_occurrences!==pending.length||g.cycle_complete!==(pending.length===0)||g.visited_is_not_successful_provider_response!==true||g.request_order_is_rank!==false||g.original_selected_population_order_preserved!==true||g.cycle_scope!=='Existing selected request cap only; not the whole source universe')throw Error('Acquisition coverage counters differ');
  if(!['planned_window_complete','runtime_reserve','source_byte_budget','provider_rate_limit','provider_authorization_error'].includes(g.stop_reason)||g.stop_reason==='planned_window_complete'&&pending.length)throw Error('Acquisition stop reason differs');
  let bytes=0,received=0;
  records.forEach((r,i)=>{if(r.ticker!==selected[i]||r.request_index!==i||!Array.isArray(r.acquisitions)||r.acquisitions[0]?.endpoint!=='quote'||typeof r.acquisitions[0].status!=='string'||(r.acquisitions[0].status!=='not_attempted_runtime_rate_or_size_limit')!==visited.includes(i))throw Error('Acquisition outcomes and progress differ');
   for(const a of r.acquisitions){const n=a.original_bytes??0;if(!Number.isSafeInteger(n)||n<0)throw Error('Whole original response byte accounting required');bytes+=n;}
   if(obj(r.source_population_status)['analyst-estimates']==='received_array')received++;
  });
  if(bytes!==g.retained_provider_bytes)throw Error('Acquisition response byte accounting differs');
  return {selected:selected.length,visited:visited.length,pending:pending.length,received,
   message:visited.length+' of '+selected.length+' selected request records visited this run; '+pending.length+' remain in the current visit cycle. Annual-estimate arrays received: '+received+'. A completed visit cycle is not complete data coverage; the original selected cap still applies. Stop: '+g.stop_reason+'.'};
 }
 function model(p){
  const out={targets:[],ratings:[],requests:[],native:p.measurement_contract===CONTRACT};
  if(out.native){
   if(!Array.isArray(p.request_records))throw Error('Complete request population required');
   out.coverage=coverage(p);
   p.request_records.forEach((value,requestIndex)=>{
    const r=obj(value);
    for(const k of ['acquisitions','quote_records','estimate_observations','same_target_comparisons','rating_observations'])if(!Array.isArray(r[k]))throw Error('Complete source population required: '+k);
    if(r.estimate_observations.length!==r.same_target_comparisons.length||new Set(r.estimate_observations.map(e=>obj(e).source_index)).size!==r.estimate_observations.length)throw Error('Complete unique comparison population required');
    const priorSource=comparisonSource(r.comparison_source,r.ticker);
    const acquisitions=r.acquisitions.map(a=>{const{original_base64,...metadata}=obj(a);return metadata;});
    out.requests.push({ticker:text(r.ticker),index:requestIndex,target:'',value:r.estimate_observations.length,unit:'forecast records',change:r.rating_observations.length,
     action:r.acquisitions.map(a=>text(obj(a).endpoint)+': '+text(obj(a).status)).join(' / '),status:'Research acquisition; no investment authority',
     source:priorSource,raw:{comparison_source:r.comparison_source,request_index:requestIndex,source_population_status:r.source_population_status,quote_records:r.quote_records,acquisitions},pointer:'/request_records/'+requestIndex});
    r.estimate_observations.forEach((row,i)=>{
     const e=obj(row),v=obj(e.values),valid=e.measurement_status==='reported_forecast_observation',matches=r.same_target_comparisons.filter(c=>obj(c).source_index===e.source_index);
     if(matches.length!==1)throw Error('Unique same-target comparison coordinate required');
     const comparison=matches[0];
     if(r.comparison_source&&r.comparison_source.status!=='received'&&comparison.eps_change!==null)throw Error('Comparison lacks a received original');
     out.targets.push({ticker:text(r.ticker),reported:text(obj(e.raw).symbol),index:out.targets.length,target:text(e.target_period_end),
      value:valid?v.epsAvg:null,unit:text(e.reported_currency)||'Unreported',change:valid?comparison.eps_change:null,
      action:text(e.target_status),status:text(e.measurement_status),source:priorSource,raw:{observation:row,comparison,comparison_source:r.comparison_source,acquisition_metadata:acquisitions},
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
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={targets:[],ratings:[],requests:[]},page=0,key='index',direction=1,loaded=false,sourceRequest=0;
  async function inspect(ref,pointer){
   const request=++sourceRequest;$('source-original').textContent='';$('source-status').textContent='Verifying complete retained original for '+pointer;$('source-status').scrollIntoView({block:'center'});
   try{const received=await loadSource(ref);if(request!==sourceRequest)return;$('source-status').textContent='Verified '+received.bytes+' bytes; SHA-256 '+received.sha256+'; '+pointer;$('source-original').textContent=received.raw;}
   catch(e){if(request===sourceRequest)$('source-status').textContent='Retained source unavailable: '+e.message;}
  }
  function render(){
   if(!loaded)return;
   sourceRequest++;$('source-status').textContent='Select a retained original beside a comparison to verify its complete bytes.';$('source-original').textContent='';const links=[];
   const mode=$('mode').value||'targets',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const list=all.filter(r=>[r.ticker,r.reported,r.target,r.action,r.status,r.pointer].join(' ').toLowerCase().includes(query));
   const numeric=mode==='ratings'?['index']:['index','value','change'];list.sort((a,b)=>api.compare(a,b,key,direction,numeric.includes(key)?'number':'text'));
   const pages=Math.max(1,Math.ceil(list.length/100));page=Math.min(Math.max(page,0),pages-1);const shown=list.slice(page*100,(page+1)*100);
   $('rows').textContent=list.length+' matching / '+all.length+' '+(mode==='requests'?'selected request occurrences':mode==='targets'?'received forecast records':'received rating records')+'. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const fields=mode==='ratings'?[['target','Rating date'],['value','Previous grade'],['unit','New grade'],['change','Grading company'],['action','Reported action']]:
    mode==='requests'?[['value','Forecast records'],['change','Rating records'],['action','Acquisition outcomes']]:[['target','Annual target end'],['value','Reported EPS estimate'],['unit','Reported currency'],['change','Same-target EPS change'],['action','Target timing']];
   const cols=[['index','Occurrence'],['ticker','Requested ticker'],...fields,['status','Qualification']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Source record and evidence</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+escape(k==='index'?r.index+1:numeric.includes(k)?(api.format(r[k],mode==='requests'?0:3)||'Unavailable'):r[k])+'</td>').join('')+'<td><details><summary>Inspect '+escape(r.pointer)+'</summary><pre>'+escape(JSON.stringify(r.raw,null,2))+'</pre></details>'+(r.source?'<p>Prior received '+escape(r.source.received_at)+' · '+escape(r.source.status)+'</p><button type="button" data-source-index="'+(links.push({ref:r.source.ref,pointer:r.pointer})-1)+'">Inspect complete retained original</button>':'')+'</td></tr>').join('')+'</tbody></table>';
   $('board').querySelectorAll('button[data-source-index]').forEach(button=>{button.onclick=()=>{const item=links[+button.dataset.sourceIndex];inspect(item.ref,item.pointer);};});
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{
   const received=await api.load('/data/eps-revision-velocity.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);
   $('status').textContent=(data.native?'Source observations; no qualified investment signal. ':'Legacy output: revision, tier and outperformance claims are unverified. ')+
    'Generated '+(text(p.generated_at)||'unavailable')+'. This is a collection clock, not an estimate target or original release date.';
   $('coverage').textContent=data.native?data.requests.length+' request records, '+data.targets.length+' annual forecast records and '+data.ratings.length+' rating records. Inspect acquisition coverage separately; unattempted and failed requests remain explicit. Original response bytes and full universe membership are in the complete packet.':
    'Every published company and summary occurrence is retained. Legacy scores and forecasts are available in source details; missing acquisition history cannot be reconstructed. The legacy packet does not establish a complete company or analyst population.';
   if(data.coverage)$('coverage').textContent+=' '+data.coverage.message;
   loaded=true;render();
  }catch(e){loaded=false;data={targets:[],ratings:[],requests:[]};sourceRequest++;$('source-original').textContent='';$('status').textContent='Stored EPS research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,start,escape,source,loadSource,comparisonSource,coverage};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHEPSObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
