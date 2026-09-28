/* Dated descriptive OHLCV observations with complete sources and explicit coverage. */
(function(root){
 'use strict';
 const CONTRACT='momentum-price-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER?v:null;
 const decimal=v=>typeof v==='string'&&/^(0|[1-9]\d{0,29})(?:\.\d{1,12})?$/.test(v)?v:null;
 function source(ref){
  if(ref===undefined)return null;
  const r=obj(ref);if(Object.keys(r).sort().join(',')!=='bytes,format,key,sha256'||r.format!=='json'||typeof r.sha256!=='string'||!/^[a-f0-9]{64}$/.test(r.sha256)||!Number.isInteger(r.bytes)||r.bytes<0||r.bytes>8*1024*1024||r.key!=='data/momentum-breakout/sources/'+r.sha256+'.'+r.format)throw Error('Invalid declared public source identity');
  return {href:'/'+r.key,bytes:r.bytes,sha256:r.sha256,ref:r};
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

 function progress(p){
  const g=p.acquisition_progress;if(g===undefined){if(p.version==='2.1.0')throw Error('Acquisition progress missing');return null;}
  const selected=p.universe_membership.selected,counts=new Map(),keys=selected.map(r=>{const n=counts.get(r.ticker)||0;counts.set(r.ticker,n+1);return JSON.stringify([r.ticker,n]);});
  const integer=v=>Number.isSafeInteger(v)&&v>=0,valid=i=>typeof selected[i]?.ticker==='string'&&/^[A-Z0-9][A-Z0-9.\-^]{0,24}$/.test(selected[i].ticker);
  if(!g||g.contract!=='momentum-acquisition-progress.v1'||!['initial_population','resume_prior_unattempted_occurrences','resume_remaining_occurrences','previous_cycle_complete_or_retired'].includes(g.plan_reason)
    ||g.occurrence_identity!=='ticker label and duplicate ordinal; not legal-security identity'||g.request_order_is_rank!==false||g.schedule_accelerated!==false||g.visit_does_not_mean_success!==true||g.whole_universe_current_coverage_verified!==false
    ||typeof g.cycle_complete!=='boolean'||!['visited_occurrences','pending_occurrences','retained_source_bytes','original_dispatch_byte_budget'].every(k=>integer(g[k]))||g.original_dispatch_byte_budget!==96*1024*1024
    ||!Array.isArray(g.planned_request_indices)||!Array.isArray(g.visited_request_indices)||!Array.isArray(g.planned_occurrence_keys)||!Array.isArray(g.remaining_occurrence_keys))throw Error('Invalid acquisition progress contract');
  const planned=g.planned_request_indices,visited=g.visited_request_indices,done=new Set(visited);
  if(planned.some(i=>!integer(i)||i>=selected.length||!valid(i))||new Set(planned).size!==planned.length||visited.some(i=>!integer(i))||done.size!==visited.length
    ||JSON.stringify(g.planned_occurrence_keys)!==JSON.stringify(planned.map(i=>keys[i]))||JSON.stringify(visited)!==JSON.stringify(planned.filter(i=>done.has(i))))throw Error('Acquisition progress coordinates differ');
  const pending=planned.filter(i=>!done.has(i)).map(i=>keys[i]);
  const actual=p.request_records.map((r,i)=>valid(i)&&r.acquisition.status!=='not_attempted_runtime_rate_or_size_limit'?i:null).filter(i=>i!==null);
  if(g.visited_occurrences!==visited.length||g.pending_occurrences!==pending.length||g.cycle_complete!==(pending.length===0)||JSON.stringify(pending)!==JSON.stringify(g.remaining_occurrence_keys)
    ||actual.length!==visited.length||actual.some(i=>!done.has(i))||!['planned_window_complete','runtime_reserve','source_byte_budget','provider_denial_or_rate_limit'].includes(g.stop_reason)
    ||(g.stop_reason==='planned_window_complete')!==g.cycle_complete)throw Error('Acquisition progress and actual outcomes differ');
  const refs=new Map();for(const a of [p.universe_acquisition,p.benchmark.acquisition,...p.request_records.map(r=>r.acquisition)])if(a.original_ref){const ref=source(a.original_ref);refs.set(ref.href,ref.bytes);}
  const bytes=[...refs.values()].reduce((a,b)=>a+b,0);if(bytes!==g.retained_source_bytes||bytes!==p.retained_unique_source_bytes||bytes>160*1024*1024)throw Error('Retained source coverage differs');
  return {visited:visited.length,pending:pending.length,selected:selected.length,planned:planned.length,stop:g.stop_reason,cycle_complete:g.cycle_complete};
 }
 function model(p){
  p=obj(p);const out={native:p.measurement_contract===CONTRACT,requests:[],measurements:[],windows:[],universe:[]};
  function add(mode,v,raw,sources,pointer){out[mode].push({index:out[mode].length,ticker:'',name:'',date:'',value:null,unit:'',status:'',...v,raw,sources:sources.filter(Boolean),pointer});}
  if(out.native){
   for(const k of ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','independent_evidence_eligible','private_state_read_or_written'])if(p[k]!==false)throw Error('Research permissions invalid');
   if(p.call!==null)throw Error('Research call must abstain');
   const members=obj(p.universe_membership),u=source(obj(p.universe_acquisition).original_ref);
   if(!Array.isArray(p.request_records)||!Array.isArray(members.occurrences)||!Array.isArray(members.selected)||members.selected.length!==p.request_records.length)throw Error('Complete acquisition and membership populations required');
   p.request_records.forEach((r,i)=>{
    if(r.request_index!==i||!Number.isInteger(r.universe_index)||r.universe_index<0||r.universe_index>=members.occurrences.length||members.selected[i].universe_index!==r.universe_index||members.selected[i].ticker!==r.ticker)throw Error('Request membership coordinate invalid');
    const a=obj(r.acquisition),o=obj(r.observations),s=source(a.original_ref),m=obj(o.measurements),pointer='/request_records/'+i;
    if(a.status==='received'&&!s||a.status!=='received'&&s)throw Error('Received source identity required');
    add('requests',{ticker:text(r.ticker),date:text(a.received_at),name:text(a.status),value:o.source_records??null,unit:'received source records',status:text(o.status)},r,[s,u],pointer);
    if(Object.keys(m).length){
     add('windows',{ticker:text(r.ticker),date:text(m.first_date)+' → '+text(m.last_date),name:'Selected observation window',value:m.observations??null,unit:'reported observations',status:text(m.status)+'; currency and session calendar unverified'},o,[s],pointer+'/observations');
     if(m.status==='descriptive_observations')for(const [key,v]of Object.entries(m)){
      if(['status','observations','first_date','last_date','method_notes'].includes(key))continue;
      const metric=obj(v);add('measurements',{ticker:text(r.ticker),date:text(m.last_date),name:key,value:number(metric.value),unit:text(metric.unit),status:typeof metric.value==='boolean'?'Reported descriptive predicate: '+String(metric.value):metric.value===null?'Unavailable denominator/window; inspect details':'Descriptive only; inspect definition and window'},v,[s],pointer+'/observations/measurements/'+key);
     }
    }
    for(const [length,v]of Object.entries(obj(o.benchmark_comparisons))){
     const b=source(obj(obj(p.benchmark).acquisition).original_ref),metric=obj(v);
     add('measurements',{ticker:text(r.ticker),date:text(metric.start_date)+' → '+text(metric.end_date),name:'Stock minus SPY local-price change: '+length+' observation intervals',value:number(metric.value),unit:text(metric.unit),status:text(metric.status)+'; not verified tradable excess return'},v,[s,b],pointer+'/observations/benchmark_comparisons/'+length);
    }
    if(o.selected_indices!==undefined){if(!Array.isArray(o.selected_indices)||!o.selected_indices.every(n=>Number.isInteger(n)&&n>=0&&n<o.source_records))throw Error('Selected source coordinate invalid');}
   });
   const b=obj(p.benchmark),ba=obj(b.acquisition),bo=obj(b.observations),bs=source(ba.original_ref);
   if(b.ticker!=='SPY'||ba.status==='received'&&!bs||ba.status!=='received'&&bs)throw Error('Benchmark acquisition required');
   add('requests',{ticker:'SPY',name:'Benchmark '+text(ba.status),date:text(ba.received_at),value:bo.source_records??null,unit:'received source records',status:text(bo.status)},b,[bs],'/benchmark');
   add('windows',{ticker:'SPY',name:'Benchmark selected window',value:Array.isArray(bo.selected_indices)?bo.selected_indices.length:null,unit:'reported observations',status:text(bo.status)},bo,[bs],'/benchmark/observations');
   members.occurrences.forEach((v,i)=>add('universe',{ticker:text(v.ticker),name:text(obj(v.raw).name),value:v.selected===true?1:0,unit:'selected occurrence',status:text(v.status)},v,[u],'/universe_membership/occurrences/'+i));
   out.progress=progress(p);return out;
  }
  if(!Array.isArray(p.all_qualifying))throw Error('Dedicated momentum packet unavailable');
  const populations=[['all_qualifying',p.all_qualifying],...Object.entries(obj(p.summary)).filter(([,v])=>Array.isArray(v)).map(([k,v])=>['summary/'+k,v])];
  for(const [key,values]of populations)values.forEach((v,i)=>{const r=obj(v);add('requests',{ticker:text(r.symbol)||text(r.ticker)||text(v),date:text(p.generated_at),name:'Legacy '+key,status:'Unqualified legacy score/tier; observation dates and acquisition coverage unavailable'},v,[],'/'+key+'/'+i);});
  return out;
 }
 function summary(p){const m=model(p);return{native:m.native,message:m.native?m.requests.length+' acquisition outcomes; '+m.measurements.length+' descriptive measurement records. Exact-date SPY comparisons remain descriptive. No qualified breakout or trade signal.':m.requests.length+' legacy occurrences retained. Scores, tiers, source dates and coverage are unqualified.',generated_at:text(p.generated_at)||'unavailable'};}
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={requests:[],measurements:[],windows:[],universe:[]},page=0,key='index',direction=1,sourceRequest=0;
  async function inspect(ref,pointer){
   const request=++sourceRequest;$('source-original').textContent='';$('source-status').textContent='Verifying whole original for '+pointer;$('source-status').scrollIntoView({block:'center'});
   try{const received=await loadSource(ref);if(request!==sourceRequest)return;$('source-status').textContent='Verified '+received.bytes+' bytes; SHA-256 '+received.sha256+'; '+pointer;$('source-original').textContent=received.raw;}
   catch(e){if(request===sourceRequest)$('source-status').textContent='Original source unavailable: '+e.message;}
  }
  function render(){
   sourceRequest++;$('source-status').textContent='Select Inspect whole original to verify every source row.';$('source-original').textContent='';const links=[];
   const mode=$('mode').value||'requests',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const rows=all.filter(r=>[r.ticker,r.name,r.date,r.unit,r.status,r.pointer].join(' ').toLowerCase().includes(query));
   rows.sort((a,b)=>api.compare(a,b,key,direction,['index','value'].includes(key)?'number':'text')||a.index-b.index);
   const pages=Math.max(1,Math.ceil(rows.length/100));page=Math.min(Math.max(page,0),pages-1);const shown=rows.slice(page*100,(page+1)*100);
   $('rows').textContent=rows.length+' matching / '+all.length+' '+(mode==='requests'?(data.native?'selected and benchmark request':'legacy'):mode==='universe'?'source universe':mode==='windows'?'observation window':'descriptive measurement')+' occurrences. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const cols=[['index','Occurrence'],['ticker','Literal ticker'],['name','Measurement / request'],['date','Observation / receipt date'],['value','Numeric value'],['unit','Unit'],['status','Scope and quality']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Complete evidence</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='index'?r.index+1:k==='value'?r.value===null?'Unavailable':r.value:r[k]||'Unavailable')+'</td>').join('')+'<td>'+r.sources.map(s=>'<button type="button" data-source-index="'+(links.push({ref:s.ref,pointer:r.pointer})-1)+'">Inspect whole original</button><details><summary>Source SHA-256</summary><code>'+s.sha256+'</code></details>').join('')+'<details><summary>Inspect '+esc(r.pointer)+'</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   $('board').querySelectorAll('button[data-source-index]').forEach(button=>{button.onclick=()=>{const value=links[Number(button.dataset.sourceIndex)];if(value)inspect(value.ref,value.pointer);};});
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{const received=await api.load('/data/momentum-breakout.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);const s=summary(p);
   $('status').textContent=s.message+' Generated '+s.generated_at+'. Collection time is distinct from the last price observation.';
   $('coverage').textContent=(data.progress?data.progress.visited+' selected request occurrences visited this run; '+data.progress.pending+' remain in the selected visit cycle. Stop: '+data.progress.stop+'. Selected requests resume on the original schedule; the SPY benchmark is acquired separately each run. A visit is not a successful response; a completed visit cycle is not simultaneous whole-market coverage. Unvisited observations are not refreshed. ':'')+'Inspect all selected request outcomes, complete original universe occurrences, explicit observation windows and calculation definitions. Duplicate memberships do not create independent evidence.';render();
  }catch(e){$('status').textContent='Stored price research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,summary,start,source,loadSource,escape:esc};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHMomentumObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
