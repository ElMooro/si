/* Dated descriptive OHLCV observations with complete sources and explicit coverage. */
(function(root){
 'use strict';
 const CONTRACT='leader-price-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER?v:null;
 const decimal=v=>typeof v==='string'&&/^(0|[1-9]\d{0,29})(?:\.\d{1,12})?$/.test(v)?v:null;
 function source(ref){
  if(ref===undefined)return null;
  const r=obj(ref);if(Object.keys(r).sort().join(',')!=='bytes,format,key,sha256'||r.format!=='json'||typeof r.sha256!=='string'||!/^[a-f0-9]{64}$/.test(r.sha256)||!Number.isInteger(r.bytes)||r.bytes<0||r.bytes>8*1024*1024||r.key!=='data/momentum-leaders/sources/'+r.sha256+'.'+r.format)throw Error('Invalid declared public source identity');
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

 function model(p){
  p=obj(p);const out={native:p.measurement_contract===CONTRACT,requests:[],measurements:[],windows:[],universe:[]};
  function add(mode,v,raw,sources,pointer){out[mode].push({index:out[mode].length,ticker:'',name:'',date:'',value:null,unit:'',status:'',...v,raw,sources:sources.filter(Boolean),pointer});}
  if(out.native){
   for(const k of ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','independent_evidence_eligible','private_state_read_or_written'])if(p[k]!==false)throw Error('Research permissions invalid');
   if(p.call!==null)throw Error('Research call must abstain');
   const members=obj(p.universe_membership),inputs=obj(p.input_acquisitions),keys=['data/convergence-radar.json','data/ticker-trends.json'],us=[];
   if(Object.keys(inputs).sort().join('|')!==keys.join('|'))throw Error('Complete declared selection-source outcomes required');
   for(const k of keys){const a=obj(inputs[k]),s=source(a.original_ref);if(a.endpoint!==k||a.status==='received'&&!s||a.status!=='received'&&s)throw Error('Selection-source identity required');if(s)us.push(s);
    add('requests',{name:k,date:text(a.received_at),unit:'selection source',status:text(a.status)},a,[s],'/input_acquisitions/'+k.replace(/~/g,'~0').replace(/\//g,'~1'));}
   if(!Array.isArray(p.request_records)||!Array.isArray(members.occurrences)||!Array.isArray(members.selected)||members.selected.length!==p.request_records.length)throw Error('Complete acquisition and membership populations required');
   p.request_records.forEach((r,i)=>{
    if(r.request_index!==i||!Number.isInteger(r.universe_index)||r.universe_index<0||r.universe_index>=members.occurrences.length||members.selected[i].universe_index!==r.universe_index||members.selected[i].ticker!==r.ticker)throw Error('Request membership coordinate invalid');
    const a=obj(r.acquisition),o=obj(r.observations),s=source(a.original_ref),m=obj(o.measurements),pointer='/request_records/'+i;
    if(a.status==='received'&&!s||a.status!=='received'&&s)throw Error('Received source identity required');
    add('requests',{ticker:text(r.ticker),date:text(a.received_at),name:text(a.status),value:o.source_records??null,unit:'received source records',status:text(o.status)},r,[s,...us],pointer);
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
   members.occurrences.forEach((v,i)=>add('universe',{ticker:text(v.ticker),name:text(v.category)+' · '+text(v.input_key),value:v.selected===true?1:0,unit:'selected occurrence',status:text(v.status)},v,[source(obj(inputs[v.input_key]).original_ref)],'/universe_membership/occurrences/'+i));
   return out;
  }
  if(!Array.isArray(p.all_scored))throw Error('Dedicated Momentum Leaders packet unavailable');
  const populations=['all_scored','leaders','pump_confirmed'].filter(k=>Array.isArray(p[k])).map(k=>[k,p[k]]);
  for(const [key,values]of populations)values.forEach((v,i)=>{const r=obj(v);add('requests',{ticker:text(r.symbol)||text(r.ticker)||text(v),date:text(p.generated_at),name:'Legacy '+key,status:'Unqualified legacy score/tier; observation dates and acquisition coverage unavailable'},v,[],'/'+key+'/'+i);});
  return out;
 }
 function summary(p){const m=model(p);return{native:m.native,message:m.native?m.requests.length+' acquisition outcomes; '+m.measurements.length+' descriptive measurement records. Exact-date SPY comparisons remain descriptive. No qualified leadership, pump confirmation or trade signal.':m.requests.length+' legacy occurrences retained. Scores, tiers, source dates and coverage are unqualified.',generated_at:text(p.generated_at)||'unavailable'};}
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
   $('rows').textContent=rows.length+' matching / '+all.length+' received '+mode+' occurrences. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const cols=[['index','Occurrence'],['ticker','Literal ticker'],['name','Measurement / request'],['date','Observation / receipt date'],['value','Numeric value'],['unit','Unit'],['status','Scope and quality']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Complete evidence</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='index'?r.index+1:k==='value'?r.value===null?'Unavailable':r.value:r[k]||'Unavailable')+'</td>').join('')+'<td>'+r.sources.map(s=>'<button type="button" data-source-index="'+(links.push({ref:s.ref,pointer:r.pointer})-1)+'">Inspect whole original</button><details><summary>Source SHA-256</summary><code>'+s.sha256+'</code></details>').join('')+'<details><summary>Inspect '+esc(r.pointer)+'</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   $('board').querySelectorAll('button[data-source-index]').forEach(button=>{button.onclick=()=>{const value=links[Number(button.dataset.sourceIndex)];if(value)inspect(value.ref,value.pointer);};});
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{const received=await api.load('/data/momentum-leaders.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);const s=summary(p);
   $('status').textContent=s.message+' Generated '+s.generated_at+'. Collection time is distinct from the last price observation.';
   $('coverage').textContent='Inspect all selected request outcomes, complete original universe occurrences, explicit observation windows and calculation definitions. Duplicate memberships do not create independent evidence.';render();
  }catch(e){$('status').textContent='Stored price research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,summary,start,source,loadSource,escape:esc};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHLeaderObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
