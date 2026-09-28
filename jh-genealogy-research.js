/* Public collection chronology. Stored source labels are text, never fetch targets. */
(function(root){
 'use strict';
 const PREFIX='data/signal-genealogy-research/',HEAD=PREFIX+'current.json',LIMIT=64*1024*1024;
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 const count=x=>Number.isSafeInteger(x)&&x>=0;
 const hex=x=>typeof x==='string'&&/^[a-f0-9]{64}$/.test(x);
 const escape=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const need=(v,m)=>{if(!v)throw new Error(m);};
 const canonical=x=>JSON.stringify(sort(x));
 function sort(x){return Array.isArray(x)?x.map(sort):object(x)?Object.fromEntries(Object.keys(x).sort().map(k=>[k,sort(x[k])])):x;}
 const orders={a_observed_presence_before_b_bounded_entry:'A observed before B’s bounded entry',b_observed_presence_before_a_bounded_entry:'B observed before A’s bounded entry',unresolved_prior_observation_missing:'Unresolved: prior observation missing',unresolved_initial_membership_ambiguity:'Unresolved: initial membership ambiguous',overlapping_observation_intervals:'Overlapping observation intervals'};
 const coverage=['captures','complete_capture_scans','partial_capture_scans','registered_records','retained_records','excluded_records','ineligible_source_snapshots','identity_partial_source_snapshots','first_observed_groups','possible_comparisons'];
 function clock(x){
  need(typeof x==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$/.test(x),'UTC timestamp missing or malformed');
  const n=Date.parse(x);need(Number.isFinite(n)&&new Date(n).toISOString().slice(0,19)===x.slice(0,19),'Invalid calendar timestamp');return n;
 }
 function authority(p){need(object(p.authority)&&['calls_eligible','execution_eligible','forecast_qualified','sizing_eligible'].every(k=>p.authority[k]===false)&&p.independent_evidence_count===null&&p.original_engine_replay_verified===false,'Unexpected research authority');}
 function ref(r,kind){need(['runs','artifacts','compilers'].includes(kind)&&object(r)&&Object.keys(r).sort().join(',')==='bytes,key,sha256'&&count(r.bytes)&&r.bytes>0&&r.bytes<=LIMIT&&hex(r.sha256)&&r.key===PREFIX+kind+'/'+r.sha256+(kind==='compilers'?'.py':'.json'),'Invalid retained artifact reference');return '/'+r.key;}
 function model(p){
  need(object(p)&&p.contract==='genealogy-research-head.v1'&&p.engine==='signal-genealogy'&&p.version==='2.0.0','Reviewed research publication unavailable');
  authority(p);clock(p.generated_at);ref(p.replay,'runs');need(hex(p.input_sha256),'Input identity missing');
  need(object(p.coverage)&&coverage.every(k=>count(p.coverage[k])),'Complete typed coverage required');
  const c=p.coverage;need(c.captures===c.complete_capture_scans+c.partial_capture_scans&&c.registered_records===c.retained_records+c.excluded_records,'Coverage totals disagree');
  need(Array.isArray(p.comparisons)&&p.comparisons.length===c.possible_comparisons&&object(p.comparison_status_counts),'Comparison population missing');
  const counts={},seen=new Set();
  for(const row of p.comparisons){
   need(object(row)&&typeof row.instrument_id==='string'&&/^equity:US:[A-Z][A-Z.\-]{0,6}$/.test(row.instrument_id)&&['UP','DOWN'].includes(row.direction),'Invalid comparison identity');
   need(['source_a','source_b'].every(k=>typeof row[k]==='string'&&/^data\/[A-Za-z0-9_.-]+\.json$/.test(row[k]))&&row.source_a<row.source_b,'Invalid source identity');
   const identity=canonical([row.instrument_id,row.direction,row.source_a,row.source_b]);need(!seen.has(identity),'Repeated comparison identity');seen.add(identity);
   need(Object.hasOwn(orders,row.interval_order)&&row.causal_order_qualified===false&&row.independent_evidence_count===null,'Unexpected comparison authority');
   for(const side of ['a','b']){clock(row[side+'_upper_inclusive_utc']);if(row[side+'_lower_exclusive_utc']!==null)clock(row[side+'_lower_exclusive_utc']);}
   need(Array.isArray(row.shared_presence_capture_keys)&&row.shared_presence_capture_keys.every(k=>typeof k==='string'&&/^data\/research-forecasts\/captures\/[a-f0-9]{64}\.json$/.test(k)),'Invalid capture references');
   counts[row.interval_order]=(counts[row.interval_order]||0)+1;
  }
  need(canonical(counts)===canonical(p.comparison_status_counts),'Comparison status totals disagree');
  return {packet:p,rows:p.comparisons.map((row,index)=>({row,path:'/comparisons/'+index})),coverage:c};
 }
 function archiveModel(output,head){
  model(head);need(object(output)&&output.contract==='signal-genealogy-research.v2'&&output.generated_at===head.generated_at&&output.input_sha256===head.input_sha256,'Retained calculation identity differs');authority(output);
  const r=output.registration,m=output.membership;need(object(r)&&object(m),'Complete retained populations missing');
  const arrays=[['registration/first_registrations',r.first_registrations,head.coverage.first_observed_groups],['registration/records',r.records,head.coverage.retained_records],['registration/exclusions',r.exclusions,head.coverage.excluded_records],['membership/first_observations',m.first_observations,head.coverage.first_observed_groups],['membership/intervals',m.intervals,null],['registration/same_instrument_comparisons',r.same_instrument_comparisons,head.coverage.possible_comparisons]];
  need(canonical(m.comparisons)===canonical(head.comparisons),'Retained comparisons differ from current summary');
  return Object.fromEntries(arrays.map(([path,rows,total])=>{need(Array.isArray(rows)&&(total===null||rows.length===total)&&rows.every(object),'Retained population incomplete: '+path);return [path,rows.map((row,index)=>({row,path:'/'+path+'/'+index}))];}));
 }
 function windowRows(rows,query='',page=0,size=50){
  need(Array.isArray(rows)&&count(page)&&count(size)&&size>0,'Invalid page request');
  const q=query.trim().toLocaleLowerCase(),filtered=q?rows.filter(x=>canonical(x.row).toLocaleLowerCase().includes(q)):rows;
  const pages=Math.max(1,Math.ceil(filtered.length/size)),current=Math.min(page,pages-1);
  return {rows:filtered.slice(current*size,(current+1)*size),total:filtered.length,all:rows.length,page:current,pages,start:filtered.length?current*size+1:0,end:Math.min((current+1)*size,filtered.length)};
 }
 function table(result){
  const labels=['Instrument / direction','Source A / source B','Observed interval A (UTC)','Observed interval B (UTC)','Collection order / source status','Original coordinate and record'];
  let html='<table><thead><tr>'+labels.map(x=>'<th scope="col">'+x+'</th>').join('')+'</tr></thead><tbody>';
  for(const {row:r,path} of result.rows){
   const interval=side=>r[side+'_upper_inclusive_utc']?'('+ (r[side+'_lower_exclusive_utc']??'prior observation unavailable')+', '+r[side+'_upper_inclusive_utc']+']':side==='a'&&r.upper_inclusive_utc?'('+(r.lower_exclusive_utc??'prior observation unavailable')+', '+r.upper_inclusive_utc+']':side==='a'&&r.registered_at?'Registration: '+r.registered_at:'Not applicable to this record';
   const status=orders[r.interval_order]||r.status||r.registration_order||(Array.isArray(r.reasons)?r.reasons.join('; '):'Recorded observation');
   const cells=[(r.instrument_id||'Unavailable')+' / '+(r.direction||'Unavailable'),(r.source_a||r.source_key||'Unavailable')+(r.source_b?' / '+r.source_b:''),interval('a'),interval('b'),status];
   html+='<tr>'+cells.map(x=>'<td>'+escape(x)+'</td>').join('')+'<td><details><summary>'+escape(path)+'</summary><pre tabindex="0">'+escape(JSON.stringify(r,null,2))+'</pre></details></td></tr>';
  }
  return html+'</tbody></table>'+(result.total?'':'<p>No matching records. No missing source is inferred to be inactive.</p>');
 }
 async function read(path,fetcher=root.fetch,signal){
  const response=await fetcher('/'+path+'?exact=1&nogen=1',{cache:'no-store',signal});
  need(response.ok,'Publication unavailable (HTTP '+response.status+')');
  let raw;
  if(response.body?.getReader){
   const reader=response.body.getReader(),parts=[];let total=0;
   try{for(;;){const value=await reader.read();if(value.done)break;total+=value.value.byteLength;need(total<=LIMIT,'Whole artifact exceeds supported size');parts.push(value.value);}raw=new Uint8Array(total);let offset=0;for(const p of parts){raw.set(p,offset);offset+=p.length;}}
   catch(error){try{await reader.cancel();}catch(_){}throw error;}finally{reader.releaseLock();}
  }else{raw=new Uint8Array(await response.arrayBuffer());need(raw.length<=LIMIT,'Whole artifact exceeds supported size');}
  const text=new TextDecoder('utf-8',{fatal:true}).decode(raw);let packet;
  try{packet=JSON.parse(text);}catch(_){const error=new Error('Received publication is not valid JSON');error.original=text;throw error;}
  return {raw,text,packet};
 }
 async function retained(reference,kind,fetcher,signal){
  ref(reference,kind);const result=await read(reference.key,fetcher,signal);
  const hash=Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',result.raw)),x=>x.toString(16).padStart(2,'0')).join('');
  need(result.raw.length===reference.bytes&&hash===reference.sha256,'Whole retained artifact bytes differ');return result;
 }
 async function loadArchive(head,fetcher,signal){
  model(head);const manifest=await retained(head.replay,'runs',fetcher,signal),m=manifest.packet;
  need(m.contract==='genealogy-streamed-replay.v1'&&m.cutoff===head.generated_at&&object(m.files)&&Object.keys(m.files).sort().join(',')==='head,inputs,membership,output','Retained run contract differs');
  for(const file of Object.values(m.files))ref(file,'artifacts');
  need(m.files.inputs.sha256===head.input_sha256,'Retained input identity differs');
  const output=await retained(m.files.output,'artifacts',fetcher,signal);return {manifest,output,populations:archiveModel(output.packet,head)};
 }
 function mount(doc=root.document,fetcher=root.fetch){
  const el=id=>doc.getElementById('genealogy-'+id),state={epoch:0,head:null,groups:{},page:0,controller:null};
  function abort(){state.epoch++;state.controller?.abort();state.controller=null;}
  function clear(){state.head=null;state.groups={};state.page=0;el('board').textContent='No reviewed publication loaded.';for(const id of ['coverage','clock','evidence','page','archive-original','run-original'])el(id).textContent='';el('population').innerHTML='<option value="comparisons">Membership comparisons</option>';el('population').value='comparisons';el('query').value='';for(const id of ['population','query','previous','next','archive'])el(id).disabled=true;}
  function paint(){
   const rows=state.groups[el('population').value]||[],result=windowRows(rows,el('query').value,state.page);state.page=result.page;
   el('board').innerHTML=table(result);el('page').textContent=result.start+'–'+result.end+' of '+result.total+' matching records ('+result.all+' in population). Page '+(result.page+1)+' of '+result.pages+'.';el('previous').disabled=result.page===0;el('next').disabled=result.page+1>=result.pages;
  }
  async function operation(work){abort();const epoch=state.epoch,controller=new AbortController();state.controller=controller;const timeout=setTimeout(()=>controller.abort(),60000);try{await work(epoch,controller.signal);}catch(error){if(epoch===state.epoch)el('status').textContent='Unavailable: '+error.message+'. No legacy result is substituted.';}finally{clearTimeout(timeout);if(epoch===state.epoch)state.controller=null;}}
  async function refresh(){return operation(async(epoch,signal)=>{clear();el('original').textContent='Unavailable';el('status').textContent='Loading reviewed public chronology…';let received;
   try{received=await read(HEAD,fetcher,signal);}catch(error){if(epoch===state.epoch&&error.original)el('original').textContent=error.original;throw error;}
   if(epoch!==state.epoch)return;el('original').textContent=received.text;const view=model(received.packet);state.head=view.packet;state.groups={comparisons:view.rows};
   el('clock').textContent='Storage cutoff: '+state.head.generated_at+'. This is collection time, not the time a market signal began.';
   el('coverage').textContent=coverage.map(k=>k.replaceAll('_',' ')+': '+view.coverage[k].toLocaleString()).join(' · ');
   el('status').textContent='Recorded observations only. Forecast, Calls, execution and sizing permissions are off.';
   el('evidence').textContent='Whole publication retained below. Load the retained calculation to inspect registrations, exclusions and membership records; each downloaded artifact is checked against its declared SHA-256 and byte count.';
   for(const id of ['population','query','archive'])el(id).disabled=false;paint();
  });}
  el('reload').onclick=refresh;
  el('population').onchange=()=>{state.page=0;paint();};el('query').oninput=()=>{state.page=0;paint();};
  el('previous').onclick=()=>{state.page=Math.max(0,state.page-1);paint();};el('next').onclick=()=>{state.page++;paint();};
  el('archive').onclick=()=>operation(async(epoch,signal)=>{
   const head=state.head;need(head,'Load a reviewed publication first');el('archive').disabled=true;el('status').textContent='Checking complete retained calculation bytes…';
   try{const result=await loadArchive(head,fetcher,signal);if(epoch!==state.epoch)return;state.groups={comparisons:state.groups.comparisons,...result.populations};
    el('population').innerHTML='<option value="comparisons">Membership comparisons</option>'+Object.keys(result.populations).map(k=>'<option value="'+k+'">'+escape(k.replaceAll('/',' · ').replaceAll('_',' '))+'</option>').join('');
    el('evidence').textContent='Retained run and calculation SHA-256 and byte counts match. This browser checks artifact identity and population consistency; it does not replay Python or verify the original engines. Run: '+head.replay.key+'; full calculation: '+result.manifest.packet.files.output.key;
    el('archive-original').textContent=result.output.text;el('run-original').textContent=result.manifest.text;el('status').textContent='All retained calculation populations are available. Investment permissions remain off.';paint();
   }finally{if(epoch===state.epoch)el('archive').disabled=false;}
  });
  const dispose=()=>abort();root.addEventListener?.('pagehide',dispose);root.addEventListener?.('pageshow',e=>{if(e.persisted)refresh();});
  clear();const ready=refresh();return {ready,refresh,dispose,state};
 }
 const api={model,archiveModel,windowRows,table,ref,read,retained,loadArchive,mount,clock};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHGenealogyResearch=api;
})(typeof globalThis!=='undefined'?globalThis:this);
