/* Prospective collection status is not performance or trading permission. */
(function(root){
  'use strict';
  function view(packet, now){
    now=now==null?Date.now():now;
    if(!packet || packet.schema_version!=='prospective-research-summary.v1') return {ok:false,message:'Prospective journal is unavailable. No capture or performance claim can be made.'};
    const stamp=Date.parse(packet.generated_at), age=now-stamp;
    if(!Number.isFinite(age) || age<0 || age>30*3600000) return {ok:false,message:'Prospective journal status is overdue or has an invalid clock. Open the last capture for its original dates.'};
    const ref=packet.capture, protocol=packet.protocol;
    const valid=(r,prefix)=>r && /^[a-f0-9]{64}$/.test(r.sha256) && r.key===prefix+r.sha256+'.json';
    if(!valid(ref,'data/research-forecasts/captures/') || !valid(protocol,'data/research-forecasts/protocols/') || packet.sizing_eligible!==false || packet.promotion_eligible!==false)
      return {ok:false,message:'Prospective journal contract is invalid. Research registration cannot authorize trading.'};
    const counts=['records_in_capture','new_records','rank_observations','ineligible_sources','unsupported_identity_count'];
    if(counts.some(k=>!Number.isSafeInteger(packet[k]) || packet[k]<0) || packet.new_records>packet.records_in_capture)
      return {ok:false,message:'Prospective journal counts are invalid.'};
    return {ok:true,at:packet.generated_at,records:packet.records_in_capture,created:packet.new_records,
      ranks:packet.rank_observations,ineligible:packet.ineligible_sources,unsupported:packet.unsupported_identity_count,
      complete:packet.coverage && packet.coverage.candidate_scan_complete===true,
      capture:'/'+ref.key,protocol:'/'+protocol.key};
  }
  function outcomeView(packet, now){
    now=now==null?Date.now():now;
    const bad={ok:false,message:'Forward measurement status is unavailable, overdue or invalid. No performance claim can be made.'};
    if(!packet || packet.schema_version!=='prospective-outcome-batch.v1' || packet.sizing_eligible!==false || packet.promotion_eligible!==false) return bad;
    const age=now-Date.parse(packet.generated_at),ref=packet.batch;
    if(!Number.isFinite(age)||age<0||age>3*3600000||!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='data/research-forecasts/evaluation-runs/'+ref.sha256+'.json')return bad;
    const allowed=['PENDING_FORWARD_WINDOW','PENDING_SOURCE','MISSING_MATCHING_ENDPOINT','DEFERRED_REQUEST_BUDGET','EVIDENCE_OR_REPLAY_REJECTED','MEASURED_PRICE_ONLY','UNSUPPORTED_SOURCE_IDENTITY'];
    if(!packet.status_counts||Object.entries(packet.status_counts).some(([k,v])=>!allowed.includes(k)||!Number.isSafeInteger(v)||v<0))return bad;
    const n=packet.forecasts_checked;
    if(!Number.isSafeInteger(n)||n<0||packet.net_return_pct!==null||packet.portfolio_pnl!==null)return bad;
    return {ok:true,checked:n,measured:packet.status_counts.MEASURED_PRICE_ONLY||0,
      excluded:packet.status_counts.UNSUPPORTED_SOURCE_IDENTITY||0,
      pending:(packet.status_counts.PENDING_FORWARD_WINDOW||0)+(packet.status_counts.PENDING_SOURCE||0),
      gaps:(packet.status_counts.MISSING_MATCHING_ENDPOINT||0)+(packet.status_counts.EVIDENCE_OR_REPLAY_REJECTED||0),
      deferred:packet.status_counts.DEFERRED_REQUEST_BUDGET||0,batch:'/'+ref.key,at:packet.generated_at};
  }
  const canonical=value=>JSON.stringify(value,(_,v)=>v && !Array.isArray(v) && typeof v==='object'
    ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,v[k]])):v);
  const equal=(a,b)=>canonical(a)===canonical(b);
  const count=n=>Number.isSafeInteger(n)&&n>=0;
  function captureMatches(head,doc){
    if(!doc || doc.contract!=='prospective-research-capture.v1' || doc.generated_at!==head.generated_at ||
       doc.sizing_eligible!==false || doc.promotion_eligible!==false || !equal(doc.protocol_ref,head.protocol) ||
       !equal(doc.coverage,head.coverage) || !equal(doc.identity_policy,head.identity_policy) || !equal(doc.source_read_policy,head.source_read_policy) ||
       !Array.isArray(doc.records) || !Array.isArray(doc.sources))return false;
    if(doc.records.some(r=>!r || typeof r.created!=='boolean') || doc.sources.some(s=>!s ||
       !Array.isArray(s.observations) || !Array.isArray(s.eligibility_reasons) || !count(s.unsupported_identity_count)))return false;
    return head.records_in_capture===doc.records.length && head.new_records===doc.records.filter(r=>r.created).length &&
      head.rank_observations===doc.sources.reduce((n,s)=>n+s.observations.filter(r=>r.origin==='rank_observation').length,0) &&
      head.ineligible_sources===doc.sources.filter(s=>s.eligibility_reasons.length>0).length &&
      head.unsupported_identity_count===doc.sources.reduce((n,s)=>n+s.unsupported_identity_count,0);
  }
  function outcomeMatches(head,doc){
    const {batch,...expected}=head;
    if(!doc || !equal(expected,doc) || !Array.isArray(doc.results))return false;
    const counts=Object.create(null);
    for(const row of doc.results){
      if(!row || typeof row.status!=='string')return false;
      counts[row.status]=(counts[row.status]||0)+1;
    }
    return equal(counts,doc.status_counts);
  }
  async function readJson(path,options={}){
    const {fetcher=root.fetch.bind(root),limit=8*1024*1024,timeout=10000,expectedHash=null,crypto=root.crypto}=options;
    const controller=new AbortController();let timer,reader;
    const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new Error('Research request timed out'));},timeout);});
    try{return await Promise.race([deadline,(async()=>{
      const response=await fetcher(path+'?exact=1&nogen=1',{cache:'no-store',signal:controller.signal,redirect:'error'});
      if(!response.ok || !response.body || !response.body.getReader)throw new Error('Research response unavailable');
      reader=response.body.getReader();const chunks=[];let length=0;
      for(;;){const {done,value}=await reader.read();if(done)break;length+=value.byteLength;
        if(length>limit)throw new Error('Whole research response exceeds bound');chunks.push(value);}
      const raw=new Uint8Array(length);let offset=0;for(const chunk of chunks){raw.set(chunk,offset);offset+=chunk.length;}
      if(expectedHash){
        const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),b=>b.toString(16).padStart(2,'0')).join('');
        if(hash!==expectedHash)throw new Error('Retained research bytes differ');
      }
      return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
    })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
  }
  async function loadResearch(kind,options={}){
    const capture=kind==='capture';if(!capture && kind!=='outcome')throw new Error('Research kind required');
    const path=capture?'/data/prospective-research.json':'/data/prospective-outcomes.json';
    const head=await readJson(path,{...options,limit:1024*1024});
    const state=(capture?view:outcomeView)(head,options.now);
    if(!state.ok)throw new Error(state.message);
    const ref=capture?head.capture:head.batch;
    const retained=await readJson('/'+ref.key,{...options,expectedHash:ref.sha256});
    if(!(capture?captureMatches:outcomeMatches)(head,retained))throw new Error('Summary does not match its complete retained research');
    return {...state,retainedVerified:true};
  }
  function mount(host,loader=loadResearch){
    host.replaceChildren();
    const section=(id,title)=>{const node=root.document.createElement('section');node.id=id;host.appendChild(node);
      const add=(tag,text,href)=>{const el=root.document.createElement(tag);el.textContent=text;if(href)el.href=href;node.appendChild(el);return el;};
      add('h3',title);const message=add('p','Checking complete retained research…');return {node,add,message};};
    const capture=section('research-capture-status','Prospective forecast journal');
    const outcome=section('research-outcome-status','Forward measurement status');
    const run=async(kind,panel)=>{
      try{
        const state=await loader(kind);panel.message.remove();const add=panel.add;
        if(kind==='capture'){
          add('p',state.records+' registered directions in this capture · '+state.created+' newly recorded · '+state.ranks+' ranked observations kept separate.');
          add('p',state.ineligible+' source packets ineligible · '+state.unsupported+' unsupported identities. Candidate scan '+(state.complete?'complete':'partial — inspect coverage gaps')+'.');
          add('p','Fixed research windows start at the next observed session after registration, then span 5 or 20 further observed sessions. Registration does not validate the upstream model or establish an executable trade.');
          add('a','Download complete capture',state.capture);add('span',' · ');add('a','Read fixed measurement rules',state.protocol);
          add('p','Capture published '+state.at).className='timestamp';
        }else{
          add('p',state.checked+' forecasts checked in this batch · '+state.measured+' measured price windows · '+state.pending+' pending windows · '+state.gaps+' evidence gaps · '+state.excluded+' records excluded for unsupported source identity · '+state.deferred+' deferred for request budget.');
          add('p','Batch counts are not lifetime results or independent samples. Future windows remain pending. Price observations do not establish net returns or portfolio profit.');
          add('a','Download complete evaluation batch',state.batch);add('p','Evaluated '+state.at).className='timestamp';
        }
        add('p','Counts match the complete retained file and its SHA-256. This check does not replay original provider data or qualify investment performance.');
      }catch(_){panel.message.textContent='Retained research is unavailable, overdue or failed verification. No counts or performance claim are shown.';}
    };
    return Promise.allSettled([run('capture',capture),run('outcome',outcome)]);
  }
  if(typeof module==='object' && module.exports) module.exports={view,outcomeView,captureMatches,outcomeMatches,readJson,loadResearch,mount};
  if(!root.document)return;
  const host=root.document.getElementById('prospective-research');if(host)mount(host);
})(typeof window==='undefined'?globalThis:window);
