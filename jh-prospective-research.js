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
    if(counts.some(k=>!Number.isInteger(packet[k]) || packet[k]<0) || packet.new_records>packet.records_in_capture)
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
    const allowed=['PENDING_FORWARD_WINDOW','PENDING_SOURCE','MISSING_MATCHING_ENDPOINT','DEFERRED_REQUEST_BUDGET','EVIDENCE_OR_REPLAY_REJECTED','MEASURED_PRICE_ONLY'];
    if(!packet.status_counts||Object.entries(packet.status_counts).some(([k,v])=>!allowed.includes(k)||!Number.isInteger(v)||v<0))return bad;
    const n=packet.forecasts_checked;
    if(!Number.isInteger(n)||n<0||packet.net_return_pct!==null||packet.portfolio_pnl!==null)return bad;
    return {ok:true,checked:n,measured:packet.status_counts.MEASURED_PRICE_ONLY||0,
      pending:(packet.status_counts.PENDING_FORWARD_WINDOW||0)+(packet.status_counts.PENDING_SOURCE||0),
      gaps:(packet.status_counts.MISSING_MATCHING_ENDPOINT||0)+(packet.status_counts.EVIDENCE_OR_REPLAY_REJECTED||0),
      deferred:packet.status_counts.DEFERRED_REQUEST_BUDGET||0,batch:'/'+ref.key,at:packet.generated_at};
  }
  if(typeof module==='object' && module.exports) module.exports={view,outcomeView};
  if(!root.document) return;
  const host=root.document.getElementById('prospective-research'); if(!host) return;
  const add=(tag,text,href)=>{const el=root.document.createElement(tag);el.textContent=text;if(href)el.href=href;host.appendChild(el);return el;};
  fetch('/data/prospective-research.json?_='+Date.now(),{cache:'no-store'})
    .then(r=>r.ok?r.json():null).then(packet=>{
      const state=view(packet);host.replaceChildren();add('h3','Prospective forecast journal');
      if(!state.ok){add('p',state.message);return;}
      add('p',state.records+' registered directions in this capture · '+state.created+' newly recorded · '+state.ranks+' ranked observations kept separate.');
      add('p',state.ineligible+' source packets ineligible · '+state.unsupported+' unsupported identities. Candidate scan '+(state.complete?'complete':'partial — inspect coverage gaps')+'.');
      add('p','Rules are retained before future measurements: next observed session after registration, then 5 and 20 session windows. Registration proves a collected direction, not the upstream model, an executable trade, an independent sample or net performance.');
      add('a','Download capture',state.capture);add('span',' · ');add('a','Read fixed measurement rules',state.protocol);
      const time=add('p','Capture published '+state.at);time.className='timestamp';
      fetch('/data/prospective-outcomes.json?_='+Date.now(),{cache:'no-store'}).then(r=>r.ok?r.json():null).then(data=>{
        const out=outcomeView(data);add('h3','Forward measurement status');
        if(!out.ok){add('p',out.message);return;}
        add('p',out.checked+' forecasts checked in this batch · '+out.measured+' measured price windows · '+out.pending+' pending windows · '+out.gaps+' evidence gaps · '+out.deferred+' deferred for request budget.');
        add('p','These are batch counts, not lifetime results or independent samples. Future windows remain pending. Price observations do not establish net returns or portfolio profit.');
        add('a','Download evaluation batch',out.batch);add('p','Evaluated '+out.at).className='timestamp';
      }).catch(()=>add('p','Forward measurement status is unavailable.'));
    }).catch(()=>{host.replaceChildren();add('p','Prospective journal is unavailable. No capture or performance claim can be made.');});
})(typeof window==='undefined'?globalThis:window);
