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
  if(typeof module==='object' && module.exports) module.exports={view};
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
    }).catch(()=>{host.replaceChildren();add('p','Prospective journal is unavailable. No capture or performance claim can be made.');});
})(typeof window==='undefined'?globalThis:window);
