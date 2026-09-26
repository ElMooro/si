/* Calls v2: ledger availability, generation health and decision eligibility differ. */
(function (root) {
  'use strict';
  const colors = {ERROR:'#ff4d6d', ABSTAIN:'#b56bff', EXPIRED:'#ffc400', INVALID:'#ff7a18',
    LEGACY:'#6f7b91', LONG:'#00e676', LOAD:'#26ffaf', LEVER:'#00d4ff',
    TRIM:'#ff7a18', HEDGE:'#ffc400', EXIT:'#ff4d6d', EXIT_ALL_RISK:'#ff174a'};
  const reasons = {generation_failed:'Brief generation failed; no allocation instruction.',
    empty_or_stub_brief:'The generator returned an empty or incomplete brief.',
    decision_model_not_validated:'Observation-only brief. The allocation model has not passed validation.'};
  const escape = value => String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const stamp = value => typeof value === 'string' && /T.*(?:Z|[+-]\d\d:\d\d)$/.test(value) ? Date.parse(value) : NaN;
  const date = value => Number.isFinite(stamp(value)) ? new Date(value).toLocaleString() : 'Unknown';
  const regime = value => typeof value === 'object' && value ? value.khalid ?? value.ka ?? value.composite ?? '—' : value;
  function state(row, now=Date.now()) {
    if (!row || row.schema_version !== 'calls.v2') return {label:'LEGACY', eligible:false, reason:'Historical entry without a qualified decision contract.'};
    const at = stamp(row.timestamp), expiry = stamp(row.expires_at);
    if (!Number.isFinite(at) || at > now) return {label:'INVALID', eligible:false, reason:'Decision timestamp is invalid or in the future.'};
    if (row.decision_status === 'ERROR') return {label:'ERROR', eligible:false, reason:reasons[row.decision_reason] || 'Generation failed.'};
    if (row.decision_status === 'ABSTAIN') return {label:'ABSTAIN', eligible:false, reason:reasons[row.decision_reason] || 'No qualified allocation instruction.'};
    if (!Number.isFinite(expiry) || expiry <= at) return {label:'INVALID', eligible:false, reason:'Decision expiry is invalid.'};
    if (expiry <= now) return {label:'EXPIRED', eligible:false, reason:'The decision has expired.'};
    if (row.decision_status !== 'VALID' || row.decision_eligible !== true || row.sizing_eligible !== true ||
        row.validation_status !== 'validated' || !row.model_version || !Array.isArray(row.evidence_ids) || !row.evidence_ids.length ||
        row.evidence_ids.some(x => typeof x !== 'string' || !x) ||
        !['LONG','LOAD','LEVER','TRIM','HEDGE','EXIT','EXIT_ALL_RISK'].includes(row.call_verb))
      return {label:'INVALID', eligible:false, reason:'Required decision qualification is missing.'};
    return {label:row.call_verb, eligible:true, reason:'Qualified allocation instruction; review its evidence and constraints.'};
  }
  function health(row, now=Date.now()) {
    const at=stamp(row?.timestamp), current=state(row,now);
    if (!Number.isFinite(at) || at > now) return 'invalid timestamp';
    return (now-at > 4.5*3600000 ? 'overdue · ' : '') + current.label.toLowerCase();
  }
  function tag(label) { return `<span class="call-tag" style="color:${colors[label] || colors.LEGACY};border-color:currentColor">${escape(label.replaceAll('_',' '))}</span>`; }
  const value = number => typeof number === 'number' && Number.isFinite(number) ? number.toLocaleString(undefined,{maximumFractionDigits:2}) : '—';
  let ledger=null,activeRow=null,pageIndex=0,pageSize=50,filter='ALL',revision=0;
  function historicalState(row,now=Date.now()){
    if(!row||typeof row!=='object'||!Number.isFinite(stamp(row.timestamp))||stamp(row.timestamp)>now)
      return {label:'INVALID',eligible:false,reason:'Missing, invalid or future timestamp; original entry retained.'};
    return state(row,stamp(row.timestamp));
  }
  function historyPage(snapshots,{page=0,size=50,kind='ALL',now=Date.now()}={}){
    if(!Array.isArray(snapshots))throw new Error('Invalid ledger schema');
    size=[25,50,100].includes(size)?size:50;
    const all=snapshots.map((row,index)=>({row,index,at:stamp(row?.timestamp),status:historicalState(row,now)}));
    const rows=all.filter(x=>kind==='ALL'||(kind==='QUALIFIED'?x.status.eligible:x.status.label===kind));
    rows.sort((a,b)=>{const av=Number.isFinite(a.at),bv=Number.isFinite(b.at);return av&&bv?b.at-a.at||a.index-b.index:av?-1:bv?1:a.index-b.index;});
    const pages=Math.max(1,Math.ceil(rows.length/size));page=Math.max(0,Math.min(Number.isInteger(page)?page:0,pages-1));
    return {rows:rows.slice(page*size,(page+1)*size),page,pages,total:snapshots.length,matched:rows.length,
      first:rows.length?page*size+1:0,last:Math.min((page+1)*size,rows.length),invalid:all.filter(x=>x.status.label==='INVALID').length};
  }
  function renderHistory(now=Date.now()){
    if(!ledger)return;const doc=root.document,result=historyPage(ledger.snapshots,{page:pageIndex,size:pageSize,kind:filter,now});pageIndex=result.page;
    doc.getElementById('history-body').innerHTML=result.rows.map(item=>{
      const r=item.row&&typeof item.row==='object'?item.row:{},status=item.status,acc=r.weighted_mean_accuracy;
      return `<tr><td>${escape(date(r.timestamp))}${status.label==='INVALID'?`<div>Original entry ${item.index+1} · invalid clock or row</div>`:''}</td><td>${tag(status.label)}<div>${escape(r.call_verb || '—')}</div></td>
        <td>${escape(regime(r.regime))}</td><td>${escape(r.phase)}</td><td class="num">${value(r.khalid_score)}</td>
        <td class="num">${typeof acc==='number' && Number.isFinite(acc) ? (acc*100).toFixed(1)+'%' : '—'}</td>
        <td>${escape(r.highest_weight_signal)}</td><td class="num">${value(r.brief_chars)}</td></tr>`;
    }).join('')||'<tr><td colspan="8">No entries match this filter.</td></tr>';
    const range=doc.getElementById('history-range');if(range)range.textContent=`${result.first}–${result.last} of ${result.matched} matching entries · ${result.total} retained in full`;
    const previous=doc.getElementById('history-prev'),next=doc.getElementById('history-next');
    if(previous){previous.disabled=result.page===0;previous.onclick=()=>{pageIndex--;renderHistory();};}
    if(next){next.disabled=result.page+1>=result.pages;next.onclick=()=>{pageIndex++;renderHistory();};}
    const select=doc.getElementById('history-filter'),size=doc.getElementById('history-size');
    if(select){select.disabled=false;select.value=filter;select.onchange=()=>{filter=select.value;pageIndex=0;renderHistory();};}
    if(size){size.disabled=false;size.value=String(pageSize);size.onchange=()=>{pageSize=Number(size.value);pageIndex=0;renderHistory();};}
  }
  function render(documentData) {
    const doc=root.document, now=Date.now();
    if (!Array.isArray(documentData.snapshots)) throw new Error('Invalid ledger schema');
    ledger=documentData;
    const rows=documentData.snapshots.filter(r=>r && Number.isFinite(stamp(r.timestamp))&&stamp(r.timestamp)<=now).sort((a,b)=>stamp(a.timestamp)-stamp(b.timestamp));
    const last=rows.at(-1), current=state(last,now);
    activeRow=last;
    const invalid=documentData.snapshots.length-rows.length;
    doc.getElementById('n-total').textContent=documentData.snapshots.length;
    doc.getElementById('last-when').textContent=last ? date(last.timestamp) : 'No entries';
    doc.getElementById('status').textContent=last ? health(last,now) : 'empty ledger';
    doc.getElementById('now-banner').style.display=last ? '' : 'none';
    doc.getElementById('notice-area').innerHTML=`<div class="notice">${escape(current.reason)} Historical weighted signal accuracy is not Calls accuracy. A new computation does not make its underlying observations current.${invalid?` ${invalid} entries have missing, invalid or future clocks; retained in the history table and excluded from the current decision and timeline.`:''}</div>`;
    if (last) {
      doc.getElementById('now-verb').textContent=(last.generation_method === 'warehouse_deterministic_v1' && !current.eligible ? 'WAIT' : current.label.replaceAll('_',' '));
      doc.getElementById('now-verb').style.color=colors[current.label] || colors.LEGACY;
      doc.getElementById('now-when').textContent=`Computed ${date(last.timestamp)} · expires ${date(last.expires_at)}`;
      const context=[['Decision use',current.eligible?'Qualified':'No allocation instruction'],['Phase',last.phase],
        ['Regime',regime(last.regime)],['Khalid Index',value(last.khalid_score)],
        ['Generation',last.generation_status || 'Unverified legacy'],['Forecast horizon',last.forecast_horizon_days == null ? 'Not established' : `${last.forecast_horizon_days} days`]];
      doc.getElementById('now-ctx').innerHTML=context.map(([key,val])=>`<div class="row"><span class="key">${escape(key)}</span><span class="val">${escape(val)}</span></div>`).join('');
    }
    const recent=rows.filter(r=>now-stamp(r.timestamp)<=7*86400000);
    const qualified=rows.filter(r=>state(r,stamp(r.timestamp)).eligible);
    const tiles=[['Decision status',last?current.label:'No entries'],['Qualified decisions · 7d',recent.filter(r=>state(r,stamp(r.timestamp)).eligible).length],
      ['Generation failures · 7d',recent.filter(r=>r.decision_status==='ERROR').length],['Historical entries',documentData.snapshots.length]];
    doc.getElementById('kpi-row').innerHTML=tiles.map(([key,val],i)=>`<div class="kpi"><h3>${escape(key)}</h3><div class="v"${i===0?' id="decision-kpi-value"':''}>${escape(val)}</div></div>`).join('');
    renderHistory(now);
    const changes=[];
    for(let i=1;i<qualified.length;i++) if(qualified[i].call_verb!==qualified[i-1].call_verb) changes.push([qualified[i-1],qualified[i]]);
    doc.getElementById('changes-section').style.display=changes.length ? '' : 'none';
    doc.getElementById('changes-list').innerHTML=changes.reverse().map(([a,b])=>`<div class="change">${tag(a.call_verb)} → ${tag(b.call_verb)} <span>${escape(date(b.timestamp))}</span></div>`).join('');
    const labels=[...new Set(rows.map(r=>state(r,stamp(r.timestamp)).label))];
    const first=rows.length ? stamp(rows[0].timestamp) : now, range=Math.max(1,now-first);
    doc.getElementById('timeline-chart').innerHTML=labels.map((label,i)=>`<text x="8" y="${25+i*150/Math.max(labels.length,1)}" fill="${colors[label] || colors.LEGACY}" font-size="10">${escape(label)}</text>`).join('')+
      rows.map(r=>{const label=state(r,stamp(r.timestamp)).label,x=130+940*(stamp(r.timestamp)-first)/range,y=22+labels.indexOf(label)*150/Math.max(labels.length,1);
        return `<circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="4" fill="${colors[label] || colors.LEGACY}"><title>${escape(date(r.timestamp))} · ${escape(label)} · recorded ${escape(r.call_verb)}</title></circle>`;}).join('');
    doc.getElementById('legend').innerHTML=labels.map(label=>tag(label)).join(' ');
  }
  async function get(fetcher,timeout=15000){
    const controller=new AbortController();let timer;
    try{return await Promise.race([(async()=>{
      const response=await fetcher('/data/decisive-call-history.json?exact=1&nogen=1',{cache:'no-store',signal:controller.signal});
      if(!response.ok)throw new Error(`HTTP ${response.status}`);return await response.json();
    })(),new Promise((resolve,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new Error('Ledger request timed out'));},timeout);})]);}
    finally{clearTimeout(timer);}
  }
  async function load(fetcher=root.fetch.bind(root)) {
    const doc=root.document,current=++revision;
    try {
      const packet=await get(fetcher);if(current!==revision)return;
      render(packet);
    } catch(error) {
      if(current!==revision)return;ledger=null;activeRow=null;
      doc.getElementById('status').textContent='ledger unavailable';
      doc.getElementById('now-banner').style.display='none';
      doc.getElementById('kpi-row').innerHTML='<p>Current ledger statistics unavailable.</p>';
      for(const id of ['history-prev','history-next','history-filter','history-size']){const node=doc.getElementById(id);if(node)node.disabled=true;}
      doc.getElementById('notice-area').innerHTML=`<div class="err">Could not verify the current ledger: ${escape(error.message)}. Previously rendered history is not a current decision.</div>`;
    }
  }
  function expire(){
    if(!ledger||!activeRow)return;const doc=root.document,now=Date.now(),current=state(activeRow,now);
    doc.getElementById('status').textContent=health(activeRow,now);
    if(current.label==='EXPIRED'||current.label==='INVALID'){
      doc.getElementById('now-verb').textContent=current.label;doc.getElementById('now-verb').style.color=colors[current.label];
      const decision=doc.getElementById('decision-kpi-value');if(decision)decision.textContent=current.label;
      doc.getElementById('now-ctx').textContent=current.reason;
      doc.getElementById('notice-area').innerHTML=`<div class="notice">${escape(current.reason)} Historical records remain available for inspection.</div>`;
    }
  }
  const api={state,health,historicalState,historyPage,renderHistory,render,get,load,expire};
  if(typeof module !== 'undefined' && module.exports) module.exports=api;
  else {root.CallsView=api;let refresh,expiry;
    const start=()=>{root.clearInterval(refresh);root.clearInterval(expiry);load();refresh=root.setInterval(()=>load(),5*60*1000);expiry=root.setInterval(expire,30000);};
    start();root.addEventListener('pagehide',()=>{revision++;root.clearInterval(refresh);root.clearInterval(expiry);});
    root.addEventListener('pageshow',event=>{if(event.persisted)start();});}
})(typeof window !== 'undefined' ? window : globalThis);
