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
  function render(documentData) {
    const doc=root.document, now=Date.now();
    if (!Array.isArray(documentData.snapshots)) throw new Error('Invalid ledger schema');
    const rows=documentData.snapshots.filter(r=>r && Number.isFinite(stamp(r.timestamp))).sort((a,b)=>stamp(a.timestamp)-stamp(b.timestamp));
    const last=rows.at(-1), current=state(last,now);
    doc.getElementById('n-total').textContent=rows.length;
    doc.getElementById('last-when').textContent=last ? date(last.timestamp) : 'No entries';
    doc.getElementById('status').textContent=last ? health(last,now) : 'empty ledger';
    doc.getElementById('now-banner').style.display=last ? '' : 'none';
    doc.getElementById('notice-area').innerHTML=`<div class="notice">${escape(current.reason)} Historical weighted signal accuracy is not Calls accuracy. A new computation does not make its underlying observations current.</div>`;
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
      ['Generation failures · 7d',recent.filter(r=>r.decision_status==='ERROR').length],['Historical entries',rows.length]];
    doc.getElementById('kpi-row').innerHTML=tiles.map(([key,val])=>`<div class="kpi"><h3>${escape(key)}</h3><div class="v">${escape(val)}</div></div>`).join('');
    doc.getElementById('history-body').innerHTML=[...rows].reverse().map(r=>{
      const status=state(r,stamp(r.timestamp)), acc=r.weighted_mean_accuracy;
      return `<tr><td>${escape(date(r.timestamp))}</td><td>${tag(status.label)}<div>${escape(r.call_verb || '—')}</div></td>
        <td>${escape(regime(r.regime))}</td><td>${escape(r.phase)}</td><td class="num">${value(r.khalid_score)}</td>
        <td class="num">${typeof acc==='number' && Number.isFinite(acc) ? (acc*100).toFixed(1)+'%' : '—'}</td>
        <td>${escape(r.highest_weight_signal)}</td><td class="num">${value(r.brief_chars)}</td></tr>`;
    }).join('') || '<tr><td colspan="8">No history available.</td></tr>';
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
  async function load() {
    const doc=root.document;
    try {
      const response=await root.fetch(`/data/decisive-call-history.json?t=${Date.now()}`,{cache:'no-store',signal:AbortSignal.timeout(15000)});
      if(!response.ok) throw new Error(`HTTP ${response.status}`);
      render(await response.json());
    } catch(error) {
      doc.getElementById('status').textContent='ledger unavailable';
      doc.getElementById('now-banner').style.display='none';
      doc.getElementById('notice-area').innerHTML=`<div class="err">Could not verify the current ledger: ${escape(error.message)}. Previously rendered history is not a current decision.</div>`;
    }
  }
  const api={state,health,render,load};
  if(typeof module !== 'undefined' && module.exports) module.exports=api;
  else {root.CallsView=api;load();root.setInterval(load,5*60*1000);}
})(typeof window !== 'undefined' ? window : globalThis);
