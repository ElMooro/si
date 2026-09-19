(function(root){
 'use strict';
 const CONTRACT='liquidity-inflection-research.v1';
 const esc=v=>String(v==null?'—':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const path=k=>typeof k==='string'&&/^data\/(evidence|inflection-research|report-research|vintage-research)\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(key,label)=>path(key)?`<a href="${esc(path(key))}" target="_blank" rel="noopener">${esc(label)}</a>`:'Unavailable';
 const num=v=>v!=null&&v!==''&&Number.isFinite(Number(v))?Number(v).toLocaleString('en-US',{maximumFractionDigits:3}):'—';
 function fresh(row,packet,now=Date.now()){
  const limits={D:10,W:21,BW:35,M:100,Q:200,SA:370,A:550};
  if(row.quality?.status!=='fresh'||row.available!==true)return row.quality?.status||'unavailable';
  const clocks=[row.acquired_at,packet.source_generated_at,packet.generated_at].map(Date.parse),obs=Date.parse(row.date+'T00:00:00Z');
  if(clocks.some(d=>!Number.isFinite(d)||d>now)||!Number.isFinite(obs)||!limits[row.frequency])return 'invalid_clock';
  if(clocks.some(d=>now-d>26*3600000))return 'stale_source';
  if(Math.floor(now/86400000)-Math.floor(obs/86400000)>limits[row.frequency])return 'stale_observation';
  return row.current_decimal!=null&&Number.isFinite(Number(row.current_decimal))?'fresh':'unavailable';
 }
 function archiveFresh(packet,now=Date.now()){
  const refs=packet.calendar_research?.archive_references||{};
  return ['WALCL','WTREGEN','RRPONTSYD'].every(sid=>{
   const d=Date.parse(refs[sid]?.generated_at);return Number.isFinite(d)&&d<=now&&now-d<=8*86400000;
  });
 }
 function chart(history){
  const rows=Object.entries(history||{}).map(([day,row])=>({day,value:row.slope_usd_mn_per_week})).sort((a,b)=>a.day.localeCompare(b.day));
  const values=rows.filter(r=>typeof r.value==='number'&&Number.isFinite(r.value));
  if(values.length<2)return '<p>Not enough complete weekly windows to plot a trend.</p>';
  const start=Date.parse(rows[0].day),end=Date.parse(rows[rows.length-1].day),lo=Math.min(0,...values.map(r=>r.value)),hi=Math.max(0,...values.map(r=>r.value));
  const y=v=>150-130*(v-lo)/(hi-lo||1),x=day=>65+775*(Date.parse(day)-start)/(end-start||1);
  let d='',open=false;for(const r of rows){if(typeof r.value!=='number'||!Number.isFinite(r.value)){open=false;continue;}d+=(open?'L':'M')+x(r.day).toFixed(2)+' '+y(r.value).toFixed(2)+' ';open=true;}
  return `<svg role="img" aria-label="Descriptive 13-week liquidity slope in USD millions per week; gaps represent incomplete windows" viewBox="0 0 860 195"><line x1="65" y1="${y(0)}" x2="840" y2="${y(0)}" stroke="#52687d"/><path d="${d}" stroke="#60c6de" stroke-width="2" fill="none"/><text x="3" y="20">${esc(num(hi))}</text><text x="3" y="150">${esc(num(lo))}</text><text x="65" y="184">${esc(rows[0].day)}</text><text x="840" y="184" text-anchor="end">${esc(rows[rows.length-1].day)}</text></svg>`;
 }
 function seriesRows(packet,now){
  return Object.entries(packet.series||{}).map(([sid,row])=>{
   const status=fresh(row,packet,now),e=row.evidence||{};
   return `<tr><th scope="row">${esc(sid)}<span class="small">${esc(row.name||row.requested_label)}</span></th><td>${status==='fresh'?esc(row.current_decimal):'—'}<span class="small">${esc(row.unit)}</span></td><td>${esc(row.date)}<span class="small">${esc(row.frequency)} · ${esc(row.seasonal_adjustment)}</span></td><td>${esc(status)}<span class="small">Acquired ${esc(row.acquired_at)}</span></td><td>${link(e.observations?.key,'Original observations')}<br>${link(e.definition?.key,'Definition')}<span class="small">Original row ${esc(row.current_row_index)}</span><details><summary>Calendar comparisons & last observed value</summary><p>Last observed: ${esc(row.last_observed_value)} ${esc(row.unit)}</p><pre>${esc(JSON.stringify(row.changes&&Object.keys(row.changes).length?row.changes:row.historical_changes||{},null,2))}</pre></details></td></tr>`;
  }).join('');
 }
 function render(packet,now=Date.now()){
  if(packet?.contract!==CONTRACT)throw Error('Original-source liquidity research is not available; legacy scores are not shown.');
  const proxy=packet.net_liquidity||{},features=packet.calendar_research||{},latest=features.latest||{},archiveOK=archiveFresh(packet,now);
  const currentOK=['WALCL','WTREGEN','RRPONTSYD'].every(sid=>fresh(packet.series?.[sid]||{},packet,now)==='fresh');
  const legs=Object.entries(proxy.components||{}).map(([sid,row])=>`<tr><th>${esc(sid)}</th><td>${esc(row.value_decimal)}</td><td>${esc(row.unit)}</td><td>${esc(row.multiplier_to_usd_millions)}</td><td>${esc(row.date)}</td><td>${link(row.evidence?.observations?.key,'Original')} · row ${esc(row.row_index)}</td></tr>`).join('');
  const history=features.history||{},days=Object.keys(history).sort().reverse();
  const context=Object.entries(packet.retained_context||{}).map(([name,row])=>`<tr><th>${esc(name)}</th><td>${esc(row.status)}</td><td>${esc(row.generated_at)}</td><td>Retained in the ${link(packet.replay?.manifest_key,'run input')}; no vote or sizing permission.</td></tr>`).join('');
  return {stamp:`Generated ${packet.generated_at} · macro source ${packet.source_generated_at} · ${packet.quality?.fresh_series}/${packet.quality?.expected_series} source rows fresh at build`,
   summary:`<article><span class="eyebrow">Current mixed-basis proxy</span><strong>${currentOK?num(proxy.net):'—'}</strong><span>USD millions</span><p>${currentOK?'Latest dated components below.':'Current source data is unavailable or expired.'}</p></article><article><span class="eyebrow">13-calendar-week slope</span><strong>${archiveOK?num(latest.slope_usd_mn_per_week):'—'}</strong><span>USD millions / week</span><p>${esc(latest.start)} → ${esc(latest.end)} · ${esc(latest.present_weekly_samples)}/14 samples</p></article><article><span class="eyebrow">Change in slope over 13 weeks</span><strong>${archiveOK?num(latest.acceleration_usd_mn_per_week2):'—'}</strong><span>USD millions / week²</span><p>Descriptive acceleration; no calibrated forecast.</p></article>`,
   legs,series:seriesRows(packet,now),chart:chart(history),context,
   archive:`${archiveOK?'Archive publication within the eight-day research limit.':'Archive publication unavailable or expired; current statistics withheld.'} Historical chart remains dated research. ${features.weekly_observations||0} weekly calendar slots. Latest three-year descriptive z: ${archiveOK?num(latest.z_3y?.z):'—'} (${esc(latest.z_3y?.status)}).`,
   dates:days.map(d=>`<option value="${esc(d)}">${esc(d)}</option>`).join(''),
   evidence:`${link(packet.replay?.manifest_key,'Immutable run manifest')} · ${link(packet.legacy_context?.key,'Preserved legacy output')} · <a href="https://github.com/ElMooro/si/blob/main/scripts/replay_inflection_research.py">Independent replay script</a>`,
   methodology:packet.methodology||'',reason:packet.decision?.reason||'Investment qualification remains unavailable.'};
 }
 function inspect(packet,day){
  const f=packet.calendar_research||{},row=f.history?.[day];if(!row)return '<p>Selected calendar sample unavailable.</p>';
  const collectionKey=f.archive_collection_key;
  const archiveLinks=['WALCL','WTREGEN','RRPONTSYD'].map(sid=>{
   // Friday noon uses the prior completed archive date, not observation date.
   const archiveDay=new Date(Date.parse(day+'T12:00:00Z')-86400000).toISOString().slice(0,10);
   const run=typeof collectionKey==='string'?collectionKey.match(/collections\/([a-f0-9]{64})\.json$/)?.[1]:null;
   return run?`<a href="/vintages.html?series=${sid}&day=${archiveDay}&run=${run}">${sid} archive inspector</a>`:`${sid}: pinned collection unavailable`;
  }).join(' · ');
  const change=row.endpoint_change_decomposition||{},legs=Object.entries(change.components||{}).map(([sid,c])=>`<tr><th>${esc(sid)}</th><td>${esc(c.baseline_usd_mn_decimal)}</td><td>${esc(c.current_usd_mn_decimal)}</td><td>${esc(c.formula_sign)}</td><td>${esc(c.signed_change_usd_mn_decimal)}</td></tr>`).join('');
  return `<p>Research evaluation: ${esc(day)} 12:00 UTC · level ${esc(f.levels_usd_mn_decimal?.[day])} USD millions.</p><p>${archiveLinks}</p><h3>13-week endpoint change by component</h3><p>Arithmetic contributions in USD millions, ${esc(change.start)} → ${esc(change.end)}. Total proxy change: ${esc(change.net_change_usd_mn_decimal)}. These changes describe the proxy; they do not establish causation or an investment return.</p><div class="table"><table><thead><tr><th>Series</th><th>Baseline USD mn</th><th>Current USD mn</th><th>Formula sign</th><th>Signed contribution USD mn</th></tr></thead><tbody>${legs}</tbody></table></div><details><summary>Exact calculation & component row references</summary><pre>${esc(JSON.stringify({calculation:row,components:f.components?.[day]||null},null,2))}</pre></details>`;
 }
 const api={CONTRACT,esc,path,num,fresh,archiveFresh,chart,render,inspect};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.JHInflectionResearch=api;
})(typeof globalThis!=='undefined'?globalThis:this);
