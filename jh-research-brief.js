(function(root){
  'use strict';
  const CONTRACT='research-intelligence.v1';
  const LIMITS={D:10,W:21,BW:35,M:100,Q:200,SA:370,A:550};
  const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function link(key,label){
    if(typeof key!=='string'||!/^data\/(evidence|research-intelligence|report-research)\/[A-Za-z0-9_./-]+$/.test(key)||key.includes('..'))return 'Evidence unavailable';
    return `<a href="/${key}">${esc(label)}</a>`;
  }
  function status(row,packet,now){
    if(row.status!=='fresh')return row.status||'unavailable';
    const acquired=Date.parse(row.acquired_at),observed=Date.parse(row.observed_at+'T00:00:00Z'),source=Date.parse(packet.source_generated_at);
    const day=Date.parse(new Date(now).toISOString().slice(0,10)+'T00:00:00Z');
    if(![acquired,observed,source].every(Number.isFinite)||!(row.frequency in LIMITS)||acquired>now||observed>day||source>now)return 'invalid_clock';
    if((day-observed)/86400000>LIMITS[row.frequency])return 'stale_observation';
    if(now-acquired>26*3600000||now-source>26*3600000)return 'stale_source';
    if(row.value==null||row.value===''||!Number.isFinite(Number(row.value)))return 'unavailable';
    return 'fresh';
  }
  function render(packet,now=Date.now()){
    if(packet?.contract!==CONTRACT||!Array.isArray(packet.brief_items)||!Array.isArray(packet.metrics_table))throw Error('Source-backed brief unavailable');
    const fresh=packet.metrics_table.filter(r=>status(r,packet,now)==='fresh').length;
    const cards=packet.brief_items.map(row=>{
      const state=status(row,packet,now),live=state==='fresh',m=row.changes?.month;
      const unit=m?.change_unit==='percentage_points'?'percentage points':m?.change_unit;
      const change=live&&m?.change_decimal!=null?`${esc(m.change_decimal)} ${esc(unit)} versus ${esc(m.baseline_date)}`:'One-month comparison unavailable';
      return `<article class="brief-card"><div class="brief-label">${esc(row.series_id)} · ${esc(state.replaceAll('_',' '))}</div>
        <h2>${esc(row.metric)}</h2><p class="brief-value">${live?esc(row.value):'Unavailable'} <span>${live?esc(row.unit):''}</span></p>
        <p>Observed ${esc(row.observed_at)} · ${esc(row.seasonal_adjustment)}</p><p>${change}</p>
        <details><summary>Inspect the observation</summary><p>${esc(row.frequency)} frequency · retrieved ${esc(row.acquired_at)}</p>
        ${!live&&row.last_observed_value!=null?`<p>Last observed: ${esc(row.last_observed_value)} ${esc(row.unit)}. This is not a current reading.</p>`:''}
        <p>Original observation row ${esc(row.current_row_index)} (zero based).</p><p>${link(row.evidence?.observations?.key,'Original observations')} · ${link(row.evidence?.definition?.key,'Official definition')}</p>
        <p>Calendar target ${esc(m?.target_date)}; baseline ${esc(m?.baseline_date)}: ${esc(m?.baseline_decimal)} ${esc(row.unit)}.</p></details></article>`;
    }).join('');
    return `<header class="brief-head"><p class="brief-label">RESEARCH BRIEF · ${fresh} / ${packet.metrics_table.length} SERIES FRESH</p>
      <h1>Dated evidence. A clear decision boundary.</h1><p><strong>WAIT — abstain.</strong> These observations do not establish a validated investment call or a portfolio weight.</p>
      <p class="brief-clock">Brief ${esc(packet.generated_at)}<br>Source packet ${esc(packet.source_generated_at)}</p>
      <p>${link(packet.replay?.manifest_key,'Replay this brief')} · <a href="/read.html">Explore all measurements</a></p></header>
      <section class="brief-grid" aria-label="Core macro observations">${cards}</section>
      <section class="brief-boundary"><h2>Connect research to your portfolio</h2><p>Allocation remains unavailable until the forecast has prospective validation and current holdings, cash, constraints, costs and a risk budget are available. A WAIT here means abstention; it does not prescribe holding, buying or selling an asset.</p>
      <p><a href="/signal-scorecard.html">Inspect validation coverage</a> · <a href="/portfolio/">Review your private portfolio</a></p>
      <p>These are current retrieved vintages. Original publication timing, historical as-known-at values and causal investment effects are not established.</p></section>`;
  }
  async function refresh(host,fetcher=root.fetch.bind(root),now=Date.now()){
    const inspector=root.document?.getElementById('full-intelligence-report');
    try{
      const response=await fetcher('/intelligence-report.json',{cache:'no-store'});
      if(!response.ok)throw Error('Research brief request failed');
      const packet=await response.json();host.innerHTML=render(packet,now);
      if(inspector){inspector.replaceChildren();if(root.JHDataInspector)root.JHDataInspector.inspect(inspector,packet,'Complete research packet, including unavailable series');}
      return packet;
    }catch(error){
      host.innerHTML='<p role="status">Research brief unavailable. Previously displayed readings have been cleared. No investment call or allocation is available.</p>';
      if(inspector)inspector.replaceChildren();return null;
    }
  }
  const api={render,status,refresh,link};
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(root.document)root.document.addEventListener('DOMContentLoaded',()=>{
    const host=root.document.getElementById('research-brief');if(!host)return;
    let loading=false;
    const load=async()=>{if(loading)return;loading=true;try{await refresh(host);}finally{loading=false;}};
    load();root.setInterval(load,5*60*1000);
  });
})(typeof window==='object'?window:globalThis);
