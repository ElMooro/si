/* Euro dashboard: published ECB engine documents and the existing FRED API. */
(function () {
  'use strict';
  const API = 'https://justhodl-data-proxy.raafouis.workers.dev';
  const ENGINE_SOURCES = {
    detail: '/data/ecb-detail.json',
    derived: '/data/ecb-derived.json',
  };
  const CONFIG_KEY = 'khalid-metrics-config';
  const DEFAULTS = [
    ['VIXCLS','VIX index','volatility'],['VXVCLS','S&P 500 3-month volatility index','volatility'],
    ['DGS10','10-year Treasury yield (%)','treasuries'],['DGS2','2-year Treasury yield (%)','treasuries'],
    ['DGS30','30-year Treasury yield (%)','treasuries'],['T10Y2Y','10Y–2Y spread (percentage points)','treasuries'],
    ['T10Y3M','10Y–3M spread (percentage points)','treasuries'],['BAMLH0A0HYM2','HY OAS (%)','credit'],
    ['BAMLC0A4CBBB','BBB corporate OAS (%)','credit'],['BAMLC0A0CM','IG OAS (%)','credit'],
    ['UNRATE','US unemployment (%)','macro'],['CPIAUCSL','Consumer price index (level)','macro'],
    ['GDP','Nominal GDP ($bn)','macro'],['UMCSENT','Consumer sentiment index','macro'],['INDPRO','Industrial production index','macro'],
    ['WALCL','Fed assets ($mn)','liquidity'],['RRPONTSYD','Reverse repo ($bn)','liquidity'],
    ['SOFR','SOFR (%)','liquidity'],['EFFR','Effective fed funds rate (%)','liquidity'],
  ].map(([id,name,cat])=>({id,name,cat,signal:'inverse',weight:50,isEcb:false}));
  const EURO_METRICS = [
    ['ECB.CISS.EA','Euro-area CISS (index)'],['ECB.UNEMP.EA','Euro-area unemployment (%)'],
    ['ECB.REFI','ECB main refinancing rate (%)'],['ECB.DEPO','ECB deposit facility rate (%)'],
    ['ECB.MARG','ECB marginal lending rate (%)'],['ECB.M3','Euro-area M3 growth (YoY %)'],
  ].map(([id,name])=>({id,name,cat:'ecb',signal:'inverse',weight:50,isEcb:true}));
  const $=id=>document.getElementById(id);
  const esc=value=>String(value==null?'':value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const finite=value=>typeof value==='number'&&Number.isFinite(value);
  const fmt=value=>finite(value)?value.toLocaleString('en-US',{maximumFractionDigits:4}):'Unavailable';
  const entries=value=>value&&typeof value==='object'&&!Array.isArray(value)?Object.entries(value):[];
  const blankChanges=()=>Object.fromEntries(['1W','1M','3M','6M','1Y'].map(k=>[k,null]));
  let metrics=[],metricsData={},documents={},fredResponses={},sourceErrors={},activeCategory='all',refreshing=false;

  function loadConfig(){
    let saved;
    try{saved=JSON.parse(localStorage.getItem(CONFIG_KEY));}catch(_){saved=null;}
    const valid=Array.isArray(saved)?saved.filter(m=>m&&typeof m.id==='string'&&/^[A-Z0-9._-]{1,40}$/.test(m.id)&&typeof m.name==='string'):null;
    metrics=valid&&valid.length?valid.map(m=>({...m,weight:finite(m.weight)?Math.max(0,Math.min(100,m.weight)):50})):DEFAULTS.map(m=>({...m}));
    for(const m of EURO_METRICS)if(!metrics.some(row=>row.id===m.id))metrics.push({...m});
    for(const m of metrics){
      if(m.id==='VXVCLS'&&m.name==='VVIX (Vol of Vol)')m.name='S&P 500 3-month volatility index';
      if(m.id==='GDP'&&m.name==='Real GDP')m.name='Nominal GDP ($bn)';
    }
  }
  function saveConfig(){try{localStorage.setItem(CONFIG_KEY,JSON.stringify(metrics));}catch(_){}}
  function objectView(value){return value==null?'<p>Unavailable from the current engine response.</p>':`<pre class="engine-fields">${esc(JSON.stringify(value,null,2))}</pre>`;}
  function snapshotStatus(doc){
    if(!doc)return 'Unavailable';
    const age=(Date.now()-Date.parse(doc.generated_at))/3600000;
    return `${doc.generated_at||'Timestamp not supplied'}${!Number.isFinite(age)||age<0?' · invalid timestamp':age>48?' · STALE SNAPSHOT':` · ${age.toFixed(1)}h old`}`;
  }
  async function engine(name){
    try{
      const response=await fetch(API+ENGINE_SOURCES[name],{cache:'no-store'});
      if(!response.ok)throw Error('unavailable');
      const doc=await response.json();
      const valid=name==='detail'?doc&&doc.schema_version==='1.0'&&doc.method==='ecb_eurosystem_liquidity_detail':doc&&doc.engine==='ecb-derived'&&typeof doc.version==='string'&&doc.indicators&&typeof doc.indicators==='object';
      if(!valid)throw Error('schema');
      documents[name]=doc;
    }catch(_){sourceErrors[name]='No verified engine response.';}
  }

  // Calendar windows use actual observation dates. Monthly/quarterly series
  // never receive daily-observation offsets masquerading as weeks/months.
  function fredMetric(doc){
    const bars=doc.bars.filter(p=>p&&finite(p.value)&&typeof p.date==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(p.date)&&Number.isFinite(Date.parse(p.date)))
      .sort((a,b)=>a.date.localeCompare(b.date));
    if(!bars.length)return null;
    const last=bars[bars.length-1],end=new Date(last.date+'T00:00:00Z'),changes=blankChanges(),comparison_dates={};
    const gaps=bars.slice(1).map((p,i)=>(Date.parse(p.date)-Date.parse(bars[i].date))/86400000).filter(n=>n>0).sort((a,b)=>a-b);
    const cadence=gaps.length?gaps[Math.floor(gaps.length/2)]:Infinity;
    for(const [label,months,days] of [['1W',0,7],['1M',1,0],['3M',3,0],['6M',6,0],['1Y',12,0]]){
      const target=new Date(end);
      if(months){const day=target.getUTCDate();target.setUTCDate(1);target.setUTCMonth(target.getUTCMonth()-months);const limit=new Date(Date.UTC(target.getUTCFullYear(),target.getUTCMonth()+1,0)).getUTCDate();target.setUTCDate(Math.min(day,limit));}
      else target.setUTCDate(target.getUTCDate()-days);
      const windowDays=(end-target)/86400000;
      if(cadence>windowDays*1.5)continue;
      const baseline=bars.filter(p=>p.date<last.date).sort((a,b)=>Math.abs(Date.parse(a.date)-target)-Math.abs(Date.parse(b.date)-target))[0];
      if(!baseline||Math.abs(target-Date.parse(baseline.date))/86400000>Math.max(3,cadence/2)||baseline.value===0)continue;
      changes[label]=(last.value-baseline.value)/Math.abs(baseline.value)*100;
      comparison_dates[label]=baseline.date;
    }
    return {current:last.value,date:last.date,source:'FRED '+doc.series,changes,comparison_dates};
  }
  async function fetchFredSeries(id){
    try{
      const response=await fetch(`${API}/fred?series=${encodeURIComponent(id)}&obs=600`);
      if(!response.ok)return;
      const doc=await response.json();
      if(doc.series!==id||!Array.isArray(doc.bars))return;
      fredResponses[id]=doc;
      const metric=fredMetric(doc);if(metric)metricsData[id]=metric;
    }catch(_){}
  }
  function mapEurope(){
    const detail=documents.detail,derived=documents.derived,ind=derived?.indicators||{};
    function add(id,value,date,source){if(finite(value))metricsData[id]={current:value,date:date||'Observation date not supplied',source,changes:blankChanges()};}
    const rates=detail?.policy_rates||{};
    add('ECB.REFI',rates.main_refinancing_pct,detail?'Snapshot '+detail.generated_at:null,'justhodl-ecb-detail');
    add('ECB.DEPO',rates.deposit_facility_pct,detail?'Snapshot '+detail.generated_at:null,'justhodl-ecb-detail');
    add('ECB.MARG',rates.marginal_lending_pct,detail?'Snapshot '+detail.generated_at:null,'justhodl-ecb-detail');
    const points=derived?.charts?.ciss_level?.points;
    add('ECB.CISS.EA',ind.ciss_acceleration?.ciss_level,Array.isArray(points)&&points.length?points[points.length-1][0]:null,'justhodl-ecb-derived');
    add('ECB.UNEMP.EA',ind.ea_unemployment?.unemployment_rate_pct,ind.ea_unemployment?.as_of,'justhodl-ecb-derived');
    add('ECB.M3',derived?.credit?.m3_yoy,derived?.credit?.m3_as_of,'justhodl-ecb-derived');
  }
  async function refreshAll(){
    if(refreshing)return;
    refreshing=true;metricsData={};documents={};fredResponses={};sourceErrors={};
    $('lastUpdate').textContent='Loading…';render();
    try{
      await Promise.allSettled([engine('detail'),engine('derived'),...metrics.filter(m=>!m.isEcb&&!m.id.startsWith('ECB.')).map(m=>fetchFredSeries(m.id))]);
      mapEurope();render();
      $('lastUpdate').textContent=`${Object.keys(documents).length}/2 ECB engines · ${Object.keys(metricsData).length}/${metrics.length} metrics`;
    }finally{refreshing=false;}
  }

  function metricRows(list,main=false){return list.map(m=>{
    const d=metricsData[m.id],id=esc(m.id);
    const name=`<button class="metric-name" data-edit="${id}">${esc(m.name)}</button><div class="metric-asof">${esc(d?`${d.date} · ${d.source}`:'No valid observation')}</div>`;
    const periods=main?['1W','1M','3M','6M','1Y']:['1W','1M','3M','1Y'];
    return `<tr><td>${d?'●':'—'}</td><td>${name}</td>${main?`<td>${esc(m.cat)}</td>`:''}<td class="m-val">${fmt(d?.current)}</td>`+
      periods.map(p=>`<td title="${esc(d?.comparison_dates?.[p]?'Baseline '+d.comparison_dates[p]:'Comparison unavailable')}">${finite(d?.changes?.[p])?fmt(d.changes[p])+'%':'—'}</td>`).join('')+
      (main?`<td><input type="range" min="0" max="100" value="${m.weight}" data-weight="${id}" aria-label="Local display weight"><span>${m.weight}</span></td>`:'')+'</tr>';
  }).join('');}
  function bindMetricControls(){
    document.querySelectorAll('[data-edit]').forEach(el=>el.addEventListener('click',()=>editName(el.dataset.edit)));
    document.querySelectorAll('[data-weight]').forEach(el=>el.addEventListener('change',()=>updateWeight(el.dataset.weight,el.value)));
  }
  function renderMainTable(){
    const q=($('searchBox').value||'').toLowerCase();
    $('mainBody').innerHTML=metricRows(metrics.filter(m=>(activeCategory==='all'||m.cat===activeCategory)&&`${m.name} ${m.id}`.toLowerCase().includes(q)).sort((a,b)=>b.weight-a.weight),true);
    bindMetricControls();
  }
  function renderEurope(){
    const detail=documents.detail,derived=documents.derived,ind=derived?.indicators||{};
    $('engine-status').textContent=`justhodl-ecb-detail: ${snapshotStatus(detail)} · justhodl-ecb-derived: ${snapshotStatus(derived)}`;
    $('detail-json').textContent=detail?JSON.stringify(detail,null,2):sourceErrors.detail||'Awaiting response.';
    $('derived-json').textContent=derived?JSON.stringify(derived,null,2):sourceErrors.derived||'Awaiting response.';
    $('fred-json').textContent=JSON.stringify(fredResponses,null,2);
    $('ecbRatesGrid').innerHTML=EURO_METRICS.filter(m=>['ECB.REFI','ECB.DEPO','ECB.MARG','ECB.M3'].includes(m.id)).map(m=>{
      const d=metricsData[m.id];return `<div class="ecb-card"><div class="ecb-card-label">${esc(m.name)}</div><div class="ecb-card-val">${fmt(d?.current)}</div><div class="metric-asof">${esc(d?.date||'No valid observation')}</div></div>`;
    }).join('');
    const ciss=metricsData['ECB.CISS.EA'];
    $('ecbCissMain').textContent=fmt(ciss?.current);
    $('ecbCissStatus').textContent=ind.ciss_acceleration?.signal||'Unknown';
    $('ecbCissGrid').innerHTML=objectView(ind.ciss_acceleration);
    $('ecbUnempGrid').innerHTML=entries(ind.country_unemployment?.countries).map(([cc,r])=>`<div class="ecb-card"><div>${esc(cc)} unemployment</div><div class="ecb-card-val">${fmt(r.rate_pct)}%</div><div>3M change ${fmt(r.chg_3m_pp)} percentage points</div><div class="metric-asof">${esc(r.as_of||'Date unavailable')}</div></div>`).join('')||'<p>Country unemployment unavailable.</p>';
    $('ecbFxGrid').innerHTML=objectView(derived?.fx);
    $('ecbFullBody').innerHTML=EURO_METRICS.map(m=>{const d=metricsData[m.id];return `<tr><td>${d?'●':'—'}</td><td>${esc(m.name)}</td><td>ECB</td><td>${fmt(d?.current)}</td><td>See complete engine fields</td><td>${esc(d?.date||'Unavailable')}</td></tr>`;}).join('');
    $('ecbAiSummary').textContent=detail?.headline||'ECB detail headline unavailable.';
    $('analysisText').innerHTML=objectView(detail?{liquidity:detail.liquidity,balance_sheet:detail.balance_sheet,policy_rates:detail.policy_rates,sources:detail.sources,errors:detail.errors}:null);
    $('analysisEcbText').innerHTML=entries(ind).map(([key,value])=>`<details><summary>${esc(value?.name||key)} · ${esc(value?.signal||value?.tier||'No status supplied')}</summary>${objectView(value)}</details>`).join('')||'ECB-derived indicators unavailable.';
    $('riskSignals').textContent=derived?`Published flashing indicators (${derived.n_flashing??'unknown'}): ${Array.isArray(derived.flashing)?derived.flashing.join(', ')||'None reported':'Unavailable'}. Missing indicators do not imply low risk.`:'No verified engine risk signals.';
    $('aiReport').textContent=derived?.headline||'ECB-derived headline unavailable.';
    $('aiEcbReport').innerHTML=objectView(derived?.ai_brief);
    $('aiSignals').innerHTML=objectView(derived?{dump_score:derived.dump_score,event_study:derived.event_study,signal_logged:derived.signal_logged}:null);
    $('statCritical').textContent=entries(ind).filter(([,r])=>['CRITICAL','ACUTE','TAIL_RISK','BLACK_SWAN'].includes(r?.signal)).length;
    $('statWarning').textContent=entries(ind).filter(([,r])=>['WATCH','ELEVATED','TIGHTENING','SEVERE_TIGHTENING','CREDIT_STRESS'].includes(r?.signal)).length;
    $('statNormal').textContent=entries(ind).filter(([,r])=>['NORMAL','CALM'].includes(r?.signal)).length;
    $('statEcb').textContent=entries(ind).length;
    if(!derived)for(const id of ['statCritical','statWarning','statNormal','statEcb'])$(id).textContent='—';
    const esi=ind.eurodollar_stress_index,score=esi?.esi_0_100,valid=finite(score)&&score>=0&&score<=100;
    $('kiNum').textContent=valid?fmt(score):'—';$('kiLabel').textContent='ECB ESI';
    $('kiRegime').textContent=valid?esi.tier||'No tier supplied':'ENGINE SCORE UNAVAILABLE';
    $('kiSignal').textContent=valid?`Published ECB-derived score · ${snapshotStatus(derived)}`:'No substitute score is calculated from missing inputs.';
    $('kiRing').style.strokeDashoffset=valid?364.42*(1-score/100):364.42;
  }
  function render(){
    renderMainTable();
    for(const [cat,id]of Object.entries({volatility:'volBody',treasuries:'trsBody',credit:'crdBody',macro:'macBody',liquidity:'liqBody'}))$(id).innerHTML=metricRows(metrics.filter(m=>m.cat===cat));
    bindMetricControls();renderEurope();renderSettings();
    $('metricCount').textContent=`${Object.keys(metricsData).length} available metrics`;
  }
  function buildCatPills(){
    $('catBar').innerHTML='';
    for(const cat of new Set(['all',...metrics.map(m=>m.cat)])){
      const el=document.createElement('button');el.className='cat-pill'+(cat===activeCategory?' active':'');el.textContent=cat;el.addEventListener('click',()=>filterCat(cat));$('catBar').appendChild(el);
    }
  }
  function filterCat(cat){activeCategory=cat;buildCatPills();renderMainTable();}
  function switchTab(id){document.querySelectorAll('.tab').forEach(el=>el.classList.toggle('active',el.dataset.tab===id));document.querySelectorAll('.panel').forEach(el=>el.classList.toggle('active',el.id==='panel-'+id));}
  function renderSettings(){
    for(const [id,direction]of [['greenList','normal'],['redList','inverse']]){
      $(id).innerHTML=metrics.filter(m=>m.signal===direction).map(m=>`<div>${esc(m.name)} <button class="btn" data-signal="${esc(m.id)}">Change local label</button></div>`).join('')||'No metrics.';
    }
    document.querySelectorAll('[data-signal]').forEach(el=>el.addEventListener('click',()=>toggleSignal(el.dataset.signal)));
  }
  function editName(id){const m=metrics.find(m=>m.id===id);if(!m)return;const name=window.prompt('Display name',m.name);if(name&&name.trim()){m.name=name.trim();saveConfig();render();}}
  function updateWeight(id,value){const m=metrics.find(m=>m.id===id),weight=Number(value);if(m&&finite(weight)){m.weight=Math.max(0,Math.min(100,weight));saveConfig();renderMainTable();}}
  function toggleSignal(id){const m=metrics.find(m=>m.id===id);if(m){m.signal=m.signal==='normal'?'inverse':'normal';saveConfig();renderSettings();}}
  function addMetricModal(){$('addModal').classList.add('show');}
  function closeModal(){$('addModal').classList.remove('show');}
  function addMetric(){
    const id=$('newMetricId').value.trim().toUpperCase(),name=$('newMetricName').value.trim();
    if(!/^[A-Z0-9._-]{1,40}$/.test(id)||id.startsWith('ECB.')||!name)return window.alert('Enter a valid FRED series ID and display name. ECB metrics come from the dedicated engines.');
    if(metrics.some(m=>m.id===id))return window.alert('This metric is already listed.');
    const weight=Number($('newMetricWeight').value);
    metrics.push({id,name,cat:$('newMetricCat').value,signal:$('newMetricSignal').value,weight:finite(weight)?Math.max(0,Math.min(100,weight)):50,isEcb:false});
    saveConfig();buildCatPills();closeModal();refreshAll();
  }
  function exportData(){
    const payload={exported_at:new Date().toISOString(),engine_sources:ENGINE_SOURCES,documents,fred_responses:fredResponses,metrics,observations:metricsData,source_errors:sourceErrors};
    const url=URL.createObjectURL(new Blob([JSON.stringify(payload,null,2)],{type:'application/json'}));const link=document.createElement('a');link.href=url;link.download='justhodl-euro-research.json';link.click();URL.revokeObjectURL(url);
  }
  Object.assign(window,{refreshAll,filterMetrics:renderMainTable,filterCat,switchTab,addMetricModal,closeModal,addMetric,editName,updateWeight,toggleSignal,exportData});
  document.addEventListener('DOMContentLoaded',()=>{loadConfig();buildCatPills();document.querySelectorAll('.tab').forEach(el=>el.addEventListener('click',()=>switchTab(el.dataset.tab)));refreshAll();});
}());
