(function (root) {
  'use strict';
  const HEAD = 'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX', DAY = 86400000, MAX = 64 * 1024 * 1024;
  const esc = value => String(value == null ? '—' : value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  const fmt = (v, digits = 5) => finite(v) ? v.toLocaleString('en-US', {maximumFractionDigits: digits}) : '—';
  const path = key => typeof key === 'string' && /^data\/[A-Za-z0-9_./-]+$/.test(key) && !key.includes('..') ? '/'+key : null;
  function state(row, packet, now = Date.now()) {
    const q = row.quality || {}, end = Date.parse(row.observation_period_end+'T00:00:00Z');
    const age = (Math.floor(now / DAY)*DAY-end)/DAY, acquired = now-Date.parse(row.acquired_at), generated=now-Date.parse(packet.generated_at);
    if (packet.contract !== 'ciss-research.v1') return 'unverified';
    if (![age, acquired, generated].every(Number.isFinite) || age<0 || acquired<0 || generated<0) return 'invalid';
    if (q.status !== 'fresh') return q.status || 'unavailable';
    if (!finite(row.latest)) return 'missing';
    if (!finite(q.maximum_observation_age_days) || !finite(q.maximum_acquisition_age_seconds)) return 'unverified';
    return age>q.maximum_observation_age_days || acquired>q.maximum_acquisition_age_seconds*1000 || generated>72*3600000 ? 'stale' : 'fresh';
  }
  function pointsPath(points, width=880, height=220) {
    const timed = (points||[]).map(p=>({time:Date.parse(p[0].length===7?p[0]+'-01':p[0]), value:p[1]})).filter(p=>Number.isFinite(p.time));
    const valid=timed.filter(p=>finite(p.value));
    if (!valid.length) return {d:'', min:null, max:null, first:null, last:null};
    const min=Math.min(...valid.map(p=>p.value)),max=Math.max(...valid.map(p=>p.value));
    const first=timed[0].time,last=timed[timed.length-1].time,span=last-first||1,range=max-min||1;
    let pen=false;
    const d=timed.map(p=>{
      if(!finite(p.value)){pen=false;return '';}
      const command=pen?'L':'M';pen=true;
      return command+((p.time-first)/span*width).toFixed(2)+','+(height-(p.value-min)/range*height).toFixed(2);
    }).join(' ');
    return {d,min,max,first,last};
  }
  function csvRows(text) {
    const rows=[];let row=[],cell='',quoted=false;
    text=text.replace(/^\uFEFF/,'');
    for(let i=0;i<text.length;i++){
      const c=text[i];
      if(c==='"'){if(quoted&&text[i+1]==='"'){cell+='"';i++;}else quoted=!quoted;}
      else if(!quoted&&(c===','||c==='\n'||c==='\r')){
        row.push(cell);cell='';
        if(c!==','){if(row.some(v=>v!==''))rows.push(row);row=[];if(c==='\r'&&text[i+1]==='\n')i++;}
      }else cell+=c;
    }
    if(quoted)throw Error('Incomplete quoted CSV field');
    if(cell||row.length){row.push(cell);rows.push(row);}
    const names=rows.shift();if(!names||!names.includes('KEY'))throw Error('ECB CSV header missing');
    return rows.map(values=>Object.fromEntries(names.map((name,i)=>[name,values[i]||''])));
  }
  async function bounded(stream, bound=MAX) {
    const reader=stream.getReader(),chunks=[];let total=0;
    try{while(true){const {value,done}=await reader.read();if(done)break;total+=value.byteLength;if(total>bound)throw Error('Source exceeds size limit');chunks.push(value);}}
    finally{await reader.cancel().catch(()=>{});}
    const out=new Uint8Array(total);let offset=0;for(const chunk of chunks){out.set(chunk,offset);offset+=chunk.length;}return out;
  }
  async function verifiedRow(row, fetcher=root.fetch.bind(root)) {
    const e=row.evidence||{},url=path(e.key);
    if(!url||e.contract!=='source-evidence.v1'||e.provider!=='ecb'||e.captured!==true||!Number.isInteger(e.bytes)||e.bytes<1||e.bytes>MAX)throw Error('Original receipt unavailable');
    const sha=async raw=>Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),b=>b.toString(16).padStart(2,'0')).join('');
    const requestSha=await sha(new TextEncoder().encode(e.source_url));
    if(e.key!=='data/evidence/ecb/'+requestSha+'/'+e.sha256+'.bin.gz')throw Error('Source identity differs');
    const response=await fetcher(url,{cache:'no-store'});if(!response.ok)throw Error('Original source HTTP '+response.status);
    const zipped=await bounded(response.body);
    const raw=await bounded(new Blob([zipped]).stream().pipeThrough(new DecompressionStream('gzip')));
    if(raw.length!==e.bytes||await sha(raw)!==e.sha256)throw Error('Original source hash differs');
    const original=csvRows(new TextDecoder('utf-8',{fatal:true}).decode(raw))[row.source_row];
    if(!original||original.KEY!==row.key||original.TIME_PERIOD!==row.latest_date||original.OBS_STATUS!==row.observation_status||original.UNIT!=='PURE_NUMB'||original.UNIT_MULT!=='0')throw Error('Selected original row differs');
    if(row.latest_decimal!==null&&row.latest_decimal!==undefined&&Number(original.OBS_VALUE)!==row.latest)throw Error('Selected observation value differs');
    return {sha256:e.sha256,source_row:row.source_row,original};
  }
  const api={state,pointsPath,csvRows,verifiedRow,path};
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(!root.document)return;
  const doc=root.document,$=id=>doc.getElementById(id);
  if(!$('ciss-table'))return;
  const names={ea_headline:'Euro-area composite',ea_subindex:'Euro-area contributions',sovereign_ea:'Euro-area SovCISS',country_ciss:'Country CISS',sovereign_country:'Country SovCISS',clifs:'CLIFS',other:'Other / legacy methodology'};
  let packet=null,narrative=null,signature='',selected=null;
  async function get(key){const url=path(key),current=['data/ciss-stress.json','data/ciss-ai.json'].includes(key);const response=await fetch(url+(current?'?exact=1':''),{cache:'no-store'});if(!response.ok)throw Error('HTTP '+response.status);return response.json();}
  const link=(key,label)=>path(key)?`<a href="${esc(path(key))}" target="_blank" rel="noopener">${esc(label)}</a>`:'';
  function renderNarrative(){
    const head=packet.series.find(r=>r.key===HEAD),current=head&&state(head,packet)==='fresh'&&packet.quality?.status==='fresh';
    const bound=narrative?.contract==='ciss-commentary.v1'&&narrative.source_replay?.manifest_key===packet.replay?.manifest_key&&narrative.source_replay?.output_sha256===packet.replay?.output_sha256;
    $('ciss-read').textContent=current?`ECB euro-area CISS measured ${head.latest_decimal} on ${head.latest_date}.`:'A current qualified euro-area headline is unavailable.';
    $('ciss-reconcile').textContent=current?`${packet.headline_reconciliation.status} · ${packet.headline_reconciliation.date} · residual ${packet.headline_reconciliation.residual_decimal??'unavailable'}`:'Headline unavailable; inspect source quality below.';
    $('ciss-narrative').textContent=bound&&current?narrative.interpretation.stress_source:'Five market contributions plus the signed correlation contribution form the composite. Each exact ECB methodology retains its own dates.';
    $('ciss-commentary-proof').innerHTML=bound?link(narrative.replay?.manifest_key,'Commentary replay'):'Commentary is awaiting this source run; an older interpretation is withheld.';
  }
  function render(){
    if(!packet)return;
    const query=$('ciss-search').value.toLowerCase(),group=$('ciss-category').value,status=$('ciss-quality').value;
    const rows=packet.series.filter(r=>(!query||[r.key,r.label,r.country].join(' ').toLowerCase().includes(query))&&(!group||r.category===group)&&(!status||state(r,packet)===status));
    rows.sort((a,b)=>a.country.localeCompare(b.country)||a.key.localeCompare(b.key));
    $('ciss-count').textContent=`${rows.length} shown · ${packet.n_series} compiled / ${packet.discovered_series} discovered · ${Object.keys(packet.errors||{}).length} collection errors`;
    $('ciss-table-body').innerHTML=rows.map(r=>{const s=state(r,packet),live=s==='fresh';return `<tr><td><button class="series-button" data-key="${esc(r.key)}">${esc(r.country)}</button><span class="series-label">${esc(r.source_metadata.TITLE||r.label)}</span><code>${esc(r.key)}</code></td><td>${live?esc(r.latest_decimal):'—'}<small>${esc(r.unit)}</small></td><td>${esc(r.latest_date)}<small>${esc(r.freq)} · status ${esc(r.observation_status)}</small></td><td><span class="quality ${s==='fresh'?'fresh':''}">${esc(s)}</span><small>${r.quality.observation_age_days} days at collection</small></td><td>${live?fmt(r.chg_1y):'—'}<small>index points · dated baseline</small></td><td>${live?fmt(r.percentile_3y,1):'—'}<small>within this series · %</small></td></tr>`;}).join('')||'<tr><td colspan="6">No matching series.</td></tr>';
    $('ciss-table-body').querySelectorAll('[data-key]').forEach(button=>button.addEventListener('click',()=>openSeries(button.dataset.key)));
    renderNarrative();
    signature=packet.series.map(r=>state(r,packet)).join('|');
  }
  function openSeries(key){
    const r=packet.series.find(row=>row.key===key);if(!r)return;selected=key;
    const s=state(r,packet),live=s==='fresh',curve=pointsPath(r.chart_points),baseline=r.annual_comparison;
    $('ciss-detail-title').textContent=r.country+' · '+(names[r.category]||r.category);
    $('ciss-detail-body').innerHTML=`<p>${esc(r.label)}</p><code>${esc(r.key)}</code><p class="quality">${esc(s)} · ${esc(r.unit)} · latest source period ${esc(r.latest_date)} · status ${esc(r.observation_status)}</p>
      <dl><div><dt>Qualified value</dt><dd>${live?esc(r.latest_decimal):'unavailable'}</dd></div><div><dt>Acquired at</dt><dd>${esc(r.acquired_at)}</dd></div><div><dt>Last numeric source row</dt><dd>${fmt(r.last_observed_value)} · ${esc(r.last_observed_date)} (historical)</dd></div><div><dt>Source publication time</dt><dd>Not independently available</dd></div></dl>
      <p>History: ${esc(r.start_date)} → ${esc(r.latest_date)} · ${r.n_obs} source rows / ${r.n_numeric} numeric. ${esc(r.chart_aggregation)}. Horizontal spacing uses dates; missing rows break the line.</p>
      ${curve.d?`<svg class="history-chart" viewBox="0 0 980 270" role="img" aria-label="${esc(r.key)} retrieved-vintage history"><text x="5" y="20">${fmt(curve.max,4)}</text><text x="5" y="232">${fmt(curve.min,4)}</text><path transform="translate(85 10)" d="${curve.d}" fill="none" stroke="currentColor" stroke-width="1.5"/><text x="85" y="265">${esc(r.start_date)}</text><text x="965" y="265" text-anchor="end">${esc(r.latest_date)}</text></svg>`:'<p>No numeric history available.</p>'}
      <p>Annual change ${live?fmt(r.chg_1y):'unavailable'} index points; baseline ${fmt(baseline.baseline_value)} on ${esc(baseline.baseline_period)}. Target ${esc(baseline.target_date)}. ${esc(r.distribution_definition)}.</p>
      <p>Observation ceiling ${r.quality.maximum_observation_age_days} days; acquisition ceiling ${r.quality.maximum_acquisition_age_seconds/3600} hours. The exact release calendar is not yet verified. ${r.quality.excluded_future_rows} future rows excluded.</p>
      <p class="links">${link(r.evidence.key,'Original ECB CSV (.gz)')} ${link(packet.replay.manifest_key,'Run and compiler receipt')}</p>
      <button id="ciss-verify-original" class="action">Verify and inspect the original row</button><pre id="ciss-original" aria-live="polite"></pre>
      <details><summary>Exact source definition and quality fields</summary><pre>${esc(JSON.stringify({metadata:r.source_metadata,quality:r.quality,annual_comparison:r.annual_comparison,evidence:r.evidence},null,2))}</pre></details>`;
    $('ciss-verify-original').addEventListener('click',async event=>{
      const button=event.currentTarget,display=$('ciss-original');button.disabled=true;display.textContent='Checking the original CSV bytes and selected row…';
      try{const result=await verifiedRow(r);if(selected===key)display.textContent='VERIFIED original bytes and selected row\n'+JSON.stringify(result,null,2);}
      catch(error){if(selected===key)display.textContent='Verification failed: '+error.message;}
      finally{button.disabled=false;}
    });
    if(!$('ciss-detail').open)$('ciss-detail').showModal();
  }
  let auxiliaryData=[];
  function renderSupplementary(){
    $('ciss-aux').innerHTML=auxiliaryData.map(({id,label,unit,key,data,error})=>{
      if(error)return `<article><h3>${esc(label)}</h3><p>Unavailable · ${esc(error)}</p>${link(key,'Auxiliary packet')}</article>`;
      const q=data.quality||{},age=(Math.floor(Date.now()/DAY)*DAY-Date.parse(q.period_end+'T00:00:00Z'))/DAY;
      const generated=Date.now()-Date.parse(data.generated_at),expected=id==='eurusd'?'USD':'EUR_bn';
      const valid=data.unit===expected&&q.status==='fresh'&&finite(age)&&age>=0&&age<=q.max_age_days&&generated>=0&&generated<=72*3600000;
      return `<article><h3>${esc(label)}</h3><p>${valid?fmt(data.latest):'Unavailable'} <small>${esc(unit)} · ${esc(data.latest_date)}</small></p><p class="muted">${valid?'Dated auxiliary measurement':'Stale, unavailable or unit mismatch'}; original-source replay for this auxiliary is pending.</p>${link(key,'Inspect auxiliary packet')}</article>`;
    }).join('');
  }
  async function supplementary(){
    const specs=[['eurusd','EUR/USD reference rate','USD per EUR'],['ilm_usd_claims','Claims on euro-area residents in foreign currency','EUR bn'],['fx_claims_nonea','Claims on non-euro-area residents in foreign currency','EUR bn']];
    auxiliaryData=await Promise.all(specs.map(async([id,label,unit])=>{
      const key='data/ecb-hist/'+id+'.json';
      try{return {id,label,unit,key,data:await get(key)};}
      catch(error){return {id,label,unit,key,error:error.message};}
    }));renderSupplementary();
  }
  async function load(){
    $('ciss-status').textContent='Loading retained ECB measurements…';
    try{
      const [source,comment]=await Promise.all([get('data/ciss-stress.json'),get('data/ciss-ai.json').catch(()=>null)]);
      if(source.contract!=='ciss-research.v1'||!Array.isArray(source.series)||!path(source.replay?.manifest_key))throw Error('Source contract or replay receipt unavailable');
      packet=source;narrative=comment;
      $('ciss-status').textContent='Collected '+source.generated_at+' · current retrieved vintage; historical as-known-at releases are not reconstructed.';
      $('ciss-proofs').innerHTML=link('data/ciss-stress.json','Full warehouse')+link(packet.replay.manifest_key,'Source replay manifest')+link('data/ops/releases/justhodl-ciss-stress.json','Current warehouse runtime');
      $('ciss-errors').textContent=JSON.stringify(source.errors,null,2);render();if($('ciss-detail').open)openSeries(selected);await supplementary();
    }catch(error){packet=null;auxiliaryData=[];$('ciss-status').textContent='Measurements unavailable: '+error.message;$('ciss-table-body').innerHTML='';$('ciss-count').textContent='';$('ciss-aux').innerHTML='';$('ciss-errors').textContent='Unavailable';$('ciss-read').textContent='A current qualified ECB source is unavailable.';$('ciss-proofs').innerHTML='';$('ciss-reconcile').textContent='Unavailable';$('ciss-narrative').textContent='';$('ciss-commentary-proof').textContent='';$('ciss-detail').close();}
  }
  $('ciss-close').addEventListener('click',()=>$('ciss-detail').close());
  $('ciss-detail').addEventListener('click',e=>{if(e.target===$('ciss-detail'))$('ciss-detail').close();});
  $('ciss-search').addEventListener('input',render);$('ciss-category').addEventListener('change',render);$('ciss-quality').addEventListener('change',render);
  $('ciss-refresh').addEventListener('click',load);
  const requestedSeries=new URLSearchParams(root.location.search).get('series');
  if(requestedSeries)$('ciss-search').value=requestedSeries.slice(0,120);
  setInterval(()=>{if(packet)renderSupplementary();if(packet&&signature!==packet.series.map(r=>state(r,packet)).join('|')){render();if($('ciss-detail').open)openSeries(selected);}},60000);
  load();
})(typeof window!=='undefined'?window:globalThis);
