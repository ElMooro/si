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
  /* A number for reading: four decimals as the ECB publishes them; the exact source decimal stays in the detail panel. */
  function tidy(value, digits = 4) {
    if (!finite(value)) return '—';
    if (value !== 0 && Math.abs(value) < 0.5 / 10 ** digits) return value.toPrecision(2).replace(/e-(\d+)$/, ' × 10⁻$1').replace(/⁻(\d+)$/, (_, n) => '⁻' + n.replace(/\d/g, d => '⁰¹²³⁴⁵⁶⁷⁸⁹'[d]));
    return value.toLocaleString('en-US', {minimumFractionDigits: digits, maximumFractionDigits: digits});
  }
  const ENDED_DAYS = 400;
  /* What a row really is: fresh, blank (the ECB printed its newest row without a value), ended (no row for over 400 days),
     or whatever state() says.  A blank or ended row still has a last value with its own date. */
  function lifecycle(row, packet, now = Date.now()) {
    const s = state(row, packet, now), end = Date.parse(row.observation_period_end + 'T00:00:00Z');
    const days = (Math.floor(now / DAY) * DAY - end) / DAY;
    if (s === 'missing' && finite(row.last_observed_value) && row.last_observed_date && row.last_observed_date < row.latest_date) return 'blank';
    if ((s === 'stale' || s === 'missing') && Number.isFinite(days) && days > ENDED_DAYS) return 'ended';
    return s;
  }
  const LIFE = {fresh: 'current', blank: 'newest row blank at the ECB', ended: 'ended at the ECB', stale: 'late', missing: 'missing', incomplete: 'incomplete history response'};
  function headline(head, packet, now = Date.now()) {
    const current = !!head && state(head, packet, now) === 'fresh' && (packet.quality || {}).status === 'fresh';
    if (!current) return {current: false, text: 'A current qualified euro-area headline is unavailable.', exact: ''};
    const place = finite(head.pctile) ? (head.pctile >= 50 ? ' — higher than ' + Math.floor(head.pctile) : ' — lower than ' + Math.floor(100 - head.pctile)) + '% of its readings since ' + String(head.start_date).slice(0, 4) : '';
    return {current: true, text: 'ECB euro-area CISS: ' + tidy(head.latest) + ' on ' + head.latest_date + place + '.',
            exact: 'Exact source value ' + head.latest_decimal + (finite(head.max) ? ' · highest reading on record ' + tidy(head.max) : '') + ' · scale 0 to 1'};
  }
  function reconciliation(r) {
    if (!r || r.status === 'unavailable') return {text: 'Not available for this run.', detail: ''};
    if (r.status !== 'matched') return {text: 'MISMATCH on ' + r.date + ' — the contributions do not add up to the composite.', detail: 'difference ' + r.residual_decimal + ', tolerance ' + r.tolerance_decimal};
    return {text: 'Matched on ' + r.date + ' — the six contributions add up to the composite.', detail: 'difference ' + r.residual_decimal + ' (tolerance ' + r.tolerance_decimal + ')'};
  }
  /* An auxiliary measure of the sovereign document against the ECB response it was read from: same bytes, same row, same value. */
  async function verifiedMeasure(m, fetcher=root.fetch.bind(root)) {
    const e=m.evidence||{},url=path(e.key);
    if(!url||e.contract!=='source-evidence.v1'||e.provider!=='ecb'||e.captured!==true||!Number.isInteger(e.bytes)||e.bytes<1||e.bytes>MAX||!Number.isInteger(m.source_row))throw Error('Original receipt unavailable');
    const sha=async raw=>Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),b=>b.toString(16).padStart(2,'0')).join('');
    if(e.key!=='data/evidence/ecb/'+await sha(new TextEncoder().encode(e.source_url))+'/'+e.sha256+'.bin.gz')throw Error('Source identity differs');
    const response=await fetcher(url,{cache:'no-store'});if(!response.ok)throw Error('Original source HTTP '+response.status);
    const zipped=await bounded(response.body);
    const raw=await bounded(new Blob([zipped]).stream().pipeThrough(new DecompressionStream('gzip')));
    if(raw.length!==e.bytes||await sha(raw)!==e.sha256)throw Error('Original source hash differs');
    const original=csvRows(new TextDecoder('utf-8',{fatal:true}).decode(raw))[m.source_row];
    if(!original||original.KEY!==m.key||original.TIME_PERIOD!==m.period||original.OBS_VALUE!==m.exact)throw Error('Selected original row differs');
    return {sha256:e.sha256,source_row:m.source_row,original:{KEY:original.KEY,TIME_PERIOD:original.TIME_PERIOD,OBS_VALUE:original.OBS_VALUE,OBS_STATUS:original.OBS_STATUS,UNIT:original.UNIT,UNIT_MULT:original.UNIT_MULT,TITLE:original.TITLE}};
  }
  const api={state,pointsPath,csvRows,verifiedRow,path,tidy,lifecycle,headline,reconciliation,verifiedMeasure,LIFE};
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
    const line=headline(head,packet),sum=reconciliation(packet.headline_reconciliation);
    $('ciss-read').textContent=line.text;if($('ciss-exact'))$('ciss-exact').textContent=line.exact;
    $('ciss-reconcile').textContent=current?sum.text:'Headline unavailable; inspect source quality below.';if($('ciss-reconcile-detail'))$('ciss-reconcile-detail').textContent=current?sum.detail:'';
    $('ciss-narrative').textContent=bound&&current?narrative.interpretation.stress_source:'Five market contributions plus the signed correlation contribution form the composite. Each exact ECB methodology retains its own dates.';
    $('ciss-commentary-proof').innerHTML=bound?link(narrative.replay?.manifest_key,'Commentary replay'):'Commentary is awaiting this source run; an older interpretation is withheld.';
  }
  function render(){
    if(!packet)return;
    const query=$('ciss-search').value.toLowerCase(),group=$('ciss-category').value,status=$('ciss-quality').value;
    const life=new Map(packet.series.map(r=>[r.key,lifecycle(r,packet)])),order={fresh:0,blank:1,stale:2,incomplete:3,missing:4,ended:5};
    const rows=packet.series.filter(r=>(!query||[r.key,r.label,r.country].join(' ').toLowerCase().includes(query))&&(!group||r.category===group)&&(!status||life.get(r.key)===status));
    rows.sort((a,b)=>(order[life.get(a.key)]??4)-(order[life.get(b.key)]??4)||a.country.localeCompare(b.country)||a.key.localeCompare(b.key));
    const tally=packet.series.reduce((n,r)=>{const k=life.get(r.key);n[k]=(n[k]||0)+1;return n;},{});
    $('ciss-count').textContent=`${rows.length} shown · ${tally.fresh||0} current · ${tally.blank||0} with a blank newest row · ${tally.ended||0} ended at the ECB${tally.stale?` · ${tally.stale} late`:''} · ${Object.keys(packet.errors||{}).length} collection errors`;
    const last=r=>finite(r.last_observed_value)?`${tidy(r.last_observed_value)} on ${esc(r.last_observed_date)}`:'no numeric row';
    $('ciss-table-body').innerHTML=rows.map(r=>{const s=life.get(r.key),live=s==='fresh';
      const value=live?`<span title="exact source value ${esc(r.latest_decimal)}">${tidy(r.latest)}</span>`:s==='blank'?`<span class="past">${tidy(r.last_observed_value)}</span><small>last value, ${esc(r.last_observed_date)} · the ${esc(r.latest_date)} row is blank</small>`:s==='ended'?`<span class="past">${finite(r.last_observed_value)?tidy(r.last_observed_value):'—'}</span><small>last value, ${esc(r.last_observed_date||r.latest_date)}</small>`:`—<small>last ${last(r)}</small>`;
      return `<tr class="${live?'':'dim'}"><td><button class="series-button" data-key="${esc(r.key)}">${esc(r.country)}</button><span class="series-label">${esc(r.source_metadata.TITLE||r.label)}</span><code>${esc(r.key)}</code></td><td>${value}${live?`<small>${esc(r.unit)}</small>`:''}</td><td>${esc(r.latest_date)}<small>${esc(r.freq)} · status ${esc(r.observation_status)}</small></td><td><span class="quality ${live?'fresh':''}">${esc(LIFE[s]||s)}</span><small>${s==='ended'?'no row since '+esc(r.last_observed_date||r.latest_date):r.quality.observation_age_days+' days old at collection'}</small></td><td>${live?(r.chg_1y>0?'+':'')+tidy(r.chg_1y):'—'}<small>index points · dated baseline</small></td><td>${live?fmt(r.percentile_3y,1):'—'}<small>within this series · %</small></td></tr>`;}).join('')||'<tr><td colspan="6">No matching series.</td></tr>';
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
  let auxiliaryData=[],sovereign=null;
  const UNIT_WORDS={usd:'USD per EUR',eur_bn:'EUR bn'},ALLOWED={D:10,W:21,M:100,Q:230};
  function renderSupplementary(){
    if(sovereign){
      const now=Date.now(),items=(sovereign.auxiliaries||[]).map(id=>sovereign.measures[id]).filter(Boolean);
      $('ciss-aux').innerHTML=items.map(m=>{
        const age=(Math.floor(now/DAY)*DAY-Date.parse(m.date+'T00:00:00Z'))/DAY,late=!(age>=0&&age<=ALLOWED[m.freq]);
        return `<article><h3>${esc(m.label)}</h3><p>${fmt(m.value,m.unit==='usd'?4:3)} <small>${esc(UNIT_WORDS[m.unit]||m.unit)} · ${esc(m.date)}${late?' · '+Math.round(age)+' days old':''}</small></p><p class="muted">Read from the ECB response received ${esc(String(m.evidence.first_received_at).slice(0,10))} and kept as it arrived; source value ${esc(m.exact)} for ${esc(m.period)}.</p><code>${esc(m.key)}</code><p class="links">${link(m.evidence.key,'Original ECB response (.gz)')} ${link(sovereign.replay.manifest_key,'Run receipt')}</p><button class="action" data-verify="${esc(m.id)}">Check it against the original</button><pre data-result="${esc(m.id)}" aria-live="polite" hidden></pre></article>`;
      }).join('')||'<article><p>No auxiliary measurement arrived in this run.</p></article>';
      $('ciss-aux').querySelectorAll('[data-verify]').forEach(button=>button.addEventListener('click',async()=>{
        const m=sovereign.measures[button.dataset.verify],display=$('ciss-aux').querySelector(`[data-result="${button.dataset.verify}"]`);
        button.disabled=true;display.hidden=false;display.textContent='Checking the original response…';
        try{display.textContent='VERIFIED: same bytes, same row, same value\n'+JSON.stringify(await verifiedMeasure(m),null,2);}
        catch(error){display.textContent='Verification failed: '+error.message;}
        finally{button.disabled=false;}
      }));
      return;
    }
    $('ciss-aux').innerHTML=auxiliaryData.map(({id,label,unit,key,data,error})=>{
      if(error)return `<article><h3>${esc(label)}</h3><p>Unavailable · ${esc(error)}</p>${link(key,'Auxiliary packet')}</article>`;
      const q=data.quality||{},age=(Math.floor(Date.now()/DAY)*DAY-Date.parse(q.period_end+'T00:00:00Z'))/DAY;
      const generated=Date.now()-Date.parse(data.generated_at),expected=id==='eurusd'?'USD':'EUR_bn';
      const valid=data.unit===expected&&q.status==='fresh'&&finite(age)&&age>=0&&age<=q.max_age_days&&generated>=0&&generated<=72*3600000;
      return `<article><h3>${esc(label)}</h3><p>${valid?fmt(data.latest):'Unavailable'} <small>${esc(unit)} · ${esc(data.latest_date)}</small></p><p class="muted">${valid?'Dated measurement from an older packet':'Stale, unavailable or unit mismatch'}; the ECB response behind this packet was not kept, so it cannot be checked here.</p>${link(key,'Inspect auxiliary packet')}</article>`;
    }).join('');
  }
  async function supplementary(){
    // The sovereign document carries these three with the ECB responses they were read from; the older packets are the fallback.
    try{const doc=await fetch('/data/ciss-sovereign.json',{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('HTTP '+r.status);return r.json();});
      if(doc.contract==='ciss-sovereign.v1'&&(doc.auxiliaries||[]).length&&path(doc.replay?.manifest_key)){sovereign=doc;renderSupplementary();return;}}
    catch(error){}
    sovereign=null;
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
    }catch(error){packet=null;auxiliaryData=[];$('ciss-status').textContent='Measurements unavailable: '+error.message;$('ciss-table-body').innerHTML='';$('ciss-count').textContent='';$('ciss-aux').innerHTML='';$('ciss-errors').textContent='Unavailable';$('ciss-read').textContent='A current qualified ECB source is unavailable.';if($('ciss-exact'))$('ciss-exact').textContent='';if($('ciss-reconcile-detail'))$('ciss-reconcile-detail').textContent='';sovereign=null;$('ciss-proofs').innerHTML='';$('ciss-reconcile').textContent='Unavailable';$('ciss-narrative').textContent='';$('ciss-commentary-proof').textContent='';$('ciss-detail').close();}
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
