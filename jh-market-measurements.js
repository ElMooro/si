(function(root){
 'use strict';
 const esc=v=>String(v??'Unavailable').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const fmt=v=>typeof v==='number'&&Number.isFinite(v)?new Intl.NumberFormat('en-US',{maximumFractionDigits:6}).format(v):'Unavailable';
 const safeKey=key=>typeof key==='string'&&/^data\/(evidence|daily-research)\/[a-zA-Z0-9_./-]+$/.test(key)&&!key.includes('..');
 function status(row,now=Date.now()){
  const q=row.quality||{},obs=Date.parse(row.observed_at),acq=Date.parse(row.acquired_at);
  if(q.original_source_verified!==true)return 'unverified';
  if(!Number.isFinite(obs)||!Number.isFinite(acq)||obs>now||acq>now)return 'unavailable';
  const limit=row.asset_class==='crypto'?7200:345600,acqLimit=row.asset_class==='crypto'?7200:93600;
  return q.status==='fresh' && (now-obs)/1000<=limit && (now-acq)/1000<=acqLimit ? 'fresh':q.status==='missing'?'missing':'stale';
 }
 function records(packet){
  if(packet?.contract!=='daily-research-report.v1'||!packet.market_measurement_quality)throw Error('Original-source market report unavailable');
  return [...Object.values(packet.stocks||{}),...Object.values(packet.crypto_by_id||{})];
 }
 function render(row,now){
  const state=status(row,now),fresh=state==='fresh';
  const values=row.asset_class==='crypto'?Object.entries(row.provider_reported_changes||{}).map(([k,v])=>`<tr><th>${esc(k)}</th><td>${fmt(fresh?v.value:null)}%</td><td>Provider reported; baseline unverified</td></tr>`):Object.entries(row.changes||{}).map(([k,v])=>`<tr><th>${esc(k)}</th><td>${fmt(fresh?v.value:null)}%</td><td>${esc(v.baseline_date)} → ${esc(v.current_date)}<br>Baseline ${fmt(v.baseline_close)} USD</td></tr>`);
  const evidence=row.evidence||{},key=safeKey(evidence.key)?evidence.key:null;
  return `<article class="card"><h2>${esc(row.symbol)} <span class="state">${esc(state.toUpperCase())}</span></h2><div class="identity">${esc(row.name)} · ${esc(row.instrument_id)}</div>
   <div class="price">${fmt(fresh?row.price:null)} <small>USD</small></div><div class="note">${esc(row.price_kind)}</div>
   <p class="clock">Observed ${esc(row.observed_at)}<br>Acquired ${esc(row.acquired_at)}</p>
   <details><summary>Inspect measurement and comparisons</summary><div class="facts"><div><span>Unit</span>${esc(row.unit)}</div><div><span>Last observed price</span>${fmt(row.last_observed_price)}</div></div>
   <p class="note">${row.asset_class==='crypto'?'Provider market aggregate; coin identity is its provider ID.':'Split-adjusted price history; current retrieved vintage. No dividend adjustment.'}</p>
   <table><thead><tr><th>Period</th><th>Change</th><th>Comparison</th></tr></thead><tbody>${values.join('')}</tbody></table>
   ${key?`<p class="evidence"><a href="/${esc(key)}?exact=1">Original response archive</a></p><button type="button" data-original="${esc(row.instrument_id)}">Verify and show original row</button><pre class="original-row" hidden></pre>`:'<p>Original evidence unavailable</p>'}
   <p class="note">No call or sizing authority. ${esc(row.unavailable_measurements||'Percentage-change baselines remain provider reported.')}</p></details></article>`;
 }
 async function original(row,fetcher=root.fetch.bind(root)){
  const ref=row.evidence;
  if(!safeKey(ref?.key)||!Number.isSafeInteger(ref.bytes)||ref.bytes<=0||ref.bytes>4194304||!/^[a-f0-9]{64}$/.test(ref.sha256))throw Error('Invalid source reference');
  const response=await fetcher('/'+ref.key+'?exact=1',{cache:'no-store'});
  if(!response.ok)throw Error('Original response unavailable');
  if(response.headers.get('X-JH-Artifact-Key')!==ref.key)throw Error('Artifact identity differs');
  async function bounded(stream){const reader=stream.getReader(),chunks=[];let size=0;try{while(true){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>4194304)throw Error('Original response too large');chunks.push(value);}}catch(error){await reader.cancel();throw error;}const bytes=new Uint8Array(size);let offset=0;for(const part of chunks){bytes.set(part,offset);offset+=part.length;}return bytes;}
  let bytes=await bounded(response.body);
  if(bytes[0]===31&&bytes[1]===139)bytes=await bounded(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')));
  if(bytes.length!==ref.bytes)throw Error('Original response length differs');
  const hash=Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
  if(hash!==ref.sha256)throw Error('Original response hash differs');
  const payload=JSON.parse(new TextDecoder().decode(bytes)),entry=row.asset_class==='crypto'?payload[row.source_row]:payload.results?.[row.source_row];
  if(!entry)throw Error('Original row missing');
  if(row.asset_class==='crypto'?(entry.id!==row.provider_id||entry.current_price!==row.last_observed_price||Date.parse(entry.last_updated)!==Date.parse(row.observed_at)):(payload.ticker!==row.symbol||entry.c!==row.last_observed_price||payload.adjusted!==true||entry.t!==Date.parse(row.period_start)))throw Error('Displayed measurement differs from original');
  return {verified_sha256:hash,source_row:row.source_row,original:entry};
 }
 const api={status,records,render,original,safeKey};if(typeof module==='object'&&module.exports)module.exports=api;
 if(!root.document)return;
 root.document.addEventListener('DOMContentLoaded',()=>{
  const host=root.document.getElementById('market-list'),summary=root.document.getElementById('market-summary');if(!host)return;
  const query=root.document.getElementById('market-search'),kind=root.document.getElementById('market-kind');let data=[],generation=0,ageSignature='';
  function ages(){const now=Date.now();return data.map(row=>status(row,now)).join('|');}
  function paint(){const q=query.value.toLowerCase();ageSignature=ages();host.innerHTML=data.filter(row=>(kind.value==='all'||row.asset_class===kind.value)&&[row.symbol,row.name,row.instrument_id].join(' ').toLowerCase().includes(q)).map(row=>render(row,Date.now())).join('')||'<p>No matching measurements available.</p>';}
  async function refresh(){const n=++generation;
   try{const response=await root.fetch('/data/report.json',{cache:'no-store'});if(!response.ok)throw Error('Report unavailable');const packet=await response.json();const rows=records(packet);if(n!==generation)return;
    data=rows;const quality=packet.market_measurement_quality;summary.innerHTML=`Report ${esc(packet.generated_at)} · ${esc(quality.equities_compiled)} equities / ${esc(quality.equity_universe.length)} requested · ${esc(quality.crypto_compiled)} crypto observations · ${Object.keys(quality.errors||{}).length} source gaps.<br><span class="note">Public source evidence is available below. Missing inputs and the retained snapshot can be inspected in the <a href="/data/report.json">full packet</a>${safeKey(packet.replay?.manifest_key)?` and <a href="/${packet.replay.manifest_key}?exact=1">replay manifest</a>`:''}.</span>`;paint();
   }catch(error){if(n!==generation)return;data=[];host.replaceChildren();summary.textContent='Source market measurements unavailable. Previously displayed observations have been cleared.';}
  }
  host.addEventListener('click',async event=>{const button=event.target.closest('button[data-original]');if(!button)return;const row=data.find(r=>r.instrument_id===button.dataset.original),box=button.nextElementSibling;button.disabled=true;box.hidden=false;box.textContent='Verifying original response…';
   try{box.textContent=JSON.stringify(await original(row),null,2);}catch(error){box.textContent='Original verification failed: '+error.message;}finally{button.disabled=false;}
  });
  query.addEventListener('input',paint);kind.addEventListener('change',paint);root.document.getElementById('market-refresh').addEventListener('click',refresh);refresh();
  // Recheck expiration without collapsing an open source inspector each minute.
  root.setInterval(()=>{if(ages()!==ageSignature)paint();},60000);
 });
})(typeof window==='object'?window:globalThis);
