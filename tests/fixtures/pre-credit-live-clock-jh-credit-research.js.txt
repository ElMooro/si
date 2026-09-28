/* Native credit: exact source units, dated comparisons and explicit scenarios. */
(function(root){
 'use strict';
 const PREFIX='data/credit-research/',CURRENT='data/credit-stress.json';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=v=>typeof v==='number'&&Number.isFinite(v);
 const fmt=(v,d=3)=>finite(v)?(Object.is(v,-0)?0:v).toLocaleString('en-US',{maximumFractionDigits:d}):'Unavailable';
 const safe=key=>/^data\/credit-research\/(?:runs|inputs|outputs|compilers)\/[a-f0-9]{64}\.(?:json|py)$/.test(key||'');
 const link=(key,label)=>safe(key)?'<a href="/'+key+'">'+esc(label)+'</a>':esc(label+' unavailable');
 const permissions=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const typed=p=>p?.contract==='credit-native-research.v1'&&p.call===null&&p.portfolio_action==='WAIT'&&permissions.every(k=>p[k]===false)&&p.measurements&&typeof p.measurements==='object'&&!Array.isArray(p.measurements)&&Array.isArray(p.source_evidence);
 function current(p,now=Date.now()){
  const age=now-Date.parse(p.generated_at),until=Date.parse(p.freshness?.pipeline_check_due_at);
  return Number.isFinite(age)&&age>=0&&age<=36*3600000&&Number.isFinite(until)&&now<until;
 }
 function rowCurrent(p,m,now=Date.now()){
  return current(p,now)&&m.quality?.status==='within_age_ceiling'&&finite(m.value_pct)&&now<Date.parse(m.source_valid_until)&&Date.parse(m.observation_date+'T00:00:00Z')<=now;
 }
 function publisher(sid,label){return /^[A-Z0-9_]+$/.test(sid||'')?'<a href="https://fred.stlouisfed.org/series/'+sid+'">'+esc(label||sid)+'</a>':esc(label||sid);}
 function render(p,now=Date.now()){
  if(!typed(p))return '<section class="notice" role="alert"><h2>Verified credit research unavailable</h2><p>The native measurement contract could not be established.</p></section>';
  const rows=Object.values(p.measurements),fresh=rows.filter(m=>rowCurrent(p,m,now)).length;
  let html='<section class="notice"><p class="eyebrow">CREDIT OBSERVATIONS</p><h2>Compensation, dispersion and source quality</h2><p>'+fresh+' of '+rows.length+' measurements pass the current age checks. Collected '+esc(p.generated_at)+'.</p><p>Trade status: WAIT · this research supplies no qualified return forecast or target position.</p>';
  if(!current(p,now))html+='<p class="warning" role="status">The collection deadline has passed. Readings below retain their observation dates and are withheld from current use.</p>';
  html+='<p>'+link(p.replay?.manifest_key,'Run evidence')+' · <a href="/data/credit-stress.json">Download current packet</a> · <button type="button" data-credit-refresh>Refresh</button></p></section>';
  html+='<section><h2>Credit snapshot</h2><div class="grid">'+[['BAMLH0A0HYM2','US high yield OAS'],['BAMLC0A0CM','US investment grade OAS'],['BAMLEMCBPIOAS','EM corporate plus OAS'],['T10Y2Y','US Treasury 10y minus 2y']].map(([sid,label])=>{
   const m=p.measurements[sid]||{};return '<article><h3>'+esc(label)+'</h3><p class="value">'+fmt(m.value_bps)+' <small>bp</small></p><p>'+fmt(m.value_pct)+' '+(sid==='T10Y2Y'?'percentage points':'%')+' · '+esc(m.observation_date||'unavailable')+'</p><p class="muted">'+(sid==='BAMLEMCBPIOAS'?'Mixed investment-grade and below-investment-grade corporate/quasi-government debt.':sid==='T10Y2Y'?'Difference between two Treasury yields.':'Provider model-based option-adjusted spread.')+'</p><p>'+publisher(sid,'Source definition')+' · '+(rowCurrent(p,m,now)?'within age ceiling':'unavailable or aged')+'</p></article>';
  }).join('')+'</div></section>';
  html+='<section><h2>Compare matched observation dates</h2><div class="grid">'+Object.values(p.comparisons||{}).map(c=>'<article><h3>'+esc(c.label)+'</h3><p class="value">'+fmt(c.value_bps)+' <small>bp</small></p><p>'+esc(c.observation_date||'No matched date')+' · '+esc(c.status)+'</p><p class="muted">'+esc(c.current_comparison_available?'Both latest source dates match.':'Current comparison withheld; any value shown is dated common-source context.')+'</p><p>'+publisher(c.left_series_id,'Left series')+' − '+publisher(c.right_series_id,'right series')+'</p></article>').join('')+'</div><p class="muted">These index differences include changes in rating, maturity, currency and sector composition. They are not pure default premiums or executable spreads.</p></section>';
  html+='<section><h2>Explore the credit measurements</h2><div class="controls"><label for="credit-unit">Display units</label><select id="credit-unit"><option value="bp">Basis points</option><option value="pct">Source percent / percentage points</option></select></div><div id="credit-measurements"></div></section>';
  html+='<section><h2>Inspect a calculation</h2><label for="credit-series">Series</label><select id="credit-series">'+rows.map(m=>'<option value="'+esc(m.series_id)+'"'+(m.series_id==='BAMLH0A0HYM2'?' selected':'')+'>'+esc(m.meta?.name||m.series_id)+' · '+esc(m.series_id)+'</option>').join('')+'</select><div id="credit-detail"></div></section>';
  html+='<section><h2>Test a position’s spread sensitivity</h2><p>Illustrative inputs. Replace them with a position’s signed market value and measured spread duration. This is an isolated first-order spread shock, not a trade recommendation.</p><div class="scenario-grid"><label>Signed market value (USD)<input id="credit-exposure" type="number" value="1000000" step="1000" min="-1000000000000" max="1000000000000"></label><label>Spread duration (years)<input id="credit-duration" type="number" value="5" step="0.1" min="0" max="100"></label><label>Spread shock (bp)<input id="credit-shock" type="number" value="100" step="10" min="-10000" max="10000"></label></div><div id="credit-scenario" aria-live="polite"></div><p class="muted">Wider spreads usually lower a long position’s price under this approximation. Rate duration, convexity, currency, carry, defaults and liquidity effects are excluded. Large shocks can invalidate the linear approximation.</p><p><a href="/position-sizer.html">Open Portfolio scenarios</a> to examine entered exposures and additional risks.</p></section>';
  html+='<section class="two"><article><h2>What these measurements mean</h2><p>'+esc(p.definitions?.oas)+'</p><p>'+esc(p.definitions?.effective_yield)+'</p><p>'+esc(p.definitions?.maturity_buckets)+'</p><p>'+esc(p.definitions?.em_corporate_plus)+'</p></article><article><h2>Timing and evidence</h2><p>'+esc(p.freshness?.source_age_rule)+'</p><p>Pipeline check due '+esc(p.freshness?.pipeline_check_due_at)+'. No source release time is invented.</p><p>'+esc(p.definitions?.weekends)+'</p><p>'+esc(p.quality?.independence_note)+'</p></article></section>';
  html+='<section><h2>Reproduction and history</h2><p>The authorized AWS runner re-executes the calculations from complete retained FRED definitions and observations before publication. This page verifies the run and exact retained output. Original index responses remain in the protected source archive.</p><p>All 28 source identities remain in the inventory. Statistics show the actual returned window; no five-year or all-time coverage is inferred from a shorter response.</p><p>'+esc(p.definitions?.vintage)+'</p><details><summary>Previous public history</summary><p>The prior history packet remains dated and unchanged. It is not an input to these native calculations and has no native replay or forecasting qualification.</p><p><a href="/data/credit-stress-history.json">Inspect the legacy history packet</a></p></details></section>';
  return html;
 }
 function table(p,unit,now=Date.now()){
  if(!['bp','pct'].includes(unit))return '';
  const groups=[['oas','Option-adjusted spreads'],['effective_yield','Effective yields and maturity baskets'],['treasury_slope','Treasury curve context']];
  return groups.map(([kind,title])=>'<h3>'+title+'</h3><div class="table-wrap" tabindex="0" role="region" aria-label="'+title+'"><table><thead><tr><th>Series</th><th>Value</th><th>Observed</th><th>Change (bp)</th><th>Previous observation</th><th>Current use</th></tr></thead><tbody>'+Object.values(p.measurements).filter(m=>(m.kind||((m.series_id||'').endsWith('EY')?'effective_yield':m.series_id==='T10Y2Y'?'treasury_slope':'oas'))===kind).map(m=>'<tr><td>'+publisher(m.series_id,m.meta?.name||m.series_id)+'</td><td>'+fmt(m[unit==='bp'?'value_bps':'value_pct'])+' '+(unit==='bp'?'bp':kind==='treasury_slope'?'pp':'%')+'</td><td>'+esc(m.observation_date||'unavailable')+'</td><td>'+fmt(m.change_bps)+'</td><td>'+esc(m.previous_observation_date||'unavailable')+(finite(m.comparison_gap_days)?' · '+fmt(m.comparison_gap_days)+' calendar days':'')+'</td><td>'+(rowCurrent(p,m,now)?'Within age ceiling':'Withheld')+'</td></tr>').join('')+'</tbody></table></div>').join('');
 }
 function detail(p,sid){
  const m=p.measurements[sid];if(!m)return '';
  const stats=m.descriptive_statistics||{},h=m.history_coverage||{},pct=stats.calendar_365_day_percentile||{};
  let html='<article><h3>'+esc(m.definition||sid)+'</h3><p>'+publisher(sid)+' · native unit '+esc(m.source_unit||'unavailable')+' · '+esc(m.frequency||'unavailable')+' · '+esc(m.seasonal_adjustment||'unavailable')+'</p><p>'+fmt(h.provider_rows)+' provider rows: '+fmt(h.numeric_rows)+' numeric and '+fmt(h.null_rows)+' missing, from '+esc(h.first_date||'unavailable')+' to '+esc(h.last_date||'unavailable')+'.</p><p>'+esc(h.history_limit||'History unavailable')+'</p><div class="table-wrap" tabindex="0" role="region" aria-label="Descriptive observation windows"><table><thead><tr><th>Reference window</th><th>Actual dates</th><th>Mean (source units)</th><th>Sample standard deviation (pp)</th><th>Descriptive z-score</th><th>Missing rows inside span</th></tr></thead><tbody>'+Object.values(stats.observation_windows||{}).map(s=>'<tr><td>'+fmt(s.numeric_observations)+' / '+fmt(s.requested_observations)+' numeric observations</td><td>'+esc(s.first_date||'unavailable')+' → '+esc(s.last_date||'unavailable')+'</td><td>'+fmt(s.mean_pct)+'</td><td>'+fmt(s.sample_stddev_pp)+'</td><td>'+fmt(s.z_score)+'</td><td>'+fmt(s.missing_rows_inside_span)+'</td></tr>').join('')+'</tbody></table></div><p>Calendar 365-day empirical percentile: '+fmt(pct.percentile_pct)+'%. '+fmt(pct.numeric_observations)+' numeric rows from '+esc(pct.first_returned_date||'unavailable')+' to '+esc(pct.last_returned_date||'unavailable')+'. Ties receive half weight. This is a sample rank, not a probability forecast.</p><p class="muted">The 60/252 windows count numeric provider observations, not assumed trading days. Missing records remain counted in their actual date span. A constant window has no z-score.</p><details><summary>Inspect retained source evidence</summary><p>Definition updated '+esc(m.provider_updated_at||'unavailable')+'. Compilation does not establish when historical values first became available.</p>'+p.source_evidence.filter(e=>e.series_id===sid).map(e=>'<p>'+esc(e.kind)+' · acquired '+esc(e.acquired_at)+' · '+fmt(e.bytes)+' bytes<br><code>'+esc(e.sha256)+'</code></p>').join('')+'</details></article>';
  return html;
 }
 function scenario(value,duration,shock){
  if(![value,duration,shock].every(finite)||Math.abs(value)>1e12||duration<0||duration>100||Math.abs(shock)>10000)return '<p role="alert">Enter a finite signed market value, spread duration from 0 to 100 years, and a shock from −10,000 to 10,000 bp.</p>';
  const fraction=-duration*shock/10000,pnl=value*fraction;
  const points=[-100,-50,0,50,100].map(bp=>({bp,pnl:value*(-duration*bp/10000)}));
  return '<article><h3>Illustrative spread-only effect</h3><p class="value">'+fmt(pnl,2)+' <small>USD</small></p><p>Underlying price approximation: '+fmt(100*fraction,2)+'% for '+fmt(shock)+' bp. Signed market value × (−spread duration × shock / 10,000).</p>'+(Math.abs(fraction)>=.2?'<p class="warning">This large modeled price effect calls for full repricing; the linear approximation may be unsuitable.</p>':'')+'<div class="table-wrap" tabindex="0" role="region" aria-label="Spread shock sensitivity"><table><thead><tr><th>Spread shock</th><th>Approximate P&amp;L</th></tr></thead><tbody>'+points.map(p=>'<tr><td>'+fmt(p.bp)+' bp</td><td>'+fmt(p.pnl,2)+' USD</td></tr>').join('')+'</tbody></table></div><p>No order or portfolio change is produced. Inputs stay in this page.</p></article>';
 }
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved research path');const r=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!r.ok)throw Error('HTTP '+r.status);
  const raw=await r.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(packet,fetcher,signal){
  const key=packet?.replay?.manifest_key;if(!typed(packet)||!/^data\/credit-research\/runs\/[a-f0-9]{64}\.json$/.test(key||''))throw Error('Native research required');
  const run=await load(key,fetcher,signal),m=run.doc;
  if(key!==PREFIX+'runs/'+await sha(run.raw)+'.json'||m.contract!=='credit-native-replay.v1'||m.output_sha256!==packet.replay.output_sha256||m.generated_at!==packet.generated_at)throw Error('Run differs');
  const ref=m.output;if(ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=packet;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current packet differs');return packet;
 }
 let pending;
 async function mount(){
  const host=root.document?.getElementById('credit-research');if(!host)return;if(pending)pending.abort();const controller=new AbortController();pending=controller;
  const timer=setTimeout(()=>controller.abort(),15000);host.innerHTML='<p role="status">Loading and verifying native credit measurements…</p>';
  try{
   const p=await verifyPacket((await load(CURRENT,root.fetch.bind(root),controller.signal)).doc,root.fetch.bind(root),controller.signal);if(controller!==pending)return;
   host.innerHTML=render(p);host.querySelector('#credit-measurements').innerHTML=table(p,'bp');host.querySelector('#credit-detail').innerHTML=detail(p,'BAMLH0A0HYM2');
   host.querySelector('#credit-unit').onchange=e=>{host.querySelector('#credit-measurements').innerHTML=table(p,e.target.value);};
   host.querySelector('#credit-series').onchange=e=>{host.querySelector('#credit-detail').innerHTML=detail(p,e.target.value);};
   function recalc(){const values=['credit-exposure','credit-duration','credit-shock'].map(id=>{const el=host.querySelector('#'+id);return el.value.trim()===''?NaN:Number(el.value);});host.querySelector('#credit-scenario').innerHTML=scenario(...values);}
   for(const id of ['credit-exposure','credit-duration','credit-shock'])host.querySelector('#'+id).oninput=recalc;
   recalc();host.querySelector('[data-credit-refresh]').onclick=mount;
  }catch(e){if(controller===pending){host.innerHTML='<section class="notice" role="alert"><h2>Verified credit research unavailable</h2><p>The current packet or retained output could not be verified. No current reading or scenario has been substituted.</p><button type="button" id="credit-retry">Retry</button></section>';host.querySelector('#credit-retry').onclick=mount;}}
  finally{clearTimeout(timer);}
 }
 const api={render,table,detail,scenario,verifyPacket,current,rowCurrent,typed};root.JHCreditResearch=api;if(typeof module!=='undefined')module.exports=api;
 if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',mount);else mount();}
})(typeof globalThis!=='undefined'?globalThis:this);
