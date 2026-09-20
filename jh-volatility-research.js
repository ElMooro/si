/* Native volatility: source-defined indices, matched dates and explicit vega scenarios. */
(function(root){
 'use strict';
 const PREFIX='data/volatility-research/',CURRENT='data/vol-surface.json';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=v=>typeof v==='number'&&Number.isFinite(v);
 const fmt=(v,d=3)=>finite(v)?(Object.is(v,-0)?0:v).toLocaleString('en-US',{maximumFractionDigits:d}):'Unavailable';
 const safe=key=>/^data\/volatility-research\/(?:runs|inputs|outputs|compilers)\/[a-f0-9]{64}\.(?:json|py)$/.test(key||'');
 const link=(key,label)=>safe(key)?'<a href="/'+key+'">'+esc(label)+'</a>':esc(label+' unavailable');
 const permissions=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const typed=p=>p?.contract==='volatility-native-research.v1'&&p.call===null&&p.portfolio_action==='WAIT'&&permissions.every(k=>p[k]===false)&&p.measurements&&typeof p.measurements==='object'&&!Array.isArray(p.measurements)&&Array.isArray(p.source_evidence);
 function current(p,now=Date.now()){
  const age=now-Date.parse(p.generated_at),until=Date.parse(p.freshness?.pipeline_check_due_at);
  return Number.isFinite(age)&&age>=0&&age<=36*3600000&&Number.isFinite(until)&&now<until;
 }
 function rowCurrent(p,m,now=Date.now()){
  return current(p,now)&&m.quality?.status==='within_age_ceiling'&&finite(m.value)&&now<Date.parse(m.source_valid_until)&&Date.parse(m.observation_date+'T00:00:00Z')<=now;
 }

 function comparisonCurrent(p,c,now){return current(p,now)&&c.current_comparison_available===true&&Object.keys(c.latest_dates||{}).length>1&&Object.keys(c.latest_dates).every(k=>p.measurements[k]&&rowCurrent(p,p.measurements[k],now));}
 function publisher(m){const url=m.source_url||'';return /^https:\/\/(?:fred\.stlouisfed\.org\/series\/[A-Z0-9]+|www\.cboe\.com\/us\/indices\/dashboard\/(?:vvix|skew)\/|cdn-api\.cboe\.com\/api\/global\/us_indices\/daily_prices\/(?:VVIX|SKEW)_History\.csv)$/.test(url)?'<a href="'+url+'">'+esc(m.label)+'</a>':esc(m.label);}
 function table(p,now=Date.now()){
  return '<div class="table-wrap" tabindex="0" role="region" aria-label="Volatility index observations"><table><thead><tr><th>Index / underlying</th><th>Index points</th><th>Observed</th><th>Change (points)</th><th>Previous observation</th><th>Current use</th></tr></thead><tbody>'+Object.values(p.measurements).map(m=>'<tr><td>'+publisher(m)+'<br><small>'+esc(m.underlying)+'</small></td><td>'+fmt(m.value)+'</td><td>'+esc(m.observation_date||'Unavailable')+'</td><td>'+fmt(m.change_points)+'</td><td>'+esc(m.previous_observation_date||'Unavailable')+(finite(m.comparison_gap_days)?'<br><small>'+fmt(m.comparison_gap_days)+' calendar days</small>':'')+'</td><td>'+(rowCurrent(p,m,now)?'Within age ceiling':'Withheld')+'</td></tr>').join('')+'</tbody></table></div>';
 }
 function detail(p,label){
  const m=p.measurements[label];if(!m)return '';const h=m.history_coverage||{},stats=m.descriptive_statistics||{},rank=stats.calendar_365_day_percentile||{};
  return '<article><h3>'+esc(m.definition||label)+'</h3><p>'+publisher(m)+' · '+esc(m.provider)+' · source unit '+esc(m.source_unit||'Unavailable')+' · '+esc(m.frequency||'Unavailable')+'</p><p>Exact retained value: <code>'+esc(m.exact?.value??'Unavailable')+'</code> index points. Observed '+esc(m.observation_date||'Unavailable')+'.</p><p>'+fmt(h.provider_rows)+' provider rows, '+fmt(h.numeric_rows)+' numeric and '+fmt(h.null_rows)+' missing; '+esc(h.first_date||'Unavailable')+' → '+esc(h.last_date||'Unavailable')+'.</p><div class="table-wrap" tabindex="0" role="region" aria-label="Descriptive history windows"><table><thead><tr><th>Numeric observations</th><th>Actual dates</th><th>Mean (points)</th><th>Sample SD (points)</th><th>Descriptive z-score</th><th>Missing rows inside span</th></tr></thead><tbody>'+Object.values(stats.observation_windows||{}).map(s=>'<tr><td>'+fmt(s.numeric_observations)+' / '+fmt(s.requested_observations)+'</td><td>'+esc(s.first_date||'Unavailable')+' → '+esc(s.last_date||'Unavailable')+'</td><td>'+fmt(s.mean_index_points)+'</td><td>'+fmt(s.sample_stddev_points)+'</td><td>'+fmt(s.z_score)+'</td><td>'+fmt(s.missing_rows_inside_span)+'</td></tr>').join('')+'</tbody></table></div><p>The 60/252 windows count numeric provider observations, not assumed trading days. Missing rows stay in the date span. A constant window has no z-score.</p><p>365-day empirical percentile: '+fmt(rank.percentile_pct)+'%; '+fmt(rank.numeric_observations)+' numeric observations, '+esc(rank.first_returned_date||'Unavailable')+' → '+esc(rank.last_returned_date||'Unavailable')+'. Full requested span: '+(rank.full_requested_span_available?'yes':'no')+'. Ties receive half weight. This is a sample rank, not a forecast probability.</p><details><summary>Inspect retained source evidence</summary>'+p.source_evidence.filter(e=>e.label===label).map(e=>'<p>'+esc(e.kind)+' · acquired '+esc(e.acquired_at)+' · '+fmt(e.bytes)+' bytes<br><code>'+esc(e.sha256)+'</code></p>').join('')+'<p>Complete source histories stay in the protected archive. Public compiler and run records identify the retained originals.</p></details></article>';
 }
 function render(p,now=Date.now()){
  if(!typed(p))return '<section role="alert">Verified volatility research unavailable.</section>';
  const count=Object.values(p.measurements).filter(m=>rowCurrent(p,m,now)).length,t=p.tenor_comparison||{},d=p.single_name_dispersion||{};
  let html='<section class="notice"><p class="eyebrow">MEASUREMENT STATUS</p><h2>'+count+' / 16 within the age ceiling</h2><p>Collected '+esc(p.generated_at)+'. Observation dates are shown separately for each index.</p><p>Research only · WAIT. Trade, forecast, sizing and execution authority are withheld.</p><button type="button" data-volatility-refresh>Refresh and verify</button><p>'+link(p.replay?.manifest_key,'Inspect this run')+' · <a href="/data/volatility-research-verification.json">Deployment acceptance</a></p></section>';
  html+='<section class="grid">'+['VIX_30D','VIX_3M','VVIX','SKEW'].map(k=>{const m=p.measurements[k];return '<article><h3>'+publisher(m)+'</h3><p class="value">'+(rowCurrent(p,m,now)?fmt(m.value):'Withheld')+'</p><p>index points · '+esc(m.observation_date||'Unavailable')+'</p></article>';}).join('')+'</section>';
  html+='<section><h2>Source-defined measurements</h2><p>Fourteen Cboe indices distributed by FRED, plus publisher VVIX and SKEW. Every original input remains identifiable; missing data remains missing.</p>'+table(p,now)+'</section>';
  html+='<section class="two"><article><h2>30-day vs 3-month VIX</h2><p class="value">'+(comparisonCurrent(p,t,now)?fmt(t.difference_points)+' points':'Withheld')+'</p><p>30-day minus 3-month index, observed '+esc(t.observation_date||'Unavailable')+'. Latest observation dates match: '+(t.both_latest_dates_match?'yes':'no')+'.</p><p>'+esc(t.interpretation)+'</p></article><article><h2>Five single-name indices</h2><p>Same-date cross-sectional standard deviation: '+(comparisonCurrent(p,d,now)?fmt(d.population_stddev_points)+' points':'Withheld')+'. Observed '+esc(d.observation_date||'Unavailable')+'.</p><p>'+esc(d.interpretation)+'</p></article></section>';
  html+='<section><h2>Matching-date cross-asset ratios</h2><div class="table-wrap" tabindex="0" role="region" aria-label="Cross asset ratios"><table><thead><tr><th>Numerator / VIX</th><th>Ratio</th><th>Common date</th><th>Latest dates match</th></tr></thead><tbody>'+Object.values(p.cross_asset_comparisons||{}).map(c=>'<tr><td>'+esc(c.numerator_label)+'</td><td>'+(comparisonCurrent(p,c,now)?fmt(c.ratio):'Withheld')+'</td><td>'+esc(c.observation_date||'Unavailable')+'</td><td>'+(c.both_latest_dates_match?'yes':'no')+'</td></tr>').join('')+'</tbody></table></div><p>Different underlying assets and option baskets. Ratios do not establish relative hedge cost or a trade.</p></section>';
  html+='<section><h2>Inspect a measurement</h2><label for="volatility-series">Index</label><select id="volatility-series">'+Object.keys(p.measurements).map(k=>'<option'+(k==='VIX_30D'?' selected':'')+'>'+esc(k)+'</option>').join('')+'</select><div id="volatility-detail"></div></section>';
  html+='<section><h2>Connect an explicit shock to a position</h2><p>A hypothetical first-order vega scenario. An index move does not supply the option’s IV shock or its vega. Enter both explicitly; no account is read.</p><div class="scenario-grid"><label>Signed position vega (USD per volatility point)<input id="volatility-vega" type="number" value="1000" min="-1000000000" max="1000000000" step="100"></label><label>Option IV shock (volatility points)<input id="volatility-shock" type="number" value="5" min="-100" max="100" step="1"></label></div><div id="volatility-scenario" aria-live="polite"></div></section>';
  html+='<section class="two"><article><h2>Definitions and timing</h2>'+Object.values(p.definitions||{}).map(v=>'<p>'+esc(v)+'</p>').join('')+'<p>'+esc(p.freshness?.basis)+'</p><p>Next pipeline check due '+esc(p.freshness?.pipeline_check_due_at)+'. No historical release time is invented.</p></article><article><h2>Reproduction and independence</h2><p>The authorized runner rebuilds the calculations from retained complete source responses before publication. This page verifies the run and exact output bytes.</p><p>'+esc(p.quality?.provider_root)+'</p><p>'+esc(p.quality?.independence_note)+'</p><p>Current-vintage history can be revised or backfilled. It is not a point-in-time investment backtest.</p><details><summary>Previous public history</summary><p>Legacy snapshots are unchanged and are not inputs to these calculations.</p><a href="/data/vol-surface-history.json">Inspect legacy history</a></details></article></section>';
  return html;
 }
 function scenario(vega,shock){
  if(![vega,shock].every(finite)||Math.abs(vega)>1e9||Math.abs(shock)>100)return '<p role="alert">Enter finite signed vega within ±1 billion USD per volatility point and a shock within ±100 volatility points.</p>';
  return '<article><h3>Illustrative vega-only effect</h3><p class="value">'+fmt(vega*shock,2)+' <small>USD</small></p><p>Signed position vega × explicitly entered option-IV shock. One volatility point is one percentage point of annualized option IV; +5 points is distinct from +5% relative.</p><p>Delta, gamma, time decay, volatility smile, rates, FX and changing vega are excluded. Large shocks require full repricing.</p><p>No forecast or order is produced. Inputs stay in this page.</p></article>';
 }
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved research path');const r=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!r.ok)throw Error('HTTP '+r.status);
  const raw=await r.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(packet,fetcher,signal){
  const key=packet?.replay?.manifest_key;if(!typed(packet)||!/^data\/volatility-research\/runs\/[a-f0-9]{64}\.json$/.test(key||''))throw Error('Native research required');
  const run=await load(key,fetcher,signal),m=run.doc;
  if(key!==PREFIX+'runs/'+await sha(run.raw)+'.json'||m.contract!=='volatility-native-replay.v1'||m.output_sha256!==packet.replay.output_sha256||m.generated_at!==packet.generated_at)throw Error('Run differs');
  const ref=m.output;if(ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=packet;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current packet differs');return packet;
 }

 let pending;
 async function mount(){
  const host=root.document?.getElementById('volatility-research');if(!host)return;if(pending)pending.abort();const c=new AbortController();pending=c;
  const timer=setTimeout(()=>c.abort(),15000);host.innerHTML='<p role="status">Loading and verifying volatility measurements…</p>';
  try{
   const p=await verifyPacket((await load(CURRENT,root.fetch.bind(root),c.signal)).doc,root.fetch.bind(root),c.signal);if(c!==pending)return;
   host.innerHTML=render(p);host.querySelector('#volatility-detail').innerHTML=detail(p,'VIX_30D');
   host.querySelector('#volatility-series').onchange=e=>host.querySelector('#volatility-detail').innerHTML=detail(p,e.target.value);
   function recalc(){const args=['volatility-vega','volatility-shock'].map(id=>{const v=host.querySelector('#'+id).value;return v.trim()===''?NaN:Number(v);});host.querySelector('#volatility-scenario').innerHTML=scenario(...args);}
   for(const id of ['volatility-vega','volatility-shock'])host.querySelector('#'+id).oninput=recalc;
   recalc();host.querySelector('[data-volatility-refresh]').onclick=mount;
  }catch(e){if(c===pending){host.innerHTML='<section class="notice" role="alert"><h2>Verified volatility research unavailable</h2><p>The packet or retained output could not be verified. No substitute reading is shown.</p><button id="volatility-retry" type="button">Retry</button></section>';host.querySelector('#volatility-retry').onclick=mount;}}
  finally{clearTimeout(timer);}
 }
 const api={render,table,detail,scenario,verifyPacket,current,rowCurrent,typed};root.JHVolatilityResearch=api;if(typeof module!=='undefined')module.exports=api;
 if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',mount);else mount();}
})(typeof globalThis!=='undefined'?globalThis:this);
