(function(root){
 'use strict';
 const esc=x=>String(x==null?'':x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const numeric=x=>typeof x==='number'&&Number.isFinite(x);
 const fmt=(x,n=2)=>numeric(x)?x.toLocaleString('en-US',{maximumFractionDigits:n}):'Unavailable';
 const day=x=>typeof x==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(x)?esc(x):'Unavailable';
 const val=x=>typeof x==='string'&&x.trim()!==''&&Number.isFinite(Number(x))?Number(x):null;
 function link(key,label){return typeof key==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(key)&&!key.includes('..')?'<a href="/'+esc(key)+'" target="_blank" rel="noopener">'+esc(label)+'</a>':esc(label+' unavailable');}
 function chart(rows,basis,horizon){
  const data=(rows||[]).slice(horizon==='all'?-2000:-Number(horizon)-1),field=basis==='price'?'close':'adjusted_close';
  const finite=data.filter(r=>val(r[field])!==null);if(finite.length<2)return '<p>Insufficient finite history.</p>';
  const first=Date.parse(data[0].date),last=Date.parse(data.at(-1).date);if(!Number.isFinite(first)||last<=first)return '<p>History dates unavailable.</p>';
  const values=finite.map(r=>val(r[field])),low=Math.min(...values),high=Math.max(...values),span=high-low||1;
  let path='',connected=false;data.forEach(r=>{const value=val(r[field]);if(value===null){connected=false;return;}
   path+=(connected?' L ':' M ')+(8+584*(Date.parse(r.date)-first)/(last-first)).toFixed(2)+' '+(78-64*(value-low)/span).toFixed(2);connected=true;});
  return '<figure><svg viewBox="0 0 600 92" role="img" aria-label="'+esc(basis==='price'?'USD split-adjusted price; excludes dividends.':'USD provider dividend-adjusted close; not an execution price.')+'"><path d="'+path+'" fill="none" stroke="currentColor" stroke-width="2"/></svg><figcaption>'+day(data[0].date)+' — '+day(data.at(-1).date)+' · '+fmt(low)+' to '+fmt(high)+' USD · calendar spacing; missing rows break the line</figcaption></figure>';
 }
 function sourceDetails(r){
  const refs=r.originals||{},cov=r.coverage||{};
  return '<details><summary>Identity, source records and calculations</summary><p>'+esc(r.title)+'<br>'+esc(r.isin)+' · '+esc(r.exchange)+' · '+esc(r.currency)+'</p><p>'+esc(r.interpretation)+'</p>'+
   '<p>'+Object.entries(refs).map(([k,v])=>link(v.key,'Original '+k)).join(' · ')+'</p><p>History requested '+day(cov.requested_start)+'; first observed '+day(cov.first_observed)+'. '+fmt(cov.n_union_dates,0)+' union dates; '+fmt(cov.missing_in_one_ledger,0)+' dates missing in one price ledger.</p>'+
   '<p>Exchange-session completeness: not independently verified. First publication timestamp: not supplied. Current adjustment vintage '+esc(r.current_vintage_date)+'.</p><ul>'+Object.entries(refs).map(([k,v])=>'<li>'+esc(k)+' acquired '+esc(v.acquired_at)+'</li>').join('')+'</ul>'+
   '<ul>'+['volatility','drawdown','trend'].map(k=>'<li>'+esc((r[k]||{}).formula)+' · '+day((r[k]||{}).start_date)+' → '+day((r[k]||{}).end_date)+' · '+fmt((r[k]||{}).n_observations,0)+' observations.</li>').join('')+'</ul>'+
   '<div class="table-wrap"><table><caption>Recent source rows · USD</caption><thead><tr><th>Date</th><th>Price</th><th>Dividend adjusted</th><th>Price row</th><th>Adjusted row</th></tr></thead><tbody>'+(r.history||[]).slice(-8).reverse().map(v=>'<tr><td>'+day(v.date)+'</td><td>'+fmt(val(v.close))+'</td><td>'+fmt(val(v.adjusted_close))+'</td><td>'+esc(v.price_row_index==null?'Missing':v.price_row_index)+'</td><td>'+esc(v.adjusted_row_index==null?'Missing':v.adjusted_row_index)+'</td></tr>').join('')+'</tbody></table></div></details>';
 }
 function card(r,basis,horizon){
  const q=r.quality||{},ret=r.returns||{};
  return '<article><header><h3>'+esc(r.symbol)+'</h3><span class="badge">'+esc(q.status||'unavailable')+'</span></header><p>'+esc(r.label)+' · USD ETF proxy</p>'+
   '<div class="value">'+fmt(basis==='price'?r.close:r.adjusted_close)+' <small>USD</small></div><p class="muted">'+(basis==='price'?'Split-adjusted price; dividends excluded':'Provider dividend-adjusted close')+' · '+day(r.observation_date)+'</p>'+chart(r.history,basis,horizon)+
   '<dl>'+[21,63].map(n=>{const x=(ret[n]||{})[basis==='price'?'price':'dividend_adjusted']||{};return '<dt>'+n+' observed-interval return</dt><dd>'+fmt(x.value)+'%</dd><dt>Exact endpoints</dt><dd>'+day(x.start_date)+' → '+day(x.end_date)+'</dd>';}).join('')+
   '<dt>20-interval realized volatility</dt><dd>'+fmt((r.volatility||{}).value)+'% annualized</dd><dt>Drawdown · 252 observations</dt><dd>'+fmt((r.drawdown||{}).value)+'%</dd><dt>Distance from 200-observation mean</dt><dd>'+fmt((r.trend||{}).value)+'%</dd></dl>'+
   '<p class="muted">Volatility, drawdown and trend use dividend-adjusted closes. Source age '+fmt(q.age_days,0)+' days; ceiling 7 days.</p>'+sourceDetails(r)+'</article>';
 }
 function correlationTable(c){
  const pairs=c.pairs||[],symbols=[...new Set(pairs.flatMap(r=>[r.left,r.right]))],lookup=new Map(pairs.map(r=>[[r.left,r.right].sort().join(':'),r]));
  return '<div class="table-wrap" tabindex="0" role="region" aria-label="Scrollable date-matched correlation matrix"><table class="matrix"><caption>Adjusted-return correlations · minimum 40 matching intervals. Select source details below for sample dates.</caption><thead><tr><th scope="col">ETF</th>'+symbols.map(s=>'<th scope="col">'+esc(s)+'</th>').join('')+'</tr></thead><tbody>'+symbols.map(a=>'<tr><th scope="row">'+esc(a)+'</th>'+symbols.map(b=>{if(a===b)return '<td aria-label="Same instrument">—</td>';const r=lookup.get([a,b].sort().join(':'))||{},value=r.value;return '<td title="'+esc(a+' / '+b+'; '+(r.n_matched_intervals||0)+' matching intervals')+'">'+(numeric(value)?fmt(value,2):'N/A')+'<small>n='+fmt(r.n_matched_intervals,0)+'</small></td>';}).join('')+'</tr>').join('')+'</tbody></table></div>'+
   '<details><summary>Exact paired samples ('+pairs.length+' comparisons)</summary><p>'+esc(c.method)+'</p>'+pairs.map(r=>'<details><summary>'+esc(r.left)+' / '+esc(r.right)+' · '+fmt(r.value)+' · '+fmt(r.n_matched_intervals,0)+' matched / '+fmt(r.n_available_union_intervals,0)+' available intervals</summary><p>'+esc(r.reason||'Both interval endpoints match; no positional alignment.')+'</p><p>'+esc((r.matched_intervals||[]).map(d=>d.join(' → ')).join('; '))+'</p></details>').join('')+'</details>';
 }
 function qualification(status){
  const valid=status&&status.contract==='gsi-qualification-status.v1';
  return '<section><h2>Qualification before portfolio use</h2><p>No Global Stress forecasting, calibration weight or horizon-specific recommendation is qualified. A fresh score, historical correlation, or a producer’s own eligibility flag cannot grant portfolio authority.</p>'+
   (valid?'<p>Qualification status published '+esc(status.generated_at)+'. Validated forecast observations: '+fmt(status.validated_forecast_observations,0)+'.</p><ol>'+(status.requirements||[]).map(x=>'<li>'+esc(x)+'</li>').join('')+'</ol>':'<p>Independent qualification requires point-in-time source vintages, a predeclared target, chronological held-out tests, costs, uncertainty, regime coverage, complete trial records and prospective shadow results.</p>')+
   '<p>'+link('data/gsi-calibration.json','Calibration status')+' · '+link('data/gsi-horizons.json','Horizon status')+' · <a href="/position-sizer.html">Explicit portfolio scenarios</a> · <a href="/risk-gate.html">Risk Gate</a></p></section>';
 }
 function render(p,options={}){
  if(!p||p.contract!=='global-stress-research.v1')return '<section class="notice" role="alert"><h2>Verified Global Stress research unavailable</h2><p>No legacy composite score or portfolio instruction is shown.</p></section>';
  const basis=options.basis==='price'?'price':'adjusted',horizon=['21','63','all'].includes(options.horizon)?options.horizon:'63';
  const age=((options.now==null?Date.now():options.now)-Date.parse(p.generated_at))/3600000,q=p.quality||{};
  let html='<section class="notice"><div class="eyebrow">RESEARCH AUTHORITY</div><h2>WAIT <span>— research abstains</span></h2><p>Inspect cross-market prices, credit spreads and co-movement. No qualified crisis probability, trading vote or portfolio multiplier.</p><p class="muted">Compiled '+esc(p.generated_at)+' · '+(age<0||age>18||!Number.isFinite(age)?'<strong class="warn">Packet stale or clock invalid.</strong>':'Source observation dates determine freshness.')+'</p><p>'+link((p.replay||{}).manifest_key,'Reproduce this run')+' · '+link('data/global-stress.json','Download native histories')+'</p></section>';
  if(options.view==='qualification')return html+qualification(options.status);
  html+='<section><h2>Cross-market source desk</h2><p>'+fmt(q.fresh_instruments,0)+' / '+fmt(q.expected_instruments,0)+' fresh instruments · '+fmt(q.fresh_native_series,0)+' / '+fmt(q.expected_native_series,0)+' fresh spread, volatility and yield series. USD ETFs are proxies for their portfolios; they are not local-currency market indices.</p>'+
   '<div class="controls"><label for="global-basis">Chart and return basis <select id="global-basis"><option value="adjusted"'+(basis==='adjusted'?' selected':'')+'>Dividend adjusted</option><option value="price"'+(basis==='price'?' selected':'')+'>Price, excludes dividends</option></select></label><label for="global-window">Chart window <select id="global-window">'+[['21','21 observed intervals'],['63','63 observed intervals'],['all','Available history']].map(([v,l])=>'<option value="'+v+'"'+(v===horizon?' selected':'')+'>'+l+'</option>').join('')+'</select></label></div></section>'+
   ['Equity','Bond','Gold'].map(group=>'<section><h2>'+esc(group)+' instruments</h2><div class="measure-grid">'+Object.values(p.instruments||{}).filter(r=>r.group===group).map(r=>card(r,basis,horizon)).join('')+'</div></section>').join('');
  html+='<section><h2>Credit spreads, VIX and Treasury yields</h2><p>Each level and change retains its native unit. VIX uses index points; yields and OAS use percent levels with basis-point changes. DGS10 is a yield, not MOVE or implied rate volatility.</p><div class="table-wrap"><table><thead><tr><th>Series / original sources</th><th>Level</th><th>Observed</th><th>13-week change</th><th>Baseline</th><th>Quality</th></tr></thead><tbody>'+Object.values(p.measurements||{}).map(r=>'<tr><td><details><summary>'+esc(r.series_id)+'</summary><p>'+esc(r.title)+'</p><p>'+link(((r.originals||{}).definition||{}).key,'Definition')+' · '+link(((r.originals||{}).observations||{}).key,'Observations')+'</p><p>'+esc(r.frequency)+' · '+esc(r.seasonal_adjustment)+'</p><p>Provider updated '+esc(r.provider_updated_at)+'; acquired '+esc(r.acquired_at)+'. First publication: not supplied.</p><p>'+esc((r.change||{}).formula)+'</p></details></td><td>'+fmt(r.value)+' '+(r.unit==='percent'?'%':'index points')+'</td><td>'+day(r.observation_date)+'</td><td>'+fmt((r.change||{}).value)+' '+((r.change||{}).unit==='basis_points'?'bp':'index points')+'</td><td>'+day((r.change||{}).start_date)+'</td><td>'+esc((r.quality||{}).status)+'</td></tr>').join('')+'</tbody></table></div><p>CCC-and-lower minus BB OAS: '+fmt((p.credit_dispersion||{}).value)+' basis points on '+day((p.credit_dispersion||{}).observation_date)+'. Both source rows must share that date.</p></section>';
  html+='<section><h2>Date-matched co-movement</h2><p>'+esc((p.correlations||{}).interpretation)+'</p>'+correlationTable(p.correlations||{})+'</section>'+
   '<section><h2>Interpretation and dependencies</h2><ul>'+Object.values(p.methodology||{}).map(v=>'<li>'+esc(v)+'</li>').join('')+'</ul><details><summary>Related research ('+Object.keys(p.context||{}).length+')</summary><ul>'+Object.entries(p.context||{}).map(([k,r])=>'<li>'+link(r.source,k)+' · '+esc(r.source_generated_at||'Unavailable')+' · '+esc(r.declared_quality)+'</li>').join('')+'</ul><p>Links describe upstream packets; they do not certify original sources or create independent votes.</p></details></section>'+qualification(options.status)+
   '<details><summary>Source failures ('+Object.keys(p.source_failures||{}).length+')</summary><ul>'+Object.entries(p.source_failures||{}).map(([k,v])=>'<li>'+esc(k)+': '+esc(v)+'</li>').join('')+'</ul></details>';
  return html;
 }
 async function start(){
  const target=document.getElementById('global-stress-research');if(!target)return;
  const ctl=new AbortController(),timer=setTimeout(()=>ctl.abort(),25000),view=target.dataset.view||'research';
  async function load(key){const r=await fetch('/data/'+key+'.json',{cache:'no-store',signal:ctl.signal});if(!r.ok)throw new Error('HTTP '+r.status);return r.json();}
  try{const p=await load('global-stress');let status=null;if(view==='qualification')status=await load(target.dataset.status==='horizons'?'gsi-horizons':'gsi-calibration');
   const options={basis:'adjusted',horizon:'63',view,status};target.innerHTML=render(p,options);
   target.addEventListener('change',event=>{if(!['global-basis','global-window'].includes(event.target.id))return;
    const id=event.target.id;options[id==='global-basis'?'basis':'horizon']=event.target.value;target.innerHTML=render(p,options);document.getElementById(id).focus();});
  }catch(_){target.innerHTML='<section class="notice" role="alert"><h2>Global Stress research temporarily unavailable</h2><p>The public source contract could not be loaded. No score or portfolio advice is shown.</p><button id="global-retry" type="button">Retry</button></section>';document.getElementById('global-retry').addEventListener('click',start);}
  finally{clearTimeout(timer);}
 }
 const api={render,chart,link,fmt,correlationTable};if(typeof module!=='undefined'&&module.exports)module.exports=api;
 if(typeof document!=='undefined')start();root.JHGlobalStressResearch=api;
})(typeof globalThis!=='undefined'?globalThis:this);
