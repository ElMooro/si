(function(root){
  'use strict';
  const esc=x=>String(x==null?'':x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const numeric=x=>typeof x==='number'&&Number.isFinite(x);
  const fmt=(x,digits=2)=>numeric(x)?x.toLocaleString('en-US',{maximumFractionDigits:digits}):'Unavailable';
  const units=x=>({Index:'index points',Percent:'%',percentage_points:'percentage points',basis_points:'basis points',
    'Millions of U.S. Dollars':'USD millions','Billions of US Dollars':'USD billions','Index Feb 5, 1971=100':'index points (1971=100)'}[x]||x||'');
  const day=x=>typeof x==='string'&&/^\d{4}-\d{2}-\d{2}/.test(x)?esc(x.slice(0,10)):'Not supplied';
  function link(key,label){return typeof key==='string'&&/^data\/[a-zA-Z0-9_.\/-]+$/.test(key)&&!key.includes('..')?'<a href="/'+esc(key)+'" target="_blank" rel="noopener">'+esc(label)+'</a>':esc(label+' unavailable');}
  function method(url){return /^https:\/\/(?:fred\.stlouisfed\.org|www\.chicagofed\.org|www\.kansascityfed\.org|fredblog\.stlouisfed\.org)\//.test(url||'')?'<a href="'+esc(url)+'" target="_blank" rel="noopener">Official definition</a>':'Official definition unavailable';}
  function chart(rows,unit,years){
    if(!Array.isArray(rows)||!rows.length)return '<p>History unavailable.</p>';
    const end=Date.parse(rows.at(-1).date),cutoff=years==='all'?-Infinity:end-Number(years)*365.25*86400000;
    const data=rows.filter(r=>Date.parse(r.date)>=cutoff),finite=data.filter(r=>numeric(r.value));
    if(finite.length<2)return '<p>Insufficient finite history.</p>';
    const low=Math.min(...finite.map(r=>r.value)),high=Math.max(...finite.map(r=>r.value)),span=high-low||1;
    const first=Date.parse(data[0].date),last=Date.parse(data.at(-1).date);
    if(!Number.isFinite(first)||last<=first)return '<p>History dates unavailable.</p>';
    let path='',connected=false;
    data.forEach(r=>{if(!numeric(r.value)){connected=false;return;}const x=8+584*(Date.parse(r.date)-first)/(last-first),y=78-64*(r.value-low)/span;path+=(connected?' L ':' M ')+x.toFixed(2)+' '+y.toFixed(2);connected=true;});
    return '<figure><svg viewBox="0 0 600 92" role="img" aria-label="'+esc('Native observations in '+unit+'. Calendar spacing; missing rows break the line.')+'"><path d="'+path+'" fill="none" stroke="currentColor" stroke-width="2"/></svg><figcaption>'+day(data[0].date)+' — '+day(data.at(-1).date)+' · '+fmt(low)+' to '+fmt(high)+' '+esc(unit)+'</figcaption></figure>';
  }
  function measurement(r,years){
    const q=r.quality||{},c=r.change||{},span=r.history_span||{},p=r.percentiles||{},refs=r.originals||{};
    return '<article><header><h3>'+esc(r.series_id)+'</h3><span class="badge">'+esc(q.status||'unavailable')+'</span></header><p class="muted">'+esc(r.title||'Source unavailable')+'</p>'+
      '<div class="value">'+fmt(r.value,4)+' <small>'+esc(units(r.unit))+'</small></div><p>'+esc(r.frequency||'Frequency unavailable')+' · '+(r.frequency_short==='M'?'Reference month '+esc(r.observation_period||'unavailable'):'Observed '+day(r.observation_date))+'</p>'+
      '<p class="muted">Source age '+fmt(q.age_days,0)+' days · ceiling '+fmt(q.max_age_days,0)+' days</p>'+chart(r.history,units(r.unit),years)+
      '<dl><dt>Change over '+esc(c.label||'dated interval')+'</dt><dd>'+fmt(c.value,4)+' '+esc(units(c.unit))+'</dd><dt>Baseline → current</dt><dd>'+day(c.baseline_date)+' → '+day(c.current_date)+'</dd>'+['2y','5y','10y'].map(k=>'<dt>'+esc(k)+' historical rank</dt><dd>'+fmt((p[k]||{}).value,1)+(numeric((p[k]||{}).value)?'/100':'')+'</dd>').join('')+'</dl>'+
      '<p class="muted">'+esc(r.interpretation||'No decision authority.')+'</p><details><summary>Source records, coverage and method</summary><p>'+esc(r.date_semantics||'')+'</p><p>'+esc(c.formula||'')+'</p>'+
      '<p>Actual finite history '+day(span.first_finite)+' — '+day(span.last_finite)+' · '+fmt(span.n_finite,0)+' finite / '+fmt(span.n_missing,0)+' missing provider rows. Requested history begins in 1990; availability differs by series.</p>'+
      '<p>Current vintage '+esc(r.current_vintage_date||'Unavailable')+' · provider updated '+esc(r.provider_updated_at||'Not supplied')+'<br>Acquired '+esc(r.acquired_at||'Unavailable')+'<br>First publication timestamp: not supplied. These histories cannot establish what was known at a past trading time.</p>'+
      '<p>'+link((refs.definition||{}).key,'Original definition')+' · '+link((refs.observations||{}).key,'Original observations')+' · '+method(r.method_url)+'</p>'+
      '<div class="table-wrap"><table><caption>Rank coverage · each native observation counted once</caption><thead><tr><th>Window</th><th>Finite</th><th>Missing</th><th>Full span</th></tr></thead><tbody>'+Object.entries(p).map(([k,v])=>'<tr><td>'+esc(k)+'</td><td>'+fmt(v.n_finite,0)+'</td><td>'+fmt(v.n_missing,0)+'</td><td>'+(v.complete_span?'Yes':'No')+'</td></tr>').join('')+'</tbody></table></div>'+
      '<p>Rank = 100 × (observations below + ½ observations equal) / finite observations. Includes latest. Historical rank is not a crisis probability or a predicted return.</p>'+
      '<div class="table-wrap"><table><caption>Most recent 12 native rows · '+esc(units(r.unit))+'</caption><thead><tr><th>Date</th><th>Value</th><th>Source row</th></tr></thead><tbody>'+(r.history||[]).slice(-12).reverse().map(v=>'<tr><td>'+day(v.date)+'</td><td>'+fmt(v.value,4)+'</td><td>'+esc(v.row_index)+'</td></tr>').join('')+'</tbody></table></div></details></article>';
  }
  function render(p,years='2',now=Date.now()){
    if(!p||p.contract!=='stress-research.v1')return '<section class="notice" role="alert"><h2>Verified stress research unavailable</h2><p>The current source-backed contract has not loaded. No legacy composite score or return claim is shown.</p></section>';
    if(!['2','5','10','all'].includes(years))years='2';
    const age=(now-Date.parse(p.generated_at))/3600000,stale=!Number.isFinite(age)||age<0||age>18;
    const groups=['Published financial-condition indices','Market prices and spreads','Central-bank balance sheet and funding','Market benchmark'];
    return '<section class="notice"><div class="eyebrow">RESEARCH AUTHORITY</div><h2>WAIT <span>— research abstains</span></h2><p>No qualified JSI trade, crisis probability or position multiplier. Read the native measurements and test explicit portfolio scenarios.</p><p class="muted">Compiled '+esc(p.generated_at)+' · '+(stale?'<strong class="warn">Packet stale or clock invalid.</strong>':'Observation dates below determine source age.')+'</p><p>'+link((p.replay||{}).manifest_key,'Reproduce this run')+' · '+link('data/jsi.json','Download native histories')+' · <a href="/position-sizer.html">Portfolio scenarios</a></p></section>'+
      '<section><h2>Source history</h2><p>Every series retains its native frequency and unit. Missing observations remain visible. Revised current-vintage histories support description; they do not prove historical trading performance.</p><label for="stress-window">Chart window </label><select id="stress-window">'+[['2','2 years'],['5','5 years'],['10','10 years'],['all','Available history']].map(([v,label])=>'<option value="'+v+'"'+(v===years?' selected':'')+'>'+label+'</option>').join('')+'</select></section>'+
      groups.map(g=>'<section><h2>'+esc(g)+'</h2><div class="measure-grid">'+Object.values(p.measurements||{}).filter(r=>r.group===g).map(r=>measurement(r,years)).join('')+'</div></section>').join('')+
      '<section><h2>Matched-date comparisons</h2><div class="two-columns">'+Object.values(p.spreads||{}).map(r=>'<article><h3>'+esc(r.label)+'</h3><div class="value">'+fmt(r.value)+' <small>'+esc(units(r.unit))+'</small></div><p>Observed '+day(r.observation_date)+' · '+esc((r.quality||{}).status||'unavailable')+'</p><p>'+esc(r.formula||'')+'</p><p class="muted">'+esc(r.reason||'Both source rows exist on the same date. This spread alone does not establish a crisis.')+'</p></article>').join('')+'</div></section>'+
      '<section><h2>Shared inputs and related research</h2><p>'+esc(p.dependency_note)+'</p><details><summary>'+Object.keys(p.context||{}).length+' linked engines; no score votes</summary><div class="table-wrap"><table><thead><tr><th>Engine</th><th>Compiled</th><th>Declared quality</th></tr></thead><tbody>'+Object.entries(p.context||{}).map(([k,r])=>'<tr><td>'+link(r.source,k)+'</td><td>'+esc(r.source_generated_at||'Unavailable')+'</td><td>'+esc(r.declared_quality)+'</td></tr>').join('')+'</tbody></table></div><p>Linked packet metadata does not verify that engine’s original sources.</p></details></section>'+
      '<section><h2>Qualification before portfolio use</h2><p>The former blended score, full-sample return atlas and fixed-confidence QQQ signals are unqualified. Current-vintage data, correlated overlays and a historical percentile cannot establish an out-of-sample edge.</p><p>Promotion requires point-in-time vintages, a predeclared target, sequential held-out results, costs, uncertainty, regime coverage and an independently reviewed scorecard. No JSI-driven portfolio scaling or automated transition alert is authorized.</p><p>'+link('data/jsi-calibration.json','Qualification status')+' · <a href="/risk-gate.html">Risk Gate</a> · <a href="/report.html">Research report</a></p><details><summary>Source failures ('+Object.keys(p.source_failures||{}).length+')</summary><ul>'+Object.entries(p.source_failures||{}).map(([k,v])=>'<li>'+esc(k)+': '+esc(v)+'</li>').join('')+'</ul></details></section>';
  }
  async function start(){
    const target=document.getElementById('stress-research');if(!target)return;
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),25000);
    try{const response=await fetch('/data/jsi.json',{cache:'no-store',signal:controller.signal});if(!response.ok)throw new Error('HTTP '+response.status);const p=await response.json();
      target.innerHTML=render(p);target.addEventListener('change',event=>{if(event.target.id==='stress-window'){target.innerHTML=render(p,event.target.value);document.getElementById('stress-window').focus();}});
    }catch(_){target.innerHTML='<section class="notice" role="alert"><h2>Stress research temporarily unavailable</h2><p>The public packet could not be loaded. No score or portfolio advice is shown.</p><button id="stress-retry" type="button">Retry</button></section>';document.getElementById('stress-retry').addEventListener('click',start);}
    finally{clearTimeout(timer);}
  }
  const api={render,chart,fmt,link};if(typeof module!=='undefined'&&module.exports)module.exports=api;
  if(typeof document!=='undefined')start();root.JHStressResearch=api;
})(typeof globalThis!=='undefined'?globalThis:this);
