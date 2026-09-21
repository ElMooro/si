const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-sector-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/sector-native.json'),'utf8'));
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
const zero=()=>Object.fromEntries(api.SECTORS.map(k=>[k,'0']));
test('Sector browser verification matches actual Python original-replay outputs for both producers',async()=>{
 for(const p of [f.rotation,f.tilt]){assert.deepEqual(await api.verifyPacket(p,fetcher),p);const bad=structuredClone(p);(bad.sectors||bad.tilts)[0].comparisons['21'].value+=1;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);}
 await assert.rejects(api.verifyPacket(f.rotation,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
});
test('Sector artifacts and output authority have strict boundaries',async()=>{
 for(const key of ['accounts/example.json','data/sector-research/runs/../../private.json','https://example.invalid/x'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
 for(const k of ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible'])assert.equal(api.typed({...f.rotation,[k]:true}),false);
 const bad=structuredClone(f.rotation);bad.sectors[0].calls_eligible=true;assert.equal(api.typed(bad),false);
 assert.equal(api.typed({...f.rotation,generated_at:'1900-01-01'}),false);
});
test('Sector pages label dates, observed intervals, excess and ratio separately',()=>{
 for(const p of [f.rotation,f.tilt])for(const window of api.WINDOWS){const html=api.render(p,{window},Date.parse(p.generated_at));assert.match(html,/Excess · percentage points/);assert.match(html,/Ratio return · %/);assert.match(html,/exclude dividends and costs/);assert.match(html,/zero independent investment votes/);assert.match(html,/no pairwise deletion/);assert.ok(html.includes(p.reference_date));}
 assert.throws(()=>api.render(f.rotation,{window:20}),/declared/);
 assert.equal(api.current(f.rotation,Date.parse(f.rotation.generated_at)-1),false);
 assert.match(api.render(f.rotation,{},Date.parse(f.rotation.source_valid_until)),/source check overdue/);
});
test('Sector signed exposure P&L uses matching horizon returns and a separately reset-dollar risk sample',()=>{
 const exposures=zero();exposures.XLK='100000';exposures.XLF='-50000';const s=api.scenario(f.rotation,exposures,21);
 const rows=f.rotation.sectors,expected=100000*rows.find(r=>r.symbol==='XLK').comparisons['21'].value/100-50000*rows.find(r=>r.symbol==='XLF').comparisons['21'].value/100;assert.equal(s.pnl_usd,expected);
 const dollars=f.rotation.risk_sample.return_rows.map(r=>1000*r.returns_percent.XLK-500*r.returns_percent.XLF),mean=dollars.reduce((a,b)=>a+b,0)/63,sd=Math.sqrt(dollars.reduce((a,b)=>a+(b-mean)**2,0)/62);
 assert.ok(Math.abs(s.sample.stddev_usd-sd)<1e-9);assert.equal(s.sample.worst_observed_usd,Math.min(...dollars));
 for(const k of api.SECTORS)exposures[k]=String(-Number(exposures[k]));const opposite=api.scenario(f.rotation,exposures,21);assert.equal(opposite.pnl_usd,-s.pnl_usd);assert.equal(opposite.sample.stddev_usd,s.sample.stddev_usd);
 assert.equal(api.scenario(f.tilt,zero(),21).sample.stddev_usd,0);
});
test('Sector scenarios reject missing exposed legs and invalid inputs without silent omission',()=>{
 const bad=structuredClone(f.rotation),row=bad.sectors[0];row.comparisons['21'].value=null;row.comparisons['21'].status='missing_endpoint';const e=zero();e[row.symbol]='100';assert.throws(()=>api.scenario(bad,e,21),/Missing matched return/);e[row.symbol]='0';assert.equal(api.scenario(bad,e,21).pnl_usd,0);
 for(const value of ['', 'Infinity','10000000001','NaN',null]){const e=zero();e.XLK=value;assert.throws(()=>api.scenario(f.rotation,e,21));}
 const missing=zero();delete missing.XLK;assert.throws(()=>api.scenario(f.rotation,missing,21));
 const noRisk=structuredClone(f.rotation);noRisk.risk_sample.status='unavailable';assert.equal(api.scenario(noRisk,zero(),21).sample,null);
});
test('Sector scenario controls invalidate results on edits and source/window changes',()=>{
 const form={elements:Object.fromEntries(api.SECTORS.map(k=>[k,{value:k==='XLK'?'100000':'0'}]))},out={textContent:''},host={querySelector:s=>s==='[data-sector-exposures]'?form:out};let p=f.rotation,window=21;
 const invalidate=api.bindScenario(host,()=>p,()=>window);form.onsubmit({preventDefault(){}});assert.match(out.textContent,/Retrospective 21-interval/);assert.match(out.textContent,/resets those dollar exposures/);form.oninput();assert.match(out.textContent,/recalculate/);window=5;invalidate();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/Retrospective 5-interval/);p=null;invalidate();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/Wait for verified/);
});
test('Sector chart keeps gaps, exact range and safe labels',()=>{
 const p=structuredClone(f.rotation);p.relative_performance_history=p.relative_performance_history.slice(-3);p.relative_performance_history.forEach((r,i)=>{r.sectors.XLK.excess_percentage_points=i===1?null:i?0.15:0.05;});const chart=api.chart(p,'XLK');assert.match(chart,/cy="55"/);assert.match(chart,/cy="215"/);assert.doesNotMatch(chart,/<line/);
 p.sectors[0].name='<img src=x onerror=alert(1)>';p.replay.manifest_key='x" onclick="bad';const html=api.render(p);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('Sector pages use the verified renderer and consumers receive no legacy recommendations',()=>{
 for(const file of ['rotation/index.html','sectors.html','sector-tilt.html']){const html=fs.readFileSync(path.join(__dirname,'..',file),'utf8');assert.match(html,/jh-sector-research.js\?v=20260921-native1/);assert.match(html,/data-sector-exposures/);assert.doesNotMatch(html,/jh-wire.js|jh-page-ai.js|interp-kit.js/);}
 const view=api.decisionView();assert.equal(view.portfolio_action,'WAIT');assert.deepEqual(view.sectors,[]);assert.deepEqual(view.tilts,[]);
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'sector-rotation.json', 'sector-tilt.json'/);assert.match(worker,/activity-research\|sector-research\|sector-tilt-research/);
 for(const [fn,bytes] of [['sector-rotation',35346],['sector-tilt',18200]])assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-'+fn+'/source/legacy_'+fn.replaceAll('-','_')+'.py')).size,bytes);
});
test('Every native sector related-research link resolves to a repository page',()=>{
 for(const name of ['rotation/index.html','sectors.html','sector-tilt.html']){
  const html=fs.readFileSync(path.join(__dirname,'..',name),'utf8');
  for(const match of html.matchAll(/href="(\/[^"?#]*)"/g)){
   const route=match[1],target=route.endsWith('/')?route+'index.html':route;
   assert.ok(fs.existsSync(path.join(__dirname,'..',target.slice(1))),name+' has missing related route '+route);
  }
 }
});
