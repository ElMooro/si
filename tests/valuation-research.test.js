const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-valuation-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/valuation-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('valuation browser verifies exact Python retained output and run',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('changed current values and retained bytes fail verification',async()=>{
 const bad=structuredClone(p);bad.measurements.GDP.value=1;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 const tampered=async key=>{const r=await fetcher(key);return {...r,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer};};
 await assert.rejects(api.verifyPacket(p,tampered),/Retained run differs/);
 for(const k of ['portfolio/state.json','https://other.invalid/data.json','data/valuation-research/runs/../../private.json'])await assert.rejects(api.load(k,fetcher),/Unapproved research path/);
});
test('legacy valuation score and self-qualified trade authority refused',()=>{
 assert.equal(api.typed({composite:{score:70.2,regime:'OVERVALUED'}}),false);
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 assert.equal(api.typed({...p,composite:{score:50,regime:'FAIR VALUE'}}),false);
 const b=structuredClone(p);b.measurements.GDP.value=true;assert.equal(api.typed(b),false);
});
test('units, missing definitions, source dates and comparison caveats displayed',()=>{
 const html=api.render(p,at);for(const re of [/Billions of Dollars/,/Observation period/,/GDP is an annualized quarterly flow/,/Source unqualified/,/Current-vintage revisions/,/not probabilities/,/not an option-adjusted spread/,/exact twelve-month baseline/,/ETF share multiplier/,/Matching reviewed compiler/])assert.match(html,re);
 assert.doesNotMatch(html,/bullish|bearish|buy signal/i);assert.match(html,/Original response history coverage/);
});
test('expired evidence stays historical rather than renewed by the page',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);
 const html=api.render(p,at+27*3600000);assert.match(html,/refresh overdue/);assert.match(html,/Unavailable or expired/);
});
test('source text and run links cannot inject HTML',()=>{
 const bad=structuredClone(p);bad.measurements.GDP.label='<img src=x onerror=alert(1)>';bad.source_gaps.GLD='<script>alert(1)</script>';bad.replay.manifest_key='x" onclick="alert(1)';
 const html=api.render(bad,at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|<script|onclick=|javascript:/);
});
test('EPS and multiple produce signed fixed-unit scenario P&L',()=>{
 assert.deepEqual(api.scenario('100000','100','4','20'),{assumed_price:80,change_percent:-19.999999999999996,pnl:-19999.999999999996});
 assert.ok(Math.abs(api.scenario('-100000','100','4','20').pnl-20000)<1e-8);
 assert.equal(api.scenario('100000','100','5','20').pnl,0);assert.equal(api.scenario('100000','100','0','20').pnl,-100000);
 assert.equal(api.scenario('0','100','5','20').pnl,0);
 for(const args of [['','100','5','20'],['1','0','5','20'],['1','100','-1','20'],['1','100','5','-2'],['Infinity','100','5','20'],['1','100','5','1001']])assert.throws(()=>api.scenario(...args));
});
test('editing assumptions invalidates the old calculation',()=>{
 const old=globalThis.document,form={elements:{exposure:{value:'-100000'},price:{value:'100'},eps:{value:'4'},multiple:{value:'20'}}},out={textContent:''};globalThis.document={getElementById:id=>id==='valuation-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$20,000.00/);assert.match(out.textContent,/\$80.00/);form.oninput();assert.match(out.textContent,/Assumptions changed/);form.elements.eps.value='5';form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$0.00/);}finally{globalThis.document=old;}
});
test('native page, preserved whole predecessor and mutable edge paths',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../valuations-macro.html'),'utf8');assert.match(html,/jh-valuation-research.js\?v=20260920-native1/);assert.match(html,/EPS × P\/E/);assert.doesNotMatch(html,/lambda-url|gauge-fill|triggerEngine/);
 for(const m of html.matchAll(/<script[^>]+src="(\/[^"?]+)(?:\?[^\"]*)?"/g))assert.ok(fs.existsSync(path.join(__dirname,'..',m[1])));
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'valuations-data.json'/);assert.match(worker,/retail-research\|valuation-research/);
 assert.match(fs.readFileSync(path.join(__dirname,'../jh-extremes-research.js'),'utf8'),/retail\|valuation/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-valuations-agent/source/legacy_valuations_agent.py')).size,18441);
});
