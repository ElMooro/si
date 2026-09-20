const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-vrp-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/vrp-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('native VRP output verifies against exact Python run and output bytes',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('source edits and changed retained bytes are rejected',async()=>{
 const x=structuredClone(p);x.measurements.SP500.value=1;await assert.rejects(api.verifyPacket(x,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket({...p,replay:{manifest_key:'https://other.invalid/key'}},fetcher),/Native research required/);
 await assert.rejects(api.load('portfolio/state.json',fetcher),/Unapproved research path/);
});
test('legacy scores and self-enabled authority are not research qualification',()=>{
 assert.equal(api.typed({regime:'RICH',score:99}),false);
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 assert.equal(api.typed({...p,regime:'NORMAL'}),false);assert.equal(api.typed({...p,score:99}),false);
});
test('dated windows, actual coverage and distinct units are displayed',()=>{
 const html=api.render(p,at);assert.match(html,/21 reported intervals equal 30 calendar days/);
 assert.match(html,/Variance difference · %²/);assert.match(html,/not a strategy hit rate/);
 assert.match(html,/non-overlapping/i);assert.match(html,/252 overlapping observations/);
 assert.match(html,/First close/);assert.match(html,/Exact maturity/);assert.match(html,/√252/);assert.match(html,/no native verified nine-day/i);
});
test('expired current evidence is visibly withheld and future clocks are not current',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);
 assert.match(api.render(p,at+27*3600000),/refresh overdue/);assert.match(api.render(p,at+27*3600000),/Unavailable or expired/);
});
test('rendered source text and links cannot inject HTML',()=>{
 const x=structuredClone(p);x.measurements.SP500.label='<img src=x onerror=alert(1)>';x.replay.manifest_key='x" onclick="alert(1)';
 const html=api.render(x,at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('chart has a zero reference, proportional dates and an accessible data alternative',()=>{
 const chart=api.chart([{date:'2026-09-01',volatility_points:-2},{date:'2026-09-02',volatility_points:0},{date:'2026-09-11',volatility_points:2}]);
 assert.match(chart,/60.00,220.00 130.00,125.00 760.00,30.00/);assert.match(chart,/stroke-dasharray/);assert.match(chart,/role="img"/);
 assert.match(api.chart([]),/Insufficient/);assert.match(api.render(p,at),/Complete dated gap table/);
});
test('vega units, negative exposures, zero and rejected assumptions',()=>{
 assert.equal(api.scenario('1000','2'),2000);assert.equal(api.scenario('-1000','2'),-2000);assert.equal(api.scenario('-1000','-2'),2000);assert.equal(api.scenario('-1000','0'),0);
 for(const a of [['','2'],['1',''],['NaN','1'],['1000000001','1'],['1','101']])assert.throws(()=>api.scenario(...a));
});
test('editing a scenario invalidates the displayed estimate',()=>{
 const old=globalThis.document,form={elements:{vega:{value:'-1000'},shock:{value:'2'}}},out={textContent:''};globalThis.document={getElementById:id=>id==='vrp-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$2,000.00/);form.elements.shock.value='0';form.oninput();assert.match(out.textContent,/Assumptions changed/);form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$0.00/);}finally{globalThis.document=old;}
});
test('page loads native renderer and edge bypass covers publication and requests',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../vrp.html'),'utf8');assert.match(html,/jh-vrp-research.js\?v=20260920-native1/);assert.match(html,/Signed vega · USD per vol point/);
 for(const m of html.matchAll(/<script[^>]+src="(\/[^"?]+)(?:\?[^\"]*)?"/g))assert.ok(fs.existsSync(path.join(__dirname,'..',m[1])));
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'market-extremes.json', 'vrp.json'/);assert.match(worker,/extremes-research\|vrp-research/);
 const extremes=fs.readFileSync(path.join(__dirname,'../jh-extremes-research.js'),'utf8');assert.match(extremes,/fails\|vrp/);
});
