const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-insider-research.js');
const f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/insider-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Python insider replay output verifies exact current, run and output bytes',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('edited counts, incorrect run and foreign paths are rejected',async()=>{
 const x=structuredClone(p);x.windows.last_30d.buy_count=999;await assert.rejects(api.verifyPacket(x,fetcher),/Current packet differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Run differs/);
 const y=structuredClone(p);y.replay.manifest_key='https://other.invalid/run';await assert.rejects(api.verifyPacket(y,fetcher),/Native insider research required/);
});
test('sample cannot qualify itself, a regime or a market population',()=>{
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 assert.equal(api.typed({...p,regime:'ACCUMULATING'}),false);assert.equal(api.typed({...p,coverage:{population_complete:true}}),false);
});
test('weekend schedule does not hide sample-age expiry or future generation',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+80*3600000),false);
 const x=structuredClone(p);x.freshness.sample_valid_until=new Date(at-1).toISOString();assert.equal(api.current(x,at),false);
 assert.match(api.render(x,at),/current use withheld/);
});
test('both date bases and missing currency are visible without an invented dollar ratio',()=>{
 const html=api.render(p,at);assert.match(html,/Transaction-date windows/);assert.match(html,/Filing-date windows/);
 assert.match(html,/Unknown currency/);assert.match(html,/no aggregate dollar ratio/);assert.match(html,/not a complete market population/);
 assert.match(html,/7 rows across 1 pages/);assert.match(html,/0000000123/);assert.doesNotMatch(html,/INSIDERS_ACCUMULATING|GENERATIONAL_BUY|buy\/sell \(\$\)/);
});
test('text, numeric fields and a malformed run link cannot inject HTML',()=>{
 const x=structuredClone(p);x.coverage.stop_reason='<img onerror=1>';x.windows.last_7d.days='<img>';x.replay.manifest_key='javascript:alert(1)';
 x.notable_cluster_buys[0].reported_symbols=['<svg onload=1>'];const html=api.render(x,at);assert.doesNotMatch(html,/<img|<svg|javascript:/);assert.match(html,/&lt;img/);
});
test('signed, zero and invalid explicit portfolio scenarios',()=>{
 assert.equal(api.scenario('100000','-10'),-10000);assert.equal(api.scenario('-100000','-10'),10000);assert.equal(api.scenario('0','-10'),0);
 for(const args of [['','10'],['100',''],['NaN','10'],['100','-101'],['1000000000001','10']])assert.throws(()=>api.scenario(...args));
});
test('related pages direct readers to current evidence and remove legacy timing',()=>{
 for(const name of ['baggers.html','sector-flow.html','insiders.html'])assert.match(fs.readFileSync(path.join(__dirname,'..',name),'utf8'),/href="\/insider-research.html"/);
 const baggers=fs.readFileSync(path.join(__dirname,'../baggers.html'),'utf8');assert.doesNotMatch(baggers,/ia.headline_ratio_30d_dollar\|\|0/);
 const html=fs.readFileSync(path.join(__dirname,'../insider-research.html'),'utf8');assert.match(html,/jh-insider-research.js\?v=20260920-native1/);assert.match(html,/id="insider-scenario"/);
});


test('editing a scenario invalidates its previous displayed result',()=>{
 const prior=globalThis.document,form={elements:{exposure:{value:'100000'},shock:{value:'-10'}}},out={textContent:''};
 globalThis.document={getElementById:id=>id==='insider-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$10,000.00/);form.elements.shock.value='5';form.oninput();assert.match(out.textContent,/Assumptions changed/);form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$5,000.00/);}finally{globalThis.document=prior;}
});
