const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-retail-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/retail-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('complete Python attention output verifies exact run and body in browser',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('edited counts, foreign references and empty run cannot pass',async()=>{
 const x=structuredClone(p);x.communities['all-stocks'].sample_mentions=999;await assert.rejects(api.verifyPacket(x,fetcher),/Current packet differs/);
 const y=structuredClone(p);y.replay.manifest_key='https://other.invalid';await assert.rejects(api.verifyPacket(y,fetcher),/Native retail/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Run differs/);
});
test('attention cannot self-qualify or claim a regime or representative population',()=>{
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 assert.equal(api.typed({...p,market_regime:'MANIA'}),false);assert.equal(api.typed({...p,quality:{population_complete:true}}),false);
});
test('sample expiry precedes next daily pipeline and future sample is refused',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+7200000),false);
 assert.match(api.render(p,at+7200000),/current use withheld/);
});
test('render preserves actual coverage, undefined baselines and source-time limits',()=>{
 const h=api.render(p,at);for(const s of ['Community coverage','StockTwits tagged-message samples','unknown','Zero baseline','daily at 19:10','not whole-market growth','retained run']){
  if(s!=='unknown')assert.ok(h.includes(s),s);
 }
 assert.match(h,/663/);assert.match(h,/missing or invalid/);assert.match(h,/zero baseline/);assert.doesNotMatch(h,/PRIVATE_TEXT|PRIVATE_NAME|PRIVATE_ACCOUNT_FLAG|9,999%|price-confirmed/);
});
test('provider text and malformed trace link are escaped',()=>{
 const x=structuredClone(p);x.communities['all-stocks'].rows[0].symbol='<img onerror=1>';x.replay.manifest_key='javascript:alert(1)';
 const h=api.render(x,at);assert.doesNotMatch(h,/<img|javascript:/);assert.match(h,/&lt;img/);
});
test('signed long, short, zero and rejected scenarios',()=>{
 assert.equal(api.scenario('100000','-10'),-10000);assert.equal(api.scenario('-100000','-10'),10000);assert.equal(api.scenario('100000','0'),0);
 for(const args of [['','10'],['100',''],['100','-101'],['NaN','10']])assert.throws(()=>api.scenario(...args));
});
test('changed assumptions invalidate old scenario output',()=>{
 const prior=globalThis.document,form={elements:{exposure:{value:'-100000'},shock:{value:'-10'}}},out={textContent:''};globalThis.document={getElementById:id=>id==='retail-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$10,000.00/);form.elements.shock.value='0';form.oninput();assert.match(out.textContent,/Assumptions changed/);form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$0.00/);}finally{globalThis.document=prior;}
});
test('real pages remove unsupported attention score displays and link research',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../retail/index.html'),'utf8');assert.match(html,/jh-retail-research.js\?v=20260920-native1/);assert.match(html,/id="retail-scenario"/);
 for(const name of ['classic-dashboard.html','digest-trends.html','chart-pro.html'])assert.match(fs.readFileSync(path.join(__dirname,'..',name),'utf8'),/href="\/retail\/"/);
 const chart=fs.readFileSync(path.join(__dirname,'../chart-pro.html'),'utf8');assert.match(chart,/State.retail = \{research_context: retail, biggest_velocity_surges: \[\]/);
 const ws=fs.readFileSync(path.join(__dirname,'../assets/jh-workspaces.js'),'utf8');assert.match(ws,/k === 'retail' \? \{research_context: values\[i\], biggest_velocity_surges: \[\]/);
});
