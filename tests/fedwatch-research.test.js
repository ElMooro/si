const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-fedwatch-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/fedwatch-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('fedwatch verifies the complete retained Python publication',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('fedwatch rejects changed current bytes, retained bytes, or unapproved paths',async()=>{
 const bad=structuredClone(p);bad.contracts[0].latest_bar.close_field=1;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
 for(const key of ['portfolio/state.json','https://other.invalid/data.json','data/fedwatch-research/runs/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
});
test('fedwatch typed boundary rejects probabilities and investment authority',()=>{
 assert.equal(api.typed({meetings_ahead:[{probabilities_pct:{hike:100}}]}),false);
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 const bad=structuredClone(p);bad.contracts[0].official_settlement_verified=true;assert.equal(api.typed(bad),false);
 bad.contracts[0].official_settlement_verified=false;bad.meetings_ahead[0].probabilities_pct={hike:100};assert.equal(api.typed(bad),false);
});
test('fedwatch separates quote clocks and rate definitions on the page',()=>{
 const html=api.render(p,at);for(const re of [/Rate equivalent = 100/,/in progress/,/not certified/,/newer metadata mark does not renew/,/monthly average/,/not substituted for EFFR/,/not establish a future policy effective date/,/never clipped into 100%/,/not the CME FedWatch model/,/Current-vintage replay/,/Exact close field/])assert.match(html,re);
 assert.doesNotMatch(html,/AGGRESSIVE_HIKING|100% chance/);
});
test('fedwatch expiry is recalculated from bar and capture clocks',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);assert.equal(api.usable(p,p.contracts[0],at),true);
 assert.match(api.render(p,at+27*3600000),/capture refresh overdue/);
 const q=structuredClone(p.contracts[0]);q.quote_valid_until=new Date(at-1).toISOString();assert.equal(api.usable(p,q,at),false);
 const bad=structuredClone(p);bad.freshness.capture_check_due_at=new Date(at+365*86400000).toISOString();assert.equal(api.typed(bad),false);
});
test('fedwatch escapes provider strings and does not link injected paths',()=>{
 const bad=structuredClone(p);bad.contracts[0].reason='<img src=x onerror=alert(1)>';bad.contracts[0].available=false;bad.replay.manifest_key='x" onclick="bad';
 const html=api.render(bad,at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('ZQ entered price changes preserve long and short contract signs',()=>{
 let s=api.scenario('10','96','95.75');assert.equal(s.pnl,-10417.5);assert.equal(s.rate_equivalent_change_bps,25);assert.ok(Math.abs(s.pnl_per_rate_bp+416.7)<1e-9);
 s=api.scenario('-10','96','95.75');assert.equal(s.pnl,10417.5);assert.equal(s.rate_equivalent_change_bps,25);
 s=api.scenario('0','96','95.75');assert.equal(s.pnl,0);assert.equal(s.pnl_per_rate_bp,0);
 s=api.scenario('1','100','100.25');assert.equal(s.pnl,1041.75);assert.equal(s.rate_equivalent_change_bps,-25);
});
test('ZQ scenario rejects missing, fractional count, nonfinite and extreme inputs',()=>{
 for(const args of [['','96','95.75'],['1.5','96','95.75'],['Infinity','96','95.75'],['1000001','96','95.75'],['1','1001','96'],['1','96','NaN']])assert.throws(()=>api.scenario(...args));
});
test('ZQ changing assumptions invalidates the earlier result',()=>{
 const old=globalThis.document,form={elements:Object.fromEntries(Object.entries({contracts:'10',entry:'96',exit:'95.75'}).map(([k,value])=>[k,{value}]))},out={textContent:''};globalThis.document={getElementById:id=>id==='fedwatch-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$10,417.50/);form.oninput();assert.match(out.textContent,/Assumptions changed/);form.elements.contracts.value='1.5';form.onsubmit({preventDefault(){}});assert.match(out.textContent,/integer contract count/);assert.doesNotMatch(out.textContent,/hypothetical futures price P&L/);}finally{globalThis.document=old;}
});
test('Fedwatch dedicated page replaces unchecked generic feed and retains old source',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../fedwatch.html'),'utf8');assert.match(html,/jh-fedwatch-research.js\?v=20260920-native1/);assert.match(html,/margin funding/);
 const lce=fs.readFileSync(path.join(__dirname,'../lce.html'),'utf8');assert.match(lce,/href="\/fedwatch.html"/);assert.doesNotMatch(lce,/data\/fedwatch.json\|/);
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'fedwatch.json'/);assert.match(worker,/implied-research\|fedwatch-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-fedwatch-rate-probability/source/legacy_fedwatch.py')).size,17403);
});
