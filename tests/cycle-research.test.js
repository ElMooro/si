const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-cycle-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/cycle-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Cycle browser verifies a complete retained Python output',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('Cycle rejects altered arithmetic, retained bytes and unsafe paths',async()=>{
 const bad=structuredClone(p);bad.measurements.INDPRO.value=999;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
 for(const key of ['portfolio/state.json','https://other.invalid/data.json','data/cycle-research/runs/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
});
test('Cycle cannot promote legacy phases, probabilities or score authority',()=>{
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 const bad=structuredClone(p);bad.cycle.phase='EARLY';assert.equal(api.typed(bad),false);bad.cycle.phase=null;bad.cycle.recession_prob_pct=0;assert.equal(api.typed(bad),false);bad.cycle.recession_prob_pct=null;bad.track_record.hit_rate=100;assert.equal(api.typed(bad),false);
});
test('Cycle reference-month controls preserve missing history and component evidence',()=>{
 for(const month of ['2026-09-01','2026-08-01','2025-10-01']){
  const html=api.render(p,{month},at);for(const re of [/Current-vintage descriptive/,/Twelve-month baseline/,/Window mean/,/Missing YoY months/,/Signed 13-week change/,/SAHMREALTIME/,/SAHMCURRENT/,/weekly average|week average/,/Publication age/,/not observation freshness/,/current-vintage/i])assert.match(html,re);
  assert.doesNotMatch(html,/undefined|NaN/);
 }
 assert.equal(api.selection(p,{month:'2026-09-01'}).available,false);
 assert.equal(api.selection(p,{}).reference_month,'2026-08-01');assert.throws(()=>api.selection(p,{month:'1900-01-01'}));
});
test('Cycle expiry does not renew source clocks and retained values remain dated',()=>{
 assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+27*3600000),false);
 assert.match(api.render(p,{},at+27*3600000),/source check overdue/);
 const bad=structuredClone(p);bad.source_valid_until=new Date(at+365*86400000).toISOString();assert.equal(api.typed(bad),false);
});
test('Cycle source labels and artifact links are escaped',()=>{
 const bad=structuredClone(p);bad.measurements.INDPRO.label='<img src=x onerror=alert(1)>';bad.replay.manifest_key='x" onclick="bad';const html=api.render(bad,{},at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('Cycle equity and DV01 scenarios keep units, both signs and zero legs',()=>{
 assert.deepEqual(api.scenario('100000','-10','100','25'),{equity_pnl:-10000,bond_first_order_pnl:-2500,combined_pnl:-12500});
 assert.deepEqual(api.scenario('-100000','-10','-100','25'),{equity_pnl:10000,bond_first_order_pnl:2500,combined_pnl:12500});
 assert.deepEqual(api.scenario('0','0','100','-25'),{equity_pnl:0,bond_first_order_pnl:2500,combined_pnl:2500});
 for(const args of [['','0','0','0'],['1','-101','0','0'],['Infinity','0','0','0'],['1','0','10000001','0']])assert.throws(()=>api.scenario(...args));
});
test('Cycle changed assumptions invalidate previously calculated results',()=>{
 const old=globalThis.document,form={elements:Object.fromEntries(Object.entries({exposure:'100000',priceReturn:'-10',dv01:'100',yieldChange:'25'}).map(([k,value])=>[k,{value}]))},out={textContent:''};globalThis.document={getElementById:id=>id==='cycle-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$12,500.00/);form.oninput();assert.match(out.textContent,/Assumptions changed/);form.elements.priceReturn.value='-101';form.onsubmit({preventDefault(){}});assert.doesNotMatch(out.textContent,/combined approximation/);}finally{globalThis.document=old;}
});
test('Cycle page, edge and classic dashboard use the reviewed research path',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../cycle-clock.html'),'utf8');assert.match(html,/jh-cycle-research.js\?v=20260921-native1/);assert.doesNotMatch(html,/cycle-clock-history.json|recession_prob_pct|squeeze_risk_score/);
 const classic=fs.readFileSync(path.join(__dirname,'../classic-dashboard.html'),'utf8');assert.doesNotMatch(classic,/gj\("data\/cycle-clock.json"\)/);assert.match(classic,/Open verified cycle research/);
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'cycle-clock.json'/);assert.match(worker,/fomc-research\|cycle-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-cycle-clock/source/legacy_cycle_clock.py')).size,90235);
});
