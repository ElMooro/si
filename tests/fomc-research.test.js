const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-fomc-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/fomc-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('FOMC browser verifies the full retained Python event study',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('FOMC rejects altered current outcomes, retained bytes and unsafe paths',async()=>{
 const bad=structuredClone(p);bad.events[0].assets.DGS2.forward_changes['1'].value=999;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
 for(const key of ['portfolio/state.json','https://other.invalid/data.json','data/fomc-research/runs/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
});
test('FOMC cannot promote a legacy surprise, accuracy figure or forecast probability',()=>{
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 const bad=structuredClone(p);bad.surprise.label='DOVISH';assert.equal(api.typed(bad),false);bad.surprise.label=null;bad.self_grading.directional_accuracy_pct=100;assert.equal(api.typed(bad),false);bad.self_grading.directional_accuracy_pct=null;bad.summaries[0].forecast_probability=.99;assert.equal(api.typed(bad),false);
});
test('FOMC selections expose true horizons and all sample evidence',()=>{
 for(const opts of [{series:'DGS2',horizon:63,condition:'all'},{series:'SP500',horizon:5,condition:'up'},{series:'NASDAQCOM',horizon:1,condition:'down'}]){
  const html=api.render(p,opts,at);for(const re of [/not certified exchange sessions/,/No 1-day result is relabelled 5-day/,/Overlapping sample pairs/,/Actual start/,/Original source row references/,/not bond total returns/,/former single-event accuracy figure is unqualified/,/not forecast or confidence intervals/,/<svg/])assert.match(html,re);
 }
 for(const opts of [{series:'QQQ'},{horizon:2},{condition:'hawkish'}])assert.throws(()=>api.selection(p,opts));
});
test('FOMC missing histories remain missing and expiry stays source bound',()=>{
 const bad=structuredClone(p);bad.measurements.NASDAQCOM.value=null;bad.measurements.NASDAQCOM.history.numeric_rows=0;bad.measurements.NASDAQCOM.history.returned_rows=0;
 for(const e of bad.events)e.assets.NASDAQCOM.forward_changes['5']={...e.assets.NASDAQCOM.forward_changes['5'],available:false,value:null,reason:'exact_event_observation_missing'};
 assert.match(api.render(bad,{series:'NASDAQCOM'},at),/No observed outcomes/);assert.match(api.render(p,{},at+27*3600000),/source refresh overdue/);
 assert.equal(api.current(p,at-1),false);bad.freshness.pipeline_check_due_at=new Date(at+365*86400000).toISOString();assert.equal(api.typed(bad),false);
});
test('FOMC source labels are escaped and cannot inject links',()=>{
 const bad=structuredClone(p);bad.measurements.DGS2.label='<img src=x onerror=alert(1)>';bad.replay.manifest_key='x" onclick="bad';const html=api.render(bad,{},at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('equity and DV01 scenarios preserve both signs and separate units',()=>{
 assert.deepEqual(api.scenario('100000','-10','100','25'),{equity_pnl:-10000,bond_first_order_pnl:-2500,combined_pnl:-12500});
 assert.deepEqual(api.scenario('-100000','-10','-100','25'),{equity_pnl:10000,bond_first_order_pnl:2500,combined_pnl:12500});
 assert.deepEqual(api.scenario('0','0','100','-25'),{equity_pnl:0,bond_first_order_pnl:2500,combined_pnl:2500});
 for(const args of [['','0','0','0'],['1','-101','0','0'],['Infinity','0','0','0'],['1','0','10000001','0']])assert.throws(()=>api.scenario(...args));
});
test('FOMC scenario edits discard earlier results',()=>{
 const old=globalThis.document,form={elements:Object.fromEntries(Object.entries({exposure:'100000',priceReturn:'-10',dv01:'100',yieldChange:'25'}).map(([k,value])=>[k,{value}]))},out={textContent:''};globalThis.document={getElementById:id=>id==='fomc-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$12,500.00/);form.oninput();assert.match(out.textContent,/Assumptions changed/);form.elements.priceReturn.value='-101';form.onsubmit({preventDefault(){}});assert.doesNotMatch(out.textContent,/combined approximation/);}finally{globalThis.document=old;}
});
test('FOMC page and edge use the native publication and retain full old source',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../fomc.html'),'utf8');assert.match(html,/jh-fomc-research.js\?v=20260920-native1/);assert.doesNotMatch(html,/fomc-calibration.json|prob_up_pct|directional_accuracy_pct/);
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'fomc-reaction.json'/);assert.match(worker,/fedwatch-research\|fomc-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-fomc-reaction/source/legacy_fomc_reaction.py')).size,19413);
});
