const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-nowcast-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/nowcast-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Monthly macro verifies the retained Python calculation and rejects changed values',async()=>{
 assert.deepEqual(await api.verifyPacket(p,fetcher),p);
 const bad=structuredClone(p);bad.research_index.current.value=999;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
});
test('Monthly macro rejects unsafe artifact paths and invented authority',async()=>{
 for(const key of ['portfolio/state.json','https://other.invalid/data.json','data/nowcast-research/runs/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 for(const k of ['regime','normalized_score','raw_score','confidence','call','score'])assert.equal(api.typed({...p,[k]:1}),false);
});
test('Monthly controls preserve incomplete months and exact calendar horizons',()=>{
 assert.equal(api.selection(p,{}).reference_month,'2026-07-01');assert.equal(api.selection(p,{month:'2026-08-01'}).available,false);
 for(const month of ['2026-07-01','2026-08-01','2025-10-01'])for(const horizon of [1,3,6,12]){
  const html=api.render(p,{month,horizon},at);for(const re of [/Current-vintage descriptive/,/sample SD/,/15 observations/,/Annualized|annualized/,/Nominal retail/,/not GDP growth/,/overlapping pairs/,/not release timestamps/])assert.match(html,re);
  assert.doesNotMatch(html,/undefined|NaN/);
 }
 assert.throws(()=>api.selection(p,{month:'1900-01-01'}));assert.throws(()=>api.render(p,{horizon:2},at));
});
test('Monthly macro summary binds verified research and expires source clocks',()=>{
 const summary=api.render(p,{summary:true},at);assert.match(summary,/macro-nowcast.html/);assert.doesNotMatch(summary,/<table|<select/);
 assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+27*3600000),false);
 assert.match(api.render(p,{},at+27*3600000),/source check overdue/);
 assert.equal(api.typed({...p,source_valid_until:new Date(at+365*864e5).toISOString()}),false);
});
test('Monthly macro labels and artifact URLs are escaped',()=>{
 const bad=structuredClone(p);bad.measurements.INDPRO.label='<img src=x onerror=alert(1)>';bad.replay.manifest_key='x" onclick="bad';
 const html=api.render(bad,{},at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('Monthly macro explicit equity/DV01 scenarios retain units and signs',()=>{
 assert.deepEqual(api.scenario('100000','-10','100','25'),{equity_pnl:-10000,bond_first_order_pnl:-2500,combined_pnl:-12500});
 assert.deepEqual(api.scenario('-100000','-10','-100','25'),{equity_pnl:10000,bond_first_order_pnl:2500,combined_pnl:12500});
 for(const args of [['','0','0','0'],['1','-101','0','0'],['Infinity','0','0','0']])assert.throws(()=>api.scenario(...args));
});
test('Monthly macro changed inputs clear the prior scenario',()=>{
 const old=globalThis.document,form={elements:Object.fromEntries(Object.entries({exposure:'100000',priceReturn:'-10',dv01:'100',yieldChange:'25'}).map(([k,value])=>[k,{value}]))},out={textContent:''};globalThis.document={getElementById:id=>id==='nowcast-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$12,500.00/);form.oninput();assert.match(out.textContent,/Assumptions changed/);}finally{globalThis.document=old;}
});
test('Pages replace unverified regime cards with research and keep the whole predecessor',()=>{
 for(const name of ['macro-nowcast.html','macro-data.html'])assert.match(fs.readFileSync(path.join(__dirname,'../'+name),'utf8'),/jh-nowcast-research.js\?v=20260921-native1/);
 for(const name of ['brief.html','chart-macro.html','alpha-scoreboard.html']){const html=fs.readFileSync(path.join(__dirname,'../'+name),'utf8');assert.match(html,/macro-nowcast.html/);assert.doesNotMatch(html,/fetch\([^\n]*macro-nowcast.json/);}
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'macro-nowcast.json'/);assert.match(worker,/cycle-research\|nowcast-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-macro-nowcast/source/legacy_macro_nowcast.py')).size,26143);
});
test('Sector context cannot turn a predecessor tilt into a recommendation',()=>{
 const sectors=require('../jh-sector-research.js'),html=fs.readFileSync(path.join(__dirname,'../sector-tilt.html'),'utf8');
 const predecessor={regime:'MUDDLE',tilts:[{ticker:'XLU',regime_tilt_score:3,implication:'CONFIRMED_BUY',rs_20d:2}]};
 assert.equal(sectors.typed(predecessor),false);assert.throws(()=>sectors.render(predecessor),/Native sector research required/);
 const out=sectors.decisionView(predecessor);assert.equal(out.portfolio_action,'WAIT');assert.deepEqual(out.tilts,[]);assert.equal(out.sizing_eligible,false);
 assert.doesNotMatch(html,/data-bars="tilts:ticker:regime_tilt_score"/);
 const alpha=fs.readFileSync(path.join(__dirname,'../alpha/index.html'),'utf8');assert.match(alpha,/regime_picks:\[\],regime_avoids:\[\]/);assert.match(alpha,/regime-confidence'\).textContent='Unavailable'/);
});
