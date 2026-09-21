const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-activity-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/activity-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Activity page verifies the actual retained Python output and rejects altered measurements',async()=>{
 assert.deepEqual(await api.verifyPacket(p,fetcher),p);
 const bad=structuredClone(p);bad.measurements.WEI.value=999;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
});
test('Activity cannot load arbitrary artifacts or acquire investment authority',async()=>{
 for(const key of ['accounts/example.json','https://other.invalid/data.json','data/activity-research/runs/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 for(const k of ['activity_index','activity_z','regime','momentum','call'])assert.equal(api.typed({...p,[k]:1}),false);
});
test('Activity selection keeps original weekly dates and incomplete current weeks',()=>{
 assert.equal(api.selection(p,{}).reference_week_ending,'2026-09-05');
 assert.equal(api.selection(p,{week:'2026-09-19'}).available_components,1);
 for(const week of ['2026-09-05','2026-09-19','2025-01-04'])for(const series of ['WEI','ICSA','BAA10Y']){
  const html=api.render(p,{week,series},at);assert.match(html,new RegExp(week));assert.match(html,/source observations/);assert.match(html,/does not establish what was publicly known/);assert.match(html,/native sign/);
 }
 assert.throws(()=>api.selection(p,{week:'1900-01-01'}));assert.throws(()=>api.render(p,{series:'UNKNOWN'},at));
});
test('Activity shows overlapping evidence, forecast target quarter and honest gaps',()=>{
 const html=api.render(p,{},at);assert.match(html,/WEI already includes initial and continuing claims/);assert.match(html,/not six independent votes/);
 assert.match(html,/target quarter, not the forecast publication day/);assert.match(html,/No Cleveland recession probability is inferred/);
 const bad=structuredClone(p);bad.weekly_context.current=null;assert.match(api.render(bad,{},at),/Latest complete common week: Unavailable/);
 assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+27*3600000),false);assert.match(api.render(p,{},at+27*3600000),/source check overdue/);
});
test('Activity labels and retained artifact URLs are escaped',()=>{
 const bad=structuredClone(p);bad.measurements.WEI.label='<img src=x onerror=alert(1)>';bad.replay.manifest_key='x" onclick="bad';
 const html=api.render(bad,{},at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('Activity hypothetical signed equity and DV01 arithmetic is explicit',()=>{
 assert.deepEqual(api.scenario('100000','-10','100','25'),{equity_pnl:-10000,bond_first_order_pnl:-2500,combined_pnl:-12500});
 assert.deepEqual(api.scenario('-100000','-10','-100','25'),{equity_pnl:10000,bond_first_order_pnl:2500,combined_pnl:12500});
 for(const args of [['','0','0','0'],['1','-101','0','0'],['Infinity','0','0','0']])assert.throws(()=>api.scenario(...args));
});
test('Activity scenario edits invalidate the prior calculation',()=>{
 const old=globalThis.document,form={elements:Object.fromEntries(Object.entries({exposure:'100000',priceReturn:'-10',dv01:'100',yieldChange:'25'}).map(([k,value])=>[k,{value}]))},out={textContent:''};globalThis.document={getElementById:id=>id==='activity-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$12,500.00/);form.oninput();assert.match(out.textContent,/Assumptions changed/);}finally{globalThis.document=old;}
});
test('Activity binds native page and mutable routes while preserving the whole predecessor',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../activity-nowcast.html'),'utf8');assert.match(html,/jh-activity-research.js\?v=20260921-native2/);assert.doesNotMatch(html,/jh-wire.js|jh-page-ai.js|interp-kit.js/);
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'activity-nowcast.json'/);assert.match(worker,/nowcast-research\|activity-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-activity-nowcast/source/legacy_activity_nowcast.py')).size,12066);
});


test('Activity displays spread deltas in basis points and uses the actual chart range',()=>{
 const out=api.render(p,{},at),c=p.weekly_context.current.components.BAA10Y;
 const cells=[...out.matchAll(/<tr><th scope="row">BAA10Y<\/th>(.*?)<\/tr>/g)].map(m=>m[1]);
 const changes=cells.find(x=>x.includes('percent / basis points'));assert.ok(changes);
 for(const n of ['1','4','13'])assert.ok(changes.includes('<td>'+c.changes[n].basis_point_change.toLocaleString('en-US',{maximumFractionDigits:5})+'</td>'));
 const q=structuredClone(p);q.weekly_context.trail.forEach((r,i)=>{r.components.NFCI.observation={value:i?0.15:0.05};});
 let chart=api.render(q,{series:'NFCI'},at).match(/<svg[\s\S]*?<\/svg>/)[0];
 assert.match(chart,/cy="50"/);assert.match(chart,/cy="220"/);
 q.weekly_context.trail.forEach(r=>{r.components.NFCI.observation={value:0.05};});
 chart=api.render(q,{series:'NFCI'},at).match(/<svg[\s\S]*?<\/svg>/)[0];
 assert.doesNotMatch(chart,/y="45"|NaN|Infinity/);assert.match(chart,/cy="220"/);
});
