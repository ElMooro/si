const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-provider-flows.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/provider-flow-native.json'),'utf8'));
function packet(kind){const ref=f.publications[kind].replay,run=JSON.parse(f.artifacts[ref.manifest_key]);return {...JSON.parse(f.artifacts[run.output.key]),replay:ref};}
const flow=packet('flow'),radar=packet('radar');
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};

test('Both flow views verify the actual Python run and output bytes',async()=>{
 assert.deepEqual(await api.verifyPacket(flow,fetcher),flow);assert.deepEqual(await api.verifyPacket(radar,fetcher),radar);
 const wrong=structuredClone(flow);wrong.funds.SPY.aligned_windows['5'].flow_usd_decimal='999';
 await assert.rejects(api.verifyPacket(wrong,fetcher),/Current flow body differs/);
 await assert.rejects(api.verifyPacket({...flow,replay:radar.replay},fetcher),/run path differs/);
 await assert.rejects(api.verifyPacket(flow,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained flow run differs/);
 for(const key of ['private/positions.json','https://provider.invalid/','data/provider-flow-research/runs/../../secret.json'])await assert.rejects(api.load(key,fetcher),/Unapproved/);
});
test('Selected history requires exact retained bytes and the selected ticker',async()=>{
 const h=await api.history(flow,'SPY',fetcher);assert.equal(h.history.length,25);assert.equal(h.history.at(-1).flow_decimal,'0');
 const fake=structuredClone(flow);fake.funds.XLF.history=flow.funds.SPY.history;
 await assert.rejects(api.history(fake,'XLF',fetcher),/history bytes differ/);
 await assert.rejects(api.history(flow,'SPY',async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/history bytes differ/);
 const html=api.detail(flow,'SPY','5',h);assert.match(html,/Effective date/);assert.match(html,/Processed date/);assert.match(html,/Prior|prior/);assert.match(html,/current NAV/);assert.match(html,/original response|Original response/);
});
test('Eligibility, gaps and partial subtotals remain explicit',()=>{
 for(const key of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'])assert.equal(api.typed({...flow,[key]:true}),false);
 assert.equal(api.typed({...flow,quality:{...flow.quality,configured_funds:299}}),false);
 assert.equal(api.current(flow,Date.parse(flow.generated_at)-1),false);assert.equal(api.current(flow,Date.parse(flow.source_valid_until)),false);
 assert.match(api.render(flow,'flow',Date.parse(flow.source_valid_until)),/source check overdue/);
 const p=structuredClone(flow);p.complexes[0].windows['5'].flow_usd_decimal=null;p.complexes[0].windows['5'].observed_subset_flow_usd_decimal='123';p.complexes[0].windows['5'].status='incomplete';
 const html=api.groupTable(p,'5');assert.match(html,/Full group/);assert.match(html,/Observed subset/);assert.match(html,/\$123.00/);assert.match(html,/Unavailable/);
 assert.match(api.compareTable(flow,'5'),/Actual total fund flow/);assert.match(api.compareTable(flow,'5'),/Bull minus inverse/);
 assert.match(api.fundTable(flow,'5','SPY'),/1 of 300/);assert.throws(()=>api.fundTable(flow,'20'));
});
test('User-defined NAV shock uses signed exposures and separately assumed costs',()=>{
 const at=Date.parse(flow.generated_at),a=api.scenario(flow,'SPY',100000,-5,125,at),b=api.scenario(flow,'SPY',-100000,-5,125,at);
 assert.equal(a.gross_change_usd,-5000);assert.equal(a.net_change_usd,-5125);assert.equal(a.implied_units,1000);
 assert.equal(b.gross_change_usd,5000);assert.equal(b.net_change_usd,4875);assert.equal(b.implied_units,-1000);
 assert.equal(api.scenario(flow,'SPY',100000,0,0,at).net_change_usd,0);
 for(const args of [[0,1,0],[true,1,0],[1,'',0],[NaN,1,0],[1,-101,0],[1,1001,0],[1,1,-1]])assert.throws(()=>api.scenario(flow,'SPY',...args,at));
 assert.throws(()=>api.scenario(flow,'SPY',100,1,0,Date.parse(flow.source_valid_until)),/current verified/);
 const p=structuredClone(flow);p.funds.SPY.quality.status='invalid';assert.throws(()=>api.scenario(p,'SPY',100,1,0,at),/unavailable/);
});
test('Scenario form invalidates assumptions and handles unavailable research',()=>{
 const form={elements:{exposure:{value:'100000'},shock:{value:'-5'},cost:{value:'125'}}},out={innerHTML:'',textContent:''},host={querySelector:s=>s==='[data-pf-scenario]'?form:out};
 let p=flow;const invalidate=api.bindScenario(host,()=>p,()=>'SPY');const realNow=Date.now;Date.now=()=>Date.parse(flow.generated_at);
 try{form.onsubmit({preventDefault(){}});assert.match(out.innerHTML,/-\$5,125.00/);assert.match(out.innerHTML,/not a historical total return/);form.oninput();assert.match(out.textContent,/recalculate/);p=null;invalidate();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/current verified/);}finally{Date.now=realNow;}
});
test('Pages preserve source escaping, all predecessor content and related research routes',()=>{
 const p=structuredClone(flow);p.complexes[0].name='<img src=x onerror=evil()>';assert.match(api.groupTable(p,'5'),/&lt;img/);assert.doesNotMatch(api.groupTable(p,'5'),/<img/);
 for(const page of ['flows.html','capital-flow-radar.html']){
  const source=fs.readFileSync(path.join(__dirname,'..',page),'utf8');assert.match(source,/jh-provider-flows.js\?v=20260921-native2/);assert.doesNotMatch(source,/jh-page-ai.js|ai-analysis.json/);
  for(const match of source.matchAll(/href="(\/[^"?#]*)"/g)){const route=match[1],target=route.endsWith('/')?route+'index.html':route;assert.ok(fs.existsSync(path.join(__dirname,'..',target.slice(1))),route);}
  const old=fs.readFileSync(path.join(__dirname,'../docs/legacy/provider-flow-'+page.replace('.html','')+'-pre-native-20260921.html.txt'),'utf8');assert.ok(old.length>5000);
 }
});
