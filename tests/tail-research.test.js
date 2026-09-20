const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-tail-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/tail-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('tail browser verifies retained Python run and exact output bytes',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('changed output values and altered retained bytes are refused',async()=>{
 const bad=structuredClone(p);bad.indices[0].terms[0].selections.put25.contract.strike=1;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
 for(const key of ['portfolio/state.json','https://other.invalid/data.json','data/tail-research/runs/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
});
test('legacy crash and timing authority rejected',()=>{
 assert.equal(api.typed({indices:[],system_tail_gauge:99,tail_valuation:'CHEAP'}),false);
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 assert.equal(api.typed({...p,system_tail_gauge:99}),false);
 const bad=structuredClone(p);bad.indices[0].p_drop_10=.95;assert.equal(api.typed(bad),false);
});
test('source clocks, incomplete capture and vendor-model meaning visible',()=>{
 const html=api.render(p,at);for(const re of [/observation timestamps/,/not executable/,/26 calendar days/,/61 calendar days/,/no synchronized surface claim/,/All returned pages; non-atomic/,/zero-based row reference/,/American ETF options/,/physical crash-probability/])assert.match(html,re);
 assert.doesNotMatch(html,/P\(drop|Tail Valuation/);
 const bad=structuredClone(p);bad.indices[0].sample.pagination_complete=false;bad.indices[0].sample.stop_reason='page_limit';assert.match(api.render(bad,at),/page_limit/);
});
test('expired captures become retained research without renewing evidence',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);
 const html=api.render(p,at+27*3600000);assert.match(html,/capture refresh overdue/);assert.match(html,/unqualified or historical/);
});
test('contract text cannot inject markup or link to arbitrary artifacts',()=>{
 const bad=structuredClone(p);bad.indices[0].terms[0].selections.put25.contract.contract_id='<img src=x onerror=alert(1)>';bad.replay.manifest_key='x" onclick="bad';
 const html=api.render(bad,at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('stock and purchased-put scenario accounts for premium and signed shares',()=>{
 assert.deepEqual(api.scenario('100','100','1','95','2','80'),{stock_pnl:-2000,put_premium:200,put_intrinsic:1500,put_pnl:1300,total_pnl:-700});
 assert.equal(api.scenario('100','100','1','95','2','110').total_pnl,800);
 assert.equal(api.scenario('-100','100','0','95','2','80').total_pnl,2000);
 assert.equal(api.scenario('0','100','0','95','0','0').total_pnl,0);
 for(const args of [['','100','1','95','2','80'],['100','0','1','95','2','80'],['100','100','-1','95','2','80'],['100','100','1.5','95','2','80'],['100','100','1','95','-2','80'],['Infinity','100','1','95','2','80']])assert.throws(()=>api.scenario(...args));
});
test('editing a position invalidates the old scenario result',()=>{
 const old=globalThis.document,form={elements:Object.fromEntries(Object.entries({shares:'100',price:'100',contracts:'1',strike:'95',premium:'2',settlement:'80'}).map(([k,value])=>[k,{value}]))},out={textContent:''};globalThis.document={getElementById:id=>id==='tail-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$700.00/);assert.match(out.textContent,/\$200.00 premium/);form.oninput();assert.match(out.textContent,/Assumptions changed/);form.elements.settlement.value='110';form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$800.00/);}finally{globalThis.document=old;}
});
test('whole predecessor, native page and freshness routes stay wired',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../tail-risk.html'),'utf8');assert.match(html,/jh-tail-research.js\?v=20260920-native1/);assert.match(html,/Stock and purchased-put expiry scenario/);assert.doesNotMatch(html,/System Tail Gauge|P\(drop/);
 for(const m of html.matchAll(/<script[^>]+src="(\/[^"?]+)(?:\?[^\"]*)?"/g))assert.ok(fs.existsSync(path.join(__dirname,'..',m[1])));
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'tail-risk.json'/);assert.match(worker,/valuation-research\|tail-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-tail-risk/source/legacy_tail_risk.py')).size,16030);
});
