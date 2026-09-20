const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-implied-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/implied-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('implied browser verifies the complete Python run and retained output',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('altered measurement, retained bytes and arbitrary paths are rejected',async()=>{
 const bad=structuredClone(p);bad.measurements.DFF.value=999;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
 for(const key of ['portfolio/state.json','https://other.invalid/data.json','data/implied-research/runs/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved research path/);
});
test('legacy forecast labels and investment authority cannot pass the native contract',()=>{
 assert.equal(api.typed({recession:{ny_fed_12m_prob_pct:99}}),false);
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 const bad=structuredClone(p);bad.recession.ny_fed_12m_prob_pct=1;assert.equal(api.typed(bad),false);
 bad.recession.ny_fed_12m_prob_pct=null;bad.measurements.DFF.unit='USD';assert.equal(api.typed(bad),false);
 bad.measurements.DFF.unit='Percent';bad.descriptive_comparisons.target_midpoint.calls_eligible=true;assert.equal(api.typed(bad),false);
});
test('source identities, clocks and exact arithmetic limits are visible',()=>{
 const html=api.render(p,at);for(const re of [/Chauvet\/Piger/,/not the New York Fed/,/most recent common reported date/,/No interpolation/,/percentage points/,/not calendar days/,/252 numeric daily/,/not ten-year percentiles/,/SPX and NDX/,/IV/,/point-in-time backtest/])assert.match(html,re);
 assert.doesNotMatch(html,/MARKET PRICING HIKES|LOW RECESSION RISK|Recession composite/);
});
test('old acquisition and future publications cannot appear currently usable',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);
 assert.match(api.render(p,at+27*3600000),/source refresh overdue/);
 const bad=structuredClone(p);bad.measurements.DFF.source_valid_until=new Date(at-1).toISOString();assert.equal(api.usable(bad,'DFF',at),false);
 bad.freshness.pipeline_check_due_at=new Date(at+365*86400000).toISOString();assert.equal(api.typed(bad),false);
});
test('source text stays escaped and public artifact paths stay constrained',()=>{
 const bad=structuredClone(p);bad.measurements.DFF.label='<img src=x onerror=alert(1)>';bad.replay.manifest_key='x" onclick="bad';
 const html=api.render(bad,at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|onclick=|javascript:/);
});
test('hypothetical discrete outcomes give exact long and short signed exposure consequences',()=>{
 let s=api.scenario('100000','100','80','25','100','50','120','25');assert.ok(Math.abs(s.expected_pnl)<1e-8);assert.equal(s.loss_probability_percent,25);assert.ok(Math.abs(s.worst_entered_pnl+20000)<1e-8);
 s=api.scenario('-100000','100','80','50','100','25','120','25');assert.ok(Math.abs(s.expected_pnl-5000)<1e-8);assert.equal(s.loss_probability_percent,25);assert.ok(Math.abs(s.outcomes[0].pnl-20000)<1e-8);
 s=api.scenario('1000','100','0','0','100','75','200','25');assert.equal(s.worst_entered_pnl,0);assert.equal(s.loss_probability_percent,0);assert.equal(s.expected_pnl,250);
 s=api.scenario('0','100','0','100','100','0','200','0');assert.equal(s.worst_entered_pnl,0);assert.equal(s.loss_probability_percent,0);
});
test('scenario rejects absent, impossible, nonfinite and incomplete distributions without normalization',()=>{
 for(const args of [['','100','80','25','100','50','120','25'],['1','0','80','25','100','50','120','25'],['1','100','-1','25','100','50','120','25'],['1','100','80','-1','100','76','120','25'],['1','100','80','25','100','40','120','25'],['Infinity','100','80','25','100','50','120','25'],['10000000000','0.00000001','10000000','100','0','0','0','0']])assert.throws(()=>api.scenario(...args));
 assert.throws(()=>api.scenario(1,100,80,25,100,50));
});
test('every input edit clears the previous probability and P&L result',()=>{
 const old=globalThis.document,form={elements:Object.fromEntries(Object.entries({exposure:'100000',price:'100',terminal1:'80',probability1:'25',terminal2:'100',probability2:'50',terminal3:'120',probability3:'25'}).map(([k,value])=>[k,{value}]))},out={textContent:''};globalThis.document={getElementById:id=>id==='implied-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/probability of a price loss 25%/);assert.match(out.textContent,/-\$20,000.00/);form.oninput();assert.match(out.textContent,/Assumptions changed/);form.elements.probability3.value='24';form.onsubmit({preventDefault(){}});assert.match(out.textContent,/must sum to 100%/);assert.doesNotMatch(out.textContent,/expected price P&L/);}finally{globalThis.document=old;}
});
test('native page, whole predecessor and fresh edge publications remain wired',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../implied-prob.html'),'utf8');assert.match(html,/jh-implied-research.js\?v=20260920-native1/);assert.match(html,/three-outcome distribution/);assert.doesNotMatch(html,/jh-enhance|ny_fed_12m_prob_pct|what the market is PRICING/);
 for(const m of html.matchAll(/<script[^>]+src="(\/[^"?]+)(?:\?[^\"]*)?"/g))assert.ok(fs.existsSync(path.join(__dirname,'..',m[1])));
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'implied-prob.json'/);assert.match(worker,/tail-research\|implied-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-implied-prob/source/legacy_implied_prob.py')).size,23950);
});
