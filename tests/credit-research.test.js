const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const api=require('../jh-credit-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/credit-native.json'),'utf8'));
const p=fixture.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=fixture.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};

test('retained Python-generated packet and run verify exactly',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('a rewritten current number fails the retained output check',async()=>{const x=structuredClone(p);x.measurements.BAMLH0A0HYM2.value_bps=999;await assert.rejects(api.verifyPacket(x,fetcher),/Current packet differs/);});
test('tampered run bytes and foreign paths are rejected',async()=>{
 await assert.rejects(api.verifyPacket(p,async key=>{const r=await fetcher(key);return {...r,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer};}),/Run differs/);
 const x=structuredClone(p);x.replay.manifest_key='https://other.invalid/data.json';await assert.rejects(api.verifyPacket(x,fetcher),/Native research required/);
});
test('an unqualified packet cannot self-authorize a trade',()=>{for(const flag of ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible']){const x={...p,[flag]:true};assert.equal(api.typed(x),false);}});
test('pipeline age and observation age are separate checks',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+37*3600000),false);
 const m={...p.measurements.BAMLH0A0HYM2,observation_date:'2026-09-01',source_valid_until:'2026-09-07T00:00:00Z'};
 assert.equal(api.rowCurrent(p,m,at),false);
});
test('basis-point and percent views use the correct fields',()=>{
 const x=structuredClone(p);x.measurements.BAMLH0A0HYM2.value_bps=270;x.measurements.BAMLH0A0HYM2.value_pct=2.7;
 assert.match(api.table(x,'bp',at),/>270 bp</);assert.match(api.table(x,'pct',at),/>2.7 %</);
 assert.equal(api.table(x,'invented'), '');
});
test('missing observations remain unavailable and zero remains visible',()=>{
 const x=structuredClone(p);x.measurements.BAMLH0A0HYM2.value_bps=null;x.measurements.BAMLC0A0CM.value_bps=0;
 const html=api.table(x,'bp',at);assert.match(html,/>Unavailable bp</);assert.match(html,/>0 bp</);
});
test('source definitions and actual history windows are exposed',()=>{
 const html=api.detail(p,'BAMLH0A0HYM2');assert.match(html,/numeric provider observations, not assumed trading days/);assert.match(html,/sample rank, not a probability forecast/);assert.match(html,/Inspect retained source evidence/);
 assert.match(api.render(p,at),/Mixed investment-grade and below-investment-grade/);assert.match(api.render(p,at),/28 source identities/);
});
test('text from data is escaped rather than executed',()=>{
 const x=structuredClone(p);x.measurements.BAMLH0A0HYM2.definition='<img src=x onerror=alert(1)>';
 const html=api.detail(x,'BAMLH0A0HYM2');assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
});
test('long and short spread-only P&L use signed exposure',()=>{
 assert.match(api.scenario(1000000,5,100),/-50,000/);assert.match(api.scenario(1000000,5,100),/-5%/);
 assert.match(api.scenario(-1000000,5,100),/>50,000/);
 assert.match(api.scenario(0,5,100),/>0 <small>USD/);assert.match(api.scenario(1000000,0,100),/>0 <small>USD/);
});
test('invalid or out-of-range scenario inputs do not produce an estimate',()=>{
 for(const args of [[NaN,5,100],[1e13,5,100],[100,NaN,100],[100,-1,100],[100,5,Infinity],[100,5,10001]])assert.match(api.scenario(...args),/role="alert"/);
 assert.match(api.scenario(1e6,20,500),/full repricing/);
});
test('credit page wires the native renderer and contains no legacy auto-call script',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../credit/index.html'),'utf8');assert.match(html,/id="credit-research"/);assert.match(html,/jh-credit-research.js\?v=20260920-native1/);assert.doesNotMatch(html,/echarts|function renderRegime/);
});
