const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-volatility-research.js');
const f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/volatility-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Python-generated volatility run and retained bytes verify exactly',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('altered values, run bytes and foreign paths fail verification',async()=>{
 const x=structuredClone(p);x.measurements.VIX_30D.value=999;await assert.rejects(api.verifyPacket(x,fetcher),/Current packet differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Run differs/);
 const y=structuredClone(p);y.replay.manifest_key='https://other.invalid/run.json';await assert.rejects(api.verifyPacket(y,fetcher),/Native research required/);
});
test('self-qualified investment packets are rejected',()=>{for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);});
test('pipeline age and observation age separately withhold current use',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+37*3600000),false);
 assert.equal(api.rowCurrent(p,{...p.measurements.VIX_30D,source_valid_until:'2026-09-01T00:00:00Z'},at),false);
 assert.match(api.render(p,at+37*3600000),/0 \/ 16 within the age ceiling/);
});
test('all sixteen inputs, actual dates, zero and null are preserved',()=>{
 const x=structuredClone(p);x.measurements.VIX_30D.value=null;x.measurements.SKEW.value=0;
 const html=api.table(x,at);for(const k of Object.keys(p.measurements))assert.ok(html.includes(k));
 assert.match(html,/>Unavailable</);assert.match(html,/>0</);assert.match(html,/2026-09-17/);assert.match(html,/Index points/);
});
test('statistics show actual windows and no implied crash probability',()=>{
 const html=api.detail(p,'VIX_30D');assert.match(html,/numeric provider observations, not assumed trading days/);
 assert.match(html,/sample rank, not a forecast probability/);assert.match(html,/Inspect retained source evidence/);
 const page=api.render(p,at);assert.match(page,/not a VIX futures curve/);assert.match(page,/not correlation or portfolio diversification/);
});
test('lagged comparisons are withheld from current display',()=>{
 const x=structuredClone(p);x.tenor_comparison.current_comparison_available=false;x.tenor_comparison.difference_points=999;
 assert.doesNotMatch(api.render(x,at),/999 points/);
});
test('data text and unapproved publisher URLs cannot inject markup',()=>{
 const x=structuredClone(p);x.measurements.VIX_30D.definition='<img onerror=alert(1)>';
 x.measurements.VIX_30D.source_url='javascript:alert(1)';
 assert.doesNotMatch(api.detail(x,'VIX_30D'),/<img|javascript:/);assert.match(api.detail(x,'VIX_30D'),/&lt;img/);
});
test('signed vega and explicit option-IV shock produce a unit-correct scenario',()=>{
 assert.match(api.scenario(1000,5),/>5,000/);assert.match(api.scenario(-1000,5),/>-5,000/);
 assert.match(api.scenario(1000,-5),/>-5,000/);assert.match(api.scenario(0,5),/>0 <small>USD/);
 assert.match(api.scenario(1000,5),/distinct from \+5% relative/);
 for(const args of [[NaN,5],[1e10,5],[1000,Infinity],[1000,101]])assert.match(api.scenario(...args),/role="alert"/);
});
test('both pages use verified native research and no old tail multiplier',()=>{
 const page=fs.readFileSync(path.join(__dirname,'../volatility.html'),'utf8');assert.match(page,/id="volatility-research"/);assert.match(page,/jh-volatility-research.js/);
 const intel=fs.readFileSync(path.join(__dirname,'../intelligence/index.html'),'utf8');
 const block=intel.slice(intel.indexOf('async function renderVolSurface'),intel.indexOf('// ─── MARKET INTERNALS'));
 assert.match(block,/await api.verifyPacket/);assert.doesNotMatch(block,/tail_mult|composite_stress_score/);
});

test('rendered punctuation preserves UTF-8 without mojibake',()=>{
 const html=api.render(p,at)+api.scenario(1000,5);
 assert.ok(html.includes('Research only · WAIT'));
 assert.ok(html.includes('×'));assert.doesNotMatch(html,/Â|â€|�/);
});
