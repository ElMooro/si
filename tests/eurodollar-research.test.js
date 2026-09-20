const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-eurodollar-research.js');
const f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/eurodollar-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('actual Python replay output verifies exact public bytes',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('edited current values, wrong run and foreign path are rejected',async()=>{
 const x=structuredClone(p);x.measurements.SOFR.value=999;await assert.rejects(api.verifyPacket(x,fetcher),/Current packet differs/);
 await assert.rejects(api.verifyPacket(p,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Run differs/);
 const y=structuredClone(p);y.replay.manifest_key='https://other.invalid/run.json';await assert.rejects(api.verifyPacket(y,fetcher),/Native research required/);
});
test('self-qualification and untyped feeds rejected',()=>{
 for(const key of ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[key]:true}),false);
 assert.equal(api.typed({contract:'legacy'}),false);
});
test('source clock cannot be renewed by recompilation',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+27*3600000),false);
 const x=structuredClone(p);x.generated_at=new Date(at+27*3600000).toISOString();assert.equal(api.current(x,at+27*3600000),false);
 assert.match(api.render(p,at+27*3600000),/0 \/ 9 within age ceilings/);
});
test('observation expiry separately withholds current comparison',()=>{
 const x=structuredClone(p);x.measurements.SOFR.source_valid_until='2026-09-01T00:00:00Z';x.repo_comparison.difference_bps=999;
 assert.equal(api.rowCurrent(x,x.measurements.SOFR,at),false);assert.doesNotMatch(api.render(x,at),/999 bp/);
});
test('nine source units and dates are shown without a composite gauge',()=>{
 const html=api.render(p,at);assert.match(html,/9 \/ 9 within age ceilings/);assert.match(html,/118.2126/);
 assert.match(html,/Index Jan 2006=100/);assert.match(html,/2026-09-11/);assert.match(html,/Percent/);
 assert.match(html,/multiply by 100 for basis points/);assert.match(html,/no vote/);assert.match(html,/scenario-form/);
 assert.doesNotMatch(html,/exit risk|CRITICAL|0-100/);
});
test('zero survives while absent measurements stay unavailable',()=>{
 const x=structuredClone(p);x.measurements.SOFR.exact_value='0';x.measurements.DFF.exact_value=null;
 const html=api.render(x,at);assert.match(html,/>0<\/td>/);assert.match(html,/>Unavailable<\/td>/);
});
test('text and links cannot inject markup',()=>{
 const x=structuredClone(p);x.measurements.SOFR.label='<img onerror=alert(1)>';x.measurements.SOFR.source_url='javascript:alert(1)';
 const html=api.render(x,at);assert.doesNotMatch(html,/<img|javascript:/);assert.match(html,/&lt;img/);
});
test('all three consumers use the verifier and Funding retains its explicit scenario',()=>{
 for(const name of ['eurodollar.html','macro-rooms.html','plumbing.html']){
  const s=fs.readFileSync(path.join(__dirname,'..',name),'utf8');assert.match(s,/id="eurodollar-context"/);assert.match(s,/jh-eurodollar-research.js\?v=20260920-native1/);
 }
 assert.match(fs.readFileSync(path.join(__dirname,'../eurodollar.html'),'utf8'),/id="scenario-form"/);
 const m=fs.readFileSync(path.join(__dirname,'../macro-rooms.html'),'utf8');assert.doesNotMatch(m,/ed.composite_score/);
 const q=fs.readFileSync(path.join(__dirname,'../plumbing.html'),'utf8');assert.match(q,/if\(f==='eurodollar-stress.json'\)/);
});
