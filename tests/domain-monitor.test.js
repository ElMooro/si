const test=require('node:test'),assert=require('node:assert/strict'),m=require('../jh-domain-monitor.js');
const now=Date.parse('2026-09-20T06:00:00Z');
test('old, missing, future and unreviewed publications cannot display current rule scores',()=>{
  for(const packet of [{},{generated_at:'2026-09-20T05:00:00Z'},{contract:m.CONTRACT,generated_at:'2026-09-11T12:20:00Z'},
    {contract:m.CONTRACT,generated_at:'2026-09-21T12:20:00Z'},{contract:m.CONTRACT,generated_at:'2026-09-20T05:00:00'}])assert.equal(m.describe(packet,now).renderRules,false);
  const out=m.describe({contract:m.CONTRACT,generated_at:'2026-09-20T05:00:00Z'},now);assert.equal(out.renderRules,true);assert.match(out.title,/unvalidated/);
});
test('negative and zero level comparisons never become ratio-based percentages',()=>{
  assert.deepEqual(m.comparison({value:-2,prev:-1,chg_pct:100}),{value:-1,unit:'source units unverified'});
  assert.deepEqual(m.comparison({value:2,prev:0,unit:'index points'}),{value:2,unit:'index points'});
  assert.deepEqual(m.comparison({value:2,prev:2,unit:'% YoY'}),{value:0,unit:'percentage points'});
});
test('invalid observations are unavailable and reported percent stays distinguished',()=>{
  for(const value of [null,true,'2',NaN,Infinity])assert.equal(m.comparison({value,prev:1,chg_pct:2}).value,null);
  assert.deepEqual(m.comparison({value:2,chg_pct:0}),{value:0,unit:'% (provider reported)'});
});
test('all catalog rows remain reachable through complete pagination',()=>{
  const rows=Array.from({length:10483},(_,i)=>i),seen=[];
  for(let n=0;n<53;n++)seen.push(...m.paginate(rows,n).rows);
  assert.deepEqual(seen,rows);assert.equal(m.paginate(rows,999).last,10483);
  assert.deepEqual(m.paginate([],4),{rows:[],page:0,pages:1,total:0,first:0,last:0});
});
