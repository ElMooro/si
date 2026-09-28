const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const api=require('../jh-credit-research.js');
const p=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/credit-native.json'),'utf8')).packet;
const decimals=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/credit-comparison-decimals.json'),'utf8'));
const at=Date.parse(p.generated_at);
function checkChange(change,status){const x=structuredClone(p),c=x.comparisons.hy_minus_ig;change(x,c);const result=api.comparisonTrace(x,c);assert.equal(result.status,status);assert.equal(api.comparisonCurrent(x,c,at),false);return {x,c,result};}

test('all four comparisons expose both exact source terms and all source receipts without fetching originals',()=>{
 for(const c of Object.values(p.comparisons)){
  const before=JSON.stringify(p),trace=api.comparisonTrace(p,c),html=api.traceDetails(p,c);
  assert.equal(trace.status,'reproduced');assert.equal(trace.terms.length,2);assert.equal(trace.terms.flatMap(t=>t.evidence).length,4);
  for(const t of trace.terms){assert.equal(t.date,c.observation_date);assert.equal(typeof t.value_pct,'string');assert.equal(t.row_index,c[t.side+'_original_row_index']);assert.match(html,new RegExp(t.series_id));}
  assert.match(html,/zero-based/);assert.match(html,/has not read the protected provider originals/);
  assert.doesNotMatch(html,/audit-private|api\.stlouisfed|apikey|\[object Object\]/);assert.equal(JSON.stringify(p),before);
 }
 assert.equal((api.render(p,at).match(/Trace calculation:/g)||[]).length,4);
});

test('integer arithmetic agrees with Decimal for negative, zero, tie, tiny and twelve-place terms',()=>{
 assert.equal(decimals.cases.length,27);
 for(const example of decimals.cases){
  const x=structuredClone(p),c=x.comparisons.hy_minus_ig;
  for(const side of ['left','right']){const m=x.measurements[c[side+'_series_id']];m.exact.value_pct=example[side];m.value_pct=example[side+'_pct'];m.value_bps=example[side+'_bps'];}
  c.value_bps=example.value_bps;c.value_pp=example.value_pp;
  assert.equal(api.comparisonTrace(x,c).status,'reproduced',JSON.stringify(example));
 }
});

test('whole predecessor accepts an inconsistent comparison that the repaired boundary withholds',()=>{
 const context={module:{exports:{}},Date};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'fixtures/pre-credit-comparison-trace-jh-credit-research.js.txt'),'utf8'),context);
 const {x,c,result}=checkChange((x,c)=>c.value_bps+=1,'mismatch');
 assert.equal(context.module.exports.comparisonCurrent(x,c,at),true);
 assert.match(result.reason,/differs/);assert.ok(result.equation);
 for(const change of [(x,c)=>c.value_pp+=0.01,(x,c)=>c.value_bps=null,(x,c)=>c.value_pp=true])checkChange(change,'mismatch');
});

test('wrong identity, units, row, date and rounded or missing terms cannot borrow a calculation',()=>{
 for(const change of [(x,c)=>c.right_series_id=c.left_series_id,(x,c)=>c.id='made-up',
  (x,c)=>c.left_original_row_index=true,(x,c)=>c.left_original_row_index+=1,(x,c)=>c.observation_date='2026-02-30',
  (x,c)=>c.observation_date='2026-01-01',(x,c)=>c.unit='percent',(x,c)=>c.calls_eligible=true,
  (x,c)=>x.measurements[c.left_series_id].unit='basis_points',(x,c)=>x.measurements[c.left_series_id].value_bps+=1,
  (x,c)=>x.measurements[c.left_series_id].exact.value_pct=null,(x,c)=>x.measurements[c.left_series_id].exact.value_pct=0,
  (x,c)=>x.measurements[c.left_series_id].exact.value_pct='1e-3',(x,c)=>x.measurements[c.left_series_id].exact.value_pct='0.1234567890123',
  (x,c)=>x.measurements[c.left_series_id].value_pct=true])checkChange(change,'unavailable');
 const {result}=checkChange((x,c)=>x.measurements[c.left_series_id].observation_date='2026-01-01','unavailable');
 assert.equal(result.terms.length,2);assert.ok(result.terms.every(t=>t.value_pct===null));
});

test('duplicate, absent, malformed and future receipts never establish complete source references',()=>{
 for(const change of [(x,c)=>x.source_evidence.push(x.source_evidence.find(e=>e.series_id===c.left_series_id)),
  (x,c)=>x.source_evidence=x.source_evidence.filter(e=>e.series_id!==c.right_series_id),
  x=>x.source_evidence=null,x=>x.source_evidence={},
  (x,c)=>x.source_evidence.find(e=>e.series_id===c.left_series_id).bytes=true,
  (x,c)=>x.source_evidence.find(e=>e.series_id===c.left_series_id).sha256='not a hash',
  (x,c)=>x.source_evidence.find(e=>e.series_id===c.left_series_id).acquired_at='2099-01-01T00:00:00Z'])checkChange(change,'unavailable');
});

test('trace does not renew stale observations, change permissions or execute source labels',()=>{
 const c=p.comparisons.hy_minus_ig;
 assert.equal(api.comparisonTrace(p,c).status,'reproduced');assert.equal(api.comparisonCurrent(p,c,at+37*3600000),false);
 const x=structuredClone(p);x.measurements[c.left_series_id].meta.name='<img src=x onerror=alert(1)>';
 x.source_evidence.find(e=>e.series_id===c.left_series_id).sha256='<script>throw 1</script>';
 const html=api.traceDetails(x,c);assert.doesNotMatch(html,/<img|<script/);assert.match(html,/&lt;img/);
 for(const flag of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'])assert.equal(p[flag],false);
});
