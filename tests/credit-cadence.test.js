const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path');
const api=require('../jh-credit-research.js');
const examples=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/credit-collection-policy.json'),'utf8'));
const original=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/credit-native.json'),'utf8')).packet;

test('browser calendar agrees field-for-field with reviewed Python across all boundary cases',()=>{
 for(const example of examples.cases)assert.deepEqual(api.collectionPolicy(example.generated_at),example.policy);
});

function proposed(){
 const p=structuredClone(original);p.generated_at='2026-09-25T22:11:08.786018+00:00';p.version='2.1.0';
 const policy=api.collectionPolicy(p.generated_at);
 p.freshness={...p.freshness,pipeline_check_due_at:policy.pipeline_check_due_at,valid_until:policy.pipeline_check_due_at,collection_policy:policy};
 for(const row of Object.values(p.measurements)){row.observation_date='2026-09-24';row.source_valid_until='2026-09-30T00:00:00Z';}
 for(const c of Object.values(p.comparisons))c.observation_date=c.left_latest_date=c.right_latest_date='2026-09-24';
 return p;
}

test('new policy spans an unscheduled weekend but expiry and observation limits remain exclusive',()=>{
 const p=proposed(),at=Date.parse('2026-09-28T17:00:00Z');
 assert.equal(api.current(p,at),true);assert.equal(api.comparisonCurrent(p,p.comparisons.hy_minus_ig,at),true);
 const cutoff=Date.parse(p.freshness.pipeline_check_due_at);
 assert.equal(api.current(p,cutoff-1),true);assert.equal(api.current(p,cutoff),false);
 p.measurements.BAMLH0A0HYM2.source_valid_until='2026-09-28T16:00:00Z';
 assert.equal(api.comparisonCurrent(p,p.comparisons.hy_minus_ig,at),false);
});

test('forged, incomplete or mixed policy declarations never grant the longer clock',()=>{
 const p=proposed(),now=Date.parse('2026-09-28T17:00:00Z');
 for(const change of [x=>delete x.freshness.collection_policy,x=>x.version='2.0.0',
  x=>x.freshness.collection_policy=null,x=>x.freshness.collection_policy.weekdays=[false,true,2,3,4],
  x=>x.freshness.collection_policy.completion_allowance_seconds=3600,x=>x.freshness.pipeline_check_due_at='2026-09-30T00:00:00Z',
  x=>x.freshness.collection_policy.successful_collection_asserted=true]){
  const x=structuredClone(p);change(x);assert.equal(api.collectionState(x,now),'invalid');assert.equal(api.current(x,now),false);
 }
 const old={...p,version:'2.0.0',freshness:{pipeline_check_due_at:'2026-09-27T10:11:08.786018+00:00'}};
 assert.equal(api.current(old,now),false,'The old published packet keeps its old expiry');
});

test('missing Friday late collection cannot be concealed by a Monday deadline',()=>{
 const p=proposed();p.generated_at='2026-09-25T20:01:00Z';p.freshness.collection_policy=api.collectionPolicy(p.generated_at);
 p.freshness.pipeline_check_due_at=p.freshness.collection_policy.pipeline_check_due_at;
 assert.equal(p.freshness.pipeline_check_due_at,'2026-09-25T22:15:00+00:00');
 assert.equal(api.current(p,Date.parse('2026-09-28T17:00:00Z')),false);
});
