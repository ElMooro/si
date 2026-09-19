const test=require('node:test'),assert=require('node:assert/strict');
const {state,path}=require('../jh-ciss-readthrough.js');
const now=Date.parse('2026-09-19T00:00:00Z');
const q={contract:'ciss-readthrough.v1',status:'fresh',value:0,value_decimal:'0',source_replay:{manifest_key:'data/ciss-research/runs/example.json'},acquired_at:'2026-09-18T23:00:00Z',warehouse_generated_at:'2026-09-18T23:01:00Z',period_end:'2026-09-15',maximum_observation_age_days:14,maximum_acquisition_age_seconds:72*3600};
test('source expiry and missing data cannot render a calm or current context',()=>{
 const p={macro_stress:q,generated_at:'2026-09-18T23:02:00Z'};
 assert.equal(state(p,now).fresh,true);assert.equal(state(p,now+4*86400000).fresh,false);
 assert.equal(state({...p,macro_stress:{...q,value:null}},now).fresh,false);
 assert.equal(state({...p,macro_stress:{...q,acquired_at:'2026-09-20T00:00:00Z'}},now).fresh,false);
 assert.equal(state({macro_stress:{ciss_regime:null,ciss_composite:0}},now).fresh,false);
 assert.equal(state({...p,generated_at:'2026-08-01T00:00:00Z'},now).fresh,false);
});
test('source links are limited to exact public artifact paths',()=>{
 assert.equal(path('https://elsewhere/'),null);assert.equal(path('data/../../private'),null);
 assert.equal(path('data/a.json?redirect=elsewhere'),null);assert.equal(path('data/a.json'),'/data/a.json');
});
