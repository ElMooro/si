'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {current,rows,path}=require('../jh-ciss-desk-evidence.js');
const now=Date.parse('2026-09-19T00:00:00Z');
const q={status:'fresh',source_acquired_at:'2026-09-18T00:00:00Z',warehouse_generated_at:'2026-09-18T01:00:00Z',period_end:'2026-09-15',maximum_acquisition_age_seconds:259200,maximum_observation_age_days:14};
test('Consumer evidence expires by its source clock even when desk just regenerated',()=>{
  assert.equal(current(q,now),true);
  assert.equal(current({...q,source_acquired_at:'2026-09-14T00:00:00Z'},now),false);
  assert.equal(current({...q,status:'missing'},now),false);
  assert.equal(current({...q,period_end:'2026-09-21'},now),false);
  assert.equal(current({...q,warehouse_generated_at:null},now),false);
});
test('Sovereign and fragmentation display exact decimal identity and separate families',()=>{
  const a=rows({systemic_stress_ciss:{euro_area:{level_decimal:'0.0123456789',source_key:'CISS.X',quality:q}},sovereign_stress_sovciss:{france:{level_decimal:'0.02',source_key:'CISS.Y',quality:q}}});
  assert.equal(a.length,2);assert.equal(a[0].value,'0.0123456789');assert.equal(a[1].name,'france · SovCISS');
  const b=rows({countries:{FR:{name:'France',sovciss_decimal:'0',sovciss_source_key:'CISS.Y',sovciss_quality:q}}});
  assert.equal(b[0].value,'0');assert.equal(b[0].key,'CISS.Y');
  assert.equal(path('data/../../private.json'),null);
});
