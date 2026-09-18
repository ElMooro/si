const test=require('node:test');
const assert=require('node:assert/strict');
const {view}=require('../jh-prospective-research.js');
const now=Date.parse('2026-09-18T20:00:00Z');
function packet(){return {schema_version:'prospective-research-summary.v1',generated_at:'2026-09-18T19:00:00Z',
  capture:{key:'data/research-forecasts/captures/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64)},
  protocol:{key:'data/research-forecasts/protocols/'+'b'.repeat(64)+'.json',sha256:'b'.repeat(64)},
  records_in_capture:0,new_records:0,rank_observations:12,ineligible_sources:3,unsupported_identity_count:2,
  coverage:{candidate_scan_complete:true},sizing_eligible:false,promotion_eligible:false};}
test('zero captured forecasts stays zero without implying a performance result',()=>{
  const v=view(packet(),now);assert.equal(v.ok,true);assert.equal(v.records,0);assert.equal(v.ranks,12);
});
test('stale counts, authority changes and arbitrary links cannot appear valid',()=>{
  for(const edit of [p=>p.generated_at='2026-09-16T00:00:00Z',p=>p.sizing_eligible=true,
    p=>p.capture.key='https://example.com',p=>p.new_records=1,p=>p.records_in_capture=null]){
    const p=packet();edit(p);assert.equal(view(p,now).ok,false);
  }
});
test('partial capture stays partial',()=>{
 const p=packet();p.coverage.candidate_scan_complete=false;assert.equal(view(p,now).complete,false);
});
