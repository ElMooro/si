const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const api=require('../jh-capital-boundary.js');
const revised={generated_at:'2026-09-20T04:00:00Z',capital_flow_exclusion:{basis:'capital-flow-stock-score-retired.v1'}};

test('absence and old revisions never claim the CapitalFlow exclusion',()=>{
  for(const p of [null,{}, {calls_eligible:false},{capital_flow_exclusion:{basis:'wrong'}}]){
    assert.equal(api.describe(p).revised,false); assert.match(api.describe(p).text,/historical calculations/);
    assert.deepEqual(api.narrativeRows(p),[]);
  }
});
test('revised output identifies its time and limited authority',()=>{
  const s=api.describe(revised); assert.equal(s.revised,true);assert.equal(s.asof,revised.generated_at);
  assert.match(s.text,/does not establish independent evidence/);assert.match(s.text,/position sizes/);
});
test('legacy trade narratives cannot reappear under a revised envelope',()=>{
  const old={ticker:'KO',tape:'institutions accumulating'};
  const research={ticker:'KO',tape:'Upstream dislocation screen: cheap & inflecting'};
  assert.deepEqual(api.narrativeRows({...revised,quiet_accumulation:[null,old,research]}),[research]);
  assert.deepEqual(api.narrativeRows({quiet_accumulation:[research]}),[]);
});
test('public consumers load the boundary and retire contradictory wording',()=>{
  for(const name of ['flow-confluence','deep-value-overlap','equity-confluence','industry-rotation','cockpit']){
    const text=fs.readFileSync(path.join(__dirname,'..',name+'.html'),'utf8');
    assert.match(text,/src="\/jh-capital-boundary.js"/);assert.match(text,/id="capital-boundary"/);
    assert.match(text,/JHCapitalBoundary.render/);
    assert.doesNotMatch(text,/smart money is buying|≥2 flow engines agree|capital-flow accumulation|smart_money\.capital_flow/);
  }
});
