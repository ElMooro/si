const test=require('node:test'), assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const root=path.join(__dirname,'..');
const ctx={};ctx.window=ctx;ctx.globalThis=ctx;
vm.runInNewContext(fs.readFileSync(path.join(root,'jh-khalid-sniper.js'),'utf8'),ctx);
const project=ctx.jhSniperQualification;
const source=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/khalid-qualification-synthetic.json'),'utf8'));
const NOW=Date.parse('2026-10-01T04:05:00Z');
const fresh=()=>structuredClone(source);
test('only final backend action qualifies; requested strategy remains unresolved',()=>{
 const p=project(fresh(),NOW);assert.equal(p.valid,true);
 assert.equal(p.rows[0].existing_backend_qualification.status,'PASS');
 assert.equal(p.rows[0].requested_strategy_qualification.complete,false);
 assert.equal(p.rows[0].requested_strategy_qualification.criteria.length,23);
 const f=fresh();f.sniper=true;f.scored={sniper:true};f.browserMeasurements={flows:'inflow',sniper:true};
 f.opportunity_radar[0].action='TRACKING';f.qualification_evidence.rows[0].existing_backend_qualification.reported_action='TRACKING';
 f.qualification_evidence.rows[0].existing_backend_qualification.status='FAIL';
 assert.equal(project(f,NOW).rows[0].existing_backend_qualification.status,'FAIL');
 assert.equal(project(f,NOW,'NOT_IN_BACKEND').valid,false);
});
test('missing stale future unknown contracts, revisions and identities fail closed',()=>{
 const mutations=[f=>delete f.qualification_evidence,f=>f.qualification_evidence.schema_version='v999',
 f=>f.generated_at='2026-09-30T04:00:00+00:00',f=>f.qualification_evidence.expires_at='2026-10-01T03:59:00Z',
 f=>f.qualification_evidence.rows[0].artifact_revision='f'.repeat(64),f=>f.qualification_evidence.rows[0].ticker='OTHER',
 f=>f.qualification_evidence.rows[0].existing_backend_qualification.schema_version='v999',
 f=>f.qualification_evidence.rows[0].requested_strategy_qualification.complete=true,
 f=>f.qualification_evidence.rows[0].existing_backend_qualification.criteria.pop(),
 f=>f.qualification_evidence.requested_criteria.pop(),f=>f.qualification_evidence.requested_criteria[0].id='unknown',
 f=>f.qualification_evidence.backend_definitions[0].definition_version='v999',
 f=>delete f.qualification_evidence.rows[0].existing_backend_qualification.clocks.effective_at,
 f=>f.qualification_evidence.rows[0].existing_backend_qualification.sources[0].status='STALE',
 f=>f.qualification_evidence.rows[0].existing_backend_qualification.sources.pop(),
 f=>f.qualification_evidence.rows[0].existing_backend_qualification.sources[0].as_of='invalid',
 f=>f.qualification_evidence.rows[0].existing_backend_qualification.criteria[0].status='FAIL',
 f=>f.qualification_evidence.rows[0]=null];
 for(const mutate of mutations){const f=fresh();mutate(f);assert.equal(project(f,NOW).valid,false,String(mutate));}
 assert.equal(project(fresh(),Date.parse('2026-10-02T06:00:01Z')).valid,false);
 assert.equal(project(fresh(),Date.parse('2026-10-01T03:59:59Z')).valid,false);
});
test('all criterion fields reach the expanded display model, no synthetic availability',()=>{
 const p=project(fresh(),NOW);const row=p.rows[0];
 for(const c of [...row.existing_backend_qualification.criteria,...row.requested_strategy_qualification.criteria]) {
  for(const k of ['id','label','definition','definition_version','applicability','status','value','unit','clocks','provenance'])assert.ok(Object.hasOwn(c,k),k);
  assert.equal(c.clocks.effective_at,null);assert.equal(c.clocks.available_at,null);
 }
 assert.match(row.requested_strategy_qualification.criteria.find(c=>c.id==='resilience').definition,/SPY is not silently substituted/);
});

test('backend criterion values reject wrong types and null/status contradictions',()=>{
 const gates=source.qualification_evidence.rows[0].existing_backend_qualification.criteria;
 const bools=new Set(['structure','dilution','risk_permission','trigger']);
 const counts=new Set(['legacy_flow','catalyst','vetoes']);
 for(const gate of gates){
  const invalid=bools.has(gate.id)?[null,'false','true',0,1,{},[],false]:[null,'unknown','0',true,false,{},[],NaN,Infinity,-Infinity];
  if(counts.has(gate.id))invalid.push(-1,.5);
  for(const value of invalid){const f=fresh();f.qualification_evidence.rows[0].existing_backend_qualification.criteria.find(c=>c.id===gate.id).value=value;
   assert.equal(project(f,NOW).valid,false,gate.id+' invalid PASS '+String(value));}
  for(const status of ['PASS','FAIL']){const f=fresh(),b=f.qualification_evidence.rows[0].existing_backend_qualification;
   b.status='FAIL';b.reported_action=f.opportunity_radar[0].action='TRACKING';
   const c=b.criteria.find(c=>c.id===gate.id);c.status=status;c.value=null;
   assert.equal(project(f,NOW).valid,false,gate.id+' '+status+' null');}
  const f=fresh(),b=f.qualification_evidence.rows[0].existing_backend_qualification;
  b.status='FAIL';b.reported_action=f.opportunity_radar[0].action='TRACKING';
  const c=b.criteria.find(c=>c.id===gate.id);c.status='UNAVAILABLE';c.value=null;
  assert.equal(project(f,NOW).valid,true,gate.id+' valid missing');
  c.value=gate.value;assert.equal(project(f,NOW).valid,false,gate.id+' unavailable non-null');
  c.status='FAIL';c.value=bools.has(gate.id)?false:gate.value;
  assert.equal(project(f,NOW).valid,true,gate.id+' valid typed FAIL');
  if(bools.has(gate.id)){c.value=true;assert.equal(project(f,NOW).valid,false,gate.id+' FAIL true');}
 }
 const f=fresh();f.qualification_evidence.rows[0].existing_backend_qualification.criteria[0].unit='boolean';
 assert.equal(project(f,NOW).valid,false,'criterion cannot override definition unit');
});
test('unresolved requested values stay null and numerical thresholds remain backend-owned',()=>{
 for(const c of source.qualification_evidence.requested_criteria){const f=fresh();f.qualification_evidence.requested_criteria.find(x=>x.id===c.id).value=0;assert.equal(project(f,NOW).valid,false,c.id);}
 const f=fresh(),g=f.qualification_evidence.rows[0].existing_backend_qualification.criteria;
 // Deliberately contradictory numerical evaluations prove this renderer does not
 // reimplement the 0.60 confidence, <=0 location or >=2.5 reward/risk thresholds.
 g.find(c=>c.id==='confidence').value=.5;g.find(c=>c.id==='location').value=3;g.find(c=>c.id==='reward_risk').value=1;
 assert.equal(project(f,NOW).valid,true);
});
test('source SLA equality remains valid, first later millisecond and publication boundary expire',()=>{
 const f=fresh();f.qualification_evidence.rows[0].existing_backend_qualification.sources.find(s=>s.name==='khalid_risk').max_age_h=2;
 const edge=Date.parse('2026-10-01T06:00:00Z');
 assert.equal(project(f,edge-1).valid,true);assert.equal(project(f,edge).valid,true);assert.equal(project(f,edge+1).valid,false);
 const publication=Date.parse(source.qualification_evidence.expires_at);
 assert.equal(project(fresh(),publication-1).valid,true);assert.equal(project(fresh(),publication).valid,false);
});
