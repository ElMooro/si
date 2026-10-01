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
