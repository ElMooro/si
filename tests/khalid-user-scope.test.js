const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const ctx={};ctx.window=ctx;ctx.globalThis=ctx;vm.runInNewContext(fs.readFileSync('jh-khalid-sniper.js','utf8'),ctx);
const fixture=JSON.parse(fs.readFileSync('tests/fixtures/khalid-user-scope-synthetic.json','utf8'));
const now=Date.parse('2026-10-01T04:05:00Z'),fresh=()=>structuredClone(fixture),project=ctx.jhUserScopeEvidence;
test('scope preserves backend values, shared definitions and overlapping filter counts',()=>{
 const f=fresh(),before=JSON.stringify(f),p=project(f,now);assert.equal(p.valid,true);
 assert.equal(p.rows.length,12);assert.equal(JSON.stringify(p.rows[0].checks),JSON.stringify(f.user_scope_evidence.rows[0].checks));
 for(const [filter,n] of [['ALL',12],['BIOTECH',3],['SMALL',2],['UNKNOWN',2]])assert.equal(p.rows.filter(r=>ctx.jhUserScopeMatches(r,filter)).length,n);
 assert.equal(ctx.jhUserScopeMatches(null,'ALL'),true);assert.equal(ctx.jhUserScopeMatches(null,'BIOTECH'),false);
 assert.equal(JSON.stringify(f),before);assert.equal(p.packet.effective_at,null);assert.equal(p.packet.available_at,null);
 const mismatch=fresh();mismatch.opportunity_radar[0].sources=['katlin'];assert.equal(project(mismatch,now).valid,false);
 const original=ctx.jhSniperQualification(f,now);delete f.user_scope_evidence;assert.equal(JSON.stringify(ctx.jhSniperQualification(f,now)),JSON.stringify(original));
 assert.equal(project(f,now).valid,false);
});
test('scope fails closed on malformed contracts, values, references and clocks without withholding legacy',()=>{
 const edits=[p=>p.schema_version='v9',p=>p.qualification_revision='b'.repeat(64),p=>p.effective_at=p.generated_at,
 p=>p.sources=null,p=>p.sources[0]=null,p=>p.sources[0].producer='other',p=>p.sources[0].max_age_h=1e300,
 p=>p.sources[0].research_at=null,p=>p.sources[0].published_at='invalid',p=>p.sources[0].finviz_snapshot_at=null,
 p=>p.sources[0].finviz_snapshot_at='2026-10-02T00:00:00Z',p=>p.sources[0].census_snapshot_at=null,
 p=>p.sources[0].status='STALE',p=>p.rows.pop(),p=>p.rows[0].i=2,p=>p.rows[0].refs=[[9,'board/0']],
 p=>p.rows[0].refs=[[0,'picks/0']],p=>p.rows[0].refs=[[0,'board/0'],[0,'board/1']],p=>p.rows[0].refs=[],
 p=>p.rows[0].checks[0][1]='CRYPTO',p=>p.rows[0].refs=[[0,'etfs/0']],p=>p.definitions[1].source_labels=[],p=>p.rows[0].checks[1][1]='biotechnology',p=>p.rows[0].checks[1][1]='garbage',p=>p.rows[0].checks[1][1]=null,p=>p.rows[0].checks[2][1]=Infinity,
 p=>p.rows[0].checks[2][1]=2**53,p=>p.rows[0].checks[2][1]=true,p=>p.rows[0].checks[2][1]='500000000',
 p=>p.rows[0].checks[2][0]='UNAVAILABLE',p=>p.rows[0].checks[0][2]='missing-reason'];
 for(const edit of edits){const f=fresh();edit(f.user_scope_evidence);assert.equal(project(f,now).valid,false,String(edit));assert.equal(ctx.jhSniperQualification(f,now).valid,true,String(edit));}
 const f=fresh();f.user_scope_evidence.rows[0].checks[2][1]=10e9;
 assert.equal(project(f,now).valid,true,'browser does not recompute cap thresholds or change producer FAIL');
 assert.equal(project(f,now).rows[0].checks[2][0],'FAIL');
});
test('producer deadline equality is valid; expiry removes scope only',()=>{
 const f=fresh(),end=Date.parse(f.user_scope_evidence.sources[0].expires_at);
 assert.equal(project(f,end).valid,true);assert.equal(project(f,end+1).valid,false);
 const p=f.user_scope_evidence;p.sources[0].research_at='2026-09-27T16:05:00Z';p.sources[0].expires_at='2026-10-01T04:05:00Z';
 p.sources[0].finviz_snapshot_at=p.sources[0].census_snapshot_at=p.sources[0].research_at;
 assert.equal(project(f,now).valid,true);assert.equal(project(f,now+1).valid,false);assert.equal(ctx.jhSniperQualification(f,now+1).valid,true);
});
