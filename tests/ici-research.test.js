const test=require('node:test'),assert=require('node:assert/strict'),{webcrypto}=require('node:crypto');
const api=require('../jh-ici-research.js'),transport=require('../jh-fifx-research.js'),fixture=require('./fixtures/ici-native-public.json');
const copy=v=>JSON.parse(JSON.stringify(v));
function document(){const els={};return {els,getElementById(id){return els[id]??={value:id==='ici-kind'?'mmf':'',hidden:false,textContent:'',innerHTML:'',disabled:false,handlers:{},addEventListener(k,f){this.handlers[k]=f;}};}};}
function wire(p=fixture.packet,artifacts=fixture.artifacts){return async url=>{const u=new URL(url,'https://example.invalid');assert.equal(u.searchParams.get('exact'),'1');assert.equal(u.searchParams.get('nogen'),'1');const k=u.pathname.slice(1);assert.ok(!k.includes('audit-private/'));if(k==='data/ici-flows.json')return Response.json(p);if(!(k in artifacts))throw Error('Unexpected artifact');return new Response(artifacts[k]);};}

test('binds complete publication and independent proof without requesting protected originals',async()=>{
 const result=await api.verify(fixture.packet,transport,wire(),webcrypto);assert.equal(result.contract,'ici-replay.v1');
 const doc=document(),app=api.mount(doc,transport,wire(),webcrypto);await app.refresh();
 assert.equal(doc.els['ici-content'].hidden,false);assert.match(doc.els['ici-status'].textContent,/WAIT/);
 assert.match(doc.els['ici-count'].textContent,/36 of 36/);assert.match(doc.els['ici-source'].innerHTML,/USD billions/);
 assert.equal((doc.els['ici-observations'].innerHTML.match(/<tr>/g)||[]).length,37);
 assert.match(doc.els['ici-flow-summary'].innerHTML,/includes municipal/);assert.match(doc.els['ici-flow-summary'].innerHTML,/subset of bond/);
 doc.els['ici-kind'].value='combined_flows';doc.els['ici-kind'].handlers.change();
 assert.match(doc.els['ici-source'].innerHTML,/USD millions/);assert.match(doc.els['ici-count'].textContent,/45 of 45/);
 assert.equal((doc.els['ici-checks'].innerHTML.match(/<tr>/g)||[]).length,16);app.destroy();
});

test('rejects permission changes, incomplete sources, legacy packets and invalid units',()=>{
 for(const name of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified']){const p=copy(fixture.packet);p[name]=true;assert.throws(()=>api.valid(p));}
 for(const mutate of [p=>p.contract='legacy',p=>p.sources.mmf.observations.pop(),p=>p.sources.mmf.unit='usd_mn',p=>p.signal=1,p=>p.portfolio_consequences.target_weights={},p=>p.quality.reconciliation_issues=1]){const p=copy(fixture.packet);mutate(p);assert.throws(()=>api.valid(p));}
});

test('detects corruption of output, proof and manifest and blocks off-origin artifact injection',async()=>{
 const run=JSON.parse(fixture.artifacts[fixture.packet.replay.manifest_key]);
 for(const key of [run.output.key,run.proof.key,fixture.packet.replay.manifest_key]){const artifacts={...fixture.artifacts,[key]:fixture.artifacts[key]+' '};await assert.rejects(api.verify(fixture.packet,transport,wire(fixture.packet,artifacts),webcrypto));}
 const p=copy(fixture.packet);p.replay.manifest_key='https://other.invalid/secret.json';let calls=0;
 await assert.rejects(api.verify(p,transport,()=>{calls++;},webcrypto));assert.equal(calls,0);
});

test('failed refresh clears old measurements instead of repainting their timestamp',async()=>{
 const doc=document();let denied=false;const app=api.mount(doc,transport,u=>denied?Promise.resolve(new Response('Unavailable',{status:403})):wire()(u),webcrypto);
 await app.refresh();assert.equal(doc.els['ici-content'].hidden,false);denied=true;await app.refresh();
 assert.equal(doc.els['ici-content'].hidden,true);assert.equal(doc.els['ici-raw'].textContent,'');assert.equal(doc.els['ici-summary'].innerHTML,'');assert.match(doc.els['ici-status'].textContent,/withheld/);app.destroy();
});

test('source clocks remain separate and stale publications never become fresh observations',()=>{
 assert.match(api.age(fixture.packet,Date.parse('2026-10-10T00:00:00Z')),/does not certify observation freshness/);
 assert.match(api.age({generated_at:'invalid'}),/unavailable/);
 assert.match(api.age(fixture.packet,Date.parse('2026-09-01T00:00:00Z')),/Future/);
});
