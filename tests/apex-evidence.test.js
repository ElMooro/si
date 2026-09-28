const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const ui=require('../jh-apex-evidence.js');
const flags={calls_eligible:false,ranking_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function packet(){return {measurement_contract:'apex-context-evidence.v1',status:'research_only',call:'WAIT',call_semantics:'abstain',...flags,generated_at:'2026-09-28T01:00:00Z',sources:[]};}
function source(name,key){return {source:name,source_key:key,status:'received_object_unqualified',...flags,original_ref:{bytes:10000,sha256:'a'.repeat(64)},source_generated_at:'2026-09-28T00:00:00Z',source_clock_status:'reported_publication_clock'};}
test('legacy and forged eligibility cannot render conviction grades or ranks',()=>{
 for(const p of [null,{}, {top:[{ticker:'SYNA',apex_score:99,tier:'LIFTOFF'}]}, {...packet(),forecast_qualified:true}]){
  const o=ui.present(p);assert.match(o.headline,/WAIT \/ abstain/);assert.match(o.status,/legacy/);assert.doesNotMatch(o.rows,/SYNA|99|LIFTOFF/);assert.equal((o.rows.match(/<th scope="row">/g)||[]).length,9);
 }
});
test('exact duplicate payload identities are distinct from votes and missing rows',()=>{
 const p=packet();p.sources=[source('momentum','data/momentum-leaders.json'),source('positioning','data/pump-positioning.json')];
 const o=ui.present(p);assert.match(o.counts,/2 of 9/);assert.match(o.counts,/1 distinct/);assert.match(o.counts,/not independent votes/);assert.match(o.rows,/10000 bytes/);assert.match(o.rows,/Missing or conflicting source/);
});
test('duplicate source records, booleans and arbitrary URLs cannot claim coverage',()=>{
 const p=packet();const r=source('momentum','data/momentum-leaders.json');p.sources=[r,{...r}];assert.match(ui.present(p).counts,/0 of 9/);
 r.original_ref.bytes=true;p.sources=[r];assert.match(ui.present(p).counts,/0 of 9/);
 r.source_key='https://example.com/private';assert.match(ui.present(p).counts,/0 of 9/);
});
test('future clocks and untrusted labels stay explicit and inert',()=>{
 const p=packet();const r=source('momentum','data/momentum-leaders.json');r.source_clock_status='future';r.source_generated_at='<img src=x>';r.status='<script>bad</script>';p.sources=[r];
 const o=ui.present(p);assert.match(o.rows,/Future publication clock/);assert.match(o.rows,/Unrecognized status/);assert.doesNotMatch(o.rows,/<script|<img/);
});
test('render clears prior ranking text and preserves a keyboard-accessible source desk',()=>{
 const nodes=Object.fromEntries(['headline','publication','coverage','evidence-rows'].map(id=>[id,{textContent:'99 confidence',innerHTML:'99 confidence'}]));ui.render({getElementById:id=>nodes[id]},null);
 assert.match(nodes.headline.textContent,/WAIT/);assert.doesNotMatch(nodes['evidence-rows'].innerHTML,/99 confidence/);
 const html=fs.readFileSync(path.join(__dirname,'../apex.html'),'utf8');assert.match(html,/role="region" tabindex="0" aria-label="Apex source evidence table"/);
 assert.match(html,/apex-fusion.json\?exact=1&nogen=1/);assert.doesNotMatch(html,/jh-enhance.js|jh-page-ai.js|probabilistic conviction/i);
 assert.ok(fs.readFileSync(path.join(__dirname,'fixtures/pre-apex-evidence-page.html.txt')).length>7000);
});
