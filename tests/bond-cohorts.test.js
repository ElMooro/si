const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto').webcrypto;
const ui=require('../jh-bond-cohorts.js');
const hash=raw=>require('node:crypto').createHash('sha256').update(raw).digest('hex');
function fixture(){
 const files={},prefix='data/bond-desk-research/flows/';
 function put(value,kind){const raw=Buffer.from(JSON.stringify(value)),sha256=hash(raw),key=prefix+kind+'/'+sha256+'.json';files[key]=raw;return {key,sha256,bytes:raw.length};}
 const input={contract:'etf-original-research.v1',generated_at:'2026-09-26T15:00:00Z',replay:{manifest_key:'data/etf-research/runs/'+'c'.repeat(64)+'.json',output_sha256:'d'.repeat(64)}};
 const iref=put(input,'inputs'),proof={all_checks_passed:true};
 const win={observations:5,start_date:'2026-09-17',end_date:'2026-09-24',included_count:1,configured_count:2,coverage_subtotal_decimal:'0',complete_cohort_decimal:null,status:'partial_coverage_subtotal',members:[{ticker:'A',value_decimal:'0',start_date:'2026-09-17',end_date:'2026-09-24',acquired_at:'2026-09-26T14:00:00Z',history:{key:'data/etf-research/histories/'+'e'.repeat(64)+'.json'},original_evidence:{key:'javascript:alert(1)'}}],excluded:[{ticker:'B',reason:'missing'}]};
 const output={contract:'bond-flow-cohorts.v1',calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false,independent_votes:0,decision:{verb:'WAIT',meaning:'abstain'},evaluated_at:'2026-09-26T19:00:00Z',source:{original_replay_verified_here:false,sha256:iref.sha256,bytes:iref.bytes,generated_at:input.generated_at,replay:input.replay},upstream_binding:{issuer_originals_replayed_here:false},arithmetic_checks:proof,configured_funds:2,cohorts:{test:{windows:{'5_observations':win}}}};
 const run={contract:'bond-flow-replay.v1',evaluated_at:output.evaluated_at,input:iref,output:put(output,'outputs'),proof:put(proof,'proofs')},ref=put(run,'runs');
 const packet={...output,replay:{manifest_key:ref.key,output_sha256:run.output.sha256}};
 const requests=[];const fetcher=async url=>{requests.push(url);const key=url.slice(1).split('?')[0];return new Response(files[key]||'missing',{status:files[key]?200:404});};
 return {packet,run,files,fetcher,requests};
}
test('cohort browser verifies entire source/output/proof before displaying exact partial zero',async()=>{
 const f=fixture(),run=await ui.verify(f.packet,f.fetcher,crypto),html=ui.render(f.packet,run);
 assert.equal(f.requests.length,4);assert.ok(f.requests.every(p=>p.endsWith('?exact=1&nogen=1')));
 assert.match(html,/<td>0<\/td><td>Unavailable<\/td>/);assert.match(html,/1 \/ 2/);assert.match(html,/partial coverage subtotal/);
 assert.match(html,/does not rerun issuer workbooks/);assert.ok(!html.includes('href="javascript:'));
 assert.match(html,/Twenty observations are not relabeled twenty-one days/);
 f.packet.cohorts.test.windows['5_observations'].coverage_subtotal_decimal='1234567.123456';
 assert.match(ui.render(f.packet,run),/>1,234,567\.123456<\/td>/);
});
test('cohort body corruption, asserted authority and altered values clear all rendered results',async()=>{
 for(const mutation of ['source','output','value','authority']){
  const f=fixture();if(mutation==='source')f.files[f.run.input.key]=Buffer.from('{}');
  if(mutation==='output')f.files[f.run.output.key]=Buffer.from('{}');
  if(mutation==='value')f.packet=structuredClone(f.packet),f.packet.cohorts.test.windows['5_observations'].coverage_subtotal_decimal='10';
  if(mutation==='authority')f.packet.calls_eligible=true;
  const node={innerHTML:'stale values',set textContent(x){this.innerHTML='';this.text=x;}};
  await ui.mount(node,f.packet,f.fetcher,crypto);assert.equal(node.innerHTML,'');assert.match(node.text,/withheld/);
 }
});
test('complete cohort population and exclusion text remain inert with no presentation truncation',()=>{
 const f=fixture(),template=f.packet.cohorts.test;
 f.packet.cohorts=Object.fromEntries(Array.from({length:80},(_,i)=>['group_'+i,structuredClone(template)]));
 f.packet.cohorts.group_79.windows['5_observations'].excluded[0].reason='<img src=x onerror=alert(1)>';
 const html=ui.render(f.packet,f.run);assert.equal((html.match(/every configured fund/g)||[]).length,80);
 assert.ok(!html.includes('<img'));assert.match(html,/&lt;img/);assert.match(html,/group 79/);
});
