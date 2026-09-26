const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto').webcrypto;
const ui=require('../jh-bond-credit.js'),sha=x=>require('node:crypto').createHash('sha256').update(x).digest('hex');
function fixture(){
 const files={},prefix='data/bond-desk-research/credit/';
 function put(value,kind){const raw=Buffer.from(JSON.stringify(value)),hash=sha(raw),key=prefix+kind+'/'+hash+'.json';files[key]=raw;return {key,sha256:hash,bytes:raw.length};}
 const input={contract:'credit-native-research.v1',generated_at:'2026-09-26T15:00:00Z',replay:{manifest_key:'data/credit-research/runs/'+'a'.repeat(64)+'.json'}};
 const iref=put(input,'inputs'),proof={checks_passed:true},out={contract:'bond-credit-comparisons.v1',evaluated_at:'2026-09-26T19:00:00Z',calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false,independent_votes:0,decision:{verb:'WAIT',meaning:'abstain'},upstream_binding:{credit_originals_replayed_here:false},source:{sha256:iref.sha256,bytes:iref.bytes,generated_at:input.generated_at,replay:input.replay},arithmetic_checks:proof,comparisons:{em_hy_minus_us_hy:{value_decimal:'2.00',observation_date:'2026-09-24',status:'dated_descriptive_comparison',reason:null,formula:'100 * (left - right)',components:[{series_id:'BAMLEMHBHYCRPIOAS',value_percent_decimal:'3.25',observation_date:'2026-09-24',original_row_index:794}]},missing:{value_decimal:null,observation_date:null,status:'unavailable',reason:'source_expired_or_future',components:[]}}};
 const run={contract:'bond-credit-replay.v1',evaluated_at:out.evaluated_at,input:iref,output:put(out,'outputs'),proof:put(proof,'proofs')},ref=put(run,'runs');
 const packet={...out,replay:{manifest_key:ref.key,output_sha256:run.output.sha256}};
 const requests=[],fetcher=async url=>{requests.push(url);const key=url.slice(1).split('?')[0];return new Response(files[key]||'missing',{status:files[key]?200:404});};
 return {packet,run,files,fetcher,requests};
}
test('credit UI binds the full source and shows explicit bps, dates, coordinates and missingness',async()=>{
 const f=fixture(),run=await ui.verify(f.packet,f.fetcher,crypto),html=ui.render(f.packet,run);
 assert.equal(f.requests.length,4);assert.ok(f.requests.every(k=>k.endsWith('?exact=1&nogen=1')));
 assert.match(html,/>2\.00<\/td><td>Basis points/);assert.match(html,/source expired or future/);assert.match(html,/794 \(zero-based\)/);
 assert.match(html,/original replay requires the authorized AWS runner/);assert.match(html,/Unavailable/);
});
test('credit UI clears prior values on source corruption, authority tampering or mismatched output',async()=>{
 for(const change of ['source','authority','output']){
  const f=fixture();if(change==='source')f.files[f.run.input.key]=Buffer.from('{}');
  if(change==='authority')f.packet.sizing_eligible=true;
  if(change==='output'){f.packet=structuredClone(f.packet);f.packet.comparisons.em_hy_minus_us_hy.value_decimal='200.00';}
  const node={innerHTML:'old',set textContent(v){this.innerHTML='';this.text=v;}};
  await ui.mount(node,f.packet,f.fetcher,crypto);assert.equal(node.innerHTML,'');assert.match(node.text,/withheld/);
 }
});
test('credit UI renders untrusted labels inertly and never links protected raw originals',()=>{
 const f=fixture();f.packet.comparisons.missing.reason='<img src=x onerror=alert(1)>';
 const c=f.packet.comparisons.em_hy_minus_us_hy.components[0];c.series_id='javascript:alert(1)';c.originals={observations:{key:'audit-private/secrets'}};
 const html=ui.render(f.packet,f.run);assert.ok(!html.includes('<img'));assert.ok(!html.includes('href="javascript:'));assert.ok(!html.includes('href="/audit-private/'));assert.match(html,/&lt;img/);
});
