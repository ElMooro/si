const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const ui=require('../jh-volume-observations.js');
const flags={calls_eligible:false,ranking_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function packet(){return {measurement_contract:'velocity-volume-observations.v1',status:'research_only',call:'WAIT',call_semantics:'abstain',...flags,generated_at:'2026-09-28T07:00:00Z',volume_observations:[{ticker:'SYNA',...flags,end_date:'2026-09-25',observation_age_calendar_days:3,history_original_ref:{bytes:1234,sha256:'a'.repeat(64)},measurements:[{name:'last_relative_volume',status:'measured',...flags,value:0,value_exact:'0',unit:'multiple_of_prior_baseline',start_date:'2026-08-20',end_date:'2026-09-25',definition:'Latest volume divided by the previous baseline',operands:[...Array.from({length:20},(_,i)=>({source_pointer:'/'+i+'/volume',source_index:i,observation_date:'2026-08-20',value_exact:'100'})),{source_pointer:'/26/volume',source_index:26,observation_date:'2026-09-25',value_exact:'0'}]}]}]};}
test('legacy missing and forged eligible data abstain without scores',()=>{
 for(const p of [null,{}, {fresh_fires:[{ticker:'FAKE',composite_score:99}]},{...packet(),sizing_eligible:true}]){
  const html=ui.render(p);assert.match(html,/WAIT \/ abstain/);assert.match(html,/unavailable, conflicting or legacy/);assert.doesNotMatch(html,/FAKE|99/);
 }
});
test('real zero has its units dates source identity and exact operand',()=>{
 const html=ui.render(packet());assert.match(html,/<td>0<\/td>/);assert.match(html,/1234 bytes/);assert.match(html,/a{64}/);
 assert.match(html,/2026-08-20 → 2026-09-25/);assert.match(html,/3 calendar days/);assert.match(html,/\/26\/volume/);
 assert.equal((html.match(/<th scope="row">/g)||[]).length,5);assert.match(html,/tabindex="0" role="region"/);
});
test('malformed units values and whole-source identities withhold measurements',()=>{
 for(const value of [true,null,'0',NaN,Infinity]){const p=packet();p.volume_observations[0].measurements[0].value=value;assert.doesNotMatch(ui.render(p),/<td>0<\/td>/);}
 const p=packet();p.volume_observations[0].history_original_ref.bytes=false;assert.doesNotMatch(ui.render(p),/<td>0<\/td>/);
 const q=packet();q.volume_observations[0].measurements[0].unit='USD';assert.doesNotMatch(ui.render(q),/<td>0<\/td>/);
});
test('duplicate issuer and metrics never add evidence',()=>{
 const p=packet();p.volume_observations.push({...p.volume_observations[0]});assert.match(ui.render(p),/No unambiguous/);
 const q=packet();q.volume_observations[0].measurements.push({...q.volume_observations[0].measurements[0]});assert.doesNotMatch(ui.render(q),/<td>0<\/td>/);
 assert.match(ui.render(q),/Missing or duplicate/);
});
test('missing duplicate or misbound operands and inconsistent exact values withhold numbers',()=>{
 for(const edit of [m=>m.operands.pop(),m=>m.operands[1]={...m.operands[0]},m=>m.operands[0].source_index=99,m=>m.value_exact='1']){
  const p=packet();edit(p.volume_observations[0].measurements[0]);assert.doesNotMatch(ui.render(p),/<td>0<\/td>/);
 }
});
test('source labels and operand fields cannot inject markup or private context',()=>{
 const p=packet();p.generated_at='<img src=x>';p.volume_observations[0].measurements[0].definition='<script>bad</script>';
 p.volume_observations[0].measurements[0].operands[0].private_note='private-pending-content';
 const html=ui.render(p);assert.doesNotMatch(html,/<img|<script|private-pending-content/);assert.match(html,/&lt;script/);
 assert.match(html,/not independent votes/);assert.match(html,/without promotion or expiration/);
});
test('current page entrypoint replaces legacy claims and clears data before failed refresh',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../pre-pump-radar.html'),'utf8'),nodes={'early-panel':{style:{}},'early-content':{innerHTML:'Confirmed trade'}};
 const start=html.indexOf('function renderEarlyDetection(){'),end=html.indexOf('function _legacy_renderEarlyDetection(){');
 const scope={EARLY:null,JHVolumeObservations:ui,document:{getElementById:id=>nodes[id]}};
 vm.runInNewContext(html.slice(start,end)+'renderEarlyDetection();',scope);
 assert.equal(nodes['early-panel'].style.display,'block');assert.doesNotMatch(nodes['early-content'].innerHTML,/Confirmed trade/);
 assert.match(html,/async function load\(\)\{\s+EARLY = null;\s+renderEarlyDetection\(\);/);
 assert.match(html,/fetch\(EARLY_URL \+ "\?exact=1&nogen=1&t="/);assert.match(html,/src="\/jh-volume-observations.js/);
 assert.doesNotMatch(html,/volume acceleration on names already in active themes — 1-3 days/);
});
