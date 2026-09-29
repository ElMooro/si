const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const fixture=require('./fixtures/auction-participation.json');
const html=fs.readFileSync(path.join(__dirname,'../auctions.html'),'utf8');
const helpers=html.slice(html.indexOf('  function participationGrade(a) {'),html.indexOf('  function renderBanner(D) {'));
const modal=html.slice(html.indexOf('  function openGradeExplainer(a) {'),html.indexOf('  function wireExplainer(D) {'));
const esc=value=>String(value??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
function context(){
 const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',classList:{add(){}},focus(){}});return elements.get(id);};
 const scope={$:get,esc,bn:String,securityLabel:a=>a.instrument_kind,document:{activeElement:null}};vm.createContext(scope);vm.runInContext(helpers+modal,scope);
 return {scope,get};
}
test('complete current native grade exposes all four prior input observations and fixed denominator',()=>{
 const c=context(),row=fixture.cases.complete_zero.today.auctions[0];c.scope.openGradeExplainer(row);
 assert.match(c.get('gx-title').innerHTML,/>C</);assert.match(c.get('gx-sub').textContent,/score 0\.00/);
 const body=c.get('gx-body').innerHTML;assert.match(body,/fixed denominator of three/);
 assert.match(body,/Inspect every comparison observation and calculation/);
 for(const day of ['2026-09-25','2026-09-26','2026-09-27','2026-09-28'])assert.match(body,new RegExp(day));
 assert.match(body,/population_standard_deviation/);assert.match(body,/reported_competitive_accepted_usd/);
});
test('missing current history zero variance and unknown cohort never render a letter or numeric grade',()=>{
 for(const name of ['missing','history_gap','constant','unknown']){
  const c=context(),row=fixture.cases[name].today.auctions[0];c.scope.openGradeExplainer(row);
  assert.match(c.get('gx-title').innerHTML,/>n\/a</);assert.match(c.get('gx-sub').textContent,/score n\/a/);
  assert.match(c.get('gx-body').innerHTML,/&quot;status&quot;: &quot;unavailable&quot;/);
 }
});
test('old or corrupted grade cannot inherit the new completeness declaration',()=>{
 const c=context(),row=structuredClone(fixture.cases.strong.today.auctions[0]);assert.equal(c.scope.participationGrade(row),'A');
 for(const mutate of [r=>delete r.grading_inputs,r=>r.grading_inputs.features.btc.status='unavailable',
   r=>{r.grading_inputs.status='unavailable';r.grading_contract='auction-participation-inputs.v1';r.grading_status='complete';},r=>r.grading_inputs.denominator=2,r=>r.grading_inputs.score=true,r=>r.grading_inputs.grade='B']){
  const bad=structuredClone(row);mutate(bad);assert.equal(c.scope.participationGrade(bad),'n/a');assert.equal(c.scope.participationScore(bad),null);
 }
 assert.equal(c.scope.participationGrade({grade:'A',grading_contract:'auction-participation-inputs.v1',grading_status:'unavailable'}),'n/a');
});
test('hostile retained source fields cannot create active markup in the full grade disclosure',()=>{
 const c=context();c.scope.openGradeExplainer(fixture.cases.hostile.today.auctions[0]);
 assert.doesNotMatch(c.get('gx-body').innerHTML,/<img|<script/);assert.match(c.get('gx-body').innerHTML,/&lt;img/);
});
test('native page sample limit is explicit and missing BTC remains in the packet',()=>{
 const p=fixture.cases.missing;assert.equal(p.auction_population.btc_filter_applied,false);
 assert.equal(p.auction_population.analyzed_settled_rows,5);assert.equal(p.auctions.length,5);
 assert.equal(p.today.auctions[0].btc,null);assert.equal(p.reactions.today_classes[0],'unclassified');
 assert.equal(p.reactions.price_lineage_verified,false);
});
