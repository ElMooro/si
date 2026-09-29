const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const fixture=require('./fixtures/auction-reference-catalog.json');
const source=fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8');
function context(name='complete_zero'){
 const elements=new Map(),inspections=[],get=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',style:{},className:'',append(){},setAttribute(){}});return elements.get(id);};
 class FixedDate extends Date {constructor(...args){super(...(args.length?args:['2026-09-29T12:00:00Z']));}static now(){return Date.parse('2026-09-29T12:00:00Z');}}
 const scope={console,Date:FixedDate,Math,JSON,Number,String,Array,Object,Set,Map,Promise,fetch:()=>new Promise(()=>{}),setInterval:()=>{},setTimeout:()=>{},
  document:{getElementById:get,addEventListener:()=>{},querySelectorAll:()=>[],createElement:()=>({append(){},setAttribute(){}})},
  window:{addEventListener:()=>{}},localStorage:{getItem:()=>null},JHDataInspector:{inspect:(...args)=>inspections.push(args)},
  packet:{generated_at:'2026-09-29T12:00:00Z',historical_analog:JSON.parse(JSON.stringify(fixture.cases[name]))}};
 scope.globalThis=scope;vm.createContext(scope);vm.runInContext(source,scope);
 const render=()=>{vm.runInContext('DATA=packet;renderAnalogDataOnly();renderHistoricalReference()',scope);return get('hist-grid').innerHTML;};
 return {scope,get,render,inspections};
}
test('all ten actual catalog entries and original narratives remain inspectable without rankings',()=>{
 const c=context(),html=c.render();assert.equal((html.match(/<article/g)||[]).length,10);assert.equal(c.inspections.length,10);
 assert.match(c.get('analog-top').innerHTML,/Historical similarity unavailable/);assert.equal(c.get('analog-others').innerHTML,'');
 assert.doesNotMatch(html,/What happened next:|SIMILARITY|% similar/);
 assert.equal(c.inspections[0][1].legacy_narrative.what_next,fixture.cases.complete_zero.all_matches[0].legacy_narrative.what_next);
 assert.ok(c.inspections.every(entry=>entry[2]==='Unverified legacy entry · complete source fields'));
});
test('zero high and missing current vectors cannot change catalog order or create a top match',()=>{
 for(const name of ['complete_zero','complete_nonzero','missing']){
  const c=context(name);c.render();assert.equal(c.get('analog-others').innerHTML,'');
  assert.match(c.get('analog-top').innerHTML,/No match, probability or position size is supported/);
  assert.equal(c.inspections.length,10);
 }
});
test('duplicate dates retain distinct disclosure targets and every occurrence',()=>{
 const c=context('duplicate'),html=c.render();assert.equal((html.match(/<article/g)||[]).length,11);
 assert.equal(c.inspections.length,11);assert.match(html,/reference-input-11/);
 assert.equal(c.inspections[0][1].date,c.inspections[1][1].date);
 assert.notEqual(c.inspections[0][1].entry_id,c.inspections[1][1].entry_id);
});
test('unverified source text cannot inject markup or become a verified measurement',()=>{
 const c=context('invalid'),html=c.render();assert.doesNotMatch(html,/<img|<script/);assert.match(html,/&lt;img/);
 assert.match(html,/Low quote \(basis unverified\)/);assert.match(html,/UNVERIFIED LEGACY INPUT/);
});
test('older fabricated ranks and permission promotion cannot reactivate the comparison',()=>{
 for(const mutate of [c=>{delete c.contract;},c=>{c.top_matches=[{similarity:1,regime:'GFC_PEAK'}];},
   c=>{c.ranking_eligible=true;},c=>{c.calls_eligible=true;},c=>{c.all_matches[0].forecast_eligible=true;},
   c=>{c.all_matches[0].rank=1;},c=>{c.all_matches[0].similarity=0;},c=>{c.similarity_metric='cosine';}]){
  const c=context();mutate(c.scope.packet.historical_analog);const html=c.render();
  assert.doesNotMatch(html,/<article/);assert.equal(c.inspections.length,0);
  assert.match(c.get('analog-top').innerHTML,/Older similarity rankings are withheld/);
 }
});
test('missing or duplicated occurrences mismatched totals and outdated calculation dates fail whole',()=>{
 for(const mutate of [c=>c.reference_count--,c=>c.all_matches.pop(),c=>{c.all_matches[1].occurrence=1;},
   c=>{c.all_matches[0].entry_id='unknown';},c=>{c.calculation_as_of='2026-09-28';},
   c=>{c.missing_verification=[];}]){
  const c=context();mutate(c.scope.packet.historical_analog);assert.doesNotMatch(c.render(),/<article/);
 }
});
test('explicitly empty catalog recovers and a cached inspector without its method cannot crash',()=>{
 const c=context('empty');assert.match(c.render(),/catalog is explicitly empty/);
 c.scope.packet.historical_analog=fixture.cases.complete_zero;c.scope.JHDataInspector={};
 assert.doesNotThrow(()=>c.render());assert.match(c.get('hist-grid').innerHTML,/UNVERIFIED LEGACY INPUT/);
});
