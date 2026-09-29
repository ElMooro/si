const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const fixture=require('./fixtures/auction-concerns.json');
const source=fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8');
const clone=v=>JSON.parse(JSON.stringify(v));
function context(name='complete'){
 const data=fixture.cases[name],elements=new Map(),inspections=[];
 const get=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',style:{},className:'',append(){},setAttribute(){}});return elements.get(id);};
 class Fixed extends Date{static now(){return Date.parse(data.generated_at);}}
 const scope={console,Date:Fixed,Math,JSON,Number,String,Array,Object,Set,Map,Promise,fetch:()=>new Promise(()=>{}),setInterval:()=>{},setTimeout:()=>{},
  document:{getElementById:get,addEventListener:()=>{},querySelectorAll:()=>[],createElement:()=>({append(){},setAttribute(){}})},
  window:{addEventListener:()=>{}},localStorage:{getItem:()=>null},
  JHDataInspector:{inspect:(...args)=>inspections.push(args)},packet:{generated_at:data.generated_at,tail_risk:clone(data.tail_risk)}};
 scope.globalThis=scope;vm.createContext(scope);vm.runInContext(source,scope);
 const render=()=>{vm.runInContext('DATA=packet;renderTailRiskDataOnly()',scope);return get('tail-grid').innerHTML;};
 return {scope,get,render,inspections};
}
test('actual compiler fixtures show qualified arithmetic and withheld historical anchor',()=>{
 const c=context(),html=c.render();assert.match(html,/>45<span class="unit">\/100/);
 assert.match(html,/historical anchor comparability unverified/);assert.match(html,/—<span class="unit">\/100/);
 assert.equal(c.inspections.length,3);assert.ok(c.inspections.every(x=>x[2]==='Complete concern inputs and arithmetic'));
 assert.equal((html.match(/<details class="concern-trace">/g)||[]).length,3);assert.doesNotMatch(html,/<details[^>]*open/);
 assert.equal(c.inspections[0][1].input_trace.momentum.endpoint_windows.length,2);
});
test('all six actual native-compiler cases render with their complete input traces',()=>{
 for(const name of Object.keys(fixture.cases)){
  const c=context(name),html=c.render();assert.equal((html.match(/class="tail-card"/g)||[]).length,3);assert.equal(c.inspections.length,3);
  if(['missing','sparse','invalid'].includes(name))assert.equal((html.match(/—<span class="unit">\/100/g)||[]).length,3);
 }
});
test('zero measurements and fixed formula intercept remain distinct from missing values',()=>{
 const c=context('zero'),html=c.render();assert.match(html,/>5<span class="unit">\/100/);
 assert.match(html,/Seven-day score change: <b>0\.0<\/b>/);assert.match(html,/Dealer threshold met: <b>No<\/b>/);
 const missing=context('missing').render();assert.match(missing,/Unavailable/);assert.doesNotMatch(missing,/\[object Object\]|<b>null<\/b>|<b>undefined<\/b>/);
});
test('legacy unqualified score cannot regain authority through the old alias',()=>{
 const c=context(),soft=c.scope.packet.tail_risk.p_soft_demand_30d;
 delete c.scope.packet.tail_risk.p_soft_demand_30d;delete c.scope.packet.tail_risk.p_failed_auction_30d.measurement_contract;
 const html=c.render();assert.doesNotMatch(html,/>45<span/);assert.match(html,/complete-input concern calculation unavailable/);
 c.scope.packet.tail_risk.p_failed_auction_30d=soft;assert.match(c.render(),/>45<span/);
});
test('forecast permissions malformed scores and missing qualification withhold the number',()=>{
 for(const change of [{calls_eligible:true},{sizing_eligible:true},{forecast_eligible:true},{execution_eligible:true},
                     {probability:0},{calibrated:true},{heuristic_score:true},{heuristic_score:'45'},{status:'unavailable'},
                     {missing_inputs:['missing']},{calculation_as_of:'2026-09-28'}]){
  const c=context();Object.assign(c.scope.packet.tail_risk.p_soft_demand_30d,change);assert.doesNotMatch(c.render(),/>45<span/);
 }
});
test('stale future undated and rolled-over publication dates never display scored arithmetic',()=>{
 for(const stamp of [null,'2020-01-01T00:00:00Z','2999-01-01T00:00:00Z','2026-02-31T12:00:00Z','2026-09-29']){
  const c=context();c.scope.packet.generated_at=stamp;const html=c.render();assert.equal((html.match(/—<span class="unit">\/100/g)||[]).length,3);
  assert.match(html,/stale or undated/);
 }
});
test('missing publication can recover and source text remains escaped',()=>{
 const c=context('missing');c.render();c.scope.packet.tail_risk=clone(fixture.cases.complete.tail_risk);
 c.scope.packet.tail_risk.p_soft_demand_30d.drivers.context='<img src=x onerror=alert(1)>';
 const html=c.render();assert.match(html,/>45<span/);assert.doesNotMatch(html,/<img/);assert.match(html,/&lt;img/);
 assert.doesNotMatch(html,/\[object Object\]/);
});
