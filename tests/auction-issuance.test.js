const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const fixture=require('./fixtures/auction-issuance.json');
const source=fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8');
function context(name='complete'){
 const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',style:{},className:'',append(){},setAttribute(){}});return elements.get(id);};
 class FixedDate extends Date {constructor(...args){super(...(args.length?args:['2026-09-29T12:00:00Z']));}static now(){return Date.parse('2026-09-29T12:00:00Z');}}
 const scope={console,Date:FixedDate,Math,JSON,Number,String,Array,Object,Set,Map,Promise,fetch:()=>new Promise(()=>{}),setInterval:()=>{},setTimeout:()=>{},
  document:{getElementById:get,addEventListener:()=>{},querySelectorAll:()=>[]},window:{addEventListener:()=>{}},localStorage:{getItem:()=>null},
  packet:{generated_at:'2026-09-29T12:00:00Z',issuance_anomaly:JSON.parse(JSON.stringify(fixture.cases[name]))}};
 scope.globalThis=scope;vm.createContext(scope);vm.runInContext(source,scope);
 const render=()=>{vm.runInContext('DATA=packet;renderIndicators()',scope);return get('indicator-grid').innerHTML;};
 return {scope,get,render};
}
test('bill comparison shows dates zero-day denominator units and entire replay ledger',()=>{
 const c=context('zero_day'),html=c.render();assert.match(html,/-86\.4%/);assert.match(html,/2 bill auction days/);
 assert.match(html,/2025-09-29–2026-09-29 inclusive/);assert.match(html,/USD bn/);assert.match(html,/not YoY/);assert.match(html,/&lt;0\.001 USD bn/);
 assert.match(html,/Inspect dates, amounts, exclusions and calculation/);assert.match(html,/source_ordinal/);
 assert.equal((html.match(/source_ordinal/g)||[]).length,3);
});
test('genuine zero comparison renders and missing or uncovered population does not become zero',()=>{
 assert.match(context('zero').render(),/>0\.0%/);
 for(const name of ['missing','unknown_coverage']){
  const html=context(name).render();assert.match(html,/UNAVAILABLE/);assert.doesNotMatch(html,/66\.7%|>0\.0%/);
  assert.match(html,/known_sum_usd/);assert.match(html,/&quot;score&quot;: null/);
 }
 assert.match(context('not_applicable').render(),/NOT APPLICABLE/);
});
test('old undated and malformed ledgers are withheld while complete dated input can recover',()=>{
 for(const mutate of [v=>delete v.contract,v=>v.ledger.row_count++,v=>v.calculation_as_of='2026-09-28',
  v=>v.baseline.source_slice=[0,900],v=>v.recent.start='2026-09-02',v=>v.calls_eligible=true,
  v=>v.baseline.population_coverage_verified=false,v=>v.score=true]){
  const c=context();mutate(c.scope.packet.issuance_anomaly);assert.match(c.render(),/Dated complete input ledger unavailable/);
  c.scope.packet.issuance_anomaly=fixture.cases.complete;assert.match(c.render(),/66\.7%/);
 }
});
test('all original hostile text is preserved only as inert escaped disclosure text',()=>{
 const html=context('hostile').render();assert.doesNotMatch(html,/<script>|<img/);
 assert.match(html,/&lt;script&gt;alert\(1\)&lt;\/script&gt;/);
});
test('combined history does not silently substitute a base value for a missing overlay',()=>{
 const c=context();c.scope.packet.composite_history={issuance_contract:'auction-issuance-inputs.v1',series:[
  {date:'2026-09-27',composite:40,with_issuance_composite:53.5},
  {date:'2026-09-28',composite:40,with_issuance_composite:null},
  {date:'2026-09-29',composite:40,with_issuance_composite:53.5}],with_issuance_change_points:[]};
 vm.runInContext('DATA=packet;renderCompositeChart()',c.scope);
 assert.doesNotMatch(c.get('composite-chart').innerHTML,/<polyline/);
 assert.match(c.get('composite-chart').innerHTML,/53\.5/);assert.doesNotMatch(c.get('composite-chart').innerHTML,/40\.0/);
 assert.match(c.get('composite-series-basis').textContent,/missing inputs break the line/);
});
test('hero explains missing supply separately from the available base score',()=>{
 const c=context('missing');Object.assign(c.scope.packet,{composite_score:null,base_composite_score:25,
  weighting_quality:{status:'complete'},composite_calculation:{issuance_status:'unavailable'}});
 vm.runInContext('DATA=packet;renderHero()',c.scope);
 assert.match(c.get('regime-desc').textContent,/Base auction score: 25/);
 assert.equal(c.get('regime-text').textContent,'UNAVAILABLE');
});
