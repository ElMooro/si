const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const fixture=require('./fixtures/auction-score-completeness.json');
const source=fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8');
function context(name='complete'){
 const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',style:{},className:'',append(){},setAttribute(){}});return elements.get(id);};
 const scope={console,Date,Math,JSON,Number,String,Array,Object,Set,Map,Promise,fetch:()=>new Promise(()=>{}),setInterval:()=>{},setTimeout:()=>{},
  document:{getElementById:get,addEventListener:()=>{},querySelectorAll:()=>[],createElement:()=>({append(){},setAttribute(){}})},
  window:{addEventListener:()=>{}},localStorage:{getItem:()=>null},packet:JSON.parse(JSON.stringify(fixture.cases[name]))};
 scope.globalThis=scope;vm.createContext(scope);vm.runInContext(source,scope);
 const render=()=>{vm.runInContext('DATA=packet;renderAuctionsTable();renderIndicators()',scope);return get('auctions-tbl').innerHTML;};
 return {scope,get,render};
}
test('all retained actual native rows render beyond the legacy ten-row sample',()=>{
 const c=context('many'),html=c.render();assert.equal((html.match(/<tr>/g)||[]).length,28);
 assert.match(c.get('auction-population-count').textContent,/27 retained observations · full acquired population/);
 assert.equal((html.match(/auction-score-trace/g)||[]).length,27);
});
test('actual incomplete and unsupported observations stay visible with explanatory inputs',()=>{
 for(const name of ['missing','unsupported']){
  const c=context(name),html=c.render();assert.equal((html.match(/<tr>/g)||[]).length,3);
  assert.match(html,name==='missing'?/missing inputs/:/unsupported instrument/);
  assert.match(html,/required_features/);assert.match(html,/population_rule/);
  assert.match(html,/stress-no-data/);
 }
});
test('true measured zero and an incomplete zero-weight auction remain distinct',()=>{
 const c=context('zero_weight'),html=c.render();assert.match(html,/>0\.0<\/td>/);
 assert.match(html,/missing inputs/);assert.match(html,/stress-no-data/);
});
test('partial indicator coverage shows observed lower bounds rather than apparent calm',()=>{
 const c=context('missing');c.render();const html=c.get('indicator-grid').innerHTML;
 assert.match(html,/INCOMPLETE/);assert.match(html,/counts are lower bounds/);
 assert.match(html,/14d observed scores ≥ 50/);assert.match(html,/1 unavailable/);
});
test('older packets retain their visibly identified limited sample',()=>{
 const c=context();delete c.scope.packet.auction_observations;delete c.scope.packet.recent_auctions[0].score_quality;
 const html=c.render();assert.match(html,/Legacy score · completeness unverified/);
 assert.match(c.get('auction-population-count').textContent,/legacy recent sample only/);
});
test('score trace and source identities remain escaped and recover after empty refresh',()=>{
 const c=context();c.scope.packet.auction_observations[0].score_quality.note='<img src=x onerror=alert(1)>';
 let html=c.render();assert.doesNotMatch(html,/<img/);assert.match(html,/&lt;img/);
 c.scope.packet.auction_observations=[];html=c.render();assert.equal((html.match(/<tr>/g)||[]).length,1);
 c.scope.packet.auction_observations=fixture.cases.missing.auction_observations;assert.match(c.render(),/missing inputs/);
});
