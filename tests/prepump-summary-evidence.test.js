const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../classic-dashboard.html'),'utf8');
const start=html.indexOf('function renderPrePumpRadar(){'),end=html.indexOf('function _legacy_renderPrePumpRadar(){',start);assert.ok(start>=0&&end>start);const code=html.slice(start,end);
const ids=['ppConviction','ppTemperature','ppAGrade','ppPumpConf','ppClusters','ppEarly','ppSub','ppPicks'];
function render(packet){
 const nodes=Object.fromEntries(ids.map(id=>[id,{textContent:'old actionable',innerHTML:'old actionable',style:{},removeAttribute(){}}]));
 vm.runInNewContext(code+'\nrenderPrePumpRadar();',{STATE:{data:{ppSummary:packet}},document:{getElementById:id=>nodes[id]}});return nodes;
}
function packet(){return {measurement_contract:'prepump-summary-evidence.v1',status:'research_only',call:'WAIT',call_semantics:'abstain',calls_eligible:false,ranking_eligible:false,sizing_eligible:false,execution_eligible:false,sources:[]};}
test('legacy and missing summary are visibly unqualified and cannot restore old positions',()=>{
 for(const p of [null,{}, {top_picks:[{ticker:'BAD',position_pct:250}],conviction:'A'}, {...packet(),sizing_eligible:true}]){
  const n=render(p);assert.equal(n.ppConviction.textContent,'WAIT');assert.match(n.ppSub.textContent,/unavailable or legacy/);assert.match(n.ppPicks.innerHTML,/leader-observations.html/);assert.doesNotMatch(n.ppPicks.innerHTML,/BAD|250/);assert.equal(n.ppAGrade.textContent,'Unqualified');
 }
});
test('available JSON paths are counted separately from evidence and duplicate rows do not count',()=>{
 const p=packet();p.sources=[{source:'brief',source_key:'data/pump-radar-brief.json',status:'received_object_unqualified'},{source:'brief',source_key:'data/pump-radar-brief.json',status:'received_object_unqualified'},{source:'early',source_key:'data/velocity-acceleration.json',status:'received_object_unqualified'}];
 assert.equal(render(p).ppPumpConf.textContent,'1 / 5 declared inputs contain JSON objects; not votes');
});
test('missing numbers never become zero market, grades or catalyst confirmation',()=>{
 const n=render(packet());assert.equal(n.ppAGrade.textContent,'Unqualified');assert.match(n.ppTemperature.textContent,/no position instruction/);assert.match(n.ppEarly.textContent,/does not recommend holding/);assert.match(n.ppClusters.textContent,/No validated catalyst/);assert.doesNotMatch(n.ppTemperature.textContent,/0\/100|COOL/);
});
test('model/source narratives and malicious markup are never used in the card',()=>{
 const p=packet();p.executive_summary='<img src=x onerror=alert(1)>';p.suggested_additions=[{ticker:'BUY_THIS'}];p.top_picks=[{ticker:'BUY_THIS',position_pct:999}];
 const n=render(p);const all=JSON.stringify(n);assert.doesNotMatch(all,/<img|BUY_THIS|999/);
});
test('complete preceding page remains available and component has responsive columns',()=>{
 const old=fs.readFileSync(path.join(__dirname,'fixtures/pre-prepump-summary-evidence-classic-dashboard.html.txt'),'utf8');assert.ok(old.length>100000);assert.ok(old.includes('function renderPrePumpRadar(){'));
 const card=html.slice(html.indexOf('id="prePumpHero"'),html.indexOf('<!-- Retail Sentiment hero -->'));
 assert.match(card,/repeat\(auto-fit,minmax\(min\(100%,210px\),1fr\)\)/);assert.match(card,/Evidence status/);assert.doesNotMatch(card,/>LIVE<|A-grade Catalysts|Top Picks/);
 assert.match(card,/PRE-PUMP DESK/);assert.match(card,/white-space:nowrap/);assert.match(card,/flex-wrap:wrap/);
});
