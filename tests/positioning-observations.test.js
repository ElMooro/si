const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const ui=require('../jh-positioning-observations.js');
const flags={calls_eligible:false,ranking_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function packet(){return {measurement_contract:'positioning-price-observations.v1',status:'research_only',call:'WAIT',call_semantics:'abstain',...flags,generated_at:'2026-09-28T01:00:00Z',price_observations:[{ticker:'SYNA',...flags,history_original_ref:{bytes:12000,sha256:'a'.repeat(64)},measurements:[{name:'price_change_5',status:'measured',...flags,value:0,unit:'percent_price_change',start_date:'2026-09-18',end_date:'2026-09-25',definition:'Price change, not total return',operands:[{source_pointer:'/2/close',value_exact:'100'}]}]}]};}
test('missing, legacy and forged packets abstain without allocations',()=>{
 for(const p of [null,{}, {aggressive_basket:{positions:[{ticker:'SYNA',position_pct:99}]}},{...packet(),sizing_eligible:true}]){
  const html=ui.render(p);assert.match(html,/WAIT \/ abstain/);assert.match(html,/unavailable, conflicting or legacy/);assert.doesNotMatch(html,/99|SYNA/);
 }
});
test('real zero, whole source identity, dates and exact operand references survive',()=>{
 const html=ui.render(packet());assert.match(html,/<td>0<\/td>/);assert.match(html,/12000 bytes/);assert.match(html,/a{64}/);
 assert.match(html,/2026-09-18 → 2026-09-25/);assert.match(html,/\/2\/close/);assert.match(html,/reported dates/);
 assert.equal((html.match(/<th scope="row">/g)||[]).length,7);assert.match(html,/tabindex="0" role="region"/);
});
test('booleans, missing and duplicate metrics do not become measured zero',()=>{
 for(const value of [true,null,'0',NaN,Infinity]){const p=packet();p.price_observations[0].measurements[0].value=value;assert.doesNotMatch(ui.render(p),/<td>0<\/td>/);}
 const p=packet();p.price_observations[0].measurements.push({...p.price_observations[0].measurements[0]});assert.match(ui.render(p),/Missing or duplicate measurement/);assert.doesNotMatch(ui.render(p),/<td>0<\/td>/);
});
test('ambiguous issuer and malformed source content cannot inject markup',()=>{
 const p=packet();p.price_observations.push({...p.price_observations[0]});assert.match(ui.render(p),/No unambiguous/);
 const q=packet();q.price_observations[0].measurements[0].definition='<img src=x onerror=alert(1)>';q.generated_at='<script>bad</script>';
 assert.doesNotMatch(ui.render(q),/<img|<script/);assert.match(ui.render(q),/&lt;img/);
});
test('ticker drilldown is explicit and basket never reports risk from absent holdings',()=>{
 assert.match(ui.render(packet(),'OTHER'),/No unambiguous/);assert.match(ui.render(packet(),'SYNA'),/reported price observations/);
 assert.match(ui.basket(),/Portfolio risk unavailable/);assert.match(ui.basket(),/not a maximum-loss guarantee/);
});
test('wrong units withhold values and operand projection excludes unrelated fields',()=>{
 const p=packet();const m=p.price_observations[0].measurements[0];m.unit='USD';m.operands[0].private_note='not for publication';
 const html=ui.render(p);assert.doesNotMatch(html,/<td>0<\/td>|not for publication|private_note/);assert.match(html,/% price change/);
});
test('current page entrypoints clear stale allocation and reset regime',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../pre-pump-radar.html'),'utf8');const nodes={'agg-content':{innerHTML:'99%'},'basket-content':{innerHTML:'99%'},'regime-badge':{className:'RISK_ON',textContent:'RISK_ON'}};
 let code='';for(const name of ['renderAggressivePanel','renderBasketPanel'])code+=html.slice(html.indexOf('function '+name+'(){'),html.indexOf('function _legacy_'+name+'(){'));
 const scope={POSITIONING:null,JHPositioningEvidence:ui,document:{getElementById:id=>nodes[id]}};vm.runInNewContext(code+'renderAggressivePanel();renderBasketPanel();',scope);
 assert.doesNotMatch(nodes['agg-content'].innerHTML+nodes['basket-content'].innerHTML,/99%/);assert.equal(nodes['regime-badge'].textContent,'unqualified');
 assert.match(html,/const pos = null; \/\/ Preserved legacy sizing renderer/);assert.match(html,/src="\/jh-positioning-observations.js\?v=20260928"/);
 assert.ok(fs.readFileSync(path.join(__dirname,'fixtures/pre-positioning-observations-pre-pump-radar.html.txt')).length>200000);
});
