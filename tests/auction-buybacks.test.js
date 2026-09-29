const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const cases=require('./fixtures/auction-buybacks.json').cases;
const html=fs.readFileSync(path.join(__dirname,'../auctions.html'),'utf8');
const code=html.slice(html.indexOf('  const buybackComplete ='),html.indexOf('  function securityLabel'));
const esc=value=>String(value??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
function context(){
 const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:''});return elements.get(id);};
 const scope={$:get,esc,bn:v=>v==null?'—':String(v),pct:v=>v==null?'—':String(v),num:String};vm.createContext(scope);vm.runInContext(code,scope);
 return {scope,get};
}
test('buyback chart keeps measured zero distinct from unavailable and clears on empty refresh',()=>{
 const c=context();c.scope.renderBuybacks(cases.measured_zero);
 assert.match(c.get('bb-chart').innerHTML,/measured zero/);assert.match(c.get('bb-chart').innerHTML,/<circle/);
 c.scope.renderBuybacks(cases.missing);assert.match(c.get('bb-chart').innerHTML,/: unavailable/);
 assert.match(c.get('bb-sample').textContent,/1 unavailable results/);
 c.scope.renderBuybacks(cases.empty);assert.doesNotMatch(c.get('bb-chart').innerHTML,/<rect|<circle/);
 assert.match(c.get('bb-chart').innerHTML,/No dated/);
});
test('displayed buyback sample never claims to be the complete program',()=>{
 const c=context();c.scope.renderBuybacks(cases.sample_45);
 assert.match(c.get('bb-sample').textContent,/40 of 45/);assert.match(c.get('bb-sample').textContent,/14 of 45/);
 assert.doesNotMatch(c.get('bb-chart').innerHTML,/cumulative/);
 assert.match(c.get('bb-stats').innerHTML,/full program coverage unverified/);
 c.scope.renderBuybacks(cases.future);assert.match(c.get('bb-sample').textContent,/1 future/);
 assert.doesNotMatch(c.get('bb-chart').innerHTML,/2026-10-02/);
});
test('legacy and duplicate operations cannot render a fabricated grade or aggregate',()=>{
 const c=context();for(const name of ['legacy','duplicates']){
  c.scope.renderBuybacks(cases[name]);assert.match(c.get('bb-table').innerHTML,/Unavailable/);
  assert.match(c.get('bb-stats').innerHTML,/>—</);
  const card=c.scope.buybackCard(cases[name].today.buybacks[0]);assert.match(card,/MEASUREMENT UNAVAILABLE/);
  assert.match(card,/Inspect buyback fields/);
 }
});
test('hostile source fields remain escaped in cards charts tables and evidence',()=>{
 const c=context(),packet=cases.hostile;c.scope.renderBuybacks(packet);
 const output=c.scope.buybackCard(packet.today.buybacks[0])+c.get('bb-table').innerHTML+c.get('bb-chart').innerHTML+c.get('bb-inputs').innerHTML;
 assert.doesNotMatch(output,/<img|<script/);assert.match(output,/&lt;img/);
 assert.match(output,/role="region"/);assert.match(output,/tabindex="0"/);
});

test('older buyback cash-language tags render as unmeasured without altering retained fields',()=>{
 const c=context(),packet=require('./fixtures/auction-buyback-pre-fill-classification.json'),copy=structuredClone(packet);
 c.scope.renderBuybacks(packet);const card=c.scope.buybackCard(packet.today.buybacks[0]);
 const output=card.split('<details class="grade-inputs"')[0]+c.get('bb-table').innerHTML;
 assert.match(output,/CASH EFFECT UNMEASURED/);assert.doesNotMatch(output,/TGA.CASH.OUT|EASING CALL/);
 assert.deepEqual(packet,copy);
 assert.match(card,/TGA-CASH-OUT SIGNAL/); // Complete raw evidence remains inspectable.
 assert.deepEqual(Array.from(c.scope.measurementTags(['TGA-CASH-OUT SIGNAL','TGA CASH-OUT; NOT AN EASING CALL','MAX FILL'])),['CASH EFFECT UNMEASURED','MAX FILL']);
});
