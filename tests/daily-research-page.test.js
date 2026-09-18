const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

test('Intelligence adaptive card clears scores instead of coloring null as bearish and divergence as aligned',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../intelligence/index.html'),'utf8');
  const source=html.slice(html.indexOf('function renderKhalid(d)'),html.indexOf('// ─── STRESS SCENARIOS',html.indexOf('function renderKhalid(d)')));
  const nodes=Object.fromEntries(['khalid-kpis','khalid-table','khalid-interp'].map(id=>[id,{innerHTML:'STALE BULLISH 99',textContent:''}]));
  const scope={document:{getElementById:id=>nodes[id]}};vm.createContext(scope);vm.runInContext(source,scope);
  for(const packet of [null,{standard:{score:null},adaptive:{score:null}},
      {contract:'adaptive-research-status.v1',standard:{score:100},adaptive:{score:100},divergence:{score_delta:0}}]){
    scope.renderKhalid(packet);
    assert.match(nodes['khalid-kpis'].innerHTML,/Unavailable/);
    assert.doesNotMatch(nodes['khalid-kpis'].innerHTML,/null|STALE|neg|pos/);
    assert.match(nodes['khalid-interp'].innerHTML,/WAIT — abstain/);
    assert.doesNotMatch(nodes['khalid-interp'].innerHTML,/agreement|divergence 0/);
    assert.match(nodes['khalid-table'].textContent,/history has been retained unchanged/);
  }
});

test('ATH coverage unavailable is not zero breakouts or a very weak market',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../ath.html'),'utf8');
  const start=html.indexOf('function render()');
  const end=html.indexOf('\nfunction ',start+1);
  assert.ok(end>start,'next function boundary must exist');
  const source=html.slice(start,end);
  const nodes=Object.fromEntries(['countBreakouts','countNear','countTracked','marketStrength','updatedAt','main'].map(id=>[id,{textContent:'OLD',innerHTML:'OLD',style:{}}]));
  const scope={D:{ath_breakouts:{status:'unavailable'}},document:{getElementById:id=>nodes[id]}};
  vm.createContext(scope);vm.runInContext(source,scope);scope.render();
  assert.equal(nodes.countBreakouts.textContent,'Unavailable');assert.equal(nodes.countNear.textContent,'Unavailable');
  assert.equal(nodes.marketStrength.textContent,'Unverified');
  assert.match(nodes.main.innerHTML,/Missing coverage does not mean zero breakouts/);
  assert.doesNotMatch(nodes.main.innerHTML,/VERY WEAK/);
});
