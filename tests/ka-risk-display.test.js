const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(require.resolve('../ka/index.html'),'utf8');
function loadFunction(name,scope){const line=source.split('\n').find(line=>line.startsWith('function '+name+'('));assert.ok(line);vm.runInNewContext(line,scope);return scope[name];}
test('KA gauge preserves zero and displays missing risk as unavailable',()=>{
 for(const risk of [0,null,NaN]) {
  const nodes=new Map(),document={getElementById(id){if(!nodes.has(id))nodes.set(id,{style:{}});return nodes.get(id)}};
  const scope={DATA:{risk_index:risk},document};loadFunction('renderGauge',scope)();
  assert.equal(nodes.get('riskNum').textContent,risk===0?0:'--');
  assert.equal(nodes.get('riskRegime').textContent,risk===0?'LOW RISK':'UNAVAILABLE');
 }
});
test('KA local weight preview never submits a global configuration',()=>{
 let renders=0,recalcs=0;const node={};const scope={document:{getElementById(){return node}},render(){renders++},recalc(){recalcs++},fetch(){throw new Error('global write forbidden')}};
 loadFunction('saveConfig',scope)();assert.equal(renders,1);assert.equal(recalcs,1);assert.match(node.textContent,/Local preview only/);
});
