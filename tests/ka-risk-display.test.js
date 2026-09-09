const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(require.resolve('../ka/index.html'),'utf8');
function loadFunction(name,scope){const line=source.split('\n').find(line=>line.startsWith('function '+name+'('));assert.ok(line);vm.runInNewContext(line,scope);return scope[name];}
test('KA gauge preserves zero and displays missing risk as unavailable',()=>{
 for(const risk of [0,null,NaN]) {
  const nodes=new Map(),document={getElementById(id){if(!nodes.has(id))nodes.set(id,{style:{}});return nodes.get(id)}};
  const scope={DATA:{engine:'justhodl-ka-metrics',risk_index:risk,generated:new Date().toISOString()},document,localPreview:false};loadFunction('snapshotState',scope);loadFunction('renderGauge',scope)();
  assert.equal(nodes.get('riskNum').textContent,risk===0?0:'--');
  assert.equal(nodes.get('riskRegime').textContent,risk===0?'LOW RISK':'UNAVAILABLE');
 }
});
test('KA local weight preview never submits a global configuration',()=>{
 let renders=0,recalcs=0;const node={};const scope={document:{getElementById(){return node}},render(){renders++},recalc(){recalcs++},fetch(){throw new Error('global write forbidden')}};
 loadFunction('saveConfig',scope)();assert.equal(renders,1);assert.equal(recalcs,1);assert.match(node.textContent,/Local preview only/);
});

test('KA stale, missing, future and mismatched snapshots cannot show live classifications',()=>{
 const now=new Date().toISOString();const doc={engine:'justhodl-ka-metrics',generated:now,risk_index:33};
 for(const change of [{generated:'2026-08-01T00:00:00Z'},{generated:'2026-08-01T00:00:00'},{generated:new Date(Date.now()+600000).toISOString()},{engine:'another-engine'}]){
  const nodes=new Map(),document={getElementById(id){if(!nodes.has(id))nodes.set(id,{style:{}});return nodes.get(id)}};
  const scope={DATA:{...doc,...change},document,localPreview:false};loadFunction('snapshotState',scope);loadFunction('renderGauge',scope)();
  assert.equal(nodes.get('riskNum').textContent,'--');assert.notEqual(nodes.get('snapshotStatus').textContent,'CURRENT');
 }
 const scope={DATA:doc,AI:{...doc,schema_version:'macro-analysis.v1',input_artifact:'data/ka-metrics.json',input_generated:now,llm_status:'available'},localPreview:false};
 loadFunction('snapshotState',scope);loadFunction('analysisUsable',scope);assert.equal(scope.analysisUsable(),true);
 scope.AI.input_generated='2026-08-01T00:00:00Z';assert.equal(scope.analysisUsable(),false);
 scope.AI.input_generated=now;scope.localPreview=true;assert.equal(scope.analysisUsable(),false);
});
