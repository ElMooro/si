const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/warehouse-observations'),{normalize,manifest}=require('./helpers/warehouse-observation-preservation.cjs');
const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
test('warehouse scalar repair preserves every unrelated function and exact prior module',()=>{for(const file of Object.keys(manifest.entries))normalize(fs.readFileSync(path.join(R,file),'utf8'),file);});
test('older preservation gates retain all assertions and only normalize the reviewed next layer',()=>{
 for(const file of ['observation-cache','observation-diagnostics']){
  const p='tests/helpers/'+file+'-preservation.cjs',prior=fs.readFileSync(path.join(D,'pre502',p+'.txt'),'utf8'),current=fs.readFileSync(path.join(R,p),'utf8');
  const hook="\n const later=require('./warehouse-observation-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
  assert.equal(current,prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
 }
 const file='tests/observation-cache-preservation.test.js',prior=fs.readFileSync(path.join(D,'pre502',file+'.txt'),'utf8'),current=require('./helpers/chart-refresh-status-preservation.cjs').normalizeLegacyTest(fs.readFileSync(path.join(R,file),'utf8').replace(".replace('/jh-khalid-sniper.js?v=20261001-user-scope','/jh-khalid-sniper.js?v=20261001-snapshot')",''),file);
 assert.equal(current,prior.replace("file==='jh-observation-series.js'?path.join(R,file)","file==='jh-observation-series.js'?path.join(__dirname,'fixtures/warehouse-observations/pre502',file+'.txt')"));
 assert.equal(require('./helpers/chart-refresh-status-preservation.cjs').normalizeLegacyTest(fs.readFileSync(path.join(R,'tests/chart-observation-preservation.test.js'),'utf8').replace(".replace('/jh-khalid-sniper.js?v=20261001-user-scope','/jh-khalid-sniper.js?v=20261001-snapshot')",''),'tests/chart-observation-preservation.test.js'),fs.readFileSync(path.join(D,'pre502/tests/chart-observation-preservation.test.js.txt'),'utf8'));
});
test('a changed unrelated function or outer statement fails preservation',()=>{
 const file='jh-observation-series.js',current=fs.readFileSync(path.join(R,file),'utf8');
 assert.throws(()=>normalize(current.replace("v.trim()","v.trim()+'1'"),file));assert.throws(()=>normalize(current.replace("chart-observations.v1","chart-observations.v0"),file));
});

test('detail harness retains every assertion and adds only real resolver/browser dependencies',()=>{
 const file='tests/chart-detail-integrity.test.js',prior=fs.readFileSync(path.join(D,'pre502',file+'.txt'),'utf8'),current=fs.readFileSync(path.join(R,file),'utf8');
 const first="ctx.barEvidence.set(ctx.lastBars,{symbol:'SPY',interval:'1d',source:'synthetic'});vm.createContext(ctx);";
 const second="vm.runInContext(source.slice(source.indexOf('  function observationId(')";
 assert.equal(current,prior.replace(first,'ctx.window=ctx;'+first).replace(second,"vm.runInContext(source.slice(source.indexOf('  function resolveSym('),source.indexOf('  function nyOffset('))+source.slice(source.indexOf('  function observationId(')"));
});
