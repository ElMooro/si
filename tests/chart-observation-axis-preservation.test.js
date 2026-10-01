const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-observation-axis/pre506'),{normalize}=require('./helpers/chart-observation-axis-preservation.cjs');
test('axis repair preserves every unrelated function and reviewed complete outer initialization',()=>normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js'));
test('earlier point preservation assertions remain under one exact normalization hook',()=>{
 const file='tests/helpers/chart-observation-points-preservation.cjs',prior=fs.readFileSync(path.join(D,file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-observation-axis-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('prior scalar tests retain every assertion with only real dependency and chart API adapters',()=>{
 for(const file of ['tests/chart-observation-points.test.js','tests/chart-observation-integration.test.js']){
  let prior=fs.readFileSync(path.join(D,file+'.txt'),'utf8').replace('barEvidence:new WeakMap(),','barEvidence:new WeakMap(),observationAxes:new WeakMap(),').replace("'paintObservations',","'observationAxisFormatter','bindObservationAxis','paintObservations',");
  if(file.endsWith('points.test.js'))prior=prior.replace('const row={options,type,seriesType:','const row={options,type,applyOptions(o){Object.assign(this.options,o);},seriesType:');
  else prior=prior.replace('chart:{priceScale:','chart:{applyOptions(){},priceScale:').replace('addLineSeries:()=>({setData:','addLineSeries:()=>({applyOptions(){},setData:');
  assert.equal(file.endsWith('integration.test.js')?require('./helpers/chart-html-labels-preservation.cjs').normalizeLegacyTest(fs.readFileSync(path.join(R,file),'utf8'),file):fs.readFileSync(path.join(R,file),'utf8'),prior);
 }
});
test('source rounding or unrelated arithmetic edits fail the preservation gate',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');
 assert.throws(()=>normalize(raw.replace('if(exact.has(value))return String(value);','if(exact.has(value))return value.toFixed(2);'),file));
 assert.throws(()=>normalize(raw.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){var changed=true;'),file));
});
