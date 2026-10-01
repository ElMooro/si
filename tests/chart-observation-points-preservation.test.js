const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-observation-points'),{normalize}=require('./helpers/chart-observation-points-preservation.cjs');
test('scalar drawing changes preserve all unrelated chart functions and outer statements',()=>normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js'));
test('all prior OBV preservation checks remain under one exact normalization hook',()=>{
 const file='tests/helpers/chart-obv-preservation.cjs',prior=fs.readFileSync(path.join(D,'pre505',file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-observation-points-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('prior browser assertions remain with only the explicit point-mode label update',()=>{
 for(const file of ['tests/chart-observation-browser-qa.cjs','tests/chart-observation-cache-browser-qa.cjs','tests/chart-observation-diagnostics-browser-qa.cjs','tests/chart-warehouse-observations-browser-qa.cjs']){
  const prior=fs.readFileSync(path.join(D,'pre505',file+'.txt'),'utf8');assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace("['btn-kind','Source line']","['btn-kind','Source points']"));
 }
});
test('restoring interpolation or changing unrelated market arithmetic fails preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');
 assert.throws(()=>normalize(raw.replace('lineVisible:false,pointMarkersVisible:true,pointMarkersRadius:3,title:', 'lineVisible:true,pointMarkersVisible:true,pointMarkersRadius:3,title:'),file));
 assert.throws(()=>normalize(raw.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){var changed=true;'),file));
});

test('earlier diagnostic-browser preservation changes only the expected source-mode caption',()=>{
 const file='tests/observation-diagnostics-preservation.test.js',prior=fs.readFileSync(path.join(D,'pre505',file+'.txt'),'utf8');
 const old='assert.equal(current,old.replaceAll(',next='assert.equal(current,old.replace("[\'btn-kind\',\'Source line\']","[\'btn-kind\',\'Source points\']").replaceAll(';
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace(old,next));
});
