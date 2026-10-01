const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-signed-volume/pre510'),{normalize,normalizeLegacyTest,manifest}=require('./helpers/chart-signed-volume-preservation.cjs');
test('signed estimate changes preserve every unrelated engine function and outer statement',()=>normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js'));
test('HTML-label preservation retains all earlier assertions through two hash-bound source/test hooks',()=>{
 const file='tests/helpers/chart-html-labels-preservation.cjs',prior=fs.readFileSync(path.join(D,file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-signed-volume-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 const legacy="\n const later=require('./chart-signed-volume-preservation.cjs');if(later.manifest.test_adaptations[file]&&hash(raw)!==later.manifest.test_adaptations[file].prior.sha256)raw=later.normalizeLegacyTest(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook).replace('function normalizeLegacyTest(raw,file){','function normalizeLegacyTest(raw,file){'+legacy));
});
test('all earlier volume assertions survive with only actual new function dependencies',()=>{for(const file of Object.keys(manifest.test_adaptations))normalizeLegacyTest(fs.readFileSync(path.join(R,file),'utf8'),file);});
test('restoring the sign defect or changing market arithmetic fails preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');assert.throws(()=>normalize(raw.replace('(value<0?"-":"+")','(value<0?"":"+")'),file));assert.throws(()=>normalize(raw.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){var altered=true;'),file));
});
