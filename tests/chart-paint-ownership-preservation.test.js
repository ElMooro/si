const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-paint-ownership/pre508'),{normalize}=require('./helpers/chart-paint-ownership-preservation.cjs');
test('render ownership changes preserve every unrelated engine function and outer statement',()=>normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js'));
test('volume preservation retains its complete earlier assertions through one hash-bound hook',()=>{
 const file='tests/helpers/chart-market-volume-preservation.cjs',prior=fs.readFileSync(path.join(D,file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-paint-ownership-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('removing a response guard or changing market arithmetic fails preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');
 assert.throws(()=>normalize(raw.replace('if(current&&!current())return [];',''),file));
 assert.throws(()=>normalize(raw.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){var altered=true;'),file));
});
