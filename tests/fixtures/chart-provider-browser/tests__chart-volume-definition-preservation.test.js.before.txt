const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-volume-definition'),{normalize,manifest}=require('./helpers/chart-volume-definition-preservation.cjs');
test('RVOL repairs preserve every unrelated function and exact predecessor statements',()=>{normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js');});
test('warehouse preservation retains every check with only the reviewed RVOL normalization hook',()=>{
 const file='tests/helpers/warehouse-observation-preservation.cjs',prior=fs.readFileSync(path.join(D,'pre503',file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-volume-definition-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('RVOL reviewed and unrelated mutations are both rejected by exact preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');
 assert.throws(()=>normalize(raw.replace('n=n===undefined?20:n;','n=n===undefined?19:n;'),file));
 assert.throws(()=>normalize(raw.replace('function obv(d){','function obv(d){ var changed=true;'),file));
 assert.throws(()=>normalize(raw.replace('chart.subscribeCrosshairMove(function(param){','chart.subscribeCrosshairMove(function(param){ var changed=true;'),file));
 assert.equal(Object.keys(manifest.entries).length,1);
});
