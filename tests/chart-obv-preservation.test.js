const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-obv'),{normalize,manifest}=require('./helpers/chart-obv-preservation.cjs');
test('OBV and research arithmetic preserve every unrelated function and outer statement',()=>{for(const file of Object.keys(manifest.entries))normalize(fs.readFileSync(path.join(R,file),'utf8'),file);});
test('RVOL preservation retains all checks with only the reviewed OBV normalization hook',()=>{
 const file='tests/helpers/chart-volume-definition-preservation.cjs',prior=fs.readFileSync(path.join(D,'pre504',file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-obv-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('changed reviewed or unrelated functions fail preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');
 assert.throws(()=>normalize(raw.replace('total=0,broken=false','total=1,broken=false'),file));
 assert.throws(()=>normalize(raw.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){ var changed=true;'),file));
 const other='jh-stock-desk-research.js';assert.throws(()=>normalize(fs.readFileSync(path.join(R,other),'utf8').replace('forecast_qualified:false','forecast_qualified:true'),other));
});
