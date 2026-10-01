const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-market-volume/pre507'),{normalize}=require('./helpers/chart-market-volume-preservation.cjs');
test('market volume repair preserves all unrelated functions and every outer initialization statement',()=>normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js'));
test('earlier axis preservation remains under one exact normalization hook',()=>{
 const file='tests/helpers/chart-observation-axis-preservation.cjs',prior=fs.readFileSync(path.join(D,file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-market-volume-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('old regression assertions remain with only the real new dependency and DOM adapters',()=>{
 const a='tests/chart-volume-definition.test.js',b='tests/chart-observation-axis.test.js';
 const c='tests/observation-diagnostics.test.js';
 assert.equal(fs.readFileSync(path.join(R,c),'utf8'),fs.readFileSync(path.join(D,c+'.txt'),'utf8').replace("['identifyBars','observationId','resampleToTf','klines','clearObservationFrame','observationText','paint','load']","['reportedVolume','volumeTotal','identifyBars','observationId','resampleToTf','klines','clearObservationFrame','observationText','paint','load']"));
 assert.equal(require('./helpers/chart-html-labels-preservation.cjs').normalizeLegacyTest(fs.readFileSync(path.join(R,a),'utf8'),a),fs.readFileSync(path.join(D,a+'.txt'),'utf8').replace("['rvolAt','rvolSeries','volCandlePaint','quoteUI']","['reportedVolume','rvolAt','rvolSeries','volCandlePaint','quoteUI']"));
 assert.equal(fs.readFileSync(path.join(R,b),'utf8'),fs.readFileSync(path.join(D,b+'.txt'),'utf8').replace('const c={observationAxes:new WeakMap()};','const c={observationAxes:new WeakMap(),document:{getElementById:()=>null}};'));
});
test('restoring false-zero normalization or changing unrelated price logic fails preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');assert.throws(()=>normalize(raw.replace('volume:reportedVolume(b[5])','volume:+(b[5]||0)'),file));assert.throws(()=>normalize(raw.replace('function computeChange(d,m){','function computeChange(d,m){var altered=true;'),file));
});
