const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-html-labels/pre511'),{normalize,normalizeWhole,normalizeLegacyTest,manifest}=require('./helpers/chart-html-labels-preservation.cjs');
test('HTML label repair preserves every unrelated engine function and outer statement',()=>normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js'));
test('complete indicator UX file changes only its helper and three reviewed substitutions',()=>normalizeWhole(fs.readFileSync(path.join(R,'jh-chart-indux.js'),'utf8'),'jh-chart-indux.js'));
test('refresh preservation retains every earlier assertion through one exact normalization hook',()=>{
 const file='tests/helpers/chart-refresh-status-preservation.cjs',prior=fs.readFileSync(path.join(D,file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-html-labels-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('all earlier volume assertions survive with the actual escaping dependency',()=>{for(const file of Object.keys(manifest.test_adaptations))normalizeLegacyTest(fs.readFileSync(path.join(R,file),'utf8'),file);});
test('restoring executable labels or changing unrelated functions fails preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');assert.throws(()=>normalize(raw.replace('escHtml(active)','active'),file));assert.throws(()=>normalize(raw.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){var altered=true;'),file));
 const ux='jh-chart-indux.js',ix=fs.readFileSync(path.join(R,ux),'utf8');assert.throws(()=>normalizeWhole(ix.replace('escapeLabel(ctx.active)','ctx.active'),ux));assert.throws(()=>normalizeWhole(ix+'\nthrow Error("unrelated")',ux));
});
