const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-refresh-status/pre509'),{normalize,manifest}=require('./helpers/chart-refresh-status-preservation.cjs');
test('refresh status repair preserves every unrelated engine function and outer statement',()=>normalize(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'jh-chart-engine.js'));
test('render ownership preservation retains every earlier assertion through one hash-bound hook',()=>{
 const file='tests/helpers/chart-paint-ownership-preservation.cjs',prior=fs.readFileSync(path.join(D,file+'.txt'),'utf8');
 const hook="\n const later=require('./chart-refresh-status-preservation.cjs');if(later.manifest.entries[file]&&hash(raw)!==later.manifest.entries[file].prior.sha256)raw=later.normalize(raw,file);";
 assert.equal(fs.readFileSync(path.join(R,file),'utf8'),prior.replace('function normalize(raw,file){','function normalize(raw,file){'+hook));
});
test('only the reviewed initial footer status changes in the complete chart page',()=>{
 const record=manifest.html,old=fs.readFileSync(path.join(R,record.prior.path),'utf8'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 assert.equal(hash(old),record.prior.sha256);assert.equal(old.split(record.old).length-1,1);
 const current=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace('/jh-khalid-sniper.js?v=20261001-user-scope','/jh-khalid-sniper.js?v=20261001-snapshot');assert.equal(current,old.replace(record.old,record.fresh));assert.equal(hash(current),record.sha256);
 for(const file of Object.keys(manifest.test_adaptations))require('./helpers/chart-refresh-status-preservation.cjs').normalizeLegacyTest(require('./helpers/squeeze-research-preservation.cjs').normalizeTest(fs.readFileSync(path.join(R,file),'utf8'),file).split(".replace(\".replace('/jh-khalid-sniper.js?v=20261001-user-scope','/jh-khalid-sniper.js?v=20261001-snapshot')\",'')").join('').replace(".replace('/jh-khalid-sniper.js?v=20261001-user-scope','/jh-khalid-sniper.js?v=20261001-snapshot')",''),file);
});
test('removing selected-symbol validation or changing market arithmetic fails preservation',()=>{
 const file='jh-chart-engine.js',raw=fs.readFileSync(path.join(R,file),'utf8');
 assert.throws(()=>normalize(raw.replace('tape.sym!==active||',''),file));
 assert.throws(()=>normalize(raw.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){var altered=true;'),file));
});
