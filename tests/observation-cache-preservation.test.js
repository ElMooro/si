const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {manifest,normalize}=require('./helpers/observation-cache-preservation.cjs'),R=path.join(__dirname,'..');
test('cache integration preserves every unrelated function and exact preceding source',()=>{for(const file of Object.keys(manifest.entries))normalize(fs.readFileSync(path.join(R,file),'utf8'),file);});
test('both whole HTML consumers add the bounded cache before its consumers without removing other content',()=>{
 for(const file of ['chart.html','crypto/index.html']){const raw=fs.readFileSync(path.join(R,file),'utf8'),tag='<script src="/jh-observation-cache.js?v=20261001"></script>\n';assert.equal(raw.split(tag).length,2);assert.equal(raw.replace(tag,'').replace('/jh-khalid-sniper.js?v=20261001-snapshot','/jh-khalid-sniper.js?v=20261001-qualification').replace('/jh-chart-tvrail.js?v=20261001-snapshot','/jh-chart-tvrail.js?v=20260925-bottom-pump'),fs.readFileSync(path.join(__dirname,'fixtures/observation-cache/pre500',file+'.txt'),'utf8'));assert.ok(raw.indexOf('jh-observation-cache.js')<raw.indexOf('jh-cq-fuse.js'));}
});
test('whole predecessor reproductions and exact source identities remain available',()=>{
 const f=require('./fixtures/observation-cache/reproduction.json');for(const [file,digest]of Object.entries(f.source)){const source=file==='jh-observation-series.js'?path.join(__dirname,'fixtures/warehouse-observations/pre502',file+'.txt'):path.join(__dirname,'fixtures/observation-cache/pre500',file+'.txt');assert.equal(crypto.createHash('sha256').update(fs.readFileSync(source)).digest('hex'),digest);}
 assert.equal(f.fuse_permanent_failure.additional_requests,0);assert.equal(f.fuse_permanent_success.additional_requests,0);assert.equal(f.catalog_permanent_success.additional_requests,0);assert.equal(f.fuse_reset_race.after_older_completes.series.series.sample.v[0],4);
});
