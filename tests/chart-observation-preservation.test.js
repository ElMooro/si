const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {normalize,manifest}=require('./helpers/chart-observation-preservation.cjs');
const R=path.join(__dirname,'..');
test('whole catalog, fuse, engine and inspection modules preserve all unrelated code',()=>{
 for(const file of Object.keys(manifest.entries))normalize(fs.readFileSync(path.join(R,file),'utf8'),file);
});
test('both consumers load the exact observation parser before the existing modules and preserve remaining HTML',()=>{
 for(const p of ['chart.html','crypto/index.html']){
  const prior=fs.readFileSync(path.join(__dirname,'fixtures/chart-observations/pre499',p+'.txt'),'utf8'),current=fs.readFileSync(path.join(R,p),'utf8').replace('<script src="/jh-observation-cache.js?v=20261001"></script>\n','');
  const added='<script src="/jh-observation-series.js?v=20261001"></script>\n';assert.equal(current.split(added).length,2);// The snapshot performance follow-up changes only these two cache keys.
  const normalized=current.replace(added,'')
   .replace('/jh-khalid-sniper.js?v=20261001-user-scope','/jh-khalid-sniper.js?v=20261001-snapshot').replace('/jh-khalid-sniper.js?v=20261001-snapshot','/jh-khalid-sniper.js?v=20261001-qualification')
   .replace('/jh-chart-tvrail.js?v=20261001-snapshot','/jh-chart-tvrail.js?v=20260925-bottom-pump');
  assert.equal(normalized,prior);
  assert.ok(current.indexOf('jh-observation-series.js')<current.indexOf('jh-cq-fuse.js'));
 }
});
test('whole observed predecessor outputs and source identities remain retained',()=>{
 const f=require('./fixtures/chart-observations/whole-predecessor-reproduction.json');assert.equal(f.actual_network_requests,0);
 for(const source of f.source){const raw=fs.readFileSync(path.join(__dirname,'fixtures/chart-observations/pre499',source.path+'.txt'));assert.equal(raw.length,source.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),source.sha256);}
 for(const key of ['whole_catalog_cq_result','whole_catalog_ciss_result','whole_fuse_cq_result'])for(const index of f.false_zero_ordinals)assert.equal(f[key].d[index].close,0);
 assert.ok(f.whole_monthly_query_result.d.every(b=>new Date(b.time*1000).getUTCDate()===1));
});
test('the complete pinned drawing fixture matches the library already referenced by the page',()=>{
 const proof=require('./fixtures/chart-observations/vendor/source.json'),raw=fs.readFileSync(path.join(__dirname,'fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt'));
 assert.equal(raw.length,proof.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),proof.sha256);
 assert.ok(fs.readFileSync(path.join(R,'chart.html'),'utf8').includes(proof.source_url));assert.match(raw.toString('utf8'),/Licensed under Apache License 2\.0/);
});
