const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),zlib=require('node:zlib'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8'),core=require('../jh-observation-series.js');
const publicDoc=name=>JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname,'fixtures/chart-cq-definition/public/'+name+'.gz'))));
const doc=publicDoc('cryptoquant-series.json'),onchain=publicDoc('cryptoquant-onchain.json'),spec=publicDoc('published-spec.json');
test('complete public packets retain original byte receipts',()=>{
 for(const r of require('./fixtures/chart-cq-definition/public/receipts.json')){const raw=zlib.gunzipSync(fs.readFileSync(path.join(R,r.path)));assert.equal(raw.length,r.original_bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),r.original_sha256);}
});
test('all 344 histories remain byte-equivalent observations and proxies, without requalification',()=>{
 const scope={module:{exports:{}}};vm.runInNewContext(read('tests/fixtures/chart-cq-definition/jh-observation-series.js.before.txt'),scope);const prior=scope.module.exports;
 assert.equal(Object.keys(doc.series).length,344);const original=JSON.stringify(doc);
 for(const id of Object.keys(doc.series)){
  const actual=core.cq(doc,'CQ:'+id),before=JSON.parse(JSON.stringify(prior.cq(doc,'CQ:'+id)));
  assert.deepEqual(actual.d,before.d,id);assert.deepEqual(actual.evidence.records,before.evidence.records,id);
  assert.deepEqual(actual.evidence.proxy_histories,before.evidence.proxy_histories,id);
  assert.equal(actual.evidence.whole_packet,doc);assert.equal(actual.evidence.calls_eligible,false);assert.equal(actual.evidence.sizing_eligible,false);
  if(id!=='btc_fees_total')assert.deepEqual(actual,before,id);
 }
 assert.equal(JSON.stringify(doc),original);
});
test('fee conflict cannot borrow a total-fee unit, a corrected label or a proxy history',()=>{
 const metric=spec.metrics.find(r=>r.name==='btc_fees_total');assert.equal(metric.resolved_key,'fees_block_mean');assert.equal(metric.unit,'BTC/d');
 for(const id of ['CQ:btc_fees_total','cq:BTC_FEES_TOTAL']){
  const packet={series:{btc_fees_total:{d:['2026-10-01'],v:[0.000123],unit:'BTC/d',label:'Repaired total fees'}},twins:{btc_fees_total:{d:['2009-01-01'],v:[123]}}};
  const result=core.cq(packet,id);assert.equal(result.d.length,1);assert.equal(result.d[0].close,0.000123);assert.equal(result.evidence.unit,null);assert.equal(result.evidence.reported_unit,'BTC/d');
  assert.equal(result.evidence.definition_review.historical_field_verified,false);assert.equal(result.evidence.definition_review.total_fees_qualified,false);assert.match(result.src,/do not interpret these values as total fees/);assert.equal(result.evidence.proxy_histories[0].joined,false);
 }
 for(const id of ['btc_fees_total_other','eth_fees_total','btc_fees_tx_mean','BTC:btc_fees_total'])assert.equal(core.cqDefinition(id),null);
 assert.equal(core.cq({series:{}},'CQ:btc_fees_total').d.length,0);
});
test('fuse and catalogue expose conflict, withhold only affected derived interpretation, preserve raw packets',async()=>{
 const inputs={'/data/cryptoquant-series.json':doc,'/data/cryptoquant-onchain.json':onchain,'/data/config/cryptoquant-spec.json':spec,'/data/cq-feed.json':{metrics:{}},'/data/cq-catalog.json':{catalog:{}},'/cq-universe.json':{rows:[]}};
 const c={setTimeout,clearTimeout};c.window=c;c.fetch=async url=>{assert.ok(Object.hasOwn(inputs,url),url);return {ok:true,json:async()=>inputs[url]};};
 for(const file of ['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js'])vm.runInNewContext(read(file),c);
 const pack=await c.JHCqFuse.load(),row=pack.chartable.find(r=>r.id==='btc_fees_total');assert.match(row.name,/definition conflict/);assert.equal(row.unit,'');assert.equal(pack.onchain,onchain);assert.equal(pack.series,doc.series);
 const parsed=await c.JHChartCatalog.klines('CQ:btc_fees_total');assert.equal(parsed.evidence.definition_review.status,'definition_conflict');assert.equal(parsed.evidence.whole_packet,doc);
 const hits=c.JHCqFuse.searchHits('CQ:btc_fees_total');assert.match(hits[0].name,/definition conflict/);assert.match(hits[0].extra,/fees_block_mean/);
 assert.match(c.JHChartCatalog.cqMeta.find(r=>r[0]==='btc_fees_total')[1],/definition conflict/);
 const pane=c.JHCqFuse.paneHTML({}),card=pane.split("data-cqhit='").find(s=>s.startsWith(row.blob.replace(/'/g,'')));
 assert.ok(card);assert.match(card,/z —/);assert.doesNotMatch(card,/th pctl/);assert.ok(!onchain.metrics.btc_fees_total.hist_read||!card.includes(onchain.metrics.btc_fees_total.hist_read));
});
test('whole predecessor source and all earlier assertions are reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-cq-definition-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
});
