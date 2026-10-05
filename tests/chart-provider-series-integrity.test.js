const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
const harness=read('tests/chart-qualified-series.test.js').split("test('predecessor accepts")[0];
const scope={require,process,console,__dirname:path.join(R,'tests'),structuredClone};vm.createContext(scope);vm.runInContext(harness+'\nglobalThis.bootFixture=boot;globalThis.packetFixture=packet;',scope);
const history=require('../jh-observation-series.js');
function scalar(packet){const c=scope.bootFixture(packet);c.observationId=()=>true;c.window.JHObservationSeries=history;c.window.JHChartCatalog={klines:async id=>history.warehouse(packet,id,"/series?id="+encodeURIComponent(id))};c.active=packet.id;return c;}
test('provider catalogue retains the complete packet and scalar records instead of market candles',async()=>{
 const packet={id:'census:qss:ADM:7111T:no:US',provider:'census',freq:'Q',unit:'Number',obs:[['2026-01-01',-4],['2026-04-01',0],['2026-07-01',null]]},c=scalar(packet),bars=await c.klines(packet.id,'1d'),e=c.barEvidence.get(bars).observations;
 assert.equal(bars.length,2);assert.deepEqual(Array.from(bars,b=>b.close),[-4,0]);assert.equal(e.contract,'chart-observations.v1');assert.equal(e.whole_packet,packet);assert.equal(e.records.length,3);assert.equal(e.source_frequency,'Q');assert.ok(bars.every(b=>Array.isArray(b.observation_ordinals)&&b.volume===null));assert.equal(c.barEvidence.get(bars).market_history,undefined);
});
test('a prior catalogue refusal is never retried through the provider fallback',async()=>{
 const p={id:'census:mrts:IM:44000:no:US'},c=scalar(p);c.window.JHChartCatalog={klines:async()=>{throw new Error('HTTP 403');}};
 assert.equal((await c.klines(p.id,'1d')).length,0);assert.equal(c.calls.length,0);
});
test('prefix and appended dimensions cannot satisfy exact market identity',async()=>{
 for(const suffix of [':OTHER',':M','X']){const p=scope.packetFixture('NASDAQ:ABC'+suffix),c=scope.bootFixture(p);assert.equal((await c.klines('NASDAQ:ABC','1d')).length,0);assert.equal(c.calls.length,1);}
});
test('booleans, conflicts and absent values cannot silently become scalar prices',async()=>{
 const p={id:'census:mrts:IM:44000:no:US',provider:'census',freq:'M',unit:'Millions',obs:[['2026-01-01',false],['2026-02-01',1],['2026-02-01',9],['2026-03-01',null],['2026-04-01',0]]},c=scalar(p),bars=await c.klines(p.id,'1d'),e=c.barEvidence.get(bars).observations;
 assert.deepEqual(Array.from(bars,b=>b.close),[0]);assert.equal(e.records.length,5);assert.equal(e.rejected_records,4);
});
