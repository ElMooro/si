const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.join(__dirname,'..'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/symbol-directory/browser-cache-reproduction.json'),'utf8'));
const source=fs.readFileSync(path.join(R,'jh-chart-catalog.js'),'utf8');
const master='/data/symbology/master.json',instruments='https://justhodl-data-proxy.raafouis.workers.dev/data/symdir/instruments.json.gz';
const flush=async()=>{for(let i=0;i<30;i++)await Promise.resolve();};
function load(raw=source){
 let now=Date.parse('2026-10-01T00:00:00Z'),mono=0,id=0,available=true,hang=null;
 const inputs=structuredClone(fixture.whole_inputs),requests=[],timers=new Map(),context={AbortController};
 class Clock extends Date {constructor(...args){super(...(args.length?args:[now]));}static now(){return now;}}
 Object.assign(context,{window:context,Date:Clock,performance:{now:()=>mono},setTimeout:(fn,ms)=>{timers.set(++id,{fn,end:mono+ms});return id;},clearTimeout:n=>timers.delete(n)});
 context.fetch=async (url,options)=>{assert.ok(Object.hasOwn(inputs,url),url);requests.push({url,available,signal:options?.signal});
  if(hang===url)return new Promise(()=>{});
  const value=inputs[url],ok=available && value!==undefined;
  return {ok,status:ok?200:503,json:async()=>structuredClone(value)};
 };
 vm.runInNewContext(raw,context,{filename:'invented-catalog.js'});
 return {api:context.JHChartCatalog,inputs,requests,timers,context,fail:()=>{available=false;},recover:()=>{available=true;},hang:url=>{hang=url;},
  tick:async(ms,wall=ms)=>{mono+=ms;now+=wall;for(const [n,t] of [...timers])if(t.end<=mono){timers.delete(n);t.fn();}await flush();},
  wall:ms=>{now+=ms;},monotonic:ms=>{mono+=ms;}};
}
test('complete predecessor reproduces the permanent failed-session cache',async()=>{
 const raw=fs.readFileSync(path.join(__dirname,'fixtures/symbol-directory/pre-browser-cache.js.txt'));
 assert.equal(raw.length,fixture.source_bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),fixture.source_sha256);
 const h=load(raw.toString());h.fail();assert.deepEqual(JSON.parse(JSON.stringify(await h.api.ensureIndex())),fixture.whole_first_load);
 const count=h.requests.length;h.recover();await h.tick(600000);assert.deepEqual(JSON.parse(JSON.stringify(await h.api.ensureIndex())),fixture.whole_after_recovery_load);
 assert.equal(h.requests.length,count);assert.equal(h.api.suggest('ZZRECOVERED').length,0);
});
test('cold failures report unknown counts and recover in the same session after bounded retry',async()=>{
 const h=load();h.fail();const first=await h.api.ensureIndex();assert.equal(first.n_sym,null);assert.ok(first.sources.every(s=>s.status==='unavailable'));
 const count=h.requests.length;h.recover();await h.tick(29999);await h.api.ensureIndex();assert.equal(h.requests.length,count);
 await h.tick(1);const next=await h.api.ensureIndex();assert.equal(next.n_sym,1);assert.equal(h.api.suggest('ZZRECOVERED')[0].s,'ZZRECOVERED');assert.equal(h.timers.size,0);
});
test('only failed sources retry; good populations retain their five-minute window',async()=>{
 const h=load();h.inputs[master]=undefined;await h.api.ensureIndex();const count=h.requests.length;
 h.inputs[master]=fixture.whole_inputs[master];await h.tick(30000);await h.api.ensureIndex();
 assert.deepEqual(h.requests.slice(count).map(r=>r.url),[master]);assert.equal(h.api.lookupSym('ZZRECOVERED'),'ZZRECOVERED');
});
test('successful downloads are reused before expiry and complete replacements load at expiry',async()=>{
 const h=load();await h.api.ensureIndex();h.inputs[master]={by_ticker:{ZZNEW:{name:'Invented new issuer'}}};
 await h.tick(299999);await h.api.ensureIndex();assert.equal(h.requests.length,5);assert.equal(h.api.lookupSym('ZZRECOVERED'),'ZZRECOVERED');
 await h.tick(1);await h.api.ensureIndex();assert.equal(h.requests.length,10);assert.equal(h.api.lookupSym('ZZRECOVERED'),'');assert.equal(h.api.lookupSym('ZZNEW'),'ZZNEW');
});
test('malformed replacement leaves the complete previous population and explicit cached status',async()=>{
 const h=load();h.inputs[instruments]={rows:[['ZZONE','Invented one'],['ZZTWO','Invented two']]};await h.api.ensureIndex();
 const before=JSON.parse(JSON.stringify(h.api.suggest('Invented',80)));h.inputs[instruments]={rows:[['ZZPARTIAL','Partial'],null]};await h.tick(300000);
 const next=await h.api.ensureIndex();assert.equal(next.n_inst,2);assert.equal(next.sources.find(s=>s.id==='instruments').status,'cached');
 assert.deepEqual(JSON.parse(JSON.stringify(h.api.suggest('Invented',80))),before);assert.equal(h.api.suggest('ZZPARTIAL').length,0);
});
test('malformed cold rows are isolated by source and never report successful zero coverage',async()=>{
 const h=load();h.inputs[instruments]={rows:[null]};h.inputs['/data/provider-catalog.json']={providers:[null]};
 const result=await h.api.ensureIndex();assert.equal(result.n_sym,1);assert.equal(result.n_inst,null);assert.equal(result.n_prov,null);
 assert.equal(result.sources.filter(s=>s.status==='unavailable').length,2);
});
test('concurrent callers share one in-flight job and timed-out sources cannot wedge future retries',async()=>{
 const h=load();h.hang(master);const first=h.api.ensureIndex(),second=h.api.ensureIndex();assert.equal(first,second);await flush();assert.equal(h.requests.length,5);
 await h.tick(10000);const value=await first;assert.equal(value.n_sym,null);assert.equal(h.requests.find(r=>r.url===master).signal.aborted,true);
 h.hang(null);await h.tick(30000);assert.equal((await h.api.ensureIndex()).n_sym,1);assert.equal(h.timers.size,0);
});
test('a late response after timeout cannot publish into a newer attempt',async()=>{
 const h=load(),original=h.context.fetch;let release;
 h.context.fetch=(url,options)=>url===master?new Promise(r=>{release=()=>r({ok:true,json:async()=>({by_ticker:{ZZLATE:{}}})});}):original(url,options);
 const pending=h.api.ensureIndex();await flush();await h.tick(10000);await pending;
 h.context.fetch=original;await h.tick(30000);await h.api.ensureIndex();release();await flush();assert.equal(h.api.lookupSym('ZZLATE'),'');assert.equal(h.api.lookupSym('ZZRECOVERED'),'ZZRECOVERED');
});
test('wall and monotonic clock regressions trigger checks rather than extending freshness',async()=>{
 for(const change of [h=>h.wall(-1),h=>h.monotonic(-1),h=>h.wall(300000),h=>h.monotonic(300000)]){
  const h=load();await h.api.ensureIndex();change(h);assert.equal(h.api.indexStatus().sources[0].status,'cached');await h.api.ensureIndex();assert.equal(h.requests.length,10);
 }
});
test('empty valid sources remain measured zero and never claim observation freshness',async()=>{
 const h=load();h.inputs[master]={by_ticker:{}};const s=await h.api.ensureIndex();assert.equal(s.n_sym,0);assert.equal(s.source_freshness_verified,false);
 assert.ok(s.sources.every(r=>r.status==='download_checked' && r.source_freshness_verified===false));
});
test('direct instrument fallback remains bounded in the same source attempt',async()=>{
 const h=load();h.inputs[instruments]=undefined;h.inputs['/data/symdir/instruments.json.gz']={rows:[['ZZFALLBACK','Invented fallback']]};
 assert.equal((await h.api.ensureIndex()).n_inst,1);assert.equal(h.requests.length,6);assert.equal(h.api.suggest('ZZFALLBACK')[0].s,'ZZFALLBACK');
});
test('on-chain refresh uses newly received complete source rather than the old fulfilled promise',async()=>{
 const h=load();await h.api.ensureIndex();const start=h.requests.length;await h.tick(300000);await h.api.ensureIndex();
 assert.ok(h.requests.slice(start).some(r=>r.url==='/data/cryptoquant-series.json'));
});
test('optional enrichment failure neither crashes primary sources nor blocks the timeout',async()=>{
 const h=load();h.context.JHCqFuse={load:()=>new Promise(()=>{})};const pending=h.api.ensureIndex();await flush();await h.tick(10000);const value=await pending;
 assert.equal(value.n_sym,1);assert.equal(value.sources.find(s=>s.id==='fuse').status,'unavailable');assert.equal(h.timers.size,0);
});
test('optional module cache cannot masquerade as a newly checked download',async()=>{
 const h=load();h.context.JHCqFuse={load:async()=>({chartable:[]})};const value=await h.api.ensureIndex();const enrichment=value.sources.find(s=>s.id==='fuse');
 assert.equal(enrichment.status,'enrichment_unverified');assert.equal(enrichment.checked_at,null);assert.equal(enrichment.age_s,null);assert.equal(enrichment.source_freshness_verified,false);
});
