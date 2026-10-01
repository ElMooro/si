const test=require('node:test'),assert=require('node:assert/strict');
const core=require('../jh-observation-cache.js');
const packet=n=>({extra:{whole:'retained'},series:{invented:{d:['2026-01-01'],v:[n]}}});
const flush=async()=>{for(let i=0;i<30;i++)await Promise.resolve();};
function setup(handler){let utc=Date.parse('2026-01-01'),mono=1000,n=0;const timers=new Map(),requests=[];
 const cache=core.create({now:()=>utc,monotonic:()=>mono,setTimeout:(fn,ms)=>{timers.set(++n,{fn,ms});return n;},clearTimeout:id=>timers.delete(id),fetch:(path,opts)=>{requests.push({path,opts});return handler(path,opts);}});
 return {cache,requests,timers,advance:ms=>{utc+=ms;mono+=ms;},utc:delta=>{utc+=delta;},mono:delta=>{mono+=delta;},tick:async()=>{for(const [id,t]of [...timers]){timers.delete(id);t.fn();}await flush();}};
}
const ok=body=>({ok:true,status:200,json:async()=>structuredClone(body)});
test('success is shared for five minutes and refreshed exactly at its boundary',async()=>{
 let value=1;const h=setup(()=>ok(packet(value)));
 const p=h.cache.read('series'),q=h.cache.read('series');assert.equal(p,q);const first=await p;assert.deepEqual(first.packet,packet(1));assert.equal(h.requests.length,1);assert.equal(h.timers.size,0);
 assert.equal(first.cache.state,'checked_within_interval');assert.equal(first.cache.source_freshness_verified,false);assert.equal(first.cache.calls_eligible,false);
 h.advance(299999);value=2;assert.equal((await h.cache.read('series')).packet.series.invented.v[0],1);h.advance(1);
 assert.equal((await h.cache.read('series')).packet.series.invented.v[0],2);assert.equal(h.requests.length,2);
});
test('initial failure is unavailable and retries after thirty seconds without poisoning success',async()=>{
 let fail=true;const h=setup(()=>fail?{ok:false,status:503,json:()=>{throw Error('must not read');}}:ok(packet(3)));
 const first=await h.cache.read('series');assert.equal(first.packet,null);assert.equal(first.cache.state,'unavailable');assert.equal(first.cache.last_error.http_status,503);fail=false;h.advance(29999);assert.equal((await h.cache.read('series')).packet,null);h.advance(1);
 assert.equal((await h.cache.read('series')).packet.series.invented.v[0],3);assert.equal(h.requests.length,2);
});
test('malformed refresh keeps the complete last good packet and rejected document separately',async()=>{
 let current=packet(1);const h=setup(()=>ok(current));await h.cache.read('series');h.advance(300000);current={series:[],whole_bad:{original:1}};
 const bad=await h.cache.read('series');assert.deepEqual(bad.packet,packet(1));assert.equal(bad.cache.state,'cached_after_failure');assert.deepEqual(bad.cache.attempts[0].rejected_packet,current);assert.equal(bad.cache.checked_at,null);
 current=packet(2);h.advance(30000);assert.equal((await h.cache.read('series')).packet.series.invented.v[0],2);
});
test('universe fallback is scoped, retains a rejected original, and shares one request deadline',async()=>{
 const h=setup(path=>ok(path==='/cq-universe.json'?{bad:'first original'}:{rows:[],extra:'retained'}));
 const r=await h.cache.read('universe');assert.deepEqual(h.requests.map(x=>x.path),['/cq-universe.json','/assets/cq-universe.json']);assert.equal(r.cache.accepted_path,'/assets/cq-universe.json');assert.deepEqual(r.cache.attempts[0].rejected_packet,{bad:'first original'});assert.equal(r.cache.attempts[1].status,'accepted');assert.equal(h.timers.size,0);
});
test('hung fetch and hung JSON both have bounded deadlines and cannot commit late results',async()=>{
 for(const phase of ['fetch','json']){
  let release;const delayed=new Promise(r=>{release=r;});let delayedCall=true;
  const h=setup(()=>delayedCall?(phase==='fetch'?delayed:{ok:true,json:()=>delayed}):ok(packet(9)));
  const p=h.cache.read('series');await flush();assert.equal([...h.timers.values()][0].ms,10000);await h.tick();
  const timed=await p;assert.equal(timed.packet,null);assert.equal(timed.cache.last_error.kind,'timeout');assert.equal(h.requests[0].opts.signal.aborted,true);assert.equal(h.timers.size,0);
  delayedCall=false;h.advance(30000);await h.cache.read('series');release(phase==='fetch'?ok(packet(4)):packet(4));await flush();assert.equal((await h.cache.read('series')).packet.series.invented.v[0],9);
 }
});
test('reset settles old callers and old completions cannot replace newer source generations',async()=>{
 let release,slow=true;const h=setup(()=>slow?new Promise(r=>{release=r;}):ok(packet(5)));
 const old=h.cache.read('series');await flush();h.cache.reset(['series']);const cancelled=await old;assert.equal(cancelled.cache.state,'superseded');assert.equal(cancelled.packet,null);assert.equal(h.requests[0].opts.signal.aborted,true);
 slow=false;await h.cache.read('series');release(ok(packet(4)));await flush();assert.equal((await h.cache.read('series')).packet.series.invented.v[0],5);assert.equal(h.timers.size,0);
});
test('wall or monotonic rollback cannot extend a successful or failed cache interval',async()=>{
 for(const fail of [false,true])for(const clock of ['utc','mono']){
  const h=setup(()=>fail?Promise.reject(Error('invented')):ok(packet(1)));await h.cache.read('series');h[clock](-1);await h.cache.read('series');assert.equal(h.requests.length,2);
 }
});
test('every source has its own state, retry interval and exact input schema',async()=>{
 let fail=true;const h=setup(path=>path.includes('ciss')?ok({series:[]}):fail?ok({series:false}):ok(packet(1)));
 await h.cache.read('series');await h.cache.read('ciss');h.advance(30000);fail=false;await h.cache.read('series');await h.cache.read('ciss');assert.equal(h.requests.length,3);
 assert.throws(()=>h.cache.read('__proto__'),/Unknown observation source/);assert.throws(()=>h.cache.status('constructor'),/Unknown observation source/);
});
test('decode and synchronous request failures are explicit and leave no timer',async()=>{
 for(const [handler,kind]of [[()=>{throw Error('transport');},'request_error'],[()=>({ok:true,json:()=>Promise.reject(Error('bad json'))}),'decode_error']]){
  const h=setup(handler),r=await h.cache.read('series');assert.equal(r.packet,null);assert.equal(r.cache.last_error.kind,kind);assert.equal(h.timers.size,0);
 }
});
test('synchronous decoding past the deadline is rejected even before the timer callback runs',async()=>{
 let slow=false;const h=setup(()=>({ok:true,json:()=>{if(slow)h.advance(10001);return packet(slow?2:1);}}));
 await h.cache.read('series');h.advance(300000);slow=true;const late=await h.cache.read('series');
 assert.deepEqual(late.packet,packet(1));assert.equal(late.cache.state,'cached_after_failure');assert.equal(late.cache.last_error.kind,'timeout');
 assert.equal(late.cache.attempts[0].status,'rejected');assert.deepEqual(late.cache.attempts[0].rejected_packet,packet(2));assert.equal(h.timers.size,0);
});
test('a delayed fetch cannot start decoding or a fallback after the overall deadline',async()=>{
 let parsed=0;const h=setup(()=>{h.advance(10000);return {ok:true,json:()=>{parsed++;return {rows:[]};}};});
 const result=await h.cache.read('universe');assert.equal(result.packet,null);assert.equal(result.cache.last_error.kind,'timeout');assert.equal(h.requests.length,1);assert.equal(parsed,0);assert.equal(h.timers.size,0);
});
