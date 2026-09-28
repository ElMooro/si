const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const api=require('../jh-credit-research.js');
const packet=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/credit-native.json'),'utf8')).packet;
const at=Date.parse(packet.generated_at),later=at+37*3600000;

function events(){const listeners=new Map();return {listeners,addEventListener(k,v){listeners.set(k,v);},removeEventListener(k,v){if(listeners.get(k)===v)listeners.delete(k);},emit(k){listeners.get(k)?.();}};}
function element(attr){return {textContent:'',getAttribute(){return attr;}};}
function host(){
 const count=element(),notice=element(),rows=Object.keys(packet.measurements).map(element),comparisons=Object.keys(packet.comparisons).map(element);
 return {count,notice,rows,comparisons,isConnected:true,querySelector(k){return k==='[data-credit-age-count]'?count:notice;},querySelectorAll(k){return k==='[data-credit-measurement-status]'?rows:comparisons;}};
}

test('whole predecessor reproduces the expired comparison-card bug',()=>{
 const raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-credit-live-clock-jh-credit-research.js.txt'));
 assert.equal(raw.length,16133);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'af4605685210754647b2aa69a7de82177a1fe5718fda1549ec659e1d1571dcd1');
 const context={module:{exports:{}},Date};vm.runInNewContext(raw.toString('utf8'),context);
 assert.equal(context.module.exports.current(packet,later),false);
 assert.match(context.module.exports.render(packet,later),/Both latest source dates match\./);
 const corrected=api.render(packet,later);
 assert.equal((corrected.match(/Current use: withheld\. Any value shown is dated comparison context\./g)||[]).length,4);
 assert.match(corrected,/publication status: available/);assert.match(corrected,/The collection deadline has passed/);
 assert.doesNotMatch(corrected,/Current use: within age ceiling/);
});

test('exact deadline, future and invalid clocks fail closed without extending old packets',()=>{
 const p=structuredClone(packet);p.freshness.pipeline_check_due_at=new Date(at+100*3600000).toISOString();
 assert.equal(api.current(p,at+36*3600000-1),true);assert.equal(api.current(p,at+36*3600000),false);
 p.freshness.pipeline_check_due_at=new Date(at+3600000).toISOString();
 assert.equal(api.current(p,at+3600000),false);
 assert.equal(api.collectionState(packet,at-1),'future');assert.match(api.render(packet,at-1),/publication clock is in the future/);
 for(const bad of [null,true,123,'2026-02-30T12:00:00Z','2026-09-20','2026-09-20T14:00:00']){
  assert.equal(api.collectionState({...packet,generated_at:bad},at),'invalid');
  assert.equal(api.collectionState({...packet,freshness:{pipeline_check_due_at:bad}},at),'invalid');
 }
 for(const bad of [NaN,Infinity,'2026-09-20',null])assert.equal(api.current(packet,bad),false);
});

test('comparison currentness requires two current same-date source rows',()=>{
 const p=structuredClone(packet),c=p.comparisons.hy_minus_ig;
 assert.equal(api.comparisonCurrent(p,c,at),true);
 for(const change of [r=>r.source_valid_until=new Date(at).toISOString(),r=>r.quality.status='stale',
                     r=>r.observation_date='2026-02-30',r=>r.observation_date='2026-09-21',r=>r.value_pct=null]){
  const x=structuredClone(p);change(x.measurements[c.right_series_id]);assert.equal(api.comparisonCurrent(x,c,at),false);
 }
 for(const change of [{both_latest_dates_match:false},{current_comparison_available:false},{left_latest_date:'2026-01-01'},
                     {observation_date:'2026-01-01'},{right_series_id:'missing'},{value_bps:null}])assert.equal(api.comparisonCurrent(p,{...c,...change},at),false);
 const old={...p.measurements[c.left_series_id],observation_date:'2026-09-01',source_valid_until:'2027-01-01T00:00:00Z'};
 assert.equal(api.rowCurrent(p,old,at),false,'A far-away claimed deadline cannot renew a five-day-old observation');
});

test('open-page clock updates every source and comparison label without erasing controls or fetching data',()=>{
 const h=host(),e=events(),doc={...events(),visibilityState:'visible'},tasks=new Set();let now=at;
 const timers={setInterval(fn,ms){assert.equal(ms,60000);tasks.add(fn);return fn;},clearInterval(fn){tasks.delete(fn);}};
 const controls={unit:'pct',series:'BAMLC0A0CM',exposure:'-9876',duration:'2.5',shock:'42'};h.controls=controls;
 const stop=api.watchCurrentness(h,packet,{events:e,doc,timers,now:()=>now});
 assert.equal(tasks.size,1);assert.match(h.count.textContent,/28 of 28/);assert.ok(h.comparisons.every(n=>n.textContent.startsWith('Current use: within age ceiling')));
 now=later;for(const fn of tasks)fn();
 assert.match(h.count.textContent,/0 of 28/);assert.ok(h.rows.every(n=>n.textContent==='Withheld'));
 assert.ok(h.comparisons.every(n=>n.textContent.startsWith('Current use: withheld')));assert.equal(h.controls,controls);
 now=at-1;for(const fn of tasks)fn();assert.match(h.notice.textContent,/publication clock is in the future/);
 doc.visibilityState='hidden';doc.emit('visibilitychange');assert.equal(tasks.size,0);
 doc.visibilityState='visible';doc.emit('visibilitychange');assert.equal(tasks.size,1);
 e.emit('pagehide');assert.equal(tasks.size,0);e.emit('pageshow');assert.equal(tasks.size,1);
 stop();stop();assert.equal(tasks.size,0);assert.equal(e.listeners.size,0);assert.equal(doc.listeners.size,0);
 e.emit('pageshow');assert.equal(tasks.size,0);
});

test('detached page releases the timer and no newer mount can retain old clock listeners',()=>{
 const h=host(),e=events(),doc={...events(),visibilityState:'visible'},tasks=new Set();
 const timers={setInterval(fn){tasks.add(fn);return fn;},clearInterval(fn){tasks.delete(fn);}};
 api.watchCurrentness(h,packet,{events:e,doc,timers,now:()=>at});h.isConnected=false;
 for(const fn of tasks)fn();assert.equal(tasks.size,0);assert.equal(e.listeners.size,0);assert.equal(doc.listeners.size,0);
 const h2=host();const stop=api.watchCurrentness(h2,packet,{events:e,doc,timers,now:()=>later});
 assert.equal(tasks.size,1);assert.match(h2.count.textContent,/0 of 28/);stop();assert.equal(tasks.size,0);
});
