'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const code=fs.readFileSync(require('node:path').join(__dirname,'../jh-khalid-sniper.js'),'utf8');
const fixture=JSON.parse(fs.readFileSync(require('node:path').join(__dirname,'fixtures/khalid-qualification-synthetic.json')));
const NOW=Date.parse('2026-10-01T04:05:00Z');
function setup(fetcher){let now=NOW,calls=0,parses=0;class Clock extends Date{static now(){return now;}}
 const ctx={Date:Clock,AbortController,fetch:async(...args)=>{calls++;return fetcher?fetcher(...args):{ok:true,json:async()=>{parses++;return structuredClone(fixture);}}}};
 vm.runInNewContext(code,ctx);return {store:ctx.jhKhalidSnapshot,project:ctx.jhSniperQualification,setNow:n=>now=n,counts:()=>({calls,parses})};}
test('one concurrent fetch/parse; immutable indexed snapshot and identical cached projection',async()=>{
 const x=setup(),[a,b]=await Promise.all([x.store.load(),x.store.load()]);assert.equal(a,b);assert.deepEqual(x.counts(),{calls:1,parses:1});
 assert.equal(await x.store.load(),a);assert.equal(a.view(NOW),a.view(NOW));assert.equal(a.view(NOW,'TEST'),a.view(NOW,'TEST'));
 assert.equal(a.view(NOW,'ABSENT').valid,false);assert.equal(a.view(NOW,'TEST').rows.length,1);
 assert.throws(()=>{a.feed.opportunity_radar[0].action='TRACKING';},TypeError);
 assert.throws(()=>{a.view(NOW).rows[0].existing_backend_qualification.criteria[0].value=123;},TypeError);
 assert.equal(JSON.stringify(a.view(NOW)),JSON.stringify(x.project(a.feed,NOW)));
});
test('same revision with changed/malformed body always receives new validation; force immediately revokes old snapshot',async()=>{
 let packet=structuredClone(fixture);const x=setup(async()=>({ok:true,json:async()=>structuredClone(packet)}));const a=await x.store.load();
 packet.qualification_evidence.rows[0].existing_backend_qualification.criteria[0].value='unknown';
 const pending=x.store.load({force:true});assert.equal(a.view(NOW).valid,false);assert.equal(a.isCurrent(),false);
 const b=await pending;assert.equal(b.isCurrent(),true);assert.notEqual(a,b);assert.equal(b.view(NOW).valid,false);assert.equal(x.counts().calls,2);
 packet=structuredClone(fixture);packet.qualification_evidence.schema_version='future.v999';assert.equal((await x.store.load({force:true})).view(NOW).valid,false);
});
test('source deadline is checked every view, equality valid, expiry terminal even after rollback',async()=>{
 const f=structuredClone(fixture);f.qualification_evidence.rows[0].existing_backend_qualification.sources.find(s=>s.name==='khalid_risk').max_age_h=2;
 const x=setup(async()=>({ok:true,json:async()=>f})),s=await x.store.load(),edge=Date.parse('2026-10-01T06:00:00Z');
 assert.equal(s.deadline,edge+1);assert.equal(s.view(edge-1).valid,true);assert.equal(s.view(edge).valid,true);assert.equal(s.view(edge+1).valid,false);assert.equal(s.view(edge-1).valid,false);
});
test('publication expiry and backwards clocks fail closed without a second validation',async()=>{
 const x=setup(),s=await x.store.load(),edge=Date.parse(fixture.qualification_evidence.expires_at);
 assert.equal(s.view(edge-1).valid,true);assert.equal(s.view(edge).valid,false);
 const b=await x.store.load({force:true});assert.equal(b.view(NOW+1000).valid,true);assert.equal(b.view(NOW+999).valid,false);assert.equal(b.view(NOW+1001).valid,false);
});
test('superseded pending response cannot replace new snapshot, even if fetch ignores abort',async()=>{
 const resolves=[];const x=setup((url,opts)=>new Promise(resolve=>resolves.push({resolve,signal:opts.signal})));
 const first=x.store.load();await new Promise(r=>setImmediate(r));const second=x.store.load({force:true});const failed=assert.rejects(first,{name:'AbortError'});await new Promise(r=>setImmediate(r));
 assert.equal(resolves[0].signal.aborted,true);resolves[1].resolve({ok:true,json:async()=>structuredClone(fixture)});const s=await second;
 resolves[0].resolve({ok:true,json:async()=>({})});await failed;assert.equal(await x.store.load(),s);assert.equal(s.view(NOW).valid,true);
});
test('transport failure clears previous authority, fallback remains public, unsubscribing last consumer aborts pending',async()=>{
 let fail=false;const urls=[];const x=setup(async(url)=>{urls.push(url);if(fail)throw Error('offline');return {ok:true,json:async()=>structuredClone(fixture)}});const s=await x.store.load();fail=true;await assert.rejects(x.store.load({force:true}),/offline/);assert.equal(s.view(NOW).valid,false);assert.equal(urls.at(-1),'https://justhodl-data-proxy.raafouis.workers.dev/data/khalid.json');
 let resolve,signal;const y=setup((url,opts)=>{signal=opts.signal;return new Promise(r=>resolve=r)}),stop=y.store.subscribe(()=>{}),pending=y.store.load();const rejected=assert.rejects(pending,{name:'AbortError'});await new Promise(r=>setImmediate(r));stop();assert.equal(signal.aborted,true);resolve({ok:true,json:async()=>structuredClone(fixture)});await rejected;
});
test('reuse is bounded to one minute; refreshed body revokes externally retained old snapshot',async()=>{
 const x=setup(),a=await x.store.load();x.setNow(NOW+59999);assert.equal(await x.store.load(),a);x.setNow(NOW+60000);const b=await x.store.load();assert.notEqual(a,b);assert.equal(a.view(NOW+60000).valid,false);assert.equal(b.view(NOW+60000).valid,true);assert.equal(x.counts().calls,2);
});
test('script order makes dashboard and panel use the same store; no disk persistence or per-search cache',()=>{
 const html=fs.readFileSync(require('node:path').join(__dirname,'../khalid.html'),'utf8');assert.ok(html.indexOf('src="/jh-khalid-sniper.js')<html.indexOf('src="/khalid.js'));
 assert.doesNotMatch(code.slice(code.indexOf('var snapshotGeneration')),/localStorage|sessionStorage|indexedDB/);
});

test('malformed successful JSON fails closed without falling back to another artifact',async()=>{
 const x=setup(async()=>({ok:true,json:async()=>{throw new SyntaxError('invalid JSON')}}));
 await assert.rejects(x.store.load(),/invalid JSON/);assert.equal(x.counts().calls,1);
});
