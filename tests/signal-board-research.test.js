const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{webcrypto}=require('node:crypto');
const api=require('../jh-signal-board-research.js'),transport=require('../jh-fifx-research.js'),catalog=require('../assets/signal-board-registry.json');
const copy=v=>JSON.parse(JSON.stringify(v));
const packet=()=>({generated_at:'2026-09-26T06:15:00Z',n_engines:99,composite_signal:1.8,composite_posture:'STRONG RISK-ON',deep_read:{lean:'BUY'},engines:catalog.feeds.map((r,i)=>({engine:r.engine,category:r.category,signal:i%5-2,signal_label:'STRONG RISK-ON',read:'unvalidated narrative',as_of:i===0?null:'2026-09-25T06:00:00Z',stale:i%3===0}))});
const native=require('./fixtures/signal-board-native-view.json');
function nativeWire(artifacts=native.artifacts){return async url=>{const key=new URL(url,'https://example.invalid').pathname.slice(1);if(key==='assets/signal-board-registry.json')return Response.json(catalog);if(!(key in artifacts))throw Error('Unknown artifact');return new Response(artifacts[key]);};}
function document(){const els={};return {els,getElementById(id){return els[id]??={value:'',textContent:'',innerHTML:'',hidden:false,handlers:{},addEventListener(name,fn){this.handlers[name]=fn;}};}};}
function wire(p=packet(),raw){return async u=>new Response(u.startsWith('/data/signal-board.json')?raw||JSON.stringify(p):JSON.stringify(catalog));}

test('every reported row and shared source remains visible without votes',()=>{
 const p=packet(),rows=api.inventory(p,catalog);assert.equal(rows.length,99);assert.equal(new Set(rows.map(r=>r.source.source_key)).size,98);
 assert.equal(rows.filter(r=>r.source_views===2).length,2);assert.ok(rows.every(r=>r.calls_eligible===false&&r.sizing_eligible===false));
 assert.equal(rows[0].as_of,'Not reported');assert.equal(rows[0].raw,p.engines[0]);
});
test('private inputs never become links and untrusted mappings are rejected',()=>{
 const rows=api.inventory(packet(),catalog);for(const key of ['data/pm-decision.json','data/sizing.json'])assert.ok(!api.sourceLink(rows.find(r=>r.source.source_key===key)).includes('<a'));
 for(const value of ['https://evil.invalid','data/../private.json','data/portfolio/snapshot.json']){const c=copy(catalog);c.feeds[0].source_key=value;assert.throws(()=>api.registry(c));}
 const c=copy(catalog);c.feeds.pop();assert.throws(()=>api.registry(c));
});
test('duplicate, malformed and unmapped rows are retained without a source assertion',()=>{
 const p=packet();p.engines.push(p.engines[0],null,{engine:'__proto__',category:'macro'});const rows=api.inventory(p,catalog);
 assert.equal(rows.length,102);assert.equal(rows[0].source,null);assert.equal(rows[99].source,null);assert.equal(rows[100].raw,null);assert.equal(rows[101].source,null);
 p.engines[1].category='wrong category';assert.equal(api.inventory(p,catalog)[1].source,null);
 for(const bad of [null,{},[],{engines:{}},{engines:[],contract:'future.v1'}])assert.throws(()=>api.inventory(bad,catalog));
});
test('clock disclosures never convert generation time into source freshness',()=>{
 const now=Date.parse('2026-09-26T08:00:00Z');assert.match(api.clockStatus('bad',now),/unavailable/);
 assert.match(api.clockStatus('2026-09-27T08:00:00Z',now),/Future/);assert.match(api.clockStatus('2026-09-20T08:00:00Z',now),/older than 40/);
 assert.match(api.clockStatus('2026-09-26T06:15:00Z',now),/observation freshness unverified/);
});
test('mounted inventory filters complete rows and exposes unqualified original fields',async()=>{
 const doc=document(),old=setInterval;globalThis.setInterval=()=>1;
 try{const app=api.mount(doc,transport,wire(),webcrypto);await app.refresh();assert.equal(doc.els['sb-content'].hidden,false);assert.match(doc.els['sb-count'].textContent,/99 of 99/);
 assert.ok(!doc.els['sb-table'].innerHTML.includes('STRONG RISK-ON'));assert.ok(!doc.els['sb-table'].innerHTML.includes('unvalidated narrative'));
 doc.els['sb-search'].value='liquidity-inflection';doc.els['sb-search'].handlers.input();assert.match(doc.els['sb-count'].textContent,/2 of 99/);
 doc.els['sb-table'].handlers.click({target:{closest:selector=>selector==='[data-sb-row]'?{dataset:{sbRow:'0'}}:null}});assert.match(doc.els['sb-detail'].textContent,/unvalidated narrative/);assert.match(doc.els['sb-detail-note'].textContent,/Unqualified decoded row/);
 assert.match(doc.els['sb-extra'].innerHTML,/S&amp;P/);app.destroy();}finally{globalThis.setInterval=old;}
});
test('whole downloaded source preserves numeric lexemes and unsafe text stays inert',async()=>{
 const doc=document(),old=setInterval;globalThis.setInterval=()=>1;const p=packet();p.engines[0].engine='<img src=x onerror=evil()>';
 const raw=JSON.stringify(p).replace('"n_engines":99','"n_engines":99,"precise":9007199254740993');
 try{const app=api.mount(doc,transport,wire(p,raw),webcrypto);await app.refresh();assert.equal(doc.els['sb-raw'].textContent,raw);
 assert.ok(doc.els['sb-table'].innerHTML.includes('&lt;img'));assert.ok(!doc.els['sb-table'].innerHTML.includes('<img'));app.destroy();}finally{globalThis.setInterval=old;}
});
test('failed refresh clears the prior packet and late reads cannot resurrect it',async()=>{
 const doc=document(),old=setInterval;globalThis.setInterval=()=>1;let fail=false,release,first=true;
 const fetcher=async u=>{if(fail)throw Error('offline');if(first&&u.startsWith('/data/')){first=false;return new Promise(r=>release=r);}return wire()(u);};
 try{const app=api.mount(doc,transport,fetcher,webcrypto);await app.refresh();assert.equal(doc.els['sb-content'].hidden,false);
 fail=true;await app.refresh();assert.equal(doc.els['sb-content'].hidden,true);assert.equal(doc.els['sb-raw'].textContent,'');release(new Response(JSON.stringify(packet())));await new Promise(setImmediate);assert.equal(doc.els['sb-content'].hidden,true);app.destroy();}finally{globalThis.setInterval=old;}
});
test('all previous supplementary sources are retained as read-only references',()=>{
 const old=fs.readFileSync(path.join(__dirname,'fixtures/legacy-signal-board-stage140.html.txt'),'utf8');
 const oldFeeds=old.match(/data-feeds="([^"]+)"/)[1].split(';').map(v=>v.split('|')[0]);for(const key of oldFeeds)assert.ok(catalog.supplemental.some(r=>r.source_key===key));
 assert.equal(catalog.supplemental.length,17);
 const wiring=require('../data/engine-wiring.json');assert.equal(wiring.wired.filter(row=>row.page==='signal-board.html').length,0,'source references must not be recorded as rendered jh-wire cards');
 const html=fs.readFileSync(path.join(__dirname,'../signal-board.html'),'utf8');assert.ok(html.includes('/jh-fifx-board.js'));assert.ok(!html.includes('jh-page-ai.js'));assert.ok(!html.includes('AI commentary loading'));assert.ok(html.includes('00:15, 06:15, 12:15 and 18:15 UTC'));
});

test('native inventory binds complete view, registry and all capture outcomes without granting authority',async()=>{
 const bound=await api.bindNative(native.packet,catalog,transport,nativeWire(),webcrypto);
 assert.equal(bound.binding_checked,true);assert.equal(bound.complete_source_replay,false);assert.equal(bound.original_provider_verified,false);
 const rows=api.inventory(native.packet,catalog);assert.equal(rows.length,99);assert.equal(rows.filter(r=>r.original).length,96);
 assert.ok(rows.every(r=>r.calls_eligible===false));assert.ok(api.sourceLink(rows.find(r=>r.original)).includes('Inspect retained input'));
});
test('tampered view, missing input and changed head are rejected before display',async()=>{
 const run=JSON.parse(native.artifacts[native.packet.replay.manifest_key]);
 for(const key of [native.packet.replay.manifest_key,run.view.key,run.input.key]){
   const data={...native.artifacts,[key]:native.artifacts[key]+' '};
   await assert.rejects(api.bindNative(native.packet,catalog,transport,nativeWire(data),webcrypto));
 }
 const p=copy(native.packet);p.engines[0].signal=2;assert.throws(()=>api.inventory(p,catalog));
 p.engines[0].signal=null;p.calls_eligible=true;assert.throws(()=>api.inventory(p,catalog));
 await assert.rejects(api.bindNative(p,catalog,transport,nativeWire(),webcrypto));
});
test('whole native retained input is hash checked including empty HTTP error responses',async()=>{
 for(const row of Object.values(native.packet.sources).filter(r=>r.original)){
   const raw=await api.retained(row.original,'originals',transport,nativeWire(),webcrypto,'bin');assert.equal(raw.length,row.original.bytes);
 }
 const row=Object.values(native.packet.sources).find(r=>r.original?.bytes>0),data={...native.artifacts,[row.original.key]:'tampered'};
 await assert.rejects(api.retained(row.original,'originals',transport,nativeWire(data),webcrypto,'bin'));
 for(const key of ['data/pm-decision.json','audit-private/x','https://evil.invalid'])assert.throws(()=>api.artifact({...row.original,key},'originals','bin'));
});
test('native page inspects complete retained bytes and withholds malformed bindings',async()=>{
 const doc=document(),old=setInterval;globalThis.setInterval=()=>1;
 try{
   const app=api.mount(doc,transport,nativeWire(),webcrypto);await app.refresh();assert.equal(doc.els['sb-content'].hidden,false);assert.match(doc.els['sb-status'].textContent,/bound derived inventory/);
   const index=native.packet.engines.findIndex(row=>native.packet.sources[row.source_key].original?.bytes>0);
   await doc.els['sb-table'].handlers.click({target:{closest:selector=>selector==='[data-sb-source]'?{dataset:{sbSource:String(index)}}:null}});
   assert.equal(doc.els['sb-detail'].textContent,native.artifacts[native.packet.sources[native.packet.engines[index].source_key].original.key]);
   assert.match(doc.els['sb-detail-note'].textContent,/complete bytes/);app.destroy();
   const bad=api.mount(doc,transport,nativeWire({...native.artifacts,[native.packet.replay.manifest_key]:'{}'}),webcrypto);await bad.refresh();assert.equal(doc.els['sb-content'].hidden,true);bad.destroy();
 }finally{globalThis.setInterval=old;}
});
