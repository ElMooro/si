const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{webcrypto}=require('node:crypto');
const api=require('../jh-signal-board-research.js'),transport=require('../jh-fifx-research.js'),catalog=require('../assets/signal-board-registry.json');
const copy=v=>JSON.parse(JSON.stringify(v));
const packet=()=>({generated_at:'2026-09-26T06:15:00Z',n_engines:99,composite_signal:1.8,composite_posture:'STRONG RISK-ON',deep_read:{lean:'BUY'},engines:catalog.feeds.map((r,i)=>({engine:r.engine,category:r.category,signal:i%5-2,signal_label:'STRONG RISK-ON',read:'unvalidated narrative',as_of:i===0?null:'2026-09-25T06:00:00Z',stale:i%3===0}))});
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
 doc.els['sb-table'].handlers.click({target:{closest:()=>({dataset:{sbRow:'0'}})}});assert.match(doc.els['sb-detail'].textContent,/unvalidated narrative/);assert.match(doc.els['sb-detail-note'].textContent,/Unqualified decoded row/);
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
