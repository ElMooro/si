const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {webcrypto,createHash}=require('node:crypto'),api=require('../jh-term-premium-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/term-premium-browser-whole-inputs.json'),'utf8'));
const sha=raw=>createHash('sha256').update(raw).digest('hex'),tick=()=>new Promise(r=>setImmediate(r));
const current='/data/term-premium.json?exact=1&nogen=1';
function wire(){
 const reads=[];let packet=structuredClone(fixture.first);
 const objects=Object.fromEntries(Object.entries(fixture.objects).map(([key,raw])=>[key,Buffer.from(raw,'base64')]));
 const fetcher=async(url,options={})=>{
  assert.ok(url===current||/^\/data\/term-premium-research\/(runs|inputs|views|tables|sources|originals)\/[a-f0-9]{64}\.(json|xls)\?exact=1&nogen=1$/.test(url),'Unreviewed test request '+url);
  assert.equal(options.credentials,'omit');reads.push(url);
  const raw=url===current?Buffer.from(JSON.stringify(packet)):objects[url.slice(1).split('?')[0]];
  assert.ok(raw,'Missing complete invented object');return new Response(raw,{status:200});
 };
 return {fetcher,reads,objects,get packet(){return packet;},set packet(value){packet=structuredClone(value);}};
}
function environment(){
 const elements={},timers=new Map(),listeners=new Map();let next=0;
 const doc={getElementById(id){if(!elements[id]){let html='';elements[id]={hidden:false,value:'',textContent:'',disabled:false,writes:0,querySelectorAll:()=>[]};Object.defineProperty(elements[id],'innerHTML',{get:()=>html,set:value=>{html=value;elements[id].writes++;}});}return elements[id];}};
 doc.getElementById('term-frequency').value='D';doc.getElementById('term-family').value='ACMTP';doc.getElementById('term-tenor').value='10';
 const saved=Object.fromEntries(['setInterval','clearInterval','addEventListener','removeEventListener'].map(k=>[k,globalThis[k]])),now=Date.now;
 globalThis.setInterval=fn=>{timers.set(++next,fn);return next;};globalThis.clearInterval=id=>timers.delete(id);
 globalThis.addEventListener=(name,fn)=>listeners.set(name,fn);globalThis.removeEventListener=(name,fn)=>{if(listeners.get(name)===fn)listeners.delete(name);};
 Date.now=()=>Date.parse(fixture.second.generated_at);
 return {doc,elements,timers,listeners,restore(){for(const [key,value] of Object.entries(saved)){if(value===undefined)delete globalThis[key];else globalThis[key]=value;}Date.now=now;}};
}
async function mounted(work,override){
 const e=environment(),w=wire();let app;
 try{app=api.mount(e.doc,override?override(w):w.fetcher,webcrypto);await app.refresh();await work({e,w,app});}
 finally{app?.destroy();e.restore();}
}
function bodyResponse(raw,record){
 return {ok:true,status:200,headers:new Headers(),body:{cancel(){record.cancelled++;return Promise.resolve();},getReader(){record.readers++;let sent=false;return {async read(){if(sent)return {done:true};sent=true;return {done:false,value:new Uint8Array(raw)};},cancel(){record.cancelled++;return Promise.resolve();},releaseLock(){record.released++;}};}}};
}

test('matching hashes cannot make duplicate, nonfinite or invalid UTF-8 JSON acceptable',async()=>{
 for(const raw of [Buffer.from('{"whole":1,"whole":2}'),Buffer.from('{"whole":1e999}'),Buffer.from('{"whole":"\\ud800"}'),Buffer.from([123,34,120,34,58,34,0xff,34,125])]){
  const ref={key:'data/term-premium-research/views/'+sha(raw)+'.json',sha256:sha(raw),bytes:raw.length};
  await assert.rejects(api.artifact(ref,'views',async()=>new Response(raw),webcrypto));
 }
});

test('partial HTTP status and ranges are refused and their bodies are closed',async()=>{
 for(const change of [{status:206},{headers:new Headers({'Content-Range':'bytes 0-1/20'})}]){
  const record={cancelled:0,readers:0,released:0},response={...bodyResponse(Buffer.from('{}'),record),...change};
  await assert.rejects(api.bytes('/invented',async()=>response),/unavailable/);assert.equal(record.cancelled,1);assert.equal(record.readers,0);
 }
});

test('a response arriving after timeout is closed without opening its reader',async()=>{
 let resolve;const record={cancelled:0,readers:0,released:0};
 const pending=api.bytes('/invented',()=>new Promise(r=>resolve=r),100,5);
 await assert.rejects(pending,/timed out/);resolve(bodyResponse(Buffer.from('{}'),record));await tick();
 assert.deepEqual(record,{cancelled:1,readers:0,released:0});
});

test('known complete decoded length is checked independently of compressed wire metadata',async()=>{
 const raw=Buffer.from('{"whole_invented":true}'),ref={key:'data/term-premium-research/views/'+sha(raw)+'.json',sha256:sha(raw),bytes:raw.length};
 const fetcher=async()=>new Response(new ReadableStream({start(c){for(const byte of raw)c.enqueue(Uint8Array.of(byte));c.close();}}),{headers:{'Content-Encoding':'gzip','Content-Length':'7'}});
 assert.deepEqual(await api.artifact(ref,'views',fetcher,webcrypto),{whole_invented:true});
 for(const size of [raw.length-1,raw.length+1])await assert.rejects(api.artifact({...ref,bytes:size},'views',fetcher,webcrypto),/bytes|bound/);
});

test('caller cancellation closes a late response and does not borrow timeout cooperation',async()=>{
 let resolve;const control=new AbortController(),record={cancelled:0,readers:0,released:0};
 const pending=api.bytes('/invented',()=>new Promise(r=>resolve=r),100,15000,{signal:control.signal});
 control.abort();await assert.rejects(pending,{name:'AbortError'});
 resolve(bodyResponse(Buffer.from('{}'),record));await tick();assert.equal(record.cancelled,1);assert.equal(record.readers,0);
});

test('complete invented native views, both worksheets and original workbook remain inspectable',async()=>{
 const w=wire(),manifest=await api.verifyView(w.packet,w.fetcher,webcrypto);
 const daily=await api.verifyTable(w.packet,'ACM Daily',w.fetcher,webcrypto),monthly=await api.verifyTable(w.packet,'ACM Monthly',w.fetcher,webcrypto);
 assert.equal(daily.rows.length,300);assert.equal(monthly.rows.length,20);assert.equal(daily.headers.length,31);
 const source=await api.verifyOriginal(w.packet,manifest,w.fetcher,webcrypto);assert.equal(source.sha256,w.packet.source.sha256);assert.equal(source.bytes,w.packet.source.bytes);
});

test('a new publication clears the old original-workbook proof before displaying new evidence',async()=>{
 await mounted(async({e,w,app})=>{
  await e.elements['term-verify-source'].onclick();assert.ok(e.elements['term-history-status'].textContent.includes(fixture.first.source.sha256));
  w.packet=fixture.second;await app.refresh();assert.equal(e.elements['term-history-status'].textContent,'');
  assert.ok(e.elements['term-metadata'].innerHTML.includes(fixture.second.generated_at));
  await e.elements['term-verify-source'].onclick();assert.ok(e.elements['term-history-status'].textContent.includes(fixture.second.source.sha256));
  assert.ok(!e.elements['term-history-status'].textContent.includes(fixture.first.source.sha256));
 });
});

test('changing the selected frequency cancels pending history without restoring the old notice',async()=>{
 let release,hold=true;const record={cancelled:0,readers:0,released:0};
 await mounted(async({e,w})=>{
  const pending=e.elements['term-load-history'].onclick();await tick();
  e.elements['term-frequency'].value='M';e.elements['term-frequency'].onchange();await pending;
  const key=w.packet.tables['ACM Daily'].complete_table_artifact.key;release(bodyResponse(w.objects[key],record));await tick();
  assert.equal(e.elements['term-history-status'].textContent,'');assert.equal(record.cancelled,1);assert.equal(record.readers,0);
  hold=false;await e.elements['term-load-history'].onclick();assert.match(e.elements['term-history-status'].textContent,/ACM Monthly.*20 original rows/);assert.match(e.elements['term-pagination'].textContent,/1–20 of 20/);
 },w=>async(url,options)=>hold&&url.startsWith('/'+w.packet.tables['ACM Daily'].complete_table_artifact.key+'?')?new Promise(r=>release=r):w.fetcher(url,options));
});

test('an older failed original verification cannot erase a later successful worksheet result',async()=>{
 let release;const record={cancelled:0,readers:0,released:0};
 await mounted(async({e})=>{
  const original=e.elements['term-verify-source'].onclick();while(!release)await tick();
  await e.elements['term-load-history'].onclick();await original;
  const notice=e.elements['term-history-status'].textContent;assert.match(notice,/ACM Daily.*300 original rows/);
  release({...bodyResponse(Buffer.from('failure'),record),ok:false,status:503});await tick();
  assert.equal(e.elements['term-history-status'].textContent,notice);assert.equal(e.elements['term-native'].hidden,false);assert.equal(record.cancelled,1);
 },w=>async(url,options)=>url.includes('/originals/')?new Promise(r=>release=r):w.fetcher(url,options));
});

test('unchanged age ticks preserve open content while an actual expiry updates availability',async()=>{
 await mounted(async({e})=>{
  const reading=e.elements['term-reading'],count=reading.writes;for(const fn of e.timers.values())fn();assert.equal(reading.writes,count);
  Date.now=()=>Date.parse(fixture.first.generated_at)+27*3600000;for(const fn of e.timers.values())fn();
  assert.ok(reading.writes>count);assert.match(e.elements['term-status'].textContent,/0\/60/);
 });
});

test('page suspension clears bound evidence and resumes exactly one clock and fresh verification',async()=>{
 await mounted(async({e,w,app})=>{
  await e.elements['term-verify-source'].onclick();assert.equal(e.timers.size,1);
  e.listeners.get('pagehide')();assert.equal(e.timers.size,0);assert.equal(e.elements['term-history-status'].textContent,'');assert.equal(e.elements['term-reading'].innerHTML,'');
  const before=w.reads.length;await e.listeners.get('pageshow')();assert.equal(e.timers.size,1);assert.ok(w.reads.length>before);assert.equal(e.elements['term-native'].hidden,false);
  await e.listeners.get('pageshow')();assert.equal(e.timers.size,1);
  app.destroy();assert.equal(e.timers.size,0);assert.equal(e.listeners.size,0);const after=w.reads.length;await app.refresh();assert.equal(w.reads.length,after);
 });
});

test('duplicate identities in a whole current packet are rejected before requesting its manifest',async()=>{
 await mounted(async({e,w})=>{
  assert.equal(e.elements['term-native'].hidden,true);assert.match(e.elements['term-status'].textContent,/withheld/);assert.equal(w.reads.length,0);
 },w=>async url=>{assert.equal(url,current);return new Response('{"contract":"term-premium-research.v1",'+JSON.stringify(w.packet).slice(1));});
});
