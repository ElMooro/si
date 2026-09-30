const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {webcrypto,createHash}=require('node:crypto'),api=require('../jh-fifx-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/fifx-browser-whole-inputs.json'),'utf8'));
const sha=raw=>createHash('sha256').update(raw).digest('hex'),tick=()=>new Promise(r=>setImmediate(r));
const current='/data/fifx-vol.json?exact=1&nogen=1';
function wire(){
 const reads=[];let packet=structuredClone(fixture.first);
 const objects=Object.fromEntries(Object.entries(fixture.objects).map(([key,raw])=>[key,Buffer.from(raw,'base64')]));
 const fetcher=async(url,options={})=>{
  assert.ok(url===current||/^\/data\/fifx-vol-research\/(runs|inputs|views|series|receipts|originals)\/[a-f0-9]{64}\.(json|bin)\?exact=1&nogen=1$/.test(url),'Unreviewed test request '+url);
  assert.equal(options.credentials,'omit');reads.push(url);
  const raw=url===current?Buffer.from(JSON.stringify(packet)):objects[url.slice(1).split('?')[0]];
  assert.ok(raw,'Missing complete invented object');return new Response(raw,{status:200});
 };
 return {fetcher,reads,objects,get packet(){return packet;},set packet(value){packet=structuredClone(value);}};
}
function environment(){
 const elements={},timers=new Map(),listeners=new Map();let next=0;
 const doc={getElementById(id){if(!elements[id]){let html='';elements[id]={hidden:false,value:'',textContent:'',disabled:false,writes:0,handlers:{},addEventListener(name,fn){this.handlers[name]=fn;}};Object.defineProperty(elements[id],'innerHTML',{get:()=>html,set:value=>{html=value;elements[id].writes++;}});}return elements[id];}};
 doc.getElementById('fx-kind').value='original';
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
function reboundSource(packet,raw){const digest=sha(raw);packet.series.DGS10.complete_source_artifact={key:'data/fifx-vol-research/series/'+digest+'.json',sha256:digest,bytes:raw.length};}

test('whole hash-bound source rejects duplicate identities, nonfinite values and invalid Unicode',async()=>{
 const w=wire(),base=w.objects[w.packet.series.DGS10.complete_source_artifact.key];
 for(const prefix of ['{"source_id":"INVENTED CONFLICT",','{"invented_overflow":1e999,','{"invented_surrogate":"\\ud800",',Buffer.from([123,34,120,34,58,34,0xff,34,44])]){
  const packet=structuredClone(w.packet),raw=Buffer.concat([Buffer.from(prefix),base.subarray(1)]);reboundSource(packet,raw);
  await assert.rejects(api.verifySeries(packet,'DGS10',async()=>new Response(raw),webcrypto));
 }
});

test('complete HTTP response status and absent range are required before opening the body',async()=>{
 for(const change of [{status:206},{status:201},{headers:new Headers({'Content-Range':'bytes 0-1/20'})}]){
  const record={cancelled:0,readers:0,released:0},response={...bodyResponse(Buffer.from('{}'),record),...change};
  await assert.rejects(api.bytes('/invented',async()=>response),/unavailable/);assert.deepEqual(record,{cancelled:1,readers:0,released:0});
 }
});

test('a response arriving after deadline is cancelled without reading its late body',async()=>{
 let resolve;const record={cancelled:0,readers:0,released:0};
 const pending=api.bytes('/invented',()=>new Promise(r=>resolve=r),100,5);await assert.rejects(pending,/timed out/);
 resolve(bodyResponse(Buffer.from('{}'),record));await tick();assert.deepEqual(record,{cancelled:1,readers:0,released:0});
});

test('external cancellation closes late bodies without depending on fetch cooperation',async()=>{
 let resolve;const control=new AbortController(),record={cancelled:0,readers:0,released:0};
 const pending=api.bytes('/invented',()=>new Promise(r=>resolve=r),100,15000,{signal:control.signal});control.abort();await assert.rejects(pending,{name:'AbortError'});
 resolve(bodyResponse(Buffer.from('{}'),record));await tick();assert.deepEqual(record,{cancelled:1,readers:0,released:0});
});

test('every fragmented decoded source byte is bound independently of compressed wire metadata',async()=>{
 const w=wire(),key=w.packet.series.DGS10.complete_source_artifact.key,raw=w.objects[key];
 const fetcher=async()=>new Response(new ReadableStream({start(c){for(let i=0;i<raw.length;i+=7)c.enqueue(new Uint8Array(raw.subarray(i,i+7)));c.close();}}),{headers:{'Content-Encoding':'gzip','Content-Length':'7'}});
 const result=await api.verifySeries(w.packet,'DGS10',fetcher,webcrypto);assert.equal(result.original_rows.length,42);
 for(const size of [raw.length-1,raw.length+1]){
  const packet=structuredClone(w.packet);packet.series.DGS10.complete_source_artifact.bytes=size;
  await assert.rejects(api.verifySeries(packet,'DGS10',fetcher,webcrypto),/bytes|bound/);
 }
});

test('both complete native publications retain all 18 sources, originals and missing states',async()=>{
 const w=wire();
 for(const packet of [fixture.first,fixture.second]){
  w.packet=packet;const run=await api.verifyView(w.packet,w.fetcher,webcrypto);let rows=0;
  for(const sid of api.IDS){
   const source=await api.verifySeries(w.packet,sid,w.fetcher,webcrypto),proof=await api.verifyOriginal(w.packet,run,sid,w.fetcher,webcrypto);
   rows+=source.original_rows.length;assert.equal(source.source_id,sid);
   if(sid==='DEXJPUS'){assert.equal(proof.missing,true);assert.equal(source.original_rows.length,0);}
   else{assert.equal(proof.sha256,source.original_sha256);assert.equal(proof.acquired_at,source.receipt.acquired_at);assert.equal(proof.bytes,source.original_bytes);}
  }
  assert.equal(rows,714);assert.equal(w.packet.series['^MOVE'].quality.status,'identity_mismatch');
  assert.equal(w.packet.calls_eligible,false);assert.equal(w.packet.sizing_eligible,false);assert.equal(w.packet.decision.verb,'WAIT');
 }
});

test('a new publication discards old source proof before displaying a newly verified identity',async()=>{
 await mounted(async({e,w,app})=>{
  await e.elements['fx-original'].handlers.click();assert.ok(e.elements['fx-history-status'].textContent.includes(fixture.first.series.DGS10.original_sha256));
  w.packet=fixture.second;await app.refresh();assert.equal(e.elements['fx-history-status'].textContent,'');assert.equal(e.elements['fx-history'].innerHTML,'');
  assert.ok(e.elements['fx-metadata'].textContent.includes(fixture.second.generated_at));
  await e.elements['fx-original'].handlers.click();assert.ok(e.elements['fx-history-status'].textContent.includes(fixture.second.series.DGS10.original_sha256));
  assert.ok(!e.elements['fx-history-status'].textContent.includes(fixture.first.series.DGS10.original_sha256));
 });
});

test('an older failed history request cannot clear a newer successful original verification',async()=>{
 let reject,hold=true;
 await mounted(async({e})=>{
  const old=e.elements['fx-load'].handlers.click();while(!reject)await tick();
  await e.elements['fx-original'].handlers.click();await old;const notice=e.elements['fx-history-status'].textContent;
  assert.match(notice,/acquisition receipt verified/);reject(Error('invented old failure'));await tick();
  assert.equal(e.elements['fx-history-status'].textContent,notice);assert.equal(e.elements['fx-native'].hidden,false);
 },w=>async(url,options)=>{
  if(hold&&url.startsWith('/'+w.packet.series.DGS10.complete_source_artifact.key+'?')){hold=false;return new Promise((_,r)=>reject=r);}
  return w.fetcher(url,options);
 });
});

test('selection away and back cannot resurrect an older action for the same source',async()=>{
 let release,hold=true;const record={cancelled:0,readers:0,released:0};
 await mounted(async({e,w})=>{
  const pending=e.elements['fx-load'].handlers.click();while(!release)await tick();
  e.elements['fx-series'].value='VIXCLS';e.elements['fx-series'].handlers.change();e.elements['fx-series'].value='DGS10';e.elements['fx-series'].handlers.change();await pending;
  release(bodyResponse(w.objects[w.packet.series.DGS10.complete_source_artifact.key],record));await tick();
  assert.equal(e.elements['fx-history-status'].textContent,'');assert.equal(e.elements['fx-history'].innerHTML,'');assert.equal(record.cancelled,1);assert.equal(record.readers,0);
  await e.elements['fx-load'].handlers.click();assert.match(e.elements['fx-pagination'].textContent,/1–42 of 42/);
 },w=>async(url,options)=>{if(hold&&url.startsWith('/'+w.packet.series.DGS10.complete_source_artifact.key+'?')){hold=false;return new Promise(r=>release=r);}return w.fetcher(url,options);});
});

test('changing history kind cancels an older action and keeps existing inspected rows',async()=>{
 let release,hold=false;const record={cancelled:0,readers:0,released:0};
 await mounted(async({e,w})=>{
  await e.elements['fx-load'].handlers.click();hold=true;const pending=e.elements['fx-original'].handlers.click();while(!release)await tick();
  e.elements['fx-kind'].value='calculated';e.elements['fx-kind'].handlers.change();await pending;
  const notice=e.elements['fx-pagination'].textContent;assert.match(notice,/of 12 retained rows/);
  release(bodyResponse(w.objects[w.packet.series.DGS10.complete_source_artifact.key],record));await tick();assert.equal(e.elements['fx-history-status'].textContent,'');assert.equal(e.elements['fx-pagination'].textContent,notice);
 },w=>async(url,options)=>{if(hold&&url.startsWith('/'+w.packet.series.DGS10.complete_source_artifact.key+'?')){hold=false;return new Promise(r=>release=r);}return w.fetcher(url,options);});
});

test('unchanged age ticks preserve open disclosures while expiry removes current readings',async()=>{
 await mounted(async({e})=>{
  const reading=e.elements['fx-reading'],count=reading.writes;for(const fn of e.timers.values())fn();assert.equal(reading.writes,count);
  Date.now=()=>Date.parse(fixture.first.generated_at)+27*3600000;for(const fn of e.timers.values())fn();assert.ok(reading.writes>count);assert.match(e.elements['fx-status'].textContent,/0 \/ 18/);
 });
});

test('page suspension clears evidence, cancels reads and resumes only one refresh clock',async()=>{
 await mounted(async({e,w,app})=>{
  await e.elements['fx-original'].handlers.click();assert.equal(e.timers.size,1);e.listeners.get('pagehide')();assert.equal(e.timers.size,0);
  for(const id of ['reading','metadata','identity','window','history','chart','pagination','history-status'])assert.equal(e.elements['fx-'+id].innerHTML||e.elements['fx-'+id].textContent,'',id);
  const before=w.reads.length;await e.listeners.get('pageshow')();assert.equal(e.timers.size,1);assert.ok(w.reads.length>before);assert.equal(e.elements['fx-native'].hidden,false);
  await e.listeners.get('pageshow')();assert.equal(e.timers.size,1);app.destroy();assert.equal(e.timers.size,0);assert.equal(e.listeners.size,0);
  const after=w.reads.length;await app.refresh();assert.equal(w.reads.length,after);
 });
});

test('duplicate current packet identities are rejected before any archive request',async()=>{
 await mounted(async({e,w})=>{assert.equal(e.elements['fx-native'].hidden,true);assert.match(e.elements['fx-status'].textContent,/withheld/);assert.equal(w.reads.length,0);},w=>async url=>{assert.equal(url,current);return new Response('{"contract":"fifx-vol-research.v1",'+JSON.stringify(w.packet).slice(1));});
});

test('every source consumer loads complete evidence IO before the FI/FX helper',()=>{
 for(const page of ['fifx-vol.html','bond-desk.html','signal-board.html','ici-flows.html']){
  const html=fs.readFileSync(path.join(__dirname,'..',page),'utf8');assert.ok(html.indexOf('/jh-evidence-io.js')>=0);assert.ok(html.indexOf('/jh-evidence-io.js')<html.indexOf('/jh-fifx-research.js'));
 }
});
