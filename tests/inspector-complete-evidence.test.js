const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),vm=require('node:vm');
const io=require('../jh-evidence-io.js'),scenario=require('../jh-portfolio-scenario-io.js'),inspector=require('../jh-data-inspector.js'),gzip=require('node:zlib').gzipSync;
const tick=()=>new Promise(setImmediate),pause=ms=>new Promise(r=>setTimeout(r,ms));
const entry={engine:'synthetic',access:'public',key:'data/synthetic-only.json'},headers={'X-JH-Artifact-Key':entry.key};

test('plain and gzip evidence reject duplicate decoded keys, overflow and invalid Unicode',async()=>{
 const bad=['{"nav":1000,"nav":2000}','{"a":1,"\\u0061":2}','{"x":{"n":0,"n":null}}','{"n":1e400}','{"a":"\\ud800"}','{"a":"\\udfff"}','[1,]','{} true','\ufeff{}','['.repeat(130)+'0'+']'.repeat(130)];
 for(const text of bad)for(const raw of [text,gzip(text)])await assert.rejects(inspector.decodeArtifactResponse(new Response(raw)),undefined,text);
 for(const raw of [Buffer.from([0xc3,0x28]),gzip(Buffer.from([0xc3,0x28]))])await assert.rejects(inspector.decodeArtifactResponse(new Response(raw)));
});

test('all valid values, escaped names, Unicode and prototype-like keys survive complete decoding',async()=>{
 const text='{"__proto__":{"safe":true},"constructor":false,"zero":0,"negative":-0,"missing":null,"空/😀~":[1,false,"\\u0061"],"empty":{},"rows":[null,{"later":0}]}';
 for(const raw of [text,gzip(text)]){
  const got=await inspector.decodeArtifactResponse(new Response(raw));assert.deepEqual(got,JSON.parse(text));assert.equal(Object.getPrototypeOf(got),Object.prototype);assert.equal({}.safe,undefined);
  assert.ok(Object.is(got.negative,-0));assert.ok(inspector.leafPaths(got).includes('/rows/1/later'));
 }
 assert.equal(scenario.strictJSON,io.strictJSON);
});

test('evidence reads continue through short chunks to EOF and enforce exact bounds without slicing',async()=>{
 const text='{"late":{"value":0},"empty":null}',raw=Buffer.from(text);let i=0,cancelled=0;
 const response=new Response(new ReadableStream({pull(c){if(i===raw.length)c.close();else c.enqueue(raw.subarray(i,++i));},cancel(){cancelled++;}}));
 assert.deepEqual(await inspector.decodeArtifactResponse(response,{limit:raw.length}),JSON.parse(text));
 await assert.rejects(inspector.decodeArtifactResponse(new Response(raw),{limit:raw.length-1}),/bound/);
 assert.equal(cancelled,0); // Clean EOF is not a source failure.
});

test('gzip expansion is independently bounded and corrupt/truncated gzip never produces evidence',async()=>{
 const raw=Buffer.from(JSON.stringify({large:'a'.repeat(10000)})),zip=gzip(raw);assert.ok(zip.length<200);
 await assert.rejects(inspector.decodeArtifactResponse(new Response(zip),{limit:200}),/bound/);
 assert.deepEqual(await inspector.decodeArtifactResponse(new Response(zip),{limit:raw.length}),JSON.parse(raw));
 for(const invalid of [zip.subarray(0,-1),Buffer.concat([zip.subarray(0,-8),Buffer.alloc(8)])])await assert.rejects(inspector.decodeArtifactResponse(new Response(invalid)));
});

test('gzip processing shares the header/body time budget instead of starting a new deadline',async()=>{
 let now=0;const scope={module:{exports:{}},require:()=>io,performance:{now:()=>now},Blob,Response,DecompressionStream,Date};
 vm.runInNewContext(fs.readFileSync(require.resolve('../jh-data-inspector.js'),'utf8'),scope);
 await assert.rejects(scope.module.exports.loadArtifact(entry,async()=>{now=21;return new Response(gzip('{}'),{headers});},null,{timeoutMs:20}),/timed out/);
});

test('invalid bounds and deadlines cannot open an artifact source',async()=>{
 let calls=0;
 for(const options of [{limit:true},{limit:32*1024*1024+1},{limit:0},{timeoutMs:false},{timeoutMs:60001},{timeoutMs:0}])await assert.rejects(inspector.loadArtifact(entry,()=>{calls++;},null,options));
 assert.equal(calls,0);
});

test('header and body stalls time out, abort request signals, cancel late bodies and release readers',async()=>{
 let resolve,signal,cancelled=0;
 const pending=inspector.loadArtifact(entry,(_url,options)=>{signal=options.signal;return new Promise(r=>resolve=r);},null,{timeoutMs:5});
 await assert.rejects(pending,/timed out/);assert.equal(signal.aborted,true);
 resolve({ok:true,headers:new Headers(headers),body:{cancel(){cancelled++;}}});await tick();assert.equal(cancelled,1);
 let released=0;
 const body={getReader:()=>({read:()=>new Promise(()=>{}),cancel(){cancelled++;},releaseLock(){released++;}})};
 await assert.rejects(inspector.decodeArtifactResponse({ok:true,body},{timeoutMs:5}),/timed out/);assert.equal(cancelled,2);assert.equal(released,1);
});

test('external cancellation rejects immediately and cancels only an inspector-owned body',async()=>{
 const controller=new AbortController();let cancelled=0;
 const body={getReader:()=>({read:()=>new Promise(()=>{}),cancel(){cancelled++;},releaseLock(){}})};
 const pending=inspector.decodeArtifactResponse({ok:true,body},{signal:controller.signal});await tick();controller.abort();
 await assert.rejects(pending,{name:'AbortError'});assert.equal(cancelled,1);
 let opened=0;await assert.rejects(inspector.loadArtifact(entry,()=>{opened++;},null,{signal:controller.signal}),{name:'AbortError'});assert.equal(opened,0);
});

test('identity failures cancel unread public responses without an alternate request or route',async()=>{
 let calls=0,cancelled=0;
 await assert.rejects(inspector.loadArtifact(entry,async()=>{calls++;return {ok:true,headers:new Headers({'X-JH-Artifact-Key':'data/other.json'}),body:{cancel(){cancelled++;}}};}),/identity/);
 assert.equal(calls,1);assert.equal(cancelled,1);
});

test('registry rejects duplicate ownership even when its complete bytes match the pinned hash',async()=>{
 const raw='{"pages":{"one":1,"one":2}}',hash=crypto.createHash('sha256').update(raw).digest('hex');
 await assert.rejects(inspector.fetchRegistry(async()=>new Response(raw),'a'.repeat(40),hash),/Duplicate/);
 let cancelled=0;
 await assert.rejects(inspector.fetchRegistry(async()=>({ok:true,body:{getReader:()=>({read:async()=>({value:new Uint8Array(16*1024*1024+1),done:false}),cancel(){cancelled++;},releaseLock(){}})}}),null,null),/bound/);
 assert.equal(cancelled,1);
});

test('shared scenario API keeps its original four-MiB bound after extraction',async()=>{
 let calls=0;
 for(const limit of [4*1024*1024+1,true,0,Infinity]){
  await assert.rejects(scenario.readComplete(()=>{calls++;},{limit}),/bound/);assert.throws(()=>scenario.decode(new Uint8Array(),limit),/bound/);
 }
 assert.equal(calls,0);assert.equal(scenario.LIMIT,4*1024*1024);
 const raw=await io.readComplete(()=>new Response('{}'),{limit:16*1024*1024});assert.deepEqual(io.decode(raw),{});
});

function observer(fetcher,owner=()=>({uid:null,epoch:0})){
 const records=[],contract={engine:'synthetic',origin:'https://synthetic.invalid',pathname:'/read',methods:['GET']};
 return {records,wrapped:inspector.observeResponses(fetcher,[contract],r=>records.push(r),owner)};
}
test('latest request wins even when an older header arrives after a newer complete result',async()=>{
 let resolve,calls=0;const old=new Promise(r=>resolve=r);
 const {records,wrapped}=observer(()=>++calls===1?old:Promise.resolve(new Response('{"new":0}')));
 const pending=wrapped('https://synthetic.invalid/read?t=1');const newer=await wrapped('https://synthetic.invalid/read?t=2');await newer.json();await pause(20);
 resolve(new Response('{"old":1}'));const previous=await pending;await previous.json();await pause(20);
 assert.equal(calls,2);assert.equal(records.length,1);assert.deepEqual(records[0].payload,{new:0});
});

test('a failed latest capture replaces previous evidence while preserving the original caller response',async()=>{
 const cases=[()=>new Response('{"value":1,"value":2}'),()=>new Response('down',{status:503}),()=>{throw Error('synthetic network failure');}];
 for(const fail of cases){
  let count=0;const {records,wrapped}=observer(async()=>++count===1?new Response('{"value":0}'):fail());
  await(await wrapped('https://synthetic.invalid/read')).text();await pause(20);assert.equal(records[0].payload.value,0);
  try{const received=await wrapped('https://synthetic.invalid/read');assert.ok(received instanceof Response);await received.text();}catch(error){assert.match(error.message,/synthetic network/);}
  await pause(20);assert.equal(records.length,2);assert.equal(records[1].unavailable,true);assert.equal(records[1].payload,null);assert.equal(count,2);
 }
});

test('malformed previous-owner capture does not replace new-owner evidence or retain source diagnostics',async()=>{
 let owner={uid:'a',epoch:1},resolve;const records=[],contract={engine:'synthetic',origin:'https://synthetic.invalid',pathname:'/owner',owner_authenticated:true};
 const response=new Response(new ReadableStream({start(c){resolve=()=>{c.enqueue(Buffer.from('{"sensitive":1,"sensitive":2}'));c.close();};}}));
 const wrapped=inspector.observeResponses(async()=>response,[contract],r=>records.push(r),()=>({...owner}));
 await wrapped('https://synthetic.invalid/owner');owner={uid:'b',epoch:2};resolve();await response.text();await pause(20);assert.deepEqual(records,[]);
});

test('all five complete predecessor sources remain inert and hash-checked',()=>{
 const doc=JSON.parse(fs.readFileSync(path.join(__dirname,'../docs/audit/2026-09-29/shared-inspector-predecessor.json')));
 assert.equal(doc.sources.length,5);
 for(const row of doc.sources){const raw=fs.readFileSync(path.join(__dirname,'..',row.fixture));assert.equal(raw.length,row.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),row.sha256);}
});
