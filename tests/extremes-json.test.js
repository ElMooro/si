const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),vm=require('node:vm');
const api=require('../jh-extremes-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/extremes-native.json'),'utf8'));
const key='data/'+f.packet.engine+'.json',bytes=s=>new TextEncoder().encode(s);
const duplicate=raw=>' {"generated_at":"2000-01-01T00:00:00Z",'+raw.trim().slice(1);
const fetcher=async url=>new Response(f.artifacts[url.slice(1)],{status:200});
test('current packets cannot be verified after silently selecting the last conflicting clock',async()=>{
 const raw=duplicate(JSON.stringify(f.packet));await assert.rejects(api.load(key,async()=>new Response(raw)),/Duplicate JSON key/);
});
test('nested and escaped duplicate identities, nonfinite constants and overflow are rejected',async()=>{
 for(const raw of ['{"clock":1,"clo\\u0063k":2}','{"x":[{"same":0,"same":0}]}','{"x":1e999}','{"x":-1e999}','{"x":NaN}','{"x":Infinity}','{"x":-Infinity}'])await assert.rejects(api.load(key,async()=>new Response(raw)));
});
test('valid JSON retains ordinary types, exact response bytes and inert prototype names',async()=>{
 const raw=' {"__proto__":{"safe":true},"zero":0,"minus":-0.0,"large":1e308,"false":false,"null":null,"text":"NaN \\u2603","array":[1,{},[]]} \n';
 const out=await api.load(key,async()=>new Response(raw));assert.deepEqual(new Uint8Array(out.raw),bytes(raw));assert.deepEqual(out.doc,JSON.parse(raw));assert.equal(Object.getPrototypeOf(out.doc),Object.prototype);assert.equal({}.safe,undefined);assert.equal(out.doc.__proto__.safe,true);assert.equal(Object.is(out.doc.minus,-0),true);
});
test('truncated, trailing, invalid and excessively nested structures fail explicitly',async()=>{
 for(const raw of ['{}{}','{"x":true,}','{"x":01}','{"x":}','[1,]','{"x":"\\q"}','[ '.repeat(130)+'0'+' ]'.repeat(130)])await assert.rejects(api.load(key,async()=>new Response(raw)));
});
test('hash-consistent retained run cannot conceal a duplicate clock',async()=>{
 const p=structuredClone(f.packet),raw=duplicate(f.artifacts[p.replay.manifest_key]),digest=crypto.createHash('sha256').update(raw).digest('hex'),runkey='data/extremes-research/runs/'+digest+'.json';p.replay.manifest_key=runkey;
 await assert.rejects(api.verifyPacket(p,async url=>url==='/'+runkey?new Response(raw):fetcher(url)),/Duplicate JSON key/);
});
test('hash-consistent retained output cannot conceal duplicate identities',async()=>{
 const p=structuredClone(f.packet),m=JSON.parse(f.artifacts[p.replay.manifest_key]),raw=duplicate(f.artifacts[m.output.key]),digest=crypto.createHash('sha256').update(raw).digest('hex');
 m.output={key:'data/extremes-research/outputs/'+digest+'.json',sha256:digest,bytes:bytes(raw).byteLength};m.output_sha256=digest;p.replay.output_sha256=digest;
 const run=JSON.stringify(m),runkey='data/extremes-research/runs/'+crypto.createHash('sha256').update(run).digest('hex')+'.json';p.replay.manifest_key=runkey;
 await assert.rejects(api.verifyPacket(p,async url=>new Response(url==='/'+runkey?run:raw)),/Duplicate JSON key/);
});
test('ambiguous parser failures release the reader and a subsequent valid verification succeeds',async()=>{
 let read=0,cancelled=0,released=0;await assert.rejects(api.load(key,async()=>({ok:true,body:{getReader:()=>({read:async()=>read++?{done:true}:{done:false,value:bytes('{"x":0,"x":1}')},cancel:async()=>{cancelled++;},releaseLock:()=>{released++;}})}})),/Duplicate JSON key/);
 assert.equal(cancelled,1);assert.equal(released,1);assert.deepEqual(await api.verifyPacket(f.packet,fetcher),f.packet);
});
test('mounted research clears ambiguous data, exposes retry, and recovers with complete valid evidence',async()=>{
 const node={dataset:{extremesEngine:f.packet.engine},innerHTML:'',button:{},querySelector(){return this.button;}},doc={readyState:'complete',querySelectorAll(){return[node];},getElementById(){return null;}};
 let invalid=true,calls=[];const context={document:doc,crypto:crypto.webcrypto,Uint8Array,TextDecoder,AbortController,setTimeout,clearTimeout,console,fetch:async url=>{calls.push(url);return new Response(url==='/'+key?(invalid?duplicate(JSON.stringify(f.packet)):JSON.stringify(f.packet)):f.artifacts[url.slice(1)]);}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-extremes-research.js'),'utf8'),context);
 async function until(predicate){for(let n=0;n<100&&!predicate();n++)await new Promise(done=>setTimeout(done,2));assert.ok(predicate(),node.innerHTML.slice(0,200));}
 await until(()=>node.innerHTML.includes('Retry verification'));assert.doesNotMatch(node.innerHTML,/Dated measurements/);assert.deepEqual(calls,['/'+key]);
 invalid=false;await node.button.onclick();assert.match(node.innerHTML,/Dated measurements/);assert.match(node.innerHTML,/zero qualified investment votes/);
 invalid=true;await node.button.onclick();assert.match(node.innerHTML,/Retry verification/);assert.doesNotMatch(node.innerHTML,/Dated measurements/);
});
