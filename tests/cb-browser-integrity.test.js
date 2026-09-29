const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const api=require('../jh-cb-research.js'),root=path.join(__dirname,'..');
const stamp='2026-03-02T11:00:00Z',now=Date.parse('2026-03-02T12:00:00Z'),key=api.PREFIX+'outputs/'+'a'.repeat(64)+'.json';
function packet(){return {contract:api.CONTRACT,generated_at:stamp,source_generated_at:stamp,call:null,calls_eligible:false,sizing_eligible:false,measurements:{},central_banks:[],fx_context:{},quality:{},decision:{}};}
async function bundle(text=JSON.stringify(packet()),change=()=>{}){
 const output=new TextEncoder().encode(text),sha=await api.sha(output),m={contract:'cb-replay.v1',generated_at:stamp,compilers:{},output_sha256:sha,output:{key:api.PREFIX+'outputs/'+sha+'.json',sha256:sha,bytes:output.length}};change(m);
 const raw=new TextEncoder().encode(JSON.stringify(m)),id=await api.sha(raw),files={['/'+api.PREFIX+'runs/'+id+'.json']:raw,['/'+m.output.key]:output},calls=[];
 const fetcher=async (url,options)=>{calls.push({url,options});return new Response(files[url],{status:files[url]?200:404});};
 return {id,m,files,calls,fetcher};
}

test('CB dates require complete Gregorian dates and timezone-aware clocks',()=>{
 assert.equal(api.day('2024-02-29'),Date.parse('2024-02-29T00:00:00Z'));
 for(const value of ['2026-02-30','2026-02-29','2026-13-01','2026-3-01','0000-01-01',[],true,null])assert.equal(api.day(value),null);
 assert.equal(api.clock('2026-03-02T12:00:00+01:00'),Date.parse(stamp));
 for(const value of ['2026-02-30T11:00:00Z','2026-03-02T24:00:00Z','2026-03-02T11:00:00','2026-03-02T11:00:00+24:00',stamp.replace(':00Z',':60Z'),[],true])assert.equal(api.clock(value),null);
 const q={status:'fresh',observation_date:'2026-02-30',max_age_days:1,acquired_at:stamp,source_generated_at:stamp},p=packet();assert.equal(api.fresh({latest:0,quality:q},p,now),false);
 q.observation_date='2026-03-02';assert.equal(api.fresh({latest:0,quality:q},p,now),true);q.max_age_days=true;assert.equal(api.fresh({latest:0,quality:q},p,now),false);
});
test('CB numeric formatting does not coerce arrays or whitespace into observations',()=>{
 for(const value of [[],[5],{},' ','0','5',true,false,null,undefined,NaN,Infinity])assert.equal(api.num(value),'Unavailable');
 assert.equal(api.num(0),'0');assert.equal(api.num(-.045),'-0.045');assert.match(api.render(packet(),now).ts,/^Compiled /);
});
test('currently usable counts exclude every unmeasured numeric value',()=>{
 const p=packet(),q={status:'fresh',observation_date:'2026-03-02',max_age_days:1,acquired_at:stamp,source_generated_at:stamp};
 p.quality.expected_native_series=1;
 for(const latest of [[],[5],{},' ','0',true,false,null,undefined,NaN,Infinity]){
  p.measurements={TEST:{latest,quality:q}};assert.equal(api.fresh(p.measurements.TEST,p,now),false);assert.match(api.render(p,now).hero,/0 of 1 native measurements/);
 }
 p.measurements.TEST.latest=0;assert.match(api.render(p,now).hero,/1 of 1 native measurements/);
});
test('strict CB JSON rejects duplicate decoded keys, overflow and lone surrogates',()=>{
 for(const raw of ['{"call":"LONG","call":null}','{"call":null,"ca\\u006cl":null}','{"nested":{"v":1,"v":2}}','[1e400]','{"x":"\\ud800"}','{"x":"\\udc00"}','{"x":01}','{} trailing','\ufeff{}'])assert.throws(()=>api.strictJSON(raw));
 const result=api.strictJSON('{"__proto__":{"protected":1},"text":"\\ud83c\\udfe6","v":0}');
 assert.equal(Object.getPrototypeOf(result),Object.prototype);assert.equal(result.__proto__.protected,1);assert.equal({}.protected,undefined);assert.equal(result.text,'🏦');assert.equal(result.v,0);
});
test('hash-matching snapshots with duplicate output fields remain invalid',async()=>{
 const f=await bundle(JSON.stringify(packet()).replace('"call":null','"call":"LONG","call":null'));
 await assert.rejects(api.loadSnapshot(f.id,f.fetcher),/Duplicate JSON key/);assert.equal(f.calls.length,2);
});
test('snapshot bytes require strict UTF-8 and an unambiguous manifest',async()=>{
 const f=await bundle();let raw=Buffer.from(JSON.stringify(packet()).replace('"quality":{}','"quality":{"note":"x"}'));
 const at=raw.indexOf('"x"')+1;raw=Buffer.concat([raw.subarray(0,at),Buffer.from([0xc3,0x28]),raw.subarray(at+1)]);
 const digest=await api.sha(raw);f.m.output_sha256=digest;f.m.output={key:api.PREFIX+'outputs/'+digest+'.json',sha256:digest,bytes:raw.length};
 let m=Buffer.from(JSON.stringify(f.m)),id=await api.sha(m);const files={['/'+api.PREFIX+'runs/'+id+'.json']:m,['/'+f.m.output.key]:raw};
 await assert.rejects(api.loadSnapshot(id,async url=>new Response(files[url])),/encoded data|UTF-8|encoding/i);
 m=Buffer.from(JSON.stringify(f.m).replace('"contract":"cb-replay.v1"','"contract":"wrong","contract":"cb-replay.v1"'));id=await api.sha(m);
 await assert.rejects(api.loadSnapshot(id,async()=>new Response(m)),/Duplicate JSON key/);
});
test('invalid output identities and impossible manifest clocks stop before output requests',async()=>{
 for(const mutate of [m=>m.output.bytes=-1,m=>m.output.bytes=true,m=>m.output.bytes=2.5,m=>m.output.bytes=Number.MAX_SAFE_INTEGER+1,m=>m.output.sha256='not-a-hash',m=>m.generated_at='2026-02-30T11:00:00Z']){
  const f=await bundle(undefined,mutate);await assert.rejects(api.loadSnapshot(f.id,f.fetcher),/output identity/);assert.equal(f.calls.length,1);
 }
});
test('complete genuine synthetic native bundle loads current and pinned without field loss',async()=>{
 const frozen=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/pre-cb-transport-native.json'),'utf8')),p=frozen.packet,calls=[];
 const fetcher=async url=>{calls.push(url);assert.ok(frozen.objects[url.slice(1)],url);return new Response(Buffer.from(frozen.objects[url.slice(1)],'base64'));};
 const current=await api.loadCurrent(fetcher);assert.deepEqual(current,p);assert.equal(calls.length,3);
 const id=p.replay.manifest_key.split('/').pop().slice(0,-5);assert.deepEqual(await api.loadSnapshot(id,fetcher),p);assert.equal(calls.length,5);
});
test('current pointer must match all immutable output fields and rejects duplicate pointers',async()=>{
 const f=await bundle(),p={...packet(),replay:{manifest_key:api.PREFIX+'runs/'+f.id+'.json',output_sha256:f.m.output_sha256,compilers:f.m.compilers}};
 f.files['/data/cb-injection.json']=Buffer.from(JSON.stringify(p));assert.deepEqual(await api.loadCurrent(f.fetcher),p);
 f.files['/data/cb-injection.json']=Buffer.from(JSON.stringify({...p,unexplained_extra:1}));await assert.rejects(api.loadCurrent(f.fetcher),/pointer differs/);
 f.files['/data/cb-injection.json']=Buffer.from(JSON.stringify(p).replace('"call":null','"call":"LONG","call":null'));await assert.rejects(api.loadCurrent(f.fetcher),/Duplicate JSON key/);
});
test('research requests reject unapproved paths, bad bounds and prior cancellation without fetching',async()=>{
 let calls=0;const fetcher=async()=>{calls++;throw Error('No request expected');};
 await assert.rejects(api.bytes(fetcher,'data/private.json'),/Unsupported/);
 await assert.rejects(api.bytes(fetcher,key,0),/bound/);await assert.rejects(api.bytes(fetcher,key,100,{timeoutMs:true}),/deadline/);
 const controller=new AbortController();controller.abort();await assert.rejects(api.bytes(fetcher,key,100,{signal:controller.signal}),{name:'AbortError'});assert.equal(calls,0);
});
test('fetch deadline settles even if transport ignores cancellation and late bodies are cancelled',async()=>{
 let resolve,signal,cancelled=0;const wait=new Promise(done=>resolve=done);
 await assert.rejects(api.bytes(async(_,options)=>{signal=options.signal;return wait;},key,100,{timeoutMs:5}),/timed out/);assert.equal(signal.aborted,true);
 resolve({ok:true,body:{cancel(){cancelled++;return Promise.resolve();}}});await new Promise(setImmediate);assert.equal(cancelled,1);
});
test('body deadline and external abort release the reader without waiting for cancellation',async()=>{
 for(const mode of ['deadline','abort']){
  let cancelled=0,released=0;const reader={read:()=>new Promise(()=>{}),cancel(){cancelled++;return new Promise(()=>{});},releaseLock(){released++;}};
  const controller=new AbortController(),loading=api.bytes(async()=>({ok:true,body:{getReader:()=>reader}}),key,100,{timeoutMs:mode==='deadline'?5:100,signal:controller.signal});
  if(mode==='abort')setTimeout(()=>controller.abort(),5);
  await assert.rejects(loading,mode==='deadline'?/timed out/:{name:'AbortError'});assert.equal(cancelled,1);assert.equal(released,1);
 }
});
test('oversized, nonbyte and failed streams release resources without returning a prefix',async()=>{
 for(const mode of ['oversized','nonbyte','failed']){
  let cancelled=0,released=0;const reader={async read(){if(mode==='failed')throw Error('Synthetic read failed');return {done:false,value:mode==='nonbyte'?'x':new Uint8Array(101)};},cancel(){cancelled++;return Promise.resolve();},releaseLock(){released++;}};
  await assert.rejects(api.bytes(async()=>({ok:true,body:{getReader:()=>reader}}),key,100),/bound|chunk|failed/);assert.equal(cancelled,1);assert.equal(released,1);
 }
});
test('failed HTTP responses cancel their bodies and exact byte limits remain complete',async()=>{
 let cancelled=0;await assert.rejects(api.bytes(async()=>({ok:false,status:503,body:{cancel(){cancelled++;return Promise.resolve();}}}),key,2),/503/);assert.equal(cancelled,1);
 const raw=await api.bytes(async()=>new Response(new Uint8Array([1,2])),key,2);assert.deepEqual([...raw],[1,2]);
});
async function page(url){
 const nodes=Object.fromEntries(['hero','cbs','carry','edollar','fails','evidence','note','ts','horizon'].map(id=>[id,{innerHTML:'',textContent:'',value:'1',addEventListener(){}}]));
 const calls=[],p=packet(),renderer={...api,async loadCurrent(){calls.push('current');return p;},async loadSnapshot(id){calls.push(['snapshot',id]);if(!/^[a-f0-9]{64}$/.test(id))throw Error('Invalid snapshot identifier');return p;}};
 const context={window:{CBResearch:renderer},document:{getElementById:id=>nodes[id]},URL,location:{href:url},Date:{now:()=>now},setInterval(fn,delay){calls.push(['timer',delay]);return 1;}};
 await vm.runInNewContext(fs.readFileSync(path.join(root,'jh-cb-page.js'),'utf8'),context);return {nodes,calls};
}
test('real page chooses one strict current or pinned path and keeps the existing local refresh cadence',async()=>{
 const current=await page('https://synthetic.invalid/cb-injection.html');assert.deepEqual(current.calls,['current',['timer',60000]]);assert.match(current.nodes.ts.textContent,/Compiled/);
 const id='a'.repeat(64),pinned=await page('https://synthetic.invalid/cb-injection.html?run='+id);assert.deepEqual(pinned.calls,[['snapshot',id],['timer',60000]]);assert.match(pinned.nodes.hero.innerHTML,/Pinned snapshot/);
});
test('empty and repeated snapshot parameters never fall back to current research',async()=>{
 const empty=await page('https://synthetic.invalid/cb-injection.html?run=');assert.deepEqual(empty.calls,[['snapshot','']]);assert.match(empty.nodes.hero.textContent,/Invalid snapshot/);
 const repeated=await page('https://synthetic.invalid/cb-injection.html?run=a&run=b');assert.deepEqual(repeated.calls,[]);assert.match(repeated.nodes.hero.textContent,/One snapshot/);
});
