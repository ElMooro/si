const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
const api=require('../jh-public-brief.js'),fixture=require('./fixtures/calls-byte-proof.json');
const now=Date.parse('2026-09-18T17:10:00Z'),raw=s=>new TextEncoder().encode(s).buffer;
const publicDoc=JSON.parse(fixture.raw),copy=x=>structuredClone(x);

test('actual current native proof binds noncanonical received bytes in the browser',async()=>{
 const packet=await api.readJSON(new Response(fixture.raw));assert.equal(await api.proofMatches(fixture.proof,packet.raw,now),true);
 assert.deepEqual(packet.doc,publicDoc);assert.equal(new TextDecoder().decode(packet.raw),fixture.raw);
 assert.equal(await api.proofMatches(fixture.proof,raw(JSON.stringify(publicDoc)),now),false);
});
test('changed displayed prose and evidence fail even when run reference stays identical',async()=>{
 for(const edit of [d=>d.brief_md+=' altered conclusion',d=>d.evidence[0].value=42,d=>d.evidence[0].unit='changed',d=>d.evidence_inventory.eligible_votes=1]){
  const doc=copy(publicDoc);edit(doc);assert.deepEqual(doc.research_replay,publicDoc.research_replay);
  assert.equal(await api.proofMatches(fixture.proof,raw(JSON.stringify(doc)),now),false);
 }
});
test('all proof identities and research-only authority must match',async()=>{
 for(const [key,value]of Object.entries({schema_version:'unknown',status:'failed',run_id:'other',payload_sha256:'0'.repeat(64),bundle_sha256:'0'.repeat(64),snapshot_id:'other',call_verb:'LONG',sizing_eligible:true,decision_eligible:true,private_account_data_read:true,brief_sha256:'0'.repeat(64),publication_generated_at:'2026-09-18T16:00:00Z'})){
  const proof={...fixture.proof,[key]:value};assert.equal(await api.proofMatches(proof,raw(fixture.raw),now),false,key);
 }
 for(const key of ['public_object','snapshot_id','decision_eligible','schema_version']){const p=copy(fixture.proof);delete p[key];assert.equal(await api.proofMatches(p,raw(fixture.raw),now),false,key);}
});
test('byte length, digest, key and proof clocks reject malformed or impossible binding',async()=>{
 for(const edit of [p=>p.public_object.bytes++,p=>p.public_object.bytes=String(p.public_object.bytes),p=>p.public_object.sha256='0'.repeat(64),p=>p.public_object.key='data/private.json',p=>p.generated_at='2026-09-30T00:00:00Z',p=>p.generated_at='2026-09-18T16:00:00Z',p=>p.generated_at='2026-02-30T17:00:00Z',p=>p.generated_at='2026-09-18 17:05:00']){
  const proof=copy(fixture.proof);edit(proof);assert.equal(await api.proofMatches(proof,raw(fixture.raw),now),false);
 }
});
test('historical replay validity stays separate from current-use freshness',async()=>{
 const later=now+7*86400000;assert.equal(api.state(publicDoc,later).overdue,true);
 assert.equal(await api.proofMatches(fixture.proof,raw(fixture.raw),later),true);
});
test('both packet and proof loader reject duplicate decoded keys, nonfinite and malformed JSON',async()=>{
 for(const s of ['{"generated_at":"old","generated_at":"new"}','{"status":0,"st\\u0061tus":1}','{"a":[{"x":1,"x":2}]}','{"x":1e999}','{"x":NaN}','{"x":Infinity}','{}{}','{"x":true,}','[]','null','0','\ufeff{}'])await assert.rejects(api.readJSON(new Response(s)));
 await assert.rejects(api.readJSON(new Response(new Uint8Array([123,34,120,34,58,34,255,34,125]))));
 for(const scalar of ['\\ud800','\\udc00','\\ud800X'])await assert.rejects(api.readJSON(new Response('{"text":"'+scalar+'"}')));
 assert.equal((await api.readJSON(new Response('{"text":"\\ud83d\\ude00"}'))).doc.text,'😀');
});
test('whole-body reads retain numeric spelling and escaped unicode without canonicalization',async()=>{
 const s=' {"value":1.0,"minus":-0.0,"large":9007199254740993,"zero":0,"no":false,"nil":null,"text":"\\u2603","__proto__":{"safe":true}}\n';
 const p=await api.readJSON(new Response(s));assert.deepEqual(p.raw,raw(s));assert.equal(p.doc.no,false);assert.equal(p.doc.nil,null);assert.equal(p.doc.zero,0);assert.equal(p.doc.text,'☃');assert.equal(Object.is(p.doc.minus,-0),true);assert.equal({}.safe,undefined);
});
test('stream failure cannot produce partial evidence, HTTP failure cancels its body',async()=>{
 const stream=new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('{}'));c.error(Error('truncated'));}});
 await assert.rejects(api.readJSON(new Response(stream)),/truncated/);
 let closed=0;await assert.rejects(api.readJSON({ok:false,body:{cancel:async()=>closed++}}));assert.equal(closed,1);
});
function element(tag){return{tag,children:[],attrs:{},style:{},textContent:'',appendChild(n){this.children.push(n);},replaceChildren(){this.children=[];},setAttribute(k,v){this.attrs[k]=v;}};}
function flatten(n){return[n,...n.children.flatMap(flatten)];}
function mount(fetcher){
 const main=element('main'),timers=[];main.insertBefore=n=>main.appendChild(n);
 const doc={visibilityState:'visible',querySelector:s=>s==='main'?main:null,createElement:element,getElementById:id=>flatten(main).find(n=>n.id===id)};
 class Clock extends Date{static now(){return now;}}
 const ctx={document:doc,crypto:crypto.webcrypto,TextEncoder,TextDecoder,Uint8Array,AbortSignal,Date:Clock,fetch:fetcher,setInterval:fn=>timers.push(fn)};
 vm.runInNewContext(fs.readFileSync(require.resolve('../jh-public-brief.js'),'utf8'),ctx);
 const text=()=>flatten(main).map(n=>n.textContent).join('\n');return{main,doc,timers,text};
}
async function until(fn){for(let n=0;n<100&&!fn();n++)await new Promise(r=>setTimeout(r,2));assert.ok(fn());}
test('mounted renderer verifies exact content and clears old badge on failed refresh, then recovers',async()=>{
 let mode='valid',calls=[];
 const mounted=mount(async url=>{calls.push(url);if(url.includes('proofs'))return new Response(JSON.stringify(fixture.proof));return new Response(mode==='bad'?'{"x":1,"x":2}':fixture.raw);});
 await until(()=>mounted.text().includes('Replay verified for these exact brief bytes'));
 assert.equal(mounted.timers.length,1);assert.equal(calls.length,2);assert.match(mounted.text(),/0 eligible votes/);
 mode='bad';await mounted.timers[0]();await until(()=>mounted.text().includes('Public brief unavailable'));
 assert.doesNotMatch(mounted.text(),/Replay verified/);assert.equal(calls.length,3);
 mode='valid';await mounted.timers[0]();await until(()=>mounted.text().includes('Replay verified for these exact brief bytes'));assert.equal(calls.length,5);
});
test('legacy, duplicate and mismatching proof never show a verified badge; brief remains inspectable',async()=>{
 for(const body of [JSON.stringify({...publicDoc.research_replay,status:'reproduced'}),'{"status":"failed",'+JSON.stringify(fixture.proof).slice(1),JSON.stringify({...fixture.proof,snapshot_id:'wrong'})]){
  let calls=0;const mounted=mount(async url=>{calls++;return new Response(url.includes('proofs')?body:fixture.raw);});
  await until(()=>calls===2);await new Promise(r=>setTimeout(r,10));assert.doesNotMatch(mounted.text(),/Replay verified/);
  assert.match(mounted.text(),/Inspect measurements/);assert.match(mounted.text(),/0 eligible votes/);
 }
});
test('overlapping refresh is skipped and cannot pair a new brief with an old request',async()=>{
 let release,requests=0;const gate=new Promise(r=>release=r);
 const mounted=mount(async url=>{requests++;if(url.includes('proofs')){await gate;return new Response(JSON.stringify(fixture.proof));}return new Response(fixture.raw);});
 await until(()=>requests===2);mounted.timers[0]();mounted.timers[0]();assert.equal(requests,2);
 release();await until(()=>mounted.text().includes('Replay verified for these exact brief bytes'));
 mounted.doc.visibilityState='hidden';mounted.timers[0]();assert.equal(requests,2);
});
