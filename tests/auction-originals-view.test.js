const test=require('node:test');const assert=require('node:assert/strict');
const {webcrypto,createHash}=require('node:crypto');
const api=require('../jh-auction-originals.js');
const inspector=require('../jh-data-inspector.js');
const PREFIX='data/auction-observation-originals/';
const AT=new Date().toISOString();
const coverage={pages:1,observations:52,complete_requested_window:true,current_acquisition_vintage:true,
  historical_publication_vintages_verified:false,provider_snapshot_atomicity_verified:false};
function fixture(change={}){
 const document={contract:'auction-original-measurements.v1',generated_at:AT,coverage:{...coverage},
  observations:Array.from({length:52},(_,i)=>({cusip:'ROW'+i,values_decimal:{btc:i===0?null:i===1?'0':'2.1'}})),
  calls_eligible:false,forecast_eligible:false,sizing_eligible:false,execution_eligible:false,...change};
 const raw=Buffer.from(JSON.stringify(document)),sha=createHash('sha256').update(raw).digest('hex');
 return {document,raw,packet:{contract:'auction-original-replay.v1',generated_at:AT,coverage:{...coverage},
  measurements:{key:PREFIX+'measurements/'+sha+'.json',sha256:sha,bytes:raw.length},
  manifest:{key:PREFIX+'runs/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64),bytes:100},
  original_bytes_replayed:true,direct_measurements_replayed:true,historical_point_in_time_verified:false,
  calls_eligible:false,forecast_eligible:false,sizing_eligible:false}};
}
function spec(f=fixture()){return api.specification(f.packet,AT);}
class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.events={};this.dataset={};this.attrs={};this._text='';}
 set textContent(value){this._text=String(value);this.children=[];}
 get textContent(){return this._text+this.children.map(x=>x.textContent||'').join('');}
 append(...nodes){this.children.push(...nodes);}
 replaceChildren(...nodes){this.children=[...nodes];this._text='';}
 setAttribute(key,value){this.attrs[key]=value;}
 addEventListener(name,fn){this.events[name]=fn;}
}
function all(root,predicate){return [root,...root.children.flatMap(x=>all(x,predicate))].filter(predicate);}
function dom(){global.document={createElement:tag=>new Element(tag)};global.JHDataInspector=inspector;return new Element('section');}
function button(root){return all(root,n=>n.tagName==='button'&&n.textContent==='Inspect every measurement')[0];}

test('complete reference is bound to the source publication with no inferred forecast permission',()=>{
 const f=fixture(),s=spec(f);assert.equal(s.coverage.observations,52);assert.equal(s.measurements.sha256,f.packet.measurements.sha256);
 for(const changes of [{calls_eligible:true},{direct_measurements_replayed:1},{historical_point_in_time_verified:true},{contract:'other'}])
  assert.throws(()=>api.specification({...f.packet,...changes},AT));
});
test('private external traversal and mismatched content-addressed references are rejected',()=>{
 const ref=fixture().packet.measurements;
 for(const key of ['data/prospective-outcomes.json','private/account.json','https://example.com/x',PREFIX+'measurements/../x.json',ref.key+'?other=1'])
  assert.throws(()=>api.reference({...ref,key},'measurements'));
 for(const bytes of [0,-1,true,'100',Infinity,32*1024*1024+1])assert.throws(()=>api.reference({...ref,bytes},'measurements'));
});
test('stale future naive and mismatched capture clocks cannot masquerade as current evidence',()=>{
 const f=fixture();for(const generated_at of ['2000-01-01T00:00:00Z','2999-01-01T00:00:00Z','2026-09-29','2026-02-31T00:00:00Z'])
  assert.throws(()=>api.specification({...f.packet,generated_at},AT));
 assert.throws(()=>api.specification(f.packet,AT,Date.parse(AT)+49*3600000));
});
test('missing malformed and incomplete coverage never becomes an empty or complete population',()=>{
 for(const change of [{observations:null},{observations:true},{pages:0},{pages:31},{observations:201},{complete_requested_window:false},{provider_snapshot_atomicity_verified:true}])
  assert.throws(()=>api.coverage({...coverage,...change}));
 assert.equal(api.coverage({...coverage,observations:0}).observations,0);
});
test('complete artifact length hash and every returned row are checked',async()=>{
 const f=fixture();let urls=[];
 const result=await api.loadVerified(spec(f),async(url,options)=>{urls.push(url);assert.equal(options.credentials,'omit');return new Response(f.raw);},webcrypto);
 assert.equal(result.observations.length,52);assert.equal(result.observations[0].values_decimal.btc,null);
 assert.equal(result.observations[1].values_decimal.btc,'0');assert.equal(urls.length,1);
 assert.match(urls[0],/^\/data\/auction-observation-originals\/measurements\/[a-f0-9]{64}\.json\?exact=1&nogen=1$/);
});
test('truncated oversized wrong-hash and failed HTTP artifacts do not produce data',async()=>{
 const f=fixture();const changed=Buffer.from(f.raw);changed[changed.length-2]=32;
 for(const response of [new Response(f.raw.subarray(0,-1)),new Response(Buffer.concat([f.raw,Buffer.from('x')])),new Response(changed),new Response('denied',{status:403})])
  await assert.rejects(api.loadVerified(spec(f),async()=>response,webcrypto));
});
test('hash-correct but wrong row counts clocks contracts or permissions still fail validation',async()=>{
 for(const change of [{observations:[]},{generated_at:'2000-01-01T00:00:00Z'},{contract:'other'},{sizing_eligible:true},{coverage:{...coverage,pages:2}}]){
  const f=fixture(change);await assert.rejects(api.loadVerified(spec(f),async()=>new Response(f.raw),webcrypto));
 }
});
test('oversized streams are cancelled before any further chunks are consumed',async()=>{
 let reads=0,cancelled=0;
 const response={body:{getReader:()=>({read:async()=>{reads++;return {done:false,value:new Uint8Array(11)};},cancel:async()=>cancelled++,releaseLock(){}})}};
 await assert.rejects(api.readBounded(response,10));assert.equal(reads,1);assert.equal(cancelled,1);
});
test('rendering source references makes no automatic data request and never exposes arbitrary links',()=>{
 const root=dom();let reads=0;global.fetch=()=>{reads++;throw Error('Unexpected')};
 api.render(root,fixture().packet,AT);assert.equal(reads,0);assert.match(root.textContent,/52 observations/);
 const links=all(root,n=>n.tagName==='a');assert.equal(links.length,2);assert.ok(links.every(n=>n.href.startsWith('/'+PREFIX)));
 const f=fixture();f.packet.measurements.key='private/account.json';api.render(root,f.packet,AT);
 assert.equal(all(root,n=>n.tagName==='a').length,0);assert.equal(reads,0);
});
test('real complete-data inspector displays all rows by pagination and retains null and zero',async()=>{
 const root=dom(),f=fixture();global.fetch=async()=>new Response(f.raw);
 api.render(root,f.packet,AT);await button(root).onclick();
 assert.match(root.textContent,/Artifact bytes match/);
 const details=all(root,n=>n.tagName==='details'&&n.textContent.includes('52 items'))[0];assert.ok(details);
 details.open=true;details.events.toggle();
 assert.match(details.textContent,/ROW0/);assert.doesNotMatch(details.textContent,/ROW51/);
 const next=all(details,n=>n.tagName==='button'&&n.textContent==='Next')[0];next.onclick();next.onclick();
 assert.match(details.textContent,/ROW51/);assert.equal(next.disabled,true);
});
test('replacing the publication aborts loading and prevents delayed old content repaint',async()=>{
 const root=dom(),f=fixture();let resolve,signal;
 global.fetch=(url,options)=>{signal=options.signal;return new Promise(done=>resolve=done);};
 api.render(root,f.packet,AT);const running=button(root).onclick();
 api.render(root,null,null);assert.equal(signal.aborted,true);resolve(new Response(f.raw));await running;
 assert.doesNotMatch(root.textContent,/ROW0|Artifact bytes match/);assert.match(root.textContent,/not available/);
});
