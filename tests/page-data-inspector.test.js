const test=require('node:test');const assert=require('node:assert/strict');
const inspector=require('../jh-data-inspector.js');
class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.events={};this.dataset={};this.attrs={};this._text='';}
 set textContent(value){this._text=String(value);this.children=[];}
 get textContent(){return this._text+this.children.map(x=>x.textContent||'').join('');}
 append(...nodes){this.children.push(...nodes);}
 replaceChildren(...nodes){this.children=[...nodes];this._text='';}
 setAttribute(k,v){this.attrs[k]=v;}
 addEventListener(k,fn){this.events[k]=fn;}
}
function all(root,fn){return [root,...root.children.flatMap(x=>all(x,fn))].filter(fn);}
function dom(){global.document={createElement:tag=>new Element(tag)};return new Element('div');}
test('all nested paths retain null, zero, booleans, later-row-only fields and escaped names',()=>{
 const payload={rows:[null,{a:0,b:false,later:{'a/b~c':'full'}}],empty:[]};
 assert.deepEqual(inspector.leafPaths(payload),['/rows/0','/rows/1/a','/rows/1/b','/rows/1/later/a~1b~0c','/empty']);
 assert.deepEqual(inspector.columns([null,{a:1},{last:true}]),['a','last']);
});
test('actual rendered array paginates every row and includes columns beyond the first row',()=>{
 dom();const rows=Array.from({length:52},(_,i)=>({symbol:'ROW'+i,a:i,b:false,c:0,d:'text',e:1}));rows[0]=null;rows[26].critical_risk_flag='BREACH';
 const view=inspector.collectionView(rows,'/rows');
 assert.ok(view.textContent.includes('critical_risk_flag'));assert.ok(view.textContent.includes('null'));assert.ok(!view.textContent.includes('ROW26'));
 const next=all(view,n=>n.tagName==='button'&&n.textContent==='Next')[0];next.onclick();
 assert.ok(view.textContent.includes('ROW26')&&view.textContent.includes('BREACH'));next.onclick();assert.ok(view.textContent.includes('ROW51'));assert.equal(next.disabled,true);
});
test('complete inspector preserves long text and exposes nested field values through path search',()=>{
 const target=dom(),long='Methodology '.repeat(100);inspector.inspect(target,{methodology:long,nested:{risk:{warning:'CANNOT TRADE'}}},'producer -> output');
 assert.ok(target.textContent.includes(long));const search=all(target,n=>n.tagName==='input')[0];search.value='warning';search.events.input();
 assert.ok(target.textContent.includes('CANNOT TRADE')&&target.textContent.includes('/nested/risk/warning'));
});
test('object field pagination exposes every scalar beyond the old eight-field cap',()=>{
 dom();const obj=Object.fromEntries(Array.from({length:61},(_,i)=>['field'+i,'value'+i]));const view=inspector.collectionView(obj,'');let text=view.textContent;
 const next=all(view,n=>n.tagName==='button'&&n.textContent==='Next')[0];while(!next.disabled){next.onclick();text+=view.textContent;}
 for(let i=0;i<61;i++)assert.ok(text.includes('value'+i));
});

test('owner artifact inspection requires matching authenticated route and never uses public fallback', async()=>{
 const api=require('../jh-data-inspector.js');let publicCalls=0,ownerCalls=0;
 const entry={key:'portfolio/snapshot.json',access:'owner_authenticated',private_kind:'portfolio-snapshot'};
 const publicFetch=async()=>{publicCalls++;return {ok:true};};
 await assert.rejects(api.fetchArtifact(entry,publicFetch,null),/Authenticated owner/);
 await assert.rejects(api.fetchArtifact(entry,publicFetch,{kindFor:()=>null,fetch:publicFetch}),/Authenticated owner/);
 const response=await api.fetchArtifact(entry,publicFetch,{kindFor:()=>entry.private_kind,fetch:async()=>{ownerCalls++;return {status:401};}});
 assert.equal(response.status,401);assert.equal(publicCalls,0);assert.equal(ownerCalls,1);
 await api.fetchArtifact({key:'backtest/results.json',access:'public'},publicFetch,null);assert.equal(publicCalls,1);
});

test('legacy account and note payloads cannot bypass required public projection',()=>{
 const api=require('../jh-data-inspector.js');
 assert.throws(()=>api.validateProjection({required_projection:'sizing'},{holdings:[{ticker:'PRIVATE'}]}),/not yet redacted/);
 assert.throws(()=>api.validateProjection({required_projection:'brain-compiler'},{claims:[{claim:'PRIVATE'}]}),/not yet redacted/);
 assert.deepEqual(api.validateProjection({required_projection:'brain-compiler'},{claims:[{claim_text_private:true,count:0}]}),{claims:[{claim_text_private:true,count:0}]});
});

test('response observation matches exact reviewed API, performs no extra request, and preserves every field',async()=>{
 const records=[],calls=[],contracts=[{engine:'engine',origin:'https://api.justhodl.ai',pathname:'/known',methods:['POST']}];
 const wrapped=inspector.observeResponses(async(url,init)=>{calls.push([url,init]);return new Response(JSON.stringify({zero:0,missing:null,rows:[{a:1},{later:'full'}]}),{status:200});},contracts,r=>records.push(r),()=>({uid:null,epoch:0}));
 const response=await wrapped('https://api.justhodl.ai/known?secret=do-not-label',{method:'POST',body:'do-not-retain'});await response.json();await new Promise(r=>setImmediate(r));
 assert.equal(calls.length,1);assert.equal(records.length,1);assert.equal(records[0].endpoint,'https://api.justhodl.ai/known');assert.equal(records[0].payload.rows[1].later,'full');assert.equal(records[0].payload.zero,0);assert.equal(records[0].payload.missing,null);assert.equal(records[0].body,undefined);
 await wrapped('https://unreviewed.example/known',{method:'POST'});await wrapped('https://api.justhodl.ai/known-else',{method:'POST'});await wrapped('https://api.justhodl.ai/known',{method:'GET'});await new Promise(r=>setImmediate(r));assert.equal(records.length,1);
});

test('owner response observation drops signed-out, wrong-kind and in-flight previous-owner responses',async()=>{
 let owner={uid:null,epoch:0},resolve,records=[];
 const contract={engine:'private',origin:'https://api.justhodl.ai',pathname:'/private-artifact',methods:['GET'],query:{kind:'brain'},owner_authenticated:true};
 const wrapped=inspector.observeResponses(async()=>({ok:true,clone:()=>({json:()=>new Promise(r=>{resolve=r;})})}),[contract],r=>records.push(r),()=>({...owner}));
 await wrapped('https://api.justhodl.ai/private-artifact?kind=brain');assert.equal(resolve,undefined);
 owner={uid:'owner-a',epoch:1};await wrapped('https://api.justhodl.ai/private-artifact?kind=other');assert.equal(resolve,undefined);
 await wrapped('https://api.justhodl.ai/private-artifact?kind=brain');owner={uid:'owner-b',epoch:2};resolve({notes:['private-a']});await new Promise(r=>setImmediate(r));assert.equal(records.length,0);
});

test('head bootstrap captures first inline application response before DOM ready and clears it on auth change',async()=>{
 const vm=require('node:vm'),fs=require('node:fs');const events={},head=new Element('head'),body=new Element('body');
 const apiContract={engine:'first-engine',origin:'https://api.justhodl.ai',pathname:'/first',methods:['GET']};
 const config=new Element('script');config.textContent=JSON.stringify([apiContract]);let engineCalls=0,ownerChange;
 const contract={outputs:[],primary_producers:['first-engine'],api_responses:[apiContract]},pageConfig=new Element('script');pageConfig.textContent=JSON.stringify(contract);
 const doc={head,body,readyState:'loading',createElement:tag=>new Element(tag),getElementById:id=>id==='jh-api-data-contract'?config:id==='jh-page-data-contract'?pageConfig:all(body,n=>n.id===id)[0],addEventListener:(event,fn)=>{(events[event]??=[]).push(fn);}};
 const context={document:doc,location:{pathname:'/first.html',href:'https://justhodl.ai/first.html',search:''},URL,URLSearchParams,console,
  JustHodlAuth:{getUser:()=>({id:'owner-a'}),onChange:fn=>{ownerChange=fn;}},
  fetch:async url=>{if(url==='/config/page-data-contracts.json')throw new Error('Embedded page contract must avoid registry request');engineCalls++;return new Response(JSON.stringify({first_payload:{zero:0,all_rows:[1,2,3]}}),{status:200});}};
 vm.createContext(context);vm.runInContext(fs.readFileSync(require.resolve('../jh-data-inspector.js'),'utf8'),context);
 await vm.runInContext("fetch('https://api.justhodl.ai/first').then(r=>r.json())",context);await new Promise(r=>setImmediate(r));
 for(const fn of events.DOMContentLoaded)await fn();await new Promise(r=>setImmediate(r));
 const choice=all(body,n=>n.attrs['aria-label']==='Observed engine API response')[0];assert.ok(choice.textContent.includes('first-engine'));choice.value='0';choice.onchange();
 assert.ok(body.textContent.includes('first_payload'));assert.equal(engineCalls,1);
 ownerChange();assert.ok(!choice.textContent.includes('first-engine'));assert.ok(!body.textContent.includes('first_payload'));
});

test('reviewed archive index exposes every listed matching key and rejects unrelated/private paths',()=>{
 const entry={engine:'snapshotter',key:'calibration/history-index.json',archive_index:{rows:'snapshots',key_field:'key',key_regex:'^calibration/history/[^/]+\\.json$'}};
 const rows=Array.from({length:60},(_,i)=>({key:'calibration/history/week-'+i+'.json'}));
 rows.push(null,{key:'data/brain.json'},{key:'calibration/history/../private.json'},{key:'https://evil.example/private.json'},{key:'calibration/history/week-1.json'});
 const outputs=inspector.indexedOutputs(entry,{snapshots:rows});assert.equal(outputs.length,60);assert.equal(outputs.at(-1).key,'calibration/history/week-59.json');assert.ok(outputs.every(x=>x.access==='public'));
 assert.throws(()=>inspector.indexedOutputs(entry,{snapshots:null}),/schema unavailable/);
});

test('artifact decoder accepts browser-decoded JSON or real gzip and preserves all values',async()=>{
 const gzip=require('node:zlib').gzipSync,payload={zero:0,missing:null,nested:[1,false,'complete']},encoded=JSON.stringify(payload);
 assert.deepEqual(await inspector.decodeArtifactResponse(new Response(encoded)),payload);
 assert.deepEqual(await inspector.decodeArtifactResponse(new Response(gzip(encoded))),payload);
 await assert.rejects(inspector.decodeArtifactResponse(new Response('not json')));
});


test('reviewed metadata index preserves original engine and rejects wrong or incomplete provenance',()=>{
 const entry={engine:'justhodl-public-archive-index',key:'data/archive-indexes/source-engine.json',archive_index:{engine:'source-engine',publisher_engine:'justhodl-public-archive-index',required_schema:'public-engine-archive-index.v1',require_complete:true,rows:'snapshots',key_field:'key',patterns:['data/archive/reviewed/*.json'],key_regex:'^data/archive/reviewed/[^/]+\\.json$'}};
 const payload={schema_version:'public-engine-archive-index.v1',engine:'source-engine',publisher_engine:entry.engine,complete:true,families:entry.archive_index.patterns,snapshots:[{key:'data/archive/reviewed/2026-09-09.json'}]};
 const rows=inspector.indexedOutputs(entry,payload);assert.equal(rows.length,1);assert.equal(rows[0].engine,'source-engine');assert.equal(rows[0].index_publisher,entry.engine);
 for(const change of [{complete:false},{engine:'other'},{publisher_engine:'other'},{families:['data/archive/private/*.json']},{schema_version:'unknown'}])assert.throws(()=>inspector.indexedOutputs(entry,{...payload,...change}));
});
