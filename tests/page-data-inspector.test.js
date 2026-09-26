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
 const publicFetch=async()=>{publicCalls++;return {ok:true,headers:new Headers({'X-JH-Artifact-Key':'backtest/results.json'})};};
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

test('approved non-data namespaces use the actual proxy route without credentials or key rewriting',async()=>{
 const calls=[],fetcher=async(url,options)=>{calls.push({url,options});return {ok:false,status:404};};
 for(const key of ['cot/extremes/current.json','backtest/results.json','screener/data.json','repo-data.json','calibration/history/2026-09-09.json','data/_backtest/graded.json.gz']){
  const response=await inspector.fetchArtifact({key,access:'public'},fetcher,null);
  assert.equal(response.status,404);const call=calls.at(-1);
  assert.equal(call.url,(key.startsWith('data/')?'/':'https://justhodl-data-proxy.raafouis.workers.dev/')+key+'?exact=1&nogen=1');
  assert.equal(call.options.credentials,key.startsWith('data/')?'same-origin':'omit');
 }
 assert.equal(calls.length,6); // Missing keys never cause a guessed alternate request.
});

test('unsupported public paths and unknown access modes cannot escape the reviewed proxy',async()=>{
 let calls=0;const fetcher=async()=>{calls++;};
 for(const key of ['../private.json','/cot/ES.json','cot/../secret.json','https://evil.invalid/x.json','cot/x.json?secret=1','cot//x.json']){
  await assert.rejects(inspector.fetchArtifact({key,access:'public'},fetcher,null));
 }
 await assert.rejects(inspector.fetchArtifact({key:'cot/extremes/current.json'},fetcher,null));
 assert.equal(calls,0);
 for(const header of [null,'data/cot/extremes/current.json']){
  await assert.rejects(inspector.fetchArtifact({key:'cot/extremes/current.json',access:'public'},async()=>({ok:true,headers:new Headers(header?{'X-JH-Artifact-Key':header}:{})}),null),/Exact artifact identity/);
 }
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

test('reviewed symbol manifests traverse object keys, values and ticker lists without guessing paths',()=>{
 const fixtures=[
  [{rows:'symbols',rows_mode:'object_values',key_field:'key'},{symbols:{A:{key:'data/reviewed/A.json'},B:{key:'data/private/B.json'}}}],
  [{rows:'series',rows_mode:'object_keys',key_field:'$value',key_prefix:'data/reviewed/',key_suffix:'.json'},{series:{A:{n:300},'../private/B':{n:1}}}],
  [{rows:'tickers',key_field:'$value',key_prefix:'data/reviewed/',key_suffix:'.json'},{tickers:['A','../private/B']}],
 ];
 for(const [contract,payload] of fixtures){
  const result=inspector.indexedOutputs({engine:'source',key:'data/reviewed/_index.json',archive_index:{...contract,key_regex:'^data/reviewed/[A-Z]+\\.json$'}},payload);
  assert.deepEqual(result.map(row=>row.key),['data/reviewed/A.json']);
 }
});


test('health and provider inspection requires reviewed public publication markers',()=>{
 const fixtures={
  'fleet-health':{privacy_version:'fleet-metadata-20260909-v1'},
  'fleet-errors':{privacy_version:'fleet-errors-metadata-20260909-v1',diagnostic_text_private:true},
  'fleet-freshness':{publication:{schema_version:'public-freshness-report.v1',scope:'PUBLIC_ENGINE_HEALTH',contains_private_data:false}},
  'source-map':{schema_version:'public-source-map.v1',publication:{scope:'PUBLIC_MARKET_SOURCE_METADATA',contains_private_data:false}},
  'provider-metrics':{publication:{schema_version:'public-provider-metrics.v1',diagnostics:'FIXED_CATEGORIES_ONLY',contains_provider_response_text:false}}
 };
 for(const [required_projection,doc] of Object.entries(fixtures)){
  assert.equal(inspector.validateProjection({required_projection},doc),doc);
  for(const legacy of [null,{},[],{error:'SYNTHETIC_PRIVATE_DIAGNOSTIC'}])assert.throws(()=>inspector.validateProjection({required_projection},legacy),/public projection/);
 }
});

test('standalone selection accepts one registered target and refuses ambiguous or unsafe requests',()=>{
 assert.deepEqual(inspector.inspectionSelection('engine-data.html','?page=chart.html'),{kind:'page',key:'chart.html',standalone:true});
 assert.deepEqual(inspector.inspectionSelection('engine-data.html','?engine=source-engine'),{kind:'engine',key:'source-engine',standalone:true});
 assert.deepEqual(inspector.inspectionSelection('calls.html','?page=chart.html'),{kind:'page',key:'calls.html',standalone:false});
 for(const search of ['?page=../private.html','?page=https://elsewhere.invalid/a.html','?page=chart.html&page=calls.html','?page=chart.html&engine=','?engine=','?engine=x/y'])
  assert.throws(()=>inspector.inspectionSelection('engine-data.html',search));
 const contract={outputs:[]},manifest={pages:{'chart.html':contract},engines:{'source-engine':contract}};
 assert.equal(inspector.selectedContract(manifest,{kind:'page',key:'chart.html'}),contract);
 for(const key of ['missing.html','__proto__','constructor'])assert.throws(()=>inspector.selectedContract(manifest,{kind:'page',key}));
 assert.throws(()=>inspector.selectedContract({pages:{'chart.html':{}}},{kind:'page',key:'chart.html'}));
});

test('standalone chart view loads its complete source contract without fetching outputs or claiming another tab response',async()=>{
 const vm=require('node:vm'),fs=require('node:fs'),head=new Element('head'),body=new Element('body');
 const embedded=new Element('script');embedded.textContent=JSON.stringify({outputs:[],primary_producers:['wrong-embedded-page']});
 const output={engine:'source-engine',key:'data/fixture-public.json',access:'public'},requests=[];
 const contract={outputs:[output],primary_producers:['source-engine'],api_responses:[{engine:'dynamic-engine',origin:'https://api.justhodl.ai',pathname:'/quote'}]};
 const doc={head,body,readyState:'complete',createElement:tag=>new Element(tag),getElementById:id=>id==='jh-page-data-contract'?embedded:all(body,n=>n.id===id)[0]};
 const context={document:doc,location:{pathname:'/engine-data.html',href:'https://justhodl.ai/engine-data.html?page=chart.html',search:'?page=chart.html'},URL,URLSearchParams,TextDecoder,Uint8Array,AbortController,setTimeout,clearTimeout,console,
  fetch:async(url,options)=>{requests.push({url,options});if(url==='/config/page-data-contracts.json')return new Response(JSON.stringify({schema_version:'page-data-contract.v1',pages:{'chart.html':contract}}));
    assert.equal(url,'/data/fixture-public.json?exact=1&nogen=1');return new Response(JSON.stringify({rows:[{value:0},{value:null,extra:false}]}),{headers:{'X-JH-Artifact-Key':output.key}});}};
 vm.runInNewContext(fs.readFileSync(require.resolve('../jh-data-inspector.js'),'utf8'),context);await new Promise(setImmediate);
 assert.equal(requests.length,1);assert.match(body.textContent,/chart.html · Engine data inspector · 1 outputs/);
 assert.match(body.textContent,/does not observe another tab/);assert.match(body.textContent,/1 registered API response contracts · not captured here/);
 assert.ok(!body.textContent.includes('wrong-embedded-page'));assert.ok(!body.textContent.includes('Complete API responses · current page session'));
 const choice=all(body,n=>n.attrs['aria-label']==='Engine output')[0];choice.value='source-engine::data/fixture-public.json';await choice.events.change();
 assert.equal(requests.length,2);assert.equal(all(body,n=>n.id==='jh-engine-data')[0].dataset.loadedLeafPaths,'3');
 assert.match(body.textContent,/data\/fixture-public.json/);
 choice.value='';await choice.events.change();
 assert.equal(requests.length,2);assert.equal(all(body,n=>n.id==='jh-engine-data')[0].dataset.loadedOutput,undefined);
 assert.ok(!body.textContent.includes('3 inspectable leaf paths'));
});

test('source links bind shared writer and complete invocation chain to exact build commit',()=>{
 const commit='a'.repeat(40),entry={engine:'justhodl-example',ownership_evidence:[{repository_path:'aws/shared/store.py',file:'store.py',line:29,basis:'reachable_shared_write_argument',entrypoint_basis:'configured_handler',via:[{file:'aws/lambdas/justhodl-example/source/handler.py',line:12,function:'aws/shared/store.py:publish'}]}]};
 const rows=inspector.ownershipRecords(entry,commit);assert.equal(rows.length,1);
 assert.equal(rows[0].writer.href,'https://github.com/ElMooro/si/blob/'+commit+'/aws/shared/store.py#L29');
 assert.equal(rows[0].via[0].href,'https://github.com/ElMooro/si/blob/'+commit+'/aws/lambdas/justhodl-example/source/handler.py#L12');
 assert.deepEqual(inspector.ownershipRecords(entry,null).map(r=>r.writer.href),[null]);
});

test('unsafe source paths or unpinned builds never become active source links',()=>{
 for(const path of ['https://evil.invalid/x','javascript:alert(1)','aws/shared/../private.py','aws/shared//x.py','aws/shared/x.py?token=1','aws/shared/%2e%2e/x.py','aws/shared/x.py#L1','aws/shared/x\\y.py']){
  const entry={engine:'valid',ownership_evidence:[{repository_path:path,file:'valid.py',line:1}]};
  assert.equal(inspector.ownershipRecords(entry,'a'.repeat(40))[0].writer.href,null,path);
 }
 for(const commit of ['main','a'.repeat(7),'https://elsewhere.invalid','A'.repeat(40)]){
  assert.equal(inspector.ownershipRecords({engine:'valid',ownership_evidence:[{file:'handler.py',line:1}]},commit)[0].writer.href,null);
 }
});

test('legacy inventory and conventional handler retain explicit uncertainty and all proof rows',()=>{
 dom();const entry={engine:'justhodl-example',ownership_evidence:[{file:'legacy_original.py',line:8},...Array.from({length:30},(_,i)=>({repository_path:'aws/shared/store.py',line:20+i,entrypoint_basis:'conventional_source_handler_runtime_unverified'}))]};
 const rows=inspector.ownershipRecords(entry,'b'.repeat(40));assert.equal(rows.length,31);
 assert.equal(rows[0].writer.path,'aws/lambdas/justhodl-example/source/legacy_original.py');assert.match(rows[0].basis,/reachability unverified/);
 const view=inspector.ownershipView(entry,'b'.repeat(40));assert.match(view.textContent,/31 recorded write sites/);assert.match(view.textContent,/AWS handler configuration is unverified/);assert.match(view.textContent,/does not prove the deployed Lambda/);
 assert.equal(all(view,n=>n.tagName==='a').length,31);assert.ok(view.textContent.includes('store.py:49'));
});

test('source evidence text is never parsed as HTML and missing evidence is explicit',()=>{
 dom();const view=inspector.ownershipView({engine:'example',ownership_evidence:[{repository_path:'<img src=x onerror=alert(1)>',basis:'<script>bad()</script>',via:[{file:'javascript:bad()',function:'<svg onload=bad()>',line:Infinity}]}]},'c'.repeat(40));
 assert.ok(view.textContent.includes('<script>bad()</script>'));
 assert.equal(all(view,n=>['img','script','svg','a'].includes(n.tagName)).length,0);
 assert.match(inspector.ownershipView({engine:'example'},null).textContent,/No source-write record/);
});

test('standalone registry verifies exact build bytes and refuses a stale cached registry',async()=>{
 const crypto=require('node:crypto'),commit='d'.repeat(40),body=JSON.stringify({pages:{'ciss.html':{outputs:[]}}}),hash=crypto.createHash('sha256').update(body).digest('hex'),requests=[];
 const result=await inspector.fetchRegistry(async(url,options)=>{requests.push({url,options});return new Response(body);},commit,hash);
 assert.deepEqual(result,JSON.parse(body));assert.equal(requests[0].url,'/config/page-data-contracts.json?build='+commit+'&sha256='+hash);assert.equal(requests[0].options.cache,'no-store');
 await assert.rejects(inspector.fetchRegistry(async()=>new Response('{"pages":{}}'),commit,hash),e=>e.code==='REGISTRY_BUILD_MISMATCH');
 for(const [c,h] of [[commit,null],['main',hash],[null,hash]]){
  let calls=0;await assert.rejects(inspector.fetchRegistry(async()=>{calls++;},c,h));assert.equal(calls,0);
 }
});

test('registry deadline covers a stalled response body even when abort is ignored',async()=>{
 for(const fetcher of [()=>new Promise(()=>{}),async()=>({ok:true,arrayBuffer:()=>new Promise(()=>{})})]){
  await assert.rejects(inspector.fetchRegistry(fetcher,null,null,5),/timed out/);
 }
 await assert.rejects(inspector.fetchRegistry(async()=>new Response('not JSON'),null,null));
});
