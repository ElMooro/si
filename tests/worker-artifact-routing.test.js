const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path');
const {pathToFileURL}=require('node:url');
const source=pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js')).href;
test('valuation page loader has deployed routes and reads the exact root object uncached',async()=>{
 const fs=require('node:fs'),page=require('../jh-valuation-research.js'),worker=(await import(source)).default;
 const config=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/wrangler.toml'),'utf8');
 const routes=[...config.matchAll(/pattern\s*=\s*"([^"]+)"/g)].map(m=>m[1]);
 for(const host of ['justhodl.ai','www.justhodl.ai']){
  const state=setup();assert.ok(routes.includes(host+'/valuations-data.json'),'Missing public root route for '+host);
  globalThis.fetch=async(input,options)=>{state.calls.push({url:String(input),options});return Response.json({contract:'valuation-native-research.v1'});};
  const packet=await page.load('valuations-data.json',async(key,options)=>{
   assert.equal(key,'/valuations-data.json');assert.equal(options.cache,'no-store');
   const response=await worker.fetch(new Request('https://'+host+key),{},state.context);
   assert.equal(response.headers.get('Cache-Control'),'no-store');
   assert.equal(response.status,200);return response;
  });
  assert.equal(packet.doc.contract,'valuation-native-research.v1');assert.equal(state.calls.length,1);
  assert.equal(new URL(state.calls[0].url).pathname,'/valuations-data.json');assert.equal(state.calls[0].options.cache,'no-store');
  assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);
 }
});
function setup(){
 const calls=[],keys=[],stored=new Map(),waits=[];
 globalThis.caches={default:{async match(req){keys.push(req.url);return stored.get(req.url)?.clone();},async put(req,response){stored.set(req.url,response.clone());}}};
 globalThis.fetch=async(input)=>{const url=String(input);calls.push(url);return url.endsWith('/cot/extremes/current.json')?Response.json({schema_version:'cot-extremes.v2'}):new Response('{}',{status:404});};
 return {calls,keys,stored,waits,context:{waitUntil(promise){waits.push(promise);}}};
}

test('Yield Curve current publication bypasses caches without invoking a producer',async()=>{
 const worker=(await import(source)).default;
 for(const method of ['GET','HEAD']){
  const state=setup();globalThis.fetch=async(input,options)=>{state.calls.push({url:String(input),options});return Response.json({contract:'yield-curve-research.v1'});};
  const response=await worker.fetch(new Request('https://justhodl.ai/data/yield-curve.json?exact=1&nogen=1',{method}),{},state.context);
  assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');
  assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);assert.equal(state.calls.length,1);
  assert.equal(new URL(state.calls[0].url).pathname,'/data/yield-curve.json');assert.equal(state.calls[0].options.cache,'no-store');
 }
});

test('Liquidity-agent root publication bypasses stale edge and origin caches',async()=>{
 const worker=(await import(source)).default;
 for(const method of ['GET','HEAD']){
  const state=setup();globalThis.fetch=async(input,options)=>{state.calls.push({url:String(input),options});return Response.json({contract:'liquidity-agent-research.v1'});};
  const response=await worker.fetch(new Request('https://justhodl-data-proxy.raafouis.workers.dev/liquidity-data.json',{method}),{},state.context);
  assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');
  assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);assert.equal(state.calls.length,1);
  assert.equal(new URL(state.calls[0].url).pathname,'/liquidity-data.json');assert.equal(state.calls[0].options.cache,'no-store');
 }
});

test('off-exchange current is never served from stale edge cache for GET, HEAD or Range',async()=>{
 const worker=(await import(source)).default;
 for(const [method,headers] of [['GET',{}],['HEAD',{}],['GET',{'Range':'bytes=0-20'}]]){
  const state=setup();globalThis.fetch=async(input,options)=>{state.calls.push({url:String(input),options});return Response.json({contract:'offexchange-original-research.v1'});};
  const response=await worker.fetch(new Request('https://justhodl.ai/data/dark-pool.json',{method,headers}),{},state.context);
  assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);assert.equal(state.calls.length,1);
  assert.equal(new URL(state.calls[0].url).pathname,'/data/dark-pool.json');
 }
});
test('exact reads bypass caches, attest the same key and expose that identity to the browser',async()=>{
 const state=setup(),worker=(await import(source)).default;
 const req=new Request('https://justhodl-data-proxy.raafouis.workers.dev/cot/extremes/current.json?exact=1');
 const response=await worker.fetch(req,{},state.context);await Promise.all(state.waits);
 assert.equal(response.status,200);assert.equal(response.headers.get('X-JH-Artifact-Key'),'cot/extremes/current.json');
 assert(response.headers.get('Access-Control-Expose-Headers').includes('X-JH-Artifact-Key'));
 assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);assert.equal(state.calls.length,1);
 assert.equal(response.headers.get('Cache-Control'),'no-store');
 const again=await worker.fetch(req,{},state.context);assert.equal(again.headers.get('X-JH-Artifact-Key'),'cot/extremes/current.json');assert.equal(state.calls.length,2);
});
test('exact missing keys never fall back to data aliases or trigger research generation',async()=>{
 const worker=(await import(source)).default;
 for(const key of ['repo-data.json','equity-research/ES.json']){
  const state=setup();const response=await worker.fetch(new Request('https://justhodl-data-proxy.raafouis.workers.dev/'+key+'?exact=1'),{},state.context);
  assert.equal(response.status,404);assert.equal(state.calls.length,1);assert.equal(new URL(state.calls[0]).pathname,'/'+key);
  assert.equal(state.stored.size,0);
 }
});
test('exact mode preserves private containment before all cache and origin reads',async()=>{
 const state=setup(),worker=(await import(source)).default;
 for(const key of ['data/brain.json','portfolio/snapshot.json','data/user-trades.json']){
  const response=await worker.fetch(new Request('https://justhodl-data-proxy.raafouis.workers.dev/'+key+'?exact=1'),{},state.context);
  assert([401,403].includes(response.status));
 }
 assert.equal(state.calls.length,0);assert.equal(state.keys.length,0);
});

test('DEFCON alias always reads the canonical Crisis packet without cache; exact mode retains identity',async()=>{
 const worker=(await import(source)).default;
 for(const suffix of ['', '?exact=1']){
  const state=setup();globalThis.fetch=async input=>{state.calls.push(String(input));return Response.json({key:new URL(String(input)).pathname});};
  const response=await worker.fetch(new Request('https://justhodl-data-proxy.raafouis.workers.dev/data/defcon.json'+suffix),{},state.context);
  const expected=suffix?'data/defcon.json':'data/crisis-composite.json';
  assert.equal(response.status,200);assert.equal(response.headers.get('X-JH-Artifact-Key'),expected);
  assert.equal(response.headers.get('Cache-Control'),'no-store');assert.equal((await response.json()).key,'/'+expected);
  assert.equal(state.calls.length,1);assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);
 }
});

test('Crisis current and history publications cannot be hidden behind edge caches',async()=>{
 const worker=(await import(source)).default;
 for(const key of ['crisis-composite.json','crisis-composite-history.json']){
  const state=setup();globalThis.fetch=async input=>{state.calls.push(String(input));return Response.json({ok:true});};
  const response=await worker.fetch(new Request('https://justhodl-data-proxy.raafouis.workers.dev/data/'+key),{},state.context);
  assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);
 }
});

test('Dollar, Cross-Asset Flow and Gold publications bypass edge and upstream caches for GET, HEAD and Range',async()=>{
 const worker=(await import(source)).default;
 for(const key of ['dollar-radar.json','cross-asset-flow-state.json','gold-equity-rotation.json'])for(const request of [new Request('https://justhodl.ai/data/'+key),
  new Request('https://justhodl.ai/data/'+key,{method:'HEAD'}),
  new Request('https://justhodl.ai/data/'+key,{headers:{Range:'bytes=0-9'}})]){
  const state=setup();
  globalThis.fetch=async(input,options)=>{state.calls.push({url:String(input),options});return Response.json({contract:'dollar-original-research.v1'});};
  const response=await worker.fetch(request,{},state.context);await Promise.all(state.waits);
  assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');
  assert.equal(state.keys.length,0);assert.equal(state.stored.size,0);assert.equal(state.calls.length,1);
  assert.equal(new URL(state.calls[0].url).pathname,'/data/'+key);
  assert.equal(state.calls[0].options.cache,'no-store');assert.equal(state.calls[0].options.cf,undefined);
 }
});
