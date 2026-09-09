const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path');
const {pathToFileURL}=require('node:url');
const source=pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js')).href;
function setup(){
 const calls=[],keys=[],stored=new Map(),waits=[];
 globalThis.caches={default:{async match(req){keys.push(req.url);return stored.get(req.url)?.clone();},async put(req,response){stored.set(req.url,response.clone());}}};
 globalThis.fetch=async(input)=>{const url=String(input);calls.push(url);return url.endsWith('/cot/extremes/current.json')?Response.json({schema_version:'cot-extremes.v2'}):new Response('{}',{status:404});};
 return {calls,keys,stored,waits,context:{waitUntil(promise){waits.push(promise);}}};
}
test('exact reads use a separate cache, attest the same key and expose that identity to the browser',async()=>{
 const state=setup(),worker=(await import(source)).default;
 const req=new Request('https://justhodl-data-proxy.raafouis.workers.dev/cot/extremes/current.json?exact=1');
 const response=await worker.fetch(req,{},state.context);await Promise.all(state.waits);
 assert.equal(response.status,200);assert.equal(response.headers.get('X-JH-Artifact-Key'),'cot/extremes/current.json');
 assert(response.headers.get('Access-Control-Expose-Headers').includes('X-JH-Artifact-Key'));
 assert(state.keys[0].includes('-exact-v1__'));assert.equal(state.calls.length,1);
 const again=await worker.fetch(req,{},state.context);assert.equal(again.headers.get('X-JH-Artifact-Key'),'cot/extremes/current.json');assert.equal(state.calls.length,1);
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
