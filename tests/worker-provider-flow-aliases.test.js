const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url');
test('legacy flow routes expose only the nine reviewed compatibility objects on each host',()=>{
 const source=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/wrangler.toml'),'utf8');
 const routes=[...source.matchAll(/pattern\s*=\s*"([^"]+)"/g)].map(m=>m[1]).filter(p=>p.includes('/etf-flows/'));
 const expected=['justhodl.ai','www.justhodl.ai'].flatMap(host=>['daily','measurements','composite','event-study','rotation','per-ticker-context','ai-analysis','constituent-pressure','stock-exposure-lookup'].map(n=>host+'/etf-flows/'+n+'.json'));
 assert.deepEqual(routes.slice().sort(),expected.sort());assert.ok(routes.every(p=>!p.includes('*')));
});
test('daily aliases still use the existing content review and reject raw transport diagnostics',async()=>{
 const {reviewedArtifact,serveReviewedArtifact}=await import(pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/reviewed-artifacts.js')).href);
 const previous=globalThis.fetch;
 try{
  for(const alias of ['etf-flows/daily.json','data/etf-flows/daily.json']){
   const review=reviewedArtifact(alias);assert.equal(review.key,'etf-flows/daily.json');
   const doc={contract:'provider-flow-compatibility.v1',source_key:'etf-flows/daily.json',metrics:[],calls_eligible:false};
   globalThis.fetch=async()=>Response.json(doc);
   const response=await serveReviewedArtifact(new Request('https://justhodl.ai/'+alias),review,'https://origin.test',{});
   assert.equal(response.status,200);assert.equal(response.headers.get('X-JH-Artifact-Key'),review.key);assert.deepEqual(await response.json(),doc);
   doc.raw_response='SYNTHETIC_PRIVATE_DIAGNOSTIC';
   assert.equal((await serveReviewedArtifact(new Request('https://justhodl.ai/'+alias),review,'https://origin.test',{})).status,503);
  }
 }finally{globalThis.fetch=previous;}
});
