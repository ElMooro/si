const test = require('node:test');
const assert = require('node:assert/strict');
const {pathToFileURL} = require('node:url');
const path = require('node:path');
const URL = 'https://'+'a'.repeat(32)+'.lambda-url.us-east-1.on.aws/';
const load = () => import(pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/ask_desk_api.js')).href);
const helpers = role => ({resolveIdentity: async()=>({role}), corsHeaders:()=>({'Access-Control-Allow-Origin':'*'}),
  boundedBody:async r=>{const s=await r.text();return s.length<=8192?s:null;},jsonResp:(d,s)=>Response.json(d,{status:s}),unauthorized:e=>Response.json({error:e},{status:401}),forbidden:e=>Response.json({error:e},{status:403})});
function req(body='{"question":"test"}',method='POST') {return new Request('https://api.justhodl.ai/ask-desk',{method,body:method==='POST'?body:undefined});}
test('anonymous and non-owner requests never fetch pointer or Lambda',async()=>{
  const {handleAskDesk}=await load();global.fetch=()=>{throw Error('must not fetch');};
  assert.equal((await handleAskDesk(req(),{ADMIN_TOKEN:'fixture'},helpers('anon'))).status,401);
  assert.equal((await handleAskDesk(req(),{ADMIN_TOKEN:'fixture'},helpers('user'))).status,403);
});
test('owner request sends only validated question and internal identity to source-bound endpoint',async()=>{
  const {handleAskDesk}=await load();const calls=[];
  global.fetch=async (url,options)=>{calls.push({url,options});return options?Response.json({answer:'test'}):Response.json({schema_version:'public-api-pointer.v1',function:'justhodl-ask-desk',url:URL});};
  const result=await handleAskDesk(req('{"question":" test ","test_question":"bypass"}'),{ADMIN_TOKEN:'fixture'},helpers('owner'));
  assert.equal(result.status,200);assert.equal(result.headers.get('cache-control'),'private, no-store');
  assert.equal(calls[1].url,URL);assert.equal(calls[1].options.body,'{"question":"test"}');assert.equal(calls[1].options.headers['X-JH-Service-Token'],'fixture');
});
test('invalid pointer never receives credentials and errors remain bounded',async()=>{
  const {handleAskDesk}=await load();let calls=0;
  global.fetch=async()=>{calls++;return Response.json({schema_version:'public-api-pointer.v1',function:'justhodl-ask-desk',url:'https://evil.invalid'});};
  assert.equal((await handleAskDesk(req(),{ADMIN_TOKEN:'fixture'},helpers('owner'))).status,503);assert.equal(calls,1);
  global.fetch=async()=>{throw Error('PRIVATE_UPSTREAM_ERROR');};
  assert.ok(!(await (await handleAskDesk(req(),{ADMIN_TOKEN:'fixture'},helpers('owner'))).text()).includes('PRIVATE_UPSTREAM_ERROR'));
});
test('oversize or non-string question is rejected before endpoint discovery',async()=>{
  const {handleAskDesk}=await load();global.fetch=()=>{throw Error('must not fetch');};
  assert.equal((await handleAskDesk(req(JSON.stringify({question:'x'.repeat(601)})),{ADMIN_TOKEN:'fixture'},helpers('owner'))).status,400);
  assert.equal((await handleAskDesk(req('{"question":[] }'),{ADMIN_TOKEN:'fixture'},helpers('owner'))).status,400);
  assert.equal((await handleAskDesk(req('x'.repeat(8193)),{ADMIN_TOKEN:'fixture'},helpers('owner'))).status,413);
});
