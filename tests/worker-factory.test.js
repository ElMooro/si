const test = require('node:test');
const assert = require('node:assert/strict');
const { pathToFileURL } = require('node:url');
const path = require('node:path');
const base = 'https://abcdefghijklmnopqrstuvwx.lambda-url.us-east-1.on.aws/';
const file = path.join(__dirname, '../cloudflare/workers/justhodl-data-proxy/src/factory-gateway.js');
async function run(action, identity, options = {}) {
  const { factoryGateway } = await import(pathToFileURL(file).href);
  const calls=[]; const saved=global.fetch;
  global.fetch=async (url,init)=>{calls.push({url,init});return Response.json({ok:true});};
  const method=options.method||'POST';
  const request=new Request('https://justhodl.ai/api/v1/factory/'+action, {method,
    headers:{'X-JH-Factory-Role':'owner','X-JH-Factory-Uid':'forged-user'},body:method==='POST'?'{}':undefined});
  try {
    const response=await factoryGateway(request,{ADMIN_TOKEN:'test-service-identity',AI_LAMBDA_URL:base},new URL(request.url),{
      resolveIdentity:async()=>identity,aiLambdaUrl:async()=>base,corsHeaders:()=>({}),
      boundedBody:async()=>options.oversize?null:'{}',jsonResp:(body,status)=>Response.json(body,{status})
    });
    return {response,calls};
  } finally {global.fetch=saved;}
}
test('factory denies anonymous and guest control before reaching Brain',async()=>{
  for (const [action,identity,status] of [['traces',{role:'anon'},401],['control',{role:'user',uid:'real-guest'},403],['train/classifier',{role:'owner',uid:'owner'},404]]) {
    const r=await run(action,identity);assert.equal(r.response.status,status);assert.equal(r.calls.length,0);
  }
});
test('factory identity headers come only from verified identity, never caller headers',async()=>{
  const r=await run('traces',{role:'user',uid:'real-guest'});assert.equal(r.response.status,200);
  assert.equal(r.calls.length,1);assert.equal(r.calls[0].url,base+'factory/traces');
  assert.equal(r.calls[0].init.headers['X-JH-Factory-Uid'],'real-guest');
  assert.equal(r.calls[0].init.headers['X-JH-Factory-Role'],'user');
  assert.equal(r.calls[0].init.redirect,'error');
});
test('factory bounds bodies and prevents wrong verbs',async()=>{
  assert.equal((await run('traces',{role:'user',uid:'real'}, {oversize:true})).response.status,413);
  assert.equal((await run('sandbox',{role:'user',uid:'real'})).response.status,405);
});
