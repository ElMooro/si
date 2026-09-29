const test=require('node:test'), assert=require('node:assert/strict'), crypto=require('node:crypto');
const api=require('../auction-desk-delivery.js'), fixture=require('./fixtures/auction-delivery.json');
const copy=x=>structuredClone(x), primary='https://justhodl-data-proxy.raafouis.workers.dev/';
function harness(change, options={}) {
  const f=copy(fixture),calls=[];
  if(change)change(f);
  const fetch=async (url,request)=>{
    calls.push(String(url));const key=new URL(url).pathname.slice(1);
    if(options.fetch){const result=options.fetch(String(url),key,request,f);if(result)return result;}
    if(key==='data/auction-desk-view.json')return new Response(JSON.stringify(f.locator));
    if(f.artifacts[key])return new Response(f.artifacts[key]);
    if(key==='data/auction-desk.json' && options.legacy)return new Response(JSON.stringify(options.legacy));
    return new Response('missing',{status:404});
  };
  const client=api.createClient({fetch,crypto:crypto.webcrypto,timeout:options.timeout||1000});
  return {client,f,calls};
}
function retain(f,value,category){
  const body=JSON.stringify(value),bytes=Buffer.byteLength(body),sha256=crypto.createHash('sha256').update(body).digest('hex');
  const ref={key:'data/auction-desk-delivery/'+category+'/'+sha256+'.json',sha256,bytes,encoding:'json'};
  f.artifacts[ref.key]=body;return ref;
}
function changeView(f,mutate){
  const view=JSON.parse(f.artifacts[f.locator.view.key]),manifest=JSON.parse(f.artifacts[f.locator.manifest.key]);
  mutate(view);f.locator.view=retain(f,view,'views');manifest.view=f.locator.view;f.locator.manifest=retain(f,manifest,'manifests');
}
test('initial load is compact and every requested operation reconstructs the exact complete row',async()=>{
  const h=harness(),view=await h.client.load();
  assert.equal(h.calls.length,3);assert.ok(h.calls.every(url=>!url.includes('/rows/')&&!url.endsWith('.gz')));
  assert.match(h.calls[0],/auction-desk-view\.json\?exact=1&nogen=1$/);
  assert.equal(view.buybacks.operations.length,fixture.packet.buybacks.operations.length);
  for(const i of [0,1,20,44])assert.deepEqual(await h.client.detail(view,view.buybacks.operations[i]),fixture.packet.buybacks.operations[i]);
  assert.deepEqual(await h.client.program(view),fixture.packet.buybacks.program);
  assert.match(h.client.source(view).url,/snapshots\/[a-f0-9]{64}\.json\.gz$/);
  assert.equal(view.delivery.sizing_eligible,false);assert.equal(view.reactions.supplied_input_replay_available,false);
});
test('deadline covers a response body that never settles even when abort is ignored',async()=>{
  let canceled=false;
  const stuck=()=>new Response(new ReadableStream({pull:()=>new Promise(()=>{}),cancel:()=>{canceled=true;}}));
  const h=harness(null,{timeout:10,fetch:(url,key)=>url.startsWith(primary)&&key==='data/auction-desk-view.json'?stuck():null});
  const view=await h.client.load();assert.equal(view.engine,'justhodl-auction-desk');assert.ok(canceled);
  assert.ok(h.calls.some(url=>url.includes('.s3.amazonaws.com/')));
});
test('deadline covers fetch itself even when transport ignores abort',async()=>{
  const h=harness(null,{timeout:10,fetch:(url)=>url.startsWith(primary)?new Promise(()=>{}):null});
  assert.equal((await h.client.load()).engine,'justhodl-auction-desk');
});
test('error-shaped HTTP 200 JSON cannot become a desk snapshot',async()=>{
  const h=harness(null,{fetch:()=>new Response('{"error":"gateway"}')});
  await assert.rejects(h.client.load(),/Desk publication unavailable/);
});
test('truncated or altered artifact bytes cannot be displayed',async()=>{
  for(const mutate of [s=>s.slice(0,-1),s=>s.replace('justhodl-auction-desk','justhodl-auction-xxxx')]){
    const h=harness(f=>{f.artifacts[f.locator.view.key]=mutate(f.artifacts[f.locator.view.key]);});
    await assert.rejects(h.client.load(),/length differs|checksum differs|byte limit/);
  }
});
test('manifest dates view identity and false permission flags must agree',async()=>{
  for(const mutate of [f=>f.locator.generated_at='2026-09-28T00:00:00Z',f=>f.locator.sizing_eligible=true,
                      f=>changeView(f,v=>v.delivery.source_packet.sha256='0'.repeat(64)),
                      f=>changeView(f,v=>v.buybacks.operations[0].delivery_detail.artifact.key='data/private/account.json')]){
    const h=harness(mutate);await assert.rejects(h.client.load());
    assert.ok(h.calls.every(url=>!url.includes('/private/')));
  }
});
test('a valid checksum cannot make the wrong row index match the displayed operation',async()=>{
  const h=harness(f=>changeView(f,v=>{v.buybacks.operations[0].delivery_detail.index=1;}));
  const view=await h.client.load();await assert.rejects(h.client.detail(view,view.buybacks.operations[0]),/differs/);
});
test('operation and aggregate display alterations are rejected when full inputs load',async()=>{
  const h=harness(f=>changeView(f,v=>{v.buybacks.operations[0].accepted+=1;v.buybacks.program.avg_fill_pct=999;}));
  const view=await h.client.load();
  await assert.rejects(h.client.detail(view,view.buybacks.operations[0]),/differs/);
  await assert.rejects(h.client.program(view),/differs/);
});
test('detail request from another snapshot cannot reuse the current evidence context',async()=>{
  const h=harness(),view=await h.client.load();
  await assert.rejects(h.client.detail(copy(view),view.buybacks.operations[0]),/displayed snapshot/);
  await assert.rejects(h.client.detail(view,copy(view.buybacks.operations[0])),/displayed snapshot/);
});
test('byte bound applies during streaming, without trusting a content-length header',async()=>{
  const fetch=async()=>new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array(5));c.enqueue(new Uint8Array(6));c.close();}}));
  await assert.rejects(api.bytes('fixture',10,{fetch,timeout:100}),/byte limit/);
});
test('legacy complete packets need valid shape and cannot bypass the projection manifest',async()=>{
  const legacy=copy(fixture.packet);
  const missing=(url,key)=>key==='data/auction-desk-view.json'?new Response('missing',{status:404}):null;
  const h=harness(null,{fetch:missing,legacy});assert.deepEqual(await h.client.load(),legacy);assert.equal(h.client.source(legacy),null);
  const bad=harness(null,{fetch:missing,legacy:{...legacy,delivery:{contract:api.CONTRACT}}});await assert.rejects(bad.client.load(),/projection requires/);
  legacy.today.auctions={};const wrong=harness(null,{fetch:missing,legacy});await assert.rejects(wrong.client.load(),/operation array/);
});
test('checksum and stream facilities fail closed instead of displaying unverifiable inputs',async()=>{
  const noCrypto=api.createClient({crypto:{},timeout:100,fetch:async url=>{
    const key=new URL(url).pathname.slice(1);return new Response(key==='data/auction-desk-view.json'?JSON.stringify(fixture.locator):fixture.artifacts[key]||'{}');
  }});await assert.rejects(noCrypto.load(),/checksum verification unavailable/);
  await assert.rejects(api.bytes('fixture',100,{fetch:async()=>({ok:true,body:null}),timeout:100}),/streaming unavailable/);
});
test('a cached artifact cannot bypass a changed byte reference',async()=>{
  const h=harness(),view=await h.client.load(),row=view.buybacks.operations[0];
  await h.client.detail(view,row);
  row.delivery_detail.artifact={...row.delivery_detail.artifact,bytes:row.delivery_detail.artifact.bytes-1};
  await assert.rejects(h.client.detail(view,row),/Cached evidence reference differs/);
});
