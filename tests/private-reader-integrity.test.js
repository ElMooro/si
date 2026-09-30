const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const io=require('../jh-evidence-io.js'),root=path.join(__dirname,'..'),source=fs.readFileSync(path.join(root,'private-artifacts.js'),'utf8');
const tick=()=>new Promise(setImmediate);
function setup(options={}){
 const state={uid:'invented-owner',calls:[],loads:[],panels:[],inits:0,tokens:0,handlers:{},readerOptions:[]};
 const auth={init:async()=>{state.inits++;if(options.init)await options.init();},getUser:()=>state.uid?{id:state.uid}:null,onChange(fn){state.change=fn;},getAccessToken:async()=>{state.tokens++;return options.token?options.token():'invented-value-not-a-credential';}};
 const reader={...io,readComplete(open,opts){state.readerOptions.push(opts);return io.readComplete(open,{...opts,...(options.fast?{timeoutMs:5}:{})});}};
 const element=()=>({style:{},children:[],setAttribute(){},appendChild(child){this.children.push(child);},remove(){const at=state.panels.indexOf(this);if(at>=0)state.panels.splice(at,1);}});
 const document={baseURI:'https://justhodl.ai/synthetic-only.html',documentElement:element(),getElementById:id=>state.panels.find(p=>p.id===id),createElement:element,body:{prepend(node){state.panels.push(node);}},addEventListener(name,fn){state.handlers[name]=fn;},head:{appendChild(script){state.loads.push(script);if(options.missing==='stall')return;if(options.missing==='fail')return script.onerror();window.JHEvidenceIO=reader;script.onload();}}};
 const location={hostname:'justhodl.ai',pathname:'/synthetic-only.html',href:document.baseURI,reload(){state.reload=true;}};
 const window={JustHodlAuth:auth,fetch:async(url,init)=>{state.calls.push({url,init});return options.fetcher?options.fetcher(url,init):Response.json({zero:0,missing:null,owner:'invented'});},addEventListener(){}};
 if(!options.missing)window.JHEvidenceIO=reader;
 vm.runInNewContext(source,{window,document,location,URL,Request,Response,console,setTimeout:(fn,ms)=>setTimeout(fn,options.fast?Math.min(ms,5):ms),clearTimeout});
 return {state,window,auth,client:window.JustHodlPrivateArtifacts};
}

test('complete private read retains every byte, response status, headers and owner guard',async()=>{
 const payload=JSON.stringify({zero:0,missing:null,flag:false,unicode:'東京 — 😀',rows:Array.from({length:53},(_,i)=>({i,late:i===52}))}),raw=Buffer.from(payload);let offset=0;
 const {state,client}=setup({fetcher:async()=>new Response(new ReadableStream({pull(c){if(offset===raw.length)return c.close();const end=Math.min(offset+7,raw.length);c.enqueue(raw.subarray(offset,end));offset=end;}}),{headers:{'Content-Type':'application/json','X-Synthetic':'complete'}})});
 const response=await client.fetch('/portfolio/snapshot.json');assert.equal(response.status,200);assert.equal(response.headers.get('X-Synthetic'),'complete');assert.equal(await response.text(),payload);
 assert.equal(state.calls.length,1);assert.equal(state.readerOptions[0].limit,32*1024*1024);assert.ok(state.readerOptions[0].timeoutMs<=12000);
 assert.equal(state.panels.length,0);assert.equal(state.calls[0].init.headers.Authorization,'Bearer invented-value-not-a-credential');
});

test('oversize upstream bytes fail before assembling a whole array buffer or returning a prefix',async()=>{
 let cancelled=0,arrayBuffer=0;
 const {state,client}=setup({fetcher:async()=>({status:200,headers:new Headers(),arrayBuffer(){arrayBuffer++;throw Error('Unbounded method must never be called');},body:{getReader:()=>({read:async()=>({done:false,value:new Uint8Array(32*1024*1024+1)}),cancel(){cancelled++;},releaseLock(){}})}})});
 const response=await client.fetch('/portfolio/snapshot.json');assert.equal(response.status,503);assert.equal((await response.json()).private_account,true);assert.equal(arrayBuffer,0);assert.equal(cancelled,1);assert.equal(state.calls.length,1);
});

test('a body failure after a valid prefix never returns partial owner evidence',async()=>{
 let n=0,cancelled=0;const {client,state}=setup({fetcher:async()=>({status:200,headers:new Headers(),body:{getReader:()=>({read:async()=>{if(n++)throw Error('invented source diagnostic must stay private');return {value:Buffer.from('{"prefix":true}'),done:false};},cancel(){cancelled++;},releaseLock(){}})}})});
 const response=await client.fetch('/portfolio/risk.json');assert.equal(response.status,503);const text=await response.text();assert.ok(!text.includes('prefix')&&!text.includes('diagnostic'));assert.equal(cancelled,1);assert.equal(state.calls.length,1);
});

test('already-cancelled reads perform no auth, helper bootstrap, request or outage rendering',async()=>{
 const {client,state}=setup({missing:'stall'}),controller=new AbortController();controller.abort();
 await assert.rejects(client.fetch('/portfolio/snapshot.json',{signal:controller.signal}),{name:'AbortError'});
 assert.equal(state.inits,0);assert.equal(state.loads.length,0);assert.equal(state.calls.length,0);assert.equal(state.panels.length,0);
});

test('cancellation during auth and token waits cannot later issue an authenticated request',async()=>{
 for(const stage of ['init','token']){
  let resolve;const pending=new Promise(r=>resolve=r),{client,state}=setup({[stage]:()=>pending}),controller=new AbortController();
  const result=client.fetch('/portfolio/snapshot.json',{signal:controller.signal});await tick();controller.abort();await assert.rejects(result,{name:'AbortError'});resolve('invented-late-token');await tick();
  assert.equal(state.calls.length,0);assert.equal(state.panels.length,0);
 }
});

test('cancellation closes an active reader and does not show an outage notice',async()=>{
 let cancelled=0,released=0;const {client,state}=setup({fetcher:async()=>({status:200,headers:new Headers(),body:{getReader:()=>({read:()=>new Promise(()=>{}),cancel(){cancelled++;},releaseLock(){released++;}})}})}),controller=new AbortController();
 const pending=client.fetch('/portfolio/risk.json',{signal:controller.signal});await tick();controller.abort();await assert.rejects(pending,{name:'AbortError'});
 assert.equal(cancelled,1);assert.equal(released,1);assert.equal(state.panels.length,0);
});

test('request and body deadlines settle, cancel late bodies, and do not retry providers',async()=>{
 let release,cancelled=0;const {client,state}=setup({fast:true,fetcher:()=>new Promise(r=>release=r)});
 const response=await client.fetch('/portfolio/risk.json');assert.equal(response.status,503);assert.equal(state.calls[0].init.signal.aborted,true);
 release({status:200,headers:new Headers(),body:{cancel(){cancelled++;}}});await tick();assert.equal(cancelled,1);assert.equal(state.calls.length,1);
 const stalled=setup({fast:true,fetcher:async()=>({status:200,headers:new Headers(),body:{getReader:()=>({read:()=>new Promise(()=>{}),cancel(){cancelled++;},releaseLock(){}})}})});
 assert.equal((await stalled.client.fetch('/portfolio/risk.json')).status,503);assert.equal(cancelled,2);assert.equal(stalled.state.calls.length,1);
});

test('changing identity while reading discards the complete prior-owner body',async()=>{
 let release;const {client,state}=setup({fetcher:()=>new Response(new ReadableStream({start(c){release=()=>{c.enqueue(Buffer.from('{"private":"invented-prior-owner"}'));c.close();};}}))});
 const pending=client.fetch('/portfolio/risk.json');await tick();state.uid='invented-new-owner';state.change({id:state.uid});release();
 const response=await pending;assert.equal(response.status,401);assert.ok(!(await response.text()).includes('prior-owner'));assert.equal(state.reload,true);
});

test('HEAD and bodyless statuses preserve HTTP semantics and close unexpected bodies',async()=>{
 for(const status of [204,205,304]){const {client}=setup({fetcher:async()=>new Response(null,{status})});const r=await client.fetch('/portfolio/risk.json');assert.equal(r.status,status);assert.equal(await r.text(),'');}
 let cancelled=0;const {client}=setup({fetcher:async()=>({status:200,headers:new Headers({'X-Test':'head'}),body:{cancel(){cancelled++;}}})});
 const r=await client.fetch(new Request('https://justhodl.ai/portfolio/risk.json',{method:'HEAD'}));assert.equal(r.status,200);assert.equal(await r.text(),'');assert.equal(cancelled,1);assert.equal(r.headers.get('X-Test'),'head');
});

test('complete upstream denial is preserved without fallback or diagnostic interpolation',async()=>{
 const {client,state}=setup({fetcher:async()=>Response.json({error:'invented denial'},{status:403})});const response=await client.fetch('/portfolio/risk.json');
 assert.equal(response.status,403);assert.deepEqual(await response.json(),{error:'invented denial'});assert.equal(state.calls.length,1);assert.match(state.panels[0].children[0].textContent,/only to its owner/);
});

test('absent shared reader bootstraps once for concurrent reads before any private request',async()=>{
 const {client,state}=setup({missing:'load'});await Promise.all([client.fetch('/portfolio/risk.json'),client.fetch('/portfolio/snapshot.json')]);
 assert.equal(state.loads.length,1);assert.equal(state.loads[0].src,'/jh-evidence-io.js?v=20260930');assert.equal(state.calls.length,2);assert.equal(state.inits,1);
});

test('helper bootstrap failure and stall cannot issue data requests; cancellation stays silent',async()=>{
 for(const missing of ['fail','stall']){const {client,state}=setup({missing,fast:true});assert.equal((await client.fetch('/portfolio/risk.json')).status,503);assert.equal(state.calls.length,0);assert.equal(state.inits,0);}
 const {client,state}=setup({missing:'stall'}),controller=new AbortController(),pending=client.fetch('/portfolio/risk.json',{signal:controller.signal});await tick();controller.abort();await assert.rejects(pending,{name:'AbortError'});assert.equal(state.calls.length,0);assert.equal(state.panels.length,0);
});

test('read notices clear only after every affected feed recovers and can change denial type',async()=>{
 const statuses=new Map([['portfolio-risk',503],['portfolio-snapshot',503]]);
 const {client,state}=setup({fetcher:async url=>Response.json({}, {status:statuses.get(new URL(url).searchParams.get('kind'))})});
 await client.fetch('/portfolio/risk.json');await client.fetch('/portfolio/snapshot.json');assert.equal(state.panels.length,1);
 statuses.set('portfolio-risk',200);await client.fetch('/portfolio/risk.json');assert.equal(state.panels.length,1);
 statuses.set('portfolio-snapshot',401);await client.fetch('/portfolio/snapshot.json');assert.match(state.panels[0].children[0].textContent,/Sign in/);assert.equal(state.panels[0].children.length,2);
 statuses.set('portfolio-snapshot',403);await client.fetch('/portfolio/snapshot.json');assert.match(state.panels[0].children[0].textContent,/only to its owner/);assert.equal(state.panels[0].children.length,1);
 statuses.set('portfolio-snapshot',200);await client.fetch('/portfolio/snapshot.json');assert.equal(state.panels.length,0);
});

test('an older read failure cannot restore an outage banner after the latest success',async()=>{
 let release,n=0;const {client,state}=setup({fetcher:async()=>++n===1?new Promise(resolve=>release=resolve):Response.json({fresh:true})});
 const pending=client.fetch('/portfolio/risk.json');await tick();assert.equal((await client.fetch('/portfolio/risk.json')).status,200);
 release(Response.json({},{status:503}));assert.equal((await pending).status,503);assert.equal(state.panels.length,0);
});

test('successful reads cannot clear a notice belonging to the unchanged account mutation path',async()=>{
 const {client,state}=setup();const existing={id:'private-account-status',children:[{textContent:'A separate operation notice'}]};state.panels.push(existing);
 assert.equal((await client.fetch('/portfolio/risk.json')).status,200);assert.equal(state.panels.length,1);assert.equal(state.panels[0],existing);
});

test('a separate operation error sharing an existing read notice survives read recovery',async()=>{
 let healthy=false;const {client,state}=setup({fetcher:async(_url,options)=>Response.json({}, {status:options.method==='POST'||!healthy?503:200})});
 assert.equal((await client.fetch('/portfolio/risk.json')).status,503);const panel=state.panels[0];
 // Only an in-memory fetch adapter executes here; no account operation is sent.
 assert.equal((await client.fetch('/owner-api/watchlist',{method:'POST',body:'{"synthetic":true}'})).status,503);
 healthy=true;assert.equal((await client.fetch('/portfolio/risk.json')).status,200);assert.equal(state.panels.length,1);assert.equal(state.panels[0],panel);
});

test('complete predecessor is inert and the prior public, mutation and route policies stay exact',()=>{
 const bytes=fs.readFileSync(path.join(__dirname,'fixtures/pre-private-reader-private-artifacts.js.txt'));
 assert.equal(bytes.length,8023);assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),'240e5536c4b33210f8e4fb5587e8bab4d455da146fbed13c74937ce275696e6d');
 const old=bytes.toString('utf8'),begin='  async function privateFetch(input, init) {';
 const tail=source.slice(source.indexOf(begin)).replace("    if (method === 'GET' || method === 'HEAD') return privateRead(kind, ownerApi, method, init?.signal || input?.signal);\n",'');
 assert.equal(tail,old.slice(old.indexOf(begin)));
 assert.equal(source.slice(0,source.indexOf('  // Read-only acquisition')),old.slice(0,old.indexOf(begin)));
});
