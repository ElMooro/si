const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-commentary-review.js'),NOW=Date.parse('2026-09-27T10:00:00Z');
const flags={calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function packet(){return{contract:'commentary-research.v1',page:'risk-desk',generated_at:'2026-09-26T14:00:00Z',model:'deterministic-source-inventory',model_api_calls:0,portfolio_action:'WAIT',...flags,commentary:{mode:'research_inventory',headline:'<img src=x onerror=bad()>',primary_risks:'<script>untrusted()</script>',hedge_recommendation:'Evidence needs validation',leading_indicators:'WAIT means abstain',risk_score:null,model_api_calls:0,posture:'WAIT',...flags},unknown:[0,false,null,9007199254740991]};}
const complete=p=>({packet:p,raw:JSON.stringify(p,null,2)});
class Element{
 constructor(tag,doc){this.tagName=tag.toUpperCase();this.doc=doc;this.children=[];this.textContent='';this.attributes={};this.hidden=false;this.open=false;}
 appendChild(n){this.children.push(n);return n;}replaceChildren(){this.children=[];this.textContent='';}setAttribute(k,v){this.attributes[k]=v;}all(){return[this,...this.children.flatMap(n=>n.all())];}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
 set innerHTML(x){throw Error('HTML interpolation forbidden');}
}
function document(){const d={createElement(tag){return new Element(tag,d);},getElementById(id){return[...d.head.all(),...d.body.all()].find(n=>n.id===id)||null;}};d.head=new Element('head',d);d.body=new Element('body',d);const panel=new Element('div',d);panel.id='ai-brief-panel';d.body.appendChild(panel);return d;}
function timers(){const tasks=new Set();return{tasks,setInterval(fn,ms){assert.equal(ms,60000);tasks.add(fn);return fn;},clearInterval(fn){tasks.delete(fn);}};}
const flush=async()=>{for(let i=0;i<3;i++)await new Promise(r=>setImmediate(r));};

test('exact public routes refuse account and learning-dependent reads before transport',async()=>{
 for(const route of ['/portfolio.html','/signals.html','/pre-pump-radar.html','/screener/index.html','/other/risk-desk.html','/risk-desk.html/']){
  let reads=0;const s=api.init(document(),{path:()=>route,timers:timers(),loader:async()=>{reads++;return complete(packet());}});await flush();await s.read();assert.equal(reads,0);assert.match(s.status.textContent,/does not access account/);assert.equal(s.refresh.hidden,true);s.stop();
 }
 for(const page of ['portfolio','signals','../risk-desk'])await assert.rejects(api.load(page),/Exact page identity/);
});
test('new native commentary renders only inert text and preserves every raw field',async()=>{
 const d=document(),t=timers(),raw=complete(packet()),s=api.init(d,{path:()=>'/risk-desk.html',now:()=>NOW,timers:t,loader:async()=>raw});await flush();
 assert.match(s.content.text(),/<img src=x/);assert.match(s.content.text(),/<script>untrusted/);assert.equal(d.body.all().filter(n=>['IMG','SCRIPT'].includes(n.tagName)).length,0);
 assert.match(s.status.textContent,/20.0 hours old/);assert.ok(!s.content.text().includes('risk_score'));s.details.open=true;s.details.ontoggle();assert.equal(s.original.textContent,raw.raw);assert.equal(api.init(d),null);s.stop();assert.equal(t.tasks.size,0);
});
test('legacy narratives and self-declared confidence stay in complete original disclosure',async()=>{
 const p=packet();delete p.contract;p.commentary={headline:'BUY NOW',risk_score:99,nested:{privateInference:'legacy claim'},unknown:[0,null]};const s=api.init(document(),{path:()=>'/risk-desk.html',now:()=>NOW,timers:timers(),loader:async()=>complete(p)});await flush();
 assert.match(s.content.text(),/Earlier commentary/);assert.ok(!s.content.text().includes('BUY NOW'));assert.ok(!s.content.text().includes('99'));s.details.open=true;s.details.ontoggle();assert.equal(JSON.parse(s.original.textContent).commentary.risk_score,99);s.stop();
});
test('invalid page clocks permissions and missing required text refuse display',()=>{
 for(const patch of [{page:'portfolio'},{generated_at:'2026-02-30T00:00:00Z'},{generated_at:'2030-01-01T00:00:00Z'},{generated_at:'2026-09-26'},{calls_eligible:true},{model_api_calls:1},{portfolio_action:'LONG'}])assert.throws(()=>api.view({...packet(),...patch},'risk-desk',NOW));
 const p=packet();p.commentary.sizing_eligible=true;assert.throws(()=>api.view(p,'risk-desk',NOW));p.commentary.sizing_eligible=false;delete p.commentary.primary_risks;assert.throws(()=>api.view(p,'risk-desk',NOW));
});
test('failed refresh clears old content and stale completion cannot resurrect it',async()=>{
 let failure=false;const s=api.init(document(),{path:()=>'/risk-desk.html',now:()=>NOW,timers:timers(),loader:async()=>{if(failure)throw Error('HTTP 429');return complete(packet());}});await flush();const oldToggle=s.details.ontoggle;failure=true;await s.read();oldToggle();assert.equal(s.original.textContent,'Unavailable');assert.equal(s.content.children.length,0);assert.match(s.status.textContent,/HTTP 429/);s.stop();
 const waiting=[];const x=api.init(document(),{path:()=>'/risk-desk.html',now:()=>NOW,timers:timers(),loader:()=>new Promise((resolve,reject)=>waiting.push({resolve,reject}))});const next=x.read();waiting[1].reject(Error('latest unavailable'));await next;waiting[0].resolve(complete(packet()));await flush();assert.match(x.status.textContent,/latest unavailable/);assert.equal(x.content.children.length,0);x.stop();
});
test('route exit and clock rollback clear content; age updates make no network request',async()=>{
 let route='/risk-desk.html',now=NOW,reads=0;const t=timers(),s=api.init(document(),{path:()=>route,now:()=>now,timers:t,loader:async()=>{reads++;return complete(packet());}});await flush();now+=86400000;s.age();assert.match(s.status.textContent,/44.0 hours old/);assert.equal(reads,1);now=Date.parse('2020-01-01');s.age();assert.equal(s.content.children.length,0);assert.match(s.status.textContent,/nonfuture/);route='/portfolio.html';s.age();assert.equal(t.tasks.size,0);await s.read();assert.equal(reads,1);
});
test('complete same-origin reader preserves large original numbers and rejects malformed transport',async()=>{
 const raw=JSON.stringify(packet()).replace('9007199254740991','9007199254740993'),bytes=new TextEncoder().encode(raw);let i=0,request;
 const result=await api.load('risk-desk',{now:NOW,fetcher:async(url,options)=>{request={url,options};return{ok:true,body:{getReader:()=>({read:async()=>i<bytes.length?{done:false,value:bytes.slice(i,(i+=17))}:{done:true},cancel:async()=>{}})}};}});
 assert.equal(result.raw,raw);assert.equal(request.url,'/data/ai-commentary/risk-desk.json?exact=1&nogen=1');assert.equal(request.options.redirect,'error');assert.equal(request.options.cache,'no-store');
 for(const body of [new Uint8Array([255]),new TextEncoder().encode('{"x":1,"x":2}'),new TextEncoder().encode('{"x":1e999}')]){let done=false;await assert.rejects(api.load('risk-desk',{now:NOW,fetcher:async()=>({ok:true,body:{getReader:()=>({read:async()=>done?{done:true}:(done=true,{value:body,done:false}),cancel:async()=>{}})}})}));}
 let calls=0;await assert.rejects(api.load('risk-desk',{fetcher:async()=>{calls++;return{ok:false,status:403};}}),/HTTP 403/);assert.equal(calls,1);await assert.rejects(api.load('risk-desk',{timeout:5,fetcher:()=>new Promise(()=>{})}),/timed out/);
});
test('raw/parsed disagreement fails and every old panel is fully preserved',async()=>{
 const s=api.init(document(),{path:()=>'/risk-desk.html',now:()=>NOW,timers:timers(),loader:async()=>({packet:packet(),raw:'{}'})});await flush();assert.equal(s.content.children.length,0);assert.match(s.status.textContent,/original packet differs/);s.stop();
 const hashes={'fundamentals.html':'d17a527bc9eb7d3817a9906f733dd63f2c5198014e7f64b80b3b495916364b02','risk-desk.html':'0fc0c4c593853c2718d050eb98097ee2bc06cc811e815c7768f9a1048ce8ab88','pre-pump-radar.html':'e5180b8c16354514e725ece4b447b37800e7f69e912f2f746d23ddb2ee65c983','signals.html':'ef593d56519044c0725f5e6b4308f3be393eb0ad176d12168a0804a57cea46df','screener/index.html':'49f0fa80e96612942fb08b2c2154fea76a9b9ff4d58127bf030309dbc7fccebc','portfolio.html':'5fd584cdfc94aed6a0061389dfabb22f58928e1b95502f904f6a4947b15c444d'};
 for(const name of Object.keys(hashes)){
  const current=fs.readFileSync(path.join(__dirname,'..',name),'utf8'),old=fs.readFileSync(path.join(__dirname,'fixtures','pre-commentary-reader-'+name.replace('/','-')+'.txt'),'utf8');assert.equal(require('node:crypto').createHash('sha256').update(old).digest('hex'),hashes[name]);assert.ok(old.includes('data/ai-commentary/${PAGE}'));assert.ok(!current.includes('data/ai-commentary/${PAGE}'));assert.ok(current.includes('src="/jh-commentary-review.js"'));assert.ok(!current.includes('first run after deployment populates within 2 minutes'));
 }
});
