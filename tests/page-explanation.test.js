const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-page-ai.js'),NOW=Date.parse('2026-09-27T08:00:00Z');
const packet=()=>({page:'macro-leads',generated_at:'2026-09-26T12:00:00Z',title:'Original title',what_it_is:'A published research note',what_it_does:'Source interpretation',analysis:'<img src=x onerror=alert(1)>',pick_read:'<script>untrusted()</script>',outlook:{alpha_status:'ALPHA_PROVEN',confidence:'HIGH',hit_rate_pct:99,mean_excess_vs_spy_pct:123456,n_graded:999},unknown:[0,null,false]});
const complete=p=>({packet:p,raw:JSON.stringify(p,null,2)});
class Element{
 constructor(tag,doc){this.tagName=tag.toUpperCase();this.doc=doc;this.children=[];this.textContent='';this.attributes={};this.hidden=false;this.disabled=false;this.open=false;}
 appendChild(n){this.children.push(n);n.parentElement=this;return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}setAttribute(k,v){this.attributes[k]=v;}focus(){this.doc.activeElement=this;}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}all(){return[this,...this.children.flatMap(n=>n.all())];}
 set innerHTML(value){throw Error('HTML interpolation is forbidden: '+value);}
}
function document(){const doc={createElement(tag){return new Element(tag,doc);},getElementById(id){return[...doc.head.all(),...doc.body.all()].find(n=>n.id===id)||null;}};doc.head=new Element('head',doc);doc.body=new Element('body',doc);return doc;}
function timers(){let id=0;const tasks=new Map();return{tasks,setInterval(fn,ms){assert.equal(ms,60000);tasks.set(++id,fn);return id;},clearInterval(i){tasks.delete(i);}};}
const flush=async()=>{for(let i=0;i<3;i++)await new Promise(resolve=>setImmediate(resolve));};
test('exact root-page identity never collapses nested routes, traversal or malformed publication clocks',()=>{
 assert.equal(api.pageName('/'),'index');assert.equal(api.pageName('/macro-leads.html'),'macro-leads');assert.equal(api.pageName('/MACRO-LEADS.HTML'),'macro-leads');
 for(const p of ['/rotation/index.html','/nested/macro-leads.html','//macro-leads.html','/%2e%2e/index.html','/macro-leads.html/','/../index.html'])assert.equal(api.pageName(p),null);
 const p=packet();assert.equal(api.view(p,'macro-leads',NOW).authority,false);assert.equal(api.view(p,'macro-leads',NOW).ageHours,20);assert.throws(()=>api.view(p,'index',NOW));
 for(const at of ['2026-09-28T00:00:00Z','2026-02-30T00:00:00Z','2026-09-26','2026-09-26T24:00:00Z','2026-09-26T01:00:00+25:00']){p.generated_at=at;assert.throws(()=>api.view(p,'macro-leads',NOW));}
});
test('all narrative text is inert; original score and unknown fields remain in the complete raw packet',async()=>{
 const doc=document(),t=timers(),p=packet(),raw=complete(p),s=api.init(doc,{path:()=>'/macro-leads.html',now:()=>NOW,timers:t,loader:async()=>raw});s.button.onclick();await flush();
 assert.equal(s.panel.hidden,false);assert.equal(s.button.attributes['aria-expanded'],'true');assert.match(s.content.text(),/<img src=x onerror=alert\(1\)>/);assert.match(s.content.text(),/<script>untrusted\(\)<\/script>/);assert.equal(doc.body.all().filter(n=>['IMG','SCRIPT'].includes(n.tagName)).length,0);
 assert.ok(!s.content.text().includes('123456'));assert.ok(!s.panel.text().includes('ALPHA_PROVEN'));assert.match(s.panel.text(),/confer no Calls, sizing or execution permission/);
 assert.equal(s.original.textContent,'Unavailable');s.details.open=true;s.details.ontoggle();assert.equal(s.original.textContent,raw.raw);assert.match(s.original.textContent,/ALPHA_PROVEN/);assert.deepEqual(JSON.parse(s.original.textContent).unknown,[0,null,false]);s.hide();assert.equal(t.tasks.size,0);
});
test('opening uses only an existing publication; refresh cannot trigger LLM generation or a configured external URL',async()=>{
 const doc=document(),t=timers(),calls=[],p=packet();p.url='https://untrusted.example/charge';p.generated_on_click=true;
 const s=api.init(doc,{path:()=>'/macro-leads.html',now:()=>NOW,timers:t,loader:async(page,options)=>{calls.push({page,options});return complete(p);}});assert.equal(calls.length,0);assert.equal(api.init(doc),null);
 s.button.onclick();await flush();assert.equal(calls.length,1);await s.refresh.onclick();assert.equal(calls.length,2);assert.ok(calls.every(x=>x.page==='macro-leads'&&x.options.signal instanceof AbortSignal));assert.ok(!s.panel.text().includes('Generate AI analysis'));s.hide();await s.refresh.onclick();assert.equal(calls.length,2);
});
test('minute age refresh is local, route changes close the panel, and Escape restores focus',async()=>{
 const doc=document(),t=timers();let at=NOW,route='/macro-leads.html',calls=0;
 const s=api.init(doc,{path:()=>route,now:()=>at,timers:t,loader:async()=>{calls++;return complete(packet());}});s.button.onclick();await flush();assert.match(s.status.textContent,/20.0 hours/);at+=2*86400000;[...t.tasks.values()][0]();assert.match(s.status.textContent,/68.0 hours/);assert.equal(calls,1);
 route='/other.html';[...t.tasks.values()][0]();assert.equal(s.panel.hidden,true);assert.equal(s.content.children.length,0);assert.equal(t.tasks.size,0);assert.equal(doc.activeElement,s.button);
 route='/macro-leads.html';s.button.onclick();await flush();let prevented=false;s.panel.onkeydown({key:'Escape',preventDefault(){prevented=true;}});assert.equal(prevented,true);assert.equal(s.panel.hidden,true);assert.equal(t.tasks.size,0);
});
test('failed refresh and delayed old responses cannot restore a prior explanation or disclosure callback',async()=>{
 const doc=document(),t=timers();let failure=false;const waiting=[];
 const s=api.init(doc,{path:()=>'/macro-leads.html',now:()=>NOW,timers:t,loader:async()=>{if(failure)throw Error('HTTP 429');return complete(packet());}});s.button.onclick();await flush();const oldToggle=s.details.ontoggle;failure=true;await s.refresh.onclick();oldToggle();assert.equal(s.original.textContent,'Unavailable');assert.equal(s.content.children.length,0);assert.match(s.status.textContent,/HTTP 429.*generation was not requested/);s.hide();
 const doc2=document(),s2=api.init(doc2,{path:()=>'/macro-leads.html',now:()=>NOW,timers:t,loader:()=>new Promise((resolve,reject)=>waiting.push({resolve,reject}))});s2.button.onclick();s2.hide();s2.button.onclick();waiting[1].reject(Error('latest unavailable'));await flush();waiting[0].resolve(complete(packet()));await flush();assert.match(s2.status.textContent,/latest unavailable/);assert.equal(s2.content.children.length,0);s2.hide();
});
test('nested routes make no request and damaged raw/parsed pairs refuse publication',async()=>{
 const doc=document(),t=timers();let calls=0;const s=api.init(doc,{path:()=>'/rotation/index.html',timers:t,loader:async()=>{calls++;return complete(packet());}});s.button.onclick();await flush();assert.equal(calls,0);assert.match(s.status.textContent,/No unambiguous/);s.hide();
 const d=document(),x=api.init(d,{path:()=>'/macro-leads.html',now:()=>NOW,timers:t,loader:async()=>({packet:packet(),raw:'{}'})});x.button.onclick();await flush();assert.match(x.status.textContent,/original packet differs/);assert.equal(x.content.children.length,0);x.hide();
});
test('route changes during a pending read cancel the view before old content can land',async()=>{
 const doc=document(),t=timers();let route='/macro-leads.html',resolve;
 const s=api.init(doc,{path:()=>route,now:()=>NOW,timers:t,loader:()=>new Promise(done=>{resolve=done;})});s.button.onclick();route='/other.html';s.updateAge();assert.equal(s.panel.hidden,true);resolve(complete(packet()));await flush();assert.equal(s.content.children.length,0);assert.equal(s.original.textContent,'Unavailable');assert.equal(t.tasks.size,0);
});
test('whole-body reader preserves original JSON bytes and never falls back to provider or live-generation endpoints',async()=>{
 const raw=JSON.stringify(packet()).replace('"unknown":[0,null,false]','"unknown":[9007199254740993,null,false]');const bytes=new TextEncoder().encode(raw);let i=0,cancelled=0,request;
 const result=await api.load('macro-leads',{now:NOW,fetcher:async(url,options)=>{request={url,options};return{ok:true,body:{getReader:()=>({read:async()=>i<bytes.length?{done:false,value:bytes.slice(i,(i+=43))}:{done:true},cancel:async()=>{cancelled++;}})}};}});
 assert.equal(result.raw,raw);assert.match(result.raw,/9007199254740993/);assert.equal(request.url,'/data/page-ai/macro-leads.json?exact=1&nogen=1');assert.equal(request.options.redirect,'error');assert.equal(request.options.cache,'no-store');assert.equal(cancelled,1);
 let reads=0;await assert.rejects(api.load('macro-leads',{fetcher:async()=>{reads++;return{ok:false,status:403};}}),/HTTP 403/);assert.equal(reads,1);await assert.rejects(api.load('../private'),/Exact page/);await assert.rejects(api.load('macro-leads',{timeout:5,fetcher:async()=>new Promise(()=>{})}),/timed out/);
});
test('duplicate JSON identities, malformed UTF-8, incomplete bodies and nonfinite values are rejected',async()=>{
 const bodies=[new Uint8Array([255]),new TextEncoder().encode('{"page":"macro-leads","page":"index"}'),new TextEncoder().encode('{"page":'),new TextEncoder().encode('{"x":1e400}')];
 for(const bytes of bodies){let i=0;await assert.rejects(api.load('macro-leads',{now:NOW,fetcher:async()=>({ok:true,body:{getReader:()=>({read:async()=>i++?{done:true}:{done:false,value:bytes},cancel:async()=>{}})}})}));}
 const p=api.strictJSON('{"__proto__":{"safe":true},"zero":0,"null":null}');assert.equal(Object.getPrototypeOf(p),Object.prototype);assert.equal({}.safe,undefined);assert.equal(p.__proto__.safe,true);
});
test('entire predecessor and unrelated chart-pro bootstrap remain preserved',()=>{
 const old=fs.readFileSync(path.join(__dirname,'fixtures/pre-research-jh-page-ai.js.txt')),source=fs.readFileSync(path.join(__dirname,'../jh-page-ai.js'),'utf8');assert.equal(old.length,8662);assert.equal(require('node:crypto').createHash('sha256').update(old).digest('hex'),'717f79fa1456b902624077ec3ffe35c0d6655d46be08ccd02db4c014029cde2a');
 const prefix=old.toString('utf8').split('(function () {\n  "use strict";')[0].replace('if (!/chart-pro','if (typeof location==="undefined" || !/chart-pro');assert.ok(source.startsWith(prefix));assert.ok(!source.includes('.innerHTML'));assert.ok(!source.includes('mode=live'));assert.ok(!source.includes('page-ai-live.json'));assert.ok(!source.includes('ALPHA_PROVEN'));
});
