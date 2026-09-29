const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),vm=require('node:vm');
const api=require('../jh-extremes-research.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/extremes-native.json'),'utf8')),source=fs.readFileSync(path.join(__dirname,'../jh-extremes-research.js'),'utf8');
const base=Date.parse(f.packet.generated_at),digest=s=>crypto.createHash('sha256').update(s).digest('hex');
function artifacts(p){const {replay,...body}=p,raw=JSON.stringify(body),key='data/extremes-research/outputs/'+digest(raw)+'.json',m={...JSON.parse(f.artifacts[f.packet.replay.manifest_key]),engine:p.engine,generated_at:p.generated_at,output:{key,bytes:Buffer.byteLength(raw),sha256:digest(raw)},output_sha256:digest(raw)},run=JSON.stringify(m),runkey='data/extremes-research/runs/'+digest(run)+'.json';p.replay={manifest_key:runkey,output_sha256:digest(raw)};return{['data/'+p.engine+'.json']:JSON.stringify(p),[key]:raw,[runkey]:run};}
function fixture(mutator=()=>{}){const p=structuredClone(f.packet);mutator(p);return{p,files:artifacts(p)};}
const wait=async predicate=>{for(let n=0;n<200&&!predicate();n++)await new Promise(done=>setTimeout(done,2));assert.ok(predicate(),'Mounted state did not settle');};
function mounted(){const files={...f.artifacts,['data/'+f.packet.engine+'.json']:JSON.stringify(f.packet)},events={},docEvents={},timers=new Map();let id=0,at=base,html='',renders=0,requests=0,mode='valid',release;
 const label={textContent:''},button={},details={open:false},scroller={scrollLeft:0},focus={};let rows=[];
 const node={dataset:{extremesEngine:f.packet.engine},button,label,details,scroller,get innerHTML(){return html;},set innerHTML(value){html=value;renders++;label.textContent=(/<p class="xr-state"[^>]*>([^<]*)/.exec(value)||[])[1]||'';rows=[...value.matchAll(/data-extremes-until="([^"]+)"[^>]*><span data-extremes-use>([^<]*)/g)].map(m=>({dataset:{extremesUntil:m[1]},cell:{textContent:m[2]},querySelector(){return this.cell;}}));},querySelector(selector){return selector==='[data-extremes-state]'?label:button;},querySelectorAll(){return rows;}};
 const document={readyState:'complete',hidden:false,activeElement:focus,querySelectorAll:()=>[node],getElementById:()=>null,addEventListener:(type,fn)=>{docEvents[type]=fn;}};
 class Clock extends Date{static now(){return at;}}
 const env={document,Date:Clock,crypto:crypto.webcrypto,Uint8Array,TextDecoder,AbortController,setTimeout,clearTimeout,setInterval(fn,ms){assert.equal(ms,60000);timers.set(++id,fn);return id;},clearInterval:n=>timers.delete(n),addEventListener:(type,fn)=>{events[type]=fn;},fetch:async url=>{requests++;if(mode==='pending')return new Promise(done=>release=()=>done(new Response(files[url.slice(1)])));if(mode==='failed')return new Response('{}',{status:503});return new Response(files[url.slice(1)],{status:200});}};
 vm.runInNewContext(source,env);
 return{node,document,events,docEvents,timers,focus,set time(v){at=v;},get requests(){return requests;},get renders(){return renders;},get rows(){return rows;},set mode(v){mode=v;},release:()=>release(),tick(){for(const fn of [...timers.values()])fn();}};
}

test('strict Gregorian dates and aware clocks preserve offsets and reject normalization',()=>{
 for(const value of ['2026-02-30','1900-02-29','2026-13-01','0000-01-01','2026-2-01',null,false])assert.equal(api.day(value),null);
 for(const value of ['2024-02-29','2000-02-29','2026-09-20'])assert.notEqual(api.day(value),null);
 assert.equal(api.clock('2026-09-20T23:50:00.123456+05:30'),Date.parse('2026-09-20T18:20:00.123Z'));
 for(const value of ['2026-09-20T18:20:00','2026-09-20 18:20:00Z','2026-09-20T24:00:00Z','2026-09-20T23:60:00Z','2026-09-20T23:59:60Z','2026-09-20T18:20:00+24:00','2026-09-20T18:20:00+01:60','2026-02-30T18:20:00Z',null])assert.equal(api.clock(value),null);
});
test('even hash-consistent packets with impossible clocks or unbounded freshness cannot verify',async()=>{
 const changes=[p=>p.measurements[0].observation_date='2026-02-30',p=>p.generated_at='2026-09-20T18:20:00',p=>p.measurements[0].valid_until='2026-09-20T24:00:00Z',p=>p.freshness.pipeline_check_due_at='2099-01-01T00:00:00Z',p=>p.freshness.pipeline_check_due_at=p.generated_at,p=>p.freshness.pipeline_check_due_at='2026-09-19T18:20:00Z'];
 for(const change of changes){const {p,files}=fixture(change);assert.equal(api.typed(p),false);await assert.rejects(api.verifyPacket(p,async url=>new Response(files[url.slice(1)])),/Native synthesis required/);assert.doesNotMatch(api.render(p,base),/Dated measurements/);}
 const {p}=fixture(p=>p.freshness.pipeline_check_due_at=new Date(base+26*3600000).toISOString());assert.equal(api.typed(p),true);p.freshness.pipeline_check_due_at=new Date(base+26*3600000+1).toISOString();assert.equal(api.typed(p),false);
});
test('future publication is distinguished from overdue context without current-use labels',()=>{
 const html=api.render(f.packet,base-1);assert.match(html,/Future-dated publication/);assert.match(html,/Future-dated context/);assert.doesNotMatch(html,/>Dated context</);assert.equal(api.current(f.packet,base-1),false);
});
test('local time expires individual rows and the wrapper without fetching or replacing inspection DOM',async()=>{
 const t=mounted();await wait(()=>t.timers.size===1);t.node.details.open=true;t.node.scroller.scrollLeft=87;const calls=t.requests,renders=t.renders;
 assert.match(t.node.label.textContent,/Dated partial research/);assert.ok(t.rows.some(r=>r.cell.textContent==='Dated context'));
 t.time=Date.parse(f.packet.freshness.pipeline_check_due_at);t.tick();assert.match(t.node.label.textContent,/refresh overdue/);assert.ok(t.rows.every(r=>r.cell.textContent==='Expired context'));
 assert.equal(t.requests,calls);assert.equal(t.renders,renders);assert.equal(t.node.details.open,true);assert.equal(t.node.scroller.scrollLeft,87);assert.equal(t.document.activeElement,t.focus);
 t.events.pagehide();assert.equal(t.timers.size,0);
});
test('measurement expiry is independent of wrapper expiry',()=>{
 const p=structuredClone(f.packet),first={dataset:{extremesUntil:new Date(base+1000).toISOString()},cell:{textContent:''},querySelector(){return this.cell;}},second={dataset:{extremesUntil:new Date(base+2000).toISOString()},cell:{textContent:''},querySelector(){return this.cell;}},label={};
 const node={querySelector:()=>label,querySelectorAll:()=>[first,second]};api.updateAges(node,p,base+1000);assert.match(label.textContent,/Dated partial research/);assert.equal(first.cell.textContent,'Expired context');assert.equal(second.cell.textContent,'Dated context');
});
test('page return and visibility update locally, with one timer and no added acquisition',async()=>{
 const t=mounted();await wait(()=>t.timers.size===1);const calls=t.requests;t.events.pagehide();assert.equal(t.timers.size,0);t.time=Date.parse(f.packet.freshness.pipeline_check_due_at)+1;t.events.pageshow();t.events.pageshow();assert.equal(t.timers.size,1);assert.match(t.node.label.textContent,/refresh overdue/);
 t.time=base;t.document.hidden=false;t.docEvents.visibilitychange();assert.match(t.node.label.textContent,/Dated partial research/);assert.equal(t.requests,calls);t.events.pagehide();
});
test('refresh failure clears the timer and verified research; retry creates exactly one timer',async()=>{
 const t=mounted();await wait(()=>t.timers.size===1);t.mode='failed';await t.node.button.onclick();assert.equal(t.timers.size,0);assert.match(t.node.innerHTML,/Retry verification/);assert.doesNotMatch(t.node.innerHTML,/Dated measurements/);
 t.mode='valid';await t.node.button.onclick();assert.equal(t.timers.size,1);assert.match(t.node.innerHTML,/Dated measurements/);t.events.pagehide();
});
test('pagehide cancels a pending verification and late bytes cannot repaint after return',async()=>{
 const t=mounted();await wait(()=>t.timers.size===1);t.mode='pending';const job=t.node.button.onclick();await wait(()=>t.timers.size===0);t.events.pagehide();t.events.pageshow();assert.match(t.node.innerHTML,/Retry verification/);t.release();await job;assert.match(t.node.innerHTML,/Retry verification/);assert.equal(t.timers.size,0);
 t.mode='valid';await t.node.button.onclick();assert.match(t.node.innerHTML,/Dated measurements/);assert.equal(t.timers.size,1);t.events.pagehide();
});
