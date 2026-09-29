const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
const api=require('../jh-public-brief.js'),fixture=require('./fixtures/calls-byte-proof.json'),packet=JSON.parse(fixture.raw),at=Date.parse(packet.generated_at);
const source=fs.readFileSync(require.resolve('../jh-public-brief.js'),'utf8');
const element=tag=>({tag,children:[],attrs:{},style:{},textContent:'',appendChild(n){this.children.push(n);},replaceChildren(){this.children=[];},setAttribute(k,v){this.attrs[k]=v;}}),flatten=n=>[n,...n.children.flatMap(flatten)];
async function until(fn){for(let n=0;n<100&&!fn();n++)await new Promise(r=>setTimeout(r,2));assert.ok(fn());}
function mount(){
 const main=element('main'),timers=new Map(),events={},docEvents={};main.insertBefore=n=>main.appendChild(n);
 let now=at+600000,count=0,next=0,bad=false;
 const document={visibilityState:'visible',activeElement:null,querySelector:s=>s==='main'?main:null,createElement:element,getElementById:id=>flatten(main).find(n=>n.id===id),addEventListener:(event,fn)=>docEvents[event]=fn};
 class Clock extends Date{static now(){return now;}}
 const context={document,crypto:crypto.webcrypto,TextEncoder,TextDecoder,Uint8Array,AbortSignal,Date:Clock,setInterval:(fn,ms)=>{timers.set(++next,{fn,ms});return next;},clearInterval:id=>timers.delete(id),addEventListener:(event,fn)=>events[event]=fn,fetch:async url=>{count++;return new Response(url.includes('proofs')?JSON.stringify(fixture.proof):bad?'invalid':fixture.raw);}};
 vm.createContext(context);vm.runInContext(source,context);
 return{main,document,context,events,docEvents,timers,setTime:n=>now=n,setBad:n=>bad=n,count:()=>count,tick:ms=>[...timers.values()].find(t=>t.ms===ms).fn(),text:()=>flatten(main).map(n=>n.textContent).join('\n')};
}
test('Gregorian dates and timezone-aware clocks reject normalization and date-only input',()=>{
 for(const generated_at of ['2026-02-30T17:00:00Z','2026-02-29T17:00:00Z','2026-09-18','2026-09-18 17:00:00','2026-09-18T24:00:00Z','2026-09-18T17:00:00+24:00','0000-01-01T00:00:00Z',null]){
  const state=api.state({...packet,generated_at},at);assert.equal(state.clockStatus,'invalid');assert.equal(state.overdue,true);
 }
 assert.equal(api.state({...packet,generated_at:'2024-02-29T12:00:00.000+02:00'},Date.parse('2024-02-29T10:00:00Z')).clockStatus,'current');
});
test('future and overdue publication clocks have distinct labels and preserve the original time',()=>{
 const document={createElement:element},box=element('section');
 api.render(document,box,packet,at-300001);assert.match(flatten(box).map(n=>n.textContent).join('\n'),/Future clock · WAIT/);
 api.render(document,box,{...packet,generated_at:'2026-02-30T17:00:00Z'},at);assert.match(flatten(box).map(n=>n.textContent).join('\n'),/Clock unavailable · WAIT/);
 api.render(document,box,packet,at+4.5*3600000+1);assert.match(flatten(box).map(n=>n.textContent).join('\n'),/Overdue · WAIT/);
});
test('age update changes only the label and preserves disclosures, focus, zero and proof',()=>{
 const box=element('section'),document={createElement:element};const update=api.render(document,box,packet,at);
 const nodes=flatten(box),details=nodes.find(n=>n.tag==='details'),summary=nodes.find(n=>n.tag==='summary'),proof=nodes.find(n=>n.id==='calls-replay-proof');
 details.open=true;document.activeElement=summary;proof.textContent='Synthetic independently verified replay';
 update(at+5*3600000);assert.match(flatten(box).map(n=>n.textContent).join('\n'),/Overdue · WAIT/);
 assert.deepEqual(flatten(box),nodes);assert.equal(details.open,true);assert.equal(document.activeElement,summary);assert.equal(proof.textContent,'Synthetic independently verified replay');assert.ok(nodes.some(n=>n.textContent==='0 usd_bn'));
});
test('local minute clock ages a brief without fetching or invalidating historical replay proof',async()=>{
 const m=mount();await until(()=>m.text().includes('Replay verified for these exact'));
 m.setTime(at+5*3600000);m.tick(60000);
 assert.match(m.text(),/Overdue · WAIT/);assert.match(m.text(),/Replay verified for these exact/);assert.equal(m.count(),2);
});
test('returning to a suspended tab refreshes age immediately with no data request',async()=>{
 const m=mount();await until(()=>m.count()===2);
 m.document.visibilityState='hidden';m.setTime(at+6*3600000);m.document.visibilityState='visible';m.docEvents.visibilitychange();
 assert.match(m.text(),/Overdue · WAIT/);assert.equal(m.count(),2);
});
test('page lifecycle and duplicate script execution maintain one network and one local clock timer',async()=>{
 const m=mount();await until(()=>m.text().includes('Replay verified for these exact'));
 assert.deepEqual([...m.timers.values()].map(t=>t.ms),[300000,60000]);m.events.pagehide();assert.equal(m.timers.size,0);
 m.setTime(at+6*3600000);m.events.pageshow();m.events.pageshow();assert.equal(m.timers.size,2);assert.match(m.text(),/Overdue · WAIT/);assert.equal(m.count(),2);
 vm.runInContext(source,m.context);assert.equal(m.timers.size,2);assert.equal(flatten(m.main).filter(n=>n.id==='public-market-brief').length,1);assert.equal(m.count(),2);
});
test('failed refresh removes stale evidence and subsequent age ticks cannot restore it',async()=>{
 const m=mount();await until(()=>m.text().includes('Replay verified for these exact'));
 m.setBad(true);m.tick(300000);await until(()=>m.text().includes('Public brief unavailable'));
 m.setTime(at+8*3600000);m.tick(60000);m.docEvents.visibilitychange();assert.doesNotMatch(m.text(),/Replay verified|Overdue|Inspect measurements/);
 m.setBad(false);m.tick(300000);await until(()=>m.text().includes('Replay verified for these exact'));assert.match(m.text(),/Overdue · WAIT/);
});
test('quality and native eligibility are explicitly historical assessments at brief publication',()=>{
 const p=structuredClone(packet);p.original_source_lineage={liquidity_flow:{status:'verified',current_use:{eligible:true}},settlement_fails:{status:'verified',scopes:{ust_ex_tips:{current_use:{eligible:true}}}},ciss:{status:'verified',current_headline_research_eligible:true},tic:{status:'verified',current_use:{eligible:true}}};
 const before=JSON.stringify(p),box=element('section');api.render({createElement:element},box,p,at+6*3600000);
 const text=flatten(box).map(n=>n.textContent).join('\n');assert.match(text,/Quality at publication/);assert.match(text,/Replaying it does not refresh/);assert.equal((text.match(/At brief publication/g)||[]).length,4);
 assert.doesNotMatch(text,/Current descriptive use passes|All seven headline and contribution legs pass/);assert.equal(JSON.stringify(p),before);
});
