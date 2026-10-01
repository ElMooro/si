const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const T=require('../jh-tape-truth.js');
const root=path.join(__dirname,'..');
const fixture=JSON.parse(execFileSync('python3',['-B','aws/lambdas/justhodl-tape-truth/tests/run_tests.py','--fixture'],{cwd:root,encoding:'utf8'}));
const NOW=Date.parse('2026-10-01T23:00:00Z');
class FrozenDate extends Date{static now(){return NOW;}}
const flush=()=>new Promise(r=>setImmediate(r));
function scripts(html){return [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].filter(m=>!m[1].includes('src=')).map(m=>m[2]);}
function environment(search='?ticker=SPY'){
 const elements={},listeners={};let ctx;
 const element=id=>elements[id]||={innerHTML:'',textContent:'',style:{},value:'',addEventListener(){},scrollIntoView(){}};
 ctx=vm.createContext({console,Date:FrozenDate,URL,URLSearchParams,setTimeout,clearTimeout,location:{search},
 document:{readyState:'loading',getElementById:element,querySelectorAll:()=>[],addEventListener:(k,f)=>(listeners[k]||=[]).push(f)},
 addEventListener(){},history:{replaceState(a,b,url){ctx.location.search=new URL(url,'https://test.local').search;},pushState(a,b,url){ctx.location.search=new URL(url,'https://test.local').search;}}});
 ctx.window=ctx;vm.runInContext(fs.readFileSync(path.join(root,'jh-tape-truth.js'),'utf8'),ctx);
 return {ctx,elements,boot(){ctx.document.readyState='complete';for(const f of listeners.DOMContentLoaded||[])f();}};
}
async function page(file,packet){
 const e=environment('?t=SPY');e.ctx.fetch=async()=>({ok:true,json:async()=>packet});
 for(const s of scripts(fs.readFileSync(path.join(root,file),'utf8')))vm.runInContext(s,e.ctx);
 e.boot();await flush();return e;
}
const why=fs.readFileSync(path.join(root,'why.html'),'utf8');
const modules=why.slice(why.indexOf('<!-- industry-case-module'),why.indexOf('<!-- bottom-desk module'));
const bus=scripts(why).find(s=>s.includes('if(window.__JH_TICKER_BUS)return;'));
async function whyContext(packets,late=false){
 const e=environment();const resolve={};
 e.ctx.fetch=url=>new Promise(r=>{const key=url.includes('industry-case')?'industry':'tape';resolve[key]=()=>r({ok:true,json:async()=>packets[key]});if(!late)resolve[key]();});
 for(const s of scripts(modules))vm.runInContext(s,e.ctx);
 vm.runInContext(bus,e.ctx);e.boot();await flush();return {...e,resolve};
}

test('clock handling preserves source values and rejects malformed, naive, future and non-string clocks',()=>{
 const vectors=[[null,'MISSING'],[true,'INVALID'],['2026-02-30T12:00:00Z','INVALID'],['2026-10-01T12:00:00','INVALID'],['2026-10-01T24:00:00Z','INVALID'],['2026-10-01T12:00:00+00:60','INVALID'],['2026-10-02T00:00:00Z','FUTURE'],['2026-01-06T20:00:00-05:00','VALID']];
 for(const [value,status] of vectors){const c=T.clock(value,'timestamp',NOW);assert.equal(c.status,status,String(value));assert.equal(c.value,value);}
 assert.equal(T.clock('2026-02-30','date',NOW).status,'INVALID');
 for(const n of [null,true,false,'0','',NaN,Infinity])assert.equal(T.fmt(n),'—');assert.equal(T.fmt(0),'0');
});
test('producer-to-projection clocks and values survive a new publication; unknown freshness never becomes permission',()=>{
 const v=T.view(fixture.tape,'SPY',NOW),p=T.projection(fixture.industry.cases.SPY.tape,NOW);
 assert.equal(v.available,true);assert.equal(p.available,true);assert.deepEqual(p.observations.cvd,v.observations.cvd);
 for(const s of [T.summary(v),T.summary(p)]){assert.match(s,/2026-01-06/);assert.match(s,/freshness UNKNOWN/i);assert.match(s,/calls and conviction withheld/);assert.doesNotMatch(s,/GENUINE_UP|SHAKEOUT|FAKE_UP/);}
});
test('all unknown/legacy contracts and invalid publication clocks are unavailable, even with LIVE and high conviction',()=>{
 for(const value of [undefined,'unknown']){const p=structuredClone(fixture.tape);p.measurement_contract=value;p.status='LIVE';p.symbols.SPY.verdict={call:'GENUINE_UP',conviction:99};assert.equal(T.view(p,'SPY',NOW).available,false);}
 for(const value of [null,'2026-10-01','2027-01-01T00:00:00Z',false]){const p=structuredClone(fixture.tape);p.generated_at=value;assert.equal(T.view(p,'SPY',NOW).available,false);const q=structuredClone(fixture.industry.cases.SPY.tape);q.source_generated_at=value;assert.equal(T.projection(q,NOW).available,false);}
 assert.equal(T.projection({call:'GENUINE_UP',conviction:99},NOW).available,false);
});
test('known contracts reject array symbol maps and malformed legs while retaining explicit gaps',()=>{
 const array=structuredClone(fixture.tape);array.symbols=[array.symbols.SPY];assert.equal(T.view(array,'0',NOW).available,false);
 for(const leg of ['cvd','short_vol','gex']){
  for(const value of [[],true,'bad',3]){
   const p=structuredClone(fixture.tape);p.symbols.SPY[leg]=value;assert.equal(T.view(p,'SPY',NOW).available,false,leg);
   const q=structuredClone(fixture.industry.cases.SPY.tape);q.observations[leg]=value;assert.equal(T.projection(q,NOW).available,false,leg);
  }
  const p=structuredClone(fixture.tape);p.symbols.SPY[leg]=null;assert.equal(T.view(p,'SPY',NOW).available,true);
 }
 assert.equal(T.view(fixture.tape,'_SPX',NOW).available,true);
});
test('missing and malformed observation dates remain explicit without removing source measurements',()=>{
 const p=structuredClone(fixture.tape);delete p.symbols.SPY.cvd.last_day;delete p.symbols.SPY.short_vol.observation_date;p.symbols.SPY.gex.source_timestamp='bad';
 const v=T.view(p,'SPY',NOW);assert.equal(v.available,true);assert.equal(v.observations.cvd.session_cvd,fixture.tape.symbols.SPY.cvd.session_cvd);
 assert.match(T.summary(v),/CVD ledger session: missing source date\/time/);assert.match(T.summary(v),/GEX provider timestamp: invalid source date\/time/);
});
test('actual tape page shows dated measurements and clears all sections when reloaded with legacy data',async()=>{
 const e=await page('tape-truth.html',fixture.tape);assert.match(e.elements.sub.textContent,/Published/);assert.match(e.elements.verdicts.innerHTML,/2026-01-06/);assert.match(e.elements.cvd.innerHTML,/600/);assert.match(e.elements.finra.innerHTML,/0.25/);assert.doesNotMatch(e.elements.verdicts.innerHTML,/GENUINE_UP|conviction 73/);
 e.ctx.fetch=async()=>({ok:true,json:async()=>({status:'LIVE',symbols:{SPY:{verdict:{call:'GENUINE_UP',conviction:99}}}})});
 await vm.runInContext('loadTape()',e.ctx);assert.match(e.elements.sub.textContent,/unavailable/);for(const id of ['idxgex','verdicts','cvd','gex','finra'])assert.equal(e.elements[id].innerHTML,'');
});
test('industry page keeps unrelated industry/narrative output while its tape row rejects a legacy projection',async()=>{
 const e=await page('industry-case.html',fixture.industry);assert.match(e.elements.qa.innerHTML,/2026-01-06/);assert.match(e.elements.qa.innerHTML,/SPY Inc/);assert.match(e.elements.ai.innerHTML,/mock narrative/);
 const d=structuredClone(fixture.industry);d.cases.SPY.tape={call:'GENUINE_UP',conviction:99};const l=await page('industry-case.html',d);
 assert.match(l.elements.qa.innerHTML,/Tape observations unavailable/);assert.doesNotMatch(l.elements.qa.innerHTML,/GENUINE_UP|conviction 99/);assert.match(l.elements.ai.innerHTML,/mock narrative/);
});
test('both why modules render actual projections and follow the original ticker bus, including switches during loading',async()=>{
 const e=await whyContext(fixture,true);vm.runInContext("history.replaceState(null,'','?ticker=NVDA')",e.ctx);e.resolve.industry();e.resolve.tape();await flush();await flush();
 assert.match(e.elements.ic_body.innerHTML,/NVDA/);assert.match(e.elements.tt_strip.innerHTML,/NVDA · selected/);assert.doesNotMatch(e.elements.tt_strip.innerHTML,/SPY · selected/);
 vm.runInContext("history.replaceState(null,'','?t=SPY')",e.ctx);assert.match(e.elements.tt_strip.innerHTML,/SPY · selected/);assert.match(e.elements.ic_body.innerHTML,/2026-01-06/);
 assert.equal(vm.runInContext('window.__JH_TICKER_BUS.current()',e.ctx),'SPY');
});
test('why retains industry context but never renders old calls from either route',async()=>{
 const f=structuredClone(fixture);delete f.tape.measurement_contract;f.tape.status='LIVE';f.tape.symbols.SPY.verdict={call:'GENUINE_UP',conviction:99};f.industry.cases.SPY.tape={call:'GENUINE_UP',conviction:99};
 const e=await whyContext(f);assert.match(e.elements.tt_strip.textContent,/unavailable/);assert.match(e.elements.ic_body.innerHTML,/Tape observations unavailable/);assert.match(e.elements.ic_body.innerHTML,/Industry share/);assert.doesNotMatch(e.elements.ic_body.innerHTML,/GENUINE_UP/);
});
test('all three pages load the same narrow display contract and scope why edits to the two modules',()=>{
 for(const p of ['tape-truth.html','industry-case.html','why.html'])assert.match(fs.readFileSync(path.join(root,p),'utf8'),/src="\/jh-tape-truth\.js"/);
 const hash=s=>require('node:crypto').createHash('sha256').update(s).digest('hex');
 assert.equal(hash(why.slice(0,why.indexOf('<!-- industry-case-module'))),'c990276ee63dd5cba86fdecaba1c804a1b82fa16b0526cacb79b845576468071');
 assert.equal(hash(why.slice(why.indexOf('<!-- bottom-desk module'))),'4aa437c73879fbf55740f22648bcc2c397ef8a5ee998cdeae68af702047452a1');
});
