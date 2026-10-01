const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const root=path.join(__dirname,'..');
const captured=require('./fixtures/industry-case-public-20260818.json').packet;
const html=fs.readFileSync(path.join(root,'industry-case.html'),'utf8');
const scripts=[...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].filter(m=>!m[1].includes('src=')).map(m=>m[2]);
const memberTickers=html=>[...html.matchAll(/href="industry-case\.html\?t=([^"]+)"/g)].map(m=>m[1]);
async function render(packet=captured,search='?t=NVDA'){
 const elements={},warnings=[],nodes={};let ctx;
 const element=id=>elements[id]||={innerHTML:'',textContent:'',style:{},value:'',listeners:{},addEventListener(k,f){this.listeners[k]=f;},scrollIntoView(){}};
 const querySelectorAll=selector=>{
  if(selector==='#inds tr[data-ind]')return nodes.league=[...element('inds').innerHTML.matchAll(/data-ind="([^"]+)"/g)].map(m=>({dataset:{ind:m[1]}}));
  if(selector==='#imem th')return nodes.headers=[...element('imem').innerHTML.matchAll(/data-k="([^"]+)"/g)].map(m=>({dataset:{k:m[1]}}));
  return [];
 };
 class FrozenDate extends Date{static now(){return Date.parse('2026-10-01T23:00:00Z');}}
 ctx=vm.createContext({Date:FrozenDate,URLSearchParams,console:{warn:(...a)=>warnings.push(a.map(String).join(' '))},location:{search},document:{getElementById:element,querySelectorAll},history:{replaceState(a,b,url){ctx.location.search=new URL(url,'https://test.local').search;}},fetch:async()=>({ok:true,json:async()=>packet})});
 ctx.window=ctx;vm.runInContext(fs.readFileSync(path.join(root,'jh-tape-truth.js'),'utf8'),ctx);
 // Await the actual page IIFE so route exceptions fail the test instead of being hidden.
 for(const script of scripts)await vm.runInContext(script,ctx);
 return {ctx,elements,warnings,nodes};
}
test('captured 149-row league survives and keeps its original date and legacy tape withholding',async()=>{
 const e=await render();assert.equal(e.nodes.league.length,149);assert.deepEqual(e.warnings,[]);
 assert.match(e.elements.sub.textContent,/As of 2026-08-18/);assert.match(e.elements.sub.textContent,/5,239 cases across 149 industries/);
 assert.match(e.elements.qa.innerHTML,/Tape observations unavailable/);assert.equal((e.elements.qa.innerHTML.match(/class="card"/g)||[]).length,9);
 for(const key of Object.keys(captured.industries))assert(e.nodes.league.some(n=>decodeURIComponent(n.dataset.ind)===key),key);
});
test('direct industry route restores four cards, coverage and the complete captured member cohort',async()=>{
 const e=await render(captured,'?ind=Semiconductors'),v=captured.industries.Semiconductors;
 assert.deepEqual(e.warnings,[]);assert.equal(e.elements.indwrap.style.display,'block');
 assert.equal((e.elements.icards.innerHTML.match(/class="card"/g)||[]).length,4);
 assert.match(e.elements.icards.innerHTML,new RegExp(v.ret_coverage+'/'+v.n+' covered'));
 assert.match(e.elements.ihow.textContent,/mcap-cohort labels, not product-share claims/);
 assert.equal(memberTickers(e.elements.imem.innerHTML).length,105);
 assert.equal(e.ctx.location.search,'?ind=Semiconductors');
});
test('league click, member sorting and ticker controls retain their navigation behavior',async()=>{
 const e=await render();e.nodes.league.find(n=>decodeURIComponent(n.dataset.ind)==='Semiconductors').onclick();
 const members=captured.industries.Semiconductors.members;
 assert.deepEqual(memberTickers(e.elements.imem.innerHTML),members.slice().sort((a,b)=>a.rank-b.rank).map(m=>m.t));
 e.nodes.headers.find(n=>n.dataset.k==='t').onclick();
 assert.deepEqual(memberTickers(e.elements.imem.innerHTML),members.map(m=>m.t).sort());
 e.nodes.headers.find(n=>n.dataset.k==='t').onclick();
 assert.deepEqual(memberTickers(e.elements.imem.innerHTML),members.map(m=>m.t).sort().reverse());
 e.nodes.headers.find(n=>n.dataset.k==='ret_12m_pct').onclick();
 const expected=members.slice().sort((a,b)=>a.ret_12m_pct==null?(b.ret_12m_pct==null?0:1):b.ret_12m_pct==null?-1:b.ret_12m_pct-a.ret_12m_pct).map(m=>m.t);
 assert.deepEqual(memberTickers(e.elements.imem.innerHTML),expected);
 e.elements.tk.value='amd';e.elements.go.onclick();assert.equal(e.ctx.location.search,'?t=AMD');assert.match(e.elements.ct.textContent,/^AMD/);
 e.elements.tk.listeners.keydown({key:'Enter',target:{value:'nvda'}});assert.equal(e.ctx.location.search,'?t=NVDA');assert.match(e.elements.ct.textContent,/^NVDA/);assert.deepEqual(e.warnings,[]);
});
test('actual league, cards and member rows keep numeric sign classes and missing values neutral',async()=>{
 for(const [value,klass,text] of [[5,'pos','+5.0%'],[-5,'neg','-5.0%'],[0,'','0.0%'],[null,'','–'],[undefined,'','–'],[NaN,'','–'],[Infinity,'','–'],[-Infinity,'','–'],[true,'','–'],[false,'','–'],['5','','–'],['','','–'],[{},'','–'],[{toString:null},'','–'],[[],'','–']]){
  const packet=structuredClone(captured),v=packet.industries.Semiconductors;
  packet.industries={Semiconductors:v};v.wtd_ret_12m_pct=value;v.median_ret_12m_pct=value;v.members=[{...v.members[0],ret_12m_pct:value}];
  const before=structuredClone(packet);const e=await render(packet,'?ind=Semiconductors');assert.deepEqual(e.warnings,[]);assert.deepEqual(packet,before);
  const league=e.elements.inds.innerHTML,cards=e.elements.icards.innerHTML,members=e.elements.imem.innerHTML;
  for(const content of [league,cards,members]){assert(content.includes(text));assert.doesNotMatch(content,/NaN|Infinity|\[object Object\]/);}
  assert.equal(vm.runInContext('cls(D.industries.Semiconductors.wtd_ret_12m_pct)',e.ctx).trim(),klass);
  assert.match(cards,new RegExp('12m growth \\(mcap-wtd\\)<\\/div><div class="v'+(klass?' '+klass:'')+'"'));
  assert.match(league,new RegExp('<td class="'+(klass?' '+klass:'')+'">'));
  assert.match(members,new RegExp('<td class="'+(klass?' '+klass:'')+'">'));
  if(typeof value!=='number'||!Number.isFinite(value))assert.match(cards,/12m growth \(mcap-wtd\)<\/div><div class="v">–%?<\/div>/);
 }
});
test('real qualified projection retains observation dates, UNKNOWN freshness and withheld authority',async()=>{
 const packet=JSON.parse(execFileSync('python3',['-B','aws/lambdas/justhodl-tape-truth/tests/run_tests.py','--fixture'],{cwd:root,encoding:'utf8'})).industry;
 const e=await render(packet,'?t=SPY');assert.deepEqual(e.warnings,[]);assert.match(e.elements.qa.innerHTML,/2026-01-06/);assert.match(e.elements.qa.innerHTML,/freshness UNKNOWN/i);assert.match(e.elements.qa.innerHTML,/calls and conviction withheld/i);
 assert.doesNotMatch(e.elements.qa.innerHTML,/GENUINE_UP|SHAKEOUT|FAKE_UP/);
});
test('actual repaired producer displays original source dates and publication-only clock without hiding measurements',async()=>{
 const packet=JSON.parse(execFileSync('python3',['-B','aws/lambdas/justhodl-industry-case/tests/test_publication.py','--fixture'],{cwd:root,encoding:'utf8'}));
 const e=await render(packet,'?t=T0000');assert.deepEqual(e.warnings,[]);
 assert.match(e.elements.sub.textContent,/Generated 2026-10-01T13:00:00\+00:00 \(publication time only\)/);
 assert.match(e.elements.sub.textContent,/freshness UNKNOWN/);assert.doesNotMatch(e.elements.sub.textContent,/As of 2026-10-01|LIVE/);
 assert.match(e.elements.sourceQualification.textContent,/2026-08-18T03:00:00Z/);
 assert.match(e.elements.sourceQualification.textContent,/2026-08-14/);
 assert.match(e.elements.sourceQualification.textContent,/NO_|No authoritative/);
 assert.match(e.elements.qa.innerHTML,/50\.0%/);assert.match(e.elements.ai.innerHTML,/Recorded cohort/);
 assert.equal(e.nodes.league.length,2);assert.equal(e.ctx.location.search,'?t=T0000');
 e.ctx.openInd('Semis');assert.equal(memberTickers(e.elements.imem.innerHTML).length,2);
});
test('missing and malformed qualifications remain readable without forged zero counts or HTML',async()=>{
 const missing=await render({status:'MISSING',why:'universe spine unavailable',publication_contract:'industry-case-publication.v1',sources:{}},'');
 assert.match(missing.elements.sub.textContent,/unavailable cases across unavailable industries/);
 assert.match(missing.elements.sub.textContent,/freshness UNKNOWN/);assert.match(missing.elements.sub.textContent,/MISSING/);
 const packet=structuredClone(captured);packet.publication_contract='industry-case-publication.v1';packet.status='PARTIAL';
 packet.sources={'source':{availability:'INVALID',clocks:{generated_at:{value:{toString:null},status:'INVALID'},as_of:{value:'<img src=x>',status:'INVALID'}}}};
 packet.cases.NVDA.ai_case='<img src=x onerror=boom()>';packet.cases.NVDA.ai_mode='<b>unavailable</b>';
 const e=await render(packet);assert.deepEqual(e.warnings,[]);assert.equal(e.nodes.league.length,149);
 assert.match(e.elements.sourceQualification.textContent,/INVALID/);assert.match(e.elements.sourceQualification.textContent,/<img src=x>/);
 assert.doesNotMatch(e.elements.ai.innerHTML,/<img|<b>/);assert.match(e.elements.ai.innerHTML,/&lt;img/);
});
test('legacy measured packet stays available with its original date and explicitly unavailable source qualification',async()=>{
 const e=await render();assert.equal(e.nodes.league.length,149);
 assert.match(e.elements.sub.textContent,/legacy packet date; not verified observation freshness/);
 assert.match(e.elements.sourceQualification.textContent,/Original per-source dates\/qualification unavailable/);
});
