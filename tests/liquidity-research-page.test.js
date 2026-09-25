const test=require('node:test'), assert=require('node:assert/strict'), fs=require('node:fs'), vm=require('node:vm');
const code=fs.readFileSync('jh-liquidity-research.js','utf8');
class Element {
  constructor(tag){this.tagName=tag;this.children=[];this.value='';this.attributes={};}
  appendChild(child){this.children.push(child);return child;}
  replaceChildren(...children){this.children=children;this.value='';}
  set textContent(value){this.value=String(value);this.children=[];}
  get textContent(){return this.value+this.children.map(child=>child.textContent).join(' ');}
  set innerHTML(value){throw Error('Untrusted HTML insertion');}
  setAttribute(name,value){this.attributes[name]=value;}
}
function fixture(){
 const ids=['WALCL','WTREGEN','RRPONTSYD'], series={}, legs={};
 for(const [index,id] of ids.entries()){
  series[id]={acquired_at:'2026-09-25T17:00:00Z',measurement_basis:['Wednesday level','Weekly average ending Wednesday','Daily operation'][index]};
  legs[id]={selected:{observation_date:index===2?'2026-09-24':'2026-09-23'},value:{value:index===2?0:1000},carry_days:index===2?1:2};
 }
 const comparisons={};for(const label of ['1d','1w','1m','3m'])comparisons[label]={current_valuation_date:'2026-09-25',baseline_valuation_date:'2026-08-25',legs:Object.fromEntries(ids.map(id=>[id,{signed_formula_contribution:{value:id==='WALCL'?1:-0.5}}])),change:{value:0}};
 return {contract:'liquidity-flow-research.v1',generated_at:'2026-09-25T18:00:00Z',source_generated_at:'2026-09-25T17:30:00Z',
  calls_eligible:false,sizing_eligible:false,quality:{status:'fresh'},current:{net_liquidity_b:0},series,
  last_reconstructed_snapshot:{valuation_date:'2026-09-25',legs},comparisons,replay:{manifest_key:'data/liquidity-flow-research/runs/'+'a'.repeat(64)+'.json'}};
}
async function render(packet, fail=false){
 const host=new Element('section'), requests=[], timers=[];let now=Date.parse('2026-09-25T20:00:00Z');
 class Clock extends Date{static now(){return now;}}
 const context={Date:Clock,setTimeout:(callback,delay)=>timers.push({callback,delay}),document:{getElementById:()=>host,createElement:tag=>new Element(tag)},fetch:async(url,options)=>{
  requests.push({url,options});return {ok:!fail,json:async()=>packet};}};
 vm.createContext(context);vm.runInContext(code,context);await new Promise(resolve=>setImmediate(resolve));return {host,requests,timers,advance:hours=>{now+=hours*3600000;}};
}
function all(node){return [node,...node.children.flatMap(all)];}
test('native desk separates observations, measurement bases and signed contributions',async()=>{
 const {host,requests}=await render(fixture());
 assert.match(host.textContent,/\$0\.000 bn/);assert.match(host.textContent,/Weekly average/);assert.match(host.textContent,/2026-09-23/);assert.match(host.textContent,/2026-09-24/);
 assert.match(host.textContent,/WAIT \/ abstain/);assert.match(host.textContent,/not a causal estimate/);
 assert.equal(all(host).filter(n=>n.tagName==='table').length,2);
 assert.equal(all(host).filter(n=>n.tagName==='a'&&n.href.startsWith('https://fred.stlouisfed.org/series/')).length,3);
 assert.equal(requests[0].options.credentials,'omit');assert(requests[0].url.endsWith('?exact=1&nogen=1'));
});
test('stale original, future wrapper and unavailable quality withhold current amount but retain source dates',async()=>{
 for(const change of [p=>p.series.WALCL.acquired_at='2026-09-20T00:00:00Z',p=>p.generated_at='2026-09-26T00:00:00Z',p=>p.quality.status='unavailable']){
  const p=fixture();change(p);const {host}=await render(p);assert.match(host.textContent,/Current value unavailable/);assert(!host.textContent.includes('$0.000 bn'));assert.match(host.textContent,/2026-09-23/);
 }
});
test('legacy and unavailable data never masquerade as the native calculation',async()=>{
 for(const [packet,fail] of [[{},false],[fixture(),true],[{...fixture(),sizing_eligible:true},false]]){
  const {host}=await render(packet,fail);assert.match(host.textContent,/not available/);assert(!host.textContent.includes('$'));
 }
});
test('an open page expires its freshness label without another acquisition request',async()=>{
 const view=await render(fixture());assert.equal(view.timers[0].delay,60000);
 view.advance(24);view.timers[0].callback();assert.match(view.host.textContent,/Current value unavailable/);
 assert.equal(view.requests.length,1);assert.match(view.host.textContent,/2026-09-23/);
});
test('source text remains text and arbitrary manifest paths never become links',async()=>{
 const p=fixture();p.series.WALCL.measurement_basis='<img onerror=alert(1)>';p.replay.manifest_key='https://example.com/private';
 const {host}=await render(p);assert.match(host.textContent,/<img onerror/);assert(!all(host).some(n=>n.tagName==='img'));assert(!all(host).some(n=>n.href==='https://example.com/private'));
});
test('native desk is wired and calendar-month labels do not become 30-day claims',()=>{
 const page=fs.readFileSync('liquidity.html','utf8'),signals=fs.readFileSync('signals.html','utf8');
 assert.match(page,/id="jh-liquidity-research"/);assert.match(page,/src="\/jh-liquidity-research\.js\?v=20260925"/);
 assert.match(page,/assets\/liquidity-research\.css/);assert.match(signals,/Calendar month/);assert(!signals.includes("setHTML('liq-blurb'"));
});
