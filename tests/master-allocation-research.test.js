const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const ui=require('../jh-master-allocation-research.js'),NOW=Date.parse('2026-09-26T14:00:00Z');
const packet=()=>({as_of:'2026-09-26T12:20:00Z',confidence:99,posture:'RISK_ON',target_allocation:{stock:0,cash:100},benchmark:{stock:60,cash:40},
 asset_labels:{stock:'Stock <img src=x>',cash:'Cash'},deltas_from_benchmark:{stock:-60,cash:60},best_asset:{winner:{asset:'SPY'},ranked:[{asset:'CASH',r12_1:-.001,r6:0,r3:null,score:-.05}]}});
test('legacy and new projections never become a target or a best-asset instruction',()=>{
 for(const p of [packet(),{contract:'master-allocation-research.v1',generated_at:packet().as_of,unqualified_projection:packet(),sizing_eligible:true}]){
  const html=ui.render(p,NOW);assert.match(html,/WAIT · No qualified target/);assert.match(html,/not an instruction to liquidate/);
  assert.match(html,/Stock &lt;img src=x&gt;/);assert.doesNotMatch(html,/<img|BEST ASSET NOW|Target Portfolio/);
  assert.match(html,/BIL price proxy \(reported as CASH\)/);assert.match(html,/-0.10%/);assert.match(html,/0.00%/);
  assert.match(html,/Original-source replay and historical model calibration remain unverified/);
 }
});
test('complete hypotheses and zero weights remain inspectable without false numeric coercion',()=>{
 const p=packet();p.target_allocation.other=null;p.benchmark.other=false;p.best_asset.ranked.push({asset:'TEST',score:'10'});
 const html=ui.render(p,NOW);assert.match(html,/<td>0.00<\/td>/);assert.match(html,/unavailable/);assert.match(html,/TEST/);
 assert.match(html,/&quot;confidence&quot;: 99/);assert.doesNotMatch(html,/NaN|undefined%/);
});
test('missing, future, expired and timezone-free packets do not leave current numeric tables',()=>{
 for(const p of [null,[],{...packet(),as_of:'2026-09-27T00:00:00Z'},{...packet(),as_of:'2026-09-24T00:00:00Z'},{...packet(),as_of:'2026-09-26T12:20:00'}]){
  const html=ui.render(p,NOW);assert.match(html,/snapshot unavailable/);assert.doesNotMatch(html,/<table/);
 }
 assert.match(ui.render({contract:'master-allocation-research.v1',as_of:packet().as_of},NOW),/Complete research projection unavailable/);
});
test('read-only fetch is bounded through body parsing and rejected responses cannot render',async()=>{
 const value=await ui.get(async(url,options)=>{assert.equal(url,'/data/master-allocation.json?exact=1&nogen=1');assert.equal(options.cache,'no-store');return {ok:true,text:async()=>JSON.stringify(packet())};});assert.equal(value.confidence,99);
 await assert.rejects(ui.get(async()=>({ok:false,text:async()=>JSON.stringify(packet())})),/Unavailable/);
 await assert.rejects(ui.get(async()=>new Promise(()=>{}),5),/timeout/);
 await assert.rejects(ui.get(async()=>({ok:true,text:()=>new Promise(()=>{})}),5),/timeout/);
});
test('mounted view expires without acquisition and refresh failures remove previous tables',async()=>{
 const el={innerHTML:''},doc={getElementById:()=>el};let now=NOW,tick,requests=0;
 const timers={setInterval(fn){tick=fn;return 1;},clearInterval(){}};
 await ui.mount(doc,async()=>{requests++;return {ok:true,text:async()=>JSON.stringify(packet())};},()=>now,timers);
 assert.match(el.innerHTML,/<table/);now+=31*36e5;tick();assert.doesNotMatch(el.innerHTML,/<table/);assert.equal(requests,1);
 await ui.mount(doc,async()=>{throw Error('Unavailable');},()=>NOW,timers);assert.match(el.innerHTML,/snapshot unavailable/);
});
test('an older response cannot reintroduce values after a later failed refresh',async()=>{
 const el={innerHTML:''},doc={getElementById:()=>el},timers={setInterval(){},clearInterval(){}};let resolve;
 const slow=ui.mount(doc,()=>new Promise(r=>{resolve=r;}),()=>NOW,timers);
 await ui.mount(doc,async()=>{throw Error('Unavailable');},()=>NOW,timers);
 resolve({ok:true,text:async()=>JSON.stringify(packet())});await slow;assert.doesNotMatch(el.innerHTML,/<table/);
});
test('actual page uses one reviewed reader and retains the full predecessor',()=>{
 const html=fs.readFileSync('master-allocator.html','utf8');assert.match(html,/jh-master-allocation-research\.js/);
 assert.doesNotMatch(html,/fetch\(|BEST ASSET NOW|Target Portfolio|tracking error to benchmark/);
 assert.ok(fs.readFileSync('tests/fixtures/legacy-master-allocator-before-research-boundary.html.txt').length>17000);
});
