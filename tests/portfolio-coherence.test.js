const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const binding=require('../jh-portfolio-binding.js'),io=require('../jh-portfolio-scenario-io.js'),contract=require('../jh-portfolio-risk-contract.js');
const fixture=JSON.parse(fs.readFileSync('tests/fixtures/portfolio-coherence-bound-synthetic.json','utf8'));
const NOW=Date.parse('2026-09-18T18:00:00Z');
const copy=()=>({snapshot:structuredClone(fixture.bundle.inputs.snapshot),risk:structuredClone(fixture.output)});

test('complete Python snapshot identities match browser encodings including Unicode and numeric boundaries',async()=>{
  for(const row of JSON.parse(fs.readFileSync('tests/fixtures/portfolio-value-identity-vectors.json','utf8'))){const {value,...expected}=row;assert.deepEqual(await binding.identity(value),expected);}
  const {snapshot,risk}=copy();assert.deepEqual(await binding.verify(snapshot,risk),await binding.identity(snapshot));
});
test('every snapshot field and array order matter while object order and numeric spelling do not',async()=>{
  assert.deepEqual(await binding.identity({b:[false,null,-0],a:1}),await binding.identity({a:1.0,b:[false,null,0]}));
  for(const [a,b] of [[[1,2],[2,1]],[false,0],[null,0],['1',1],[{x:1},{x:1,unused:null}]])assert.notDeepEqual(await binding.identity(a),await binding.identity(b));
  const {snapshot,risk}=copy();for(const [key,value] of [['qty',11],['market_value',999],['sector','Other'],['price_asof_unix_ms',0]]){
    const changed=structuredClone(snapshot);changed.positions[0][key]=value;await assert.rejects(binding.verify(changed,risk),/different snapshot values/);
  }
});
test('no binding, wrong key, typed length, time or hash cannot establish a matching snapshot',async()=>{
  const {snapshot,risk}=copy();
  for(const change of [null,{}, {...risk.snapshot_binding,key:'other'}, {...risk.snapshot_binding,encoded_bytes:true}, {...risk.snapshot_binding,encoded_bytes:0}, {...risk.snapshot_binding,value_sha256:'0'.repeat(64)}, {...risk.snapshot_binding,generated_at:'2026-09-18T16:00:00Z'}, {...risk.snapshot_binding,unexpected:true}])await assert.rejects(binding.verify(snapshot,{...risk,snapshot_binding:change}));
});
test('identity rejects non-JSON, unsafe numbers, invalid Unicode and excessive depth',()=>{
  for(const value of [NaN,Infinity,9007199254740992,'\ud800',undefined,new Date(),1n])assert.throws(()=>binding.encode(value));
  let deep=null;for(let i=0;i<130;i++)deep=[deep];assert.throws(()=>binding.encode(deep),/nesting/);
});

function page({timeoutMs=1000}={}){
  const elements=new Map(),requests=[],timers=new Map(),events={};let now=NOW,id=0;
  const document={hidden:false,addEventListener(name,fn){events[name]=fn;},getElementById(name){if(!elements.has(name))elements.set(name,{textContent:'',innerHTML:'',style:{}});return elements.get(name);},querySelector(){return {style:{}};}};
  const context=vm.createContext({document,window:{addEventListener(name,fn){events[name]=fn;}},console,Date,AbortController,
    JHPortfolioRisk:{...contract,view:d=>contract.view(d,now)},JHPortfolioBinding:binding,
    JHScenarioIO:{...io,readComplete:(open,opts)=>io.readComplete(open,{...opts,timeoutMs})},
    setInterval(fn,ms){const key=++id;timers.set(key,{fn,ms});return key;},clearInterval(key){timers.delete(key);},
    fetch(url,options){return new Promise(resolve=>requests.push({url,options,resolve}));}});
  const html=fs.readFileSync('portfolio/index.html','utf8');
  vm.runInContext([...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n'),context);
  return {context,elements,requests,timers,events,setNow:value=>now=value,run:code=>vm.runInContext(code,context)};
}
function respond(p,index,snapshot,risk){p.requests[index].resolve(new Response(JSON.stringify(snapshot)));p.requests[index+1].resolve(new Response(JSON.stringify(risk)));}
async function settled(p){const deadline=Date.now()+1500;while(!p.run('loadController===null')){assert.ok(Date.now()<deadline,'UI did not settle');await new Promise(r=>setTimeout(r,1));}}
async function enriched(value){const pair=copy();pair.snapshot.portfolio_summary={total_market_value:value};pair.risk.snapshot_binding={...pair.risk.snapshot_binding,...await binding.identity(pair.snapshot)};return pair;}

test('actual page accepts the complete native matching snapshot and retains no-NAV risk distinction',async()=>{
  const p=page(),{snapshot,risk}=copy();respond(p,0,snapshot,risk);await settled(p);
  assert.match(p.elements.get('risk-contract-title').textContent,/Holdings model/);
  assert.match(p.elements.get('risk-contract-detail').textContent,/Complete snapshot values match/);
  assert.equal(p.elements.get('mv-var').textContent,'—');assert.match(p.elements.get('holdings-risk-summary').textContent,/\$9.31/);
  assert.match(p.elements.get('positions-body').innerHTML,/AAA/);p.events.pagehide();
});
test('risk for a holding cannot be attributed to an empty or revised snapshot',async()=>{
  for(const mutate of [s=>s.positions=[],s=>s.positions[0].qty++,s=>s.generated_at='2026-09-18T16:00:00Z']){
    const p=page(),{snapshot,risk}=copy();mutate(snapshot);respond(p,0,snapshot,risk);await settled(p);
    assert.match(p.elements.get('risk-contract-title').textContent,/Risk withheld/);assert.equal(p.elements.get('holdings-risk-summary').textContent,'');assert.equal(p.elements.get('mv-beta').textContent,'—');p.events.pagehide();
  }
});
test('legacy risk without binding is visibly withheld without removing the separately loaded snapshot',async()=>{
  const p=page(),{snapshot,risk}=await enriched(1000);delete risk.snapshot_binding;respond(p,0,snapshot,risk);await settled(p);
  assert.equal(p.elements.get('mv-value').textContent,'$1,000');assert.match(p.elements.get('risk-contract-detail').textContent,/lacks a supported complete snapshot binding/);p.events.pagehide();
});
test('slow earlier response cannot overwrite a later successful snapshot',async()=>{
  const p=page(),a=await enriched(1000),b=await enriched(2000);const later=p.run('loadAll()');assert.equal(p.requests.length,4);
  respond(p,2,b.snapshot,b.risk);await later;assert.equal(p.elements.get('mv-value').textContent,'$2,000');
  respond(p,0,a.snapshot,a.risk);await new Promise(r=>setTimeout(r,5));assert.equal(p.elements.get('mv-value').textContent,'$2,000');
  assert.equal(p.requests[0].options.signal.aborted,true);p.events.pagehide();
});
test('late successful response cannot restore values after the newest load failed',async()=>{
  const p=page(),a=await enriched(1000);const latest=p.run('loadAll()');p.requests[2].resolve(new Response('{}',{status:503}));p.requests[3].resolve(new Response('{}'));await latest;
  respond(p,0,a.snapshot,a.risk);await new Promise(r=>setTimeout(r,5));assert.equal(p.elements.get('mv-value').textContent,'—');assert.equal(p.elements.get('updated').textContent,'unavailable');p.events.pagehide();
});
test('duplicate decoded JSON identities fail visibly instead of last-key acceptance',async()=>{
  const p=page(),{snapshot,risk}=copy();p.requests[0].resolve(new Response(JSON.stringify(snapshot)));
  p.requests[1].resolve(new Response(JSON.stringify(risk).replace('"nav":null','"nav":2000,"nav":null')));await settled(p);
  assert.match(p.elements.get('positions-body').innerHTML,/Duplicate JSON key/);assert.equal(p.elements.get('mv-var').textContent,'—');p.events.pagehide();
});
test('body failure after a valid prefix cannot leave a partially rendered risk',async()=>{
  const p=page(),{snapshot}=copy();p.requests[0].resolve(new Response(JSON.stringify(snapshot)));
  p.requests[1].resolve({ok:true,body:new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('{}'));c.error(Error('truncated'));}})});await settled(p);
  assert.match(p.elements.get('positions-body').innerHTML,/truncated/);assert.equal(p.elements.get('updated').textContent,'unavailable');p.events.pagehide();
});
test('stalled headers time out and late bodies are cancelled without restoring values',async()=>{
  const p=page({timeoutMs:15});await settled(p);assert.match(p.elements.get('positions-body').innerHTML,/timed out/);
  let cancelled=0;for(const r of p.requests)r.resolve({ok:true,body:{cancel(){cancelled++;return Promise.resolve();}}});await new Promise(r=>setTimeout(r,5));assert.equal(cancelled,2);assert.equal(p.elements.get('mv-value').textContent,'—');p.events.pagehide();
});
test('local age expiry clears risk without increasing network cadence',async()=>{
  const p=page(),{snapshot,risk}=copy();respond(p,0,snapshot,risk);await settled(p);const requests=p.requests.length;
  p.setNow(Date.parse(risk.generated_at)+4*3600000+1);[...p.timers.values()].find(t=>t.ms===60000).fn();
  assert.equal(p.elements.get('holdings-risk-summary').textContent,'');assert.match(p.elements.get('risk-contract-detail').textContent,/four-hour/);assert.equal(p.requests.length,requests);p.events.pagehide();
});
test('a future rejected publication cannot become accepted later without snapshot verification',async()=>{
  const p=page(),{snapshot,risk}=copy();p.setNow(Date.parse(risk.generated_at)-600000);respond(p,0,snapshot,risk);await settled(p);
  p.setNow(NOW);p.run('renderCurrent()');assert.match(p.elements.get('risk-contract-title').textContent,/Risk withheld/);assert.equal(p.elements.get('holdings-risk-summary').textContent,'');p.events.pagehide();
});
test('page lifecycle retains one five-minute network timer and clears abandoned work',async()=>{
  const p=page(),{snapshot,risk}=copy();respond(p,0,snapshot,risk);await settled(p);
  p.events.pageshow({persisted:false});assert.deepEqual([...p.timers.values()].map(t=>t.ms),[300000,60000]);assert.equal(p.requests.length,2);
  p.events.pagehide();assert.equal(p.timers.size,0);p.events.pageshow({persisted:true});assert.equal(p.timers.size,2);assert.equal(p.requests.length,4);
  respond(p,2,snapshot,risk);await settled(p);assert.match(p.elements.get('risk-contract-title').textContent,/Holdings model/);p.events.pagehide();
});

test('snapshot and risk strings cannot become markup or inline ticker handlers',()=>{
  const p=page();p.events.pagehide();
  const attack='<img src=x onerror="danger()">',symbol="AAA');danger();//";
  p.run('snapshot='+JSON.stringify({positions:[{symbol,sector:attack,qty:attack,alpha_score:attack,tier:attack,stop_hit:'false'}],watchlist:[{symbol,name:attack,sector:attack,current_price:attack,alpha_score:attack,tier:attack,confluence_tier:attack,regime_adj:attack}],portfolio_summary:{}})+';renderPositions();renderWatchlist();');
  for(const id of ['positions-body','watch-body']){
    const html=p.elements.get(id).innerHTML;assert.doesNotMatch(html,/<img|onclick=|<script|tier-<|STOP HIT/);assert.match(html,/rel="noopener noreferrer"/);assert.match(html,/href="\/stock\/\?symbol=AAA&#39;/);
  }
  p.run('risk='+JSON.stringify({alerts_summary:{sector_concentration_breach:true},sector_concentration:[{sector:attack,weight_pct:60}],concentration_label:attack,concentration_hhi:5000,correlation_clusters:[{symbols:[attack,'AAA'],avg_pairwise_correlation:attack,total_weight_pct:attack}],historical_scenarios:{x:{name:attack,spy_return_pct:attack,duration_days:attack,basis:attack}},correlation_matrix:{AAA:{AAA:1,BBB:attack},BBB:{AAA:attack,BBB:1}}})+';renderAlerts();renderSectors();renderScenarios();renderCorrelations();');
  for(const id of ['alert-zone','sector-body','scenarios-grid','corr-matrix'])assert.doesNotMatch(p.elements.get(id).innerHTML,/<img|onclick=|<script/);
});
test('malformed or unmeasured alerts cannot manufacture a VaR breach',()=>{
  const p=page();p.events.pagehide();
  for(const candidate of [{alerts_summary:{var_breach:true},var_1d_99_pct:null},{alerts_summary:{var_breach:'false'},var_1d_99_pct:8},{alerts_summary:{var_breach:true},var_1d_99_pct:0}]){
    p.run('risk='+JSON.stringify(candidate)+';renderAlerts();');assert.doesNotMatch(p.elements.get('alert-zone').innerHTML,/VAR BREACH/);
  }
});
