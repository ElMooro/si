const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),A=require('../jh-etf-proxy-labels.js');
test('legacy proxy labels distinguish trading activity from fund flows without trusting input text',()=>{
 for(const signal of ['HEAVY_INFLOW','HEAVY_OUTFLOW','ROTATION_IN','ROTATION_OUT','UNUSUAL_VOL','QUIET']){
  const label=A.label(signal);assert.match(label,/trading/i);assert.doesNotMatch(label,/inflow|outflow|redemption|creation/i);
 }
 for(const signal of [null,0,{},'__proto__','<img src=x onerror=alert(1)>'])assert.equal(A.label(signal),'Trading-activity proxy unavailable');
 assert.match(A.describe({flow_signal:'QUIET',price_volume_measurement:{as_of:'1970-01-01T00:00:00+00:00'}}),/1970-01-01/);
 assert.match(A.describe({flow_signal:'QUIET'}),/Bar time unavailable/);
 assert.match(A.describe({price_volume_measurement:{as_of:0}}),/Bar time unavailable/);
});
for(const page of ['livermore','wyckoff'])test(page+' scan renders proxy meaning, escapes unknown input and leaves source enums intact',async()=>{
 const html=fs.readFileSync(require('node:path').join(__dirname,'..',page+'.html'),'utf8');
 const script=[...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]).find(s=>s.includes('const PROXY='));
 const nodes={};const get=id=>nodes[id]||(nodes[id]={value:'SPY accumulation trend',innerHTML:'',textContent:''});
 const raw={ticker:'SPY',flow_signal:'HEAVY_INFLOW',change:0,inst_trans_pct:0,price_volume_measurement:{as_of:'2026-09-30T00:00:00+00:00'}};
 const ctx={document:{getElementById:get,querySelectorAll:()=>[]},localStorage:{getItem:()=>null,setItem:()=>{}},crypto:{randomUUID:()=> 'synthetic'},console,Date,Set,encodeURIComponent,JHEtfProxyLabels:A,fetch:async url=>{
  if(!url.startsWith('/data/'))throw Error('non-public request forbidden');
  return {json:async()=>url.includes('etf-flows')?{by_etf:{SPY:raw}}:{by_ticker:{}}};
 },alert:()=>{throw Error('unexpected alert')}};ctx.window=ctx;
 vm.createContext(ctx);vm.runInContext(script,ctx);vm.runInContext('persist=async()=>{}',ctx);await ctx.scan();
 assert.match(nodes.out.innerHTML,/ETF trading-activity proxies/);assert.match(nodes.out.innerHTML,/High trading dollar-volume \/ price up \(proxy\)/);
 assert.match(nodes.out.innerHTML,/Reported daily bar 2026-09-30/);assert.match(nodes.out.innerHTML,/<td class="num">0<\/td>/);
 assert.doesNotMatch(nodes.out.innerHTML,/HEAVY_INFLOW/);assert.equal(raw.flow_signal,'HEAVY_INFLOW');
 assert.match(html,/not measured fund inflows or outflows/);assert.match(html,/href="\/etf.html"/);
 raw.flow_signal='<img src=x>';raw.name='SPY';await ctx.scan();assert.doesNotMatch(nodes.out.innerHTML,/<img/);
});
test('dormant legacy desk annotations preserve zero versus missing without replacing machine fields',async()=>{
 const ctx={JHDesk:{feed:async key=>key.includes('etf-desk')?{by_etf:{SPY:{flow_1d:0}}}:key.includes('etf-flows')?{by_etf:{TLT:{dvol_z_score:3,flow_signal:'HEAVY_OUTFLOW',price_volume_measurement:{as_of:'2026-09-30T00:00:00Z'}}}}:{},quotes:async()=>({}),ohlc:async()=>[],horizons:()=>({}),closesOf:()=>[],vsSpy:()=>({}),adPhase:()=>({phase:'NEUTRAL'}),pattern:()=>null,pool:async(t,n,f)=>{for(const x of t)await f(x);}}};ctx.window=ctx;
 vm.createContext(ctx);vm.runInContext(fs.readFileSync(require('node:path').join(__dirname,'../jh-etf-engine.js'),'utf8'),ctx);
 const out=await ctx.JHEtf.run(),spy=out.rows.find(r=>r.ticker==='SPY'),tlt=out.rows.find(r=>r.ticker==='TLT'),voo=out.rows.find(r=>r.ticker==='VOO');
 assert.equal(spy.flow1d,0);assert.equal(spy.flowMeasurement,'legacy_fund_flow_unverified');
 assert.equal(tlt.flowRaw,'HEAVY_OUTFLOW');assert.equal(tlt.flowMeasurement,'price_volume_proxy');assert.equal(tlt.flowAsOf,'2026-09-30T00:00:00Z');
 assert.equal(voo.flow1d,undefined);assert.equal(voo.flowMeasurement,'unavailable');assert.equal(voo.flowAsOf,null);
 assert.equal(out.netFlow,0);assert.equal(out.inflows.length,0);assert.equal(out.outflows.length,0);
});
