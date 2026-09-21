const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const root=path.join(__dirname,'..'),A=require('../jh-etf-desk-research.js'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/etf-desk-native.json'),'utf8'));
global.crypto=require('node:crypto').webcrypto;
function adapter(fetcher){
  const ctx={console,Date,Map,Promise,Number,String,Object,Array,JSON,Math,isFinite,fetch:fetcher,JHEtfDeskResearch:A};ctx.window=ctx;
  vm.createContext(ctx);vm.runInContext(fs.readFileSync(path.join(root,'jh-etf-fuse.js'),'utf8'),ctx);return ctx.JHEtfFuse;
}
test('chart adapter binds native evidence and makes no raw-provider fallback requests',async()=>{
  const calls=[],F=adapter(async url=>{calls.push(url);const raw=url==='/'+A.CURRENT?JSON.stringify(fixture.publication):fixture.artifacts[url.slice(1)];return new Response(raw||'',{status:raw?200:404});});
  const row=await F.of('BND'),hist=await F.fullHist('BND',row.packet);assert.equal(row.native,true);assert.equal(row.aum,null);assert.equal(row.er,null);
  assert.equal(hist.length,25);assert.ok(hist.every(r=>r.point_in_time_eligible===false && r.history_sha256));
  assert.equal(await F.live('BND'),null);assert.equal(F.impliedDemand([{w:0.5,flow_1d:1000}]),null);
  assert.equal(F.isFund({}, {profile:{results:[{aum:1000}]}}),false);assert.equal(F.fmtEr(.003),'Scale unqualified');
  assert.ok(calls.every(url=>url.startsWith('/data/')));assert.ok(calls.every(url=>!url.includes('/poly/')));
  assert.equal(await F.of('UNLISTED'),null);assert.equal((await F.derived()).native,true);
});
test('unavailable native publication stays unavailable without automatic collection',async()=>{
  const calls=[],F=adapter(async url=>{calls.push(url);return new Response('',{status:503});});
  await assert.rejects(()=>F.of('SPY'),/request failed/);assert.deepEqual(calls,['/'+A.CURRENT]);
});
test('late chart responses cannot cross the visible active ticker',async()=>{
  let active='SPY',tick,boot;const waiting={},hud={innerHTML:'',className:'',textContent:''};
  const row=t=>({ticker:t,native:true,fund:{profiles:{current:{effective_date:'2026-09-18'}},holdings:{current:{effective_dates:{'2026-08-31':1}}}},packet:{},flow_hist:[]});
  const F={native:true,bare:s=>s,of:t=>new Promise(resolve=>{waiting[t]=()=>resolve(row(t));}),fullHist:async()=>[],alignHist:()=>[]};
  const ctx={console,Date,Map,Promise,JSON,Object,Array,encodeURIComponent,JHEtfFuse:F,JHEtfDeskResearch:{esc:String,exact:()=> 'Unavailable',windowValue:()=>null},
    document:{readyState:'loading',addEventListener:(_,fn)=>{boot=fn;},querySelector:selector=>{assert.equal(selector,'#tabs .tab.on[data-id]');return {getAttribute:()=>active};},getElementById:()=>hud},
    setInterval:fn=>{tick=fn;}};ctx.window=ctx;
  vm.createContext(ctx);vm.runInContext(fs.readFileSync(path.join(root,'jh-chart-etf-desk.js'),'utf8'),ctx);boot();
  active='BND';tick();assert.match(hud.textContent,/Verifying ETF research for BND/);assert.equal(ctx.jhEtfPack,null);waiting.BND();await new Promise(setImmediate);assert.equal(ctx.jhEtfPack.sym,'BND');
  waiting.SPY();await new Promise(setImmediate);assert.equal(ctx.jhEtfPack.sym,'BND');assert.match(hud.innerHTML,/BND/);assert.doesNotMatch(hud.innerHTML,/>SPY</);
  assert.equal(ctx.jhEtfFlowMarks().length,0);
  active='';tick();assert.equal(ctx.jhEtfPack,null);assert.match(hud.textContent,/Select a chart tab/);
});
test('every active shared consumer loads the native verifier before the adapter',()=>{
  for(const name of ['bonds.html','chart.html','credit-desk.html','crypto/index.html','factor-regime.html','strong.html']){
    const html=fs.readFileSync(path.join(root,name),'utf8');assert.ok(html.indexOf('/jh-etf-desk-research.js')<html.indexOf('/jh-etf-fuse.js'));
    assert.match(html,/jh-etf-fuse\.js\?v=20260921-native1/);
  }
  const source=fs.readFileSync(path.join(root,'jh-chart-tvsearch.js'),'utf8');
  assert.match(source,/if \(!\(window\.JHEtfFuse && window\.JHEtfFuse\.native\)\) try/);
  const page=fs.readFileSync(path.join(root,'etf.html'),'utf8');assert.match(page,/data-ed-scenario/);assert.doesNotMatch(page,/jh-etf-engine\.js|\$vol z fallback/);
});

test('Strong does not revive old inferred ETF context when native evidence fails',async()=>{
  const old={by_etf:{QQQ:{return_5d_pct:88,return_20d_pct:99,flow_1d:123}},by_ticker:{AAPL:{crowding_pct:90,implied_5d:456}}};
  const D={quotes:async()=>({}),ohlc:async()=>[],feed:async()=>old,closesOf:()=>[],horizons:()=>({d:null,w:null,m:null,q:null}),
    vsSpy:()=>({d:null,w:null,m:null,q:null}),round:x=>x,pool:async(t,n,fn)=>Promise.all(t.map(fn))};
  const ctx={Date,Promise,Object,Array,Math,JHDesk:D,JHEtfDeskResearch:A,JHEtfFuse:{native:true,desk:async()=>{throw Error('unavailable');},
    reverseFromDesk:()=>{throw Error('must not read legacy inference');}}};ctx.window=ctx;vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(root,'jh-strong-engine.js'),'utf8'),ctx);
  const result=await ctx.JHStrong.run(),qqq=result.rows.find(r=>r.ticker==='QQQ'),aapl=result.rows.find(r=>r.ticker==='AAPL');
  assert.equal(qqq.h.w,null);assert.equal(qqq.flow1d,undefined);assert.equal(aapl.demand,null);assert.equal(aapl.crowding,undefined);
});
