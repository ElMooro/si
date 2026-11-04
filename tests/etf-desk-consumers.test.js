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
  // strong.html left this list on 2026-11-04: its flows now come from data/strength.json (justhodl-strength reads the desk server-side)
  for(const name of ['bonds.html','chart.html','credit-desk.html','crypto/index.html','factor-regime.html']){
    const html=fs.readFileSync(path.join(root,name),'utf8');assert.ok(html.indexOf('/jh-etf-desk-research.js')<html.indexOf('/jh-etf-fuse.js'));
    assert.match(html,/jh-etf-fuse\.js\?v=20260921-native1/);
  }
  const source=fs.readFileSync(path.join(root,'jh-chart-tvsearch.js'),'utf8');
  assert.match(source,/if \(!\(window\.JHEtfFuse && window\.JHEtfFuse\.native\)\) try/);
  const page=fs.readFileSync(path.join(root,'etf.html'),'utf8');assert.match(page,/data-ed-scenario/);assert.doesNotMatch(page,/jh-etf-engine\.js|\$vol z fallback/);
});

test('Strong v2 reads only the strength artifact and never the legacy inferred ETF context',async()=>{
  const old={by_etf:{QQQ:{return_5d_pct:88,return_20d_pct:99,flow_1d:123}},by_ticker:{AAPL:{crowding_pct:90,implied_5d:456}}};
  const art={contract:'strength.v1',generated_at:'2026-11-04T03:40:00+00:00',as_of:'2026-11-03',benchmark:{px:754.35,h:{d:-0.2,w:1.1}},sleeves:{ETF:[{ticker:'QQQ'}],STOCK:[{ticker:'AAPL'}]},
    rows:[{ticker:'QQQ',name:'Nasdaq 100',sleeve:'ETF',scopes:['desk','etf-desk'],h:{d:0.3},vs:{d:0.5,w:2},strength:93,posture:{code:'LEAD'},flow:{source:'etf-desk',current:true,w5:{usd:5.7e9,pct_aum:1.2}},f13:null},
          {ticker:'AAPL',name:'Apple',sleeve:'STOCK',scopes:['desk','sp500'],h:{d:-1.5},vs:{d:-1.3,w:-2},strength:21,posture:{code:'LAG'},flow:null,f13:{n_holders:38,stance:'DISTRIBUTING'}}]};
  const keys=[];
  const D={quotes:async()=>{throw Error('proxy down');},feed:async key=>{keys.push(key);return key==='data/strength.json'?art:old;}};
  const ctx={Date,Promise,Object,Array,Math,JSON,JHDesk:D,JHEtfDeskResearch:A,JHEtfFuse:{native:true,desk:async()=>{throw Error('must not be read by the page');},
    reverseFromDesk:()=>{throw Error('must not read legacy inference');}}};ctx.window=ctx;vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(root,'jh-strong-engine.js'),'utf8'),ctx);
  const result=await ctx.JHStrong.run(),qqq=result.rows.find(r=>r.ticker==='QQQ'),aapl=result.rows.find(r=>r.ticker==='AAPL');
  assert.deepEqual(keys,['data/strength.json']);assert.equal(qqq.live,null);assert.equal(aapl.live,null);
  assert.equal(qqq.flow.w5.usd,5.7e9);assert.equal(aapl.flow,null);assert.equal(aapl.f13.stance,'DISTRIBUTING');assert.equal(aapl.demand,undefined);assert.equal(aapl.crowding,undefined);
  assert.equal(qqq.flow1d,undefined);assert.equal(result.spy.d,-0.2);assert.equal(result.dump,false);assert.deepEqual(result.sleeves,['ETF','STOCK']);
  // an artifact that is not the strength contract is refused, never substituted by the legacy feeds
  const D2={quotes:async()=>({}),feed:async()=>old};const ctx2={Date,Promise,Object,Array,Math,JSON,JHDesk:D2};ctx2.window=ctx2;vm.createContext(ctx2);
  vm.runInContext(fs.readFileSync(path.join(root,'jh-strong-engine.js'),'utf8'),ctx2);
  await assert.rejects(()=>ctx2.JHStrong.run(),/not published yet/);
});
