// Khalid 2026-11-19: "Requested: VANTAGE:SP500 - Resolved route: unavailable ... im trying to pull sp500 and i cant", then "Fix all of them":
//  (1) typing sp500 led with FRED's copy, (2) the directory's pinned I:SPX answered "No bars", (3) his ratio rows built on VANTAGE:SP500 drew from
//  FRED's copy or not at all, (4) Nasdaq-100 and Dow ids hit the same dead end, (5) the status line said "awaiting chart frame" on a drawn chart.
// Actual source functions (watchlist router, chart engine, catalog, spread evaluator); no network. The directory's pins are tested in
// tests/deployment/test_symdir_policy_pins.py.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const read=f=>fs.readFileSync(f,'utf8'),watch=read('jh-chart-tvwatch.js'),engine=read('jh-chart-engine.js'),catalog=read('jh-chart-catalog.js');
const fixture=JSON.parse(read('tests/fixtures/chart-index-routes/evidence-2026-11-19.json'));
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const trees=new Map();function nodes(src){if(trees.has(src))return trees.get(src);const out=[],stack=[acorn.exports.parse(src,{ecmaVersion:'latest'})];while(stack.length){const n=stack.pop();if(!n||typeof n!=='object')continue;if(n.type)out.push(n);for(const v of Object.values(n)){if(Array.isArray(v))stack.push(...v);else if(v&&typeof v==='object')stack.push(v);}}trees.set(src,out);return out;}
function fn(src,name){const a=nodes(src).filter(n=>n.type==='FunctionDeclaration'&&n.id.name===name);assert.equal(a.length,1,name);return src.slice(a[0].start,a[0].end);}
function variable(src,name){const n=nodes(src).find(n=>n.type==='VariableDeclarator'&&n.id.name===name);assert.ok(n,name);return 'var '+src.slice(n.start,n.end)+';';}
const assignments=nodes(engine).filter(n=>n.type==='AssignmentExpression'&&n.left.type==='MemberExpression'&&n.left.object.name==='q'&&n.left.property.name==='onkeydown'&&n.right.type==='FunctionExpression'&&engine.slice(n.right.start,n.right.end).includes('var s=q.value.trim().toUpperCase(); if(TABS.indexOf(s)<0)'));
assert.equal(assignments.length,1);const nativeQ=engine.slice(assignments[0].right.start,assignments[0].right.end);
function harness(map=null){
 const c={window:{},Set,Number,Date,AbortController,setTimeout,clearTimeout,painted:[],toasts:[],loads:[],symMap:map,symMapReady:1,chartSelection:null,chartRouteObserver:null,TABS:[],active:'INITIAL',filter:''};
 c.q={value:'',dispatchEvent(e){this.onkeydown(e);}};c.document={getElementById(id){return id==='q'?c.q:null;}};c.KeyboardEvent=function(_,o){return o;};c.paintCard=s=>c.painted.push(s);c.toast=s=>c.toasts.push(s);c.loadSymMap=()=>Promise.resolve();
 c.loadDraw=c.renderTabs=c.renderList=()=>{};c.load=()=>c.loads.push(c.active);c.activeSym=()=>c.active;c.lookupSym=()=>'';
 vm.createContext(c);vm.runInContext(fn(catalog,'H')+['EXCH','SERIES_PROV','CURATED','DEFILLAMA_IDS','REGIONAL_FED_IDS'].map(n=>variable(catalog,n)).join('\n')+fn(catalog,'isWarehouse')+fn(catalog,'chartId'),c);
 c.window.JHChartCatalog={isWarehouse:c.isWarehouse,chartId:c.chartId,curated:c.CURATED};
 vm.runInContext(['resolveSym','yahooSym','bare','observationId'].map(n=>fn(engine,n)).join('\n'),c);vm.runInContext(fn(engine,'normQ')+fn(engine,'classifySym')+variable(engine,'SEARCH_ALIASES'),c);
 const exported=nodes(engine).find(n=>n.type==='AssignmentExpression'&&n.left.type==='MemberExpression'&&n.left.property.name==='jhWatchlistResolve');assert.ok(exported);vm.runInContext(engine.slice(exported.start,exported.end),c);c.q.onkeydown=vm.runInContext('('+nativeQ+')',c);
 c.paintChartRoute=()=>{};c.paintEconomicSources=()=>{};
 vm.runInContext(['extraChart','fredId','chartSymbol','providerRest','routeHints','chartRoute','economicQualification','economicQualificationText','chartRouteText','openSym'].map(n=>fn(watch,n)).join('\n'),c);return c;
}
// the table as written in the source: target -> the ids routed to it
const table=(()=>{const src=fn(watch,'chartSymbol'),m=/var indexRoutes = (\{[^}]*\}), indexTarget;/.exec(src);assert.ok(m,'indexRoutes table');const o=JSON.parse(m[1]);return Object.fromEntries(Object.entries(o).map(([t,l])=>[t,l.split(' ')]));})();
const TARGETS=Object.keys(table),routed=new Map(TARGETS.flatMap(t=>table[t].map(id=>[id,t])));
const NOT_CASH=/futur|forward|perp|rolling|total return|net total|equal weight|leveraged|inverse|short|islamic|sector|ex-tech|binary|top 30|high beta|ESG|decrement|buy write|stocks above|trade index|3-month|9-day|1-day|6-month|one-year|constituent|monthly only|VIX VOLATILITY|india/i;
const rows=new Map(fixture.search_rows.map(r=>[r.id,r]));

test('the routed ids are exactly the reviewed table, and every id has recorded evidence of what it is',()=>{
 assert.deepEqual(Object.fromEntries(TARGETS.map(t=>[t,table[t].slice().sort()])),Object.fromEntries(Object.keys(fixture.routes).map(t=>[t,fixture.routes[t].slice().sort()])));
 assert.equal(routed.size,48);assert.equal(TARGETS.flatMap(t=>table[t]).length,48);assert.deepEqual(TARGETS,['^GSPC','^NDX','^DJI','^RUT','^IXIC','^VIX','^TNX']);
 const policy=read('aws/lambdas/justhodl-symdir/source/lambda_function.py').match(/SEARCH_POLICY = \{[\s\S]*?\n\}/)[0];
 for(const [id,target] of routed){
  assert.equal(id,id.toUpperCase());assert.match(id,/^[A-Z0-9]+:[A-Z0-9]+$/);
  const row=rows.get(id),describes=new RegExp(fixture.describes[target],'i');
  if(row){assert.equal(row.type,'index',id);assert.match(row.description,describes,id);assert.doesNotMatch(row.description,NOT_CASH,id);continue;}
  if(fixture.from_lists[id]){assert.ok(id==='SPCFD:SPX'||/index/.test(fixture.from_lists[id].type),id);continue;}
  if(fixture.directory_pins.includes(id)){assert.ok(policy.includes('"'+id+'"'),id+' is not a directory pin');continue;}
  assert.ok(fixture.engine_alias.includes(id),id+' has no recorded evidence');
 }
 assert.equal(rows.get('VANTAGE:SP500').description,'S&P Index Cash CFD (USD)');assert.equal(fixture.from_lists['TVC:TNX'].name,'CBOE 10 YR TREASURY NOTE YIELD');
});
for(const [id,target] of routed)test('charts the live index '+target+' for '+id,()=>{
 const c=harness(),r=c.chartRoute(id);
 assert.equal(r.requested,id);assert.equal(r.handoff,target);assert.equal(r.resolved,target);assert.equal(r.primary,target);assert.equal(r.relation,'mapped-route');
 assert.equal(r.reason,'Possible proxy or source substitution; equivalence unverified');
 c.openSym(id);assert.deepEqual(Array.from(c.loads),[target]);assert.equal(c.chartSelection.requested,id);assert.deepEqual(Array.from(c.toasts),[]);
 const text=c.chartRouteText(r,null,[]);assert.ok(text.startsWith('Requested: '+id+' \u00b7 Resolved route: '+target+' \u00b7 Handoff: '+target));assert.doesNotMatch(text,/No supported chart route|Chart unchanged/);
});
test('his exact request, in any letter case, is the same route',()=>{
 for(const id of ['VANTAGE:SP500','vantage:sp500','Vantage:Sp500',' VANTAGE:SP500 ']){const c=harness(),r=c.chartRoute(id);assert.equal(r.handoff,'^GSPC',id);assert.equal(r.requested,id.trim());}
 for(const id of ['SP:SPX SPCFD:SPX','I:SPX I:NDX','^GSPC ^NDX']){const c=harness();assert.equal(c.chartSymbol(id),'',id);}   // a list of ids is not an id
});
test('every search row that is not a reviewed cash index stays off the index routes',()=>{
 const before={'TVC:SPX':'^GSPC','CBOE:VIX':'^VIX'};                         // routed to an index before this change
 let checked=0;
 for(const row of fixture.search_rows){
  if(/\s/.test(row.id)||routed.has(row.id))continue;
  const c=harness(),r=c.chartRoute(row.id);checked++;
  if(before[row.id]){assert.equal(r.handoff,before[row.id],row.id);continue;}
  assert.ok(!TARGETS.includes(r.handoff),row.id+' ('+row.description+') -> '+r.handoff);
 }
 assert.ok(checked>190,String(checked));
 for(const id of ['VANTAGE:SP500FT','BLACKBULL:US500.F','PEPPERSTONE:US500F','PEPPERSTONE:US500.P','TRADENATION:US500FU','CBOE:XSP','PRIMEXBT:US500','LSE:SPX','GETTEX:SPX','XETR:SPX','CME:ES','MEXC:SP500USDT.P',
                  'NASDAQ:NDXE','NASDAQ:NDXT','NASDAQ:XNDX','FWB:NDX','HNX:NDX','XETR:NDX1','DJ:DJIM','DJ:DJITR','DJ:DJIAFP','TOCOM:DJIA','PEPPERSTONE:VIX','CAPITALCOM:VIX','FPMARKETS:VIX','CBOE:VVIX','TVC:US30']){
  const c=harness(),r=c.chartRoute(id);assert.ok(!TARGETS.includes(r.handoff),id+' -> '+r.handoff);c.openSym(id);assert.ok(!Array.from(c.loads).some(x=>TARGETS.includes(x)),id);
 }
});
test('NASDAQ:NDX, TradingView\'s first answer for ndx, reaches the index through the engine\'s own alias',()=>{
 // the live symbol map calls it a US listing named NDX; the router hands NDX on and the engine resolves that to the index
 for(const map of [null,{'NASDAQ:NDX':{source:'MARKET',id:'NDX',confidence:0.9,note:'US listing'}}]){
  const c=harness(map),r=c.chartRoute('NASDAQ:NDX');assert.equal(r.handoff,'NDX');assert.doesNotMatch(r.reason,/ignored/);
  c.openSym('NASDAQ:NDX');assert.deepEqual(Array.from(c.loads),['NDX']);assert.equal(c.resolveSym('NDX').ticker,'^NDX');
 }
 const c=harness(),d=c.chartRoute('DJI');assert.equal(d.handoff,'DJI');assert.equal(c.resolveSym('DJI').ticker,'^DJI');
});
test('the ids that already charted are untouched',()=>{
 for(const [id,handoff] of [['SPX','^GSPC'],['TVC:SPX','^GSPC'],['INDEX:SPX','^GSPC'],['INDEX:SP500','^GSPC'],['^GSPC','^GSPC'],['SPY','SPY'],['AMEX:SPY','AMEX:SPY'],['FRED:SP500','FRED:SP500'],['SP500','FRED:SP500'],['TVC:GOLD','GC=F'],
                            ['NASDAQ:AAPL','NASDAQ:AAPL'],['NDX','^NDX'],['RUT','^RUT'],['VIX','^VIX'],['CBOE:VIX','^VIX'],['TVC:US10Y','FRED:DGS10'],['TVC:DXY','DX-Y.NYB'],['FRED:VIXCLS','FRED:VIXCLS'],['NASDAQ:QQQ','NASDAQ:QQQ']]){
  const c=harness(),r=c.chartRoute(id);assert.equal(r.handoff,handoff,id);
 }
});
test('a reviewed index id decides before the symbol map; the map still decides everything else',()=>{
 // the live map sends TVC:VIX to FRED's next-day copy (curated); the chart serves the index itself
 const c=harness({'TVC:VIX':{source:'FRED',id:'VIXCLS',confidence:1,note:'curated'},'VANTAGE:SP500':{source:'FRED',id:'SP500'},'TVC:DJI':{source:'MARKET',id:'^DJI'},'ECONOMICS:USM0':{source:'FRED',id:'BOGMBASE'}});
 assert.equal(c.chartRoute('TVC:VIX').handoff,'^VIX');assert.equal(c.chartRoute('VANTAGE:SP500').handoff,'^GSPC');
 assert.equal(c.chartRoute('TVC:DJI').handoff,'^DJI');assert.equal(c.chartRoute('ECONOMICS:USM0').handoff,'FRED:BOGMBASE');
 assert.equal(c.chartRoute('FRED:VIXCLS').handoff,'FRED:VIXCLS');                       // asked for by name, FRED stays FRED
});

// ---- the chart engine
function engineScope(){
 const c={window:{},Date,Promise,WeakMap,JSON,Math,isFinite};vm.createContext(c);
 vm.runInContext(['resolveSym','yahooSym','bare','observationId','identifyBars','klines'].map(n=>fn(engine,n)).join('\n')+'\nvar barEvidence=new WeakMap(),barCache={},lastSource="";function spec(tf){return [tf,"day","1d","max"];}',c);return c;
}
test('the engine resolves the directory\'s Polygon index ids, NASDAQ:NDX and DJ:DJI to the live index',()=>{
 const c=engineScope();
 for(const [id,ticker] of [['I:SPX','^GSPC'],['I:NDX','^NDX'],['I:DJI','^DJI'],['I:RUT','^RUT'],['I:VIX','^VIX'],['I:COMP','^IXIC'],['NASDAQ:NDX','^NDX'],['DJ:DJI','^DJI'],['DJI','^DJI'],['i:spx','^GSPC']]){
  const r=c.resolveSym(id);assert.equal(r.ticker,ticker,id);assert.equal(r.raw,ticker,id);assert.equal(r.yahoo,ticker,id);assert.equal(r.engine,'equity',id);
 }
 for(const [id,ticker] of [['NASDAQ:AAPL','AAPL'],['SPY','SPY'],['TVC:SPX','^GSPC'],['INDEX:SPX','^GSPC'],['NASDAQ:NDXE','NDXE'],['NASDAQ:QQQ','QQQ'],['DJ:DJIM','DJIM'],['I:XYZ','XYZ']])assert.equal(c.resolveSym(id).ticker,ticker,id);
 // an id the alias table turns into an index skips the banked /series copy; any other exchange-qualified id still asks for its exact packet
 const guard=fn(engine,'klines').match(/if\(String\(sym\)\.indexOf\(":"\)>=0 && ([^{]*)\)\{/);assert.ok(guard);
 assert.ok(guard[1].includes('!(/^\\^/.test(String(rs.raw||"")))'));assert.ok(!guard[1].includes('/^TVC:/i.test(String(sym))'));
});
test('a cached frame keeps its own label when another name asks for the same bars',async()=>{
 const c=engineScope(),bars=Array.from({length:12},(_,i)=>({time:1700000000+i*86400,open:1,high:2,low:1,close:1.5,volume:0}));
 c.identifyBars(bars,'^GSPC','1d','yahoo');c.barCache['^GSPC|1d']={d:bars,at:Date.now(),src:'yahoo'};
 const frame=vm.runInContext('(d)=>barEvidence.get(d)',c);
 // the quote line's "vs SPX" lookup, a watchlist quote for SPX or SP500: the same instrument under another name
 for(const alias of ['SPX','SP500']){
  const got=await c.klines(alias,'1d',true);
  assert.notEqual(got,bars);assert.equal(got.length,bars.length);assert.deepEqual(JSON.parse(JSON.stringify(got)),JSON.parse(JSON.stringify(bars)));
  assert.equal(frame(got).symbol,alias);assert.equal(frame(bars).symbol,'^GSPC');assert.equal(frame(bars).interval,'1d');
 }
 // the same name gets the same array and the same frame, as before
 const again=await c.klines('^GSPC','1d',true);assert.equal(again,bars);assert.equal(frame(bars).symbol,'^GSPC');
});

// ---- spread legs
function exprScope(series,kline,route){
 const calls={series:[],klines:[],route:[]};
 const c={setInterval,clearInterval,setTimeout,Promise,JSON,Math,Date,encodeURIComponent,isFinite,fetch:async u=>{const id=decodeURIComponent(new URL(u).searchParams.get('id'));calls.series.push(id);return{json:async()=>series[id]?{id,name:id,freq:'D',obs:series[id]}:{error:'no warehouse bars for '+id}};}};
 c.window=c;if(kline)c.jhKlines=async(sym,tf)=>{calls.klines.push(sym+'|'+tf);const d=kline[sym];if(d==='throw')throw new Error('resolver failed');return (d||[]).map(([day,close])=>({time:Date.parse(day+'T00:00:00Z')/1000,open:close,high:close,low:close,close}));};
 if(route)c.jhWatchlistRoute=id=>{calls.route.push(id);return route[id]||'';};
 vm.createContext(c);vm.runInContext(read('jh-chart-expr.js'),c);return {E:c.JHChartExpr,calls};
}
test('a market leg of a spread is read from the chart\'s own bars for that symbol, through the reviewed route',async()=>{
 const kline={'^GSPC':[['2026-11-17',7300],['2026-11-18',7320],['2026-11-19',7200]],'GC=F':[['2026-11-17',3650],['2026-11-18',3660],['2026-11-19',3600]],'TLT':[['2026-11-18',90],['2026-11-19',91]]};
 const route={'VANTAGE:SP500':'^GSPC','TVC:GOLD':'GC=F','FRED:BAMLH0A0HYM2':'FRED:BAMLH0A0HYM2'};
 const series={'VANTAGE:SP500':[['2016-08-08',2180],['2026-11-17',7340.53]],'FRED:BAMLH0A0HYM2':[['2026-11-17',3.1],['2026-11-18',3.2]]};
 let {E,calls}=exprScope(series,kline,route);
 let r=await E.evaluate('VANTAGE:SP500/TVC:GOLD');
 assert.equal(JSON.stringify(r.d.map(b=>b.close)),JSON.stringify([7300/3650,7320/3660,7200/3600]));
 assert.deepEqual(calls.series,[]);assert.deepEqual(calls.klines.sort(),['GC=F|1d','^GSPC|1d']);
 assert.deepEqual(JSON.parse(JSON.stringify(r.legs)),[{id:'VANTAGE:SP500',name:'VANTAGE:SP500',first:'2026-11-17',last:'2026-11-19',n:3,via:'^GSPC'},{id:'TVC:GOLD',name:'TVC:GOLD',first:'2026-11-17',last:'2026-11-19',n:3,via:'GC=F'}]);
 assert.match(r.src,/^Spread 2 legs \u00b7 the chart's own daily bars \u00b7 as-of aligned on VANTAGE:SP500 \u00b7 VANTAGE:SP500 as \^GSPC 2026\u2192, TVC:GOLD as GC=F 2026\u2192$/);
 // a plain US listing drops its exchange prefix, exactly as a watchlist row does; the router is not asked
 ({E,calls}=exprScope(series,kline,route));r=await E.evaluate('VANTAGE:SP500/NASDAQ:TLT');
 assert.equal(JSON.stringify(r.d.map(b=>b.close)),JSON.stringify([7320/90,7200/91]));assert.deepEqual(calls.route,['VANTAGE:SP500']);assert.ok(calls.klines.includes('TLT|1d'));
 // a data-series leg is still read in full from /series; the source line says which leg came from where
 ({E,calls}=exprScope(series,kline,route));r=await E.evaluate('VANTAGE:SP500/FRED:BAMLH0A0HYM2');
 assert.deepEqual(calls.series,['FRED:BAMLH0A0HYM2']);assert.equal(r.legs[0].via,'^GSPC');assert.equal(r.legs[1].via,null);
 assert.match(r.src,/the chart's own daily bars and \/series history \u00b7 .*VANTAGE:SP500 as \^GSPC 2026\u2192, FRED:BAMLH0A0HYM2 2026\u2192$/);
 assert.equal(r.d[r.d.length-1].close,7200/3.2);                                            // as-of: the 18th's spread value carries to the 19th
});
test('a leg the chart cannot draw falls back to /series, and a leg nothing can serve still fails with its reason',async()=>{
 const series={'VANTAGE:SP500':[['2026-11-14',7400],['2026-11-17',7340.53]],'AMEX:XLP':[['2026-11-14',80],['2026-11-17',81]]};
 for(const empty of [[],'throw',[['2026-11-19',7200]]]){                                    // no bars, a failing resolver, a single bar
  const {E,calls}=exprScope(series,{'^GSPC':empty,'XLP':[['2026-11-14',80],['2026-11-17',81]]},{'VANTAGE:SP500':'^GSPC'});
  const r=await E.evaluate('VANTAGE:SP500/AMEX:XLP');
  assert.deepEqual(calls.series,['VANTAGE:SP500']);assert.equal(r.legs[0].via,null);assert.equal(r.legs[1].via,'XLP');assert.equal(JSON.stringify(r.d.map(b=>b.close)),JSON.stringify([7400/80,7340.53/81]));
 }
 const {E}=exprScope({},{},{});await assert.rejects(E.evaluate('SP:NOPE/ECONOMICS:NOPE'),/no warehouse bars/);
 // a route that is still a provider id (a Treasury par tenor, an exchange-qualified crypto pair) is not a market symbol: /series as before
 const s2=exprScope({'TVC:US02Y':[['2026-11-18',3.5],['2026-11-19',3.6]],'BINANCE:BTCUSDT':[['2026-11-18',90000],['2026-11-19',91000]]},{},{'TVC:US02Y':'ustpar:2Y','BINANCE:BTCUSDT':'BINANCE:BTCUSDT'});
 const r2=await s2.E.evaluate('BINANCE:BTCUSDT/TVC:US02Y');assert.deepEqual(s2.calls.klines,[]);assert.deepEqual(s2.calls.series.sort(),['BINANCE:BTCUSDT','TVC:US02Y']);assert.match(r2.src,/\/series full history/);
});
test('without the chart (no bar resolver on the page) every leg is read from /series exactly as before',async()=>{
 const {E,calls}=exprScope({'AMEX:XLY':[['2000-01-03',10],['2000-01-04',12],['2000-01-05',15]],'AMEX:XLP':[['2000-01-03',5],['2000-01-05',6]]},null,{'AMEX:XLY':'XLY'});
 const r=await E.evaluate('AMEX:XLY/AMEX:XLP');assert.equal(JSON.stringify(r.d.map(b=>b.close)),'[2,2.4,2.5]');assert.deepEqual(calls.series.sort(),['AMEX:XLP','AMEX:XLY']);assert.deepEqual(calls.route,[]);
 assert.equal(r.src,'Spread 2 legs \u00b7 /series full history \u00b7 as-of aligned on AMEX:XLY \u00b7 AMEX:XLY 2000\u2192, AMEX:XLP 2000\u2192');
});
test('the router exports its route without opening anything',()=>{
 assert.ok(watch.includes('window.jhWatchlistRoute = function (s) { try { return chartRoute(s).handoff || ""; } catch (e) { return ""; } };'));
 const c=harness();vm.runInContext('window.jhWatchlistRoute = function (s) { try { return chartRoute(s).handoff || ""; } catch (e) { return ""; } };',c);
 assert.equal(c.window.jhWatchlistRoute('VANTAGE:SP500'),'^GSPC');assert.equal(c.window.jhWatchlistRoute('TVC:GOLD'),'GC=F');assert.equal(c.window.jhWatchlistRoute('TVC:TNX'),'^TNX');assert.equal(c.window.jhWatchlistRoute('TVC:VIX'),'^VIX');
 assert.equal(c.window.jhWatchlistRoute('TVC:DXY'),'DX-Y.NYB');assert.equal(c.window.jhWatchlistRoute('LSE:SPX'),'');assert.equal(c.window.jhWatchlistRoute('A B'),'');assert.equal(c.window.jhWatchlistRoute(null),'');
 assert.deepEqual(Array.from(c.loads),[]);assert.equal(c.chartSelection,null);assert.equal(c.q.value,'');
});
test('the details card writes the history line of a non-ticker symbol from the last bar it holds',()=>{
 const src=read('jh-tv-details.js');
 assert.ok(src.includes('fdate(day(b[0].time)) + " \u2192 " + fdate(day(L0.time)) + "</span><span>Observations</span>'));
 assert.ok(!src.includes('fdate(day(b[0].time)) + " \u2192 " + fdate(day(L.time))'));
});
