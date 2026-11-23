// Khalid 2026-11-21: "INDEX:BTCUSD · No supported chart route; chart unchanged i cant pull btc on chart. investigate why i cant pull symbols out of search bar".
//  Measured on the live site first (tests/fixtures/chart-crypto-routes/evidence-2026-11-21.json, ops 6879 - 6882):
//  (1) INDEX:BTCUSD - TradingView's first answer for "bitcoin" - had no route: the symbol map sends it to a provider's coin id the router cannot use, and a map hit it cannot use
//      ended the search; (2) INDEX:ETHUSD went to "^ETHUSD", which nothing serves; (3) BINANCE:BTCUSDT, COINBASE:SOLUSD, KRAKEN:XBTUSD and a bare SOLUSD took the native route and
//      drew nothing; (4) the directory's own crypto ids (X:BTCUSD ...) drew banked bars that end yesterday; (5) the outside history joined to four banked coins was another coin.
// Actual source functions (watchlist router, chart engine, catalog); no network.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
const read=f=>fs.readFileSync(f,'utf8'),watch=read('jh-chart-tvwatch.js'),engine=read('jh-chart-engine.js'),catalog=read('jh-chart-catalog.js');
const fixture=JSON.parse(read('tests/fixtures/chart-crypto-routes/evidence-2026-11-21.json'));
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const trees=new Map();function nodes(src){if(trees.has(src))return trees.get(src);const out=[],stack=[acorn.exports.parse(src,{ecmaVersion:'latest'})];while(stack.length){const n=stack.pop();if(!n||typeof n!=='object')continue;if(n.type)out.push(n);for(const v of Object.values(n)){if(Array.isArray(v))stack.push(...v);else if(v&&typeof v==='object')stack.push(v);}}trees.set(src,out);return out;}
function fn(src,name){const a=nodes(src).filter(n=>n.type==='FunctionDeclaration'&&n.id.name===name);assert.equal(a.length,1,name);return src.slice(a[0].start,a[0].end);}
function variable(src,name){const n=nodes(src).find(n=>n.type==='VariableDeclarator'&&n.id.name===name);assert.ok(n,name);return 'var '+src.slice(n.start,n.end)+';';}
const assignments=nodes(engine).filter(n=>n.type==='AssignmentExpression'&&n.left.type==='MemberExpression'&&n.left.object.name==='q'&&n.left.property.name==='onkeydown'&&n.right.type==='FunctionExpression'&&engine.slice(n.right.start,n.right.end).includes('var s=q.value.trim().toUpperCase(); if(TABS.indexOf(s)<0)'));
assert.equal(assignments.length,1);const nativeQ=engine.slice(assignments[0].right.start,assignments[0].right.end);
function harness(map=null,source=watch){
 const c={window:{},Set,Number,Date,AbortController,setTimeout,clearTimeout,painted:[],toasts:[],loads:[],symMap:map,symMapReady:1,chartSelection:null,chartRouteObserver:null,TABS:[],active:'INITIAL',filter:''};
 c.q={value:'',dispatchEvent(e){this.onkeydown(e);}};c.document={getElementById(id){return id==='q'?c.q:null;}};c.KeyboardEvent=function(_,o){return o;};c.paintCard=s=>c.painted.push(s);c.toast=s=>c.toasts.push(s);c.loadSymMap=()=>Promise.resolve();
 c.loadDraw=c.renderTabs=c.renderList=()=>{};c.load=()=>c.loads.push(c.active);c.activeSym=()=>c.active;c.lookupSym=()=>'';
 vm.createContext(c);vm.runInContext(fn(catalog,'H')+['EXCH','SERIES_PROV','CURATED','DEFILLAMA_IDS','REGIONAL_FED_IDS'].map(n=>variable(catalog,n)).join('\n')+fn(catalog,'isWarehouse')+fn(catalog,'chartId'),c);
 c.window.JHChartCatalog={isWarehouse:c.isWarehouse,chartId:c.chartId,curated:c.CURATED};
 vm.runInContext(['resolveSym','yahooSym','bare','observationId'].map(n=>fn(engine,n)).join('\n'),c);vm.runInContext(fn(engine,'normQ')+fn(engine,'classifySym')+variable(engine,'SEARCH_ALIASES'),c);
 const exported=nodes(engine).find(n=>n.type==='AssignmentExpression'&&n.left.type==='MemberExpression'&&n.left.property.name==='jhWatchlistResolve');assert.ok(exported);vm.runInContext(engine.slice(exported.start,exported.end),c);c.q.onkeydown=vm.runInContext('('+nativeQ+')',c);
 c.paintChartRoute=()=>{};c.paintEconomicSources=()=>{};
 vm.runInContext(['extraChart','fredId','chartSymbol','providerRest','routeHints','formulaRoute','chartRoute','economicQualification','economicQualificationText','chartRouteText','openSym'].map(n=>fn(source,n)).join('\n'),c);return c;
}
// the router exactly as it was before this change, rebuilt from the ledger
const before=require('./helpers/chart-macro-line-preservation.cjs');
function predecessor(file,src){let raw=src;
 const names=file==='jh-chart-tvwatch.js'?['tvwatch.edits.json','tvwatch-route.edits.json']:['engine.edits.json'];
 const edits=[].concat(...names.map(f=>JSON.parse(read('tests/fixtures/chart-crypto-routes/'+f))));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,file+': '+e.after.slice(0,40));raw=raw.replace(e.after,e.before);}return raw;}
// later additions to the engine (indicator controls, 2026-11-23; the Sovereign desk button, 2026-11-22; the Repo desk button, 2026-11-21) are peeled first, newest first, then this change's own ledger
const watchBefore=predecessor('jh-chart-tvwatch.js',watch),engineBefore=predecessor('jh-chart-engine.js',before.peelRepoDeskEngine(before.peelSovDeskEngine(before.peelIndicatorControls(engine,'jh-chart-engine.js'),'jh-chart-engine.js'),'jh-chart-engine.js'));
const COINS=fixture.banked_coins.filter(c=>!fixture.empty_bank.includes(c)),LIVE_MAP=Object.fromEntries(Object.entries(fixture.symbol_map_hits));
const VENUES=['INDEX','CRYPTO','COINBASE','BITSTAMP','KRAKEN','BINANCE','BINANCEUS','BYBIT','BITFINEX','GEMINI','OKX','KUCOIN','MEXC','GATEIO','HTX','BITGET','CRYPTOCOM','POLONIEX','PYTH'];
const routerList=(()=>{const m=/var cryptoCoins = "([A-Z0-9 ]+)";/.exec(fn(watch,'chartSymbol'));assert.ok(m,'router coin list');return m[1];})();
const engineList=(()=>{const m=/var CRYPTO_TAPE="([A-Z0-9 ]+)";/.exec(engine);assert.ok(m,'engine coin list');return m[1];})();

test('the router and the engine carry one coin list, and it is the coins with banked bars',()=>{
 assert.equal(routerList,engineList);assert.equal(routerList,' '+COINS.join(' ')+' ');assert.equal(fixture.banked_coins.length,93);assert.deepEqual(fixture.empty_bank,['AR','CFX','VIRTUAL']);assert.equal(COINS.length,90);assert.deepEqual(COINS,COINS.slice().sort());
 for(const c of ['BTC','ETH','SOL','XRP','DOGE','ARB','TON','SUI','PEPE'])assert.ok(COINS.includes(c),c);
 const venues=/var cryptoPair = \/\^\(\?:\(([A-Z|]+)\):\)\?/.exec(fn(watch,'chartSymbol'));assert.ok(venues);assert.deepEqual(venues[1].split('|'),VENUES);
});
test('his exact request charts bitcoin, with the live symbol map loaded and in any letter case',()=>{
 for(const id of ['INDEX:BTCUSD','index:btcusd','Index:BtcUsd',' INDEX:BTCUSD ']){
  const c=harness(LIVE_MAP),r=c.chartRoute(id);
  assert.equal(r.requested,id.trim());assert.equal(r.handoff,'BTC-USD',id);assert.equal(r.resolved,'BTC-USD');assert.equal(r.primary,'BTC-USD');assert.equal(r.relation,'mapped-route');
  assert.match(r.reason,/^Possible proxy or source substitution; equivalence unverified\. Reviewed crypto route: the coin's US dollar tape/);
 }
 const c=harness(LIVE_MAP);c.openSym('INDEX:BTCUSD');assert.deepEqual(Array.from(c.loads),['BTC-USD']);assert.equal(c.chartSelection.requested,'INDEX:BTCUSD');assert.deepEqual(Array.from(c.toasts),[]);
 const text=c.chartRouteText(c.chartRoute('INDEX:BTCUSD'),null,[]);
 assert.ok(text.startsWith('Requested: INDEX:BTCUSD · Resolved route: BTC-USD · Handoff: BTC-USD'),text.slice(0,120));assert.doesNotMatch(text,/No supported chart route|Chart unchanged|Resolved route: unavailable/);
});
test('before the change the same request, with the same map, had no route - and the status line was the one he pasted',()=>{
 assert.equal(fixture.live_before['INDEX:BTCUSD'].verdict,'NO ROUTE');assert.equal(fixture.dialog_clicks_before[0].row,'INDEX:BTCUSD');assert.equal(fixture.dialog_clicks_before[0].verdict,'NO ROUTE');
 const c=harness(LIVE_MAP,watchBefore),r=c.chartRoute('INDEX:BTCUSD');assert.equal(r.handoff,null);assert.equal(r.reason,'No supported chart route; chart unchanged');
 c.openSym('INDEX:BTCUSD');assert.deepEqual(Array.from(c.loads),[]);assert.deepEqual(Array.from(c.toasts),['INDEX:BTCUSD · No supported chart route; chart unchanged']);
 const text=c.chartRouteText(r,null,[]);assert.match(text,/^Requested: INDEX:BTCUSD · Resolved route: unavailable · Handoff: none · No supported chart route; chart unchanged/);
 assert.equal(harness(null,watchBefore).chartRoute('INDEX:BTCUSD').handoff,'BTC-USD');      // without the map the old venue rule answered: the map hit was the dead end
});
for(const [id,target] of Object.entries(fixture.expected))test('charts '+target+' for '+id+' (live before: '+((fixture.live_before[id]||{}).verdict||'not measured')+')',()=>{
 for(const map of [null,LIVE_MAP,{[id]:{source:'COINGECKO',id:'bitcoin'}}]){
  const c=harness(map),r=c.chartRoute(id);assert.equal(r.handoff,target,id);assert.equal(r.resolved,target);assert.equal(r.relation,'mapped-route');
  c.openSym(id);assert.deepEqual(Array.from(c.loads),[target]);assert.deepEqual(Array.from(c.toasts),[]);assert.equal(c.window.jhWatchlistResolve(target).engine,'equity');
 }
});
// the dollar itself is read first: ARBUSD is ARB against the dollar, not AR against BUSD
function reading(pair){for(const q of ['USD','USDT','USDC','BUSD'])if(pair.endsWith(q)&&COINS.includes(pair.slice(0,-q.length)))return pair.slice(0,-q.length);return '';}
test('every banked coin, under every listed venue and dollar quote, is that coin\'s tape',()=>{
 let n=0;const twoReadings=new Set();
 for(const coin of COINS){
  for(const v of VENUES)for(const q of ['USD','USDT','USDC','BUSD']){const c=harness(),id=v+':'+coin+q,want=reading(coin+q);if(want!==coin)twoReadings.add(coin+q+'='+want);assert.equal(c.chartSymbol(id),want+'-USD',id);n++;}
  const c=harness();assert.equal(c.chartRoute('BINANCE:'+coin+'USDT').handoff,coin+'-USD');assert.equal(c.chartRoute('INDEX:'+coin+'USD').handoff,coin+'-USD');assert.equal(c.chartRoute(coin+'USD').handoff,coin+'-USD',coin+'USD');
 }
 assert.equal(n,90*19*4);assert.deepEqual([...twoReadings],[]);
 const ar=harness();assert.equal(ar.chartSymbol('BINANCE:ARBUSD'),'ARB-USD');assert.equal(ar.chartSymbol('ARBUSD'),'ARB-USD');      // the dollar is read first: ARB against the dollar, never AR against BUSD
 const k=harness();assert.equal(k.chartSymbol('KRAKEN:XBTUSD'),'BTC-USD');assert.equal(k.chartSymbol('KRAKEN:XBTUSDT'),'BTC-USD');assert.equal(k.chartSymbol('KRAKEN:XDGUSD'),'DOGE-USD');
});
test('what is deliberately not routed stays as it was',()=>{
 const same=['BINANCE:ETHBTC','KRAKEN:ETHBTC','BINANCE:FOOUSDT','COINBASE:FOOUSD','BINANCE:ARUSDT','COINBASE:CFXUSD','INDEX:VIRTUALUSD','VIRTUALUSD','BINANCE:BTCUSDT.P','BYBIT:ETHUSDU2025','BINANCE:BTCUSDH2025','CRYPTOCAP:BTC.D','CRYPTOCAP:TOTAL','CRYPTOCAP:BTC','BITFINEX:BTCUSDLONGS','GLASSNODE:BTC_SOPR',
  'INTOTHEBLOCK:BTC_HASHRATE','INDEX:SPXUSD','INDEX:ADVN','OANDA:EURUSD','FX:GBPUSD','FX_IDC:JPYUSD','EURUSD','XAUUSD','XAGUSD','BUSD','USDTUSD','BTCUSDT','BTCUSDC','BTC','ETH','BTC-USD','ETH-USD','AAPL','COIN','MSTR','NASDAQ:AAPL','NASDAQ:COIN',
  'AMEX:IBIT','CME_MINI:ES1!','CME:GE1!','TVC:GOLD','I:SPX','VANTAGE:SP500','FRED:DGS10','X:BTCUSD','X:SOLUSD','BINANCE:BTCUSD BINANCE:ETHUSD'];
 for(const map of [null,LIVE_MAP])for(const id of same){
  const a=harness(map),b=harness(map,watchBefore);assert.equal(a.chartSymbol(id),b.chartSymbol(id),'chartSymbol '+id);
  const ra=a.chartRoute(id),rb=b.chartRoute(id);for(const k of ['handoff','resolved','primary','fallback','relation','reason'])assert.equal(ra[k],rb[k],id+' '+k);
 }
 const c=harness();assert.equal(c.chartRoute('BINANCE:FOOUSDT').handoff,'BINANCE:FOOUSDT');assert.equal(c.chartRoute('BINANCE:FOOUSDT').relation,'native-route');   // a coin with no banked bars keeps the native route
 assert.equal(c.chartRoute('BINANCE:ETHBTC').handoff,'BINANCE:ETHBTC');assert.equal(c.chartRoute('BTCUSD').handoff,'BTC-USD');assert.equal(c.chartRoute('BTC').handoff,'BTC-USD');
 assert.equal(harness(LIVE_MAP).chartRoute('CRYPTOCAP:BTC.D').handoff,null);
});
test('a reviewed crypto route outranks the native route only for the ids the table decided',()=>{
 const c=harness();
 for(const id of ['BINANCE:BTCUSDT','BINANCE:SOLUSDT','COINBASE:SOLUSD','KRAKEN:XBTUSD','SOLUSD']){
  const native=c.window.jhWatchlistResolve(id),base=id.includes(':')?id.split(':')[1]:id;assert.equal(native.engine,'equity');assert.equal(native.ticker,base,id);      // the engine would take it natively...
  const old=harness(null,watchBefore).chartRoute(id);assert.equal(old.handoff,id,id);                                                                        // ...and before the change the router let it
  assert.match(fixture.live_before[id].verdict,/BLANK|same chart/,id);                                                                                       // which drew nothing on the live site
  const r=c.chartRoute(id);assert.notEqual(r.handoff,id);assert.equal(r.relation,'mapped-route');assert.equal(r.fallback,null);
 }
 assert.equal(c.chartRoute('NASDAQ:AAPL').handoff,'NASDAQ:AAPL');assert.equal(c.chartRoute('NASDAQ:AAPL').relation,'native-route');assert.equal(c.chartRoute('AAPL').relation,'exact');
});
test('CME bitcoin and ether futures route to the contracts the futures feed carries',()=>{
 assert.match(fixture.sources.futures,/BTC=F 2,248 daily bars/);
 for(const [id,target] of [['CME:BTC1!','BTC=F'],['CME:MBT1!','BTC=F'],['CME:ETH1!','ETH=F'],['CME:MET1!','ETH=F']])for(const map of [null,LIVE_MAP]){
  const c=harness(map),r=c.chartRoute(id);assert.equal(r.handoff,target,id);assert.equal(r.relation,'mapped-route');assert.equal(r.reason,'Possible proxy or source substitution; equivalence unverified');
 }
 assert.equal(harness(LIVE_MAP,watchBefore).chartRoute('CME:BTC1!').handoff,null);assert.equal(fixture.live_before['CME:BTC1!'].verdict,'NO ROUTE');
 const c=harness();assert.equal(c.chartRoute('CME:BTC2!').handoff,harness(null,watchBefore).chartRoute('CME:BTC2!').handoff);assert.equal(c.chartRoute('CME_MINI:ES1!').handoff,'ES=F');
});

// ---------------------------------------------------------------- the engine: directory ids, and the same-coin rule
function engineScope(src=engine){const c={Math,Number,String,Object,Date,isFinite};vm.createContext(c);vm.runInContext(fn(src,'utcMidnight')+(src===engine?variable(src,'CRYPTO_TAPE')+fn(src,'cryptoTapeId')+fn(src,'sameCoinTape'):''),c);return c;}
test('the directory\'s id of a banked coin opens that coin\'s tape; nothing else is touched',()=>{
 const c=engineScope();
 for(const coin of COINS){assert.equal(c.cryptoTapeId('X:'+coin+'USD'),coin+'-USD');assert.equal(c.cryptoTapeId('x:'+coin.toLowerCase()+'usd'),coin+'-USD');assert.equal(c.cryptoTapeId(' X:'+coin+'USD '),coin+'-USD');}
 for(const coin of COINS)assert.equal(c.cryptoTapeId(coin+'USD'),coin+'-USD');                                  // the pair typed with no venue: no feed answers "SOLUSD" (live before: the chart stayed where it was)
 assert.equal(fixture.live_before.SOLUSD.verdict,'same chart');assert.equal(c.cryptoTapeId('arbusd'),'ARB-USD');assert.equal(c.cryptoTapeId('BTCUSD'),'BTC-USD');
 for(const id of ['X:BTCEUR','X:BTCUSDT','BTCUSDT','BTCUSDC','ETHBTC','X:FOOUSD','FOOUSD','X:ARUSD','ARUSD','X:CFXUSD','X:VIRTUALUSD','EURUSD','XAUUSD','BUSD','USD','X:USD','BTC-USD','I:SPX','C:EURUSD','INDEX:BTCUSD','COINBASE:BTCUSD','FRED:DGS10','AAPL','COIN','',null,undefined])assert.equal(c.cryptoTapeId(id),'',String(id));
 const chartId=fn(engine,'chartId');assert.ok(chartId.startsWith('function chartId(s){\n    s=String(s||"").trim();\n    s=cryptoTapeId(s)||s;'),'the directory id is rewritten before anything else decides');
 assert.equal(fn(engineBefore,'chartId'),chartId.replace(/\n    s=cryptoTapeId\(s\)\|\|s;[^\n]*/,''));
 for(const id of ['X:BTCUSD','X:SOLUSD','X:ARBUSD','X:TONUSD','X:SUIUSD']){assert.equal(fixture.live_before[id].verdict,'CHARTS');assert.equal(fixture.live_before[id].last,'2026-11-20',id+' ended a day behind before the change');}
 assert.equal(fixture.live_before['BTC-USD'].last,'2026-11-21');
});
const DAY=86400,T0=Date.UTC(2026,9,1)/1000;
function series(n,price,start=0){return Array.from({length:n},(_,i)=>({time:T0+(start+i)*DAY,open:price(i),high:price(i)*1.01,low:price(i)*0.99,close:price(i),volume:10}));}
test('an outside daily series joins banked bars only when it is the same coin',()=>{
 const c=engineScope(),bank=series(60,i=>100+i);
 assert.equal(c.sameCoinTape(series(60,i=>(100+i)*1.002),bank),true);                               // another venue, the same coin
 assert.equal(c.sameCoinTape(series(400,i=>(100+i-340)*0.998,-340),bank),true);                     // long outside history, agreeing where both have days
 assert.equal(c.sameCoinTape(series(60,i=>(100+i)*0.0039),bank),false);                             // ARB / TON / AERO / JUP: another coin under the same letters
 assert.equal(c.sameCoinTape(series(60,i=>(100+i)*0.936),bank),false);                              // 6% away on the median is not the same tape
 assert.equal(c.sameCoinTape(series(60,i=>(100+i)*(i===59?0.5:1.001)),bank),true);                  // one wild day does not move a median
 assert.equal(c.sameCoinTape(series(4,i=>100+56+i,56),bank),false);                                 // four shared days cannot show it
 assert.equal(c.sameCoinTape(series(5,i=>100+55+i,55),bank),true);
 assert.equal(c.sameCoinTape(series(60,i=>100+i,200),bank),false);                                  // no shared day
 for(const [a,b] of [[[],bank],[bank,[]],[null,bank],[bank,null],[undefined,undefined]])assert.equal(c.sameCoinTape(a,b),false);
 assert.equal(c.sameCoinTape(series(60,i=>100+i).map(b=>({...b,time:b.time*1000})),bank),true);     // milliseconds or seconds
 for(const coin of ['ARB','TON','AERO','JUP']){const v=fixture.vendor_agreement[coin].yahoo;assert.equal(v.verdict,'DIFFERENT');assert.ok(v.median_pct>99,coin);}
 for(const coin of ['BTC','ETH','SOL']){const v=fixture.vendor_agreement[coin];assert.equal(v.yahoo.verdict,'SAME');assert.ok(v.yahoo.median_pct<1&&v.coinbase.median_pct<1&&v.binance.median_pct<1,coin);}
});
test('the engine\'s join asks that question before it merges, and merges exactly as before when the answer is yes',()=>{
 const klines=engine.slice(engine.indexOf('async function klines'),engine.indexOf('function computeChange')),old=engineBefore.slice(engineBefore.indexOf('async function klines'),engineBefore.indexOf('function computeChange'));
 assert.equal(klines.split('if(ydC.length>=8 && sameCoinTape(ydC, d)){').length,2);assert.equal(old.split('if(ydC.length>=8){').length,2);
 assert.equal(klines.replace('if(ydC.length>=8 && sameCoinTape(ydC, d)){','if(ydC.length>=8){'),old);
 assert.ok(klines.indexOf('sameCoinTape(ydC, d)')<klines.indexOf('d=mergeByDay(ydC, tape);'));
});
// the engine's own loader, end to end, on invented packets: every top-level function of the engine, the first answer is the banked tape and the second the outside history
function boot(packets){
 const top=acorn.exports.parse(engine,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
 const fns=top.filter(n=>n.type==='FunctionDeclaration').map(n=>engine.slice(n.start,n.end)),tfs=top.find(n=>n.type==='VariableDeclaration'&&n.declarations.some(d=>d.id.name==='TFS'));
 const calls=[],c={window:{},barCache:{},barEvidence:new WeakMap(),lastSource:'',PROXY:'https://invented.proxy.test',LIVE:'https://invented.live.test'};vm.createContext(c);vm.runInContext(fns.join('\n')+'\n'+engine.slice(tfs.start,tfs.end),c);
 c.fetchJson=async url=>{calls.push(url);assert.ok(packets.length>=calls.length,'unexpected request '+url);return structuredClone(packets[calls.length-1]);};
 return {c,calls};
}
test('the loader joins a same-coin outside history and refuses another coin\'s, end to end',async()=>{
 const bar=(i,close)=>({time:T0+i*DAY,open:close,high:close*1.01,low:close*0.99,close,volume:100}),px=i=>100+i*.1;
 const bank=Array.from({length:40},(_,i)=>bar(i,px(i))),same=Array.from({length:60},(_,i)=>bar(i-20,px(i-20)*0.995)),other=Array.from({length:60},(_,i)=>bar(i-20,px(i-20)*0.0039));
 const banked={source:'warehouse',warehouse_key:'data/warm/katlin/crypto-bars/ARB.json.gz',bars:bank};
 let h=boot([banked,{source:'yahoo',bars:same}]),out=await h.c.klines('ARB-USD','1d',false);
 assert.equal(h.calls.length,2);assert.match(h.calls[0],/\/ohlc\?ticker=ARB-USD&span=day/);assert.match(h.calls[1],/\/yf-ohlc\?symbol=ARB-USD&range=max&interval=1d&nowarehouse=1$/);
 assert.equal(out.length,60);assert.equal(out[0].time,T0-20*DAY);assert.equal(out[19].close,px(-1)*0.995);assert.match(h.c.lastSource,/\+yahoo$/);
 for(let i=0;i<40;i++)assert.equal(out[20+i].close,px(i));                                                // the banked close wins every shared day
 h=boot([banked,{source:'yahoo',bars:other}]);out=await h.c.klines('ARB-USD','1d',false);
 assert.equal(h.calls.length,2);assert.equal(out.length,40);assert.equal(out[0].time,T0);assert.doesNotMatch(h.c.lastSource,/yahoo/);
 for(let i=0;i<40;i++)assert.equal(out[i].close,px(i));                                                   // not one bar of the other coin
 const today=bar(40,px(40)*0.0039);h=boot([banked,{source:'yahoo',bars:other.concat([today])}]);out=await h.c.klines('ARB-USD','1d',false);
 assert.equal(out.length,40);assert.equal(out.at(-1).close,px(39));                                        // nor its bar for today (the 99% drop the tape showed)
});
test('the ledger reproduces both predecessors byte for byte',()=>{
 const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 assert.equal(hash(watchBefore),'73960757ad566281eb496e729cca8e2c1b4aff32f4f42e75b7f6ff05a3dd4a95');assert.equal(hash(engineBefore),'3e155083ed083a0fe3e4c6cbf9973bc0ce71f3310b564a955c254ed568087272');
 assert.equal(typeof before.normalize,'function');
 for(const name of ['extraChart','fredId','providerRest','routeHints','formulaRoute','openSym','chartRouteText'])assert.equal(fn(watch,name),fn(watchBefore,name),name);
 for(const name of ['resolveSym','yahooSym','bare','isCryptoTape','mergeByDay','asDaily','goSymbol'])assert.equal(fn(engine,name),fn(engineBefore,name),name);
});
