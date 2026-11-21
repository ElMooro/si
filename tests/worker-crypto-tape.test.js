// Khalid 2026-11-21: "i cant pull btc on chart ... i want all my crypto and stocks and etfs to refresh for my data to stay accurate".
// The banked crypto tape through the worker's real /ohlc route, every upstream invented: an outside series joins only when it is the same coin
// (ops 6881: the outside ARB, TON, AERO and JUP are other coins, 99% away), and the days after the bank's last completed day - today's forming
// bar - come from the bank's own venue, then Binance's public market-data host.
const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),fs=require('node:fs'),crypto=require('node:crypto'),{pathToFileURL}=require('node:url');
const SRC=path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src'),workerURL=pathToFileURL(path.join(SRC,'index.js')),tapeURL=pathToFileURL(path.join(SRC,'crypto-tape.js'));
const DAY=86400,NOW=Date.UTC(2026,10,21,17,30),TODAY=Date.UTC(2026,10,21)/1000,px=d=>100+d*.5;            // d = days before today (negative = past)
const day=d=>TODAY+d*DAY;
const bankRows=(from,to)=>{const r=[];for(let d=from;d<=to;d++)r.push([day(d),px(d),px(d)*1.01,px(d)*.99,px(d),1000]);return r;};
const yahoo=(days,scale=1)=>({chart:{result:[{timestamp:days.map(day),indicators:{quote:[{open:days.map(d=>px(d)*scale),high:days.map(d=>px(d)*scale*1.01),low:days.map(d=>px(d)*scale*.99),close:days.map(d=>px(d)*scale),volume:days.map(()=>50)}]}}]}});
const coinbase=(days,scale=1)=>days.slice().reverse().map(d=>[day(d),px(d)*scale*.99,px(d)*scale*1.01,px(d)*scale,px(d)*scale,77]);                       // [time, low, high, open, close, volume], newest first
const binance=(days,scale=1)=>days.map(d=>[day(d)*1000,String(px(d)*scale),String(px(d)*scale*1.01),String(px(d)*scale*.99),String(px(d)*scale),'500',day(d)*1000+DAY*1000-1,'50000',9,'1','1','0']);
const range=(a,b)=>Array.from({length:b-a+1},(_,i)=>a+i);
async function ohlc(ticker,{bank,answers}){
 const calls=[],original=global.fetch,savedCaches=global.caches,savedNow=Date.now;Date.now=()=>NOW;global.caches={default:{match:async()=>null,put:async()=>{}}};
 global.fetch=async(input,init)=>{const u=new URL(typeof input==='string'?input:input.url);calls.push(u.href);
  if(u.hostname==='bank.example')return Response.json({warehouse_empty:false,warehouse_key:'data/warm/katlin/crypto-bars/'+ticker.split('-')[0]+'.json.gz',last_modified:'2026-11-21T03:48:00Z',bars:bank,source_span:'day',source_mult:1});
  const a=answers[u.hostname];if(a===undefined)throw new Error('unexpected host '+u.hostname);
  if(u.hostname==='api.exchange.coinbase.com'){assert.match(u.pathname,/^\/products\/[A-Z0-9]+-USD\/candles$/);assert.equal(u.searchParams.get('granularity'),'86400');assert.ok(init&&init.headers&&init.headers['User-Agent'],'Coinbase refuses a request with no User-Agent');}
  if(typeof a==='number')return Response.json({msg:'refused'},{status:a});
  return Response.json(typeof a==='function'?a(u):a);};
 try{const worker=(await import(workerURL)).default,r=await worker.fetch(new Request('https://worker.example/ohlc?ticker='+ticker+'&span=day&days=12000'),{SYMDIR_URL:'https://bank.example'},{waitUntil:()=>{}});
  return {status:r.status,header:r.headers.get('X-Source'),doc:await r.json(),calls,hosts:calls.map(c=>new URL(c).hostname)};}
 finally{global.fetch=original;global.caches=savedCaches;Date.now=savedNow;}
}
const closes=doc=>Object.fromEntries(doc.bars.map(b=>[(b.time-TODAY)/DAY,b.close]));

test('the same coin: older history joins, and today comes from the bank\'s own venue',async()=>{
 const r=await ohlc('BTC-USD',{bank:bankRows(-60,-1),answers:{'query1.finance.yahoo.com':yahoo(range(-400,0),1.004),'api.exchange.coinbase.com':coinbase(range(-299,0))}});
 assert.equal(r.status,200);assert.deepEqual(r.hosts,['bank.example','query1.finance.yahoo.com','api.exchange.coinbase.com']);
 assert.equal(r.doc.source,'warehouse+yahoo+coinbase');assert.equal(r.header,r.doc.source);assert.equal(r.doc.history_source,'yahoo');assert.equal(r.doc.forming_source,'coinbase');assert.equal(r.doc.forming_n,1);
 assert.equal(r.doc.count,401);assert.equal(r.doc.bars.length,401);const c=closes(r.doc);
 assert.equal(c[-400],px(-400)*1.004);                                   // older than the bank: the outside history
 for(let d=-60;d<=-1;d++)assert.equal(c[d],px(d));                       // banked days: the bank
 assert.equal(c[0],px(0));assert.equal(r.doc.bars.at(-1).time,TODAY);    // today: the venue's forming bar, not the outside history's
 assert.equal(r.doc.outside_refused,undefined);assert.match(r.doc.forming_policy,/replaced by the banked bar once that day is banked/);
 assert.equal(r.doc.warehouse_key,'data/warm/katlin/crypto-bars/BTC.json.gz');
});
test('another coin under the same letters is refused whole: no history, no bar for today',async()=>{
 const r=await ohlc('ARB-USD',{bank:bankRows(-60,-1),answers:{'query1.finance.yahoo.com':yahoo(range(-400,0),0.0039),'api.binance.com':451,'data-api.binance.vision':[],'api.exchange.coinbase.com':coinbase(range(-299,0))}});
 assert.equal(r.status,200);assert.deepEqual(r.hosts,['bank.example','query1.finance.yahoo.com','api.binance.com','data-api.binance.vision','api.exchange.coinbase.com']);
 assert.equal(r.doc.source,'warehouse+coinbase');assert.equal(r.doc.history_source,undefined);assert.equal(r.doc.count,61);
 const c=closes(r.doc);for(let d=-60;d<=0;d++)assert.equal(c[d],px(d));assert.equal(Object.keys(c).length,61);
 assert.equal(r.doc.outside_refused.length,1);assert.equal(r.doc.outside_refused[0].source,'yahoo');assert.equal(r.doc.outside_refused[0].shared_days,30);assert.ok(Math.abs(r.doc.outside_refused[0].median_distance-0.9961)<1e-9);
});
test('before the rule the other coin\'s history and its bar for today were on the tape',async()=>{
 const before=(await import(pathToFileURL(path.join(__dirname,'fixtures/worker-crypto-source/predecessor/index.js')))).default;
 const calls=[],original=global.fetch,savedCaches=global.caches,savedNow=Date.now;Date.now=()=>NOW;global.caches={default:{match:async()=>null,put:async()=>{}}};
 global.fetch=async input=>{const u=new URL(typeof input==='string'?input:input.url);calls.push(u.hostname);if(u.hostname==='bank.example')return Response.json({warehouse_empty:false,warehouse_key:'data/warm/katlin/crypto-bars/ARB.json.gz',bars:bankRows(-60,-1),source_span:'day',source_mult:1});return Response.json(yahoo(range(-400,0),0.0039));};
 try{const r=await before.fetch(new Request('https://worker.example/ohlc?ticker=ARB-USD&span=day&days=12000'),{SYMDIR_URL:'https://bank.example'},{waitUntil:()=>{}}),doc=await r.json(),c=closes(doc);
  assert.equal(doc.bars.length,401);assert.equal(c[0],px(0)*0.0039);assert.equal(c[-1],px(-1));assert.ok(c[0]/c[-1]<0.005,'a 99% drop on the last bar');assert.equal(c[-400],px(-400)*0.0039);}
 finally{global.fetch=original;global.caches=savedCaches;Date.now=savedNow;}
});
test('a venue that does not list the coin, or lists another coin, hands over to the next',async()=>{
 let r=await ohlc('SUI-USD',{bank:bankRows(-60,-1),answers:{'query1.finance.yahoo.com':{chart:{result:[]}},'api.binance.com':451,'data-api.binance.vision':u=>u.searchParams.get('limit')==='45'?binance(range(-44,0),1.001):binance(range(-200,0),1.001),'api.exchange.coinbase.com':404}});
 assert.equal(r.doc.source,'warehouse+binance');assert.equal(r.doc.history_source,'binance');assert.equal(r.doc.forming_source,'binance');assert.equal(r.doc.forming_n,1);assert.equal(r.doc.count,201);
 assert.equal(closes(r.doc)[0],px(0)*1.001);assert.equal(closes(r.doc)[-1],px(-1));assert.equal(r.doc.outside_refused,undefined);
 assert.equal(r.calls.filter(c=>c.includes('api.binance.com')).length,1);assert.ok(r.calls.filter(c=>c.includes('data-api.binance.vision')).length>=2);       // refused once, then the public host for history and for today
 r=await ohlc('TON-USD',{bank:bankRows(-60,-1),answers:{'query1.finance.yahoo.com':yahoo(range(-90,0),0.002),'api.binance.com':binance(range(-200,-144)),'api.exchange.coinbase.com':coinbase(range(-299,0),0.5),'data-api.binance.vision':binance(range(-44,0),1.002)}});
 assert.equal(r.doc.source,'warehouse+binance');assert.equal(r.doc.forming_source,'binance');assert.equal(closes(r.doc)[0],px(0)*1.002);
 assert.deepEqual(r.doc.outside_refused.map(x=>x.source),['yahoo','binance','coinbase']);     // the old Binance rows share no day with the bank: cannot tell is a no
 r=await ohlc('KAS-USD',{bank:bankRows(-60,-1),answers:{'query1.finance.yahoo.com':yahoo(range(-90,0),0.002),'api.binance.com':451,'data-api.binance.vision':[],'api.exchange.coinbase.com':404}});
 assert.equal(r.doc.source,'warehouse');assert.equal(r.doc.count,60);assert.equal(r.doc.bars.at(-1).time,day(-1));assert.equal(r.doc.forming_source,undefined);                       // no same-coin venue: the tape ends at the bank, honestly
});
test('a weekend with no banking run is filled day by day, and nothing is invented past today',async()=>{
 const r=await ohlc('ETH-USD',{bank:bankRows(-60,-3),answers:{'query1.finance.yahoo.com':yahoo(range(-400,0),1.003),'api.exchange.coinbase.com':coinbase(range(-299,1))}});
 assert.equal(r.doc.forming_n,3);const c=closes(r.doc);for(const d of [-2,-1,0])assert.equal(c[d],px(d));assert.equal(c[-3],px(-3));assert.equal(c[1],undefined);assert.equal(r.doc.bars.at(-1).time,TODAY);
 assert.equal(r.doc.count,401);
});
test('a bank that stopped weeks ago is carried on to today when the venue\'s candles reach back over its last day',async()=>{
 const r=await ohlc('COMP-USD',{bank:bankRows(-160,-42),answers:{'query1.finance.yahoo.com':{chart:{result:[]}},'api.binance.com':451,'data-api.binance.vision':[],'api.exchange.coinbase.com':coinbase(range(-299,0))}});
 assert.equal(r.doc.source,'warehouse+coinbase');assert.equal(r.doc.forming_n,42);assert.equal(r.doc.count,161);const c=closes(r.doc);
 for(let d=-160;d<=0;d++)assert.equal(c[d],px(d),String(d));assert.equal(r.doc.bars.at(-1).time,TODAY);        // no hole between the bank's last day (ops 6883: 2026-10-10 for COMP GALA JUP ZK ZRO) and today
 for(let i=1;i<r.doc.bars.length;i++)assert.equal(r.doc.bars[i].time-r.doc.bars[i-1].time,DAY);
});
test('a bank that is already through today, or stopped long ago, is not extended',async()=>{
 let r=await ohlc('BTC-USD',{bank:bankRows(-60,0),answers:{'query1.finance.yahoo.com':yahoo(range(-400,0),1.004)}});
 assert.deepEqual(r.hosts,['bank.example','query1.finance.yahoo.com']);assert.equal(r.doc.source,'warehouse+yahoo');assert.equal(closes(r.doc)[0],px(0));
 r=await ohlc('MATIC-USD',{bank:bankRows(-460,-403),answers:{'query1.finance.yahoo.com':{chart:{result:[]}},'api.binance.com':451,'data-api.binance.vision':[]}});
 assert.deepEqual([...new Set(r.hosts)],['bank.example','query1.finance.yahoo.com','api.binance.com','data-api.binance.vision']);assert.equal(r.doc.source,'warehouse');assert.equal(r.doc.bars.at(-1).time,day(-403));
});
test('the rule itself, bar by bar',async()=>{
 const m=await import(tapeURL),bar=(d,close)=>({time:day(d),open:close,high:close,low:close,close,value:1}),bank=range(-60,-1).map(d=>bar(d,px(d)));
 assert.deepEqual(m.SAME_COIN,{min_days:5,window_days:30,max_median_distance:0.03});
 let v=m.sameCoinDaily(range(-400,0).map(d=>bar(d,px(d)*1.004)),bank);assert.equal(v.same,true);assert.equal(v.days,30);assert.ok(Math.abs(v.median-0.004)<1e-9);
 v=m.sameCoinDaily(range(-400,0).map(d=>bar(d,px(d)*0.0039)),bank);assert.equal(v.same,false);assert.equal(v.days,30);
 assert.equal(m.sameCoinDaily(range(-400,0).map(d=>bar(d,px(d)*0.936)),bank).same,false);                 // 6.4% away (BONK on the outside feed) is not the same tape
 assert.equal(m.sameCoinDaily(range(-400,0).map(d=>bar(d,px(d)*(d===-1?0.5:1.001))),bank).same,true);     // one wild print does not move a median
 assert.deepEqual(m.sameCoinDaily(range(-4,0).map(d=>bar(d,px(d))),bank),{same:false,days:4,median:null});       // four shared days cannot show it
 assert.equal(m.sameCoinDaily(range(-5,0).map(d=>bar(d,px(d))),bank).same,true);
 assert.deepEqual(m.sameCoinDaily(range(-300,-200).map(d=>bar(d,px(d))),bank),{same:false,days:0,median:null});
 for(const junk of [[],null,undefined,[{time:'x',close:1}],[{time:day(-1),close:0}],[{time:day(-1),close:NaN}]])assert.equal(m.sameCoinDaily(junk,bank).same,false);
 assert.deepEqual(m.sameCoinDaily(range(-9,0).map(d=>bar(d,px(d)*5)),bank.slice(-4)),{same:true,days:0,median:null,unjudged:true});   // a four-day bank has nothing to judge by
 for(const [t,p] of [['BTC-USD','BTC-USD'],['btcusdt','BTC-USD'],['BTCUSD','BTC-USD'],['SOLUSDC','SOL-USD'],['W-USD','W-USD'],['',''],['BTC=F',''],['^GSPC',''],['AAPL','']])assert.equal(m.coinbaseProduct(t),p,t);
 assert.deepEqual(m.coinbaseCandlesToBars([[day(0),9,11,10,10.5,7],[day(-1),8,10,9,9.5,6],['x'],[day(-2),0,1,1,1,1],null]),[{time:day(-1),open:9,high:10,low:8,close:9.5,value:6},{time:day(0),open:10,high:11,low:9,close:10.5,value:7}]);
 assert.deepEqual(m.coinbaseCandlesToBars({message:'NotFound'}),[]);
 assert.equal(m.utcToday(NOW),TODAY);assert.equal(m.bankLastDay(bank),day(-1));assert.equal(m.bankLastDay([]),0);
 const pack={bars:range(-3,0).map(d=>bar(d,d===0?1:px(d)))},f=m.formingBars(pack,range(-10,1).map(d=>bar(d,px(d)+0.25)),day(-1),TODAY);
 assert.equal(f.added,1);assert.deepEqual(f.bars.map(b=>b.close),[px(-3),px(-2),px(-1),px(0)+0.25]);                // the bar for today is the venue's; a bar dated tomorrow is not taken
 assert.equal(m.formingBars(pack,range(-10,-1).map(d=>bar(d,px(d))),day(-1),TODAY),null);assert.equal(m.formingBars(pack,[],day(-1),TODAY),null);
});
// ---------------------------------------------------------------- hourly and minute bars
const HOUR=3600,NOWSEC=NOW/1000,hourOpen=Math.floor(NOWSEC/HOUR)*HOUR;
const klines=(count,stepSec,endOpenSec,scale=1)=>Array.from({length:count},(_,i)=>{const t=endOpenSec-(count-1-i)*stepSec,p=(200+i*.01)*scale;return [t*1000,String(p),String(p*1.001),String(p*.999),String(p),'5',t*1000+stepSec*1000-1,'1000',3,'1','1','0'];});
async function intraday(route,{bank,answers}){
 const calls=[],original=global.fetch,savedCaches=global.caches,savedNow=Date.now,puts=[];Date.now=()=>NOW;global.caches={default:{match:async()=>null,put:async(k,v)=>{puts.push(k.url);}}};
 global.fetch=async(input,init)=>{const u=new URL(typeof input==='string'?input:input.url);calls.push(u.href);
  if(u.hostname==='bank.example'){if(['minute','hour'].includes(u.searchParams.get('span'))||!bank)return Response.json({warehouse_empty:true,bars:[]});
   return Response.json({warehouse_empty:false,warehouse_key:bank.key||'data/warm/katlin/crypto-bars/BTC.json.gz',last_modified:'2026-11-21T03:48:00Z',bars:bank.rows,source_span:'day',source_mult:1});}
  const a=answers[u.hostname];if(a===undefined)throw new Error('unexpected host '+u.hostname);
  if(typeof a==='number')return Response.json({msg:'refused'},{status:a});
  return Response.json(typeof a==='function'?a(u,init):a);};
 try{const worker=(await import(workerURL)).default,r=await worker.fetch(new Request('https://worker.example'+route),{SYMDIR_URL:'https://bank.example',POLYGON_KEY:'fixture'},{waitUntil:p=>p});
  return {status:r.status,header:r.headers.get('X-Source'),cache:r.headers.get('Cache-Control'),doc:await r.json(),calls,hosts:calls.map(c=>new URL(c).hostname),puts};}
 finally{global.fetch=original;global.caches=savedCaches;Date.now=savedNow;}
}
const liveBank={rows:bankRows(-60,-1)};
test('a banked coin\'s hourly view is hourly bars from a same-coin venue, newest bar the hour now forming',async()=>{
 const r=await intraday('/ohlc?ticker=BTC-USD&span=hour&mult=1&days=60',{bank:liveBank,answers:{'data-api.binance.vision':u=>u.searchParams.get('interval')==='1d'?binance(range(-44,0),1.001):
   u.searchParams.get('endTime')?klines(440,HOUR,Math.floor(Number(u.searchParams.get('endTime'))/1000/HOUR)*HOUR):klines(1000,HOUR,hourOpen)}});
 assert.equal(r.status,200);assert.equal(r.doc.source,'binance public market data');assert.equal(r.header,r.doc.source);assert.equal(r.doc.venue_pair,'BTCUSDT');assert.equal(r.doc.venue_interval,'1h');
 assert.equal(r.doc.span,'hour');assert.equal(r.doc.mult,1);assert.equal(r.doc.count,1440);assert.equal(r.doc.bars.length,1440);        // 60 days of hours, two pages
 assert.equal(r.doc.bars.at(-1).time,hourOpen);for(let i=1;i<r.doc.bars.length;i++)assert.equal(r.doc.bars[i].time-r.doc.bars[i-1].time,HOUR);
 assert.equal(r.doc.checked_against,'data/warm/katlin/crypto-bars/BTC.json.gz');assert.match(r.doc.intraday_policy,/matched the banked closes/);assert.equal(r.cache,'public, max-age=60');
 assert.deepEqual(r.hosts,['bank.example','bank.example','data-api.binance.vision','data-api.binance.vision','data-api.binance.vision']);
 assert.ok(!r.calls.some(c=>c.includes('polygon.io')),'Polygon is not asked for what its plan does not carry');assert.equal(r.puts.length,1);assert.match(r.puts[0],/crypto-intraday-v1\/BTC-USD\/1hour\/60$/);
 assert.ok(!r.calls[2].includes('endTime'),'the newest page has a stable address');
});
test('a bar the venue does not publish is rebuilt from the one below it',async()=>{
 const r=await intraday('/ohlc?ticker=ETH-USD&span=minute&mult=45&days=1',{bank:liveBank,answers:{'data-api.binance.vision':u=>u.searchParams.get('interval')==='1d'?binance(range(-44,0)):klines(96,900,Math.floor(NOWSEC/900)*900)}});
 assert.equal(r.doc.venue_interval,'15m');assert.equal(r.doc.mult,45);assert.equal(r.cache,'public, max-age=30');
 for(let i=1;i<r.doc.bars.length;i++)assert.equal(r.doc.bars[i].time-r.doc.bars[i-1].time,2700);for(const b of r.doc.bars)assert.equal(b.time%2700,0);
 assert.ok(r.doc.bars.length>=32&&r.doc.bars.length<=33);assert.equal(r.doc.bars[1].volume,15);assert.deepEqual(Object.keys(r.doc.bars[1]),['time','open','high','low','close','volume']);                       // three 15-minute bars of volume 5
});
test('another coin on the first venue is passed over; the next same-coin venue answers; with none, nothing is drawn from a venue',async()=>{
 const cb=(u,init)=>{assert.ok(init.headers['User-Agent']);if(u.searchParams.get('granularity')==='86400')return coinbase(range(-299,0));
  const end=Date.parse(u.searchParams.get('end'))/1000,start=Date.parse(u.searchParams.get('start'))/1000;assert.equal(end-start,300*HOUR);return Array.from({length:300},(_,i)=>[end-HOUR*(i+1),199,201,200,200,4]);};
 let r=await intraday('/ohlc?ticker=TON-USD&span=hour&mult=1&days=20',{bank:{rows:bankRows(-60,-1),key:'data/warm/katlin/crypto-bars/TON.json.gz'},answers:{'data-api.binance.vision':binance(range(-44,0),0.002),'api.exchange.coinbase.com':cb}});
 assert.equal(r.doc.source,'coinbase public market data');assert.equal(r.doc.venue_pair,'TON-USD');assert.equal(r.doc.count,600);                 // 20 days of hours = 480 -> two pages of 300
 for(let i=1;i<r.doc.bars.length;i++)assert.equal(r.doc.bars[i].time-r.doc.bars[i-1].time,HOUR);
 assert.ok(!r.calls.some(c=>c.includes('interval=1h')),'the other coin\'s hourly klines are never read');
 r=await intraday('/ohlc?ticker=KAS-USD&span=hour&mult=1&days=20',{bank:liveBank,answers:{'data-api.binance.vision':binance(range(-44,0),0.002),'api.exchange.coinbase.com':404,'api.polygon.io':{results:[]}}});
 assert.equal(r.doc.source,'polygon (warehouse empty)');assert.equal(r.doc.count,0);                                                             // falls through exactly as before
});
test('only banked coins, only intraday, only the dollar tape: every other request is untouched',async()=>{
 const poly={results:[{t:hourOpen*1000,o:1,h:2,l:.5,c:1.5,v:9}]};
 let r=await intraday('/ohlc?ticker=FOO-USD&span=hour&mult=1&days=5',{bank:null,answers:{'api.polygon.io':poly}});
 assert.deepEqual(r.hosts,['bank.example','bank.example','api.polygon.io']);assert.equal(r.doc.source,'polygon (warehouse empty)');assert.equal(r.doc.count,1);     // a coin with no bank has nothing to be checked against
 r=await intraday('/ohlc?ticker=AAPL&span=hour&mult=1&days=5',{bank:null,answers:{'api.polygon.io':poly}});
 assert.deepEqual(r.hosts,['bank.example','api.polygon.io']);assert.equal(r.doc.count,1);                                                                             // a stock's intraday is Polygon's, as before
 r=await intraday('/ohlc?ticker=BTCUSDT&span=hour&mult=1&days=5',{bank:null,answers:{'api.polygon.io':poly}});assert.deepEqual(r.hosts,['bank.example','api.polygon.io']);
 r=await intraday('/ohlc?ticker=MSTR-USD&span=hour&mult=1&days=5',{bank:{rows:bankRows(-60,-1),key:'data/warm/tv-bars/universe/US__MSTR.json.gz'},answers:{'api.polygon.io':poly}});
 assert.deepEqual(r.hosts,['bank.example','bank.example','api.polygon.io']);                                                                                          // a bank that is not the crypto bank is not a coin
 const m=await import(tapeURL);
 assert.deepEqual(m.intradayPlan('minute',45),{span:'minute',mult:45,seconds:2700,binance:{interval:'15m',mult:15,seconds:900},coinbase:{granularity:900,mult:15,seconds:900}});
 assert.equal(m.intradayPlan('hour',4).binance.interval,'4h');assert.equal(m.intradayPlan('hour',4).coinbase.granularity,3600);assert.equal(m.intradayPlan('hour',3).binance.interval,'1h');assert.equal(m.intradayPlan('minute',1).binance.interval,'1m');
 for(const bad of [['day',1],['week',1],['hour',0],['hour',1.5],['minute','x']])assert.equal(m.intradayPlan(...bad),null);
 assert.equal(m.intradayPages(60,3600,1000,8),2);assert.equal(m.intradayPages(730,3600,1000,8),8);assert.equal(m.intradayPages(0,3600,1000,8),1);assert.equal(m.intradayPages(7,60,1000,8),8);assert.equal(m.intradayPages(20,3600,300,6),2);
});
test('the worker change is four ledgered hunks and both preservation chains know it',()=>{
 const hash=s=>crypto.createHash('sha256').update(s).digest('hex'),t=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/worker-crypto-tape/transition.json'),'utf8'));
 let raw=fs.readFileSync(path.join(__dirname,'..',t.path),'utf8');assert.equal(hash(raw),t.after_sha256);assert.equal(t.replacements.length,4);
 for(const e of [...t.replacements].reverse()){assert.equal(raw.split(e.after).length,2);raw=raw.replace(e.after,()=>e.before);}assert.equal(hash(raw),t.before_sha256);
 assert.equal(require('./helpers/worker-userstore-preservation.cjs').normalize(fs.readFileSync(path.join(__dirname,'..',t.path),'utf8'),t.path).length>0,true);
 assert.match(fs.readFileSync(path.join(__dirname,'deployment/worker_userstore_preservation.py'),'utf8'),/def peel_crypto_tape\(raw, file\):[\s\S]*raw = peel_crypto_tape\(raw, file\)\n    raw = peel_crawler_gate\(raw, file\)/);
});
