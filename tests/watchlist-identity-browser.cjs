// Real chart and native Enter/klines in isolated Chromium contexts. All responses are invented and intercepted.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),cp=require('node:child_process'),crypto=require('node:crypto'),{chromium}=require('playwright');
const ROOT=path.resolve(__dirname,'..'),OUT=path.resolve(process.argv[2]||'/tmp/watchlist-identity-guard-browser');fs.mkdirSync(OUT,{recursive:true});
const scripts=['jh-watchlist-store.js','jh-watchlist-quotes.js','jh-chart-tvrail.js','jh-observation-series.js','jh-observation-cache.js','jh-chart-catalog.js','jh-chart-engine.js'];
const html=fs.readFileSync(ROOT+'/chart.html','utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'').replace('</body>','<script src="/fixture-library.js"></script>'+scripts.map(s=>'<script src="/'+s+'"></script>').join('')+'</body>');
const ids=['NASDAQ:AAPL','NYSE:AAPL','IEX:AAPL','BINANCE:BTCUSDT','BINANCE:BTCUSDC','COINBASE:BTCUSD','BINANCE:ETHBTC','TVC:GOLD','TVC:US10Y','OANDA:EURUSD','CME_MINI:ES1!','FRED:DGS10','AAPL','worldbank:NY.GDP.MKTP.CD:USA','FRED:BSCICP03DEUM665S','ECONOMICS:USDXY'];
const aliases=['DGS2','DGS5','DGS10','DGS30','T10Y2Y'];
const scenarios=[{name:'counterexamples',members:ids,allowed:['FRED:DGS10','AAPL','worldbank:NY.GDP.MKTP.CD:USA','FRED:BSCICP03DEUM665S']},
 {name:'synthetic-map',members:['NASDAQ:AAPL','FRED:DGS10','WB:USA|NY.GDP.MKTP.CD'],allowed:['FRED:DGS10'],map:{'NASDAQ:AAPL':{source:'MARKET',id:'MSFT'},'FRED:DGS10':{source:'FRED',id:'DGS2'},'WB:USA|NY.GDP.MKTP.CD':{source:'WORLDBANK',id:'USA|NY.GDP.MKTP.CD'}}},
 {name:'prefix-aliases',members:aliases,allowed:aliases}, {name:'prefix-mismatch',members:aliases,allowed:aliases,mismatch:true},
 {name:'unchanged-market',members:['BTC-USD','GC=F','EURUSD=X'],allowed:['BTC-USD','GC=F','EURUSD=X']},
 {name:'held-map',members:['NASDAQ:AAPL','FRED:DGS10'],allowed:['FRED:DGS10'],holdMap:true}];
const watchKeys=['jh-chart-custom-lists','jh-chart-flags','jh-chart-favs','jh-tv-watch-ui'];
const report={source_sha256:Object.fromEntries([...scripts,'jh-chart-tvwatch.js','chart.html'].map(f=>[f,crypto.createHash('sha256').update(fs.readFileSync(path.join(ROOT,f))).digest('hex')])),head:cp.execFileSync('git',['rev-parse','HEAD'],{cwd:ROOT,encoding:'utf8'}).trim(),scope:'Real local chart/native handoff at 1440/390; every request intercepted; invented stores/packets/maps only.',cases:[],external_network_requests:0};
function bars(id){return Array.from({length:25},(_,i)=>({time:Date.UTC(2026,8,i+1)/1000,open:100+i,high:102+i,low:98+i,close:101+i,volume:1000}));}
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium'});try{
for(const width of [1440,390])for(const scenario of scenarios){
 const context=await browser.newContext({viewport:{width,height:1000},isMobile:width===390,hasTouch:width===390,serviceWorkers:'block'}),page=await context.newPage(),errors=[],requests=[];let heldMap=null;
 page.on('pageerror',e=>errors.push(e.message));
 await context.addInitScript(members=>{const data={'jh-chart-custom-lists':[{id:'fixture',name:'Invented identity fixture',symbols:members,custom:1}],'jh-chart-flags':{},'jh-chart-favs':[],'jh-tv-watch-ui':{active:'fixture',cols:{last:1,chg:1,chgp:1},widths:{sym:100},order:{},extra:{},hide:{}}};for(const[k,v]of Object.entries(data))localStorage.setItem(k,JSON.stringify(v));localStorage.setItem('jh-chart-watch-pin','0');window.Notification=undefined;},scenario.members);
 await context.route('**/*',async route=>{const u=new URL(route.request().url());const record={host:u.host,path:u.pathname,query:u.search};requests.push(record);const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
  if(u.host==='fixture.identity.test'&&u.pathname==='/chart.html')return send(html,'text/html');
  if(u.pathname==='/fixture-library.js')return send(fs.readFileSync(ROOT+'/tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt','utf8'),'application/javascript');
  if(u.pathname==='/jh-chart-tvwatch.js'||scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(ROOT+u.pathname,'utf8'),'application/javascript');
  if(u.pathname==='/data/symbol-map.json'){if(scenario.holdMap){heldMap=()=>send({map:{'NASDAQ:AAPL':{source:'MARKET',id:'MSFT'}}});return;}return send({map:scenario.map||{}});}
  if(u.pathname==='/data/tv-symbol-resolver.json')return send(fs.readFileSync(ROOT+'/data/tv-symbol-resolver.json','utf8'));
  if(u.pathname==='/data/tv-watchlists.json')return send({lists:[]});
  if(['/ohlc','/yf-ohlc'].includes(u.pathname)){const id=u.searchParams.get('ticker')||u.searchParams.get('symbol');record.invented_response_id=id;return send({ticker:id,span:u.searchParams.get('span')||'day',mult:Number(u.searchParams.get('mult')||1),source:'invented exact packet '+id,bars:bars(id)});}
  if(u.pathname==='/series'){const id=u.searchParams.get('id');record.invented_response_id=scenario.mismatch?'FRED:WRONG':id;return send({id:record.invented_response_id,provider:'invented '+id.split(':')[0],source:'invented exact scalar identity',unit:'invented units',obs:Array.from({length:12},(_,i)=>['2026-09-'+String(i+1).padStart(2,'0'),i+1])});}
  if(u.pathname==='/data/warehouse/catalog.json')return send({datasets:[]});if(u.pathname==='/data/symbology/master.json')return send({by_ticker:{}});if(u.pathname==='/data/engine_inventory.json')return send({engines:[]});
  if(u.pathname.startsWith('/data/')||u.pathname.startsWith('/api/')||u.pathname.startsWith('/poly/')||u.pathname.startsWith('/quote'))return send({});return route.abort('blockedbyclient');
 });
 try{
  await page.goto('https://fixture.identity.test/chart.html');await page.waitForFunction(()=>window.jhWatchlistActive&&document.querySelector('#wlist .wrow')&&typeof document.getElementById('q').onkeydown==='function');
  if(!await page.locator('#watch').evaluate(e=>e.classList.contains('is-open')))await page.locator(width===390?'#btn-watch':'#rrail [data-rail=watch]').click();await page.waitForFunction(()=>document.getElementById('watch').classList.contains('is-open'));
  await page.evaluate(()=>{fixtureSubmits=[];const q=document.getElementById('q'),native=q.onkeydown;q.onkeydown=function(e){if(e.key==='Enter')fixtureSubmits.push(q.value);return native.call(q,e);};});
  const originals=await page.evaluate(keys=>Object.fromEntries(keys.map(k=>[k,localStorage.getItem(k)])),watchKeys);
  const saved=await page.evaluate(()=>({lists:jhWatchlistStore.read('jh-chart-custom-lists'),flags:jhWatchlistStore.read('jh-chart-flags'),favorites:jhWatchlistStore.read('jh-chart-favs'),ui:jhWatchlistStore.read('jh-tv-watch-ui')}));
  for(const requested of scenario.members){
   const prior=await page.evaluate(()=>jhWatchlistActive()),count=await page.evaluate(()=>fixtureSubmits.length),start=requests.length,allowed=scenario.allowed.includes(requested);
   const row=page.locator('#wlist .wrow[data-s="'+requested+'"]');await row.scrollIntoViewIfNeeded();if(width===390){await row.focus();await page.keyboard.press('Enter');}else await row.click();
   await page.waitForFunction(id=>document.querySelector('#tvcard .chart-route')?.dataset.requested===id,requested);
   if(allowed){await page.waitForFunction(({id,mismatch})=>jhWatchlistActive()===id.toUpperCase()&&window.jhChartEvidence?.symbol===id.toUpperCase()&&Array.isArray(window.lastBars)&&(mismatch?lastBars.length===0:lastBars.length>0),{id:requested,mismatch:!!scenario.mismatch});}
   else {assert.equal(await page.evaluate(()=>jhWatchlistActive()),prior);assert.equal(await page.evaluate(()=>fixtureSubmits.length),count);}
   const state=await page.evaluate(({requested,count})=>({requested,submitted:fixtureSubmits.slice(count),active:jhWatchlistActive(),evidence:window.jhChartEvidence?{symbol:jhChartEvidence.symbol,requested_id:jhChartEvidence.observations?.requested_id,chart_alias:jhChartEvidence.observations?.chart_alias,records:jhChartEvidence.observations?.records,diagnostics:jhChartEvidence.observations?.diagnostics}:null,bars:window.lastBars?.length||0,route:{...document.querySelector('#tvcard .chart-route').dataset},route_text:document.querySelector('#tvcard .chart-route').textContent}),{requested,count});
   assert.deepEqual(state.submitted,allowed?[requested]:[]);assert.equal(state.route.requested,requested);assert.equal(state.route.target,allowed?requested:'');
   if(!allowed)assert.match(state.route_text,/chart unchanged/);
   if(aliases.includes(requested)){assert.equal(state.evidence.requested_id,requested);assert.equal(state.evidence.chart_alias.requested,requested);assert.equal(state.evidence.chart_alias.resolved,'FRED:'+requested);assert.equal(state.route.candidate,'FRED:'+requested);assert.equal(state.bars,scenario.mismatch?0:12);}
   if(allowed&&requested.toLowerCase().startsWith('worldbank:'))assert.equal(state.evidence.requested_id,requested.toUpperCase());
   if(scenario.name==='synthetic-map'&&requested==='FRED:DGS10'){assert.equal(state.route.candidate,'FRED:DGS2');assert.equal(state.evidence.requested_id,'FRED:DGS10');assert.match(state.route_text,/Unverified candidate/);}
   assert.deepEqual(await page.evaluate(keys=>Object.fromEntries(keys.map(k=>[k,localStorage.getItem(k)])),watchKeys),originals);
   assert.deepEqual(await page.evaluate(()=>({lists:jhWatchlistStore.read('jh-chart-custom-lists'),flags:jhWatchlistStore.read('jh-chart-flags'),favorites:jhWatchlistStore.read('jh-chart-favs'),ui:jhWatchlistStore.read('jh-tv-watch-ui')})),saved);
   report.cases.push({width,scenario:scenario.name,...state,transport:requests.slice(start).filter(r=>['/ohlc','/yf-ohlc','/series'].includes(r.path))});
  }
  if(scenario.holdMap){assert.ok(heldMap);const before=await page.evaluate(()=>fixtureSubmits.slice());await heldMap();await page.waitForTimeout(100);assert.deepEqual(await page.evaluate(()=>fixtureSubmits),before);assert.equal(await page.evaluate(()=>jhWatchlistActive()),'FRED:DGS10');}
  assert.deepEqual(errors,[]);await page.screenshot({path:path.join(OUT,scenario.name+'-'+width+'.png')});
 }catch(error){fs.writeFileSync(path.join(OUT,'failure-'+scenario.name+'-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,body:await page.locator('body').innerText()},null,2));throw error;}finally{await context.close();}
}
fs.writeFileSync(OUT+'/browser-qa.json',JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({passed:true,cases:report.cases.length,external_network_requests:0,report:OUT+'/browser-qa.json'}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
