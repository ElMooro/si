// Actual chart/native Enter at desktop/mobile widths. Every response is invented and intercepted.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),cp=require('node:child_process'),crypto=require('node:crypto'),vm=require('node:vm'),{chromium}=require('playwright');
const ROOT=path.resolve(__dirname,'..'),OUT=path.resolve(process.argv[2]||'/tmp/watchlist-handoff-coverage-browser');fs.mkdirSync(OUT,{recursive:true});
const scripts=['jh-watchlist-store.js','jh-watchlist-quotes.js','jh-chart-tvrail.js','jh-observation-series.js','jh-observation-cache.js','jh-chart-catalog.js','jh-chart-engine.js'];
const html=fs.readFileSync(ROOT+'/chart.html','utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'').replace('</body>','<script src="/fixture-library.js"></script>'+scripts.map(s=>'<script src="/'+s+'"></script>').join('')+'</body>');
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);const src=fs.readFileSync(ROOT+'/jh-chart-tvwatch.js','utf8'),stack=[acorn.exports.parse(src,{ecmaVersion:'latest'})];let extra;
let provider;while(stack.length){const n=stack.pop();if(!n||typeof n!=='object')continue;if(n.type==='FunctionDeclaration'&&n.id.name==='extraChart')extra=src.slice(n.start,n.end);if(n.type==='FunctionDeclaration'&&n.id.name==='providerRest')provider=src.slice(n.start,n.end);for(const v of Object.values(n)){if(Array.isArray(v))stack.push(...v);else if(v&&typeof v==='object')stack.push(v);}}
const routeContext=vm.createContext({atob:s=>Buffer.from(s,'base64').toString('binary')});vm.runInContext(extra+';extraChart("NONE")',routeContext);const extras=Object.entries(routeContext.extraChart.map);assert.equal(extras.length,461);
vm.runInContext(provider+';providerRest("NONE")',routeContext);const providers=Object.entries(routeContext.providerRest.map);assert.equal(providers.length,55);
const ids=[['NASDAQ:AAPL','NASDAQ:AAPL'],['NYSE:AAPL','NYSE:AAPL'],['IEX:AAPL','IEX:AAPL'],['BINANCE:BTCUSDT','BINANCE:BTCUSDT'],['BINANCE:BTCUSDC','BINANCE:BTCUSDC'],['COINBASE:BTCUSD','BTC-USD'],['BINANCE:ETHBTC','BINANCE:ETHBTC'],['TVC:GOLD','GC=F'],['TVC:US10Y','FRED:DGS10'],['OANDA:EURUSD','EURUSD=X'],['CME_MINI:ES1!','ES=F'],['FRED:DGS10','FRED:DGS10'],['AAPL','AAPL'],['worldbank:NY.GDP.MKTP.CD:USA','worldbank:NY.GDP.MKTP.CD:USA'],['FRED:BSCICP03DEUM665S','FRED:BSCICP03DEUM665S'],['ECONOMICS:USDXY','FRED:DTWEXBGS']];
const aliases=['DGS2','DGS5','DGS10','DGS30','T10Y2Y'];
const scenarios=[...Array.from({length:Math.ceil(extras.length/24)},(_,i)=>({name:'extra-current-main-'+i,pairs:extras.slice(i*24,(i+1)*24)})),{name:'counterexamples',pairs:[...ids.filter(p=>p[0]!=='TVC:US10Y'),ids.find(p=>p[0]==='TVC:US10Y')]},{name:'non-chartable',pairs:[['DATA:example',null],['DESK:inst',null],['FRED:DGS10:EXTRA',null]]},
{name:'synthetic-conflicting-map',pairs:[['NASDAQ:AAPL','NASDAQ:AAPL'],['AAPL','AAPL'],['LSE:IEMD','IEMD.L'],['FRED:DGS10','FRED:DGS10'],['WB:USA|NY.GDP.MKTP.CD','worldbank:NY.GDP.MKTP.CD:USA']],map:{'NASDAQ:AAPL':{source:'MARKET',id:'MSFT'},'AAPL':{source:'MARKET',id:'MSFT'},'LSE:IEMD':{source:'MARKET',id:'MSFT'},'FRED:DGS10':{source:'FRED',id:'DGS2'},'WB:USA|NY.GDP.MKTP.CD':{source:'WORLDBANK',id:'USA|NY.GDP.MKTP.CD'}}},
{name:'prefix-aliases',pairs:aliases.map(s=>[s,s])},{name:'prefix-mismatch',pairs:aliases.map(s=>[s,s]),mismatch:true},
{name:'catalog-prefix',pairs:['DFF','SOFR','CPIAUCSL'].map(s=>[s,'FRED:'+s])},
{name:'unchanged-market',pairs:['BTC-USD','GC=F','EURUSD=X','ABCDEF','ESU6','USDJPY','BTC','ETH','^VIX'].map(s=>[s,s==='BTC'?'BTC-USD':s==='ETH'?'ETH-USD':s])},
{name:'held-map',pairs:[['TVC:GOLD','GC=F'],['FRED:DGS10','FRED:DGS10']],holdMap:true},
{name:'crypto-supplementary',pairs:[['BINANCE:BTCUSDT','BINANCE:BTCUSDT']],supplement:true},
{name:'crypto-fallback',pairs:[['BINANCE:BTCUSDT','BINANCE:BTCUSDT']],failPrimary:true},
{name:'late-response',pairs:[['AAPL','AAPL'],['NASDAQ:QALATE','NASDAQ:QALATE'],['FRED:DGS10','FRED:DGS10']],race:true}];
for(const mode of ['mouse','enter','arrow','advanced'])for(let i=0;i<Math.ceil(providers.length/24);i++)scenarios.push({name:'provider-rest-'+mode+'-'+i,pairs:providers.slice(i*24,(i+1)*24),mode,provider:true});
const selected=process.env.WATCHLIST_IDENTITY_SCENARIOS?scenarios.filter(s=>new RegExp(process.env.WATCHLIST_IDENTITY_SCENARIOS).test(s.name)):scenarios;assert.ok(selected.length);
const watchKeys=['jh-chart-custom-lists','jh-chart-flags','jh-chart-favs','jh-tv-watch-ui'];
const report={source_sha256:Object.fromEntries([...scripts,'jh-chart-tvwatch.js','chart.html'].map(f=>[f,crypto.createHash('sha256').update(fs.readFileSync(path.join(ROOT,f))).digest('hex')])),head:cp.execFileSync('git',['rev-parse','HEAD'],{cwd:ROOT,encoding:'utf8'}).trim(),scope:'Actual chart/native handoffs and loading/late-response lifecycle at 1440/390. All requests intercepted; invented stores/packets/maps only.',selected_scenarios:selected.map(s=>s.name),cases:[],external_network_requests:0,extra_routes:extras.length,provider_routes:providers.length};
const bars=()=>Array.from({length:25},(_,i)=>({time:Date.UTC(2026,8,i+1)/1000,open:100+i,high:102+i,low:98+i,close:101+i,volume:1000}));
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium'});try{
for(const width of [1440,390])for(const scenario of selected.filter(s=>!s.name.startsWith("extra-current-main")).concat(selected.filter(s=>s.name.startsWith("extra-current-main")))){
 const context=await browser.newContext({viewport:{width,height:1000},isMobile:width===390,hasTouch:width===390,serviceWorkers:'block'}),page=await context.newPage(),errors=[],requests=[];let heldMap=null,heldPrice=null,holdPrice=false;
 page.on('pageerror',e=>errors.push(e.message));
 await context.addInitScript(members=>{const data={'jh-chart-custom-lists':[{id:'fixture',name:'Invented identity fixture',symbols:members,custom:1}],'jh-chart-flags':{},'jh-chart-favs':[],'jh-tv-watch-ui':{active:'fixture',cols:{last:1,chg:1,chgp:1},widths:{sym:100},order:{},extra:{},hide:{}}};for(const[k,v]of Object.entries(data))localStorage.setItem(k,JSON.stringify(v));localStorage.setItem('jh-chart-watch-pin','0');window.Notification=undefined;},scenario.pairs.map(p=>p[0]));
 await context.route('**/*',async route=>{const u=new URL(route.request().url()),record={host:u.host,path:u.pathname,query:u.search};requests.push(record);const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
  if(u.host==='fixture.identity.test'&&u.pathname==='/chart.html')return send(html,'text/html');
  if(u.pathname==='/fixture-library.js')return send(fs.readFileSync(ROOT+'/tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt','utf8'),'application/javascript');
  if(u.pathname==='/jh-chart-tvwatch.js'||scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(ROOT+u.pathname,'utf8'),'application/javascript');
  if(u.pathname==='/data/symbol-map.json'){if(scenario.holdMap){heldMap=()=>send({map:{'NASDAQ:AAPL':{source:'MARKET',id:'MSFT'}}});return;}return send({map:scenario.map||{}});}
  if(u.pathname==='/data/tv-symbol-resolver.json')return send(fs.readFileSync(ROOT+'/data/tv-symbol-resolver.json','utf8'));
  if(u.pathname==='/data/tv-watchlists.json')return send({lists:[]});
  if(['/ohlc','/yf-ohlc'].includes(u.pathname)){const id=u.searchParams.get('ticker')||u.searchParams.get('symbol');record.invented_response_id=id;if(scenario.failPrimary&&u.pathname==='/ohlc'&&id==='BTCUSDT')return route.fulfill({status:500,contentType:'application/json',body:'{}'});const reply=()=>send({ticker:id,span:u.searchParams.get('span')||'day',mult:Number(u.searchParams.get('mult')||1),source:'invented market packet '+id,bars:scenario.supplement&&u.pathname==='/yf-ohlc'?Array.from({length:8},(_,i)=>({time:Date.UTC(2026,7,i+1)/1000,open:80+i,high:82+i,low:78+i,close:81+i,volume:1000})):bars()});if(holdPrice&&id==='QALATE'&&u.searchParams.get('days')!=='10'){heldPrice=reply;return;}return reply();}
  if(u.pathname==='/series'){const id=u.searchParams.get('id');record.invented_response_id=scenario.mismatch?'FRED:WRONG':id;return send({id:record.invented_response_id,provider:'invented '+id.split(':')[0],source:'invented scalar packet',unit:'invented units',obs:Array.from({length:12},(_,i)=>['2026-09-'+String(i+1).padStart(2,'0'),i+1])});}
  if(u.pathname==='/data/warehouse/catalog.json')return send({datasets:[]});if(u.pathname==='/data/symbology/master.json')return send({by_ticker:{}});if(u.pathname==='/data/engine_inventory.json')return send({engines:[]});
  if(u.pathname.startsWith('/data/')||u.pathname.startsWith('/api/')||u.pathname.startsWith('/poly/')||u.pathname.startsWith('/quote'))return send({});return route.abort('blockedbyclient');
 });
 try{
  await page.goto('https://fixture.identity.test/chart.html');await page.waitForFunction(()=>window.jhWatchlistActive&&document.querySelector('#wlist .wrow')&&typeof document.getElementById('q').onkeydown==='function');
  if(!await page.locator('#watch').evaluate(e=>e.classList.contains('is-open')))await page.locator(width===390?'#btn-watch':'#rrail [data-rail=watch]').click();await page.waitForFunction(()=>document.getElementById('watch').classList.contains('is-open'));
  await page.evaluate(()=>{fixtureSubmits=[];const q=document.getElementById('q'),native=q.onkeydown;q.onkeydown=function(e){if(e.key==='Enter')fixtureSubmits.push(q.value);return native.call(q,e);};});
  if(scenario.mode==='advanced'){await page.locator('#tv-pie').click();await page.waitForFunction(()=>document.getElementById('tvadv')?.classList.contains('on'));}
  const originals=await page.evaluate(keys=>Object.fromEntries(keys.map(k=>[k,localStorage.getItem(k)])),watchKeys);
  const saved=await page.evaluate(()=>({lists:jhWatchlistStore.read('jh-chart-custom-lists'),flags:jhWatchlistStore.read('jh-chart-flags'),favorites:jhWatchlistStore.read('jh-chart-favs'),ui:jhWatchlistStore.read('jh-tv-watch-ui')}));
  for(const [requested,target]of scenario.pairs){
   const prior=await page.evaluate(()=>jhWatchlistActive()),count=await page.evaluate(()=>fixtureSubmits.length),start=requests.length;
   const exactRow=page.locator((scenario.mode==='advanced'?'#tvadv tbody tr':'#wlist .wrow')+'[data-s="'+requested+'"]');await exactRow.scrollIntoViewIfNeeded();
   if(scenario.race&&requested==='NASDAQ:QALATE')holdPrice=true;
   if(scenario.mode==='mouse'||scenario.mode==='advanced')await exactRow.click();
   else if(scenario.mode==='enter'){await exactRow.focus();await page.keyboard.press('Enter');}
   else if(scenario.mode==='arrow'){const index=scenario.pairs.findIndex(p=>p[0]===requested),from=page.locator('#wlist .wrow[data-s="'+scenario.pairs[Math.max(0,index-1)][0]+'"]');await from.scrollIntoViewIfNeeded();await from.focus();await page.keyboard.press(index?'ArrowDown':'ArrowUp');}
   else if(scenario.name.startsWith('extra-current-main')){assert.equal(await exactRow.isVisible(),true);await exactRow.dispatchEvent('keydown',{key:'Enter',bubbles:true});}
   else if(width===390){await exactRow.focus();await page.keyboard.press('Enter');}else await exactRow.click();
   await page.waitForFunction(id=>document.querySelector('#watchlist-chart-route')?.dataset.requested===id,requested);
   if(scenario.race&&requested==='NASDAQ:QALATE'){
    await page.waitForFunction(()=>document.getElementById('watchlist-chart-route').textContent.includes('previous chart label AAPL'));
    for(let i=0;i<50&&!heldPrice;i++)await page.waitForTimeout(20);assert.ok(heldPrice,'QALATE transport held');
   }else if(target){await page.waitForFunction(({id,mismatch})=>jhWatchlistActive()===id.toUpperCase()&&window.jhChartEvidence?.symbol===id.toUpperCase()&&Array.isArray(window.lastBars)&&(mismatch?lastBars.length===0:lastBars.length>0),{id:target,mismatch:!!scenario.mismatch});}
   else {assert.equal(await page.evaluate(()=>jhWatchlistActive()),prior);assert.equal(await page.evaluate(()=>fixtureSubmits.length),count);}
   await page.waitForFunction(()=>document.querySelector('#tvcard .chart-route')?.textContent===document.getElementById('watchlist-chart-route').textContent);
   const state=await page.evaluate(({requested,count})=>({requested,submitted:fixtureSubmits.slice(count),active:jhWatchlistActive(),evidence:window.jhChartEvidence?{symbol:jhChartEvidence.symbol,source:jhChartEvidence.source,requested_id:jhChartEvidence.observations?.requested_id,chart_alias:jhChartEvidence.observations?.chart_alias,records:jhChartEvidence.observations?.records,diagnostics:jhChartEvidence.observations?.diagnostics}:null,bars:window.lastBars?.length||0,handoff:window.jhWatchlistHandoff,route:{...document.querySelector('#watchlist-chart-route').dataset},card_route_text:document.querySelector('#tvcard .chart-route').textContent,route_text:document.querySelector('#watchlist-chart-route').textContent}),{requested,count});
   assert.equal(state.card_route_text,state.route_text);assert.deepEqual(state.submitted,target?[target]:[]);assert.equal(state.route.requested,requested);assert.equal(state.route.handoff,target||'');assert.match(state.route_text,/Returned instrument, venue and currency: unverified/);assert.equal(state.handoff.packet_instrument_verified,false);assert.equal(state.handoff.packet_venue_verified,false);assert.equal(state.handoff.packet_currency_verified,false);
   if(!target)assert.match(state.route_text,/[Cc]hart unchanged/);
   if(aliases.includes(requested)){assert.equal(state.evidence.requested_id,requested);assert.equal(state.evidence.chart_alias.requested,requested);assert.equal(state.evidence.chart_alias.resolved,'FRED:'+requested);assert.equal(state.bars,scenario.mismatch?0:12);}
   if(target&&/^worldbank:/i.test(target))assert.equal(state.evidence.requested_id,target.toUpperCase());
   if(scenario.name==='synthetic-conflicting-map'&&/AAPL|IEMD/.test(requested)){assert.notEqual(state.active,'MSFT');assert.match(state.route_text,/ignored/);}
   if(scenario.supplement){assert.equal(state.bars,33);assert.match(state.evidence.source,/\+yahoo/);assert.match(state.route_text,/supplementary Yahoo data, including after a successful primary lookup/);assert.match(state.route_text,/BTC\/USDT/);assert.match(state.route_text,/BTC\/USD/);}
   if(scenario.failPrimary){assert.ok(requests.slice(start).some(r=>r.path==='/yf-ohlc'&&r.invented_response_id==='BTC-USD'));assert.match(state.route_text,/Possible fallback\/supplemental lookup: BTC-USD/);assert.match(state.route_text,/unverified/);}
   if(scenario.name.startsWith('extra-current-main')){assert.equal(state.handoff.resolved,target);assert.match(state.route_text,/equivalence unverified/);}
   if(scenario.provider){assert.equal(state.handoff.requested,requested);assert.equal(state.handoff.resolved,target);assert.equal(state.handoff.relation,'mapped-route');assert.match(state.route_text,/Possible proxy or source substitution; equivalence unverified/);assert.ok((await page.locator('#tvcard').innerText()).includes(requested));}
   if(scenario.race&&requested==='FRED:DGS10'){holdPrice=false;await heldPrice();await page.waitForTimeout(100);assert.equal(await page.evaluate(()=>jhWatchlistActive()),'FRED:DGS10');assert.equal(await page.evaluate(()=>jhChartEvidence.symbol),'FRED:DGS10');assert.equal(await page.evaluate(()=>jhWatchlistHandoff.requested),'FRED:DGS10');}
   assert.deepEqual(await page.evaluate(keys=>Object.fromEntries(keys.map(k=>[k,localStorage.getItem(k)])),watchKeys),originals);
   assert.deepEqual(await page.evaluate(()=>({lists:jhWatchlistStore.read('jh-chart-custom-lists'),flags:jhWatchlistStore.read('jh-chart-flags'),favorites:jhWatchlistStore.read('jh-chart-favs'),ui:jhWatchlistStore.read('jh-tv-watch-ui')})),saved);
   report.cases.push({width,scenario:scenario.name,...state,transport:requests.slice(start).filter(r=>['/ohlc','/yf-ohlc','/series'].includes(r.path))});
  }
  if(scenario.name==='non-chartable')assert.ok(requests.filter(r=>['/ohlc','/yf-ohlc','/series'].includes(r.path)).every(r=>!/(EXAMPLE|INST|EXTRA)/i.test(r.query)),'Browse/invalid IDs never enter market fallback');
  if(scenario.holdMap){assert.ok(heldMap);const before=await page.evaluate(()=>fixtureSubmits.slice());await heldMap();await page.waitForTimeout(100);assert.deepEqual(await page.evaluate(()=>fixtureSubmits),before);assert.equal(await page.evaluate(()=>jhWatchlistActive()),'FRED:DGS10');}
  if(scenario.name==='counterexamples'){
   await page.locator(width===390?'#btn-watch':'#rrail [data-rail=watch]').click();assert.equal(await page.locator('#watchlist-chart-route').isVisible(),true,'Handoff remains visible when watchlist closes');
   await page.screenshot({path:path.join(OUT,'persistent-status-'+width+'.png')});
   const previous=await page.evaluate(()=>jhWatchlistActive());
   for(const id of ['MSFT',previous]){
    await page.locator('#symchip').click();await page.locator('#ssin').fill(id);await page.locator('#ssres .ss-hit').filter({has:page.locator('[data-watch-id="'+id+'"]')}).first().click();
    await page.waitForFunction(id=>jhWatchlistActive()===id&&document.getElementById('watchlist-chart-route').hidden&&jhWatchlistHandoff===null,id);
   }
   assert.equal(await page.locator('#tvcard .chart-route').count(),0,'Expired watchlist attribution never revives on direct return');
   await page.locator(width===390?'#btn-watch':'#rrail [data-rail=watch]').click();
  }
  assert.deepEqual(errors,[]);console.log(JSON.stringify({width,scenario:scenario.name,passed:scenario.pairs.length,total:report.cases.length}));await page.screenshot({path:path.join(OUT,scenario.name+'-'+width+'.png')});
 }catch(error){fs.writeFileSync(path.join(OUT,'failure-'+scenario.name+'-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,body:await page.locator('body').innerText()},null,2));throw error;}finally{await context.close();}
}
fs.writeFileSync(OUT+'/browser-qa.json',JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({passed:true,cases:report.cases.length,extra_routes_per_width:selected.filter(s=>s.name.startsWith('extra-current-main')).reduce((n,s)=>n+s.pairs.length,0),provider_control_cases_per_width:selected.filter(s=>s.provider).reduce((n,s)=>n+s.pairs.length,0),external_network_requests:0,report:OUT+'/browser-qa.json'}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
