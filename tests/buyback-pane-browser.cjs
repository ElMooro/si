const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),{chromium}=require('playwright');
const R=path.resolve(process.env.JH_QA_SOURCE_ROOT||path.join(__dirname,'..')),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const vendor='tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt';
const scripts=['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js','jh-chart-engine.js','jh-chart-indux.js','jh-stock-desk-research.js','jh-chart-stock-desk.js','jh-chart-vol-events.js','jh-chart-buyback.js'];
const source=fs.readFileSync(path.join(R,'jh-chart-buyback.js'),'utf8');
const html=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const served=html.replace('</body>','<script src="/fixture-library.js"></script>'+scripts.map(p=>'<script src="/'+p+'"></script>').join('')+'</body>');
const clock='2026-10-02T00:00:00Z',canary='<img src="/buyback-canary" onerror="window.__qa_injected=true">';
const bars=Array.from({length:41},(_,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i,high:102+i,low:99+i,close:101+i,volume:100}));
function row(symbol){return {symbol,measurement_contract:'buyback-accounting-measurements.v1',market_cap:10000,market_cap_asof:'2026-06-30',market_cap_unit:'USD',quality:{point_in_time_availability_verified:false},measurements:{cashflow_observations:[{date:'2026-06-30',start_date:'2026-04-01',reported_currency:'USD',eligible:true,reported_calendar_duration_aligned:true,metrics:{net_common_repurchases:{value:100,status:'reported_value',source_field:'netCommonStockIssuance',sign:'negative',unit:'USD'},gross_common_repurchases:{value:120,status:'reported_value',source_field:'commonStockRepurchased',sign:'magnitude',unit:'USD'}}}]}};}
function packet(){return {generated_at:clock,tickers:{SPY:row('SPY'),QQQ:row('QQQ')}};}
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
(async()=>{const browser=await chromium.launch({headless:true,executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'}),cases=[];try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),page=await context.newPage(),errors=[],requests=[];let response=packet(),fail=false,hold=false,held=[],heldReady=null;
  page.on('pageerror',error=>errors.push(error.message));
  await context.route('**/*',route=>{
   const u=new URL(route.request().url());requests.push(u.pathname);const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
   if(u.pathname==='/chart.html')return send(served,'text/html');if(u.pathname==='/fixture-library.js')return send(fs.readFileSync(path.join(R,vendor),'utf8'),'application/javascript');
   if(u.pathname==='/jh-chart-buyback.js')return send(source,'application/javascript');
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(u.pathname==='/data/buyback-engine.json'&&hold){held.push(route);if(heldReady)heldReady();return;}
   if(u.pathname==='/data/buyback-engine.json')return fail?route.fulfill({status:503,body:'Invented failure'}):send(response);
   if(['/ohlc','/yf-ohlc'].includes(u.pathname))return send({bars,source:'invented:buyback-browser'});
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.fulfill({status:404,body:'Invented unavailable'});
  });
  await page.goto('https://invented.justhodl.test/chart.html?s=SPY');await page.waitForFunction(()=>window.lastBars?.length===41);await page.waitForFunction(()=>window.OSC?.some(x=>x.id==='buyback'));
  await page.evaluate(()=>{window.__originalPaint=window.paint;INDS.forEach(x=>x.on=false);OSC.forEach(x=>x.on=x.id==='buyback');document.getElementById('symin').value='OLDINPUT';jhBuybackDraw();});
  try {await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane table'));} catch(error){console.log(JSON.stringify({errors,requests,state:await page.evaluate(()=>({windowOSC:Array.isArray(window.OSC),lexicalOSC:typeof OSC,active:document.querySelector('#tabs .tab.on[data-id]')?.getAttribute('data-id'),pane:document.querySelector('#jh-buyback-pane')?.textContent,items:typeof OSC!=='undefined'?OSC.filter(x=>x.id==='buyback'):[]}))}));throw error;}
  const check=async scenario=>{const state=await page.evaluate(()=>({text:document.querySelector('#jh-buyback-pane').textContent,cells:[...document.querySelectorAll('#jh-buyback-pane tbody td')].map(x=>x.textContent),images:document.querySelectorAll('#jh-buyback-pane img').length,svg:document.querySelectorAll('#jh-buyback-pane svg').length,injected:window.__qa_injected===true,paint_unchanged:window.paint===window.__originalPaint,busy:document.querySelector('#jh-buyback-pane').getAttribute('aria-busy'),client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.deepEqual(errors,[]);assert.equal(state.images,0);assert.equal(state.svg,0);assert.equal(state.injected,false);assert.ok(state.paint_unchanged);assert.ok(!requests.includes('/buyback-canary'));assert.ok(state.scroll<=state.client+1);cases.push({scenario,width,state,errors:[...errors],actual_network_requests:0});return state;};
  let state=await check('active-tab-and-matched-quarter');assert.match(state.text,/Buyback accounting · SPY/);assert.equal(state.cells[1],'100 USD');assert.equal(state.cells[2],'120 USD');assert.equal(state.cells[3],'1%');
  await page.locator('#jh-buyback-pane').scrollIntoViewIfNeeded();await page.screenshot({path:path.join(D,'aligned-'+width+'.png')});
  response=packet();response.tickers.SPY.measurements.cashflow_observations[0].metrics.net_common_repurchases.value=0;await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane tbody td:nth-child(2)')?.textContent==='0 USD');state=await check('zero-preserved');assert.equal(state.cells[3],'0%');
  response=packet();delete response.tickers.SPY.measurements.cashflow_observations[0].metrics.net_common_repurchases;await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane tbody td:nth-child(2)')?.textContent==='Unavailable');state=await check('gross-not-net');assert.equal(state.cells[2],'120 USD');assert.equal(state.cells[3],'Unavailable');
  response=packet();response.tickers.SPY.market_cap_unit='EUR';await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane tbody td:nth-child(2)')?.textContent==='100 USD');state=await check('currency-mismatch');assert.equal(state.cells[3],'Unavailable');
  response=packet();response.generated_at=canary;response.tickers.SPY.measurements.cashflow_observations[0].original={long:'x'.repeat(15000)+'COMPLETE_RECORD_TAIL',markup:canary};await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane').textContent.includes('<img'));
  await page.locator('#jh-buyback-pane summary').first().click();await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane pre')?.textContent.includes('COMPLETE_RECORD_TAIL'));assert.deepEqual(JSON.parse(await page.locator('#jh-buyback-pane pre').first().textContent()),response);await check('complete-packet-and-safe-markup');
  fail=true;await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await page.getByRole('button',{name:'Retry reported packet',exact:true}).waitFor();state=await check('failure-has-retry');assert.match(state.text,/unavailable: HTTP 503/);assert.equal(state.cells.length,0);
  fail=false;response=packet();await page.getByRole('button',{name:'Retry reported packet',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane table'));await check('retry-recovers');
  await page.locator('#tabs .tab[data-id="QQQ"]').press('Enter');await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane strong')?.textContent.endsWith('QQQ'));await check('keyboard-tab-rebinds');
  await page.evaluate(()=>dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true})));await page.locator('#tabs .tab[data-id="SPY"]').press('Enter');await page.evaluate(()=>dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane strong')?.textContent.endsWith('SPY'));await check('history-return-rebinds');
  hold=true;let waiting=new Promise(resolve=>{heldReady=resolve;});await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await waiting;
  await page.locator('#tabs .tab[data-id="QQQ"]').press('Enter');hold=false;await held.shift().fulfill({status:200,contentType:'application/json',body:JSON.stringify(packet())});
  await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane strong')?.textContent.endsWith('QQQ'));state=await check('pending-response-bound-to-current-tab');assert.equal(state.busy,'false');
  hold=true;waiting=new Promise(resolve=>{heldReady=resolve;});await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await waiting;
  await page.evaluate(()=>{OSC.find(x=>x.id==='buyback').on=false;jhBuybackDraw();});hold=false;await held.shift().fulfill({status:503,body:'Invented delayed failure'});
  await page.waitForTimeout(50);state=await check('late-failure-cannot-reopen-disabled-pane');assert.equal(state.text,'');assert.equal(await page.locator('#jh-buyback-pane').isVisible(),false);
  await page.evaluate(()=>{OSC.find(x=>x.id==='buyback').on=true;jhBuybackDraw();});await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane table'));await check('enable-recovers-after-late-failure');
  hold=true;waiting=new Promise(resolve=>{heldReady=resolve;});await page.getByRole('button',{name:'Refresh reported packet',exact:true}).click();await waiting;
  await page.getByRole('button',{name:'Retry reported packet',exact:true}).waitFor({timeout:25000});state=await check('hung-request-times-out-with-retry');assert.equal(state.busy,'false');hold=false;
  await page.getByRole('button',{name:'Retry reported packet',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#jh-buyback-pane table'));await check('timeout-retry-recovers');


  await page.screenshot({path:path.join(D,'final-'+width+'.png')});cases.at(-1).requests=requests;await context.close();
 }
}finally{await browser.close();}
fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify({source_sha256:crypto.createHash('sha256').update(source).digest('hex'),scope:'Selected complete real chart modules and pinned real chart library, invented market/accounting inputs (SPY/QQQ are fixture identities only), every request intercepted.',cases},null,2)+'\n');console.log(JSON.stringify({passed:true,cases:cases.length,actual_network_requests:0}));})().catch(error=>{console.error(error);process.exitCode=1;});
