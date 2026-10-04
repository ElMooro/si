const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const ROOT=path.resolve(__dirname,'..'),OUT=path.resolve(process.argv[2]||'/tmp/watchlist-browser');fs.mkdirSync(OUT,{recursive:true});
const scripts=['jh-chart-tvrail.js','jh-observation-series.js','jh-observation-cache.js','jh-chart-catalog.js','jh-chart-engine.js'];
const html=fs.readFileSync(path.join(ROOT,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'').replace('</body>','<script src="/fixture-library.js"></script>'+scripts.map(s=>'<script src="/'+s+'"></script>').join('')+'</body>');
const bars=[0,1,2].map(i=>({time:Date.UTC(2026,9,i+1)/1000,open:100+i,high:104+i,low:98+i,close:101+i,value:1000,volume:1000}));
const members=['AAPL','ERR429','ERR500','MALFORMED','ONE','WAIT1','WAIT2','WAIT3','QUEUED','NASDAQ:AAPL','NYSE:AAPL','FRED:DGS10','EURUSD','TVC:GOLD','BINANCE:BTCUSDT',...Array.from({length:122},(_,i)=>'EQ'+String(i).padStart(3,'0'))];
const custom=[{id:'fixture',name:'Saved override',symbols:members,custom:1},{id:'small',name:'Small',symbols:['MSFT','NASDAQ:MSFT','FRED:SOFR'],custom:1}];
const source={same1:{name:'Collision',tickers:['NASDAQ:NVDA']},same2:{name:'Collision',tickers:['FRED:SOFR']}};
const result={scope:'Invented local fixtures, current real chart/rail/quote modules; every request intercepted; no user or provider data.',cases:[],actual_network_requests:0};
(async()=>{
const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium'});
try{for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:1000},isMobile:width===390,hasTouch:width===390,serviceWorkers:'block'}),page=await context.newPage(),requests=[],errors=[],counts={},held=[];
 page.on('pageerror',e=>errors.push(e.message));
 await context.addInitScript(({custom,source})=>{localStorage.setItem('jh-chart-custom-lists',JSON.stringify(custom));localStorage.setItem('jh-chart-flags',JSON.stringify({'NASDAQ:AAPL':'#f23645'}));localStorage.setItem('jh-chart-favs','[]');localStorage.setItem('jh_custom_watchlists',JSON.stringify(source));localStorage.setItem('jh_symbol_flags','{"AAPL":"red"}');localStorage.setItem('jh_favorites','{"AAPL":true}');localStorage.setItem('jh-chart-watch-pin','0');window.Notification=undefined;},{custom,source});
 await context.route('**/*',async route=>{
  const u=new URL(route.request().url());requests.push({host:u.host,path:u.pathname,query:u.search,method:route.request().method()});
  const send=(body,type='application/json',status=200)=>route.fulfill({status,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
  if(u.host==='fixture.watchlist.test'&&u.pathname==='/chart.html')return send(html,'text/html');
  if(u.pathname==='/fixture-library.js')return send(fs.readFileSync(path.join(ROOT,'tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt'),'utf8'),'application/javascript');
  if(scripts.includes(u.pathname.slice(1))||u.pathname==='/jh-chart-tvwatch.js')return send(fs.readFileSync(path.join(ROOT,u.pathname.slice(1)),'utf8'),'application/javascript');
  if(u.pathname==='/data/tv-symbol-resolver.json')return send(fs.readFileSync(path.join(ROOT,'data/tv-symbol-resolver.json'),'utf8'));
  if(u.pathname==='/data/tv-watchlists.json')return send({lists:[{id:'fixture',name:'Catalog original',symbols:['TSLA']},{id:'other',name:'Other',symbols:['MSFT']}]});
  if(['/ohlc','/yf-ohlc'].includes(u.pathname)){
   const ticker=u.searchParams.get('ticker')||u.searchParams.get('symbol');counts[ticker]=(counts[ticker]||0)+1;
   const packet={ticker,span:u.searchParams.get('span')||'day',mult:Number(u.searchParams.get('mult')||1),source:'invented warehouse',bars};
   if(ticker==='ERR429'&&counts[ticker]===1)return send({},'application/json',429);
   if(ticker==='ERR500'&&counts[ticker]===1)return send({},'application/json',500);
   if(ticker==='MALFORMED')return send({...packet,bars:[{...bars[0],close:null}]});
   if(ticker==='ONE')return send({...packet,bars:bars.slice(0,1)});
   if(ticker==='WAIT1'){held.push(()=>send(packet));return;}
   return send(packet);
  }
  if(u.pathname==='/data/warehouse/catalog.json')return send({datasets:[]});
  if(u.pathname==='/data/symbology/master.json')return send({by_ticker:{}});
  if(u.pathname==='/data/engine_inventory.json')return send({engines:[]});
  if(u.pathname.startsWith('/data/')||u.pathname.startsWith('/api/'))return send({});
  return route.abort('blockedbyclient');
 });
 try{
 await page.goto('https://fixture.watchlist.test/chart.html');
 await page.waitForFunction(()=>window.jhWatchlistQuote&&window.jhWatchlistContext&&document.querySelector('#list option[value=fixture]'));
 if(width===390){await page.locator('#btn-watch').click();}else{await page.locator('#rrail [data-rail=watch]').click();}
 await page.waitForFunction(()=>document.getElementById('watch').classList.contains('is-open'));
 await page.evaluate(()=>{const s=document.getElementById('list');s.value='fixture';s.dispatchEvent(new Event('change',{bubbles:true}));});
 await page.waitForFunction(()=>document.querySelectorAll('#wlist .wrow.tv').length>120);
 assert.equal(await page.locator('#wlist .wrow').count(),members.length);
 assert.equal(await page.locator('#list option[value=fixture]').count(),1);
 await page.waitForFunction(()=>document.querySelector('#wlist [data-s=AAPL] .qe').textContent.includes('invented warehouse'));
 const status=await page.locator('#wlist [data-s=AAPL]').innerText();assert.ok(status.includes('2026-10-03 vs 2026-10-02'));assert.ok(status.includes('completion unverified'));
 await page.waitForFunction(()=>document.querySelector('#wlist [data-s=ERR429] .qe').textContent.includes('HTTP 429'));
 assert.ok((await page.locator('#wlist [data-s=ERR500]').innerText()).includes('HTTP 500'));
 assert.ok((await page.locator('#wlist [data-s=MALFORMED]').innerText()).includes('invalid aggregate close'));
 await page.locator('#wlist [data-s=ONE]').scrollIntoViewIfNeeded();
 await page.waitForFunction(()=>document.querySelector('#wlist [data-s=ONE] .px').textContent!=='—');
 assert.equal(await page.locator('#wlist [data-s=ONE] .chg').first().innerText(),'—');
 await page.locator('#wlist [data-s="NASDAQ:AAPL"]').scrollIntoViewIfNeeded();
 assert.ok((await page.locator('#wlist [data-s="NASDAQ:AAPL"]').innerText()).includes('endpoint cannot verify'));
 assert.equal(await page.locator('#wlist [data-s="NASDAQ:AAPL"] .wsym').innerText(),"AAPL");
 assert.ok((await page.locator('#wlist [data-s="NASDAQ:AAPL"] .qe').innerText()).startsWith("NASDAQ:AAPL · "));
 await page.locator('#wlist [data-s="NASDAQ:AAPL"]').waitFor({state:"visible"});
 const geometry=await page.evaluate(()=>{const row=document.querySelector('#wlist [data-s="NASDAQ:AAPL"]');const assertConnected=!!row&&row.isConnected;const rect=e=>{const r=e.getBoundingClientRect();return {left:r.left,right:r.right,width:r.width};};return {connected:assertConnected,grid:getComputedStyle(row).gridTemplateColumns.split(' ').length,symbol:rect(row.querySelector('.wsym')),close:rect(row.querySelector('.px')),delta:rect(row.querySelectorAll('.chg')[0]),percent:rect(row.querySelectorAll('.chg')[1]),headers:Array.from(document.querySelectorAll('#cols [data-s]'),rect)};});
 const grid=geometry.grid;assert.equal(geometry.connected,true);assert.equal(grid,7);
 fs.writeFileSync(path.join(OUT,"geometry-"+width+".json"),JSON.stringify(geometry,null,2));assert.ok(geometry.symbol.width>=55);for(const [cell,i] of [[geometry.symbol,0],[geometry.close,1],[geometry.delta,2],[geometry.percent,3]])assert.ok(Math.abs(cell.left-geometry.headers[i].left)<=2,'Row/header columns misaligned');
 await page.screenshot({path:path.join(OUT,'quotes-'+width+'.png')});
 // Native context actions and raw qualified favorite persist only synthetic members.
 await page.locator('#wlist [data-s="NASDAQ:AAPL"]').click({button:'right'});
 for(const a of ['cmp','note','fin','fav','flag','al'])assert.equal(await page.locator('#ctx [data-a='+a+']').count(),1);
 await page.locator('#ctx [data-a=fav]').click();
 assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('jh-chart-favs'))),['NASDAQ:AAPL']);
 // Qualified row click opens exact namespace; arrows are confined to focused rows.
 await page.locator('#wlist [data-s="NASDAQ:AAPL"]').click();
 await page.waitForFunction(()=>jhWatchlistActive()==='NASDAQ:AAPL');
 await page.locator('#wlist [data-s="NASDAQ:AAPL"]').focus();await page.keyboard.press('ArrowDown');
 await page.waitForFunction(()=>jhWatchlistActive()==='NYSE:AAPL');
 const before=await page.locator('#symin').inputValue();await page.locator('#chart').evaluate(e=>{e.tabIndex=0;e.focus();});await page.keyboard.press('ArrowDown');assert.equal(await page.locator('#symin').inputValue(),before);
 await page.locator('#wlist [data-s="NYSE:AAPL"]').focus();await page.keyboard.press('Space');assert.ok(!(await page.locator('#replay').getAttribute('class')||'').includes('on'));
 // Sorting after native rerender is keyboard accessible and never changes saved order.
 const saved=await page.evaluate(()=>localStorage.getItem('jh-chart-custom-lists'));
 await page.locator('#cols [data-s=sym]').focus();await page.keyboard.press('Enter');assert.equal(await page.locator('#cols [data-s=sym]').getAttribute('aria-sort'),'ascending');
 await page.locator('#cols [data-s=sym]').focus();await page.keyboard.press('Space');assert.equal(await page.locator('#cols [data-s=sym]').getAttribute('aria-sort'),'descending');assert.equal(await page.evaluate(()=>localStorage.getItem('jh-chart-custom-lists')),saved);
 // Full saved tail is reachable, including scrolling on touch viewport.
 await page.locator('#wlist .wrow').last().scrollIntoViewIfNeeded();assert.ok(await page.locator('#wlist').evaluate(e=>e.scrollHeight>e.clientHeight));
 // Same count replacement/reorder and removal of flag are repainted from saved state.
 await page.evaluate(()=>{const lists=JSON.parse(localStorage.getItem('jh-chart-custom-lists'));lists[1].symbols=['FRED:SOFR','NYSE:IBM','NASDAQ:IBM'];localStorage.setItem('jh-chart-custom-lists',JSON.stringify(lists));window.dispatchEvent(new StorageEvent('storage',{key:'jh-chart-custom-lists'}));const s=document.getElementById('list');s.value='small';s.dispatchEvent(new Event('change'));});
 await page.waitForFunction(()=>document.querySelector('#wlist [data-s="NYSE:IBM"]'));
 assert.equal(await page.locator('#wlist .wrow').count(),3);
 // Delayed old-list response cannot paint the new list.
 for(const release of held.splice(0))await release().catch(()=>{});
 await page.waitForTimeout(50);assert.equal(await page.locator('#wlist .wrow').count(),3);assert.equal(await page.locator('#wlist [data-s=WAIT1]').count(),0);
 // Explicit preview performs zero writes to source/destination/ledger.
 const keys=['jh_custom_watchlists','jh_symbol_flags','jh_favorites','jh-chart-custom-lists','jh-chart-flags','jh-chart-favs','jh-chart-pro-imported'];
 const snapshot=await page.evaluate(keys=>Object.fromEntries(keys.map(k=>[k,localStorage.getItem(k)])),keys);
 await page.locator('#w-import').click();assert.ok((await page.locator('#w-import-status').innerText()).includes('Import saving is paused'));
 assert.deepEqual(await page.evaluate(keys=>Object.fromEntries(keys.map(k=>[k,localStorage.getItem(k)])),keys),snapshot);
 // Editing through original paste control retains qualified identity.
 await page.locator('#paste').fill('NASDAQ:NVDA,FRED:DGS10');await page.locator('#paste').press('Enter');
 await page.waitForFunction(()=>document.querySelector('#wlist [data-s="NASDAQ:NVDA"]'));
 assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('jh-chart-custom-lists')).at(-1).symbols),['NASDAQ:NVDA','FRED:DGS10']);
 // Ordinary Add/search path must preserve supplied venue before chart routing.
 await page.locator('#addsym').click();await page.locator('#ssin').fill('NASDAQ:TSLA');
 await page.locator('#ssres .ss-hit').filter({has:page.locator('[data-more="NASDAQ:TSLA"]')}).first().click();
 await page.waitForFunction(()=>document.querySelector('#wlist [data-s="NASDAQ:TSLA"]'));
 assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('jh-chart-custom-lists')).at(-1).symbols),['NASDAQ:TSLA','NASDAQ:NVDA','FRED:DGS10']);
 // Chart-mode More > Watchlist retains raw identity while chart routing keeps its own ID.
 await page.locator('#symchip').click();await page.locator('#ssin').fill('NYSE:IBM');
 await page.locator('#ssres [data-watch-id="NYSE:IBM"]').first().click();await page.locator('#menu [data-a=add]').click();
 await page.waitForFunction(()=>document.querySelector('#wlist [data-s="NYSE:IBM"]'));
 assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('jh-chart-custom-lists')).at(-1).symbols),['NYSE:IBM','NASDAQ:TSLA','NASDAQ:NVDA','FRED:DGS10']);
 // Actual mobile close/open uses existing visible List control.
 await page.locator('#w-close').click();await page.waitForFunction(()=>document.getElementById('watch').classList.contains('is-collapsed'));
 if(width===390)await page.locator('#btn-watch').click();else await page.locator('#rrail [data-rail=watch]').click();
 await page.waitForFunction(()=>document.getElementById('watch').classList.contains('is-open'));
 const layout=await page.evaluate(()=>({scroll:document.documentElement.scrollWidth,client:document.documentElement.clientWidth,watch:document.getElementById('watch').getBoundingClientRect().toJSON(),quoteTools:document.querySelector('.wquote-tools').getBoundingClientRect().toJSON()}));assert.ok(layout.scroll<=layout.client+1);assert.ok(layout.watch.right<=width+1);assert.ok(layout.watch.left>=-1);
 assert.deepEqual(errors,[]);await page.screenshot({path:path.join(OUT,'watchlist-'+width+'.png')});
 result.cases.push({width,requests,counts,errors,layout,saved_rows:members.length,grid_columns:grid,geometry,native_menu_preserved:true,preview_zero_writes:true});
 }catch(error){fs.writeFileSync(path.join(OUT,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,counts,watch:await page.locator("#watch").evaluate(e=>({class:e.className,display:getComputedStyle(e).display,app:document.getElementById("app").className,pin:localStorage.getItem("jh-chart-watch-pin")})),body:await page.locator('body').innerText()},null,2));await page.screenshot({path:path.join(OUT,'failure-'+width+'.png')});throw error;}finally{await context.close();}
}
fs.writeFileSync(path.join(OUT,'browser-qa.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
