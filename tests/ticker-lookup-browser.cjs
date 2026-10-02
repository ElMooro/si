const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),{chromium}=require('playwright');
const R=path.resolve(process.env.JH_QA_SOURCE_ROOT||path.join(__dirname,'..')),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const html=fs.readFileSync(path.join(R,'engines-data.html'),'utf8'),hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const clock='2026-10-02T00:00:00Z',canary='<img src="/lookup-canary" onerror="window.__lookup_injected=true">';
const fixtures={
 '/data/feed-heartbeat.json':{generated_at:clock,storage_monitor_status:'HEALTHY',n_feeds:7,alerts:[]},
 '/data/master-ranker.json':{generated_at:clock,top_tickers:[]},
 '/data/best-ideas.json':{generated_at:clock,n_total:0,stack:[]},
 '/data/flow-confluence.json':{generated_at:clock,ticker_map:{}},
 '/data/conviction.json':{generated_at:clock,n_actionable:0,setups:[]},
 '/data/options-confluence.json':{generated_at:clock,counts:{names:0},multi_engine_confluence:[]}
};
const packet=(symbol='QAONLY')=>({generated_at:clock,tickers:{[symbol]:{coverage_count:3,domains:{zero:{as_of:clock,data:0},boolean:{as_of:clock,data:false},[canary]:{as_of:clock,data:'invented '.repeat(2000)+'RECORD_TAIL'}}}}});
fs.writeFileSync(path.join(D,'invented-inputs.json'),JSON.stringify({fixtures,lookup:packet()},null,2)+'\n');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'}),cases=[];
 try {for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),page=await context.newPage(),errors=[],requests=[],held=[];let hold=false,response=packet();
  page.on('pageerror',error=>errors.push(error.message));
  await context.route('**/*',async route=>{
   const u=new URL(route.request().url()),name=u.pathname;requests.push(name);
   if(name==='/engines-data.html')return route.fulfill({contentType:'text/html',body:html});
   if(name==='/data/ticker-360.json'){
    if(hold){held.push(route);return;}
    return route.fulfill({contentType:'application/json',body:JSON.stringify(response)});
   }
   if(Object.hasOwn(fixtures,name))return route.fulfill({contentType:'application/json',body:JSON.stringify(fixtures[name])});
   const local=path.resolve(R,'.'+name),isStatic=/\.(js|css|woff2)$/.test(name)||['/nav-manifest.json','/engine-manifest.json','/config/page-data-contracts.json'].includes(name);
   if(isStatic&&local.startsWith(R+path.sep)&&fs.existsSync(local))return route.fulfill({contentType:name.endsWith('.css')?'text/css':name.endsWith('.woff2')?'font/woff2':name.endsWith('.json')?'application/json':'application/javascript',body:fs.readFileSync(local)});
   return route.fulfill({status:404,body:'Invented unavailable'});
  });
  const check=async scenario=>{
   const state=await page.evaluate(()=>({status:document.querySelector('#ticker-status').textContent,result:document.querySelector('#ticker-result').textContent,busy:document.querySelector('#ticker-result').getAttribute('aria-busy'),images:document.querySelectorAll('#ticker-result img,#ticker-status img').length,injected:window.__lookup_injected===true,client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));
   assert.equal(state.injected,false);assert.equal(state.images,0);assert.ok(!requests.includes('/lookup-canary'));assert.ok(state.scroll<=state.client+1,JSON.stringify(state));assert.deepEqual(errors,[]);cases.push({scenario,width,state,errors:[...errors],actual_network_requests:0});return state;
  };
  const submit=async symbol=>{await page.getByLabel('Ticker symbol',{exact:true}).fill(symbol);await page.getByRole('button',{name:'Lookup',exact:true}).click();};
  const completed=()=>page.waitForFunction(()=>document.querySelector('#ticker-status').textContent.startsWith('Lookup completed'));
  await page.goto('https://invented.justhodl.test/engines-data.html');await page.waitForFunction(()=>document.querySelector('#t360-body').className==='');
  await page.getByLabel('Ticker symbol',{exact:true}).fill('qaonly');await page.getByLabel('Ticker symbol',{exact:true}).press('Enter');await completed();
  await page.locator('#ticker-result summary').first().click();await page.waitForFunction(()=>!!document.querySelector('#ticker-result pre'));
  const received=await page.locator('#ticker-result pre').first().textContent();assert.deepEqual(JSON.parse(received),packet());assert.ok(received.includes('RECORD_TAIL'));
  await page.locator('#ticker-result pre').first().focus();await page.locator('#ticker-result pre').first().press('End');
  await page.waitForFunction(()=>{const el=document.querySelector('#ticker-result pre');return document.activeElement===el&&el.scrollTop>0;});
  assert.ok(await page.locator('#ticker-result pre').first().evaluate(el=>document.activeElement===el&&el.scrollTop>0));
  await check('keyboard-complete-record-and-safe-markup');await page.screenshot({path:path.join(D,'complete-'+width+'.png')});
  await page.getByLabel('Ticker symbol',{exact:true}).fill('EDITED');assert.equal(await page.locator('#ticker-result').textContent(),'');await check('edit-clears-unrelated-record');
  response={generated_at:clock,tickers:{QAONLY:{coverage_count:'3',domains:{zero:{data:0},invalid:false}}}};await submit('QAONLY');await completed();assert.match((await check('typed-coverage-and-invalid-domain')).result,/Coverage count is missing, invalid or inconsistent/);
  response={tickers:[]};await submit('QAONLY');await completed();assert.match((await check('invalid-inventory-is-not-empty')).result,/invalid ticker inventory/);
  hold=true;await submit('FIRST');await page.waitForFunction(()=>document.querySelector('#ticker-status').textContent.includes('Loading FIRST'));
  while(held.length<1)await new Promise(r=>setTimeout(r,10));await submit('SECOND');while(held.length<2)await new Promise(r=>setTimeout(r,10));
  await held[1].fulfill({contentType:'application/json',body:JSON.stringify(packet('SECOND'))});await completed();await held[0].fulfill({contentType:'application/json',body:JSON.stringify(packet('FIRST'))});await page.evaluate(()=>new Promise(r=>setTimeout(r,50)));
  const race=await check('late-success-selection-binding');assert.match(race.result,/SECOND/);assert.ok(!race.result.includes('FIRST'));assert.equal(race.busy,'false');
  await submit('OLDERROR');while(held.length<3)await new Promise(r=>setTimeout(r,10));await submit('NEWEST');while(held.length<4)await new Promise(r=>setTimeout(r,10));
  await held[2].fulfill({status:500,body:'invented failure'});await page.evaluate(()=>new Promise(r=>setTimeout(r,50)));assert.match((await check('late-error-keeps-new-loading-state')).status,/Loading NEWEST/);
  await held[3].fulfill({contentType:'application/json',body:JSON.stringify(packet('NEWEST'))});await completed();await check('newest-completes-after-old-error');
  await page.screenshot({path:path.join(D,'final-'+width+'.png')});cases.at(-1).requests=[...requests];await context.close();
 }}finally{await browser.close();}
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify({source_sha256:hash(html),scope:'Entire source or verified served page; every request intercepted; all application responses invented.',cases},null,2)+'\n');console.log(JSON.stringify({passed:true,cases:cases.length,actual_network_requests:0}));
})().catch(error=>{console.error(error);process.exitCode=1;});
