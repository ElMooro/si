// Offline Chromium acceptance. All remote resources/data are intercepted; synthetic content only.
const {chromium}=require('playwright');
const fs=require('node:fs'), path=require('node:path'), assert=require('node:assert/strict'),cp=require('node:child_process');
const root=path.resolve(__dirname,'..');
const current=fs.readFileSync(path.join(root,'news.html'),'utf8');
const baseline=cp.execFileSync('git',['show','8043841ee:news.html'],{cwd:root,encoding:'utf8'});
const attack='<img src=x onerror="window.syntheticExecution=true">';
const good={public_alert_schema:'alert-history-public.v1',generated_at:'2026-10-01T10:00:00Z',as_of:'2026-10-01T09:00:00Z',alerts:[{title:attack,detail:attack,severity:'HIGH',sent_at:'2026-10-01T09:00:00Z'}]};
const feeds={'insider-trades.json':{recent_buys:[{ticker:'TEST',name:'Synthetic insider',value:0,transaction_date:'2026-10-01'}]},'earnings-tracker.json':{pead_signals:[{ticker:'TEST',signal:'STRONG',eps_surprise_pct:0,price_return_1d_pct:0}]},'macro-surprise.json':{composite:0},'current.json':{divergences:[{pair:'Synthetic pair',residual_z:0}]},'correlation-surface.json':{regime_breaks:[{ticker_a:'AAA',ticker_b:'BBB',delta_30d_vs_90d:0}]},'auction-crisis.json':{composite_score:0},'whats-changed.json':{changes:[{title:'Synthetic change'}]}};
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 const evidence=[];
 for(const width of [1440,390]){
  const page=await browser.newPage({viewport:{width,height:1000},timezoneId:'America/New_York'}),requests=[],errors=[];
  let history=good,status=200;
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',async route=>{
   const req=route.request(),url=new URL(req.url());
   if(url.pathname==='/news.html')return route.fulfill({contentType:'text/html',body:current});
   if(req.resourceType()==='fetch'){
    requests.push(url.origin+url.pathname);
    return route.fulfill({status:url.pathname.endsWith('alert-history.json')?status:200,contentType:'application/json',body:JSON.stringify(url.pathname.endsWith('alert-history.json')?history:feeds[path.basename(url.pathname)]||{})});
   }
   // Isolate the changed consumer. Shared scripts/fonts are byte-unchanged and not executed here.
   return route.abort();
  });
  await page.goto('https://news.synthetic/news.html');
  await page.waitForFunction(()=>document.querySelector('#kpi-alerts').textContent==='1');
  assert.equal(requests.length,8);
  assert.match(await page.locator('#meta-updated').innerText(),/^Page retrieved \(UTC\) \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  assert.ok(requests.includes('https://news.synthetic/data/alert-history.json')); 
  assert.equal(await page.locator('#feed .event').count(),8);
  assert.equal(await page.locator('#feed img').count(),0);
  assert.equal(await page.evaluate(()=>window.syntheticExecution),undefined);
  assert.ok((await page.locator('#feed').innerText()).includes(attack));
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  await page.locator('[data-filter="ALERT"]').focus();
  await page.keyboard.press('Enter');
  assert.equal(await page.locator('[data-filter="ALERT"]').getAttribute('aria-pressed'),'true');
  assert.equal(await page.locator('#feed .event').count(),1);
  await page.screenshot({path:`/tmp/news-${width}-public.png`,fullPage:true});
  status=503;history={error:'synthetic withheld'};await page.evaluate(()=>load());
  assert.equal(await page.locator('#kpi-alerts').innerText(),'Unavailable');
  assert.ok((await page.locator('#feed').innerText()).includes('Source data unavailable'));
  assert.equal(await page.locator('#source-status').getAttribute('role'),'status');
  await page.screenshot({path:`/tmp/news-${width}-withheld.png`,fullPage:true});
  status=200;history={...good,generated_at:'2026-02-30T10:00:00Z',as_of:'2026-02-30T10:00:00Z',alerts:[{...good.alerts[0],sent_at:'2026-02-30T10:00:00Z'}]};
  await page.evaluate(()=>load());
  assert.equal(await page.locator('#kpi-alerts').innerText(),'1');
  assert.ok((await page.locator('#source-status').innerText()).includes('ALERT: Received · Source publication: Unavailable · Source observation: Unavailable'));
  assert.ok((await page.locator('#feed').innerText()).includes('Sent: time unavailable'));
  await page.screenshot({path:`/tmp/news-${width}-invalid-clock.png`,fullPage:true});
  status=200;history={...good,alerts:[]};await page.evaluate(()=>load());
  assert.equal(await page.locator('#kpi-alerts').innerText(),'0');
  assert.ok((await page.locator('#feed').innerText()).includes('No events reported'));
  assert.equal(requests.length,32);assert.deepEqual(errors,[]);
  evidence.push({width,timezoneId:'America/New_York',retrievalTimezone:'UTC',consumerRequestsPerLoad:8,cycles:4,keyboard:'passed',horizontalOverflow:false,scriptErrors:errors,hostileText:'literal',states:['nonempty','503','invalid-calendar-clock','empty-recovery']});
  await page.close();
 }
 // Execute predecessor with benign synthetic data to compare actual fetch requests, without unsafe content.
 const page=await browser.newPage(),requests=[];
 await page.route('**/*',async route=>{const req=route.request(),url=new URL(req.url());if(url.pathname==='/news.html')return route.fulfill({contentType:'text/html',body:baseline});if(req.resourceType()==='fetch'){requests.push(url.origin+url.pathname);return route.fulfill({contentType:'application/json',body:JSON.stringify(url.pathname.endsWith('alert-history.json')?{alerts:[]}:feeds[path.basename(url.pathname)]||{})});}return route.abort();});
 await page.goto('https://news.synthetic/news.html');await page.waitForFunction(()=>document.querySelector('#kpi-alerts').textContent==='0');assert.equal(requests.length,8);
 console.log(JSON.stringify({baselineRequests:requests,acceptance:evidence,scope:'Actual inline consumer in Chromium; shared scripts/fonts intercepted, zero live/provider reads'},null,2));
 await browser.close();
})().catch(e=>{console.error(e);process.exit(1);});
