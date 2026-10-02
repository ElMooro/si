const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),{chromium}=require('playwright');
const R=path.resolve(process.env.JH_STATIC_ROOT||path.join(__dirname,'..')),D=path.resolve(process.env.JH_BROWSER_OUTPUT||path.join(R,'..','stage561-browser'));fs.mkdirSync(D,{recursive:true});
const bad='<img src="/invented-canary" onerror="window.__research_injected=true">',long='INVENTED '.repeat(1200)+'COMPLETE_END';
const data={
 '/data/master-ranker.json':{as_of:'2026-10-02T00:00:00Z',alerts:{n_tier_3:4},top_tickers:[{ticker:'QAONLY',score:0,score_calculation:{base:0,note:long}},{ticker:'QAONLY',score:3},{ticker:bad,score:1}],unranked_tickers:[{ticker:'QABAD',score:null,reasons:[long,bad]}]},
 '/data/compound-signals.json':{generated_at:'2026-10-02T00:00:00Z',new_alerts:Array.from({length:52},(_,i)=>({symbol:'QAONLY',score:0,reason:i===51?'LAST_EVENT':bad})),compound:[{symbol:'QAONLY',compound_score:0,score:999,systems:['invented'],scores:{invented:0,missing:null},details:{invented:{text:long,attack:bad}}}]},
 '/data/best-setups.json':{top_setups:[{ticker:'QAONLY',conviction:0,verdict:'STRONG BUY'}]},
 '/data/opportunities.json':{changes:[{ticker:bad,reason:'NEW BUY is not a verified upgrade'}]},
 '/data/asymmetric-scorer.json':{top_setups:[{symbol:'QAONLY',composite_score:null}]},
 '/data/eps-revision-velocity.json':{request_records:[{ticker:'QAONLY',call:null}],all_qualifying:[]}
};
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'});const cases=[];
try{for(const name of ['why-now.html','why-cross-signal.html'])for(const width of [1440,390])for(const scenario of ['normal','unavailable','malformed','canonical-empty']){
 const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block',acceptDownloads:true}),page=await context.newPage(),errors=[],requests=[];
 await page.addInitScript(()=>sessionStorage.setItem('jh_research_qa_loads',String(Number(sessionStorage.getItem('jh_research_qa_loads')||0)+1)));
 page.on('pageerror',e=>errors.push(e.message));if(context.routeWebSocket)await context.routeWebSocket('**',ws=>ws.close());
 await context.route('**/*',async route=>{const u=new URL(route.request().url());requests.push(u.pathname);
  const local=path.resolve(R,'.'+decodeURIComponent(u.pathname));
  if(local.startsWith(R+path.sep)&&fs.existsSync(local)&&fs.statSync(local).isFile()&&(u.pathname==='/'+name||/\.(js|css)$/.test(u.pathname)||u.pathname==='/nav-manifest.json'))return route.fulfill({contentType:u.pathname.endsWith('.html')?'text/html':u.pathname.endsWith('.css')?'text/css':u.pathname.endsWith('.json')?'application/json':'application/javascript',body:fs.readFileSync(local)});
  if(u.pathname.endsWith('.json')){
   if(scenario==='unavailable')return route.fulfill({status:401,body:'invented unavailable'});
   if(scenario==='malformed' && u.pathname==='/data/master-ranker.json')return route.fulfill({contentType:'application/json',body:'{"top_tickers":[],"top_tickers":[{"ticker":"QAONLY"}]}'});
   let p=structuredClone(data[u.pathname]||{});
   if(scenario==='canonical-empty' && u.pathname==='/data/master-ranker.json')p={top_tickers:[],ranked:[{ticker:'QAONLY',score:99}],alerts:{},unranked_tickers:[]};
   if(scenario==='canonical-empty' && u.pathname==='/data/compound-signals.json')p={compound:[],ranked:[{symbol:'QAONLY',compound_score:999}],new_alerts:[]};
   return route.fulfill({contentType:'application/json',body:JSON.stringify(p)});
  }
  return route.fulfill({status:404,body:'invented unavailable'});
 });
 await page.goto('https://invented.justhodl.test/'+name+'?ticker=QAONLY');
 // The real navigation drawer clears obsolete service workers and reloads once.
 // Exercise that behavior; do not seed its guard or begin interactions on a dying document.
 await page.waitForFunction(()=>Number(sessionStorage.getItem('jh_research_qa_loads'))>=2 && document.documentElement.dataset.researchReady);
 assert.equal(await page.locator('html').getAttribute('data-research-ready'),'true');
 const navigations=await page.evaluate(()=>Number(sessionStorage.getItem('jh_research_qa_loads')));assert.equal(navigations,2);
 const main=page.locator('.research-shell'),text=await main.innerText();assert.deepEqual(errors,[]);
 assert.ok(!requests.includes('/data/momentum-breakout.json'));assert.ok(!requests.includes('/invented-canary'));
 assert.equal(await main.locator('img').count(),0);assert.equal(await page.evaluate(()=>window.__research_injected===true),false);
 const sizes=await page.evaluate(()=>({scroll:document.documentElement.scrollWidth,client:document.documentElement.clientWidth}));assert.ok(sizes.scroll<=sizes.client+1,JSON.stringify({name,width,sizes}));
 if(scenario==='unavailable'){assert.match(text,/0\/\d+ packets received/);assert.ok(!text.includes('Quiet right now'));assert.ok(!text.includes('outside the universe or no signals'));}
 if(scenario==='malformed')assert.match(text,/Duplicate JSON key/);
 if(name==='why-cross-signal.html'){
  const content=await page.locator('#content').innerText();
  if(scenario==='normal'){
   assert.match(content,/compound_score: 0/);assert.match(content,/score: 0/);assert.match(content,/master · \/top_tickers\/1/);assert.match(content,/eps · \/request_records\/0/);
   assert.ok(!content.includes('+25'));assert.ok(!content.includes('✗'));
   const audit=page.locator('#content details').filter({has:page.locator('summary',{hasText:'Complete reported calculation'})}).first();await audit.locator('summary').focus();await audit.locator('summary').press('Enter');await audit.locator('pre').waitFor();assert.match(await audit.locator('pre').innerText(),/COMPLETE_END/);await audit.locator('pre').focus();await audit.locator('pre').press('Control+End');
   await page.locator('#tickerInput').fill('QABAD');await page.locator('#tickerInput').press('Enter');assert.match(await page.locator('#content').innerText(),/Withheld from ranking/);
   await page.locator('#tickerInput').fill(bad);await page.locator('#tickerInput').press('Enter');assert.equal(await main.locator('img').count(),0);
   await page.locator('#tickerInput').fill('QAONLY');await page.locator('#tickerInput').press('Enter');
  } else if(scenario==='canonical-empty'){assert.ok(!content.includes('master · /ranked/'));assert.ok(!content.includes('compound · /ranked/'));assert.ok(!content.includes('compound_score: 999'));}
 } else if(scenario==='normal'){
  assert.match(text,/aggregate counters, not timestamped events/);assert.match(text,/Snapshot only/);
  const more=page.locator('#feed button').filter({hasText:'Show more'});await more.focus();await more.press('Enter');assert.match(await page.locator('#feed').innerText(),/LAST_EVENT/);
  const filter=page.locator('#filters button[data-f=event]');await filter.focus();await filter.press('Enter');assert.equal(await filter.getAttribute('aria-pressed'),'true');assert.ok(!(await page.locator('#feed').innerText()).includes('master · /alerts'));
  await page.locator('#filters button[data-f=all]').focus();await page.locator('#filters button[data-f=all]').press('Enter');
 }
 if(scenario==='normal'){
  // The site may prepend section badges asynchronously. Exercise the actual
  // numbering pass without treating that decoration as the source identity.
  await page.evaluate(()=>window.JustHodlSections?.rerun());
  const source=page.locator('#research-sources .research-source').filter({has:page.locator('h3',{hasText:/master — received$/})});
  assert.equal(await source.count(),1);
  await source.locator('summary').filter({hasText:'Complete original JSON text'}).focus();await source.locator('summary').filter({hasText:'Complete original JSON text'}).press('Enter');await source.locator('pre').waitFor();assert.equal(await source.locator('pre').textContent(),JSON.stringify(data['/data/master-ranker.json']));
  const pending=page.waitForEvent('download');await source.getByRole('button',{name:'Download original bytes'}).focus();await source.getByRole('button',{name:'Download original bytes'}).press('Enter');const download=await pending;assert.equal(fs.readFileSync(await download.path(),'utf8'),JSON.stringify(data['/data/master-ranker.json']));
 }
 assert.deepEqual(errors,[]);await page.evaluate(()=>scrollTo(0,0));const screenshot=path.join(D,name+'-'+width+'-'+scenario+'.png');await page.screenshot({path:screenshot});cases.push({page:name,width,scenario,screenshot,navigations,requests:[...new Set(requests)],errors,actual_network_requests:0});await context.close();
}}finally{await browser.close();}
const sources=Object.fromEntries(['why-now.html','why-cross-signal.html','jh-research-explain.js','jh-research-explain.css','jh-evidence-io.js'].map(name=>[name,crypto.createHash('sha256').update(fs.readFileSync(path.join(R,name))).digest('hex')]));
fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify({sources,cases,scope:'Whole static pages; all requests intercepted, every application response invented; no live application execution or reads.'},null,2)+'\n');console.log(JSON.stringify({passed:true,cases:cases.length,actual_network_requests:0}));})().catch(e=>{console.error(e);process.exitCode=1;});
