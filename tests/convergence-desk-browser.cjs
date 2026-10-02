const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),{execFileSync}=require('node:child_process'),{chromium}=require('playwright');
const repo=path.resolve(__dirname,'..'),R=path.resolve(process.env.JH_STATIC_ROOT||repo),D=path.resolve(process.env.JH_BROWSER_OUTPUT||path.join(repo,'..','stage564-desk-browser'));fs.mkdirSync(D,{recursive:true});
const original=JSON.parse(execFileSync(process.env.PYTHON||'python3',['-X','utf8','tests/compound_overlay_public_fixture.py'],{cwd:repo,encoding:'utf8'}));
const attack='<img src="/invented-canary" onerror="window.__convergence_injected=true">',long='INVENTED '.repeat(1200)+'COMPLETE_END';
const pagesource=fs.readFileSync(path.join(R,'convergence-desk.html'),'utf8'),expectedNavigations=pagesource.includes('/jh-nav-drawer.js')?2:1;
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'}),cases=[];
try{for(const width of [1440,390])for(const scenario of ['normal','unavailable','malformed','canonical-empty','legacy','malicious']){
 const context=await browser.newContext({viewport:{width,height:1050},serviceWorkers:'block',acceptDownloads:true}),page=await context.newPage(),errors=[],requests=[];let data=structuredClone(original);
 if(scenario==='canonical-empty')data={compound:[],ranked:[{symbol:'OLD',desk_score:999,compound_score:999}]};
 if(scenario==='legacy')data={compound:[{symbol:'LEGACY',desk_score:999,compound_score:12,entry_quality:'FRESH',archetype:'CLEAN_TAPE'}]};
 if(scenario==='malicious'){data.compound[0].systems=[attack,long];data.compound[0].families=[attack];data.compound[0].symbol=attack;data.compound[0].desk_score_calculation.unmodeled=long;data.compound.push({...data.compound[0],symbol:'QA2'});}
 const raw=JSON.stringify(data);
 await page.addInitScript(()=>sessionStorage.setItem('jh_convergence_qa_loads',String(Number(sessionStorage.getItem('jh_convergence_qa_loads')||0)+1)));
 page.on('pageerror',e=>errors.push(e.message));if(context.routeWebSocket)await context.routeWebSocket('**',ws=>ws.close());
 await context.route('**/*',async route=>{const u=new URL(route.request().url());requests.push({path:u.pathname,search:u.search});
  const local=path.resolve(R,'.'+decodeURIComponent(u.pathname));
  if(local.startsWith(R+path.sep)&&fs.existsSync(local)&&fs.statSync(local).isFile()&&(u.pathname==='/convergence-desk.html'||/\.(js|css)$/.test(u.pathname)||u.pathname==='/nav-manifest.json'))return route.fulfill({contentType:u.pathname.endsWith('.html')?'text/html':u.pathname.endsWith('.css')?'text/css':u.pathname.endsWith('.json')?'application/json':'application/javascript',body:fs.readFileSync(local)});
  if(u.pathname.endsWith('.json')){
   if(scenario==='unavailable')return route.fulfill({status:403,body:'invented unavailable'});
   if(u.pathname==='/data/compound-signals.json')return route.fulfill({contentType:'application/json',body:scenario==='malformed'?' {"compound":[],"compound":[{}]} ':raw});
   return route.fulfill({contentType:'application/json',body:'{}'});
  }return route.fulfill({status:404,body:'invented unavailable'});
 });
 await page.goto('https://invented.justhodl.test/convergence-desk.html');
 await page.waitForFunction(n=>Number(sessionStorage.getItem('jh_convergence_qa_loads'))>=n&&document.documentElement.dataset.convergenceReady,expectedNavigations);
 assert.equal(await page.locator('html').getAttribute('data-convergence-ready'),'true');assert.equal(await page.evaluate(()=>Number(sessionStorage.getItem('jh_convergence_qa_loads'))),expectedNavigations);
 const shell=page.locator('#convergence-desk'),board=page.locator('#convergence-board');assert.deepEqual(errors,[]);
 const sizes=await page.evaluate(()=>({scroll:document.documentElement.scrollWidth,client:document.documentElement.clientWidth}));assert.ok(sizes.scroll<=sizes.client+1,JSON.stringify({width,scenario,sizes}));
 assert.ok(!requests.some(r=>r.path==='/invented-canary'));assert.equal(await shell.locator('img').count(),0);assert.equal(await page.evaluate(()=>window.__convergence_injected===true),false);
 for(const r of requests.filter(r=>['/data/compound-signals.json','/data/compound-history.json'].includes(r.path))){const q=new URLSearchParams(r.search);assert.equal(q.get('exact'),'1');assert.equal(q.get('nogen'),'1');}
 if(scenario==='normal'){
  assert.equal(await board.locator('article').count(),24);const more=page.locator('#convergence-more');await more.focus();await more.press('Enter');assert.equal(await board.locator('article').count(),30);
  assert.equal(await board.locator('article').last().locator('h3').innerText(),'QA29');assert.match(await board.locator('article').last().innerText(),/Unavailable/);
  await page.locator('#convergence-search').fill('QA2');await page.locator('#convergence-search').press('Enter');
  const zero=board.locator('article').filter({has:page.locator('h3',{hasText:/^(?:§\d+)?QA2$/})});assert.equal(await zero.count(),1);assert.equal(await zero.locator('dd').first().innerText(),'0');assert.match(await zero.innerText(),/calendar duration unavailable/);
  const calc=zero.locator('details').filter({has:page.locator('summary',{hasText:'Lifecycle calculation'})});await calc.locator('summary').focus();await calc.locator('summary').press('Enter');await calc.locator('pre').waitFor();assert.match(await calc.locator('pre').innerText(),/observation_freshness_qualified/);
  const downloadPending=page.waitForEvent('download');await page.locator('#convergence-csv').focus();await page.locator('#convergence-csv').press('Enter');const csv=await downloadPending;const csvText=fs.readFileSync(await csv.path(),'utf8');assert.match(csvText,/"QA2","0","0"/);assert.match(csvText,/"unqualified"/);
  await page.locator('#convergence-search').fill('');await page.locator('#convergence-sort').selectOption('compound_score');assert.equal(await board.locator('article').first().locator('h3').innerText(),'QA29');
  assert.equal(await page.locator('#convergence-history tbody tr').count(),3);assert.deepEqual(await page.locator('#convergence-history tbody tr td:nth-child(2)').allTextContents(),['-10','0','15']);
 }
 if(scenario==='unavailable'){assert.match(await page.locator('#convergence-status').innerText(),/0\/2 packets received/);assert.equal(await board.locator('article').count(),0);}
 if(scenario==='malformed')assert.match(await page.locator('#research-sources').innerText(),/Duplicate JSON key/);
 if(scenario==='canonical-empty'){assert.equal(await board.locator('article').count(),0);assert.ok(!(await board.innerText()).includes('OLD'));}
 if(scenario==='legacy'){assert.equal(await board.locator('dd').first().innerText(),'Unavailable');assert.equal(await board.locator('dd').nth(1).innerText(),'12');assert.ok(!(await board.innerText()).includes('CLEAN_TAPE'));}
 if(scenario==='malicious'){await page.locator('#convergence-search').fill('COMPLETE_END');const r=board.locator('article').first();const calc=r.locator('details').filter({has:page.locator('summary',{hasText:'Lifecycle calculation'})});await calc.locator('summary').focus();await calc.locator('summary').press('Enter');await calc.locator('pre').waitFor();assert.match(await calc.locator('pre').innerText(),/COMPLETE_END/);assert.equal(await shell.locator('img').count(),0);}
 if(['normal','malicious','legacy'].includes(scenario)){
  await page.evaluate(()=>window.JustHodlSections?.rerun());
  const source=page.locator('#research-sources .research-source').filter({has:page.locator('h3',{hasText:/compound-signals — received$/})});assert.equal(await source.count(),1);
  const details=source.locator('summary').filter({hasText:'Complete original JSON text'});await details.focus();await details.press('Enter');await source.locator('pre').waitFor();assert.equal(await source.locator('pre').textContent(),raw);
  const pending=page.waitForEvent('download');await source.getByRole('button',{name:'Download original bytes'}).focus();await source.getByRole('button',{name:'Download original bytes'}).press('Enter');const download=await pending;assert.equal(fs.readFileSync(await download.path(),'utf8'),raw);
 }
 assert.deepEqual(errors,[]);await page.evaluate(()=>scrollTo(0,0));const screenshot=path.join(D,width+'-'+scenario+'.png');await page.screenshot({path:screenshot});cases.push({page:'convergence-desk.html',width,scenario,screenshot,navigations:expectedNavigations,requests:[...new Set(requests.map(r=>r.path))],errors,actual_network_requests:0});await context.close();
}}finally{await browser.close();}
const sources=Object.fromEntries(['convergence-desk.html','jh-convergence-desk.js','jh-convergence-desk.css','jh-research-explain.js','jh-evidence-io.js'].map(name=>[name,crypto.createHash('sha256').update(fs.readFileSync(path.join(R,name))).digest('hex')]));
fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify({sources,cases,scope:'Whole static page; every request intercepted; actual producer output from invented in-memory inputs; zero live application or provider access.'},null,2)+'\n');console.log(JSON.stringify({passed:true,cases:cases.length,actual_network_requests:0}));})().catch(e=>{console.error(e);process.exitCode=1;});
