const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),{chromium}=require('playwright');
const R=path.resolve(process.env.JH_STATIC_ROOT||path.join(__dirname,'..')),D=path.resolve(process.env.JH_BROWSER_OUTPUT||path.join(R,'..','stage560-browser'));fs.mkdirSync(D,{recursive:true});
const bad='<img src="/invented-canary" onerror="window.__ranker_injected=true">';
const packet={as_of:'2026-10-02T00:00:00Z',schema_version:'fixture',regime_context:{},top_macro:[],alerts:{},feed_health:{},top_tickers:[{ticker:'QAONLY',score:0,n_systems:1,systems:['invented'],contributions:[],rationale:'Invented zero',score_calculation:{base:{score:0},adjustments:[]}}],numeric_quality:{contract:'ranker-numeric.v1',status:'partial',input_tickers:3,rankable_tickers:1,unranked_tickers:2},calibration_quality:{available:true},unranked_tickers:[{ticker:'QABAD',score:null,reasons:[{reason:'finite_number_required'}]},{ticker:bad,score:null,reasons:[{reason:'fixture '.repeat(600)+'END_OF_COMPLETE_RECORD'}]}]};
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'});const cases=[];
try{for(const name of ['master-rank.html','engines-data.html'])for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),page=await context.newPage(),errors=[],requests=[];let active=structuredClone(packet);
 page.on('pageerror',e=>errors.push(e.message));if(context.routeWebSocket)await context.routeWebSocket('**',ws=>ws.close());
 await context.route('**/*',async route=>{const u=new URL(route.request().url());requests.push(u.pathname);
  const local=path.resolve(R,'.'+decodeURIComponent(u.pathname));
  if(local.startsWith(R+path.sep)&&fs.existsSync(local)&&fs.statSync(local).isFile()&&(u.pathname==='/'+name||/\.(js|css)$/.test(u.pathname)||u.pathname==='/nav-manifest.json'))return route.fulfill({contentType:u.pathname.endsWith('.html')?'text/html':u.pathname.endsWith('.css')?'text/css':u.pathname.endsWith('.json')?'application/json':'application/javascript',body:fs.readFileSync(local)});
  if(u.pathname.endsWith('/master-ranker.json'))return route.fulfill({contentType:'application/json',body:JSON.stringify(active)});
  const fixtures={'/data/best-ideas.json':{stack:[]},'/data/flow-confluence.json':{ticker_map:{}},'/data/conviction.json':{setups:[]},'/data/options-confluence.json':{multi_engine_confluence:[]},'/data/ticker-360.json':{tickers:{}}};
  if(u.pathname.endsWith('.json'))return route.fulfill({contentType:'application/json',body:JSON.stringify(fixtures[u.pathname]||{})});
  return route.fulfill({status:404,body:'invented unavailable'});
 });
 await page.goto('https://invented.justhodl.test/'+name);const selector=name==='master-rank.html'?'#ranker-numeric':'#mr-numeric';await page.locator(selector+' summary').waitFor();
 assert.match(await page.locator(selector).innerText(),/1 of 3.*2 withheld/);await page.locator(selector+' summary').click();await page.locator(selector+' pre').focus();await page.locator(selector+' pre').press('Control+End');
 const state=await page.locator(selector).evaluate(el=>({text:el.textContent,injected:window.__ranker_injected===true,images:el.querySelectorAll('img').length,client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth,focused:document.activeElement===el.querySelector('pre'),audit:el.querySelector('pre').textContent}));
 assert.equal(state.injected,false);assert.equal(state.images,0);assert.ok(state.focused);assert.ok(state.audit.includes('END_OF_COMPLETE_RECORD'));assert.ok(!requests.includes('/invented-canary'));assert.deepEqual(errors,[]);
 assert.ok(state.scroll<=state.client+1,`${name} ${width}: horizontal overflow ${state.scroll}`);
 const screenshot=path.join(D,name+'-'+width+'.png');await page.screenshot({path:screenshot});cases.push({page:name,width,scenario:'partial-zero-full-audit',screenshot,state:{...state,text:undefined,audit:undefined},errors:[...errors],actual_network_requests:0});
 active={...packet,top_tickers:[],numeric_quality:{...packet.numeric_quality,status:'unavailable',rankable_tickers:0,unranked_tickers:3}};await page.reload();await page.locator(selector+' summary').waitFor();assert.match(await page.locator(selector).innerText(),/0 of 3.*3 withheld/);assert.deepEqual(errors,[]);
 cases.push({page:name,width,scenario:'all-withheld',errors:[...errors],actual_network_requests:0});await context.close();
}}finally{await browser.close();}
const sources=Object.fromEntries(['master-rank.html','engines-data.html','jh-ranker-numeric.js'].map(name=>[name,crypto.createHash('sha256').update(fs.readFileSync(path.join(R,name))).digest('hex')]));fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify({sources,cases,scope:'Whole static pages; every network request intercepted and invented; no live data read.'},null,2)+'\n');console.log(JSON.stringify({passed:true,cases:cases.length,actual_network_requests:0}));})().catch(e=>{console.error(e);process.exitCode=1;});
