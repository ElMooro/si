const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),{chromium}=require('playwright');
const R=path.resolve(process.env.JH_BROWSER_SOURCE_ROOT||path.join(__dirname,'..')),F=path.resolve(__dirname,'fixtures/chart-provider-discovery'),D=path.resolve(process.env.JH_DISCOVERY_METADATA_ROOT||path.join(F,'public')),OUT=path.resolve(process.argv[2]||'/tmp/chart-provider-discovery-browser');fs.mkdirSync(OUT,{recursive:true});
const read=p=>fs.readFileSync(p),json=p=>JSON.parse(read(p)),hash=b=>crypto.createHash('sha256').update(b).digest('hex'),receipt=json(path.join(D,'receipt.json'));
for(const row of receipt.requests)assert.equal(hash(read(path.join(D,row.name+'.json'))),row.sha256);
const targets=receipt.series_ids.map((id,i)=>({id,dataset:id.split(':').slice(0,2).join(':'),i}));
const scripts=['jh-watchlist-store.js','jh-watchlist-quotes.js','jh-chart-tvrail.js','jh-observation-series.js','jh-observation-cache.js','jh-chart-cftc.js','jh-chart-catalog.js','jh-chart-engine.js','jh-chart-row-handoff.js','jh-chart-provider-browser.js'];
const html=read(path.join(R,'chart.html')).toString().replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'').replace('</body>','<script src="/fixture-library.js"></script>'+scripts.map(s=>'<script src="/'+s+'"></script>').join('')+'</body>');
function packet(target){if(process.env.JH_CENSUS_HISTORY_ROOT)return json(path.join(process.env.JH_CENSUS_HISTORY_ROOT,hash(target.id.toLowerCase())+'.json'));return {id:target.id,provider:'census',name:'Invented history fixture',unit:'Percent',freq:'M',obs:[['2025-01-01',1],['2025-02-01',null],['2025-03-01',2]],n:2,source:'Invented response; no public history claim'};}
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH}),report=[];try{for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block',acceptDownloads:true}),page=await context.newPage(),requests=[],errors=[];let wrongIdentity=false,denied=false;
 page.on('pageerror',e=>errors.push(e.message));
 await context.addInitScript(()=>{localStorage.setItem('jh-chart-custom-lists',JSON.stringify([{id:'invented-discovery',name:'Invented list',symbols:['SPY'],custom:1}]));localStorage.setItem('jh-tv-watch-ui',JSON.stringify({active:'invented-discovery'}));localStorage.setItem('jh-chart-watch-pin','0');});
 await context.route('**/*',async route=>{const u=new URL(route.request().url());requests.push(u.pathname+u.search);const send=(v,type='application/json',status=200)=>route.fulfill({status,contentType:type,body:Buffer.isBuffer(v)||typeof v==='string'?v:JSON.stringify(v)}),metadata=name=>send(read(path.join(D,name+'.json')));
  if(u.pathname==='/chart.html')return send(html,'text/html');if(u.pathname==='/fixture-library.js')return send(read(path.join(R,'tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt')),'application/javascript');
  if(scripts.includes(u.pathname.slice(1))||u.pathname==='/jh-chart-tvwatch.js')return send(read(path.join(R,u.pathname.slice(1))),'application/javascript');
  if(u.pathname==='/data/provider-catalog.json')return denied?send({},'application/json',403):metadata('catalogue');
  if(u.pathname==='/data/providers/census-us.json')return wrongIdentity?send({...json(path.join(D,'files.json')),slug:'census'}):metadata('files');
  if(u.pathname==='/data/providers/census-us/page-000.json')return metadata('files-page');
  if(u.pathname==='/explorer'){const p=u.searchParams.get('provider');assert.ok(['census','census-us'].includes(p));return metadata(p+'-directory');}
  if(u.pathname==='/browse'){const target=targets.find(t=>t.dataset===u.searchParams.get('ds'));assert.ok(target);const q=u.searchParams.get('q');assert.ok(q===''||q===target.id);return metadata((q?'series-':'dataset-')+target.i);}
  if(u.pathname==='/series'){const t=targets.find(t=>t.id.toLowerCase()===u.searchParams.get('id').toLowerCase());assert.ok(t);return send(packet(t));}
  if(u.pathname==='/data/tv-watchlists.json')return send({lists:[{id:'invented-discovery',name:'Invented list',symbols:['SPY']}]});
  if(['/ohlc','/yf-ohlc'].includes(u.pathname))return send({ticker:'SPY',span:'day',mult:1,bars:[]});
  if(u.pathname.startsWith('/data/')||u.pathname.startsWith('/api/'))return send({});return route.abort('blockedbyclient');
 });
 try{
  await page.goto('https://fixture.discovery.test/chart.html');await page.waitForFunction(()=>window.JHChartProviderBrowser&&window.__jhTvWatch2);
  // Enter through the actual chart picker, never by opening a hidden provider directly.
  await page.locator('#symchip').click();await page.locator('#jh-provider-open').click();await page.locator('button.jhp-row').filter({hasText:'Chart adapter; warehouse files: census-us'}).waitFor();
  await page.getByRole('searchbox',{name:'Filter provider metadata'}).fill('census');await page.getByRole('button',{name:'Search',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.jhp-status').textContent.startsWith('2 provider entries'));
  assert.equal(await page.locator('button.jhp-row').count(),2);assert.ok((await page.locator('.jhp-foot').innerText()).includes('not rows in the downloaded warehouse catalogue'));
  await page.screenshot({path:path.join(OUT,'census-discovery-'+width+'.png')});
  await page.locator('button.jhp-row').filter({hasText:'Chart adapter; warehouse files: census-us'}).click();await page.getByText('census:mrts',{exact:true}).waitFor();
  for(const target of targets){
   if(target.i){await page.evaluate(()=>JHChartProviderBrowser.open());await page.locator('button.jhp-row').filter({hasText:'Chart adapter; warehouse files: census-us'}).click();await page.getByText(target.dataset,{exact:true}).waitFor();}
   await page.locator('.jhp-row').filter({has:page.getByText(target.dataset,{exact:true})}).getByRole('button',{name:'Choose series / dimensions',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.jhp-status').textContent.includes('Rows 1'));
   await page.getByRole('searchbox',{name:'Filter provider metadata'}).fill(target.id);await page.getByRole('button',{name:'Search',exact:true}).click();await page.getByText(target.id,{exact:true}).waitFor();
   await page.getByRole('button',{name:'Chart exact series',exact:true}).click();await page.waitForFunction(id=>jhWatchlistActive()===id&&jhChartEvidence&&jhChartEvidence.symbol===id,target.id);
   const p=packet(target),points=p.obs.filter(r=>r[1]!==null).map(r=>[Date.parse(r[0]+'T00:00:00Z')/1000,r[1]]);assert.deepEqual(await page.evaluate(()=>lastBars.map(r=>[r.time,r.close])),points);assert.equal(await page.locator('#tabs .tab.on').getAttribute('data-id'),target.id);assert.ok((await page.locator('#quote').innerText()).includes(p.unit));
  }
  await page.evaluate(()=>JHChartProviderBrowser.open());await page.locator('button.jhp-row').filter({hasText:'census-us · api.census.gov'}).click();await page.getByRole('button',{name:'Browse reviewed economic histories',exact:true}).click();await page.getByText('census:mrts',{exact:true}).waitFor();
  await page.getByRole('button',{name:'Stored files',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.jhp-status').textContent.includes('census-us · overview'));assert.ok((await page.locator('.jhp-rows').innerText()).includes(json(path.join(D,'files.json')).keys[0].key));
  await page.getByRole('button',{name:'Next',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.jhp-status').textContent.includes('continuation 1 of 1'));assert.equal(await page.getByRole('button',{name:'Next',exact:true}).isDisabled(),true);
  wrongIdentity=true;await page.getByRole('button',{name:'Stored files',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.jhp-status').textContent.includes('identity or page count is invalid'));assert.equal(await page.locator('.jhp-rows').innerText(),'');wrongIdentity=false;
  denied=true;await page.getByRole('button',{name:'All providers',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.jhp-status').textContent.includes('HTTP 403'));assert.equal(await page.locator('.jhp-rows').innerText(),'');await page.getByRole('searchbox',{name:'Filter provider metadata'}).press('Escape');assert.equal(await page.locator('#jh-provider-browser').isVisible(),false);
  assert.ok(!requests.some(p=>p.startsWith('/data/providers/census.json')||p.startsWith('/data/warm/')||(/^\/(ohlc|yf-ohlc)\?/.test(p)&&/census/i.test(decodeURIComponent(p)))));assert.deepEqual(errors,[]);
  const size=await page.evaluate(()=>({scroll:document.documentElement.scrollWidth,client:document.documentElement.clientWidth}));assert.ok(size.scroll<=size.client+1);
  report.push({width,passed:true,errors,actual_network_requests:0,public_metadata_receipts:receipt.requests,public_histories_compared:process.env.JH_CENSUS_HISTORY_ROOT?targets.map(t=>t.id):[],requests,all_provider_entrypoint_checked:true,warehouse_bridge_checked:true,wrong_identity_rejected:true,denial_not_substituted:true});
 }catch(error){fs.writeFileSync(path.join(OUT,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,body:await page.locator('body').innerText()},null,2));await page.screenshot({path:path.join(OUT,'failure-'+width+'.png')});throw error;}finally{await context.close();}
 }fs.writeFileSync(path.join(OUT,'browser-qa.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));}finally{await browser.close();}})().catch(error=>{console.error(error);process.exit(1);});
