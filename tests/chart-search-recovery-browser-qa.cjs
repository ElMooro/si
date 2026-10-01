const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const html=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const served=html.replace('</body>','<script src="/fixture-catalog.js"></script><script src="/fixture-engine.js"></script></body>');
const inputs=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/symbol-directory/browser-cache-reproduction.json'),'utf8')).whole_inputs;
const remote={rows:[{id:'ZZREMOTE',name:'Invented remote entry',provider:'instrument',kind:'instrument'}],facets:[],index_integrity:{head_check:{status:'unavailable',checked_at:'2026-09-01T00:00:00Z',serving_cached_generation:true}},warehouse_integrity:{status:'unavailable'}};
const output={scope:'Complete chart HTML/CSS and two chart modules; chart drawing inert, all requests intercepted, complete invented sources only.',whole_inputs:inputs,whole_cached_search:remote,cases:[]};
const pause=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},hasTouch:width===390,isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage();
  const requests=[],errors=[];let mode='unavailable',firstA=true,releaseA,startedA=false;
  const slowA=new Promise(r=>{releaseA=r;});
  page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(()=>{let fake;fake=new Proxy(function(){return fake;},{get:(t,k)=>k==='then'?undefined:fake});window.LightweightCharts={createChart:()=>fake,LineStyle:{},CrosshairMode:{},PriceScaleMode:{},ColorType:{}};window.Notification=undefined;const original=Date.now;let offset=0;Date.now=()=>original()+offset;window.fixtureAdvance=ms=>{offset+=ms;};});
  await context.route('**/*',async route=>{
   const url=new URL(route.request().url()),key=url.pathname;requests.push({host:url.host,path:key,query:url.search,method:route.request().method(),mode});
   const send=(body,type='application/json',status=200)=>route.fulfill({status,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
   if(key==='/chart.html' && url.host==='invented.justhodl.test')return send(served,'text/html');
   if(key==='/fixture-catalog.js')return send(fs.readFileSync(path.join(R,'jh-chart-catalog.js'),'utf8'),'application/javascript');
   if(key==='/fixture-engine.js')return send(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'application/javascript');
   if(key==='/data/symbology/master.json')return mode==='unavailable'?send({error:'invented failure'},'application/json',503):mode==='malformed'?send({by_ticker:[]}):send(inputs[key]);
   if(Object.hasOwn(inputs,key))return send(inputs[key]);
   if(key==='/symsearch'){
    const q=url.searchParams.get('q');
    if(q==='query-A' && firstA){firstA=false;startedA=true;await slowA;return send({rows:[{id:'ZZOLD',name:'Invented old response'}],facets:[{provider:'OLD',provider_name:'Old response',n:1}]});}
    if(q==='query-A')return send({rows:[{id:'ZZNEW',name:'Invented current response'}],facets:[{provider:'NEW',provider_name:'Current response',n:1}]});
    if(q==='FAIL')return send({error:'invented directory failure'},'application/json',503);
    return send(remote);
   }
   if(key==='/tv-search')return send({symbols:url.searchParams.get('text')==='FAIL'?[{symbol:'ZZTV',description:'Invented additional source'}]:[]});
   if(key==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  await page.goto('https://invented.justhodl.test/chart.html');await page.waitForFunction(()=>typeof jhOpenSearch==='function');
  await page.evaluate(async()=>{await JHChartCatalog.ensureIndex();jhOpenSearch();});await page.locator('#ssin').fill('ZZRECOVERED');
  await page.waitForFunction(()=>document.querySelector('[data-search-status]')?.textContent.includes('coverage is incomplete'));
  assert.equal(await page.locator('#ssres').getByText('Invented recovered issuer',{exact:true}).count(),0);
  mode='recovered';await page.evaluate(()=>fixtureAdvance(31000));await page.locator('#ssin').fill('ZZREC');
  await page.waitForFunction(()=>document.querySelector('#ssres').textContent.includes('Invented recovered issuer'));
  await page.waitForFunction(()=>document.querySelector('#ssres').textContent.includes('Invented remote entry'));
  const recovered=await page.locator('#ssres').innerHTML();assert.match(recovered,/cached generation/);assert.match(recovered,/warehouse search is unavailable/);
  mode='malformed';await page.evaluate(()=>fixtureAdvance(300000));await page.locator('#ssin').fill('ZZRECOVER');
  await page.waitForFunction(()=>document.querySelector('[data-search-status]')?.textContent.includes('Using cached catalogs'));
  assert.match(await page.locator('#ssres').textContent(),/Invented recovered issuer/);
  await page.locator('#ssin').fill('FAIL');await page.waitForFunction(()=>document.querySelector('#ssres').textContent.includes('Invented additional source'));
  assert.match(await page.locator('#ssres').textContent(),/Directory search unavailable/);
  await page.locator('#ssin').fill('query-A');for(let i=0;i<80&&!startedA;i++)await pause(25);assert.ok(startedA);
  await page.locator('#ssin').fill('query-B');await pause(200);await page.locator('#ssin').fill('query-A');
  await page.waitForFunction(()=>document.querySelector('#ssres').textContent.includes('Invented current response'));releaseA();await pause(150);
  assert.doesNotMatch(await page.locator('#ssres').textContent(),/ZZOLD|Old response/);
  await page.locator('#ssin').press('Escape');assert.equal(await page.locator('#symsearch.on').count(),0);
  await page.evaluate(()=>jhOpenSearch());await page.locator('#ssin').fill('ZZRECOVER');await page.waitForFunction(()=>document.querySelector('#ssres').textContent.includes('Invented remote entry'));
  const layout=await page.evaluate(()=>{const el=document.querySelector('[data-search-status]');return {width:el.clientWidth,scroll:el.scrollWidth,height:el.clientHeight,viewport:innerWidth,left:el.getBoundingClientRect().left,right:el.getBoundingClientRect().right,text:el.textContent,css:{whiteSpace:getComputedStyle(el).whiteSpace,display:getComputedStyle(el).display,boxSizing:getComputedStyle(el).boxSizing,overflowWrap:getComputedStyle(el).overflowWrap,width:getComputedStyle(el).width}};});
  fs.writeFileSync(path.join(D,'layout-'+width+'.json'),JSON.stringify(layout,null,2)+'\n');await page.screenshot({path:path.join(D,'layout-'+width+'.png')});
  assert.ok(layout.width>0 && layout.height>0 && layout.scroll<=layout.width+1);assert.ok(layout.left>=0 && layout.right<=layout.viewport+1);
  assert.equal(errors.length,0);const screenshot=path.join(D,'recovery-'+width+'.png');await page.screenshot({path:screenshot});
  output.cases.push({width,requests,errors,recovered_dom:recovered,final_dom:await page.locator('#ssres').innerHTML(),layout,screenshot,recovered_without_reload:true,malformed_replacement_preserved:true,remote_rows_preserved_after_catalog_repaint:true,cached_generation_visible:true,old_query_withheld:true,actual_network_requests:0});
  await context.close();
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:output.cases.map(c=>c.width),actual_network_requests:0}));
 }finally{await browser.close();}})().catch(error=>{console.error(error);process.exit(1);});
