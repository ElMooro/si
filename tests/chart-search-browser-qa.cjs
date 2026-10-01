const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');
assert.ok(process.argv[2],'Pass an external output directory');fs.mkdirSync(D,{recursive:true});
const html=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const inputs=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/symbol-directory/search-reproduction.json'),'utf8')).whole_inputs;
const served=html.replace('</body>','<script src="/fixture-catalog.js"></script><script src="/fixture-engine.js"></script></body>');
const output={scope:'Complete chart/catalog modules and chart HTML/CSS, other scripts removed and chart-drawing library inert. Search UI only; every page request intercepted. No actual private/account/current/provider data.',whole_inputs:inputs,cases:[]};
const delay=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{
for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:1000},hasTouch:width===390,isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage();
 const errors=[],requests=[];let releaseSlow,slowStarted=false;
 const slow=new Promise(r=>{releaseSlow=r;});
 page.on('pageerror',e=>errors.push(e.message));
 await context.addInitScript(()=>{let fake;fake=new Proxy(function(){return fake;},{get:(t,k)=>k==='then'?undefined:fake});window.LightweightCharts={createChart:()=>fake,LineStyle:{},CrosshairMode:{},PriceScaleMode:{},ColorType:{}};window.Notification=undefined;});
 await context.route('**/*',async route=>{
  const url=new URL(route.request().url()),key=url.pathname;requests.push({host:url.host,path:key,query:url.search,method:route.request().method()});
  const fulfill=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
  if(key==='/chart.html' && url.host==='invented.justhodl.test')return fulfill(served,'text/html');
  if(key==='/fixture-catalog.js')return fulfill(fs.readFileSync(path.join(R,'jh-chart-catalog.js'),'utf8'),'application/javascript');
  if(key==='/fixture-engine.js')return fulfill(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),'application/javascript');
  if(Object.hasOwn(inputs,key))return fulfill(inputs[key]);
  if(key==='/symsearch'){
   if(url.searchParams.get('q')==='slowquery'){slowStarted=true;await slow;return fulfill({rows:[{id:'ZZLATE',symbol:'ZZLATE',name:'Invented late old query',provider:'instrument',kind:'instrument'}],facets:[{provider:'oldquery',provider_name:'Old query facet',n:1}]});}
   return fulfill({rows:[],facets:[{provider:"x'<svg>",provider_name:'<img id="invented-facet" src=x onerror="window.fixtureFacetInjection=1">',n:null}],total:0});
  }
  if(key==='/tv-search')return fulfill({symbols:[]});
  if(key==='/api/yahoo-search')return fulfill({quotes:[]});
  return route.abort('blockedbyclient');
 });
 await page.goto('https://invented.justhodl.test/chart.html');
 await page.waitForFunction(()=>typeof window.jhOpenSearch==='function',null,{timeout:10000});
 await page.evaluate(async()=>{await JHChartCatalog.ensureIndex();jhOpenSearch();});
 await page.locator('#ssin').fill('US0000000000');
 await page.waitForFunction(()=>document.querySelector('#ssres').textContent.includes('ZZFIRST'));
 assert.equal(await page.locator('#ssres .ss-hit.on').count(),0);
 assert.equal(await page.locator('#ssres .ss-hit').count(),2);
 const start=requests.length;await page.locator('#ssin').press('Enter');await delay(200);
 const unselected=requests.slice(start).filter(r=>/ZZFIRST|ZZSECOND/.test(r.query+r.path));assert.equal(unselected.length,0);
 assert.equal(await page.locator('#symsearch.on').count(),1);
 await page.locator('#ssin').press('ArrowDown');await page.locator('#ssin').press('ArrowDown');
 assert.match(await page.locator('#ssres .ss-hit.on').textContent(),/ZZSECOND/);
 const chosen=requests.length;await page.locator('#ssin').press('Enter');await delay(200);
 assert.ok(requests.slice(chosen).some(r=>/ZZSECOND/.test(r.query+r.path)));
 await page.evaluate(()=>jhOpenSearch());await page.locator('#ssin').fill('ZZMARKUP');await delay(250);
 assert.equal(await page.locator('#invented-markup,#invented-facet').count(),0);
 assert.equal(await page.evaluate(()=>window.fixtureInjection||window.fixtureFacetInjection||null),null);
 assert.ok((await page.locator('#ssres').textContent()).includes('<img'));
 const facets=await page.locator('#ssres .ssfacets').innerHTML();assert.match(facets,/&lt;img/);
 await page.locator('#ssin').fill('ZZSECOND');const exactStart=requests.length;await page.locator('#ssin').press('Enter');await delay(200);
 assert.ok(requests.slice(exactStart).some(r=>/ZZSECOND/.test(r.query+r.path)));
 await page.evaluate(()=>jhOpenSearch());await page.locator('#ssin').fill('slowquery');
 for(let n=0;n<40&&!slowStarted;n++)await delay(25);assert.ok(slowStarted);
 await page.locator('#ssin').fill('Z');releaseSlow();await delay(250);
 assert.doesNotMatch(await page.locator('#ssres').textContent(),/ZZLATE|Old query facet/);
 await page.locator('#ssin').fill('US0000000000');await delay(200);
 const box=await page.locator('#symsearch').boundingBox(),res=await page.locator('#ssres').boundingBox();
 const geometry=await page.evaluate(()=>({viewport:window.innerWidth,document:document.documentElement.scrollWidth}));
 assert.ok(res.width>0 && res.height>0);assert.equal(errors.length,0);
 const rowLayout=await page.locator('#ssres .ss-hit').evaluateAll(rows=>rows.map(row=>({width:row.clientWidth,scroll:row.scrollWidth,name:row.querySelector('.ds').getBoundingClientRect().width,meta:row.querySelector('.ss-ex').getBoundingClientRect().width})));
 assert.ok(rowLayout.every(row=>row.scroll<=row.width+1 && row.name>100 && row.meta>100));
 const screenshot=path.join(D,'search-'+width+'.png');await page.screenshot({path:screenshot});
 output.cases.push({width,errors,requests,unselected_identifier_requests:unselected.length,explicit_selection_attempts_verified:true,exact_ticker_attempt_verified:true,source_markup_rendered_as_text:true,late_old_query_withheld:true,box,res,geometry,rowLayout,screenshot,whole_final_dom:await page.locator('#ssres').innerHTML(),actual_page_network_requests:0});
 await context.close();
}
fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');
console.log(JSON.stringify({widths:output.cases.map(c=>c.width),passed:true,actual_page_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1)});
