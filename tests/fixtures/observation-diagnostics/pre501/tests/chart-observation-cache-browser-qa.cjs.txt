const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const realLibrary=process.argv.includes('--real-library'),vendor='tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt';
function installDrawingCapture(){
 const real=window.LightweightCharts;window.LightweightCharts=Object.assign({},real,{createChart(...args){
  const chart=real.createChart(...args),state={series:[],formatValue:value=>chart.options().localization.priceFormatter(value),scaleMode:()=>chart.priceScale('right').options().mode};window.fixtureCharts.push(state);
  for(const kind of ['addLineSeries','addAreaSeries','addBaselineSeries','addBarSeries','addCandlestickSeries','addHistogramSeries']){
   const add=chart[kind].bind(chart);chart[kind]=options=>{const series=add(options),set=series.setData.bind(series),row={kind,options,data:[]};state.series.push(row);series.setData=data=>{row.data=data;set(data);};return series;};
  }
  const remove=chart.removeSeries.bind(chart);chart.removeSeries=series=>{state.series=[];return remove(series);};return chart;
 }});
}
const scripts=['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js','jh-chart-engine.js','jh-stock-desk-research.js','jh-chart-stock-desk.js'];
const html=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const served=html.replace('</body>',(realLibrary?'<script src="/fixture-drawing.js"></script><script src="/fixture-drawing-capture.js"></script>':'')+scripts.map(p=>'<script src="/'+p+'"></script>').join('')+'</body>');
const dates=Array.from({length:65},(_,i)=>new Date(Date.UTC(2026,6,1+i)).toISOString().slice(0,10));
const values=dates.map((d,i)=>i===0?-0.0000000123456789:i===4?null:i===5?false:i===6?'':i===7?[]:i===8?'<img id="invented-injection" src=x onerror="window.fixtureInjected=1">':i-20);
const inputs={'/data/cryptoquant-series.json':{generated_at:'2026-10-01T00:00:00Z',series:{invented_metric:{d:dates,v:values,freq:'D',unit:'invented_ratio',points_scope:'Whole invented source'}},twins:{invented_metric:{d:['2010-01-01'],v:[999]}}},
 '/data/cryptoquant-onchain.json':{metrics:{}},'/data/cq-feed.json':{metrics:{}},'/data/cq-catalog.json':{catalog:{}},'/data/config/cryptoquant-spec.json':{metrics:[]},'/cq-universe.json':{rows:[]},
 '/data/ciss-stress.json':{generated_at:'2026-10-01T00:00:00Z',series:[{id:'MONTHLY',key:'INVENTED.MONTHLY',freq:'M',unit:'dimensionless_index',points:[['2024-01',-1],['2024-02',0],['2024-03',1]]}]},
 '/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Whole chart HTML/CSS and seven complete modules; bounded cache failure/recovery with complete invented packets. Every request intercepted; no actual site execution or provider/private/current-consumer reads.',drawing_library:realLibrary?'Pinned complete 4.2.3 library, rendered offline':'Inert mock',whole_inputs:inputs,cases:[]};
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},hasTouch:width===390,isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage();
  let packetMode='valid';const rejected={series:false,whole_rejected:{note:'invented replacement'}};const recovered=structuredClone(inputs['/data/cryptoquant-series.json']);recovered.series.invented_metric.v[0]=-0.000000023456789;
  const requests=[],errors=[];page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(()=>{
   window.fixtureCharts=[];window.fixtureBlob=null;window.Notification=undefined;const originalNow=Date.now;let clockOffset=0;Date.now=()=>originalNow()+clockOffset;window.fixtureAdvance=ms=>{clockOffset+=ms;};
   const url=URL.createObjectURL;URL.createObjectURL=b=>{window.fixtureBlob=b;return url(b);};
   let fake;fake=new Proxy(function(){return fake;},{get:(t,k)=>k==='then'?undefined:k==='getVisibleLogicalRange'?()=>null:fake});
   window.LightweightCharts={createChart:()=>{const state={series:[]};window.fixtureCharts.push(state);return new Proxy({}, {get:(t,k)=>{
    if(/^add.*Series$/.test(String(k)))return options=>{const row={kind:k,options,data:[]};state.series.push(row);return new Proxy({}, {get:(t,p)=>p==='setData'?d=>{row.data=d;}:fake});};
    if(k==='removeSeries')return()=>{state.series=[];};return fake;
   }});},LineStyle:{},CrosshairMode:{},PriceScaleMode:{},ColorType:{}};
  });
  await context.route('**/*',route=>{
   const u=new URL(route.request().url());requests.push({host:u.host,path:u.pathname,query:u.search,method:route.request().method()});
   const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
   if(u.host==='invented.justhodl.test'&&u.pathname==='/chart.html')return send(served,'text/html');
   if(realLibrary&&u.pathname==='/fixture-drawing.js')return send(fs.readFileSync(path.join(R,vendor),'utf8'),'application/javascript');
   if(realLibrary&&u.pathname==='/fixture-drawing-capture.js')return send('('+installDrawingCapture.toString()+')();','application/javascript');
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(u.pathname==='/data/cryptoquant-series.json')return send(packetMode==='malformed'?rejected:packetMode==='recovered'?recovered:inputs[u.pathname]);
   if(Object.hasOwn(inputs,u.pathname))return send(inputs[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});
   if(u.pathname==='/tv-search')return send({symbols:[]});
   if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  await page.goto('https://invented.justhodl.test/chart.html?s=CQ:invented_metric');
  try{await page.waitForFunction(()=>JHStockDeskController.getModel()?.kind==='scalar_observations',null,{timeout:10000});}catch(error){
   fs.writeFileSync(path.join(D,'failure.json'),JSON.stringify({errors,requests,state:await page.evaluate(()=>({quote:document.querySelector('#quote')?.textContent,tech:document.querySelector('#tech')?.innerHTML,evidence:window.jhChartEvidence||null,bars:window.lastBars||null,tabs:document.querySelector('#tabs')?.innerHTML}))},null,2));throw error;
  }
  const model=await page.evaluate(()=>JHStockDeskController.getModel());assert.equal(model.bars.length,60);assert.equal(model.observations.records.length,65);
  assert.deepEqual(model.observations.whole_packet,inputs['/data/cryptoquant-series.json']);assert.equal(model.bars[0].close,-0.0000000123456789);
  const controls=await page.evaluate(()=>{jhSetKind('renko');jhSetScale(1);return fixtureCharts[0].series.at(-1);});
  assert.equal(controls.kind,'addLineSeries');assert.equal(controls.data[0].value,-0.0000000123456789);assert.ok(controls.data.some(r=>r.value===0));
  for(const [id,label] of [['btn-kind','Source line'],['btn-md','Scalar'],['btn-sc','Linear']]){assert.match(await page.locator('#'+id).textContent(),new RegExp(label));assert.equal(await page.locator('#'+id).isDisabled(),true);}
  if(realLibrary){assert.equal(await page.evaluate(()=>fixtureCharts[0].formatValue(-0.0000000123456789)),'-1.23456789e-8');assert.equal(await page.evaluate(()=>fixtureCharts[0].scaleMode()),0);}
  const plot=path.join(D,'observations-plot-'+width+'.png');await page.screenshot({path:plot});
  await page.evaluate(()=>{document.querySelector('#btn-rep').click();});assert.equal(await page.locator('#replay.on').count(),0);
  await page.evaluate(()=>{document.querySelector('#btn-watch').click();document.querySelector('[data-sub=details]').click();document.querySelector('[data-stock-observation-details]').open=true;});
  for(const id of ['fin','over','season','trade','test','corr'])assert.match(await page.locator('#'+id).textContent(),/unavailable for this scalar/);
  assert.doesNotMatch(await page.locator('#detail').textContent(),/LATEST BAR RANGE|%/);
  await page.waitForFunction(()=>document.querySelector('[data-stock-observation-range]').textContent==='1–25 of 65');
  assert.equal(await page.locator('#invented-injection').count(),0);assert.equal(await page.evaluate(()=>window.fixtureInjected||null),null);
  assert.ok((await page.locator('[data-stock-observation-rows]').textContent()).includes(JSON.stringify(values[8])));
  await page.evaluate(()=>document.querySelector('[data-stock-observation-next]').click());
  await page.evaluate(()=>JHStockDeskController.refresh());assert.equal(await page.locator('[data-stock-observation-range]').textContent(),'26–50 of 65');
  await page.evaluate(()=>document.querySelector('[data-stock-observation-next]').click());assert.equal(await page.locator('[data-stock-observation-range]').textContent(),'51–65 of 65');
  await page.evaluate(()=>document.querySelector('[data-stock-observation-export]').click());
  const exported=await page.evaluate(async()=>JSON.parse(await fixtureBlob.text()));assert.deepEqual(exported,model);
  const screenshot=path.join(D,'observations-'+width+'.png');await page.screenshot({path:screenshot});
  const layout=await page.locator('[data-stock-observations]').evaluate(el=>({width:el.clientWidth,scroll:el.scrollWidth,rect:el.getBoundingClientRect().toJSON()}));
  await page.evaluate(()=>jhSetTf('1w'));await page.waitForFunction(()=>JHStockDeskController.getModel()?.interval==='1w');
  const grouped=await page.evaluate(()=>JHStockDeskController.getModel());assert.equal(grouped.observations.records.length,65);
  assert.deepEqual(grouped.bars.flatMap(b=>b.observation_ordinals).sort((a,b)=>a-b),model.bars.flatMap(b=>b.observation_ordinals).sort((a,b)=>a-b));
  packetMode='malformed';await page.evaluate(()=>{fixtureAdvance(300000);jhSetTf('1d');});
  await page.waitForFunction(()=>JHStockDeskController.getModel()?.observations.transport_cache?.state==='cached_after_failure');
  const failedRefresh=await page.evaluate(()=>JHStockDeskController.getModel());assert.deepEqual(failedRefresh.observations.whole_packet,inputs['/data/cryptoquant-series.json']);
  assert.deepEqual(failedRefresh.observations.transport_cache.attempts[0].rejected_packet,rejected);assert.match(await page.locator('[data-stock-cache]').textContent(),/Using previous packet after download failure/);
  const cachedScreenshot=path.join(D,'cached-'+width+'.png');await page.screenshot({path:cachedScreenshot});
  packetMode='recovered';await page.evaluate(()=>{fixtureAdvance(30000);jhSetTf('1d');});
  await page.waitForFunction(()=>JHStockDeskController.getModel()?.bars[0]?.close===-0.000000023456789);
  const recoveredRefresh=await page.evaluate(()=>JHStockDeskController.getModel());assert.deepEqual(recoveredRefresh.observations.whole_packet,recovered);assert.equal(recoveredRefresh.observations.transport_cache.state,'checked_within_interval');
  assert.equal(recoveredRefresh.observations.calls_eligible,false);assert.match(await page.locator('[data-stock-cache]').textContent(),/Download checks do not establish observation freshness/);
  await page.evaluate(()=>paint(lastBars.slice()));assert.equal(await page.evaluate(()=>jhChartEvidence),null);
  assert.match(await page.locator('#quote').textContent(),/evidence unavailable/);
  await page.evaluate(()=>jhOpenSymbol('CQ:unknown_metric'));await page.waitForFunction(()=>document.querySelector('#quote').textContent.includes('no other series is substituted'));
  assert.equal(await page.evaluate(()=>lastBars.length),0);assert.equal(await page.evaluate(()=>JHStockDeskController.getModel()),null);
  await page.goto('https://invented.justhodl.test/chart.html?s=CISS:MONTHLY');await page.waitForFunction(()=>JHStockDeskController.getModel()?.kind==='scalar_observations');
  const monthly=await page.evaluate(()=>JHStockDeskController.getModel());assert.deepEqual(monthly.bars.map(b=>new Date(b.time*1000).toISOString().slice(0,10)),['2024-01-31','2024-02-29','2024-03-31']);
  await page.goto('https://invented.justhodl.test/chart.html?s=CISS:UNKNOWN');await page.waitForFunction(()=>document.querySelector('#quote').textContent.includes('no other series is substituted'));
  assert.equal(await page.evaluate(()=>lastBars.length),0);assert.equal(await page.evaluate(()=>JHStockDeskController.getModel()),null);
  const forbidden=requests.filter(r=>/\/ohlc|\/yf-ohlc|\/trades|\/aggTrades|\/klines|\/yahoo-fund/.test(r.path)&&/CQ|CISS|INVENTED|MONTHLY|UNKNOWN/i.test(r.query));assert.equal(forbidden.length,0);
  assert.equal(errors.length,0,JSON.stringify(errors));
  output.cases.push({width,requests,errors,whole_export:exported,whole_grouped:grouped,whole_monthly:monthly,whole_failed_refresh:failedRefresh,whole_recovered_refresh:recoveredRefresh,layout,screenshot,plot_screenshot:plot,cached_screenshot:cachedScreenshot,scalar_price_requests:forbidden.length,actual_network_requests:0});await context.close();
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:output.cases.map(c=>c.width),actual_network_requests:0}));
 }finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
