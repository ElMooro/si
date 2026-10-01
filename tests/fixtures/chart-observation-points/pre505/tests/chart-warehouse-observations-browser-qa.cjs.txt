const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const realLibrary=process.argv.includes('--real-library'),vendor='tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt';
function installDrawingCapture(){
 const real=window.LightweightCharts;window.LightweightCharts=Object.assign({},real,{createChart(...args){
  const chart=real.createChart(...args),state={series:[],watermarkVisible:()=>chart.options().watermark.visible,formatValue:value=>chart.options().localization.priceFormatter(value),scaleMode:()=>chart.priceScale('right').options().mode};window.fixtureCharts.push(state);
  for(const kind of ['addLineSeries','addAreaSeries','addBaselineSeries','addBarSeries','addCandlestickSeries','addHistogramSeries']){
   const add=chart[kind].bind(chart);chart[kind]=options=>{const series=add(options),set=series.setData.bind(series),row={kind,options,data:[]};state.series.push(row);series.setData=data=>{row.data=data;set(data);};return series;};
  }
  const remove=chart.removeSeries.bind(chart);chart.removeSeries=series=>{state.series=[];return remove(series);};return chart;
 }});
}
const scripts=['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js','jh-chart-engine.js','jh-stock-desk-research.js','jh-chart-stock-desk.js'];
const html=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const served=html.replace('</body>',(realLibrary?'<script src="/fixture-drawing.js"></script><script src="/fixture-drawing-capture.js"></script>':'')+scripts.map(p=>'<script src="/'+p+'"></script>').join('')+'</body>');

const make=(id,obs=[['2026-01-01',-2],['2026-01-02',0]])=>({id,provider:id.split(':')[0],provider_name:'Invented provider',source:'invented:test',as_of:'2026-01-03T00:00:00Z',freq:'D',unit:'invented',obs,extra:{whole:true}});
const fred=make('FRED:invented',Array.from({length:31},(_,i)=>[new Date(Date.UTC(2026,0,i+1)).toISOString().slice(0,10),i===0?-0.0000000123456789:i===4?null:i===5?false:i===6?'':i===7?[]:i-15]));
fred.obs.push(['2026-02-30',9]);fred.source='<img id="injected-source" onerror="window.fixtureInjected=1">';
const inputs={'FRED:invented':fred,'NYFED:invented':make('NYFED:invented'),'ECB:invented':Object.assign(make('ECB:invented',[['2024-02-01',-1],['2024-03-01',0]]),{freq:'M'}),
 'FRED:DGS10':make('FRED:DGS10'),'FRED:invalid':make('FRED:invalid',[['2026-01-01',null],['2026-01-02',false]]),'FRED:wrong':make('NYFED:other'),'FRED:slow':make('FRED:slow')};
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Whole chart HTML/CSS and seven complete modules. Every request intercepted; invented warehouse data only. No actual application, provider or private reads.',drawing_library:realLibrary?'Pinned complete 4.2.3, rendered offline':'Inert mock',whole_inputs:inputs,cases:[]};
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},hasTouch:width===390,isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage();
  let mode='valid',slow=true;const held=[],requests=[],errors=[];page.on('pageerror',e=>errors.push(e.message));
  const wrong=make('NYFED:wrong'),recovered=structuredClone(fred);recovered.obs[0][1]=-0.000000023456789;
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
   if(u.pathname==='/series'){
    const id=u.searchParams.get('id');
    if(id==='FRED:slow'&&slow)return new Promise(resolve=>held.push(()=>send(inputs[id]).catch(()=>{}).finally(resolve)));
    if(id==='FRED:invented')return send(mode==='wrong'?wrong:mode==='recovered'?recovered:fred);
    return send(inputs[id]||make(id));
   }
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  const models={},screenshots=[];
  const snapshot=async name=>{const file=path.join(D,name+'-'+width+'.png');await page.screenshot({path:file});screenshots.push(file);};
  const wait=async id=>{try{await page.waitForFunction(id=>JHStockDeskController.getModel()?.observations.requested_id===id,id,{timeout:15000});}catch(error){fs.writeFileSync(path.join(D,'failure.json'),JSON.stringify({id,errors,requests,state:await page.evaluate(()=>({model:window.JHStockDeskController?.getModel(),quote:document.querySelector('#quote')?.textContent,tabs:document.querySelector('#tabs')?.innerHTML}))},null,2));throw error;}};
  await page.goto('https://invented.justhodl.test/chart.html?s=FRED:invented');await wait('FRED:invented');
  models.initial=await page.evaluate(()=>JHStockDeskController.getModel());assert.equal(models.initial.bars.length,27);assert.equal(models.initial.observations.records.length,32);assert.deepEqual(models.initial.observations.whole_packet,fred);
  assert.equal(models.initial.bars[0].close,-0.0000000123456789);assert.ok(models.initial.bars.some(b=>b.close===0));assert.equal(models.initial.observations.source_acquired_at,null);
  for(const [id,label]of [['btn-kind','Source line'],['btn-md','Scalar'],['btn-sc','Linear']]){assert.match(await page.locator('#'+id).textContent(),new RegExp(label));assert.equal(await page.locator('#'+id).isDisabled(),true);}
  assert.equal(await page.evaluate(()=>fixtureCharts[0].series.at(-1).kind),'addLineSeries');await snapshot('source-line');
  await page.evaluate(()=>{document.querySelector('#btn-watch').click();document.querySelector('[data-sub=details]').click();document.querySelector('[data-stock-observation-details]').open=true;});
  for(const id of ['fin','over','season','trade','test','corr'])assert.match(await page.locator('#'+id).textContent(),/unavailable for this scalar/);
  assert.match(await page.locator('[data-stock-warehouse]').textContent(),/packet construction metadata; not first publication/);assert.equal(await page.locator('#injected-source').count(),0);assert.equal(await page.evaluate(()=>window.fixtureInjected||null),null);
  assert.match(await page.locator('[data-stock-observations]').textContent(),/Received date/);
  await page.evaluate(()=>document.querySelector('[data-stock-observation-next]').click());assert.equal(await page.locator('[data-stock-observation-range]').textContent(),'26–32 of 32');
  await page.evaluate(()=>document.querySelector('[data-stock-observation-export]').click());const exported=await page.evaluate(async()=>JSON.parse(await fixtureBlob.text()));assert.deepEqual(exported,models.initial);await snapshot('inspection');
  await page.evaluate(()=>jhSetTf('1w'));await page.waitForFunction(()=>JHStockDeskController.getModel()?.interval==='1w');models.week=await page.evaluate(()=>JHStockDeskController.getModel());assert.equal(models.week.observations.records.length,32);
  mode='wrong';await page.evaluate(()=>{fixtureAdvance(300000);jhSetTf('1d');});await page.waitForFunction(()=>JHStockDeskController.getModel()?.observations.transport_cache.state==='cached_after_failure');
  models.failed=await page.evaluate(()=>JHStockDeskController.getModel());assert.deepEqual(models.failed.observations.whole_packet,fred);assert.deepEqual(models.failed.observations.transport_cache.attempts[0].rejected_packet,wrong);await snapshot('cached-failure');
  mode='recovered';await page.evaluate(()=>{fixtureAdvance(30000);jhSetTf('1d');});await page.waitForFunction(()=>JHStockDeskController.getModel()?.bars[0]?.close===-0.000000023456789);models.recovered=await page.evaluate(()=>JHStockDeskController.getModel());assert.deepEqual(models.recovered.observations.whole_packet,recovered);
  for(const id of ['NYFED:invented','ECB:invented','FRED:invalid','FRED:wrong']){
   await page.evaluate(id=>jhOpenSymbol(id),id);await wait(id);models[id]=await page.evaluate(()=>JHStockDeskController.getModel());const m=models[id];
   if(id==='FRED:wrong'){assert.equal(m.bars.length,0);assert.equal(m.observations.reason,'warehouse_packet_unavailable');assert.deepEqual(m.observations.transport_cache.attempts[0].rejected_packet,inputs[id]);}
   else if(id==='FRED:invalid'){assert.equal(m.valid,false);assert.equal(m.bars.length,0);assert.equal(m.observations.records.length,2);assert.equal(m.observations.reason,'no_valid_observations');}
   else{assert.deepEqual(m.observations.whole_packet,inputs[id]);assert.equal(m.bars.length,2);}
   if(id==='ECB:invented')assert.deepEqual(m.bars.map(b=>new Date(b.time*1000).toISOString().slice(0,10)),['2024-02-01','2024-03-01']);
   if(!m.bars.length){assert.equal(await page.locator('#wm').textContent(),'');if(realLibrary)assert.equal(await page.evaluate(()=>fixtureCharts[0].watermarkVisible()),false);}
   await snapshot(id.replace(':','-'));
  }
  await page.evaluate(()=>jhOpenSymbol('TVC:US10Y'));await page.waitForFunction(()=>JHStockDeskController.getModel()?.observations.selected_id==='FRED:DGS10');models.alias=await page.evaluate(()=>JHStockDeskController.getModel());
  assert.equal(models.alias.observations.chart_alias?.resolved||models.alias.observations.requested_id,'FRED:DGS10');assert.deepEqual(models.alias.observations.whole_packet,inputs['FRED:DGS10']);
  await page.evaluate(()=>jhOpenSymbol('FRED:slow'));await wait('FRED:slow');models.timeout=await page.evaluate(()=>JHStockDeskController.getModel());assert.equal(models.timeout.observations.transport_cache.last_error.kind,'timeout');assert.equal(models.timeout.bars.length,0);await snapshot('timeout');
  slow=false;for(const release of held)await release();await page.evaluate(()=>{fixtureAdvance(30000);jhSetTf('1d');});await page.waitForFunction(()=>JHStockDeskController.getModel()?.observations.requested_id==='FRED:slow'&&JHStockDeskController.getModel()?.valid);models.timeout_recovery=await page.evaluate(()=>JHStockDeskController.getModel());assert.deepEqual(models.timeout_recovery.observations.whole_packet,inputs['FRED:slow']);
  const forbidden=requests.filter(r=>/\/ohlc|\/yf-ohlc|\/trades|\/aggTrades|\/klines|\/yahoo-fund/.test(r.path)&&/FRED|NYFED|ECB|INVENTED|DGS10|US10Y/i.test(r.query));assert.equal(forbidden.length,0,JSON.stringify(forbidden));assert.equal(errors.length,0,JSON.stringify(errors));
  output.cases.push({width,requests,errors,models,whole_export:exported,screenshots,actual_network_requests:0,scalar_price_requests:0});await context.close();
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:output.cases.map(c=>c.width),actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
