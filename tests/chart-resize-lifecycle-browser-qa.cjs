const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const vendor='tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt';
const scripts=['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js','jh-chart-engine.js','jh-chart-indux.js','jh-stock-desk-research.js','jh-chart-stock-desk.js','jh-chart-vol-events.js'];
const html=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const served=html.replace('</body>','<script src="/fixture-library.js"></script><script src="/fixture-capture.js"></script>'+scripts.map(p=>'<script src="/'+p+'"></script>').join('')+'</body>');
function capture(){
 window.fixtureCharts=[];window.fixtureResizeCalls=[];const lib=window.LightweightCharts;
 window.LightweightCharts=Object.assign({},lib,{createChart(host,opts){
  const chart=lib.createChart(host,opts),state={host,chart,series:[],removed:false};fixtureCharts.push(state);
  for(const kind of ['addLineSeries','addAreaSeries','addBaselineSeries','addBarSeries','addCandlestickSeries','addHistogramSeries']){
   const add=chart[kind].bind(chart);chart[kind]=options=>{const api=add(options),row={kind,api,data:[]};state.series.push(row);const set=api.setData.bind(api);api.setData=d=>{row.data=d;set(d);};return api;};
  }
  const remove=chart.removeSeries.bind(chart);chart.removeSeries=s=>{state.series=state.series.filter(r=>r.api!==s);return remove(s);};
  const resize=chart.resize.bind(chart);chart.resize=(w,h)=>{window.fixtureResizeCalls.push({chart:fixtureCharts.indexOf(state),removed:state.removed,width:w,height:h});return resize(w,h);};
  const destroy=chart.remove.bind(chart);chart.remove=()=>{state.removed=true;destroy();};return chart;
 }});
 window.fixtureState=()=>fixtureCharts.filter(c=>!c.removed).map(c=>({host:c.host.id,series:c.series.map(r=>({kind:r.kind,data:r.data,options:r.api.options()}))}));
}
const packets={
 'FRED:invented_gap':{id:'FRED:invented_gap',freq:'D',unit:'invented',provider:'FRED',obs:[['2026-01-01',0],['2026-01-02',null],['2026-01-03',2],['2026-01-04',3],['2026-01-04',4],['2026-01-05',-1],['invalid',7]]},
 'FRED:invented_single':{id:'FRED:invented_single',freq:'D',unit:'invented',provider:'FRED',obs:[['2026-01-01',0]]}
};
packets['FRED:invented_precision']={id:'FRED:invented_precision',freq:'D',unit:'invented',provider:'FRED',obs:[['2026-01-01',-0.0000000123456789],['2026-01-02',0],['2026-01-03',1.4000000000000001],['2026-01-04',1.2345678901234567]]};
const bars=Array.from({length:41},(_,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i,high:102+i,low:99+i,close:101+i,volume:100}));
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Whole chart HTML and nine selected modules with pinned real 4.2.3 drawing library; all requests intercepted, complete invented frames. Main, navigator and split source points, plus market recovery. No actual provider or application data. Resize calls on removed charts are observed; an exposed original mkChart creates/removes a real chart with pending callbacks, without replacing the computation or lifecycle.',whole_packets:packets,whole_market_bars:bars,cases:[]};
const points=row=>{assert.equal(row.kind,'addLineSeries');assert.equal(row.options.lineVisible,false);assert.equal(row.options.pointMarkersVisible,true);assert.ok(row.options.pointMarkersRadius>0);assert.equal(row.options.priceLineVisible,false);};
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},hasTouch:width===390,isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage(),requests=[],errors=[],states={},screenshots=[];
  page.on('pageerror',e=>errors.push({message:e.message,stack:e.stack}));await context.addInitScript(()=>{window.Notification=undefined;});
  await context.route('**/*',route=>{
   const u=new URL(route.request().url());requests.push({host:u.host,path:u.pathname,query:u.search,method:route.request().method()});
   const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
   if(u.host==='invented.justhodl.test'&&u.pathname==='/chart.html')return send(served,'text/html');
   if(u.pathname==='/fixture-library.js')return send(fs.readFileSync(path.join(R,vendor),'utf8'),'application/javascript');
   if(u.pathname==='/fixture-capture.js')return send('('+capture.toString()+')();','application/javascript');
   if(u.pathname==='/jh-chart-engine.js'){let raw=fs.readFileSync(process.argv.includes('--predecessor')?path.join(R,'tests/fixtures/chart-refresh-status/pre509/jh-chart-engine.js.txt'):path.join(R,'jh-chart-engine.js'),'utf8');const end=raw.lastIndexOf('})();');assert.ok(end>0);raw=raw.slice(0,end)+'window.fixtureMkChart=mkChart;'+raw.slice(end);return send(raw,'application/javascript');}
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(u.pathname==='/series'&&packets[u.searchParams.get('id')])return send(packets[u.searchParams.get('id')]);
   if(['/ohlc','/yf-ohlc'].includes(u.pathname))return send({bars,source:'invented:market-frame'});
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  const wait=async id=>page.waitForFunction(id=>JHStockDeskController.getModel()?.observations?.requested_id===id,id,{timeout:15000});
  const shot=async name=>{await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));const p=path.join(D,name+'-'+width+'.png');await page.screenshot({path:p});screenshots.push(p);};
  try{
   await page.goto('https://invented.justhodl.test/chart.html?s=FRED:invented_gap');await wait('FRED:invented_gap');
   states.initial=await page.evaluate(()=>({drawing:fixtureState(),model:JHStockDeskController.getModel()}));
   const main=states.initial.drawing[0].series[0];points(main);assert.deepEqual(main.data.map(p=>p.value),[0,2,-1]);assert.deepEqual(states.initial.model.observations.whole_packet,packets['FRED:invented_gap']);assert.equal(states.initial.model.observations.records.length,7);
   assert.match(await page.locator('#btn-kind').textContent(),/Source points/);await shot('gapped-source');
   states.tick_labels=await page.evaluate(()=>{const c=fixtureCharts.find(c=>!c.removed&&c.host.id!=='mini'),f=c.chart.options().localization.priceFormatter;return [1.4000000000000001,1.2000000000000002,-2.7755575615628914e-17,-0.39999999999999974,0,2,-1].map(value=>({value,label:f(value)}));});
   assert.deepEqual(states.tick_labels.map(x=>x.label),['1.4','1.2','0','-0.4','0','2','-1']);

   await page.evaluate(()=>document.getElementById('btn-mini').click());states.navigator=await page.evaluate(()=>fixtureState());const mini=states.navigator.find(c=>c.host==='mini');assert.ok(mini);points(mini.series[0]);assert.deepEqual(mini.series[0].data,main.data);await shot('navigator');
   await page.evaluate(()=>jhOpenSymbol('FRED:invented_single'));await wait('FRED:invented_single');states.single=await page.evaluate(()=>fixtureState());points(states.single[0].series[0]);assert.deepEqual(states.single[0].series[0].data.map(p=>p.value),[0]);await shot('single-zero');
   // Use the actual tab-close and layout handlers to isolate the two sources.
   await page.evaluate(()=>{Array.from(document.querySelectorAll('#tabs .tab[data-id]')).filter(n=>!n.dataset.id.startsWith('FRED:')).forEach(n=>n.querySelector('[data-x]')?.click());jhOpenSymbol('FRED:invented_gap');});await wait('FRED:invented_gap');
   await page.evaluate(()=>jhSetLayout(2));await page.waitForFunction(()=>fixtureState().filter(c=>c.host!=='mini').length===2&&fixtureState().filter(c=>c.host!=='mini')[1].series.length===1);
   states.split=await page.evaluate(()=>fixtureState());const split=states.split.filter(c=>c.host!=='mini')[1].series[0];points(split);assert.deepEqual(split.data.map(p=>p.value),[0]);await shot('split-single-zero');
   await page.evaluate(()=>{jhSetLayout(1);jhOpenSymbol('SPY');});await page.waitForFunction(()=>window.lastBars?.length===41&&!window.jhChartEvidence?.observations);states.market=await page.evaluate(()=>fixtureState());assert.equal(states.market.find(c=>c.host==='mini').series[0].kind,'addAreaSeries');
   await page.evaluate(()=>jhSetLayout(2));await page.waitForFunction(()=>fixtureCharts.filter(c=>!c.removed&&c.host.id!=='mini').length===2&&fixtureCharts.filter(c=>!c.removed&&c.host.id!=='mini')[1].series.length===1);
   await page.evaluate(async()=>{document.getElementById('btn-md').click();document.querySelector('#menu [data-m="ytd"]').click();await paint(lastBars);});
   const paneLabels=()=>{const c=fixtureCharts.filter(c=>!c.removed&&c.host.id!=='mini')[1];return{title:c.series[0].api.options().title,labels:[-1,0,2].map(value=>({value,label:c.chart.options().localization.priceFormatter(value)})),data:c.series[0].data};};
   states.theme_before=await page.evaluate(paneLabels);assert.deepEqual(states.theme_before.labels.map(x=>x.label),['-1','0','2']);
   await page.evaluate(()=>document.getElementById('btn-theme').click());states.theme_after=await page.evaluate(paneLabels);
   assert.deepEqual(states.theme_after.labels.map(x=>x.label),['-1','0','2']);assert.deepEqual(states.theme_after.data,states.theme_before.data);await shot('theme-source-units');
   const marketLabel=await page.evaluate(()=>fixtureCharts.find(c=>!c.removed&&c.host.id!=='mini').chart.options().localization.priceFormatter(2));assert.equal(marketLabel,'+2.00%');
   await page.evaluate(()=>document.getElementById('btn-theme').click());assert.deepEqual((await page.evaluate(paneLabels)).labels.map(x=>x.label),['-1','0','2']);
   await page.evaluate(()=>jhSetLayout(1));
   await page.evaluate(()=>jhOpenSymbol('FRED:invented_gap'));await wait('FRED:invented_gap');states.recovered=await page.evaluate(()=>fixtureState());points(states.recovered[0].series[0]);points(states.recovered.find(c=>c.host==='mini').series[0]);assert.deepEqual(states.recovered[0].series[0].data,main.data);await shot('recovered-source');
   await page.evaluate(()=>jhOpenSymbol('FRED:invented_precision'));await wait('FRED:invented_precision');
   states.precision=await page.evaluate(()=>{const c=fixtureCharts.find(c=>!c.removed&&c.host.id!=='mini'),r=c.series[0];return{data:r.data,labels:r.data.map(p=>({value:p.value,axis:c.chart.options().localization.priceFormatter(p.value),series:r.api.options().priceFormat.formatter(p.value)})),model:JHStockDeskController.getModel()};});
   assert.deepEqual(states.precision.data.map(x=>x.value),packets['FRED:invented_precision'].obs.map(x=>x[1]));for(const x of states.precision.labels){assert.equal(x.axis,String(x.value));assert.equal(x.series,String(x.value));}assert.deepEqual(states.precision.model.observations.whole_packet,packets['FRED:invented_precision']);await shot('exact-source-precision');
   await page.evaluate(()=>jhOpenSymbol('SPY'));await page.waitForFunction(()=>window.lastBars?.length===41&&!window.jhChartEvidence?.observations);
   states.final_market=await page.evaluate(()=>{const c=fixtureCharts.find(c=>!c.removed&&c.host.id!=='mini');return{label:c.chart.options().localization.priceFormatter(2),drawing:fixtureState()};});assert.equal(states.final_market.label,'+2.00%');await shot('market-unit-recovery');
   await page.evaluate(()=>{const host=document.createElement('div');host.id='fixture-retired-host';host.style.cssText='width:500px;height:300px';document.body.appendChild(host);const chart=fixtureMkChart(host);chart.remove();window.dispatchEvent(new Event('resize'));});await page.waitForTimeout(350);
   states.resize_calls=await page.evaluate(()=>fixtureResizeCalls);assert.deepEqual(states.resize_calls.filter(x=>x.removed),[]);
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,resize_calls:await page.evaluate(()=>fixtureResizeCalls),current:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
