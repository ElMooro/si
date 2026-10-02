const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const R=process.env.JH_QA_SOURCE_ROOT||path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const vendor='tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt';
const scripts=['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js','jh-chart-engine.js','jh-chart-indux.js','jh-stock-desk-research.js','jh-chart-stock-desk.js','jh-chart-vol-events.js'];
const html=fs.readFileSync(path.join(R,'chart.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const served=html.replace('</body>','<script src="/fixture-library.js"></script><script src="/fixture-capture.js"></script>'+scripts.map(p=>'<script src="/'+p+'"></script>').join('')+'</body>');
function capture(){
 window.fixtureCharts=[];const lib=window.LightweightCharts;
 window.LightweightCharts=Object.assign({},lib,{createChart(host,opts){
  const chart=lib.createChart(host,opts),state={host,chart,series:[],callbacks:[],removed:false};fixtureCharts.push(state);
  const sub=chart.subscribeCrosshairMove.bind(chart);chart.subscribeCrosshairMove=fn=>{state.callbacks.push(fn);sub(fn);};
  for(const kind of ['addLineSeries','addAreaSeries','addBaselineSeries','addBarSeries','addCandlestickSeries','addHistogramSeries']){
   const add=chart[kind].bind(chart);chart[kind]=options=>{const api=add(options),row={kind,api,data:[]};state.series.push(row);const set=api.setData.bind(api);api.setData=d=>{row.data=d;set(d);};return api;};
  }
  const remove=chart.removeSeries.bind(chart);chart.removeSeries=s=>{state.series=state.series.filter(r=>r.api!==s);return remove(s);};
  const destroy=chart.remove.bind(chart);chart.remove=()=>{state.removed=true;destroy();};return chart;
 }});
 window.fixtureHover=index=>{const state=fixtureCharts[0],bar=lastBars[index],fn=state.callbacks.find(f=>f.toString().includes('var hud='));if(!fn)throw Error('Main hover callback unavailable');fn({time:bar.time,point:{x:20,y:20},seriesData:new Map(state.series.map(r=>[r.api,r.data.find(p=>p.time===bar.time)]))});};
 window.fixtureState=()=>({quote:document.querySelector('#quote').textContent,detail:document.querySelector('#detail').textContent,hud:document.querySelector('#hud').textContent,
  label:document.querySelector('[data-oid=rvol] .osc-v')?.textContent,title:document.querySelector('[data-oid=rvol] .osc-head')?.title,
  oscillators:fixtureCharts.filter(c=>!c.removed&&c.host.closest('[data-oid=rvol]')).map(c=>c.series.map(r=>({kind:r.kind,data:r.data,options:r.api.options()}))),
  main:fixtureCharts[0].series.map(r=>({kind:r.kind,data:r.data,options:r.api.options()}))});
}
const rows=values=>values.map((volume,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i*.1,high:102+i*.1,low:99+i*.1,close:101+i*.1,volume}));
const cases={spike:rows([...Array(60).fill(100),1000]),zero:rows([...Array(60).fill(100),0]),short:rows(Array(20).fill(100)),zero_baseline:rows([...Array(60).fill(0),100]),gap:rows(Array(65).fill(100)),missing_last:rows([...Array(60).fill(100),null])};cases.gap[25].volume=null;
cases.negative=rows(Array(61).fill(100));cases.negative[40].close=cases.negative[40].low;
cases.positive=rows(Array(61).fill(100));cases.positive[40].close=cases.positive[40].high;cases.spike=rows([...Array(60).fill(100),8000]);
cases.tiny=rows([...Array(60).fill(100),.1]);
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Current-source full-module acceptance with complete invented OHLC/volume and tape frames verifies explicit estimated-volume meaning and signed display. The tape hook supplies retained state only; it does not qualify provider adaptation or side assignment. Actual requests are all intercepted; no provider or application data.',whole_inputs:cases,cases:[]};
(async()=>{const browser=await chromium.launch({...(process.env.CHROMIUM_EXECUTABLE_PATH?{executablePath:process.env.CHROMIUM_EXECUTABLE_PATH}:{}),headless:true});try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},hasTouch:width===390,isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage(),requests=[],errors=[],states={},screenshots=[];
  page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(()=>{window.Notification=undefined;});
  await context.route('**/*',route=>{
   const u=new URL(route.request().url());requests.push({host:u.host,path:u.pathname,query:u.search,method:route.request().method()});
   const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
   if(u.host==='invented.justhodl.test'&&u.pathname==='/chart.html')return send(served,'text/html');
   if(u.pathname==='/fixture-library.js')return send(fs.readFileSync(path.join(R,vendor),'utf8'),'application/javascript');
   if(u.pathname==='/fixture-capture.js')return send('('+capture.toString()+')();','application/javascript');
   if(u.pathname==='/jh-chart-engine.js'){
    let raw=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8');const end=raw.lastIndexOf('})();');assert.ok(end>0);
    const expose='window.fixtureBadge={set:function(value){tape=value;if(tape.sym==="SELECTED")tape.sym=active;syncLivePill();},snapshot:function(){syncLivePill();var e=document.getElementById("livepill");return {active:active,age:lastPrintAgeSec(),text:e.textContent,title:e.title,aria:e.getAttribute("aria-label"),className:e.className,liveOn:liveOn,replay:replay.on};}};';
    raw=raw.slice(0,end)+expose+'window.fixtureOutlier=function(){OSC=[{id:"voldd",on:true,p:50,mult:2,n:"Relative Volume",h:260}];paintOsc(lastBars);};window.fixturePatchStable=function(){var initial=[renderLegend,ssRowHtml,openSymSearch];for(var i=0;i<6;i++)ensureChartUi();return initial[0]===renderLegend&&initial[1]===ssRowHtml&&initial[2]===openSymSearch;};window.fixtureDocked=function(){OSC=[{id:"rvol",on:true,p:20,n:"Relative Volume",c:"#2962ff"}];localStorage.setItem("jh-osc-place",JSON.stringify({rvol:"chart"}));paint(lastBars);};window.fixtureSignedTape=function(value){tape=value;tape.sym=active;quoteUI(lastBars);renderQR();};'+raw.slice(end);return send(raw,'application/javascript');
   }
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(['/ohlc','/yf-ohlc'].includes(u.pathname)){const symbol=u.searchParams.get('ticker')||u.searchParams.get('symbol')||'';const selected=symbol.includes('GAP')?cases.gap:symbol.includes('SHORT')?cases.short:symbol.includes('ZBASE')?cases.zero_baseline:symbol.includes('TINY')?cases.tiny:symbol.includes('SPIKE')?cases.spike:symbol.includes('MISSING')?cases.missing_last:symbol.includes('ZERO')?cases.zero:symbol.includes('NEGATIVE')?cases.negative:cases.positive;return send({bars:selected,source:'invented:volume-fixture',...(symbol.includes('ZBASE')?{warehouse_key:'invented/zero-volume-fixture'}:{})});}
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  try{
   for(const name of ['SPIKE','MISSING','ZERO','TINY','GAP','SHORT','ZBASE']){
    await page.goto('https://invented.justhodl.test/chart.html?s=NASDAQ:'+name);
    await page.waitForFunction(()=>window.lastBars?.length>=20&&document.getElementById('quote').textContent.includes('Vol '));
    assert.equal(await page.evaluate(()=>fixturePatchStable()),true,'Repeated UI setup must not wrap callbacks again');
    await page.evaluate(()=>{fixtureDocked();fixtureHover(lastBars.length-1);});
    const view=await page.evaluate(()=>{const s=fixtureCharts[0].series.filter(r=>r.api.options().title==='RVOL');return {bars:lastBars,series:s.map(r=>({data:r.data,options:r.api.options()})),dock:document.querySelector('#legend [data-kind=dock][data-id=rvol]')?.textContent};});
    assert.ok(view.series);assert.deepEqual(view.bars,cases[({SPIKE:'spike',MISSING:'missing_last',ZERO:'zero',TINY:'tiny',GAP:'gap',SHORT:'short',ZBASE:'zero_baseline'})[name]]);
    const runs=[];let run=[];for(let i=0;i<view.bars.length;i++){const b=view.bars[i],prior=view.bars.slice(Math.max(0,i-20),i);let expected;if(prior.length===20&&typeof b.volume==='number'&&prior.every(p=>typeof p.volume==='number')){const mean=prior.reduce((n,p)=>n+p.volume,0)/20;if(mean>0)expected=b.volume/mean;}if(expected===undefined){if(run.length)runs.push(run);run=[];}else run.push({time:b.time,value:expected});}if(run.length)runs.push(run);
    assert.deepEqual(view.series.map(s=>s.data),runs);for(const s of view.series)assert.equal(s.options.lastValueVisible,s.data.at(-1).time===view.bars.at(-1).time);
    const location=await page.evaluate(()=>{const rows=document.querySelectorAll('#legend [data-kind=dock][data-id=rvol]'),row=rows[0];return {count:rows.length,parent:row?.parentNode.id,stack:row?.parentNode.parentNode.id,label:row?.querySelector('.leg-v').textContent,title:row?.title,legendBottom:document.getElementById('legend').getBoundingClientRect().bottom,hudTop:document.getElementById('hud').getBoundingClientRect().top};});
    assert.equal(location.count,1);assert.equal(location.parent,'legend');assert.equal(location.stack,'chart-readouts');assert.ok(location.legendBottom<=location.hudTop+1);
    const latest=runs.at(-1)?.at(-1),n=latest?.time===view.bars.at(-1).time?latest.value:null;assert.equal(location.label,n===null?'Unavailable':(n>0&&n<.01?'<0.01':n.toFixed(2))+'×');assert.ok(location.title.includes('prior 20-bar mean'));view.location=location;
    if(name==='GAP'){await page.evaluate(()=>fixtureHover(25));assert.equal(await page.locator('#legend [data-kind=dock][data-id=rvol] .leg-v').textContent(),'Unavailable');await page.evaluate(()=>fixtureHover(lastBars.length-1));}
    const shot=path.join(D,name.toLowerCase()+'-'+width+'.png');await page.screenshot({path:shot});screenshots.push(shot);
    await page.locator('#legend [data-kind=dock][data-id=rvol] [data-act=dn]').click();await page.waitForSelector('[data-oid=rvol]');
    const below=await page.evaluate(()=>({placement:JSON.parse(localStorage.getItem('jh-osc-place')||'{}').rvol,series:fixtureCharts.filter(c=>!c.removed&&c.host.closest('[data-oid=rvol]')).flatMap(c=>c.series.filter(s=>s.api.options().title==='Relative Volume').map(s=>s.data))}));
    assert.equal(below.placement,undefined);assert.deepEqual(below.series,view.series.map(s=>s.data));view.returned_below=true;states[name]=view;
   }
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
