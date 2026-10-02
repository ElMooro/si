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
cases.tiny=rows([...Array(60).fill(100),.1]);cases.allzero=rows(Array(61).fill(0));cases.allmissing=rows(Array(61).fill(null));cases.sparse=rows([...Array(60).fill(null),100]);
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
    raw=raw.slice(0,end)+expose+'window.fixtureOutlier=function(){OSC=[{id:"voldd",on:true,p:50,mult:2,n:"Relative Volume",h:260}];paintOsc(lastBars);};window.fixtureSignedTape=function(value){tape=value;tape.sym=active;quoteUI(lastBars);renderQR();};'+raw.slice(end);return send(raw,'application/javascript');
   }
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(['/ohlc','/yf-ohlc'].includes(u.pathname)){const symbol=u.searchParams.get('ticker')||u.searchParams.get('symbol')||'';const selected=symbol.includes('ALLZERO')?cases.allzero:symbol.includes('ALLMISSING')?cases.allmissing:symbol.includes('SPARSE')?cases.sparse:symbol.includes('ZBASE')?cases.zero_baseline:symbol.includes('TINY')?cases.tiny:symbol.includes('SPIKE')?cases.spike:symbol.includes('MISSING')?cases.missing_last:symbol.includes('ZERO')?cases.zero:symbol.includes('NEGATIVE')?cases.negative:cases.positive;return send({bars:selected,source:'invented:volume-fixture'});}
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  try{
   for(const [name,key,hasHistogram,label] of [['ALLZERO','allzero',true,'Vol 0'],['ALLMISSING','allmissing',false,'Vol Unavailable'],['SPARSE','sparse',true,'Vol 100'],['ZBASE','zero_baseline',true,'Vol 100']]){
    await page.goto('https://invented.justhodl.test/chart.html?s=NASDAQ:'+name);
    await page.waitForFunction(()=>window.lastBars?.length===61&&document.getElementById('quote').textContent.includes('Vol '));
    const view=await page.evaluate(()=>({bars:lastBars,quote:document.getElementById('quote').textContent,source:fixtureState().main,client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));
    assert.deepEqual(view.bars,cases[key]);assert.ok(view.quote.includes(label));assert.ok(view.quote.includes('RVOL Unavailable'));
    const hist=view.source.filter(s=>s.kind==='addHistogramSeries'&&s.options.title==='Volume');assert.equal(hist.length,hasHistogram?1:0);
    if(hasHistogram){assert.equal(hist[0].data.length,61);for(let i=0;i<61;i++){assert.equal(hist[0].data[i].time,cases[key][i].time);assert.equal(hist[0].data[i].value,cases[key][i].volume===null?undefined:cases[key][i].volume);}assert.equal(hist[0].options.lastValueVisible,true);}
    assert.ok(view.scroll<=view.client+1);states[name]=view;const shot=path.join(D,name.toLowerCase()+'-'+width+'.png');await page.screenshot({path:shot});screenshots.push(shot);
   }
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
