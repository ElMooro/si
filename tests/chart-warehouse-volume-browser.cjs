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
const rows=volume=>Array.from({length:61},(_,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i*.1,high:102+i*.1,low:99+i*.1,close:101+i*.1,value:i===60?12345:100,volume:i===60?volume:100}));
const cases={conflict:rows(100),missing:rows(null),zero:rows(0),equal:rows(107),crypto:rows(8000),supplement:rows(50)};
cases.conflict.at(-1).v=200;delete cases.equal.at(-1).volume;cases.equal.at(-1).value=cases.equal.at(-1).close;
cases.crypto[0].volume=0;cases.crypto[1].volume=null;
cases.supplement=cases.supplement.map((b,i)=>({...b,open:80+i*.1,high:82+i*.1,low:79+i*.1,close:81+i*.1}));
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Complete actual chart modules with invented warehouse and crypto supplementary packets. All requests intercepted; no live data or provider requests. Unit identity and cross-provider comparability remain unqualified.',whole_inputs:cases,cases:[]};
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
   if(['/ohlc','/yf-ohlc'].includes(u.pathname)){
    const symbol=u.searchParams.get('ticker')||u.searchParams.get('symbol')||'';
    if(symbol.includes('BTC'))return send({bars:u.searchParams.get('nowarehouse')==='1'?cases.supplement:cases.crypto,source:'warehouse',warehouse_key:'invented/crypto-bars/BTC'});
    const selected=symbol.includes('CONFLICT')?cases.conflict:symbol.includes('MISSING')?cases.missing:symbol.includes('EQUAL')?cases.equal:cases.zero;
    return send({bars:selected,source:'warehouse',warehouse_key:'invented/fixture'});
   }
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  try{
   for(const [name,expected] of [['NASDAQ:CONFLICT',null],['NASDAQ:MISSING',null],['NASDAQ:ZERO',0],['NASDAQ:EQUAL',107],['BTCUSDT',8000]]){
    await page.goto('https://invented.justhodl.test/chart.html?s='+encodeURIComponent(name));
    await page.waitForFunction(()=>window.lastBars?.length===61&&document.getElementById('quote').textContent.includes('Vol '));
    await page.evaluate(()=>fixtureOutlier());
    const view=await page.evaluate(()=>({bars:window.lastBars,quote:document.querySelector('#quote').textContent,label:document.querySelector('[data-oid=voldd] .osc-v').textContent,client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));
    assert.equal(view.bars.at(-1).volume,expected);assert.ok(view.quote.includes('Vol '+(expected===null?'Unavailable':expected===8000?'8.0K':String(expected))));
    if(name==='BTCUSDT'){assert.equal(view.bars[0].volume,0);assert.equal(view.bars[1].volume,null);assert.equal(view.bars[0].close,cases.crypto[0].close);assert.equal(view.bars[1].close,cases.crypto[1].close);assert.ok(view.label.includes('80.00×'));}
    assert.ok(view.scroll<=view.client+1);states[name]=view;const shot=path.join(D,name.replace(':','-').toLowerCase()+'-'+width+'.png');await page.screenshot({path:shot});screenshots.push(shot);
   }
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
