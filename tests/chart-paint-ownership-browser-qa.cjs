const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const rejectOld=process.argv.includes('--reject-old'),calendarCase=process.argv.includes('--calendar');
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
const cases={spike:rows([...Array(40).fill(100),1000]),zero:rows([...Array(40).fill(100),0]),short:rows(Array(20).fill(100)),zero_baseline:rows([...Array(40).fill(0),100]),gap:rows(Array(65).fill(100)),missing_last:rows([...Array(40).fill(100),null])};cases.gap[25].volume=null;
cases.missing_last.forEach(b=>{for(const key of ['open','high','low','close'])b[key]+=200;});
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Complete selected production modules, real pinned drawing library and complete invented packets. Hold the first benchmark response, complete a different symbol, then release the old response. Current source acceptance. Completion instrumentation records paint settlement without replacing any computation or changing awaited responses.',whole_inputs:cases,cases:[]};
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},hasTouch:width===390,isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage(),requests=[],errors=[],states={},screenshots=[];
  let pendingBenchmark=null;page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(()=>{window.Notification=undefined;});
  await context.route('**/*',route=>{
   const u=new URL(route.request().url());requests.push({host:u.host,path:u.pathname,query:u.search,method:route.request().method()});
   const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
   if(u.host==='invented.justhodl.test'&&u.pathname==='/chart.html')return send(served,'text/html');
   if(u.pathname==='/fixture-library.js')return send(fs.readFileSync(path.join(R,vendor),'utf8'),'application/javascript');
   if(u.pathname==='/fixture-capture.js')return send('('+capture.toString()+')();','application/javascript');
   if(u.pathname==='/jh-chart-engine.js'){
    const parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
    let raw=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8');
    const body=parser.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
    const fn=body.find(n=>n.type==='FunctionDeclaration'&&n.id.name==='paint');
    raw=raw.slice(0,fn.body.end-1)+'}finally{(window.fixtureSettled||(window.fixtureSettled=[])).push({symbol:fixtureSymbol,volume:d.at(-1)?.volume});}'+raw.slice(fn.body.end-1);
    raw=raw.slice(0,fn.body.start+1)+'var fixtureSymbol=active;try{'+raw.slice(fn.body.start+1);
    return send(raw,'application/javascript');
   }
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(!calendarCase&&['/ohlc','/yf-ohlc'].includes(u.pathname)&&!pendingBenchmark&&(u.searchParams.get('ticker')||u.searchParams.get('symbol')||'')==='^GSPC'){pendingBenchmark=route;return;}
   if(calendarCase&&u.pathname==='/data/catalyst-calendar.json'&&!pendingBenchmark){pendingBenchmark=route;return;}
   if(calendarCase&&u.pathname==='/data/catalyst-calendar.json')return send({events:[]});
   if(calendarCase&&u.pathname.includes('earnings'))return send({recent_results_30d:[],forward_calendar:[],upcoming_14d:[]});
   if(['/ohlc','/yf-ohlc'].includes(u.pathname))return send({bars:(u.searchParams.get('ticker')||u.searchParams.get('symbol')||'').includes('MISSING')?cases.missing_last:cases.spike,source:'invented:volume-fixture'});
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  try{
   await page.goto('https://invented.justhodl.test/chart.html?s=NASDAQ:INVENTED');
   await page.waitForFunction(()=>window.lastBars?.length===41,{timeout:15000});
   if(calendarCase){await page.waitForFunction(()=>document.getElementById('quote').textContent.includes('RVOL 10.00x'));await page.evaluate(()=>{window.fixtureSettled=[];INDS.find(i=>i.id==='earn').on=true;void paint(lastBars);});}
   for(let i=0;!pendingBenchmark&&i<150;i++)await new Promise(resolve=>setTimeout(resolve,100));assert.ok(pendingBenchmark,'Initial benchmark request reached controlled barrier');
   states.old_paint_waiting=await page.evaluate(()=>({bars:lastBars,view:fixtureState()}));
   await page.evaluate(()=>jhOpenSymbol('MISSING'));
   await page.waitForFunction(()=>window.lastBars?.length===41&&lastBars[40].volume===null&&document.getElementById('quote').textContent.startsWith('▾ MISSING')&&document.getElementById('quote').textContent.includes('Vol Unavailable'));
   states.new_symbol_completed=await page.evaluate(()=>({bars:lastBars,view:fixtureState()}));
   assert.equal(states.new_symbol_completed.bars[40].volume,null);
   if(rejectOld)await pendingBenchmark.abort('failed');else await pendingBenchmark.fulfill({status:200,contentType:'application/json',body:JSON.stringify(calendarCase?{events:[]}:{bars:cases.spike,source:'invented:delayed-prior-benchmark'})});
   await page.waitForFunction(()=>window.fixtureSettled?.some(r=>r.symbol==='NASDAQ:INVENTED')); 
   states.obsolete_paint_settled=await page.evaluate(()=>({bars:lastBars,view:fixtureState()}));
   assert.match(states.obsolete_paint_settled.view.quote,/MISSING/);assert.equal(states.obsolete_paint_settled.view.quote,states.new_symbol_completed.view.quote);assert.deepEqual(states.obsolete_paint_settled,states.new_symbol_completed);assert.equal(states.obsolete_paint_settled.bars[40].volume,null);
   const p=path.join(D,'retained-new-paint-'+width+'.png');await page.screenshot({path:p});screenshots.push(p);
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
