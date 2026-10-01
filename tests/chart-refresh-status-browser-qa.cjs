const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
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
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Current-source full-module refresh-status acceptance with pinned real drawing library and complete invented states. All network intercepted. Test hook supplies retained tape state without qualifying provider adapters. Real settings/replay handlers and focus are exercised; toolbar discoverability on narrow layouts remains unqualified.',whole_inputs:cases,cases:[]};
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{
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
    raw=raw.slice(0,end)+expose+raw.slice(end);return send(raw,'application/javascript');
   }
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(['/ohlc','/yf-ohlc'].includes(u.pathname))return send({bars:(u.searchParams.get('ticker')||u.searchParams.get('symbol')||'').includes('MISSING')?cases.missing_last:cases.spike,source:'invented:volume-fixture'});
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  try{
   await page.goto('https://invented.justhodl.test/chart.html?s=NASDAQ:INVENTED');await page.waitForFunction(()=>window.lastBars?.length===41&&document.getElementById('quote').textContent.includes('RVOL 10.00x'));
   const read=()=>page.evaluate(()=>fixtureBadge.snapshot()),shot=async name=>{const p=path.join(D,name+'-'+width+'.png');await page.screenshot({path:p});screenshots.push(p);};
   let status=await read();assert.equal(status.text.trim(),'AUTO');assert.equal(status.age,null);states.initial=status;await shot('refresh-initial');
   for(const [name,other,future] of [['different-symbol',true,false],['future-clock',false,true],['reported-entry',false,false]]){
    await page.evaluate(({other,future})=>fixtureBadge.set({sym:other?'OTHER':'SELECTED',src:'invented-fixture',prints:[{t:Date.now()+(future?60000:-30000),px:105,sz:10,side:'buy'}]}),{other,future});
    status=await read();assert.equal(status.text.trim(),'AUTO');if(other||future){assert.equal(status.age,null);assert.match(status.title,/No usable timestamped tape entry/);}else{assert.ok(status.age>=30&&status.age<35);assert.match(status.title,/reported timestamp/);}states[name]=status;
   }
   await page.locator('#livepill').focus();assert.equal(await page.evaluate(()=>document.activeElement.id),'livepill');assert.match(await page.locator('#livepill').getAttribute('aria-label'),/do not establish real-time chart prices/);await shot('refresh-reported-entry');
   // Exercise the existing settings handler directly: its toolbar discovery on
   // narrow layouts is not claimed by this status-label acceptance.
   await page.locator('#btn-set').evaluate(el=>el.click());assert.match(await page.locator('#mbox').textContent(),/Auto refresh/);
   await page.locator('#s-live').uncheck();await page.locator('#setok').click();status=await read();assert.equal(status.text.trim(),'PAUSED');assert.equal(status.className,'');states.paused=status;await shot('refresh-paused');
   await page.locator('#btn-set').evaluate(el=>el.click());await page.locator('#s-live').check();await page.locator('#setok').click();
   await page.locator('#btn-rep').evaluate(el=>el.click());await page.waitForFunction(()=>document.getElementById('livepill').textContent.includes('REPLAY'));status=await read();assert.equal(status.text.trim(),'REPLAY');assert.equal(status.className,'');states.replay=status;await shot('refresh-replay');
   await page.locator('#rp-exit').click();await page.waitForFunction(()=>document.getElementById('livepill').textContent.includes('AUTO'));states.recovered=await read();assert.equal(states.recovered.text.trim(),'AUTO');
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
