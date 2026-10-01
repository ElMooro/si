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
const output={scope:'Complete selected chart modules with pinned real drawing library. All requests intercepted. Complete invented market frames; raw provider acquisition/defaulting not qualified.',whole_inputs:cases,cases:[]};
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
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(['/ohlc','/yf-ohlc'].includes(u.pathname))return send({bars:cases.spike,source:'invented:volume-fixture'});
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  try{
   await page.goto('https://invented.justhodl.test/chart.html?s=NASDAQ:INVENTED');await page.waitForFunction(()=>window.lastBars?.length===41,{timeout:15000});
   await page.evaluate(()=>{INDS.forEach(x=>x.on=false);OSC.forEach(x=>x.on=x.id==='rvol');});
   const draw=async(name)=>{await page.evaluate(async bars=>{await paint(bars);fixtureHover(bars.length-1);},cases[name]);const state=await page.evaluate(()=>fixtureState());states[name]=state;return state;};
   let s=await draw('spike');assert.match(s.quote,/RVOL 10\.00x/);assert.match(s.hud,/RVOL 10\.00x/);assert.equal(s.label,'10.00x');assert.match(s.title,/preceding 20/);
   assert.equal(s.oscillators.length,1);let actual=s.oscillators[0].filter(r=>/^RVOL/.test(r.options.title));assert.equal(actual.at(-1).data.at(-1).value,10);assert.equal(actual.at(-1).options.lastValueVisible,true);
   assert.deepEqual(s.oscillators[0].filter(r=>/prior mean/.test(r.options.title)).map(r=>r.options.title),['1x prior mean','2x prior mean']);
   const shot=async name=>{await page.waitForFunction(()=>{const h=document.querySelector('[data-oid=rvol] .osc-host'),c=h?.querySelector('canvas');return h?.clientHeight>50&&c?.height>50;});await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));const p=path.join(D,name+'-'+width+'.png');await page.screenshot({path:p});screenshots.push(p);};await shot('spike');
   s=await draw('zero');assert.match(s.quote,/RVOL 0\.00x/);assert.match(s.hud,/RVOL 0\.00x/);assert.equal(s.label,'0.00x');assert.equal(s.oscillators[0].filter(r=>/^RVOL/.test(r.options.title)).at(-1).data.at(-1).value,0);await shot('zero');
   for(const name of ['short','zero_baseline','missing_last']){s=await draw(name);assert.match(s.quote,/RVOL Unavailable/);assert.match(s.hud,/RVOL Unavailable/);assert.equal(s.label,'Unavailable');assert.ok(s.oscillators[0].every(r=>r.options.lastValueVisible===false));await shot(name);}
   s=await draw('gap');actual=s.oscillators[0].filter(r=>/^RVOL/.test(r.options.title));assert.equal(actual.length,2);assert.deepEqual(actual.map(r=>[r.data[0].time,r.data.at(-1).time]),[[cases.gap[20].time,cases.gap[24].time],[cases.gap[46].time,cases.gap[64].time]]);assert.equal(actual[0].options.lastValueVisible,false);assert.equal(actual[1].options.lastValueVisible,true);await shot('gap');
   await page.evaluate(()=>fixtureHover(25));assert.match((await page.evaluate(()=>fixtureState())).hud,/RVOL Unavailable/);
   await page.evaluate(()=>fixtureHover(19));assert.match((await page.evaluate(()=>fixtureState())).hud,/RVOL Unavailable/);
   await page.evaluate(()=>jhSetKind('volcandle'));s=await draw('missing_last');let candles=s.main.find(r=>r.kind==='addCandlestickSeries');assert.ok(candles);assert.equal(candles.data.at(-1).color,candles.options.upColor);assert.equal(candles.data[0].color,candles.options.upColor);states.volume_candles_missing=s;await shot('volume-candles-missing');
   s=await draw('zero');candles=s.main.find(r=>r.kind==='addCandlestickSeries');assert.notEqual(candles.data.at(-1).color,candles.options.upColor);states.volume_candles_zero=s;
   await page.evaluate(async()=>{INDS.find(x=>x.id==='voltape').on=true;await paint(lastBars);});
   assert.match(await page.locator('.annotation-timing').textContent(),/not first availability/);
   await page.locator('[data-annotation-timing]').click();assert.match(await page.locator('#indhelp').textContent(),/First availability is unknown/);await shot('annotation-timing');
   await page.keyboard.press('Escape');assert.notEqual(await page.locator('#indhelp').getAttribute('class'),'on');
   await page.evaluate(async()=>{INDS.find(x=>x.id==='voltape').on=false;await paint(lastBars);});assert.equal(await page.locator('.annotation-timing').count(),0);
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
