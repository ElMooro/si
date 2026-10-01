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
  label:document.querySelector('[data-oid=obv] .osc-v')?.textContent,title:document.querySelector('[data-oid=obv] .osc-head')?.title,
  oscillators:fixtureCharts.filter(c=>!c.removed&&c.host.closest('[data-oid=obv]')).map(c=>c.series.map(r=>({kind:r.kind,data:r.data,options:r.api.options()}))),
  main:fixtureCharts[0].series.map(r=>({kind:r.kind,data:r.data,options:r.api.options()}))});
}
const rows=values=>values.map((volume,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i*.1,high:102+i*.1,low:99+i*.1,close:101+i*.1,volume}));
const make=(closes,volumes)=>closes.map((close,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:close,high:close+1,low:close-1,close,volume:volumes[i]}));
const cases={spike:make(Array.from({length:41},(_,i)=>100+i),Array(41).fill(100)),flat:make(Array(41).fill(100),Array(41).fill(100)),mixed:make([100,101,101,99,99,102],[100,200,999,300,999,50]),missing:make(Array.from({length:41},(_,i)=>100+i),Array(41).fill(100)),zero:make(Array.from({length:41},(_,i)=>100+i),Array(41).fill(0)),anchor:make([100],[99999])};cases.missing[25].volume=null;
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Complete selected chart modules with pinned real drawing library. All requests intercepted. Complete invented market frames; OBV zero anchor, flat closes and withheld cumulative chain. Raw provider defaulting not qualified.',whole_inputs:cases,cases:[]};
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
   await page.evaluate(()=>{INDS.forEach(x=>x.on=false);OSC.forEach(x=>x.on=x.id==='obv');});
   const draw=async(name)=>{await page.evaluate(async bars=>{await paint(bars);fixtureHover(bars.length-1);},cases[name]);const state=await page.evaluate(()=>fixtureState());states[name]=state;return state;};
   const shot=async name=>{await page.waitForFunction(()=>{const h=document.querySelector('[data-oid=obv] .osc-host'),c=h?.querySelector('canvas');return h?.clientHeight>50&&c?.height>50;});await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));const p=path.join(D,name+'-'+width+'.png');await page.screenshot({path:p});screenshots.push(p);};
   let s=await draw('flat');assert.equal(s.label,'0.00');assert.match(s.title,/Zero anchor/);assert.equal(s.oscillators.length,1);assert.ok(s.oscillators[0][0].data.every(p=>p.value===0));await shot('flat');
   s=await draw('mixed');assert.deepEqual(s.oscillators[0][0].data.map(p=>p.value),[0,200,200,-100,-100,-50]);assert.equal(s.label,'-50.00');await shot('mixed');
   s=await draw('missing');assert.equal(s.label,'Unavailable');assert.equal(s.oscillators[0][0].options.lastValueVisible,false);assert.equal(s.oscillators[0][0].data.length,41);assert.equal(s.oscillators[0][0].data[24].value,2400);assert.ok(s.oscillators[0][0].data.slice(25).every(p=>p.value===undefined));await shot('missing');
   for(const name of ['zero','anchor']){s=await draw(name);assert.equal(s.label,'0.00');assert.ok(s.oscillators[0][0].data.every(p=>p.value===0));assert.equal(s.oscillators[0][0].options.lastValueVisible,true);await shot(name);}
   s=await draw('spike');assert.equal(s.oscillators[0][0].data.at(-1).value,4000);assert.equal(s.oscillators[0][0].options.lastValueVisible,true);await shot('recovered-frame');
   assert.deepEqual(errors,[]);output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
