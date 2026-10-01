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
cases.negative=rows(Array(41).fill(100));cases.negative[40].close=cases.negative[40].low;
cases.positive=rows(Array(41).fill(100));cases.positive[40].close=cases.positive[40].high;
const metadata={'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
const output={scope:'Current-source escaping acceptance on an invented host, complete local modules and invented market packets. Test payload only sets a DOM marker; all requests intercepted. No actual site requests or application data.',whole_inputs:cases,cases:[]};
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
    const labelHook=`window.fixtureHtml={render:function(value){active=value.symbol;TABS=[active];favs=[active];quotes[active]={last:12,chg:0,chgv:0};lists=[{id:value.symbol,name:value.symbol,symbols:[value.symbol],n:value.symbol}];listId=value.symbol;notes={};notes[active]={text:value.note,at:value.note};flags={};flags[active]="red' onmouseover='void(0)";alerts=[{id:value.symbol,sym:value.symbol,price:0}];renderTabs();renderFavs();renderList();document.getElementById('q').value='svg';renderHits();renderNotes();renderAlerts();},state:function(){return {active:active,lists:lists,notes:notes,alerts:alerts};}};`;
    raw=raw.slice(0,end)+expose+labelHook+raw.slice(end);return send(raw,'application/javascript');
   }
   if(u.pathname==='/jh-chart-indux.js')return send(fs.readFileSync(path.join(R,'jh-chart-indux.js'),'utf8'),'application/javascript');
   if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
   if(['/ohlc','/yf-ohlc'].includes(u.pathname))return send({bars:(u.searchParams.get('ticker')||u.searchParams.get('symbol')||'').includes('NEGATIVE')?cases.negative:cases.positive,source:'invented:volume-fixture'});
   if(Object.hasOwn(metadata,u.pathname))return send(metadata[u.pathname]);
   if(u.pathname==='/symsearch')return send({rows:[],facets:[]});if(u.pathname==='/tv-search')return send({symbols:[]});if(u.pathname==='/api/yahoo-search')return send({quotes:[]});
   return route.abort('blockedbyclient');
  });
  try{
   const code="document.documentElement.setAttribute('data-fixture-html','yes')",encoded=Array.from(code,c=>'&#'+c.charCodeAt(0)+';').join(''),symbol='<svg onload="'+encoded+'"></svg>';
   for(const route of ['?s=','#s=']){
    await page.goto('https://invented.justhodl.test/chart.html'+route+encodeURIComponent(symbol));await page.waitForFunction(()=>window.lastBars?.length===41);await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    const snapshot=()=>page.evaluate(()=>({marker:document.documentElement.getAttribute('data-fixture-html'),injected_svg_count:document.querySelectorAll('svg[onload]').length,quote:document.getElementById('quote')?.textContent,tabs:Array.from(document.querySelectorAll('#tabs .tab[data-id]'),x=>({id:x.dataset.id,text:x.textContent})),legend:document.getElementById('legend')?.textContent}));
    const result=await snapshot();assert.equal(result.marker,null);assert.equal(result.injected_svg_count,0);assert.match(result.quote,/<SVG ONLOAD=/);assert.match(result.legend,/<SVG ONLOAD=/);assert.ok(result.tabs.some(x=>x.id===symbol.toUpperCase()));states[route]={input:symbol,result};
    await page.evaluate(()=>jhOpenSymbol('SPY'));await page.waitForFunction(()=>document.getElementById('quote').textContent.includes('▾ SPY'));let recovered=await snapshot();assert.equal(recovered.marker,null);assert.equal(recovered.injected_svg_count,0);
    await page.evaluate(value=>{const tab=Array.from(document.querySelectorAll('#tabs .tab[data-id]')).find(x=>x.dataset.id===value.toUpperCase());if(!tab)throw Error('Retained literal tab is missing');tab.click();},symbol);await page.waitForFunction(()=>document.getElementById('quote').textContent.includes('<SVG ONLOAD='));recovered=await snapshot();assert.equal(recovered.marker,null);assert.equal(recovered.injected_svg_count,0);states[route].recovered=recovered;
   }
   const label="A'\"&"+symbol,note='</textarea>'+symbol+'&';
   await page.evaluate(value=>fixtureHtml.render(value),{symbol:label,note});
   await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
   const labels=await page.evaluate(()=>({marker:document.documentElement.getAttribute('data-fixture-html'),injected:document.querySelectorAll('svg[onload]').length,note:document.getElementById('noteb').value,tab:document.querySelector('#tabs .tab[data-id]').dataset.id,favorite:document.querySelector('#favs [data-f]').dataset.f,option:document.querySelector('#list option').value,watch:document.querySelector('#wlist [data-s]').dataset.s,flag:Array.from(document.querySelector('#wlist .wacc').attributes,x=>x.name),hit:document.querySelector('#hits [data-s]').dataset.s,alert:document.querySelector('#alerts [data-del]').dataset.del,state:fixtureHtml.state()}));
   assert.equal(labels.marker,null);assert.equal(labels.injected,0);assert.equal(labels.note,note);for(const key of ['tab','favorite','option','watch','hit','alert'])assert.equal(labels[key],label,key);assert.deepEqual(labels.flag,['class','style']);assert.equal(labels.state.notes[label].text,note);
   await page.locator('#alerts [data-del]').evaluate(el=>el.click());assert.equal((await page.evaluate(()=>fixtureHtml.state())).alerts.length,0);states.literal_fields={input:{label,note},result:labels,alert_delete_preserved:true};
   const comparison=await page.evaluate(value=>{
    let removed;const prior=window.jhDelCompare;window.jhDelCompare=id=>{removed=id;};
    try{jhInduxLegend({INDS:[],lastBars:[],atTime:null,tf:'1d',active:value,compare:[value],spec:()=>['1d','Daily'],overlayMap:{},valAt:()=>null,fmt:String,volOn:false});const row=document.querySelector('#legend [data-kind=cmp]'),id=row.dataset.id,text=row.querySelector('.leg-n').textContent;row.querySelector('[data-act=x]').click();return {id,text,removed,marker:document.documentElement.getAttribute('data-fixture-html'),injected:document.querySelectorAll('svg[onload]').length};}finally{window.jhDelCompare=prior;}
   },label);
   for(const key of ['id','text','removed'])assert.equal(comparison[key],label);assert.equal(comparison.marker,null);assert.equal(comparison.injected,0);states.comparison={input:label,result:comparison};
   const shot=path.join(D,'literal-symbol-'+width+'.png');await page.screenshot({path:shot});screenshots.push(shot);assert.deepEqual(errors,[]);
   output.cases.push({width,requests,errors,states,screenshots,actual_network_requests:0});
  }catch(error){fs.writeFileSync(path.join(D,'failure-'+width+'.json'),JSON.stringify({error:String(error),errors,requests,states,state:await page.evaluate(()=>window.fixtureState?.())},null,2));throw error;}finally{await context.close();}
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
