const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),zlib=require('node:zlib'),{chromium}=require('playwright');
const R=path.resolve(__dirname,'..'),S=path.resolve(process.env.JH_BROWSER_SOURCE_ROOT||R),D=path.resolve(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const read=p=>fs.readFileSync(path.join(S,p),'utf8'),pub=n=>JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(R,'tests/fixtures/chart-cq-definition/public/'+n+'.gz'))));
const doc=pub('cryptoquant-series.json'),onchain=pub('cryptoquant-onchain.json'),spec=pub('published-spec.json');
const scripts=['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js','jh-chart-engine.js','jh-stock-desk-research.js','jh-chart-stock-desk.js'];
const html=read('chart.html').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'').replace('</body>','<script src="/fixture-drawing.js"></script>'+scripts.map(p=>'<script src="/'+p+'"></script>').join('')+'</body>');
const inputs={'/data/cryptoquant-series.json':doc,'/data/cryptoquant-onchain.json':onchain,'/data/config/cryptoquant-spec.json':spec,'/data/cq-feed.json':{metrics:{}},'/data/cq-catalog.json':{catalog:{}},'/cq-universe.json':{rows:[]},'/data/symbology/master.json':{by_ticker:{}},'/data/warehouse/catalog.json':{datasets:[]},'/data/engine_inventory.json':{engines:[]}};
(async()=>{const browser=await chromium.launch({executablePath:process.env.CHROMIUM_EXECUTABLE_PATH,headless:true}),results=[];try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:1000},isMobile:width===390,serviceWorkers:'block'}),page=await context.newPage(),requests=[],errors=[];page.on('pageerror',e=>errors.push(e.message));
  await context.addInitScript(()=>{window.fixtureBlob=null;const original=URL.createObjectURL;URL.createObjectURL=b=>{window.fixtureBlob=b;return original(b);};});
  await context.route('**/*',route=>{const u=new URL(route.request().url());requests.push({path:u.pathname,query:u.search});const send=(body,type='application/json')=>route.fulfill({status:200,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
   if(u.host==='invented.justhodl.test'&&u.pathname==='/chart.html')return send(html,'text/html');
   if(u.pathname==='/fixture-drawing.js')return send(fs.readFileSync(path.join(R,'tests/fixtures/chart-observations/vendor/lightweight-charts-4.2.3.js.txt'),'utf8'),'application/javascript');
   if(scripts.includes(u.pathname.slice(1)))return send(read(u.pathname.slice(1)),'application/javascript');
   if(Object.hasOwn(inputs,u.pathname))return send(inputs[u.pathname]);
   return route.abort('blockedbyclient');
  });
  await page.goto('https://invented.justhodl.test/chart.html?s=CQ:btc_fees_total');await page.waitForFunction(()=>JHStockDeskController.getModel()?.observations?.definition_review?.status==='definition_conflict');
  const model=await page.evaluate(()=>JHStockDeskController.getModel());assert.equal(model.observations.whole_packet.generated_at,doc.generated_at);assert.deepEqual(model.observations.selected_series,doc.series.btc_fees_total);assert.equal(model.observations.unit,null);
  assert.deepEqual(model.bars.map(b=>b.close),doc.series.btc_fees_total.v);assert.equal(model.bars.length,doc.series.btc_fees_total.d.length);
  await page.evaluate(()=>{document.querySelector('#btn-watch').click();document.querySelector('[data-sub=details]').click();});
  assert.match(await page.locator('[data-stock-definition-conflict]').textContent(),/fees_block_mean.*do not interpret these values as total fees/);
  await page.evaluate(()=>document.querySelector('[data-stock-observation-export]').click());const exported=await page.evaluate(async()=>JSON.parse(await fixtureBlob.text()));assert.deepEqual(exported.observations.whole_packet,doc);assert.deepEqual(exported.observations.definition_review,model.observations.definition_review);
  const hits=await page.evaluate(async()=>{await JHCqFuse.load();return JHCqFuse.searchHits('CQ:btc_fees_total');});assert.match(hits[0].name,/definition conflict/);
  const box=await page.locator('[data-stock-definition-conflict]').boundingBox();assert.ok(box.width>0&&box.width<=width);assert.ok(box.y>=0&&box.y+box.height<=1000,'definition warning must be visible without scrolling');await page.screenshot({path:path.join(D,'cq-definition-'+width+'.png')});
  await page.evaluate(()=>jhOpenSymbol('CQ:btc_tx_count'));await page.waitForFunction(()=>JHStockDeskController.getModel()?.observations?.selected_id==='btc_tx_count');assert.equal(await page.locator('[data-stock-definition-conflict]').count(),0);assert.deepEqual(await page.evaluate(()=>JHStockDeskController.getModel().bars.map(b=>b.close)),doc.series.btc_tx_count.v);
  assert.equal(requests.filter(r=>/\/ohlc|\/yf-ohlc|\/trades|\/aggTrades|\/klines|\/yahoo-fund/.test(r.path)&&/CQ/i.test(r.query)).length,0);assert.deepEqual(errors,[]);
  results.push({width,passed:true,whole_received_history_checked:true,definition_conflict_visible:true,complete_export_checked:true,unaffected_series_checked:true,actual_network_requests:0,requests});await context.close();
 }
 fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify({scope:'Whole chart markup and seven complete modules with pinned drawing library; all requests intercepted; whole captured public packets; no upstream-original replay claim',results},null,2)+'\n');console.log(JSON.stringify({passed:true,widths:results.map(r=>r.width),actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
