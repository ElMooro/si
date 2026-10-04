const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const ROOT=path.resolve(__dirname,'..'),OUT=path.resolve(process.argv[2]||'/tmp/watchlist-lifecycle');fs.mkdirSync(OUT,{recursive:true});
const rules=JSON.parse(fs.readFileSync(path.join(ROOT,'data/tv-symbol-resolver.json'),'utf8'));
const html='<html><head><meta charset="utf-8"></head><body><div id="watch" class="is-open"><select id="list"><option value="fixture">Fixture</option><option value="second">Second</option></select><input id="q"><div id="letters"></div><div id="w-list"><div id="cols"><span></span><span data-s="sym">Symbol</span><span data-s="last">Last</span><span data-s="chgv">Chg</span><span data-s="chg">Chg%</span></div><div id="wlist" style="height:1600px;overflow:auto"></div></div></div><script src="/jh-chart-tvwatch.js"></script></body></html>';
async function scenario(browser,resolverBodyTimeout=false){
 const context=await browser.newContext({viewport:{width:700,height:1900},serviceWorkers:'block'}),page=await context.newPage(),errors=[];
 page.on('pageerror',e=>errors.push(e.message));
 await page.clock.install({time:new Date('2026-10-04T00:00:00Z')});
 await context.addInitScript(({rules,resolverBodyTimeout})=>{
  const members=['DEADA','DEADB','DEADC','GOOD','ERR429','ERR500','BADJSON','ONE'];
  const lists=[{id:'fixture',name:'Fixture',symbols:members},{id:'second',name:'Second',symbols:['SHARE','FRED:DGS10']}];
  localStorage.setItem('jh-chart-custom-lists',JSON.stringify(lists));localStorage.setItem('jh-chart-flags','{"SHARE":"#f23645"}');
  window.fixture={counts:{},pending:{},current:0,maximum:0,active:'',members,resolverRetry:!resolverBodyTimeout};
  window.jhWatchlistResolve=s=>({engine:'equity',ticker:s,yahoo:s});window.jhWatchlistActive=()=>fixture.active;
  document.addEventListener('keydown',e=>{if(e.target.id==='q'&&e.key==='Enter')fixture.active=e.target.value;});
  const packet=t=>({ticker:t,span:'day',mult:1,source:'invented daily fixture',bars:[{time:Date.UTC(2026,9,2)/1000,close:100},{time:Date.UTC(2026,9,3)/1000,close:110}]});
  function response(t){if(t==='ONE')return {...packet(t),bars:packet(t).bars.slice(0,1)};return packet(t);}
  window.fixture.release=t=>{const pending=fixture.pending[t];if(pending){delete fixture.pending[t];pending({ok:true,json:()=>Promise.resolve(response(t))});}};
  window.fetch=(url,options={})=>{
   if(String(url).includes('tv-symbol-resolver'))return Promise.resolve({ok:true,json:()=>fixture.resolverRetry?Promise.resolve(rules):new Promise(()=>{})});
   const t=new URL(url,location.href).searchParams.get('ticker');fixture.counts[t]=(fixture.counts[t]||0)+1;
   fixture.current++;fixture.maximum=Math.max(fixture.maximum,fixture.current);
   let request;
   if((/^DEAD/.test(t)&&fixture.counts[t]===1)||t==='SHARE')request=new Promise(resolve=>fixture.pending[t]=resolve); // intentionally ignores AbortSignal
   else if(t==='ERR429'&&fixture.counts[t]===1)request=Promise.resolve({ok:false,status:429});
   else if(t==='ERR500'&&fixture.counts[t]===1)request=Promise.resolve({ok:false,status:500});
   else if(t==='BADJSON'&&fixture.counts[t]===1)request=Promise.resolve({ok:true,json:()=>Promise.reject(Error('malformed JSON'))});
   else request=Promise.resolve({ok:true,json:()=>Promise.resolve(response(t))});
   const finalize=()=>{fixture.current=Math.max(0,fixture.current-1);};
   if(options.signal)options.signal.addEventListener('abort',finalize,{once:true});
   return request.finally(finalize);
  };
 },{rules,resolverBodyTimeout});
 await context.route('**/*',route=>{const u=new URL(route.request().url());if(u.pathname==='/chart.html')return route.fulfill({contentType:'text/html',body:html});if(u.pathname==='/jh-chart-tvwatch.js')return route.fulfill({contentType:'application/javascript',body:fs.readFileSync(path.join(ROOT,'jh-chart-tvwatch.js'),'utf8')});return route.abort('blockedbyclient');});
 const state={resolver_body_timeout:resolverBodyTimeout};
 try{
 await page.goto('https://fixture.lifecycle.test/chart.html');await page.clock.runFor(300);
 if(resolverBodyTimeout){
  await page.clock.fastForward(10001);await page.waitForFunction(()=>document.querySelector('#wlist .qe')?.textContent.includes('resolver unavailable'));
  assert.deepEqual(await page.evaluate(()=>fixture.counts),{});
  await page.evaluate(()=>fixture.resolverRetry=true);await page.locator('#w-quote-refresh').click();await page.waitForFunction(()=>fixture.counts.DEADA===1);
  state.resolver_timeout_withheld_all_quotes=true;state.manual_resolver_retry_started=true;
 }else{
  await page.waitForFunction(()=>fixture.counts.DEADA===1&&fixture.counts.DEADB===1&&fixture.counts.DEADC===1);
  assert.equal(await page.evaluate(()=>fixture.maximum),3);
  assert.equal(await page.evaluate(()=>fixture.counts.GOOD||0),0);
  await page.clock.fastForward(10001);
  await page.waitForFunction(()=>document.querySelector('#wlist [data-s=GOOD] .px').textContent==='110.00');
  for(const t of ['DEADA','DEADB','DEADC'])assert.ok((await page.locator('#wlist [data-s='+t+'] .qe').innerText()).includes('request timeout'));
  for(const [t,reason] of [['ERR429','HTTP 429'],['ERR500','HTTP 500'],['BADJSON','malformed JSON']])assert.ok((await page.locator('#wlist [data-s='+t+'] .qe').innerText()).includes(reason));
  assert.equal(await page.locator('#wlist [data-s=ONE] .chg').first().innerText(),'—');
  const first=await page.evaluate(()=>({...fixture.counts}));await page.locator('#w-quote-refresh').click();await page.clock.runFor(50);assert.deepEqual(await page.evaluate(()=>fixture.counts),first);
  await page.clock.fastForward(30001);await page.locator('#w-quote-refresh').click();
  for(const t of ['DEADA','DEADB','DEADC','ERR429','ERR500','BADJSON'])await page.waitForFunction(t=>document.querySelector('#wlist [data-s='+t+'] .px').textContent==='110.00',t);
  assert.equal(await page.evaluate(()=>fixture.counts.GOOD),1);
  // Switch and reorder while same exact instrument is in flight; its completion paints only that row.
  await page.evaluate(()=>{const lists=JSON.parse(localStorage.getItem('jh-chart-custom-lists'));lists[0].symbols.push('SHARE');localStorage.setItem('jh-chart-custom-lists',JSON.stringify(lists));window.dispatchEvent(new StorageEvent('storage',{key:'jh-chart-custom-lists'}));});
  await page.waitForFunction(()=>fixture.counts.SHARE===1);
  await page.evaluate(()=>{document.getElementById('list').value='second';window.dispatchEvent(new StorageEvent('storage',{key:'jh-chart-custom-lists'}));});
  await page.waitForFunction(()=>document.querySelectorAll('#wlist .wrow').length===2);
  await page.evaluate(()=>fixture.release('SHARE'));
  await page.waitForFunction(()=>document.querySelector('#wlist [data-s=SHARE] .px').textContent==='110.00');
  assert.equal(await page.locator('#wlist [data-s=GOOD]').count(),0);
  await page.evaluate(()=>{localStorage.setItem('jh-chart-flags','{}');window.dispatchEvent(new StorageEvent('storage',{key:'jh-chart-flags'}));});
  assert.equal(await page.locator('#wlist [data-s=SHARE] .tvflag').evaluate(e=>e.style.background),'');
  const original=await page.evaluate(()=>localStorage.getItem('jh-chart-custom-lists'));
  await page.evaluate(()=>{const lists=JSON.parse(localStorage.getItem('jh-chart-custom-lists'));lists[1].symbols.reverse();localStorage.setItem('jh-chart-custom-lists',JSON.stringify(lists));window.dispatchEvent(new StorageEvent('storage',{key:'jh-chart-custom-lists'}));});
  assert.equal(await page.locator('#wlist .wrow').first().getAttribute('data-s'),'FRED:DGS10');
  // Cache expires without polling; explicit visibility reuse starts one request after expiry.
  const before=await page.evaluate(()=>fixture.counts.SHARE);
  await page.clock.fastForward(300001);assert.equal(await page.evaluate(()=>fixture.counts.SHARE),before);
  await page.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));await page.waitForFunction(before=>fixture.counts.SHARE===before+1,before);
  await page.evaluate(()=>fixture.release('SHARE'));await page.waitForFunction(()=>document.querySelector('#wlist [data-s=SHARE] .px').textContent==='110.00');
  const after=await page.evaluate(()=>fixture.counts.SHARE);await page.locator('#w-quote-refresh').click();await page.clock.runFor(50);assert.equal(await page.evaluate(()=>fixture.counts.SHARE),after);
  // Malformed saved store settles to one honest unavailable state, without a render loop or overwrite.
  await page.evaluate(()=>{localStorage.setItem('jh-chart-custom-lists','{');window.dispatchEvent(new StorageEvent('storage',{key:'jh-chart-custom-lists'}));});
  await page.waitForFunction(()=>document.getElementById('wlist').textContent==='Saved list unavailable; original storage retained');await page.clock.runFor(100);
  assert.equal(await page.evaluate(()=>localStorage.getItem('jh-chart-custom-lists')),'{');
  state.three_timeouts_release_slots=true;state.http_retry_and_malformed_json_retry=true;state.no_refresh_storm=true;state.shared_late_response_correct=true;state.same_count_reorder=true;state.flag_removal=true;state.no_polling=true;state.expiry_visibility_reuse=true;state.malformed_storage_retained=true;
 }
 state.counts=await page.evaluate(()=>fixture.counts);state.maximum=await page.evaluate(()=>fixture.maximum);state.errors=errors;assert.deepEqual(errors,[]);return state;
 }catch(error){fs.writeFileSync(path.join(OUT,'failure-'+resolverBodyTimeout+'.json'),JSON.stringify({error:String(error),errors,fixture:await page.evaluate(()=>({counts:fixture.counts,maximum:fixture.maximum})),body:await page.locator('body').innerText()},null,2));throw error;}finally{await context.close();}
}
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium'});try{const cases=[await scenario(browser),await scenario(browser,true)];fs.writeFileSync(path.join(OUT,'lifecycle-qa.json'),JSON.stringify({scope:'Invented browser fixtures; mock fetch deliberately ignores abort; hard deadlines must release slots. No actual network.',cases,actual_network_requests:0},null,2));console.log(JSON.stringify({passed:true,cases:cases.length,actual_network_requests:0}));}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
