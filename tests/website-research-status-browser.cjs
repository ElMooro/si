const {chromium}=require('playwright');const fs=require('fs'),path=require('path'),assert=require('assert/strict'),crypto=require('crypto');
const W=__dirname,source=path.join(W,'../jh-ai-insights.js'),fixture=JSON.parse(fs.readFileSync(path.join(W,'fixtures/website-research-status/invented-packet.json'),'utf8')),out=process.env.JH_WEBSITE_STATUS_BROWSER_OUT||path.join(W,'../artifacts/website-research-status-browser');fs.mkdirSync(out,{recursive:true});
(async()=>{
 const browser=await chromium.launch({channel:'msedge',headless:true});const cases=[];
 try{
  for(const width of [1440,390]){
   const context=await browser.newContext({viewport:{width,height:950}}),page=await context.newPage(),errors=[],requests=[];
   page.on('pageerror',e=>errors.push(String(e)));await context.route('**/*',route=>{requests.push(route.request().url());return route.abort();});
   await page.setContent('<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;background:#080d17;color:#eee;font-family:Arial"><main style="padding:24px"><h1>Invented research desk</h1><p>Offline widget acceptance fixture. No market data.</p><button id="desk-control">Desk control</button></main></body></html>');
   await page.evaluate(()=>{
    const store=new Map();Object.defineProperty(window,'sessionStorage',{value:{getItem:k=>store.get(k)||null,setItem:(k,v)=>store.set(k,v),removeItem:k=>store.delete(k)}});
    window.__requests=[];window.fetch=(url,options)=>new Promise((resolve,reject)=>window.__requests.push({url,options,resolve,reject}));
   });
   await page.addScriptTag({path:source});await page.waitForFunction(()=>window.__requests.length===1);
   assert.equal(await page.locator('.jhi-panel').isVisible(),false);assert.equal(await page.locator('.jhi-fab').getAttribute('aria-expanded'),'false');
   await page.evaluate(p=>window.__requests[0].resolve({ok:true,json:()=>Promise.resolve(p)}),fixture);
   await page.locator('.jhi-fab').filter({hasText:'WAIT'}).waitFor();
   await page.locator('.jhi-fab').click();assert.equal(await page.locator('.jhi-fab').getAttribute('aria-expanded'),'true');assert.equal(await page.locator('[data-jhi-close]').evaluate(el=>el===document.activeElement),true);
   await page.locator('summary').click();assert.equal(await page.locator('.jhi-input').count(),12);
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
   const normal=path.join(out,'status-'+width+'.png');await page.screenshot({path:normal,fullPage:true});
   await page.keyboard.press('Escape');assert.equal(await page.locator('.jhi-panel').isVisible(),false);assert.equal(await page.locator('.jhi-fab').evaluate(el=>el===document.activeElement),true);
   // Malformed legacy response must update both surfaces and retain a close control.
   await page.evaluate(()=>{window.JHInsights.refresh();});await page.waitForFunction(()=>window.__requests.length===2);
   await page.evaluate(()=>window.__requests[1].resolve({ok:true,json:()=>Promise.resolve({synthesis:{global_posture:'RISK_ON'}})}));
   await page.locator('.jhi-fab').filter({hasText:'Unavailable'}).waitFor();await page.locator('.jhi-fab').click();await page.locator('[data-jhi-close]').waitFor();assert.doesNotMatch(await page.locator('.jhi-panel').innerText(),/RISK.ON|NEUTRAL/);
   const unavailable=path.join(out,'unavailable-'+width+'.png');await page.screenshot({path:unavailable,fullPage:true});
   // A late old response cannot replace the newest failure or populate the cache.
   await page.evaluate(()=>{window.JHInsights.refresh();window.JHInsights.refresh();});await page.waitForFunction(()=>window.__requests.length===4);
   await page.evaluate(()=>window.__requests[3].resolve({ok:false,status:503}));await page.locator('.jhi-fab').filter({hasText:'Unavailable'}).waitFor();
   await page.evaluate(p=>window.__requests[2].resolve({ok:true,json:()=>Promise.resolve(p)}),fixture);await page.evaluate(()=>new Promise(resolve=>setTimeout(resolve,0)));assert.match(await page.locator('.jhi-fab').innerText(),/Unavailable/i);
   // Recovery from failure changes panel and FAB; future timestamps remain visibly suspect.
   await page.evaluate(()=>{window.JHInsights.refresh();});await page.waitForFunction(()=>window.__requests.length===5);
   const injected=structuredClone(fixture);injected.generated_at='2099-01-01T00:00:00Z';injected.synthesis.watch_list=['<img src=x onerror="window.__injected=1">'];injected.input_status.bonds.reported_as_of='<svg onload="window.__injected=1">';
   await page.evaluate(p=>window.__requests[4].resolve({ok:true,json:()=>Promise.resolve(p)}),injected);await page.locator('.jhi-fab').filter({hasText:'WAIT'}).waitFor();
   assert.match(await page.locator('.jhi-panel').innerText(),/publication time is in the future/);assert.equal(await page.locator('.jhi-panel img,.jhi-panel svg').count(),0);assert.equal(await page.evaluate(()=>window.__injected||0),0);
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
   // Promise rejection and null response have handled errors and never strand old status.
   await page.evaluate(()=>{window.JHInsights.refresh();});await page.waitForFunction(()=>window.__requests.length===6);await page.evaluate(()=>window.__requests[5].reject(new Error('invented fetch failure')));await page.locator('.jhi-fab').filter({hasText:'Unavailable'}).waitFor();
   await page.evaluate(()=>{window.JHInsights.refresh();});await page.waitForFunction(()=>window.__requests.length===7);await page.evaluate(()=>window.__requests[6].resolve({ok:true,json:()=>Promise.resolve(null)}));await page.locator('.jhi-fab').filter({hasText:'Unavailable'}).waitFor();
   await page.locator('[data-jhi-refresh]').click();await page.waitForFunction(()=>window.__requests.length===8);assert.equal(await page.locator('.jhi-panel').getAttribute('aria-busy'),'true');
   await page.evaluate(p=>window.__requests[7].resolve({ok:true,json:()=>Promise.resolve(p)}),fixture);await page.locator('.jhi-fab').filter({hasText:'WAIT'}).waitFor();assert.equal(await page.locator('.jhi-panel').getAttribute('aria-busy'),'false');assert.equal(await page.locator('[data-jhi-refresh]').textContent(),'Refresh status');
   await page.keyboard.press('Escape');assert.equal(await page.locator('.jhi-panel').isVisible(),false);assert.equal(await page.locator('.jhi-fab').evaluate(el=>el===document.activeElement),true);
   assert.deepEqual(errors,[]);assert.deepEqual(requests,[]);
   cases.push({width,tests:['initial_loading','valid_wait','input_provenance','keyboard_open_close','mobile_overflow','legacy_withheld','late_response_ownership','failure_recovery','future_clock','literal_markup','rejected_fetch','null_response','visible_retry_control'],screens:[normal,unavailable],errors,actual_network_requests:requests.length});await context.close();
  }
 }finally{await browser.close();}
 const record={prototype:false,source_sha256:crypto.createHash('sha256').update(fs.readFileSync(source)).digest('hex'),wholly_invented_fixture:true,actual_network_requests:0,cases};fs.writeFileSync(path.join(out,'browser-qa.json'),JSON.stringify(record,null,2)+'\n');console.log(JSON.stringify(record));
})().catch(e=>{console.error(e);process.exitCode=1;});
