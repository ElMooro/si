const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const {page:source}=require('./crypto-market-cap-support.cjs');
const OUT=process.env.JH_QA_OUT||path.join(__dirname,'stage542-browser');fs.mkdirSync(OUT,{recursive:true});
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true}),results=[];
 try{for(const width of [390,1280]){
  const s=source('crypto/index.html'),context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),requests=[],errors=[];
  await context.route('**/*',r=>{requests.push(r.request().url());return r.abort();});const page=await context.newPage();page.on('pageerror',e=>errors.push(String(e)));page.on('console',m=>{if(m.type()==='error')errors.push(m.text());});
  await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+s.styles+'</style></head><body><span id="ts"></span><main id="main" class="container"></main></body></html>');
  await page.addScriptTag({content:s.code});
  for(const signal of ['INFLOW','OUTFLOW','NEUTRAL',null,true,75,'<img src=x onerror=alert(1)>']){
   await page.evaluate(value=>{window.D={stablecoins:{net_signal:value},fear_greed:{current:17},global_market:{btc_dominance:61}};render();document.querySelectorAll('.pane').forEach(el=>el.classList.remove('active'));document.getElementById('pane-sentiment').classList.add('active');},signal);
   const pane=page.locator('#pane-sentiment'),row=pane.locator('.crypto-sentiment-component').filter({hasText:'Stablecoin flow (transaction evidence unavailable)'});
   assert.equal(await row.count(),1);assert.equal(await row.isVisible(),true);
   assert.match(await row.innerText(),/Unavailable/);assert.equal(await row.locator('[style*="height:8px"]').count(),0);
   assert.doesNotMatch(await pane.innerText(),/inflow 75|outflow 25|neutral 50/);
   assert.equal(await pane.locator('.crypto-sentiment-component').count(),5);
   assert.equal(await page.locator('img,iframe').count(),0);assert.deepEqual(requests,[]);assert.deepEqual(errors,[]);
   const dims=await page.evaluate(()=>({client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.client+1,JSON.stringify({width,signal,dims}));
   if(signal==='INFLOW'){
    await page.waitForFunction(()=>Number(getComputedStyle(document.querySelector('#pane-sentiment .card')).opacity)===1);
    await pane.screenshot({path:path.join(OUT,'sentiment-'+width+'.png')});
   }
   results.push({width,invented_signal:signal,requests:0,errors:0,overflow:false});
  }await context.close();
 }}finally{await browser.close();}
 fs.writeFileSync(path.join(OUT,'acceptance.json'),JSON.stringify({scope:'Actual Crypto function declarations and inline styles; invented inputs, all network blocked',scenarios:results.length,results},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,errors:0,requests:0,overflow:0}));
})().catch(e=>{console.error(e);process.exit(1);});
