const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright'),{page:source}=require('./crypto-market-cap-support.cjs'),{packet,scenarios}=require('./crypto-funding-observations-support.cjs');
const OUT=process.env.JH_QA_OUT;assert.ok(OUT);fs.mkdirSync(OUT,{recursive:true});
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true}),results=[];
 try{for(const name of ['crypto/index.html','desk-v2.html'])for(const width of [390,1280]){
  const s=source(name),context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),requests=[],errors=[];
  await context.route('**/*',route=>{requests.push(route.request().url());return route.abort();});const page=await context.newPage();page.on('pageerror',e=>errors.push(String(e)));page.on('console',m=>{if(m.type()==='error')errors.push(m.text());});
  const body=name==='crypto/index.html'?'<div id="ts"></div><main id="main" class="container"></main>':'<main style="padding:16px"><section class="card wide"><div class="card-body" id="cardCrypto"></div></section></main>';
  await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+s.styles+'</style></head><body>'+body+'</body></html>');await page.addScriptTag({content:'var D=null;\n'+s.code});
  for(const [scenario,value] of Object.entries(scenarios())){
   await page.evaluate(({name,p})=>{if(name==='crypto/index.html'){D=p;render();document.querySelectorAll('.pane').forEach(el=>el.classList.remove('active'));document.getElementById('pane-derivatives').classList.add('active');}else renderCrypto(p);},{name,p:packet(value)});
   assert.equal(await page.locator('img,iframe').count(),0);assert.equal(await page.evaluate(()=>window.injected),undefined);
   const target=page.locator(name==='crypto/index.html'?'.crypto-funding-card':'#cardCrypto');
   await page.waitForFunction(()=>Array.from(document.querySelectorAll('.card')).every(el=>Number(getComputedStyle(el).opacity)===1));
   assert.equal(await target.isVisible(),true);assert.ok((await target.boundingBox()).height>100);
   if(name==='crypto/index.html'){
    const text=await target.innerText();assert.match(text,/No annual yield/);assert.doesNotMatch(text,/9999|Most Longed|Most Shorted/);
    if(scenario==='zero')assert.match(text,/0%/);if(['legacy','unavailable'].includes(scenario))assert.match(text,/Unavailable/);
    if(scenario==='tiny')assert.match(text,/1e-10%/);
    await target.locator('summary').focus();await page.keyboard.press('Enter');assert.equal(await target.locator('details').getAttribute('open'),'');
    const region=target.locator('[role="region"]');if(await region.count()){await region.focus();assert.equal(await region.evaluate(el=>el===document.activeElement),true);}
   }else{assert.match(await target.innerText(),/Unavailable/);assert.match(await target.innerText(),/WAIT — no qualified risk score/);}
   const dims=await page.evaluate(()=>({client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.client+1,JSON.stringify({name,width,scenario,dims}));assert.deepEqual(requests,[]);assert.deepEqual(errors,[]);
   if(['normal','zero','unavailable'].includes(scenario))await target.screenshot({path:path.join(OUT,name.replace(/[/.]/g,'-')+'-'+scenario+'-'+width+'.png')});
   results.push({page:name,width,scenario,overflow:false,errors:0,requests:0});
  }await context.close();
 }}finally{await browser.close();}
 fs.writeFileSync(path.join(OUT,'crypto-funding-observations-browser.json'),JSON.stringify({scenarios:results.length,results},null,2));console.log(JSON.stringify({scenarios:results.length,requests:0,errors:0,overflow:0}));
})().catch(error=>{console.error(error);process.exit(1);});
