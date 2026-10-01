const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright'),{page:source,ratios,packet}=require('./crypto-market-cap-support.cjs');
const OUT=process.env.JH_QA_OUT;assert.ok(OUT);fs.mkdirSync(OUT,{recursive:true});
const injection='<img src="https://fixture.invalid/x" onerror="window.injected=true">',cases={normal:ratios(),zero:ratios(0),tiny:ratios(1e-10),unavailable:{},legacy:{mvrv_approx:9,signal:'OVERVALUED'},malformed:ratios(true),injection:ratios(injection)};
cases.injection.market_cap_extension.numerator.value_usd=injection;cases.injection.market_cap_extension.observation_window.first=injection;
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true}),results=[];
 try{for(const name of ['crypto/index.html','desk-v2.html'])for(const width of [390,1280]){
  const s=source(name),context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),requests=[],errors=[];
  await context.route('**/*',route=>{requests.push(route.request().url());return route.abort();});const page=await context.newPage();page.on('pageerror',e=>errors.push(String(e)));page.on('console',m=>{if(m.type()==='error')errors.push(m.text());});
  const body=name==='crypto/index.html'?'<div id="ts"></div><main id="main" class="container"></main>':'<main style="padding:16px"><section class="card wide"><div class="card-body" id="cardCrypto"></div></section></main>';
  await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+s.styles+'</style></head><body>'+body+'</body></html>');await page.addScriptTag({content:'var D=null;\n'+s.code});
  for(const [scenario,value] of Object.entries(cases)){
   await page.evaluate(({name,p})=>{if(name==='crypto/index.html'){D=p;render();document.querySelectorAll('.pane').forEach(el=>el.classList.remove('active'));document.getElementById('pane-onchain').classList.add('active');}else renderCrypto(p);},{name,p:packet(value)});
   assert.equal(await page.locator('img,iframe').count(),0);assert.equal(await page.evaluate(()=>window.injected),undefined);
   const target=page.locator(name==='crypto/index.html'?'.crypto-market-cap-card':'#cardCrypto');
   await page.waitForFunction(()=>Array.from(document.querySelectorAll('.card')).every(el=>Number(getComputedStyle(el).opacity)===1));
   assert.equal(await target.isVisible(),true);assert.ok((await target.boundingBox()).height>100);
   assert.match(await target.innerText(),/descriptive/i);assert.doesNotMatch(await target.innerText(),/900|OVERVALUED/);
   if(scenario==='zero')assert.match(await target.innerText(),/0×/);if(['legacy','malformed','unavailable','injection'].includes(scenario))assert.match(await target.innerText(),/Unavailable/);
   if(name==='crypto/index.html'){await target.locator('summary').focus();await page.keyboard.press('Enter');assert.equal(await target.locator('details').getAttribute('open'),'');}
   const dims=await page.evaluate(()=>({client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.client+1,JSON.stringify({name,width,scenario,dims}));
   let screenshot=null;if(['normal','zero','unavailable'].includes(scenario)){screenshot=path.join(OUT,name.replace(/[/.]/g,'-')+'-'+scenario+'-'+width+'.png');await page.screenshot({path:screenshot,fullPage:true});}
   assert.deepEqual(errors,[]);assert.deepEqual(requests,[]);results.push({name,width,scenario,screenshot,keyboard_details:name==='crypto/index.html',no_overflow:true});
  }await context.close();
 }
 fs.writeFileSync(path.join(OUT,'crypto-market-cap-browser.json'),JSON.stringify({scenarios:results.length,network_requests:0,page_errors:0,scope:'Actual changed renderers and complete inline CSS with invented inputs. No live application or account data.',results},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,network_requests:0,page_errors:0}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
