const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const root=path.join(__dirname,'..'),{page:source}=require('./crypto-market-cap-support.cjs'),{classic}=require('./crypto-stablecoin-support.cjs');
const fixtures=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/crypto-stablecoin-stocks/invented-page-packets.json'),'utf8'));
const OUT=process.env.JH_QA_OUT||path.join(require('node:os').tmpdir(),'justhodl-stablecoin-browser');fs.mkdirSync(OUT,{recursive:true});
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true}),results=[];
 try{for(const width of [390,1280]){
  const s=source('crypto/index.html'),context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),requests=[],errors=[];
  await context.route('**/*',r=>{requests.push(r.request().url());return r.abort();});const page=await context.newPage();page.on('pageerror',e=>errors.push(String(e)));page.on('console',m=>{if(m.type()==='error')errors.push(m.text());});
  await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+s.styles+'</style></head><body><span id="ts"></span><main id="main" class="container"></main></body></html>');
  await page.addScriptTag({content:s.code});
  for(const [scenario,packet] of Object.entries(fixtures)){
   await page.evaluate(p=>{window.D={stablecoins:p};render();document.querySelectorAll('.pane').forEach(el=>el.classList.remove('active'));document.getElementById('pane-stablecoins').classList.add('active');},packet);
   const card=page.locator('.crypto-stablecoin-card');assert.equal(await card.isVisible(),true);await page.waitForFunction(()=>Number(getComputedStyle(document.querySelector('.card')).opacity)===1);
   const ratios=await card.locator('p,th,.card-title').evaluateAll(elements=>{
    const luminance=color=>{const parts=color.match(/[\d.]+/g).slice(0,3).map(Number).map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4);return .2126*parts[0]+.7152*parts[1]+.0722*parts[2];};
    const back=luminance(getComputedStyle(document.querySelector('.crypto-stablecoin-card')).backgroundColor);
    return elements.map(el=>{const front=luminance(getComputedStyle(el).color);return (Math.max(front,back)+.05)/(Math.min(front,back)+.05);});
   });assert.ok(ratios.every(value=>value>=4.5),'Stock labels and qualifications require readable contrast');
   const summary=card.locator('summary');if(await summary.count()){
    await summary.focus();await page.keyboard.press('Enter');assert.equal(await card.locator('details').getAttribute('open'),'');
    const region=card.locator('[role="region"]');await region.focus();assert.equal(await region.evaluate(el=>el===document.activeElement),true);
    if(width===390){await page.keyboard.press('ArrowRight');await page.waitForFunction(()=>document.querySelector('[role="region"]').scrollLeft>0);await page.waitForTimeout(250);await region.evaluate(el=>el.scrollTo({left:0,behavior:'instant'}));await page.waitForFunction(()=>document.querySelector('[role="region"]').scrollLeft===0);}
   }
   if(scenario==='normal'){assert.equal(await card.locator('tbody tr').count(),30);assert.match(await card.innerText(),/Invented 29/);}
   assert.equal(await page.locator('img,iframe').count(),0);assert.equal(await page.evaluate(()=>window.injected),undefined);
   assert.deepEqual(requests,[]);assert.deepEqual(errors,[]);
   const dims=await page.evaluate(()=>({client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.client+1,JSON.stringify({width,scenario,dims}));
   if(['normal','zero','unavailable'].includes(scenario))await card.screenshot({path:path.join(OUT,scenario+'-'+width+'.png')});
   results.push({width,scenario,requests:0,errors:0,overflow:false});
  }await context.close();
  const c=classic(),classicContext=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),classicRequests=[],classicErrors=[];
  await classicContext.route('**/*',r=>{classicRequests.push(r.request().url());return r.abort();});const cp=await classicContext.newPage();cp.on('pageerror',e=>classicErrors.push(String(e)));
  await cp.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+c.styles+'</style></head><body><section style="padding:16px"><h2>Crypto</h2><div id="cryptoSub"></div><div id="cryptoBody"></div></section></body></html>');
  await cp.addScriptTag({content:c.code});
  for(const name of ['normal','empty_population','legacy','unavailable']){
   await cp.evaluate(p=>{window.STATE={data:{crypto:{stablecoins:p}}};renderCrypto();},fixtures[name]);
   assert.match(await cp.locator('#cryptoBody').innerText(),/flow and observation dates unavailable; no sizing vote/);assert.doesNotMatch(await cp.locator('#cryptoBody').innerText(),/INFLOW|MINTING|\$99T/);
   const link=cp.getByRole('link',{name:'Inspect reported rows'});await link.focus();assert.equal(await link.evaluate(el=>el===document.activeElement),true);
   const dims=await cp.evaluate(()=>({client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.client+1,JSON.stringify({width,name,dims}));
   assert.deepEqual(classicRequests,[]);assert.deepEqual(classicErrors,[]);await cp.screenshot({path:path.join(OUT,'classic-'+name+'-'+width+'.png')});results.push({page:'classic',width,scenario:name,requests:0,errors:0,overflow:false});
  }await classicContext.close();
 }}finally{await browser.close();}
 fs.writeFileSync(path.join(OUT,'acceptance.json'),JSON.stringify({scope:'Actual integrated Crypto and Classic function declarations/CSS with invented packets; all network blocked',scenarios:results.length,results},null,2));console.log(JSON.stringify({scenarios:results.length,errors:0,requests:0,overflow:0}));
})().catch(e=>{console.error(e);process.exit(1);});
