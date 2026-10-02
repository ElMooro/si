const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const {source,fixture}=require('./crypto-sentiment-support.cjs');
const OUT=process.env.JH_QA_OUT||path.join(__dirname,'stage543-browser');fs.mkdirSync(OUT,{recursive:true});
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true}),results=[];
 try{for(const width of [390,1280]){
  const s=source(),context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),requests=[],errors=[];
  await context.route('**/*',r=>{requests.push(r.request().url());return r.abort();});const page=await context.newPage();page.on('pageerror',e=>errors.push(String(e)));page.on('console',m=>{if(m.type()==='error')errors.push(m.text());});
  await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+s.styles+'</style></head><body><span id="ts"></span><main id="main" class="container"></main></body></html>');await page.addScriptTag({content:s.code});
  const hostile=fixture();hostile.full_reported_observations.history[0].label='<img src=x onerror=alert(1)>';
  const conflict=fixture();conflict.full_reported_observations.history[0].value=99;
  const cases=[['zero',fixture()],['legacy',{current:50,full_history:[{date:'2016-01-01',value:99,synthetic:true}]}],['missing',null],['boolean',true],['hostile',hostile],['conflict',conflict],['thousand',fixture(1000)]];
  for(const [name,packet] of cases){
   await page.evaluate(p=>{window.D={fear_greed:p,global_market:{btc_dominance:61},stablecoins:{net_signal:'INFLOW'}};render();document.querySelectorAll('.pane').forEach(el=>el.classList.remove('active'));document.getElementById('pane-sentiment').classList.add('active');},packet);
   const pane=page.locator('#pane-sentiment');assert.equal(await pane.isVisible(),true);assert.match(await pane.innerText(),/Alternative.me Bitcoin/);assert.doesNotMatch(await pane.innerText(),/2016 - Present|Available after next Lambda run/);
   assert.equal(await page.locator('img,iframe').count(),0);assert.deepEqual(requests,[]);assert.deepEqual(errors,[]);
   if(['zero','hostile','conflict','thousand'].includes(name)){
    const summaries=pane.locator('.crypto-sentiment-history summary');assert.equal(await summaries.count(),2);await summaries.first().focus();await page.keyboard.press('Enter');assert.equal(await pane.locator('.crypto-sentiment-history details').first().getAttribute('open'),'');
    assert.equal(await pane.locator('.crypto-sentiment-history tbody tr').count(),name==='thousand'?2000:62);assert.equal(await pane.locator('svg[role="img"]').count(),1);
   }
   const dims=await page.evaluate(()=>({client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.client+1,JSON.stringify({width,name,dims}));
   if(name==='zero'){await page.waitForFunction(()=>Number(getComputedStyle(document.querySelector('#pane-sentiment .card')).opacity)===1);await pane.screenshot({path:path.join(OUT,'sentiment-'+width+'.png')});}
   results.push({width,name,requests:0,errors:0,overflow:false});
  }await context.close();
 }}finally{await browser.close();}
 fs.writeFileSync(path.join(OUT,'acceptance.json'),JSON.stringify({scope:'Actual function declarations and inline styles; invented inputs, all network blocked',scenarios:results.length,results},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,errors:0,requests:0,overflow:0}));
})().catch(e=>{console.error(e);process.exit(1);});
