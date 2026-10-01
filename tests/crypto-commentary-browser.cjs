const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const R=path.resolve(__dirname,'..'),W=path.resolve(process.env.JH_QA_OUT||'.qa-crypto-commentary');fs.mkdirSync(W,{recursive:true});const html=fs.readFileSync(path.join(R,'crypto/index.html'),'utf8');
const styles=[...html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)].map(m=>m[1]).join('\n');
const script=html.slice(html.indexOf('// Typed commentary presentation:'),html.indexOf('function render(){'));
const base={status:'ok',analysis:'**Invented research**\nMeasurement context remains separate from a portfolio action.',model:'Invented offline model',generated_at:'2020-01-01T00:00:00Z',system_sources:['Invented source A','Invented source B']};
const cases={normal:base,unavailable:{status:'unavailable',error:'Model commentary unavailable under the no-paid policy.'},malformed:{status:'ok',analysis:{},model:[],generated_at:{},system_sources:'bad'},injection:{...base,analysis:'**<img src=x onerror="window.INJECTED=1">**\n<script>window.INJECTED=1</script>',model:'<iframe src="https://example.invalid">',system_sources:['<img src=x>']},long:{...base,analysis:'Z'.repeat(3000),model:'M'.repeat(256),system_sources:['S'.repeat(256)]}};
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true});const results=[];
 try{for(const width of [390,1280]){
  const context=await browser.newContext({viewport:{width,height:800},serviceWorkers:'block'}),requests=[];await context.route('**/*',route=>{requests.push(route.request().url());return route.abort();});
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(String(e)));
  await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+styles+'</style></head><body><main id="fixture" style="padding:16px;max-width:100%"></main></body></html>');await page.addScriptTag({content:script});
  for(const [name,value] of Object.entries(cases)){
   await page.evaluate(value=>{document.querySelector('#fixture').innerHTML=renderCryptoCommentary(value);document.querySelector('#pane-ai').classList.add('active');},value);
   const card=page.locator('#pane-ai .card');assert.equal(await card.isVisible(),true);assert.equal(await card.locator('img,script,iframe').count(),0);assert.equal(await page.evaluate(()=>window.INJECTED),undefined);
   if(name==='normal')assert.match(await card.innerText(),/Research only/);if(['unavailable','malformed'].includes(name))assert.match(await card.innerText(),/unavailable/i);
   const dims=await card.evaluate(el=>({width:el.clientWidth,scroll:el.scrollWidth,doc:document.documentElement.clientWidth,docscroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.width+1,JSON.stringify({name,width,dims}));assert.ok(dims.docscroll<=dims.doc+1,JSON.stringify({name,width,dims}));
   const screenshot=['normal','unavailable'].includes(name)?path.join(W,'stage528-'+name+'-'+width+'.png'):null;if(screenshot)await page.screenshot({path:screenshot,fullPage:true});
   results.push({name,width,screenshot,no_markup_execution:true,no_overflow:true});
  }assert.deepEqual(errors,[]);assert.deepEqual(requests,[]);await context.close();
 }
 fs.writeFileSync(path.join(W,'stage528-browser-proof.json'),JSON.stringify({scope:'Actual candidate commentary renderer and full page inline CSS; isolated DOM, invented data, no network. Other page sections and live producers are not tested.',results,page_errors:0,network_requests:0},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,page_errors:0,network_requests:0}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
