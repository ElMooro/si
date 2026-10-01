const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const R=path.join(__dirname,'..'),OUT=process.env.JH_QA_OUT||fs.mkdtempSync(path.join(require('node:os').tmpdir(),'jh-crypto-measures-'));
const parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);const acorn=parser.exports;
const html=fs.readFileSync(path.join(R,'crypto/index.html'),'utf8'),styles=[...html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)].map(m=>m[1]).join('\n');
let functions=[];for(const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
 if(/\bsrc\s*=|type\s*=\s*["']application\//i.test(m[1]))continue;const tree=acorn.parse(m[2],{ecmaVersion:'latest'});
 if(tree.body.some(n=>n.type==='FunctionDeclaration'&&n.id.name==='render'))functions=tree.body.filter(n=>n.type==='FunctionDeclaration').map(n=>m[2].slice(n.start,n.end));
}assert.ok(functions.length);
function packet(v){return {risk_score:{score:v,regime:'Invented regime',action:'Invented action'},fear_greed:{current:v,label:'Invented label',avg_7d:v,avg_30d:v},global_market:{btc_dominance:v},technicals:{coins:{TEST:{price:v,consensus:'MIXED',timeframes:{'4h':{status:'ok',score:v,bias:'MIXED',indicators:{rsi:v,bollinger:{position:v},stochrsi:{k:v},atr_pct:v}}}}}}};}
const injection='<img src="https://fixture.invalid/x" onerror="window.injected=true"><script>window.injected=true</script>';
const cases={normal:packet(25),zero:packet(0),unavailable:packet(null),malformed:packet(true),injection:packet(injection),outside:packet(25)};
cases.injection.risk_score.regime=injection;cases.injection.risk_score.action=injection;cases.injection.fear_greed.label=injection;
cases.outside.technicals.coins.TEST.timeframes['4h'].indicators.bollinger.position=-25;cases.outside.technicals.coins.TEST.price=1e-10;
(async()=>{
 fs.mkdirSync(OUT,{recursive:true});const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true}),results=[];
 try{for(const width of [390,1280]){
  const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),requests=[],errors=[];
  await context.route('**/*',route=>{requests.push(route.request().url());return route.abort();});const page=await context.newPage();page.on('pageerror',e=>errors.push(String(e)));page.on('console',m=>{if(m.type()==='error')errors.push(m.text());});
  await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+styles+'</style></head><body><div id="ts"></div><main id="main" class="container"></main></body></html>');await page.addScriptTag({content:'var D=null;\n'+functions.join('\n')});
  for(const [name,value] of Object.entries(cases)){
   await page.evaluate(value=>{D=value;render();},value);assert.equal(await page.locator('#main img,#main script,#main iframe').count(),0);assert.equal(await page.evaluate(()=>window.injected),undefined);
   const score=page.locator('#pane-overview .stat-xl').first();assert.equal(await score.innerText(),['unavailable','malformed','injection'].includes(name)?'Unavailable':String(value.risk_score.score));
   assert.match(await page.locator('.crypto-measurement-note').innerText(),/predictive validity are unverified/);
   for(const pane of ['overview','technicals','sentiment','global']){
    await page.evaluate(pane=>{document.querySelectorAll('.pane').forEach(el=>el.classList.remove('active'));document.getElementById('pane-'+pane).classList.add('active');},pane);
    const active=page.locator('#pane-'+pane);const dims=await active.evaluate(el=>({width:el.clientWidth,scroll:el.scrollWidth,doc:document.documentElement.clientWidth,docscroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.width+1,JSON.stringify({width,name,pane,dims}));assert.ok(dims.docscroll<=dims.doc+1,JSON.stringify({width,name,pane,dims}));
    if(pane==='sentiment')assert.equal(await active.locator('.crypto-sentiment-component > div > span:last-child').evaluateAll(els=>els.every(el=>{const range=document.createRange();range.selectNodeContents(el);return range.getBoundingClientRect().height<=20;})),true);
    if(pane==='technicals'){
     const indicators=active.locator('.crypto-indicator');assert.equal(await indicators.count(),3);
     if(['unavailable','malformed','injection'].includes(name))assert.equal(await active.locator('.crypto-indicator-fill').count(),0);
     if(name==='zero')assert.deepEqual(await active.locator('.crypto-indicator-fill').evaluateAll(els=>els.map(el=>el.style.width)),['0%','0%','0%']);
     if(name==='outside'){assert.match(await indicators.nth(1).innerText(),/-25/);assert.match(await active.innerText(),/\$1\.0000e-10/i);}
     const region=active.locator('.crypto-technicals-table');await region.focus();assert.equal(await region.evaluate(el=>document.activeElement===el),true);
     if(width===390){await page.keyboard.press('ArrowRight');await page.waitForFunction(()=>document.querySelector('.crypto-technicals-table').scrollLeft>0,null,{timeout:5000});}
    }
    let screenshot=null;if(['normal','zero','unavailable'].includes(name)){screenshot=path.join(OUT,'crypto-measurements-'+name+'-'+pane+'-'+width+'.png');await page.screenshot({path:screenshot,fullPage:true});}
    results.push({name,width,pane,screenshot,no_overflow:true,no_markup_execution:true,keyboard_table_scroll:pane==='technicals'&&width===390});
   }
   assert.deepEqual(errors,[]);assert.deepEqual(requests,[]);
  }await context.close();
 }
 fs.writeFileSync(path.join(OUT,'crypto-measurements-browser.json'),JSON.stringify({scope:'Complete actual render function with invented packets and full inline CSS; overview and technicals inspected in isolated DOM. No live application/provider packet reads.',scenarios:results.length,network_requests:0,page_errors:0,results},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,network_requests:0,page_errors:0}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
