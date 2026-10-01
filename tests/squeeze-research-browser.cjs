const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const W=path.join(__dirname,'..'),OUT=process.env.JH_QA_OUT||fs.mkdtempSync(path.join(require('node:os').tmpdir(),'jh-squeeze-'));fs.mkdirSync(OUT,{recursive:true});
const flags={calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function fixture(){return {contract:'squeeze-pretrigger-research.v1',state:'UNQUALIFIED',portfolio_action:'WAIT',call:null,independent_investment_votes:0,...flags,generated_at:'2026-10-01T18:00:00+00:00',inputs:Object.fromEntries(Object.entries({finra:'data/finra-short.json',short_interest:'data/short-interest-tickers.json',catalyst:'data/catalyst-calendar.json'}).map(([key,artifact])=>[key,{artifact,...flags,identity_verified:false,observation_freshness_verified:false,read_status:key==='finra'?'malformed':'parsed',body_sha256:'a'.repeat(64),body_bytes:key==='finra'?0:2}]))};}
function source(name){
 const html=fs.readFileSync(path.join(W,name+'.html'),'utf8');
 const styles=[...html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)].map(m=>m[1]).join('\n');
 const a=html.indexOf(name==='squeeze'?'  // Squeeze forecast stays unavailable':'function renderSqueeze(d) {');
 const b=html.indexOf(name==='squeeze'?'  // ---------- CROWDED SHORTS':'\nfunction ',a+1);assert.ok(a>=0&&b>a);
 return {styles,code:html.slice(a,b)};
}
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true}),results=[];
 try{
  for(const name of ['squeeze','retail-edges'])for(const width of [390,1280]){
   const context=await browser.newContext({viewport:{width,height:950},serviceWorkers:'block'}),attempted=[];
   await context.route('**/*',route=>{attempted.push(route.request().url());return route.abort();});const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(String(e)));
   const src=source(name);await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+src.styles+'</style></head><body><main style="max-width:100%;padding:15px"><div id="regime" class="regime"></div><div id="kpis" class="grid4"></div><div id="boardSetups"></div></main></body></html>');
   await page.addScriptTag({content:fs.readFileSync(path.join(W,'jh-squeeze-research.js'),'utf8')});
   if(name==='retail-edges')await page.addScriptTag({content:src.code});
   await page.evaluate(({name,code,packet})=>{if(name==='squeeze')Function('pre',code)(packet);else document.getElementById('regime').innerHTML=renderSqueeze(packet);},{name,code:src.code,packet:fixture()});
   const section=page.locator('.jh-squeeze-research');assert.match(await section.innerText(),/WAIT/);
   await section.locator('summary').focus();await page.keyboard.press('Enter');assert.equal(await section.locator('details').evaluate(el=>el.open),true);
   await page.keyboard.press('Tab');assert.equal(await section.locator('a').first().evaluate(el=>el===document.activeElement),true);
   assert.match(await section.innerText(),/Received 0 bytes/);assert.match(await section.innerText(),/malformed/);
   const dims=await section.evaluate(el=>({width:el.clientWidth,scroll:el.scrollWidth,doc:document.documentElement.clientWidth,docscroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.width+1&&dims.docscroll<=dims.doc+1,JSON.stringify(dims));
   const screenshot=path.join(OUT,name+'-'+width+'.png');await page.screenshot({path:screenshot,fullPage:true});
   await page.evaluate(({name,code})=>{const legacy={state:'IMMINENT',signal_strength:99,forward_expectations:{'2m':80}};if(name==='squeeze')Function('pre',code)(legacy);else document.getElementById('regime').innerHTML=renderSqueeze(legacy);},{name,code:src.code});
   assert.match(await section.innerText(),/legacy contract/);assert.doesNotMatch(await section.innerText(),/80%|IMMINENT/);assert.deepEqual(errors,[]);assert.deepEqual(attempted,[]);
   results.push({page:name,width,screenshot,keyboard_details:true,keyboard_source_link:true,no_overflow:true,legacy_withheld:true,malformed_zero_bytes:true,network_requests:0,page_errors:0});await context.close();
  }
  fs.writeFileSync(path.join(OUT,'proof.json'),JSON.stringify({scope:'Actual changed render sections and complete page inline styles; isolated DOM, invented data, no full-page fetching/navigation acceptance.',results},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,page_errors:0,network_requests:0}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
