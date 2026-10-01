const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const parserModule={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parserModule.exports,parserModule);const acorn=parserModule.exports;
const W=path.join(__dirname,'..'),OUT=process.env.JH_QA_OUT||fs.mkdtempSync(path.join(require('node:os').tmpdir(),'jh-short-position-')),flags={calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function fixture(){return {row:{...flags,observation_freshness_verified:false,identity_verified:false,ticker:'TEST',reported_ticker_key:'TEST',source_row:'/by_ticker/TEST',short_interest_shares:0,settlement_date:'2026-09-15',latest_reported:false,days_to_cover:2,reported_days_to_cover:999,reported_reconstructed_ratio:2,days_to_cover_basis:'producer_reconstructed_ratio',reported_dtc_status:'provider_differs_from_reconstructed_ratio'},meta:{...flags,artifact:'data/short-interest-tickers.json',read_status:'parsed',context_status:'descriptive_only',source_contract:'short-interest-tickers.v1',context_contract:'short-position-consumer-context.v1',body_sha256:'a'.repeat(64),body_bytes:1000}};}
function sources(name){
 const html=fs.readFileSync(path.join(W,name+'.html'),'utf8'),styles=[...html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)].map(m=>m[1]).join('\n');let functions=[];
 for(const script of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
  if(/\bsrc\s*=|type\s*=\s*["']application\//i.test(script[1]))continue;
  const body=script[2],tree=acorn.parse(body,{ecmaVersion:'latest',sourceType:'script'});
  if(!tree.body.some(n=>n.type==='FunctionDeclaration'&&n.id.name===(name==='alpha-scoreboard'?'drawer':'deepDrawer')))continue;
  assert.equal(functions.length,0);
  for(const node of tree.body){
   if(node.type==='FunctionDeclaration')functions.push(body.slice(node.start,node.end));
   if(node.type==='VariableDeclaration')for(const d of node.declarations)if(['ArrowFunctionExpression','FunctionExpression'].includes(d.init?.type))functions.push(node.kind+' '+body.slice(d.start,d.end)+';');
  }
 }
 assert.ok(functions.length);return {styles,functions:functions.join('\n')};
}
(async()=>{
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true});const results=[];
 try{
 for(const name of ['alpha-scoreboard','opportunities'])for(const width of [390,1280]){
  const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'});const attempted=[];
  await context.route('**/*',route=>{attempted.push(route.request().url());return route.abort();});const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(String(e)));
  const src=sources(name);await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+src.styles+'</style></head><body><main id="fixture" style="max-width:100%;padding:10px"></main></body></html>');
  await page.addScriptTag({content:fs.readFileSync(path.join(W,'jh-short-position-context.js'),'utf8')});
  await page.addScriptTag({content:'let RESEARCH={},RMETA={},RESEARCH_META={},SF={};\n'+src.functions});
  await page.evaluate(({f,name})=>{RESEARCH={TEST:{thesis:'Invented research status for offline UI verification',short_position_context:f.row}};RMETA={confirmation_feeds:{short_interest:f.meta}};RESEARCH_META={short_interest:f.meta};document.querySelector('#fixture').innerHTML=name==='alpha-scoreboard'?drawer('TEST'):deepDrawer('TEST');},{f:fixture(),name});
  if(name==='opportunities'){const outer=page.locator('details.deep > summary');await outer.focus();await page.keyboard.press('Enter');}
  const section=page.locator('.jh-short-position');await section.waitFor();assert.match(await section.innerText(),/0 shares/);assert.match(await section.innerText(),/Explicitly historical/);
  const summary=section.locator('summary');await summary.focus();await page.keyboard.press('Enter');assert.equal(await section.locator('details').evaluate(el=>el.open),true);await page.keyboard.press('Tab');assert.equal(await section.locator('a').evaluate(el=>el===document.activeElement),true);
  const dims=await section.evaluate(el=>({width:el.clientWidth,scroll:el.scrollWidth,doc:document.documentElement.clientWidth,docscroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.width+1,JSON.stringify(dims));assert.ok(dims.docscroll<=dims.doc+1,JSON.stringify(dims));
  const image=path.join(OUT,'short-position-'+name+'-'+width+'.png');await page.screenshot({path:image,fullPage:true});
  const f=fixture();f.row=null;await page.evaluate(({f,name})=>{RESEARCH.TEST.short_position_context=f.row;RESEARCH.TEST.short_pct=98;document.querySelector('#fixture').innerHTML=name==='alpha-scoreboard'?drawer('TEST'):deepDrawer('TEST');},{f,name});
  if(name==='opportunities'){const outer=page.locator('details.deep > summary');await outer.focus();await page.keyboard.press('Enter');}
  assert.match(await section.innerText(),/unavailable/);assert.doesNotMatch(await section.innerText(),/98%|0 shares/);assert.deepEqual(errors,[]);assert.deepEqual(attempted,[]);
  results.push({page:name,width,screenshot:image,zero:true,historical:true,keyboard_details:true,keyboard_source_link:true,no_overflow:true,withheld_legacy_percentage:true,page_errors:0,network_requests:0});await context.close();
 }
 fs.writeFileSync(path.join(OUT,'short-position-browser.json'),JSON.stringify({scope:'Actual drawer functions and complete inline page styles, isolated DOM. Invented inputs only; full-page fetching/navigation and real application data are not tested.',results},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,network_requests:0,page_errors:0}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
