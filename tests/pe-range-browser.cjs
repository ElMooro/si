const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const parserModule={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parserModule.exports,parserModule);const acorn=parserModule.exports;
const W=path.join(__dirname,'..'),OUT=process.env.JH_QA_OUT||fs.mkdtempSync(path.join(require('node:os').tmpdir(),'jh-pe-range-'));
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
const cases=[
 {name:'normal',values:{pe_low:10,pe_high:30,pe_pctile:25},text:'position 25%'},
 {name:'zero',values:{pe_low:10,pe_high:30,pe_pctile:0},text:'position 0%'},
 {name:'unavailable',values:{},text:'P/E range unavailable'},
 {name:'malformed',values:{pe_low:10,pe_high:30,pe_pctile:true},text:'position unavailable'},
 {name:'injection',values:{pe_low:10,pe_high:30,pe_pctile:'"><img src="https://fixture.invalid/x" onerror="window.injected=true"><script>window.injected=true</script>'},text:'position unavailable'},
 {name:'long-finite',values:{pe_low:0.000000000000000123456789,pe_high:Number.MAX_VALUE,pe_pctile:99.12345678901234},text:'position 99.12345678901234%'}
];
(async()=>{
 fs.mkdirSync(OUT,{recursive:true});
 const browser=await chromium.launch({executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',headless:true});const results=[];
 try{
 for(const name of ['alpha-scoreboard','opportunities'])for(const width of [390,1280]){
  const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),attempted=[],errors=[];
  await context.route('**/*',route=>{attempted.push(route.request().url());return route.abort();});const page=await context.newPage();page.on('pageerror',e=>errors.push(String(e)));
  const src=sources(name);await page.setContent('<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>'+src.styles+'</style></head><body><main id="fixture" style="max-width:100%;padding:10px"></main></body></html>');
  await page.addScriptTag({content:fs.readFileSync(path.join(W,'jh-short-position-context.js'),'utf8')});
  await page.addScriptTag({content:'let RESEARCH={},RMETA={},RESEARCH_META={},SF={};\n'+src.functions});
  for(const c of cases){
   await page.evaluate(({values,name})=>{RESEARCH={TEST:{thesis:'Invented research status for offline UI verification',...values}};RMETA={};RESEARCH_META={};document.querySelector('#fixture').innerHTML=name==='alpha-scoreboard'?drawer('TEST'):deepDrawer('TEST');},{values:c.values,name});
   if(name==='opportunities'){const outer=page.locator('details.deep > summary');await outer.focus();await page.keyboard.press('Enter');assert.equal(await page.locator('details.deep').evaluate(el=>el.open),true);}
   const range=page.locator('.pe-range-context');await range.waitFor();assert.ok((await range.innerText()).includes(c.text));assert.match(await range.innerText(),/window and comparability unverified/);
   assert.match(await range.getAttribute('title'),/not a percentile/);assert.equal(await range.locator('img,script,iframe').count(),0);assert.equal(await page.evaluate(()=>window.injected),undefined);
   const dims=await range.evaluate(el=>({width:el.clientWidth,scroll:el.scrollWidth,doc:document.documentElement.clientWidth,docscroll:document.documentElement.scrollWidth}));assert.ok(dims.scroll<=dims.width+1,JSON.stringify({name,width,case:c.name,...dims}));assert.ok(dims.docscroll<=dims.doc+1,JSON.stringify({name,width,case:c.name,...dims}));
   let screenshot=null;if(['normal','unavailable'].includes(c.name)){screenshot=path.join(OUT,'pe-range-'+name+'-'+c.name+'-'+width+'.png');await page.screenshot({path:screenshot,fullPage:true});}
   assert.deepEqual(errors,[]);assert.deepEqual(attempted,[]);results.push({page:name,width,case:c.name,screenshot,range_text:await range.innerText(),no_overflow:true,no_markup_execution:true,outer_keyboard_open:name==='opportunities'});
  }
  await context.close();
 }
 fs.writeFileSync(path.join(OUT,'pe-range-browser.json'),JSON.stringify({scope:'Actual drawer functions and complete inline styles, isolated DOM, invented inputs. No live page navigation, application packets or producer execution.',scenarios:results.length,network_requests:0,page_errors:0,results},null,2)+'\n');console.log(JSON.stringify({scenarios:results.length,network_requests:0,page_errors:0}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
