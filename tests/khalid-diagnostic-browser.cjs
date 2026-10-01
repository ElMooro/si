const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),cp=require('node:child_process');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'..'),out=process.argv[2];assert.ok(out);fs.mkdirSync(out,{recursive:true});
const base=JSON.parse(cp.execFileSync('python',[path.join(__dirname,'khalid-diagnostic-fixture.py')],{cwd:root,encoding:'utf8'}));
const cases=[['complete',()=>{},4],['old',p=>delete p.risk_authority_diagnostics,0],['unknown',p=>p.risk_authority_diagnostics.schema_version='future',0],
 ['mismatch',p=>p.risk_authority_diagnostics.generated_at='2026-09-03T20:00:00+00:00',0],
 ['duplicate',p=>p.risk_authority_diagnostics.rows[1]=structuredClone(p.risk_authority_diagnostics.rows[0]),2],
 ['hostile',p=>p.risk_authority_diagnostics.rows[0].authority_diagnostic.explanation='<img src=x onerror=alert(1)>PRIVATE',3],
 ['oversized',p=>p.risk_authority_diagnostics.rows[0].authority_diagnostic.explanation='PRIVATE'.repeat(10000),3],
 ['false',p=>p.risk_authority_diagnostics.rows[0].age_h=false,3],['zero',p=>p.risk_authority_diagnostics.rows[0].age_h=0,4],
 ['null',p=>p.risk_authority_diagnostics.rows[0].authority_diagnostic=null,3],
 ['wrong-source',p=>p.risk_authority_diagnostics.rows[0].authority_diagnostic.source_id='credit_composite',3]];
(async()=>{const browser=await chromium.launch({headless:true,executablePath:'/usr/bin/chromium'});const results=[];
try{for(const width of [1440,390]){const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'});let packet;
await context.route('**/*',async route=>{const url=new URL(route.request().url());
if(url.pathname==='/khalid.html'){const html=fs.readFileSync(path.join(root,'khalid.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace('</body>','<script src="/khalid.js"></script></body>');return route.fulfill({contentType:'text/html',body:html});}
if(url.pathname==='/data/khalid.json')return route.fulfill({contentType:'application/json',body:JSON.stringify(packet)});
if(url.pathname==='/khalid.js'||url.pathname.endsWith('.css')){const file=path.join(root,url.pathname);if(fs.existsSync(file))return route.fulfill({body:fs.readFileSync(file),contentType:url.pathname.endsWith('.js')?'application/javascript':'text/css'});}
return route.fulfill({status:404,body:''});});
const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));page.on('dialog',()=>{throw Error('Unexpected executable markup');});
for(const [name,mutate,count] of cases){packet=structuredClone(base);mutate(packet);await page.goto('https://offline.test/khalid.html?case='+name+'#risk');const box=page.locator('#risk-authority-diagnostics');await box.waitFor();
const text=await box.textContent();assert.equal((text.match(/INVALID at publication/g)||[]).length,count,name);assert.equal(await box.locator('[data-source-id]').count(),4);assert.ok(!text.includes('PRIVATE'));assert.equal(await box.locator('img,script').count(),0);
assert.equal(await page.locator('#risk-board-cap').textContent(),'0%');assert.match(await page.locator('#risk-board-decision').textContent(),/STAY IN CASH/);
const geometry=await box.evaluate(n=>({width:n.clientWidth,scroll:n.scrollWidth}));assert.ok(geometry.scroll<=geometry.width+1,JSON.stringify(geometry));
if(name==='complete'){for(const row of base.risk_authority_diagnostics.rows){assert.ok(text.includes(row.authority_diagnostic.explanation.split(' Dated observations')[0]));assert.ok(text.includes(row.source_as_of));}await page.screenshot({path:path.join(out,`complete-${width}.png`),fullPage:true});}
results.push({width,name,displayed:count,geometry});}assert.deepEqual(errors,[]);await context.close();}}
finally{await browser.close();}fs.writeFileSync(path.join(out,'browser.json'),JSON.stringify({offline:true,actual_producer_to_packet_to_page:true,results},null,2));console.log('Diagnostic browser cases passed:',results.length);})();
