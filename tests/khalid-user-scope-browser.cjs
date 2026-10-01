const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const root=path.join(__dirname,'..'),out=path.resolve(process.argv[2]||'');assert.ok(process.argv[2],'External evidence directory required');fs.mkdirSync(out,{recursive:true});
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/khalid-user-scope-synthetic.json'),'utf8'));
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE||undefined});const evidence=[];try{
for(const pageName of ['khalid.html','chart.html'])for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:1000},isMobile:width===390,hasTouch:width===390,serviceWorkers:'block'});
 await context.addInitScript(()=>{window.__now=Date.parse('2026-10-01T04:05:00Z');Date.now=()=>window.__now;window.jhActive='TEST';});
 const page=await context.newPage(),errors=[],requests=[],packet=structuredClone(fixture);
 // Scope expiry precedes backend expiry, exercising independent invalidation.
 const source=packet.user_scope_evidence.sources[0];source.research_at='2026-09-27T17:00:00Z';source.expires_at='2026-10-01T05:00:00Z';source.finviz_snapshot_at=source.census_snapshot_at=source.research_at;
 page.on('pageerror',e=>errors.push(e.message));
 let html=fs.readFileSync(path.join(root,pageName),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,tag=>/rel=["']stylesheet/.test(tag)&&/href=["']\//.test(tag)?tag:'');
 html=html.replace('</body>','<script src="/jh-khalid-sniper.js"></script>'+(pageName==='khalid.html'?'<script src="/khalid.js"></script>':'<script src="/jh-chart-tvrail.js"></script>')+'</body>');
 await context.route('**/*',async route=>{const u=new URL(route.request().url());requests.push(u.pathname);
  if(u.pathname==='/'+pageName)return route.fulfill({contentType:'text/html',body:html});
  if(['/jh-khalid-sniper.js','/jh-chart-tvrail.js','/khalid.js'].includes(u.pathname))return route.fulfill({contentType:'application/javascript',body:fs.readFileSync(path.join(root,u.pathname.slice(1)),'utf8')});
  if(u.pathname.endsWith('.css')&&!u.pathname.includes('..')&&fs.existsSync(path.join(root,u.pathname)))return route.fulfill({contentType:'text/css',body:fs.readFileSync(path.join(root,u.pathname),'utf8')});
  if(u.pathname==='/data/khalid.json')return route.fulfill({contentType:'application/json',body:JSON.stringify(packet)});
  return route.fulfill({status:404,body:''});
 });
 await page.goto('https://invented.justhodl.test/'+pageName);
 if(pageName==='chart.html')await page.evaluate(()=>jhOpenWorkspace('sniper'));else{await page.locator('[data-view="k-sniper"]').focus();await page.keyboard.press('Enter');}
 const panel=page.locator('.sn-evidence');await panel.locator('.sn-card').first().waitFor();
 const total=pageName==='chart.html'?1:12;
 assert.equal(await panel.locator('.sn-card').count(),Math.min(10,total));
 assert.match(await panel.textContent(),new RegExp('biotechnology exclusions '+(total===1?1:3)+'; SMALL convention failures '+(total===1?1:2)+'; unavailable/unresolved '+(total===1?0:2)));
 const select=panel.getByLabel('Scope evidence filter (display only)');assert.equal(await select.inputValue(),'ALL');
 await select.focus();await page.keyboard.press('ArrowDown');await page.keyboard.press('Enter');
 assert.equal(await select.inputValue(),'BIOTECH');assert.equal(await panel.locator('.sn-card').count(),total===1?1:3);
 await select.selectOption('SMALL');assert.equal(await panel.locator('.sn-card').count(),total===1?1:2);
 await select.selectOption('UNKNOWN');assert.equal(await panel.locator('.sn-card').count(),total===1?0:2);
 const reset=panel.getByRole('button',{name:'Reset filters',exact:true});await reset.focus();await page.keyboard.press('Enter');
 assert.equal(await select.inputValue(),'ALL');assert.equal(await reset.evaluate(n=>document.activeElement===n),true);
 assert.equal(await panel.locator('.sn-card').count(),Math.min(10,total));
 const summary=panel.locator('.sn-scope-check summary').first();await summary.focus();await page.keyboard.press('Enter');assert.equal(await summary.evaluate(n=>n.parentElement.open),true);
 const cap=panel.locator('.sn-scope-check').nth(2);await cap.locator('summary').focus();await page.keyboard.press('Enter');assert.match(await cap.textContent(),/not a user-chosen cutoff/);
 const geometry=await panel.evaluate(n=>({client:n.clientWidth,scroll:n.scrollWidth}));assert.ok(geometry.scroll<=geometry.client+1,JSON.stringify(geometry));
 await page.screenshot({path:path.join(out,pageName+'-'+width+'.png')});
 assert.equal(requests.filter(x=>x==='/data/khalid.json').length,1);
 await page.evaluate(()=>{__now=Date.parse('2026-10-01T05:00:00.001Z');window.dispatchEvent(new Event('focus'));});
 assert.equal(await panel.locator('.sn-scope-check').count(),0);assert.equal(await panel.locator('.sn-card').count(),Math.min(10,total));
 assert.match(await panel.textContent(),/Existing backend qualification — PASS/);assert.match(await panel.textContent(),/Scope evidence — UNAVAILABLE/);
 await select.selectOption('BIOTECH');assert.equal(await panel.locator('.sn-card').count(),0);await reset.click();assert.equal(await panel.locator('.sn-card').count(),Math.min(10,total));
 assert.deepEqual(errors,[]);evidence.push({pageName,width,total,overlappingCounts:true,keyboardFilterAndReset:true,independentExpiry:true,requests:requests.filter(x=>x==='/data/khalid.json').length,geometry,errors});await context.close();
}
fs.writeFileSync(path.join(out,'results.json'),JSON.stringify({scope:'Actual page HTML/CSS and shared module; invented source evidence; all requests intercepted; no live market or production acceptance claim',cases:evidence},null,2));console.log(JSON.stringify(evidence));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
