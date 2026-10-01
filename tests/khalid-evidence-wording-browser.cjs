const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const root=path.join(__dirname,'..'),out=path.resolve(process.argv[2]||'');
assert.ok(process.argv[2],'Pass external output directory');fs.mkdirSync(out,{recursive:true});
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/khalid-qualification-synthetic.json'),'utf8'));
const evidence={scope:'Actual page HTML/CSS, complete shared qualification module and chart workspace module. Actual Khalid page controller included; unrelated scripts removed; invented backend data only. All browser requests intercepted.',cases:[]};
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE||undefined});try{
 for(const pageName of ['khalid.html','chart.html'])for(const width of [1440,390]) {
  const context=await browser.newContext({viewport:{width,height:1000},isMobile:width===390,hasTouch:width===390,serviceWorkers:'block'});
  await context.addInitScript(()=>{
   window.__now=Date.parse('2026-10-01T04:05:00Z');Date.now=()=>window.__now;window.jhActive='TEST';
   window.__visibility='visible';Object.defineProperty(document,'visibilityState',{get:()=>window.__visibility});
   Object.defineProperty(document,'hidden',{get:()=>window.__visibility==='hidden'});
   window.__timers=[];const set=window.setTimeout,clear=window.clearTimeout;
   window.setTimeout=function(fn,delay,...args){const id=set(fn,delay,...args);if(delay>1000000)window.__timers.push({id,fn,delay,due:Date.now()+delay,cancelled:false});return id;};
   window.clearTimeout=function(id){window.__timers.forEach(t=>{if(t.id===id)t.cancelled=true;});return clear(id);};
  });
  const page=await context.newPage(),errors=[],requests=[];
  const packet=structuredClone(fixture);
  const backend=packet.qualification_evidence.rows[0].existing_backend_qualification;backend.status='FAIL';const risk=backend.criteria.find(c=>c.id==='risk_permission');risk.status='FAIL';risk.value=false;
  packet.inputs=[{key:'data/katlin.json',domain:'opportunities',status:'FRESH',age_h:0.05,max_age_h:36,producer:'justhodl-katlin'}];
  packet.qualification_evidence.rows[0].existing_backend_qualification.sources.find(s=>s.name==='khalid_risk').max_age_h=2;
  if(pageName==='khalid.html')for(let i=1;i<12;i++) {
   const candidate=structuredClone(fixture.opportunity_radar[0]),row=structuredClone(packet.qualification_evidence.rows[0]);
   candidate.ticker=row.ticker='TEST'+i;row.source_path='opportunity_radar/'+i;
   row.existing_backend_qualification.ticker=row.requested_strategy_qualification.ticker=candidate.ticker;
   packet.opportunity_radar.push(candidate);packet.qualification_evidence.rows.push(row);
  }
  page.on('pageerror',e=>errors.push(e.message));
  let html=fs.readFileSync(path.join(root,pageName),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,tag=>/rel=["']stylesheet/.test(tag)&&/href=["']\//.test(tag)?tag:'');
  html=html.replace('</body>','<script src="/jh-khalid-sniper.js"></script>'+(pageName==='khalid.html'?'<script src="/khalid.js"></script>':'')+(pageName==='chart.html'?'<script src="/jh-chart-tvrail.js"></script>':'')+'</body>');
  await context.route('**/*',async route=>{
   const u=new URL(route.request().url());requests.push(u.pathname);
   if(u.pathname==='/'+pageName)return route.fulfill({contentType:'text/html',body:html});
   if(['/jh-khalid-sniper.js','/jh-chart-tvrail.js','/khalid.js'].includes(u.pathname))return route.fulfill({contentType:'application/javascript',body:fs.readFileSync(path.join(root,u.pathname.slice(1)),'utf8')});
   if(u.pathname.endsWith('.css') && !u.pathname.includes('..') && fs.existsSync(path.join(root,u.pathname)))return route.fulfill({contentType:'text/css',body:fs.readFileSync(path.join(root,u.pathname),'utf8')});
   if(u.pathname==='/data/khalid.json')return route.fulfill({contentType:'application/json',body:JSON.stringify(packet)});
   return route.fulfill({status:404,body:''});
  });
  await page.goto('https://invented.justhodl.test/'+pageName);
  if(pageName==='chart.html')await page.evaluate(()=>jhOpenWorkspace('sniper'));
  else { const nav=page.locator('[data-view="k-sniper"]');await nav.focus();await page.keyboard.press('Enter'); }
  const panel=page.locator('.sn-evidence');await panel.locator('.sn-card').first().waitFor();
  assert.match(await panel.textContent(),/Existing backend qualification — FAIL/);
  assert.match(await panel.textContent(),/Requested strategy qualification — UNRESOLVED/);
  const gate=panel.locator('.sn-criterion').filter({has:page.getByText('Existing risk permission — FAIL',{exact:true})}).first();
  await gate.locator('summary').click();assert.match(await gate.textContent(),/Passing criterion: requires risk_allows_entries to be true/);
  assert.equal(await gate.locator('dt').filter({hasText:/^Value$/}).evaluate(n=>n.nextElementSibling.textContent),'false');
  if(pageName==='khalid.html'){
   await page.locator('[data-view="method"]').click();assert.match(await page.locator('#input-table').textContent(),/FRESH0.1h36h/);
   assert.match(await page.locator('[data-jh-key="method"]').textContent(),/publication age, not original research age/);
   await page.screenshot({path:path.join(out,'method-'+width+'.png')});await page.locator('[data-view="k-sniper"]').click();
  }
  const geometry=await panel.evaluate(n=>({client:n.clientWidth,scroll:n.scrollWidth,viewport:innerWidth,box:n.getBoundingClientRect().toJSON()}));
  assert.ok(geometry.scroll<=geometry.client+1,JSON.stringify(geometry));
  const state=await panel.locator('.sn-card').first().textContent();
  await page.screenshot({path:path.join(out,pageName+'-'+width+'.png')});
  assert.equal(requests.filter(x=>x==='/data/khalid.json').length,1,'page/controller/panel must share one request');
  evidence.cases.push({pageName,width,measuredFalse:true,status:'FAIL',passingCriterionExplicit:true,geometry,errors});assert.deepEqual(errors,[]);await context.close();
 }
 fs.writeFileSync(path.join(out,'results.json'),JSON.stringify(evidence,null,2));console.log(JSON.stringify(evidence.cases));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
