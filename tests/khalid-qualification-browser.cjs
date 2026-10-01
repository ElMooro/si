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
  assert.match(await panel.textContent(),/Existing backend qualification — PASS/);
  assert.match(await panel.textContent(),/Requested strategy qualification — UNRESOLVED/);
  if(pageName==='khalid.html') {
   assert.equal(await panel.locator('.sn-card').count(),10);
   await panel.getByRole('button',{name:'Next',exact:true}).focus();await page.keyboard.press('Enter');
   assert.equal(await panel.locator('.sn-card').count(),2);
   await panel.getByRole('button',{name:'Previous',exact:true}).focus();await page.keyboard.press('Enter');
   assert.equal(await panel.locator('.sn-card').count(),10);
  }
  const find=panel.getByRole('searchbox');await find.focus();await find.fill('MISSING');assert.equal(await panel.locator('.sn-card').count(),0);
  await find.fill(pageName==='khalid.html'?'TEST9':'TEST');assert.equal(await panel.locator('.sn-card').count(),1);
  const button=panel.locator('button[aria-pressed]');await button.focus();await page.keyboard.press('Enter');
  assert.equal(await button.getAttribute('aria-pressed'),'true');assert.equal(await button.evaluate(n=>document.activeElement===n),true);
  const summary=panel.locator('.sn-criterion summary').first();await summary.focus();await page.keyboard.press('Enter');
  assert.equal(await summary.evaluate(n=>n.parentElement.open),true);
  assert.match(await summary.locator('..').textContent(),/Availability|Available/);
  const geometry=await panel.evaluate(n=>({client:n.clientWidth,scroll:n.scrollWidth,viewport:innerWidth,box:n.getBoundingClientRect().toJSON()}));
  assert.ok(geometry.scroll<=geometry.client+1,JSON.stringify(geometry));
  const state=await panel.locator('.sn-card').first().textContent();
  await page.screenshot({path:path.join(out,pageName+'-'+width+'.png')});
  assert.equal(requests.filter(x=>x==='/data/khalid.json').length,1,'page/controller/panel must share one request');
  // Same revision cannot preserve a previously valid result for a new body.
  packet.qualification_evidence.schema_version='future.v999';
  await panel.getByRole('button',{name:'Refresh data',exact:true}).click();
  await panel.getByText(/Missing or unsupported/).waitFor();
  assert.equal(await panel.locator('.sn-card').count(),0);
  packet.qualification_evidence.schema_version=fixture.qualification_evidence.schema_version;
  await panel.getByRole('button',{name:'Refresh data',exact:true}).click();
  await panel.locator('.sn-card').first().waitFor();
  assert.equal(requests.filter(x=>x==='/data/khalid.json').length,3);
  const freshnessCases=[];
  for(const mode of ['deadline','visibilitychange','pageshow','focus','publication']) {
   packet.qualification_evidence.expires_at=mode==='publication'?'2026-10-01T05:00:00Z':fixture.qualification_evidence.expires_at;
   await page.evaluate(async ({packet,pageName})=>{
    __now=Date.parse('2026-10-01T04:05:00Z');__visibility='visible';window.__fixtureFeed=packet;
    await jhSniperMount(document.getElementById(pageName==='chart.html'?'ws-sniper':'k-sniper-host'),{force:true});
   },{packet,pageName});
   assert.match(await panel.textContent(),/Existing backend qualification — PASS/);
   const edge=Date.parse(mode==='publication'?'2026-10-01T05:00:00Z':'2026-10-01T06:00:00Z');
   const timers=await page.evaluate(()=>__timers.filter(t=>!t.cancelled).map(t=>({due:t.due,delay:t.delay})));
   assert.equal(timers.length,1,'remount must clear the old timer');
   assert.equal(timers[0].due,edge+(mode==='publication'?0:1));
   assert.equal(await page.evaluate(t=>{__now=t;return jhSniperQualification(__fixtureFeed,__now).valid;},edge-1),true);
   if(mode!=='publication')assert.equal(await page.evaluate(t=>{__now=t;return jhSniperQualification(__fixtureFeed,__now).valid;},edge),true);
   if(mode==='deadline'||mode==='publication') {
    await page.evaluate(t=>{__now=t;__timers.filter(x=>!x.cancelled).at(-1).fn();},edge+(mode==='publication'?0:1));
   } else {
    await page.evaluate(t=>{__visibility='hidden';document.dispatchEvent(new Event('visibilitychange'));__now=t;},edge+1000);
    // Model a throttled background timer: no callback has run yet.
    assert.ok(await panel.locator('.sn-card').count()>0);
    await page.evaluate(event=>{
     __visibility='visible';
     if(event==='visibilitychange')document.dispatchEvent(new Event(event));
     else if(event==='pageshow')window.dispatchEvent(new PageTransitionEvent(event,{persisted:true}));
     else window.dispatchEvent(new Event(event));
    },mode);
   }
   assert.equal(await panel.locator('.sn-card').count(),0);
   assert.match(await panel.textContent(),/UNAVAILABLE/);
   assert.doesNotMatch(await panel.textContent(),/Existing backend qualification — PASS/);
   assert.equal(await page.evaluate(()=>__timers.filter(t=>!t.cancelled).length),0);
   freshnessCases.push({mode,first_invalid_ms:edge+(mode==='publication'?0:1),badge_withdrawn_without_interaction:true});
  }
  await page.evaluate(()=>{__now=Date.parse('2026-10-01T04:05:00Z');});
  if(pageName==='chart.html') {
   await page.evaluate(()=>{jhActive='ABSENT';jhOpenWorkspace('sniper');});
   await page.getByText(/No backend qualification for ABSENT/).waitFor();
   assert.equal(await page.locator('.sn-card').count(),0);
   await page.keyboard.press('Escape');assert.equal(await page.locator('#ws-overlay').getAttribute('class'),'');
   assert.equal(await page.evaluate(()=>document.getElementById('ws-sniper')._qualificationCleanup),null);
   assert.equal(await page.locator('#ws-sniper').textContent(),'');
  }
  assert.deepEqual(errors,[]);
  assert.ok(!requests.some(x=>/yf-ohlc|yahoo|sp500.json|bottom.json/.test(x)));
  evidence.cases.push({page:pageName,width,geometry,keyboard_filter_toggle_details:true,freshness_cases:freshnessCases,unknown_chart_symbol_unavailable:pageName==='chart.html',errors,requests,rendered_card:state});
  await context.close();
 }
 fs.writeFileSync(path.join(out,'browser-qa.json'),JSON.stringify(evidence,null,2)+'\n');console.log(JSON.stringify({cases:evidence.cases.length,passed:true,actual_network_requests:0}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1)});
