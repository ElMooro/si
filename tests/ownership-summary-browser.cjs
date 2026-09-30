// Intercepted local synthetic preview, never live acceptance or vendor access.
const {chromium}=require('playwright'),fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const fixture=require('./ownership-summary-fixture.cjs'),root=path.resolve(__dirname,'..');
(async()=>{const browser=await chromium.launch({executablePath:process.env.CHROMIUM_PATH||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{for(const width of [1440,390])for(const name of ['etf-holdings.html','flow-lookthrough.html']){
  const f=fixture(),m=f.manifest(),g=m.cohorts.find(g=>g.eligible_fund_count),now=Date.parse(f.p.generated_at);g.lower_bound_valid_until=new Date(now+3600000).toISOString();f.resign(m);
  // Bind the same amended synthetic manifest through the lookthrough proof too.
  const look=f.packets.lookthrough;look.ownership_summary=f.p.ownership_summary;
  const {replay,...body}=look,base='data/holdings-lookthrough-research/',output=f.put(body,'outputs',base),run=JSON.parse(f.artifacts[replay.manifest_key]);run.output=output;run.output_sha256=output.sha256;look.replay={manifest_key:f.put(run,'runs',base).key,output_sha256:output.sha256};
  const page=await browser.newPage({viewport:{width,height:1000}}),errors=[],requests=[];
  page.on('pageerror',e=>errors.push(e.message));await page.clock.install({time:new Date(now)});
  await page.route('**/*',async route=>{const key=new URL(route.request().url()).pathname.slice(1);requests.push(key);
   if(key==='data/etf-holdings-research.json'||key==='data/flow-lookthrough.json')return route.fulfill({contentType:'application/json',body:JSON.stringify(key.includes('lookthrough')?look:f.p)});
   if(f.artifacts[key])return route.fulfill({contentType:'application/json',body:f.artifacts[key]});
   if(['etf-holdings.html','flow-lookthrough.html','jh-etf-holdings.js','jh-sector-research.css'].includes(key))return route.fulfill({body:fs.readFileSync(path.join(root,key)),contentType:key.endsWith('.js')?'text/javascript':key.endsWith('.css')?'text/css':'text/html'});
   return route.abort();});
  await page.goto('http://localhost/'+name);await page.locator('[data-hd-table] table').waitFor();
  assert.equal(requests.some(k=>/\/memberships\//.test(k)),false);assert.equal(requests.includes(f.p.ownership_summary.manifest.key),false);
  await page.locator('[data-own-open]').click();await page.waitForFunction(()=>document.querySelector('[data-own-status]').textContent.startsWith('Metadata verified'));
  assert.equal(await page.locator('[data-own-cohort]').inputValue(),'');assert.equal(requests.some(k=>g.parts.some(r=>r.key===k)),false);
  await page.locator('[data-own-cohort]').selectOption(g.cohort_id);await page.locator('[data-own-next]').click();await page.locator('[data-own-result] table').waitFor();
  assert.match(await page.locator('[data-own-coverage]').innerText(),/1 eligible \/ 300 configured/);
  assert.match(await page.locator('[data-own-result]').innerText(),/200 \/ 307/);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await page.screenshot({path:'/tmp/ownership-cohort-'+width+'-'+name+'.png'});
  const before=requests.length;await page.locator('[data-own-view]').selectOption('lower');await page.locator('[data-own-next]').click();await page.locator('[data-own-result] table').waitFor();assert.equal(requests.length,before);
  await page.clock.fastForward(3600000);assert.match(await page.locator('[data-own-result]').innerText(),/lower bound unavailable/);
  await page.locator('[data-own-view]').selectOption('qualified');await page.locator('[data-own-next]').click();await page.locator('[data-own-result] table').waitFor();
  await page.locator('[data-own-cancel]').click();assert.equal(await page.locator('[data-own-result]').innerText(),'');
  await page.locator('[data-own-view]').selectOption('raw');await page.locator('[data-own-next]').click();await page.locator('[data-own-result] table').waitFor();assert.match(await page.locator('[data-own-result]').innerText(),/not ranked/);
  f.artifacts[f.p.ownership_summary.manifest.key]+=' ';await page.locator('[data-own-open]').click();await page.waitForFunction(()=>document.querySelector('[data-own-status]').textContent.startsWith('Summary unavailable:'));
  assert.equal(await page.locator('[data-own-result]').innerText(),'');
  assert.equal(await page.locator('[data-hd-heat-load]').count(),1);assert.ok(await page.locator('[data-hd-table] table').count());assert.deepEqual(errors,[]);
  console.log('PASS',name,width,'explicit cohort, lazy proof/page, cache, strict independent expiry, cancel, old-panel isolation, no overflow/errors');await page.close();
 }}finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
