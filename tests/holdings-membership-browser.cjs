// Local synthetic acceptance only. Every request is intercepted; no live hosts.
const {chromium}=require('playwright'),fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'..'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/etf-holdings-native.json')));
const publication=kind=>{const replay=fixture.publications[kind].replay,run=JSON.parse(fixture.artifacts[replay.manifest_key]);return {...JSON.parse(fixture.artifacts[run.output.key]),replay};};
(async()=>{
 const browser=await chromium.launch({executablePath:process.env.CHROMIUM_PATH||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{for(const width of [1440,390])for(const name of ['etf-holdings.html','flow-lookthrough.html']){
  const page=await browser.newPage({viewport:{width,height:1000}}),errors=[],requests=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.clock.install({time:new Date(publication('holdings').generated_at)});
  await page.route('**/*',async route=>{
   const key=new URL(route.request().url()).pathname.slice(1);requests.push(key);
   if(key==='data/etf-holdings-research.json'||key==='data/flow-lookthrough.json')return route.fulfill({contentType:'application/json',body:JSON.stringify(publication(key.includes('lookthrough')?'lookthrough':'holdings'))});
   if(fixture.artifacts[key])return route.fulfill({contentType:'application/json',body:fixture.artifacts[key]});
   if(['etf-holdings.html','flow-lookthrough.html','jh-etf-holdings.js','jh-sector-research.css'].includes(key))return route.fulfill({contentType:key.endsWith('.js')?'text/javascript':key.endsWith('.css')?'text/css':'text/html',body:fs.readFileSync(path.join(root,key))});
   return route.abort();
  });
  await page.goto('http://localhost/'+name);await page.locator('[data-hd-heat-load]').waitFor();
  await page.locator('[data-hd-table] table').waitFor();
  assert.equal(requests.some(k=>/\/memberships\//.test(k)),false);
  await page.locator('[data-hd-heat-funds]').fill('ARKK, SPY');await page.locator('[data-hd-heat-date]').fill('2026-09-17');
  await page.locator('[data-hd-heat-load]').click();await page.waitForFunction(()=>document.querySelector('[data-hd-heat-status]').textContent.startsWith('Selected sample verified'));
  assert.match(await page.locator('[data-hd-heat-result]').innerText(),/eligible \/ 2 selected \/ 300 configured/);
  const count=requests.length;await page.locator('[data-hd-heat-load]').click();await page.waitForFunction(()=>document.querySelector('[data-hd-heat-status]').textContent.includes('0 network requests'));
  assert.equal(requests.length,count);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await page.screenshot({path:'/tmp/holdings-heatmap-top-'+width+'-'+name+'.png'});
  await page.screenshot({path:'/tmp/holdings-heatmap-'+width+'-'+name+'.png',fullPage:true});
  await page.locator('[data-hd-heat-cancel]').click();assert.equal(await page.locator('[data-hd-heat-result]').innerText(),'');
  await page.locator('[data-hd-heat-funds]').fill('NOTREAL');await page.locator('[data-hd-heat-load]').click();assert.match(await page.locator('[data-hd-heat-status]').innerText(),/unavailable/);
  assert.ok(await page.locator('[data-hd-table] table').count());assert.deepEqual(errors,[]);
  console.log('PASS',name,width,'no overflow/errors; opt-in/cache/cancel/isolated failure');await page.close();
 }}finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
