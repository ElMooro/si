/* Offline real-dossier replay plus failure/mutation cases. Never reaches live AWS. */
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),{chromium}=require('playwright');
const SOURCE=path.resolve(__dirname,'..'),ROOT=path.resolve(process.env.JH_STATIC_ROOT||SOURCE),DATA=path.resolve(process.env.JH_NETWORK_REPLAY||path.join(SOURCE,'..','network-replay'));
const OUT=path.resolve(process.env.JH_BROWSER_OUTPUT||path.join(SOURCE,'..','network-browser'));fs.mkdirSync(OUT,{recursive:true});
(async()=>{const browser=await chromium.launch({headless:true,executablePath:'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'});const results=[];
try{for(const width of [1440,390])for(const scenario of ['native','missing','tampered','untrusted-text']){
 const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),page=await context.newPage(),errors=[],requests=[];
 await page.clock.setFixedTime(new Date('2026-10-07T17:01:00Z'));
 const injected='<img src="/rn-injected" onerror="window.rnInjected=true">';
 let alteredManifest,alteredShard,alteredKey;
 if(scenario==='untrusted-text'){
  alteredManifest=JSON.parse(fs.readFileSync(path.join(DATA,'data/research-network.json'),'utf8'));
  const id='listed_security:NVDA',shardId=alteredManifest.entity_states[id].shard,ref=alteredManifest.shards[shardId];alteredKey=ref.key;
  const shard=JSON.parse(fs.readFileSync(path.join(DATA,alteredKey),'utf8'));shard.entities[id].thesis.what_is_happening=injected;
  alteredShard=Buffer.from(JSON.stringify(shard));ref.bytes=alteredShard.length;ref.sha256=crypto.createHash('sha256').update(alteredShard).digest('hex');
 }
 page.on('pageerror',e=>errors.push(e.message));if(context.routeWebSocket)await context.routeWebSocket('**',ws=>ws.close());
 await context.route('**/*',async route=>{const u=new URL(route.request().url());requests.push({method:route.request().method(),path:u.pathname});
  if(u.pathname==='/dossier.html')return route.fulfill({contentType:'text/html',body:fs.readFileSync(path.join(ROOT,'dossier.html'))});
  if(u.pathname==='/data/research-network.json'&&scenario==='missing')return route.fulfill({status:404,body:'unavailable'});
  if(alteredManifest&&u.pathname==='/data/research-network.json')return route.fulfill({contentType:'application/json',body:JSON.stringify(alteredManifest)});
  if(alteredShard&&u.pathname==='/'+alteredKey)return route.fulfill({contentType:'application/json',body:alteredShard});
  let file=path.resolve(u.pathname.startsWith('/data/')?DATA:ROOT,'.'+decodeURIComponent(u.pathname));
  if((file.startsWith(DATA+path.sep)||file.startsWith(ROOT+path.sep))&&fs.existsSync(file)&&fs.statSync(file).isFile()){
   let body=fs.readFileSync(file);if(scenario==='tampered'&&/\/publications\/.*\/[a-f0-9]{2}\.json$/.test(u.pathname))body=Buffer.concat([body,Buffer.from(' ')]);
   return route.fulfill({contentType:u.pathname.endsWith('.json')?'application/json':u.pathname.endsWith('.css')?'text/css':'application/javascript',body});
  }
  if(u.pathname.endsWith('.json'))return route.fulfill({contentType:'application/json',body:'{}'});
  return route.fulfill({status:404,body:'offline unavailable'});
 });
 await page.goto('https://network.justhodl.test/dossier.html');
 await page.waitForFunction(()=>!document.querySelector('#rn-status')?.textContent.includes('Loading shared'),null,{timeout:20000});
 if(scenario==='missing'){assert.match(await page.locator('#rn-status').textContent(),/unavailable/);}
 else if(scenario==='tampered'){await page.waitForFunction(()=>document.querySelector('#rn-view')?.textContent.includes('verification failed'));}
 else{
  await page.waitForFunction(()=>document.querySelector('#rn-view')?.textContent.includes('Attributed evidence'));
  assert.equal(await page.locator('#rn-overview > .rn-grid > details').count(),16);
  const content=await page.locator('#rn-view').textContent();assert.match(content,/NVDA/);assert.match(content,/Invalidation|invalidate/);assert.match(content,/holding-specific context/);
  assert.match(await page.locator('#rn-status').textContent(),/within publication SLA/);
  if(scenario==='untrusted-text'){assert.ok(content.includes(injected));assert.equal(await page.evaluate(()=>window.rnInjected),undefined);assert.ok(!requests.some(r=>r.path==='/rn-injected'));}
  await page.locator('#rn-symbol').fill('NO_SUCH_SYMBOL');await page.locator('#rn-symbol').press('Enter');assert.match(await page.locator('#rn-view').textContent(),/No research dossier/);
  await page.locator('#rn-symbol').fill('NVDA');await page.locator('#rn-symbol').press('Enter');await page.waitForFunction(()=>document.querySelector('#rn-view')?.textContent.includes('Attributed evidence'));
  await page.locator('#rn-view details summary').first().focus();await page.keyboard.press('Enter');assert.equal(await page.locator('#rn-view details').first().getAttribute('open'),'');
  await page.getByRole('button',{name:'Refresh research',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#rn-view')?.textContent.includes('Attributed evidence'));
 }
 assert.deepEqual(errors,[]);assert.ok(requests.every(r=>r.method==='GET'));assert.ok(!requests.some(r=>/portfolio\/|data\/brain|\/private\//.test(r.path)));
 await page.locator('#research-network').scrollIntoViewIfNeeded();const sizes=await page.evaluate(()=>({client:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));assert.ok(sizes.scroll<=sizes.client+1,JSON.stringify(sizes));
 await page.screenshot({path:path.join(OUT,width+'-'+scenario+'.png')});results.push({width,scenario,errors,sizes,requests:requests.length});await context.close();
 }fs.writeFileSync(path.join(OUT,'results.json'),JSON.stringify(results,null,2));console.log(JSON.stringify({passed:true,cases:results.length,results}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
