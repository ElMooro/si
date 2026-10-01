// Actual producer/projection fixtures; all browser requests intercepted.
const {chromium}=require('playwright'),fs=require('fs'),path=require('path'),assert=require('assert/strict');
const {execFileSync}=require('child_process');const root=path.join(__dirname,'..');
const fixture=JSON.parse(execFileSync('python3',['-B','aws/lambdas/justhodl-tape-truth/tests/run_tests.py','--fixture'],{cwd:root,encoding:'utf8'}));
const why=fs.readFileSync(path.join(root,'why.html'),'utf8');
const modules=why.slice(why.indexOf('<!-- industry-case-module'),why.indexOf('<!-- bottom-desk module'));
const bus=[...why.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)].map(m=>m[1]).find(s=>s.includes('if(window.__JH_TICKER_BUS)return;'));
(async()=>{const b=await chromium.launch({headless:true,executablePath:process.env.TAPE_READER_CHROMIUM||'/usr/bin/chromium'});try{
 const p=await b.newPage();let packets=structuredClone(fixture);const errors=[];p.on('pageerror',e=>errors.push(e.message));
 await p.addInitScript(()=>{Date.now=()=>Date.parse('2026-10-01T23:00:00Z');});
 await p.route('**/*',async route=>{const u=new URL(route.request().url());
  if(u.pathname==='/data/tape-truth.json')return route.fulfill({json:packets.tape});
  if(u.pathname==='/data/industry-case.json')return route.fulfill({json:packets.industry});
  if(u.pathname==='/why-scoped.html')return route.fulfill({contentType:'text/html',body:'<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>'+modules+'<script>'+bus+'</script></body></html>'});
  if(['/tape-truth.html','/industry-case.html','/jh-tape-truth.js'].includes(u.pathname))return route.fulfill({contentType:u.pathname.endsWith('.js')?'application/javascript':'text/html',body:fs.readFileSync(path.join(root,u.pathname.slice(1)),'utf8')});
  return route.fulfill({contentType:'application/javascript',body:''});
 });
 for(const width of [1440,390]){
  await p.setViewportSize({width,height:900});
  await p.goto('http://tape-audit.local/tape-truth.html');await p.waitForSelector('#verdicts .card');
  assert.match(await p.locator('#verdicts').innerText(),/2026-01-06/);assert.match(await p.locator('#cvd').innerText(),/600/);assert.match(await p.locator('#finra').innerText(),/0.25/);
  assert.equal(await p.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),true);
  await p.goto('http://tape-audit.local/industry-case.html?t=SPY');await p.waitForFunction(()=>document.getElementById('qa').textContent.includes('2026-01-06'));
  await p.locator('#tk').fill('NVDA');await p.locator('#tk').press('Enter');assert.match(await p.locator('#ct').innerText(),/^NVDA/);assert.match(await p.locator('#qa').innerText(),/calls and conviction withheld/);
  await p.goto('http://tape-audit.local/why-scoped.html?ticker=SPY');await p.waitForFunction(()=>document.getElementById('tt_strip').textContent.includes('SPY · selected'));
  await p.evaluate(()=>history.replaceState(null,'','?ticker=NVDA'));assert.match(await p.locator('#tt_strip').innerText(),/NVDA · selected/);assert.doesNotMatch(await p.locator('#tt_strip').innerText(),/SPY · selected/);assert.match(await p.locator('#ic_body').innerText(),/Tape observations/);
 }
 packets.tape.symbols.SPY.gex.regime={toString:null};
 packets.tape.symbols.SPY.cvd.series=Array.from({length:200000},(_,i)=>({d:'2026-01-06',close:i,cum_cvd:-i}));
 await p.goto('http://tape-audit.local/tape-truth.html');await p.waitForSelector('#cvd svg');
 assert.match(await p.locator('#cvd').innerText(),/last 1000 of 200000 supplied rows/);
 assert.match(await p.locator('#cvd').innerText(),/NVDA/);assert.match(await p.locator('#finra').innerText(),/0.25/);
 assert.match(await p.locator('#gex').innerText(),/SPY/);assert.equal(await p.locator('#cvd a').first().getAttribute('href'),'https://justhodl-data-proxy.raafouis.workers.dev/data/tape-truth.json');
 assert.equal(await p.locator('#cvd polyline').first().evaluate(el=>el.points.numberOfItems),1000);
 delete packets.tape.measurement_contract;packets.tape.status='LIVE';packets.tape.symbols.SPY.verdict={call:'GENUINE_UP',conviction:99};packets.industry.cases.SPY.tape={call:'GENUINE_UP',conviction:99};
 await p.goto('http://tape-audit.local/tape-truth.html');await p.waitForFunction(()=>document.getElementById('sub').textContent.includes('unavailable'));assert.equal(await p.locator('#verdicts').innerText(),'');
 await p.goto('http://tape-audit.local/industry-case.html?t=SPY');await p.waitForFunction(()=>document.getElementById('qa').textContent.includes('Tape observations unavailable'));assert.doesNotMatch(await p.locator('#qa').innerText(),/GENUINE_UP|conviction 99/);
 await p.goto('http://tape-audit.local/why-scoped.html?ticker=SPY');await p.waitForFunction(()=>document.getElementById('tt_strip').textContent.includes('unavailable'));assert.match(await p.locator('#ic_body').innerText(),/Tape observations unavailable/);
 assert.deepEqual(errors,[]);console.log('PASS: actual producer/projection through three renderers, 1440/390 tape layout, industry Enter control, original why ticker bus switches, legacy withholding; all requests intercepted. Known pre-existing industry league cls warning outside this slice.');
 }finally{await b.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
