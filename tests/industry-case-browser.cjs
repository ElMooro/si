// Actual page and captured public packet; every request is intercepted.
const {chromium}=require('playwright'),fs=require('fs'),path=require('path'),assert=require('assert/strict');
const {execFileSync}=require('child_process');
const root=path.join(__dirname,'..'),captured=require('./fixtures/industry-case-public-20260818.json').packet;
let packet=captured;
(async()=>{const browser=await chromium.launch({headless:true,executablePath:process.env.TAPE_READER_CHROMIUM||'/usr/bin/chromium'});try{
 const page=await browser.newPage(),errors=[],warnings=[];
 page.on('pageerror',e=>errors.push(e.message));page.on('console',m=>{if(m.type()==='warning')warnings.push(m.text());});
 await page.route('**/*',route=>{const u=new URL(route.request().url());
  if(u.pathname==='/data/industry-case.json')return route.fulfill({json:packet});
  if(['/industry-case.html','/jh-tape-truth.js'].includes(u.pathname))return route.fulfill({contentType:u.pathname.endsWith('.js')?'application/javascript':'text/html',body:fs.readFileSync(path.join(root,u.pathname.slice(1)),'utf8')});
  return route.fulfill({contentType:'application/javascript',body:''});
 });
 for(const width of [1440,390]){
  await page.setViewportSize({width,height:900});await page.goto('http://industry-test.local/industry-case.html?t=NVDA');
  await page.waitForSelector('#inds tr[data-ind]');assert.equal(await page.locator('#inds tr[data-ind]').count(),149);
  assert.match(await page.locator('#sub').innerText(),/As of 2026-08-18/);assert.match(await page.locator('#qa').innerText(),/Tape observations unavailable/);
  await page.locator('#inds tr[data-ind="Semiconductors"]').click();await page.waitForSelector('#imem td');
  assert.equal(new URL(page.url()).search,'?ind=Semiconductors');assert.equal(await page.locator('#icards .card').count(),4);
  const v=captured.industries.Semiconductors;assert.match(await page.locator('#icards').innerText(),new RegExp(v.ret_coverage+'/'+v.n+' covered'));
  assert.equal(await page.locator('#imem a').count(),105);
  const tickers=()=>page.locator('#imem a').allTextContents();const sorted=v.members.map(m=>m.t).sort();
  await page.locator('#imem th[data-k="t"]').click();assert.deepEqual(await tickers(),sorted);
  await page.locator('#imem th[data-k="t"]').click();assert.deepEqual(await tickers(),sorted.slice().reverse());
  await page.locator('#imem a[href="industry-case.html?t=AMD"]').click();await page.waitForFunction(()=>document.getElementById('ct').textContent.startsWith('AMD'));
  assert.equal(new URL(page.url()).search,'?t=AMD');assert.match(await page.locator('#qa').innerText(),/Tape observations unavailable/);
  await page.locator('#tk').fill('NVDA');await page.locator('#tk').press('Enter');assert.match(await page.locator('#ct').innerText(),/^NVDA/);
  await page.goto('http://industry-test.local/industry-case.html?ind=Semiconductors');await page.waitForSelector('#imem td');assert.equal(await page.locator('#icards .card').count(),4);assert.equal(await page.locator('#imem a').count(),105);
 }
 packet=JSON.parse(execFileSync('python3',['-B','aws/lambdas/justhodl-industry-case/tests/test_publication.py','--fixture'],{cwd:root,encoding:'utf8'}));
 for(const width of [1440,390]){
  await page.setViewportSize({width,height:900});await page.goto('http://industry-test.local/industry-case.html?t=T0000');
  await page.waitForSelector('#inds tr[data-ind]');assert.equal(await page.locator('#inds tr[data-ind]').count(),2);
  assert.match(await page.locator('#sub').innerText(),/publication time only.*freshness UNKNOWN/);
  assert.match(await page.locator('#qa').innerText(),/50.0%/);
  assert.match(await page.locator('#ai').innerText(),/Recorded cohort/);
  await page.locator('details summary').click();
  assert.match(await page.locator('#sourceQualification').innerText(),/2026-08-18T03:00:00Z/);
  assert.match(await page.locator('#sourceQualification').innerText(),/2026-08-14/);
  await page.locator('#tk').fill('T0001');await page.locator('#tk').press('Enter');
  assert.match(await page.locator('#ct').innerText(),/^T0001/);
 }
 assert.deepEqual(warnings,[]);assert.deepEqual(errors,[]);
 console.log('PASS: captured 149-industry league, 105-member direct route, four cards/coverage, two-way sorting, member-link and Enter navigation, original date and legacy tape withholding, 1440/390; new producer publication/source qualification and deterministic fallback; no warnings/errors; all requests intercepted.');
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
