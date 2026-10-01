// Optional real-browser acceptance: all requests intercepted; no live data or SDKs.
const {chromium} = require('playwright');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const {execFileSync} = require('node:child_process');
const root = path.join(__dirname, '..');
const fixture = JSON.parse(execFileSync('python3', ['-B', 'aws/lambdas/justhodl-tape-reader/tests/run_tests.py', '--fixture'], {cwd:root,encoding:'utf8'}));
(async () => {
  const browser = await chromium.launch({headless:true, executablePath:process.env.TAPE_READER_CHROMIUM || (fs.existsSync('/usr/bin/chromium') ? '/usr/bin/chromium' : undefined)});
  try {
    const page = await browser.newPage(); const errors=[];
    page.on('pageerror', e=>errors.push(e.message));
    let packet = fixture;
    await page.route('**/*', async route => {
      const u = new URL(route.request().url());
      if(u.pathname.endsWith('/data/tape-reader.json')) return route.fulfill({json:packet});
      if(u.pathname === '/tape-reader.html') return route.fulfill({contentType:'text/html',body:fs.readFileSync(path.join(root,'tape-reader.html'),'utf8')});
      if(u.pathname === '/jh-enhance.js') return route.fulfill({contentType:'application/javascript',body:fs.readFileSync(path.join(root,'jh-enhance.js'),'utf8')});
      return route.fulfill({contentType:'application/javascript',body:''});
    });
    await page.goto('http://tape-audit.local/tape-reader.html');
    await page.waitForSelector('tbody tr');
    assert.equal(await page.title(), 'Tape Reader · JustHodl.AI');
    assert.equal(await page.locator('#nSize').innerText(), '1 / 2');
    assert.equal(await page.locator('tbody tr').nth(1).locator('td').nth(5).innerText(), '—');
    await page.locator('th[data-col="block_ratio"]').click();
    assert.match(await page.locator('tbody tr').last().innerText(), /UNKNOWN/);
    await page.locator('th[data-col="block_ratio"]').focus();
    await page.keyboard.press('Enter');
    assert.match(await page.locator('tbody tr').last().innerText(), /UNKNOWN/);
    assert.match(await page.locator('#jhviz-body').innerText(), /Published daily aggregate activity/);
    assert.doesNotMatch(await page.locator('#tableHost').innerText(), /BLOCK PRINTS|Block×/);
    await page.setViewportSize({width:390,height:844});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth), true);
    packet = {...fixture}; delete packet.measurement_contract;
    await page.reload();
    await page.waitForFunction(()=>document.getElementById('tableHost').textContent.includes('Updated aggregate-activity data unavailable'));
    assert.equal(await page.locator('#topScore').innerText(), '—');
    assert.match(await page.locator('#jhviz-body').innerText(), /Updated aggregate-activity data unavailable/);
    assert.deepEqual(errors, []);
    console.log('PASS: actual producer fixture, unavailable size, sort click/keyboard, mobile width, enhancement and legacy withholding; all network intercepted.');
  } finally { await browser.close(); }
})().catch(e=>{console.error(e);process.exitCode=1;});
