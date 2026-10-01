// Optional actual-browser gate: node tests/harness-mode-a-browser.cjs (Playwright + Chromium).
const path=require('node:path');
const {chromium}=require('playwright');const fs=require('fs');const assert=require('node:assert/strict');
(async()=>{const browser=await chromium.launch({headless:true,executablePath:'/usr/bin/chromium',args:['--no-sandbox']});
const html=fs.readFileSync(path.resolve(__dirname,'../backtests.html'),'utf8');
const live=[{signal_type:'fixture',graded:3,pending:0,hit_pct:0,avg_excess_pct:0,median_excess_pct:0,artifacts:0}];
const meta={model:{uplift_pp:1,test_take_precision:50,test_base_hit:49,n_train:70,n_test:30,avg_excess_taken_pct:2,avg_excess_all_pct:1},gates:[{type:'fixture',ticker:'TEST',conf:.5,meta_p:.6,verdict:'TAKE'}],n_take:1,n_pending_gated:1,threshold:.6};
let n=0;
for(const width of [1440,390])for(const [name,packet] of Object.entries({legacy:{n_pass:8,live_signal_types:live,rules:[{PASS:true,oos:{sr:98765}}],methodology:'VALIDATED DEPLOYABLE'},missing:null,forged:{n_pass:999,live_signal_types:live,generated_at:'2999-01-01T00:00:00Z',mode_a_qualification:{contract:'future.v999',status:'VALIDATED'}},blocked:{n_pass:0,live_signal_types:live,mode_a_qualification:{contract:'backtest-harness-mode-a-withdrawal.v1',status:'BLOCKED'}}})){
const page=await browser.newPage({viewport:{width,height:1000}});const errors=[];page.on('pageerror',e=>errors.push(String(e)));
await page.route('**/*',async route=>{const url=route.request().url();if(url.includes('backtest-harness.json'))return route.fulfill({json:packet});if(url.includes('meta-labeler.json'))return route.fulfill({json:meta});if(url.endsWith('.js'))return route.fulfill({contentType:'text/javascript',body:''});if(url.endsWith('/backtests.html'))return route.fulfill({contentType:'text/html',body:html});return route.fulfill({contentType:'text/html',body:'<p>Mocked navigation destination</p>'});});
await page.goto('https://fixture.test/backtests.html');await page.locator('#mg').getByText('TAKE',{exact:true}).waitFor();
assert.equal(await page.locator('#ta tr').count(),8);assert.equal(await page.locator('#ta .pill').allTextContents().then(x=>x.every(v=>v==='BLOCKED')),true);
assert.doesNotMatch(await page.locator('#ta').innerText(),/PASS|FAIL|98765/);assert.match(await page.locator('#hero').innerText(),/^0/);
assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,`${width} ${name} overflow`);
if(packet)assert.match(await page.locator('#tb').innerText(),/fixture/);
assert.deepEqual(errors,[]);
if(name==='legacy')await page.screenshot({path:`/tmp/harness-withdrawal-${width}.png`,fullPage:true});
await page.getByRole('link',{name:'Board',exact:true}).click();assert.equal(page.url(),'https://fixture.test/signal-board.html');n++;await page.close();}
await browser.close();console.log(`${n} intercepted-browser scenarios passed, 1440/390px; no external requests; Mode A blocked, Mode B and TAKE visible, Board navigation works, no page errors or horizontal overflow.`);
})().catch(e=>{console.error(e);process.exit(1)});
