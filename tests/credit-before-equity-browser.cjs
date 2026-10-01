// Synthetic, intercepted local page only. All requests are fulfilled or aborted.
const {chromium}=require('playwright'),fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const html=fs.readFileSync(path.join(__dirname,'../credit-before-equity.html'),'utf8');
(async()=>{const browser=await chromium.launch({executablePath:process.env.CHROMIUM_PATH||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
try {for(const width of [1440,390]){
const page=await browser.newPage({viewport:{width,height:900}}),errors=[],requests=[];
page.on('pageerror',e=>errors.push(e.message));
const attack='<img src=x onerror="window.injected=true">';
const row={ticker:'TEST',name:attack,signal:'CREDIT_LEADS_DOWN',distance_to_default:0,d_distance_to_default:-1,synthetic_cds_bp:0,d_synthetic_cds_bp:0,d_price_pct:0,credit_direction:'DETERIORATING',regime:attack,prior_obs_date:'2026-09-30'};
let packet={generated_at:'2026-10-01T00:00:00Z',n_names:1,n_leads:1,n_awaiting_history:0,names:[row],leads:[row],thresholds:{equity_flat_band_pct:0},degraded:[attack],gaps:[attack]};
await page.route('**/*',route=>{const url=new URL(route.request().url());requests.push(url.pathname);
if(url.hostname==='credit-preview.invalid'&&url.pathname==='/credit-before-equity.html')return route.fulfill({contentType:'text/html',body:html});
if(url.pathname==='/data/credit-before-equity.json')return route.fulfill({contentType:'application/json',headers:{'Access-Control-Allow-Origin':'*'},body:JSON.stringify(packet)});
return route.abort();});
await page.goto('https://credit-preview.invalid/credit-before-equity.html');await page.locator('#tb tr').waitFor();
assert.match(await page.locator('#evidence').innerText(),/Degraded evidence/);
assert.match(await page.locator('#hero').innerText(),/±0%/);
assert.equal(await page.locator('#tb tr').count(),1);assert.equal(await page.locator('#leads .row').count(),1);
assert.equal(await page.locator('img').count(),0);assert.equal(await page.evaluate(()=>Boolean(window.injected)),false);
assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
await page.keyboard.press('Tab');assert.equal(await page.locator('.issuer-scroll').evaluate(e=>e===document.activeElement),true);
if(width===390){await page.keyboard.press('ArrowRight');await page.waitForFunction(()=>document.querySelector('.issuer-scroll').scrollLeft>0);}
await page.keyboard.press('Tab');assert.equal(await page.evaluate(()=>document.activeElement.getAttribute('href')),'/cds-monitor.html');
await page.screenshot({path:'/tmp/credit-qualification-'+width+'.png',fullPage:true});
packet={...packet,n_names:0,n_leads:0,names:[],leads:[],degraded:['No credit leg']};await page.reload();await page.waitForFunction(()=>document.querySelector('#leads').textContent.includes('unavailable or degraded'));
packet={...packet,n_names:1,names:[{...row,signal:'NONE'}],degraded:[],gaps:[]};await page.reload();await page.waitForFunction(()=>document.querySelector('#leads').textContent.includes('No leads reported by the engine'));
packet={...packet,names:[{...row,signal:'CREDIT_LEADS_UP'}]};await page.reload();await page.waitForFunction(()=>document.querySelector('#evidence').textContent.includes('Lead count does not match the supplied issuer signals'));
assert.match(await page.locator('#leads').innerText(),/unavailable or incomplete/);assert.match(await page.locator('#tb').innerText(),/CREDIT LEADS UP/);
packet={...packet,n_leads:1,n_awaiting_history:1,leads:packet.names};await page.reload();await page.waitForFunction(()=>document.querySelector('#evidence').textContent.includes('Lead and awaiting-history counts together exceed'));
assert.match(await page.locator('#leads').innerText(),/CREDIT LEADS UP/);assert.match(await page.locator('#evidence').innerText(),/Incomplete packet/);
packet={...packet,n_awaiting_history:0,names:[{...row,ticker:'GOOGL',signal:'CREDIT_LEADS_UP',d_price_pct:null,synthetic_cds_bp:null,default_prob_5y_pct:null}]};packet.leads=packet.names;await page.reload();await page.waitForFunction(()=>document.querySelector('#tb').textContent.includes('GOOGL'));
assert.equal(await page.locator('#tb td').nth(6).innerText(),'Unavailable');assert.doesNotMatch(await page.locator('#leads').innerText(),/Unavailable(?:%|bp)/);
assert.deepEqual(errors,[]);assert.equal(requests.filter(p=>p==='/data/credit-before-equity.json').length,6);
console.log(JSON.stringify({width,populated:true,degradedEmpty:true,validNoSignal:true,contradictoryCounts:true,zeroThreshold:true,xssBlocked:true,keyboard:true,overflow:false,externalNetwork:false}));await page.close();
}} finally {await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
