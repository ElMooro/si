const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const R=path.join(__dirname,'..'),D=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);fs.mkdirSync(D,{recursive:true});
const scripts=['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js'];
const html=fs.readFileSync(path.join(R,'crypto/index.html'),'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'');
const served=html.replace('</body>',scripts.map(p=>'<script src="/'+p+'"></script>').join('')+'</body>');
const docs={'/data/cryptoquant-series.json':{series:{invented:{d:['2026-01-01'],v:[0],unit:'invented unit'}},twins:{invented:{d:['2010-01-01'],v:[1]}}},
 '/data/cryptoquant-onchain.json':{metrics:{invented:{value:false,z365:null,label:'Invented zero test'}},composite_onchain_risk_z:null},'/data/cq-feed.json':{metrics:{}},'/data/cq-catalog.json':{catalog:{}},'/data/config/cryptoquant-spec.json':{metrics:[]},'/cq-universe.json':{rows:[]}};
const output={scope:'Complete crypto HTML/CSS with only the three complete observation/fuse modules executed. All requests intercepted; invented packets only. Other crypto engines and scripts are not executed or qualified.',whole_inputs:docs,cases:[]};
(async()=>{const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_CHANNEL||undefined,headless:true});try{for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'}),page=await context.newPage(),requests=[],errors=[];let fail=true;
 page.on('pageerror',e=>errors.push(e.message));await context.addInitScript(()=>{let offset=0;const old=Date.now;Date.now=()=>old()+offset;window.advance=ms=>offset+=ms;});
 await context.route('**/*',route=>{const u=new URL(route.request().url());requests.push(u.pathname);const send=(body,type='application/json',status=200)=>route.fulfill({status,contentType:type,body:typeof body==='string'?body:JSON.stringify(body)});
  if(u.host==='invented.justhodl.test'&&u.pathname==='/crypto/')return send(served,'text/html');
  if(scripts.includes(u.pathname.slice(1)))return send(fs.readFileSync(path.join(R,u.pathname.slice(1)),'utf8'),'application/javascript');
  if(Object.hasOwn(docs,u.pathname)||u.pathname==='/assets/cq-universe.json')return fail?send({},'application/json',503):send(docs[u.pathname]||docs['/cq-universe.json']);return route.abort('blockedbyclient');
 });
 await page.goto('https://invented.justhodl.test/crypto/');
 const paint=()=>page.evaluate(async()=>{await JHCqFuse.load();document.querySelector('#main').innerHTML=JHCqFuse.paneHTML({});document.querySelector('#pane-cq').classList.add('active');return JHCqFuse.pack();});
 const failed=await paint();assert.equal(failed.n_series,null);assert.equal(failed.n_snaps,null);assert.equal(await page.locator('[data-cq-cache] tbody tr').count(),7);assert.match(await page.locator('[data-cq-cache]').textContent(),/Unavailable/);
 const failShot=path.join(D,'cq-unavailable-'+width+'.png');await page.screenshot({path:failShot});
 fail=false;await page.evaluate(()=>advance(30000));const recovered=await paint();assert.equal(recovered.n_chartable,1);assert.match(await page.locator('[data-cq-cache]').textContent(),/Download checked within five minutes/);assert.match(await page.locator('#pane-cq').textContent(),/z —/);
 assert.doesNotMatch(await page.locator('#pane-cq').textContent(),/2010→|twins to 2010|LEDGER-GRADED/);assert.equal(await page.locator('a[href="/chart.html?s=CQ:invented"]').count(),1);
 const statusTable=page.getByRole('region',{name:'Observation source download status'});await statusTable.focus();assert.equal(await statusTable.evaluate(e=>e===document.activeElement),true);
 const recoveredShot=path.join(D,'cq-recovered-'+width+'.png');await page.screenshot({path:recoveredShot});
 const layout=await page.locator('[data-cq-cache]').evaluate(e=>({width:e.clientWidth,scroll:e.scrollWidth,document:document.documentElement.scrollWidth,viewport:innerWidth}));assert.ok(layout.scroll<=layout.width+2,JSON.stringify(layout));assert.equal(errors.length,0,JSON.stringify(errors));
 output.cases.push({width,requests,errors,whole_failed:failed,whole_recovered:recovered,layout,failed_screenshot:failShot,recovered_screenshot:recoveredShot,actual_network_requests:0});await context.close();
}fs.writeFileSync(path.join(D,'browser-qa.json'),JSON.stringify(output,null,2)+'\n');console.log(JSON.stringify({passed:true,widths:[1440,390],actual_network_requests:0}));}finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
