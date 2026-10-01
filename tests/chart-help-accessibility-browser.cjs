// Normal-sandbox only. No remote pages, feeds, actions or TLS overrides.
const {chromium}=require('playwright');
const fs=require('node:fs'),assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({executablePath:process.env.CHART_QA_CHROMIUM || '/usr/bin/chromium',chromiumSandbox:true,headless:true});
 try {
  for(const width of [1440,390]){
   const page=await browser.newPage({viewport:{width,height:900},hasTouch:width===390});await page.route('**/*',r=>r.abort());
   await page.setContent('<html><body><div id="legend"></div><button id="z-minus">Zoom out</button><button id="other">Other help</button></body></html>');
   await page.addScriptTag({content:fs.readFileSync('jh-chart-indux.js','utf8')});
   await page.evaluate(()=>{window.testCtx={INDS:[{id:'volev',n:'Vol events',on:true}],lastBars:[],atTime:null,overlayMap:{},valAt:()=>null,spec:()=>['1d','Daily'],active:'TEST',tf:'1d',compare:[],volOn:false,fmt:String};jhInduxLegend(testCtx);document.getElementById('other').onclick=e=>jhInduxHelp('sc',e.currentTarget);});
   const timing=page.locator('[data-annotation-timing]');
   const openPointer=()=>width===390?timing.tap():timing.click();
   for(let i=0;i<3;i++){
    await timing.focus();await page.keyboard.press('Enter');await page.getByRole('dialog',{name:'Historical annotation timing'}).waitFor();assert.ok(await page.getByRole('button',{name:'Close help',exact:true}).evaluate(n=>n===document.activeElement));
    for(let j=0;j<9;j++){await page.keyboard.press(j%2?'Shift+Tab':'Tab');assert.ok(await page.evaluate(()=>document.querySelector('#indhelp').contains(document.activeElement)));}
    // Background focus attempts must not move focus out of the native modal.
    await page.evaluate(()=>document.getElementById('z-minus').focus());assert.notEqual(await page.evaluate(()=>document.activeElement.id),'z-minus');
    await page.getByRole('button',{name:'DIST · Distribution',exact:true}).click();await page.keyboard.press('Escape');assert.ok(await timing.evaluate(n=>n===document.activeElement));
   }
   await page.locator('#other').click();await page.getByRole('button',{name:'Close help',exact:true}).click();assert.equal(await page.evaluate(()=>document.activeElement.id),'other');
   await openPointer();await page.evaluate(()=>jhInduxLegend(testCtx));await page.getByRole('button',{name:'Close help',exact:true}).click();assert.ok(await page.locator('[data-annotation-timing]').evaluate(n=>n===document.activeElement));
   await openPointer();await page.mouse.click(2,2);assert.equal(await page.locator('#indhelp').evaluate(n=>n.open),false);
   assert.ok(await page.locator('[data-annotation-timing]').evaluate(n=>n===document.activeElement));
   // Native restoration is itself a successful return to the opener, even
   // when its JS focus method is a no-op; it must not be called a failed return.
   await page.locator('#other').click();await page.evaluate(()=>{document.getElementById('other').focus=()=>{};});
   await page.getByRole('button',{name:'Close help',exact:true}).click();assert.equal(await page.evaluate(()=>document.activeElement.id),'other');
   await page.evaluate(()=>{delete document.getElementById('other').focus;});
   for(const state of ['hidden','refused']) {
    await page.locator('#other').click();
    await page.evaluate(state=>{const n=document.getElementById('other');if(state==='hidden')n.style.visibility='hidden';else {
     // Native dialog close may restore the opener without calling its JS focus
     // override. Move that native restoration away so the custom fallback is
     // actually exercised, then refuse its explicit focus request.
     n.addEventListener('focus',()=>document.getElementById('z-minus').focus(),{once:true});n.focus=()=>{};
    }},state);
    await page.getByRole('button',{name:'Close help',exact:true}).click();
    assert.ok(await timing.evaluate(n=>n===document.activeElement));
    await page.evaluate(()=>{const n=document.getElementById('other');n.style.visibility='';delete n.focus;});
   }
   await page.evaluate(()=>{document.getElementById('indhelp').remove();Object.defineProperty(HTMLDialogElement.prototype,'showModal',{value:undefined,configurable:true});});
   await openPointer();
   assert.equal(await page.locator('#indhelp').evaluate(n=>n.tagName),'DIV');
   assert.equal(await page.locator('#indhelp').getAttribute('aria-modal'),null);
   // A nonmodal popover can physically cover nearby controls on mobile.
   // Place this fixture control outside its rectangle before testing that the
   // rest of the page remains operable; do not force a click through the help.
   await page.locator('#z-minus').evaluate(n=>{n.style.cssText='position:fixed;left:8px;bottom:8px';});
   const outside=await page.locator('#z-minus').evaluate(n=>{const a=n.getBoundingClientRect(),b=document.querySelector('#indhelp').getBoundingClientRect();return a.bottom<=b.top||a.top>=b.bottom||a.right<=b.left||a.left>=b.right;});assert.ok(outside);
   await page.locator('#z-minus').click();assert.equal(await page.evaluate(()=>document.activeElement.id),'z-minus');
   await page.getByRole('button',{name:'Close help',exact:true}).focus();await page.keyboard.press('Escape');
   assert.ok(await timing.evaluate(n=>n===document.activeElement));
   console.log(JSON.stringify({width,keyboard_and_pointer:'PASS',scope:'isolated synthetic UI, no live chart data'}));await page.close();
  }
 }finally{await browser.close();}
})().catch(e=>{console.error(e.message);process.exitCode=1;});
