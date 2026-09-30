const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const candidate = path.join(__dirname, '..', 'jh-nav-drawer.js');

function setup(file = candidate) {
  const document = {readyState:'loading', addEventListener(){}};
  const element = () => {const classes = new Set();return {isConnected:true, disabled:false, attributes:{}, classList:{add:c=>classes.add(c),remove:c=>classes.delete(c),contains:c=>classes.has(c)},setAttribute(k,v){this.attributes[k]=v;},focus(){document.activeElement=this;}};};
  const opener=element(), drawer=element(), handle=element(), search=element(), backdrop=element(), outside=element();
  document.activeElement=opener;document.getElementById=id=>id==='jhnav-search'?search:null;
  drawer.contains=el=>el===drawer||el===search;
  const timers=[];const context={window:{},document,navigator:{},location:{pathname:'/screener-fixture'},
    localStorage:{getItem:k=>k==='jh_sw_gen'?'3372':null},sessionStorage:{getItem:()=>null},setTimeout:fn=>timers.push(fn)};
  const marker='  if (document.readyState === "loading") {';const source=fs.readFileSync(file,'utf8');assert.equal(source.split(marker).length,2);
  vm.runInNewContext(source.replace(marker,'  window.fixtureSetup=function(d,h,b){drawer=d;handle=h;backdrop=b;};window.fixtureOpen=open;window.fixtureClose=close;\n'+marker),context);
  context.window.fixtureSetup(drawer,handle,backdrop);
  return {document,opener,drawer,handle,search,backdrop,outside,timers,open:context.window.fixtureOpen,close:context.window.fixtureClose};
}

test('opening focuses search, closing restores the original focus before hiding',()=>{
 const x=setup();x.open();assert.equal(x.document.activeElement,x.search);assert.equal(x.drawer.inert,false);assert.equal(x.handle.attributes['aria-expanded'],'true');
 x.close();assert.equal(x.document.activeElement,x.opener);assert.equal(x.drawer.inert,true);assert.equal(x.drawer.attributes['aria-hidden'],'true');assert.equal(x.handle.attributes['aria-expanded'],'false');
});
test('rapid open-close has no delayed focus stealing',()=>{
 const x=setup();x.open();x.close();x.timers.forEach(fn=>fn());assert.equal(x.document.activeElement,x.opener);assert.equal(x.timers.length,0);
});
test('removed or disabled opener falls back to the navigation handle',()=>{
 for(const mutation of [o=>o.isConnected=false,o=>o.disabled=true,o=>o.focus=()=>{throw Error('invented focus failure');}]){
  const x=setup();x.open();mutation(x.opener);x.close();assert.equal(x.document.activeElement,x.handle);
 }
});
test('Escape on an already closed region does not steal unrelated focus',()=>{
 const x=setup();x.document.activeElement=x.outside;x.close();assert.equal(x.document.activeElement,x.outside);
});
test('a non-modal navigation close preserves focus already moved to the page',()=>{
 const x=setup();x.open();x.document.activeElement=x.outside;x.close();assert.equal(x.document.activeElement,x.outside);assert.equal(x.drawer.inert,true);
});
test('repeated opening preserves the original opener',()=>{
 const x=setup();x.open();x.open();x.close();assert.equal(x.document.activeElement,x.opener);
});
test('whole predecessor reproduces offscreen focus after rapid close',()=>{
 const x=setup(path.join(__dirname,'fixtures/navigation/pre-focus-drawer.js.txt'));x.open();x.close();x.timers.forEach(fn=>fn());assert.equal(x.drawer.classList.contains('jhnav-open'),false);assert.equal(x.document.activeElement,x.search);assert.equal(x.drawer.inert,undefined);
});
