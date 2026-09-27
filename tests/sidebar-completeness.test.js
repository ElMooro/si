'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const root=path.join(__dirname,'..');
function mount(page='materials-orders.html'){
 const elements=new Map(),events=new Map(),scripts=[];
 function element(tag){
  const e={tag,style:{},attrs:{},listeners:{},setAttribute(k,v){this.attrs[k]=v;},addEventListener(k,v){this.listeners[k]=v;}};
  Object.defineProperty(e,'innerHTML',{get(){return this.html||'';},set(value){this.html=value;for(const match of value.matchAll(/\bid="([^"]+)"/g))if(!elements.has(match[1]))elements.set(match[1],element('fixture'));}});
  return e;
 }
 const document={getElementById:id=>elements.get(id)||null,createElement:element,
  addEventListener:(name,fn)=>events.set(name,fn),body:{appendChild:e=>elements.set(e.id,e)},head:{appendChild:e=>scripts.push(e.src)}};
 const scope={document,location:{pathname:'/'+page,search:''},window:{},URLSearchParams};
 const source=fs.readFileSync(path.join(root,'sidebar.js'),'utf8');
 vm.runInNewContext(source,scope);
 return {elements,events,scripts,source,scope};
}
test('whole sidebar renders Materials Orders, filters and opens/closes without a marker error',()=>{
 const s=mount(),all=s.elements.get('sb_all');
 for(const name of ['materials-orders','material-agreements','officer-change'])assert.ok(all.innerHTML.includes('href="/'+name+'.html"'));
 assert.ok(all.innerHTML.includes('href="/yield-curve.html"'));
 const count=(all.innerHTML.match(/<a /g)||[]).length;assert.ok(count>450);
 s.elements.get('sb_q').listeners.input({target:{value:'materials orders'}});
 assert.equal((all.innerHTML.match(/<a /g)||[]).length,1);
 s.elements.get('sb_btn').onclick();assert.equal(s.elements.get('sb_root').style.left,'0');
 s.events.get('keydown')({key:'Escape'});assert.equal(s.elements.get('sb_root').style.left,'-290px');
 s.elements.get('sb_q').listeners.input({target:{value:''}});assert.equal((all.innerHTML.match(/<a /g)||[]).length,count);
 vm.runInNewContext(s.source,s.scope);assert.equal(s.elements.size,6);
});
test('ticker extension is retained once and unrelated pages make no dynamic script request',()=>{
 assert.deepEqual(mount().scripts,[]);
 const s=mount('ticker.html');assert.deepEqual(s.scripts,['/jh-ticker-desks.js']);
 vm.runInNewContext(s.source,s.scope);assert.deepEqual(s.scripts,['/jh-ticker-desks.js']);
});
test('the complete rejected sidebar marker is retained as a runtime regression',()=>{
 const raw=fs.readFileSync(path.join(__dirname,'fixtures/rejected-sidebar-placeholder.js.txt'));
 assert.equal(raw.length,24);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'70a6e70eb95630e111f93918c361f3382e7ddf2fb20ebb26de6e8c716f8b174c');
 assert.throws(()=>vm.runInNewContext(raw.toString('utf8')),/PLACEHOLDER_WILL_NOT_USE is not defined/);
});
