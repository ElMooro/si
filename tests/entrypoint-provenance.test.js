const test=require('node:test');const assert=require('node:assert/strict');
const inspector=require('../jh-data-inspector.js');
class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.dataset={};this._text='';}
 set textContent(v){this._text=String(v);this.children=[];} get textContent(){return this._text+this.children.map(x=>x.textContent||'').join('');}
 append(...rows){this.children.push(...rows);} replaceChildren(...rows){this.children=rows;this._text='';}
 setAttribute(){} addEventListener(){}
}
test('candidate provenance remains explicit in the actual inspector without fetching an artifact',()=>{
 const oldDoc=global.document,oldFetch=global.fetch;let fetches=0;
 global.document={createElement:tag=>new Element(tag)};global.fetch=()=>{fetches++;throw Error('Unexpected artifact read');};
 try{
  const entry={engine:'example',ownership_evidence:[{file:'legacy.py',line:4,entrypoint_reachability:'unproven',basis:'Static write candidate; configured-handler reachability is unproven.'}],entrypoint_reachability:{status:'unproven',runtime_verified:false}};
  const view=inspector.ownershipView(entry,'a'.repeat(40));
  assert.match(view.textContent,/reachability is unproven/);assert.match(view.textContent,/does not prove the deployed Lambda version/);
  assert.match(view.textContent,/legacy.py:4/);assert.equal(fetches,0);
 }finally{global.document=oldDoc;global.fetch=oldFetch;}
});
