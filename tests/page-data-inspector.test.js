const test=require('node:test');const assert=require('node:assert/strict');
const inspector=require('../jh-data-inspector.js');
class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.events={};this.dataset={};this.attrs={};this._text='';}
 set textContent(value){this._text=String(value);this.children=[];}
 get textContent(){return this._text+this.children.map(x=>x.textContent||'').join('');}
 append(...nodes){this.children.push(...nodes);}
 replaceChildren(...nodes){this.children=[...nodes];this._text='';}
 setAttribute(k,v){this.attrs[k]=v;}
 addEventListener(k,fn){this.events[k]=fn;}
}
function all(root,fn){return [root,...root.children.flatMap(x=>all(x,fn))].filter(fn);}
function dom(){global.document={createElement:tag=>new Element(tag)};return new Element('div');}
test('all nested paths retain null, zero, booleans, later-row-only fields and escaped names',()=>{
 const payload={rows:[null,{a:0,b:false,later:{'a/b~c':'full'}}],empty:[]};
 assert.deepEqual(inspector.leafPaths(payload),['/rows/0','/rows/1/a','/rows/1/b','/rows/1/later/a~1b~0c','/empty']);
 assert.deepEqual(inspector.columns([null,{a:1},{last:true}]),['a','last']);
});
test('actual rendered array paginates every row and includes columns beyond the first row',()=>{
 dom();const rows=Array.from({length:52},(_,i)=>({symbol:'ROW'+i,a:i,b:false,c:0,d:'text',e:1}));rows[0]=null;rows[26].critical_risk_flag='BREACH';
 const view=inspector.collectionView(rows,'/rows');
 assert.ok(view.textContent.includes('critical_risk_flag'));assert.ok(view.textContent.includes('null'));assert.ok(!view.textContent.includes('ROW26'));
 const next=all(view,n=>n.tagName==='button'&&n.textContent==='Next')[0];next.onclick();
 assert.ok(view.textContent.includes('ROW26')&&view.textContent.includes('BREACH'));next.onclick();assert.ok(view.textContent.includes('ROW51'));assert.equal(next.disabled,true);
});
test('complete inspector preserves long text and exposes nested field values through path search',()=>{
 const target=dom(),long='Methodology '.repeat(100);inspector.inspect(target,{methodology:long,nested:{risk:{warning:'CANNOT TRADE'}}},'producer -> output');
 assert.ok(target.textContent.includes(long));const search=all(target,n=>n.tagName==='input')[0];search.value='warning';search.events.input();
 assert.ok(target.textContent.includes('CANNOT TRADE')&&target.textContent.includes('/nested/risk/warning'));
});
test('object field pagination exposes every scalar beyond the old eight-field cap',()=>{
 dom();const obj=Object.fromEntries(Array.from({length:61},(_,i)=>['field'+i,'value'+i]));const view=inspector.collectionView(obj,'');let text=view.textContent;
 const next=all(view,n=>n.tagName==='button'&&n.textContent==='Next')[0];while(!next.disabled){next.onclick();text+=view.textContent;}
 for(let i=0;i<61;i++)assert.ok(text.includes('value'+i));
});
