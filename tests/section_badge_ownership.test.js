const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../jh-sections.js'),'utf8');
const badgeCode=source.slice(source.indexOf('  function badge('),source.indexOf('  function toast('));

function setup(){
  const badges=[];
  const heading={insertBefore(b){badges.push(b);},firstChild:null};
  const document={
    querySelectorAll(selector){const owner=selector.match(/data-for="([^"]+)"/)[1];return badges.filter(b=>!b.removed&&b.dataset.for===owner);},
    querySelector(selector){return this.querySelectorAll(selector)[0]||null;},
    createElement(){return {dataset:{},classList:{add(){}},addEventListener(){},remove(){this.removed=true;}};}
  };
  const c=vm.createContext({document,CSS:{escape:s=>s},page:'yield-curve',ownHeading:()=>null,prevHeading:()=>heading,toast(){}});
  vm.runInContext(badgeCode,c);
  const block=id=>({id,querySelector(){return badges.find(b=>!b.removed)||null;},insertBefore:b=>badges.push(b)});
  return {c,document,badges,block};
}

test('refreshes do not duplicate a badge inserted in the preceding heading',()=>{
  const {c,document,block}=setup(),el=block('chart');
  for(let i=0;i<100;i++)c.badge(el,'5','chart',false);
  assert.equal(document.querySelectorAll('.jh-secbadge[data-for="chart"]').length,1);
});

test('parent updates preserve a nested child badge and their own badge',()=>{
  const {c,document,block}=setup(),parent=block('parent'),child=block('child');
  c.badge(child,'5.1','child',true);
  c.badge(parent,'5','parent',false);
  for(let i=0;i<100;i++){c.badge(parent,'5','parent',false);c.badge(child,'5.1','child',true);}
  assert.equal(document.querySelectorAll('.jh-secbadge[data-for="child"]').length,1);
  assert.equal(document.querySelectorAll('.jh-secbadge[data-for="parent"]').length,1);
});

test('existing duplicate owner badges are cleaned without removing other sections',()=>{
  const {c,document,badges,block}=setup(),parent=block('parent'),child=block('child');
  c.badge(parent,'5','parent',false);c.badge(child,'5.1','child',true);
  const duplicate=document.createElement('a');duplicate.dataset={for:'parent',n:'5'};badges.push(duplicate);
  c.badge(parent,'6','parent',false);
  assert.equal(document.querySelectorAll('.jh-secbadge[data-for="parent"]').length,1);
  assert.equal(document.querySelectorAll('.jh-secbadge[data-for="parent"]')[0].dataset.n,'6');
  assert.equal(document.querySelectorAll('.jh-secbadge[data-for="child"]').length,1);
});

const childrenCode=source.slice(source.indexOf('  function bigChildren('),source.indexOf('  // The content axis:'));
const previousCode=source.slice(source.indexOf('  function prevHeading('),source.indexOf('  function titleOf('));
function previousHeading(owner,wrapped=false){
  const heading={textContent:'Inspect the reported observations',children:[],claims:[],
    hasAttribute:()=>false,matches:s=>s==='h2',querySelectorAll(){return this.claims;}};
  if(owner!==null)heading.claims.push({dataset:{for:owner}});
  const wrapper={children:[heading],firstElementChild:heading,hasAttribute:()=>false,matches:()=>false};
  const child={id:'observations-table',previousElementSibling:wrapped?wrapper:heading};
  const c=vm.createContext({HEAD:'h2',BIG_H:96,text:el=>el.textContent||'',rect:()=>({height:20})});
  vm.runInContext(previousCode,c);
  return {heading,child,find:()=>c.prevHeading(child)};
}

test('a child cannot claim the direct or wrapped heading already numbered for its parent',()=>{
  for(const wrapped of [false,true]){
    const {find}=previousHeading('observations-section',wrapped);
    assert.equal(find(),null);
  }
});

test('an unclaimed preceding heading and this block\'s own heading remain usable on refresh',()=>{
  for(const wrapped of [false,true]){
    const {heading,find}=previousHeading(null,wrapped);
    assert.equal(find(),heading);
    heading.claims.push({dataset:{for:'observations-table'}});
    for(let i=0;i<100;i++)assert.equal(find(),heading);
    heading.claims.push({dataset:{for:'another-block'}});
    assert.equal(find(),null);
  }
});

function responsiveChildren(){
  class Element {
    constructor(tag,height,pinned=false){this.tag=tag;this.height=height;this.pinned=pinned;this.dataset={};this.children=[];}
    matches(selector){return selector.split(',').includes(this.tag);}
    hasAttribute(name){return this.pinned&&name==='data-jh-key';}
  }
  const context=vm.createContext({HTMLElement:Element,CHROME:'nav,header,footer',BIG_H:96,BIG_W:.4,
    visible:()=>true,rect:el=>({height:el.height,width:318})});
  vm.runInContext(childrenCode,context);
  return {Element,collect:children=>Array.from(context.bigChildren({children},390))};
}

test('wrapped scenario heading and explanatory paragraphs do not become separate embedded panels',()=>{
  const {Element,collect}=responsiveChildren();
  const heading=new Element('h2',180),intro=new Element('p',210),form=new Element('form',440),note=new Element('p',160);
  assert.deepEqual(collect([heading,intro,form,note]),[form]);
  // Changing the ticker/title or viewport height must not invent another section.
  heading.height=240;intro.height=300;
  assert.deepEqual(collect([heading,intro,form,note]),[form]);
});

test('large controls stay inside their numbered form while authored text pins remain supported',()=>{
  const {Element,collect}=responsiveChildren();
  const control=new Element('label',150),button=new Element('button',110),pin=new Element('p',180,true);
  assert.deepEqual(collect([control,button,pin]),[pin]);
});

test('real content panels remain discoverable at mobile widths',()=>{
  const {Element,collect}=responsiveChildren();
  const table=new Element('div',800),section=new Element('section',600);
  assert.deepEqual(collect([table,section]),[table,section]);
});
