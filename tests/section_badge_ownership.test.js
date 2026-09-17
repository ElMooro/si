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
