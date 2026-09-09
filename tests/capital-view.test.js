const test=require('node:test');const assert=require('node:assert/strict');const {execFileSync}=require('node:child_process');const path=require('node:path');
const api=require('../capital-view.js');
const payload=JSON.parse(execFileSync('python3',[path.join(__dirname,'fixtures/capital_view_fixture.py')],{encoding:'utf8'}));
test('Actual producer contracts grant only their fresh permitted capital',()=>{
 assert.deepEqual(api.permissionErrors(payload.katlin,'katlin'),[]);
 assert.deepEqual(api.permissionErrors(payload.sizer,'risk-sizer'),[]);
 assert(payload.katlin.basket.core.every(x=>x.weight_pct<=10));
});
test('Expiry is evaluated at use time and clears basket and recommendation allocations',()=>{
 for(const [kind,p] of [['katlin',payload.katlin],['risk-sizer',payload.sizer]]){
  const errors=api.permissionErrors(p,kind,Date.parse(p.expires_at));assert(errors.length);
  const held=api.restrictedCopy(p,kind,errors);
  assert.equal(kind==='katlin'?held.war_room.entries_allowed:held.entries_allowed,false);
  if(kind==='katlin'){assert.equal(held.basket.cash_pct,100);assert.deepEqual(held.basket.core,[]);}
  else assert(held.sized_recommendations.every(x=>x.recommended_size_pct===0));
 }
});
test('Future, malformed cap and mismatched authority contracts never permit display',()=>{
 for(const change of [p=>p.authority.exposure_cap_pct=NaN,p=>p.authority.generated_at='2099-01-01T00:00:00Z',p=>p.authority.schema_version='wrong',p=>p.final_constraint_check.gross_ok=false,p=>p.authority.source_health.pop()]){
  const p=structuredClone(payload.sizer);change(p);assert(api.permissionErrors(p,'risk-sizer').length);
 }
});
test('Evidence tree retains nested objects, null and every trailing array row',()=>{
 class Node{constructor(tag){this.tag=tag;this.childNodes=[];this.textContent='';this.style={};this.events={};}append(...xs){this.childNodes.push(...xs);}addEventListener(k,f){this.events[k]=f;}}
 const doc={createElement:t=>new Node(t)},value={rows:Array.from({length:201},(_,i)=>({i,nested:{value:i===200?'TAIL_SENTINEL':null}})),missing:null};
 const tree=api.evidenceNode('root',value,doc);
 function expand(n){if(n.tag==='details'){n.open=true;n.events.toggle();}n.childNodes.forEach(expand);}expand(tree);
 function text(n){return n.textContent+' '+n.childNodes.map(text).join(' ');}const all=text(tree);
 assert(all.includes('TAIL_SENTINEL'));assert(all.includes('missing: null'));assert(!all.includes('[object Object]'));
});
