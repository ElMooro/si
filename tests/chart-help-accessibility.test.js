const test=require('node:test'),assert=require('node:assert/strict');
const {chrome,context,source}=require('./helpers/chart-annotation-harness.cjs');
const event=()=>({key:'Escape',stopPropagation(){this.stopped=true;},preventDefault(){this.prevented=true;}});
const click=b=>b.onclick(event());
test('native modal semantics and initial close focus; repeated close and restoration',()=>{
 const {w,nodes,document}=chrome();w.jhInduxLegend(context());const trigger=nodes.get('legend').querySelector('[data-annotation-timing]');
 for(let i=0;i<5;i++){trigger.focus();click(trigger);const d=nodes.get('indhelp');assert.equal(d.tagName,'DIALOG');assert.equal(d.open,true);assert.equal(d['aria-labelledby'],'indhelp-title');assert.equal(document.activeElement,nodes.get('helpx'));click(nodes.get('helpx'));assert.equal(d.open,false);assert.equal(document.activeElement,trigger);}
});
test('internal detail navigation retains original trigger; Escape closes only help',()=>{
 const {w,nodes,document,node}=chrome();const parent=node();parent.className='on';nodes.set('inddlg',parent);const trigger=node('button');trigger.closest=s=>s==='#inddlg, #indset'?parent:null;trigger.focus();
 w.jhInduxHelp('volume-timing',trigger);const d=nodes.get('indhelp');click(d.querySelector("[data-kind='dist']"));assert.equal(document.activeElement,nodes.get('helpx'));const e=event();d.onkeydown(e);assert.ok(e.stopped&&e.prevented);assert.equal(d.open,false);assert.equal(parent.className,'on');assert.equal(document.activeElement,trigger);
});
test('different explicit/active triggers, native cancel, backdrop and external close restore focus',()=>{
 for(const mode of ['cancel','backdrop','external']){const {w,nodes,document,node}=chrome();const trigger=node('button');trigger.focus();w.jhInduxHelp('sc');const d=nodes.get('indhelp');if(mode==='cancel')d.oncancel(event());if(mode==='backdrop')d.onclick({target:d});if(mode==='external')d.close();assert.equal(d.open,false);assert.equal(document.activeElement,trigger);}
});
test('removed opener falls back to replacement Timing; late close event cannot hide reopened help',()=>{
 const {w,nodes,document}=chrome();w.jhInduxLegend(context());let trigger=nodes.get('legend').querySelector('[data-annotation-timing]');click(trigger);w.jhInduxLegend(context());const replacement=nodes.get('legend').querySelector('[data-annotation-timing]');click(nodes.get('helpx'));assert.equal(document.activeElement,replacement);click(replacement);const d=nodes.get('indhelp');d.onclose();assert.equal(d.open,true);assert.equal(d.className,'on');assert.equal(document.activeElement,nodes.get('helpx'));
});
test('help copy and classifier sources are not part of the accessibility repair',()=>{
 const old=source('tests/fixtures/chart-help-indux-predecessor.js.txt'),current=source('jh-chart-indux.js');
 const copy=s=>s.slice(s.indexOf('    var TAPE = {'),s.indexOf('    var d = ',s.indexOf('    var TAPE = {')));
 assert.equal(copy(current),copy(old));assert.match(current,/#indhelp\[open\]\{display:flex\}/);assert.doesNotMatch(current,/setAttribute\("aria-modal"/,'fallback must not claim modality');
});

test('hidden, collapsed, removed, disabled, inert, display:none and focus-refusing openers try visible fallback',()=>{
 for(const state of ['hidden','collapse','removed','disabled','inert','displayNone','refused','throws']) {
  const {w,nodes,document,node}=chrome();w.jhInduxLegend(context());const fallback=nodes.get('legend').querySelector('[data-annotation-timing]'),trigger=node('button');
  trigger.focus();w.jhInduxHelp('sc',trigger);
  if(state==='hidden'||state==='collapse') trigger.computedStyle={visibility:state,display:'block'};
  if(state==='removed') trigger.isConnected=false;
  if(state==='disabled')trigger.disabled=true;
  if(state==='inert')trigger.closest=()=>({});
  if(state==='displayNone')trigger.getClientRects=()=>[];
  if(state==='refused')trigger.focus=()=>{};
  if(state==='throws')trigger.focus=()=>{throw new Error('focus refused');};
  click(nodes.get('helpx'));assert.equal(document.activeElement,fallback,state);
 }
});
test('focus refusal by first fallback continues to a later visible control',()=>{
 const {w,nodes,document,node}=chrome(),parent=node(),first=node('button'),second=node('button'),trigger=node('button');parent.querySelectorAll=()=>[first,second];trigger.closest=()=>parent;
 w.jhInduxHelp('sc',trigger);trigger.isConnected=false;first.focus=()=>{};click(nodes.get('helpx'));assert.equal(document.activeElement,second);
});
test('unsupported showModal keeps nonmodal help readable and keyboard dismissible without a trap',()=>{
 const {w,nodes,document,node}=chrome();const create=document.createElement;document.createElement=tag=>{const n=create(tag);if(tag==='dialog')n.showModal=undefined;return n;};
 const trigger=node('button');trigger.focus();w.jhInduxHelp('volume-timing',trigger);const d=nodes.get('indhelp');assert.equal(d.tagName,'DIV');assert.equal(d.role,'dialog');assert.equal(d['aria-modal'],undefined);assert.equal(d.dataset.helpNonmodal,'1');assert.equal(d.open,true);assert.equal(document.activeElement,nodes.get('helpx'));
 const tab=event();tab.key='Tab';d.onkeydown(tab);assert.equal(tab.prevented,undefined,'Tab remains native and unconfined');
 d.onkeydown(event());assert.equal(d.open,false);assert.equal(document.activeElement,trigger);
 w.jhInduxHelp('sc',trigger);click(nodes.get('helpx'));assert.equal(d.open,false);assert.equal(document.activeElement,trigger);
});
