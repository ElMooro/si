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
 assert.equal(copy(current),copy(old));assert.match(current,/#indhelp\[open\]\{display:flex\}/);assert.doesNotMatch(current,/aria-modal/,'native showModal supplies modality, not a false ARIA assertion');
});
