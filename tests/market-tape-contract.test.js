const test = require('node:test');
const assert = require('node:assert/strict');
const tape = require('../jh-market-tape.js');
const now = Date.parse('2026-09-18T16:00:00Z');
function element(tag) { return {tag, children:[], attrs:{}, textContent:'',
  appendChild(e){this.children.push(e);}, replaceChildren(){this.children=[];this.textContent='';},
  setAttribute(k,v){this.attrs[k]=v;}}; }
function render(p) {const target=element('div');tape.render({createElement:element},target,p,now);return target;}
function packet(){return {schema_version:'2.0',generated_at:'2026-09-18T16:00:00Z',items:[{
  label:'US CPI SA YoY',value:5,display:'5.00%',unit:'% YoY',observation_date:'2026-08-01',
  comparison_date:'2025-08-01',definition:'CPI all urban SA',src:'FRED:CPIAUCSL',quality:{status:'fresh'},evidence:{}}],gaps:[]};}
test('Tape displays observation date and exposes definition and source, without HTML interpolation',()=>{
 const p=packet();p.items[0].label='<img src=x>';const target=render(p),chip=target.children[0];
 assert.equal(chip.children[0].textContent,'<img src=x>');
 assert.match(chip.children[2].textContent,/2026-08-01/);
 assert.match(chip.attrs['aria-label'],/Comparison: 2025-08-01/);
 assert.match(chip.title,/FRED:CPIAUCSL/);assert.equal(target.children[1].href,'/data/market-tape.json');
});
test('A recent page load cannot make an old tape or a legacy schema look current',()=>{
 const p=packet();p.generated_at='2026-09-18T15:00:00Z';
 assert.match(render(p).textContent,/stale/);p.generated_at='2026-09-18T16:00:00Z';delete p.schema_version;
 assert.equal(render(p).children.length,0);
});
test('Malformed values are omitted and coverage gaps remain visible',()=>{
 const p=packet();p.items[0].value=NaN;p.gaps=[{label:'CN GDP',reason:'stale source'}];
 const t=render(p);assert.equal(t.children.length,2);assert.equal(t.children[0].textContent,'Sources');
 assert.equal(t.children[1].textContent,'1 unavailable');assert.match(t.children[1].title,/stale source/);
});
