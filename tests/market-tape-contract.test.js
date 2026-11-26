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
 // 2026-11-26: the tape is written once a day. An hour-old packet is shown, and says how old it is before anything else;
 // a quote in it reads "last print". Nothing older than four days, future-dated or of a legacy schema is shown at all.
 const p=packet();p.generated_at='2026-09-18T15:00:00Z';
 p.items.push({label:'SPX',value:6000,display:'6,000',unit:'index',observation_date:'2026-09-18',observed_at:'2026-09-18T14:59:00Z',frequency:'quote',badge:'LIVE',src:'FMP:^GSPC'});
 const hour=render(p);
 assert.equal(hour.textContent,'');assert.equal(hour.children[0].textContent,'As of 09-18 15:00Z \u00b7 60 min old');
 assert.match(hour.children[0].className,/jhc-stale/);assert.match(hour.children[0].title,/once a day/);assert.equal(hour.children[0].attrs['data-asof'],'2026-09-18T15:00:00Z');
 assert.match(hour.children[1].children[2].textContent,/2026-08-01/);
 assert.match(hour.children[2].children[2].textContent,/09-18 14:59Z last print$/);
 p.generated_at='2026-09-17T21:51:00Z';assert.equal(render(p).children[0].textContent,'As of 09-17 21:51Z \u00b7 18 h old');
 p.generated_at='2026-09-18T15:50:00Z';const fresh=render(p);
 assert.equal(fresh.children[0].attrs['data-sym'],'US CPI SA YoY');assert.doesNotMatch(fresh.children[1].children[2].textContent,/last print/);
 p.generated_at='2026-09-14T15:59:00Z';assert.match(render(p).textContent,/unavailable or stale/);assert.equal(render(p).children.length,0);
 p.generated_at='2026-09-18T16:06:00Z';assert.match(render(p).textContent,/unavailable or stale/);
 p.generated_at='2026-09-18T16:00:00Z';delete p.schema_version;
 assert.equal(render(p).children.length,0);assert.match(render(p).textContent,/unavailable or stale/);
});
test('Malformed values are omitted and coverage gaps remain visible',()=>{
 const p=packet();p.items[0].value=NaN;p.gaps=[{label:'CN GDP',reason:'stale source'}];
 const t=render(p);assert.equal(t.children.length,2);assert.equal(t.children[0].textContent,'Sources');
 assert.equal(t.children[1].textContent,'1 unavailable');assert.match(t.children[1].title,/stale source/);
});

test('Real producer packets preserve all four quote chips and keep badges additive',()=>{
 const {execFileSync}=require('node:child_process');
 const packets=JSON.parse(execFileSync('python3',['-c',`
import json,runpy
from datetime import datetime,timezone
s=runpy.run_path('aws/lambdas/justhodl-market-tape/tests/run_tests.py',run_name='fixture')
cases=[(s['NOW'],0),(s['NOW'],901),(datetime(2026,9,20,16,tzinfo=timezone.utc),43200),(s['NOW'],-301),(s['NOW'],604801)]
print(json.dumps([s['run_tape'](now=n,quote_age=a) for n,a in cases]))
`],{cwd:require('node:path').join(__dirname,'..'),encoding:'utf8'}));
 const labels=['SPX','COMP','BTC','GOLD'];
 for(const [index,p] of packets.entries()){
  const target=element('div');tape.render({createElement:element},target,p,Date.parse(p.generated_at));
  for(const label of labels){
   const chip=target.children.find(c=>c.attrs['data-sym']===label);
   if(index>=3){assert.equal(chip,undefined);assert.ok(p.gaps.some(g=>g.label===label));continue;}
   const row=p.items.find(r=>r.label===label);assert.ok(chip,label);assert.equal(row.sizing_eligible,false);
   assert.match(chip.title,/Unit: /);assert.match(chip.title,/Source: FMP:/);assert.ok(row.evidence.quote);
   assert.ok(['fresh','delayed'].includes(row.quality.status));assert.ok(row.badge);
  }
 }
 assert.equal(packets[2].items.find(r=>r.label==='BTC').badge,'STALE');
 assert.equal(packets[2].items.find(r=>r.label==='GOLD').badge,'STALE');
 assert.equal(packets[2].items.find(r=>r.label==='SPX').badge,'SESSION');
});
