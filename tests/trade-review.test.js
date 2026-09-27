const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const v=require('../jh-trade-review.js'),freight=require('../jh-freight-review.js');
const raw=zlib.gunzipSync(fs.readFileSync(path.join(__dirname,'fixtures/trade-calendar-browser.json.gz'))),native=()=>JSON.parse(raw),now=Date.parse('2026-09-27T11:00:00Z');
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.value='';}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
 set innerHTML(_){throw Error('Untrusted HTML insertion is forbidden');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../freight-pulse.html'),'utf8'),nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
const other=()=>({generated_at:'2026-09-26T10:00:00Z'});
const freightLegacy=()=>({engine_class:'physical_trade_slow_confirmation',version:'2.0.0',series:{},generated_at:'2026-09-26T11:50:00Z'});

test('whole actual-native synthetic fixture has every one of 92 source series and 30072 monthly positions',()=>{
 const p=native(),r=v.view(p,now);assert.equal(p._synthetic_fixture,true);assert.ok(raw.length>3000000);assert.equal(r.native,true);assert.equal(r.authority,false);assert.equal(r.rows.length,92);assert.equal(r.observations,30072);
 assert.equal(r.review.cpb.calendar.length,319);assert.equal(r.review.cpb.world_trade.latest_month,'2026-07');assert.equal(r.rows[0].s.seasonal_adjustment,'NSA');assert.equal(r.rows[0].s.annualized_three_month_percent,null);
});
test('all displayed arithmetic is checked against original calendar values',()=>{
 for(const mutate of [p=>p.measurement_review.series.IR.level++,p=>p.measurement_review.series.IR.yoy.percent++,p=>p.measurement_review.series.IR.yoy.previous_month='2025-07',p=>p.measurement_review.series.IR.observations[1].month='1985-01',p=>p.measurement_review.series.IR.observations[1].value=999,p=>p.measurement_review.series.IR.returned_rows--,p=>p.measurement_review.cpb.series.ipz_us_qnmi_sp.mom.percent++,p=>p.measurement_review.cpb.series.mgz_us_qnmi_sn.monthly_observations.pop(),p=>p.measurement_review.cpb.calendar[0].month='2000-02',p=>p.measurement_review.cpb.monthly_positions--]){const p=native();mutate(p);assert.throws(()=>v.view(p,now));}
});
test('report route, workbook period, units, weights, compiler and permission identities cannot drift',()=>{
 const mutations=[p=>p.measurement_review.cpb.report.report_url='https://www.cpb.nl/agenda/publicatie-wereldhandelsmonitor-augustus-2026',p=>p.measurement_review.cpb.report.period='2026-08',p=>p.measurement_review.cpb.report.published_at='2026-09-28T00:00:00Z',p=>p.measurement_review.cpb.series.ipz_w1_qnmi_sm.section='Production weighted, seasonally adjusted',p=>p.measurement_review.cpb.series.hfl_w1_pdmi_nn.unit='USD',p=>delete p.measurement_review.cpb.series.mgz_cn_qnmi_sn,p=>p.measurement_review.series.IR.annualized_three_month_percent=1,p=>p.publication_context.manifest.key='data/exposed.json',p=>delete p.compiler_sha256['managed_secret.py'],p=>p.calls_eligible=true,p=>p.measurement_review.cpb.sizing_eligible=true,p=>p.portfolio_action='LONG',p=>p.rate_pressure=10,p=>p.measurement_review.bdi.level=2222,p=>p.generated_at='2026-02-30T00:00:00Z'];
 for(const mutate of mutations){const p=native();mutate(p);assert.throws(()=>v.view(p,now));}
});
test('missing exact months, valid zeros and unavailable overflow are distinct',()=>{
 const p=native(),s=p.measurement_review.series.IR;s.observations.splice(-13,1);s.observations.forEach((r,i)=>r.position=i);s.returned_rows--;s.yoy.status='prior_month_missing';s.yoy.percent=null;assert.equal(v.view(p,now).native,true);
 assert.deepEqual(v.comparison(0,10,'2026-07','2025-07'),{status:'measured',percent:-100,current_month:'2026-07',previous_month:'2025-07',unit:'percent'});
 assert.equal(v.comparison(null,10,'a','b').status,'latest_missing');assert.equal(v.comparison(10,0,'a','b').status,'zero_denominator');assert.equal(v.comparison(1e15,1e-300,'a','b').status,'outside_numeric_range');
 for(const n of ['1_000','NaN','Infinity','1e-999',true,-1])assert.throws(()=>v.number(n));assert.equal(v.number('0e-999'),0);
});
test('DOM pagination retains all sources and all observation pages with inert text',()=>{
 const p=native(),doc=document();p.measurement_review.cpb.series.mgz_cn_qnmi_sn.name='<img src=x onerror=alert(1)>';
 v.render(doc,p,now);assert.match(doc.getElementById('trade-measurement-count').textContent,/30072/);assert.equal(doc.getElementById('trade-choice').children.length,92);assert.match(doc.getElementById('trade-source-count').textContent,/1–20 of 92/);
 for(let i=0;i<4;i++)doc.getElementById('trade-source-next').onclick();assert.match(doc.getElementById('trade-source-count').textContent,/81–92 of 92/);assert.equal(doc.getElementById('trade-source-next').disabled,true);
 assert.match(doc.getElementById('trade-observation-count').textContent,/1–100 of 319/);for(let i=0;i<3;i++)doc.getElementById('trade-next').onclick();assert.match(doc.getElementById('trade-observation-count').textContent,/301–319 of 319/);
 const choice=doc.getElementById('trade-choice');choice.value='IR';choice.onchange();assert.match(doc.getElementById('trade-observation-count').textContent,/1–100 of 500/);assert.match(doc.getElementById('trade-definition').textContent,/NSA changes are not annualized/);
 assert.match(choice.text(),/<img src=x onerror=alert\(1\)>/);
});
test('legacy and corrupt packets clear every prior measurement and stale callback',()=>{
 const doc=document();v.render(doc,native(),now);const oldNext=doc.getElementById('trade-next').onclick,oldChoice=doc.getElementById('trade-choice').onchange;
 v.render(doc,other(),now);oldNext();oldChoice();assert.equal(doc.getElementById('trade-series').children.length,0);assert.equal(doc.getElementById('trade-next').onclick,null);assert.match(doc.getElementById('trade-summary').textContent,/Legacy trade publication/);
 v.render(doc,native(),now);const bad=native();bad.sizing_eligible=true;assert.throws(()=>v.render(doc,bad,now));assert.equal(doc.getElementById('trade-observations').children.length,0);assert.match(doc.getElementById('trade-summary').textContent,/unavailable/);
});
test('existing freight loader supplies one trade packet and keeps source failure boundaries',async()=>{
 const p=native(),doc=document(),calls=[];await freight.mount(doc,async key=>{calls.push(key);return key==='trade'?p:key==='freight'?freightLegacy():other();},now);
 assert.equal(calls.filter(k=>k==='trade').length,1);assert.match(doc.getElementById('trade-measurement-count').textContent,/92|88/);assert.equal(doc.getElementById('trade-choice').children.length,92);
 const details=doc.getElementById('trade-details');details.open=true;details.ontoggle();assert.deepEqual(JSON.parse(doc.getElementById('trade-original').textContent),p);
 await freight.mount(doc,async key=>{if(key==='trade')throw Error('HTTP 503');return key==='freight'?freightLegacy():other();},now);
 assert.equal(doc.getElementById('trade-choice').children.length,0);assert.match(doc.getElementById('trade-summary').textContent,/HTTP 503/);assert.match(doc.getElementById('air-status').textContent,/2026-09-26/);
});
test('late prior load cannot restore trade measurements after a failed refresh',async()=>{
 const doc=document(),resolvers=[];const old=freight.mount(doc,()=>new Promise(resolve=>resolvers.push(resolve)),now);
 await freight.mount(doc,async()=>{throw Error('current failure');},now);resolvers.forEach(r=>r(native()));await old;
 assert.equal(doc.getElementById('trade-series').children.length,0);assert.match(doc.getElementById('trade-summary').textContent,/current failure/);
});
test('complete preceding page and reader are pinned without truncating their earlier content',()=>{
 const originals={'freight-pulse.html':'70a644d2b0b5cdcbdd47073754ec2c56e2766fab68730fca8b6e7b79b7cbe629','jh-freight-review.js':'9fd6004190703f68d88c6635588aa94506efe4bcdf5caf138f660713dc481a58'};
 for(const[file,sha]of Object.entries(originals))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,'fixtures/pre-trade-calendar-'+file+'.txt'))).digest('hex'),sha);
 const html=fs.readFileSync(path.join(__dirname,'../freight-pulse.html'),'utf8');assert.ok(html.indexOf('/jh-trade-review.js')<html.indexOf('/jh-freight-review.js'));assert.match(html,/Every monthly position in the selected trade source/);
});
