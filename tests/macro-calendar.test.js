const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const v=require('../jh-macro-calendar.js'),raw=zlib.gunzipSync(fs.readFileSync(path.join(__dirname,'fixtures/macro-calendar-browser.json.gz'))),native=()=>JSON.parse(raw),now=Date.parse('2026-09-27T11:30:00Z');
class Element{constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.value='';}appendChild(n){this.children.push(n);return n;}replaceChildren(){this.children=[];this.textContent='';}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}set innerHTML(_){throw Error('Untrusted HTML forbidden');}}
function document(){const page=fs.readFileSync(path.join(__dirname,'../macro-leads.html'),'utf8'),nodes=new Map([...page.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}

test('complete native fixture preserves all 113 sources and 170740 monthly positions',()=>{
 const p=native(),out=v.view(p,now);assert.equal(p._synthetic_fixture,true);assert.ok(raw.length>2600000);assert.equal(out.native,true);assert.equal(out.authority,false);assert.equal(out.rows.length,113);assert.equal(out.positions,170740);assert.equal(out.review.gpr.level,117.9188461303711);assert.equal(out.review.gpr.prior_60_months.end,'2026-07');assert.equal(out.review.heavy_truck.prior_12_months.start,'2025-08');
});
test('exact months and all prior-window arithmetic are independently checked',()=>{
 const mutate=[p=>p.measurement_review.heavy_truck.yoy.percent++,p=>p.measurement_review.heavy_truck.prior_12_months.mean++,p=>p.measurement_review.heavy_truck.prior_12_months.current_month_excluded=false,p=>p.measurement_review.heavy_truck.observations[1].month='1985-01',p=>p.measurement_review.heavy_truck.observations[1].value=999,p=>p.measurement_review.gpr.prior_60_months.z++,p=>p.measurement_review.gpr.prior_60_months.start='2021-09',p=>p.measurement_review.gpr.series.GPR.values.at(-1),p=>p.measurement_review.gpr.source_dates[3].excel_serial++,p=>p.measurement_review.gpr.series.GPR.values.pop()];
 mutate.splice(7,1,p=>p.measurement_review.gpr.series.GPR.values[p.measurement_review.gpr.series.GPR.values.length-1]++);
 for(const change of mutate){const p=native();change(p);assert.throws(()=>v.view(p,now));}
});
test('full source columns, labels, normalization and research permissions cannot drift',()=>{
 for(const change of [p=>delete p.measurement_review.gpr.series.GPRC_USA,p=>p.measurement_review.gpr.source_labels[1].label='Changed base',p=>p.measurement_review.gpr.series.GPRH.column=2,p=>p.measurement_review.gpr.monthly_positions--,p=>p.measurement_review.heavy_truck.metadata.units='Units',p=>p.measurement_review.heavy_truck.returned_rows--,p=>p.measurement_review.gpr.workbook_contains_formulas_verified=true,p=>p.calls_eligible=true,p=>p.measurement_review.gpr.sizing_eligible=true,p=>p.portfolio_action='LONG',p=>p.publication_context.manifest.key='data/public.json',p=>delete p.compiler_sha256['xlrd/formula.py'],p=>p.generated_at='2026-02-30T00:00:00Z',p=>p.generated_at='2027-01-01T00:00:00Z']){const p=native();change(p);assert.throws(()=>v.view(p,now));}
});
test('missing months, zero denominators and zero variance are unavailable, never shifted',()=>{
 const p=native(),s=p.measurement_review.heavy_truck,missing=s.observations.splice(-13,1)[0].month;s.observations.forEach((r,i)=>r.position=i);s.returned_rows--;s.yoy.status='prior_month_missing';s.yoy.percent=null;s.prior_12_months={...s.prior_12_months,missing_months:[missing],status:'incomplete_baseline',mean:null,population_sd:null,z:null};assert.equal(v.view(p,now).native,true);
 assert.equal(v.comparison(1,0).status,'zero_denominator');assert.equal(v.comparison(null,1).status,'latest_missing');assert.equal(v.comparison(0,1).percent,-100);assert.equal(v.comparison(1e15,1e-300).status,'outside_numeric_range');
 for(const n of ['1_000','NaN','Infinity','1e-999',true,-1])assert.throws(()=>v.number(n));assert.equal(v.number('0e-999'),0);
 const values=new Map(Array.from({length:12},(_,i)=>['2025-'+String(i+1).padStart(2,'0'),2]));values.set('2026-01',3);assert.equal(v.baseline(values,'2026-01',12).status,'zero_variance');
});
test('unavailable sources remain visible without taking the other source down',()=>{
 const p=native();p.measurement_review.gpr=null;p.measurement_review.gpr_status='source_unavailable';assert.equal(v.view(p,now).rows.length,1);
 const q=native(),s=q.measurement_review.heavy_truck;s.status='metadata_identity_mismatch';s.observations=[];s.returned_rows=0;assert.equal(v.view(q,now).rows.length,113);assert.equal(v.view(q,now).rows[0].usable,false);
});
test('DOM reaches every source and observation and never inserts provider HTML',()=>{
 const p=native(),doc=document();p.measurement_review.gpr.series.GPRC_USA.source_label='<img src=x onerror=alert(1)>';p.measurement_review.gpr.source_labels.find(r=>r.variable==='GPRC_USA').label='<img src=x onerror=alert(1)>';
 v.render(doc,p,now);assert.equal(doc.getElementById('macro-choice').children.length,113);assert.match(doc.getElementById('macro-calendar-status').textContent,/170740/);assert.match(doc.getElementById('macro-row-count').textContent,/1501–1520 of 1520/);
 for(let i=0;i<5;i++)doc.getElementById('macro-source-next').onclick();assert.match(doc.getElementById('macro-series-count').textContent,/101–113 of 113/);assert.equal(doc.getElementById('macro-source-next').disabled,true);
 for(let i=0;i<15;i++)doc.getElementById('macro-prev').onclick();assert.match(doc.getElementById('macro-row-count').textContent,/1–100 of 1520/);assert.equal(doc.getElementById('macro-prev').disabled,true);
 const choice=doc.getElementById('macro-choice');choice.value='HTRUCKSSAAR';choice.onchange();assert.match(doc.getElementById('macro-row-count').textContent,/401–500 of 500/);assert.match(doc.getElementById('macro-definition').textContent,/annual rate/);assert.match(choice.text(),/<img src=x onerror=alert\(1\)>/);
 assert.match(doc.getElementById('gpr').text(),/prior|Prior/);assert.match(doc.getElementById('heavy-truck').text(),/No demonstrated S&P 500 lead/);
});
test('legacy, invalid and failed refreshes clear all earlier measurements and callbacks',()=>{
 const doc=document();v.render(doc,native(),now);const next=doc.getElementById('macro-next').onclick,choose=doc.getElementById('macro-choice').onchange;
 v.render(doc,{generated_at:'2026-09-26T12:20:00Z',heavy_truck_sales:{saar_millions:0,note:'peaks lead S&P downturns'},geopolitical_risk:{gpr:117.9,z_5y:999}},now);next();choose();assert.equal(doc.getElementById('macro-next').onclick,null);assert.equal(doc.getElementById('macro-choice').children.length,0);assert.match(doc.getElementById('macro-calendar-status').textContent,/Legacy publication/);assert.match(doc.getElementById('heavy-truck').text(),/reported value 0/);assert.ok(!doc.getElementById('gpr').text().includes('999'));assert.ok(!doc.getElementById('heavy-truck').text().includes('peaks lead'));
 v.render(doc,native(),now);const bad=native();bad.calls_eligible=true;assert.throws(()=>v.render(doc,bad,now));assert.equal(doc.getElementById('macro-months').children.length,0);assert.ok(!doc.getElementById('gpr').text().includes('117.918'));
});
test('whole predecessor preserved and page uses one original macro load without old lead claims',()=>{
 const before=fs.readFileSync(path.join(__dirname,'fixtures/pre-macro-calendar-macro-leads.html.txt'));assert.equal(crypto.createHash('sha256').update(before).digest('hex'),'e58f50e3d5e226ce9cda8f68757bfe7efe4e8c6ccd440ea9680668db65306ac4');
 const page=fs.readFileSync(path.join(__dirname,'../macro-leads.html'),'utf8');assert.ok(page.indexOf('/jh-business-cycle-review.js')<page.indexOf('/jh-macro-calendar.js'));assert.match(page,/JHMacroCalendar.render\(document,d\)/);assert.match(page,/Every observation for the selected macro source/);assert.ok(!page.includes('+ht.note'));assert.ok(!page.includes('/jh-enhance.js')); assert.ok(!page.includes('central banks easing'));assert.equal((page.match(/load\('macro'\)/g)||[]).length,1);
});
