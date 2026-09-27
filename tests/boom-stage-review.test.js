const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-boom-stage-review.js'),now=Date.parse('2026-09-27T13:00:00Z');
const legacy=()=>({generated_at:'2026-09-27T12:30:00Z',pairs:v.PAIRS.map(id=>({id,label:id+' <script>unsafe</script>',stage:'SUPPLY_SHOCK_PRICING',value:{yoy_pct:0,src:'equity proxy'},volume:{vs_baseline_pct:null,src:'port counts'},trade:{bias:'LONG',instruments:'unqualified'}})),macro:{slowdown_risk:99,plain_english:'unsupported claim'},unknown:{full:[0,null,false]}});
function source(){return{contract:v.CONTRACT,series:'TCU',name:'US total industry',status:'measured',country:'US',country_industry_mapping_qualified:false,read:'RESEARCH_ONLY',comparison:'same_month_previous_calendar_year',frequency:'monthly',unit:'Percent',change_unit:'percentage_points',date:'2026-08-01',level:0,prior_date:'2025-08-01',prior_level:1.25,yoy_chg:-1.25,forecast_qualified:false,calls_eligible:false,sizing_eligible:false,execution_eligible:false,returned_rows:2,observations:[{row:0,date:'2026-08-01',provider_value:'0',value:0,status:'observed'},{row:1,date:'2025-08-01',provider_value:'1.25',value:1.25,status:'observed'}]};}
function native(){const p=legacy();p.fred_measurements={contract:v.CONTRACT,calculated_at:p.generated_at,point_in_time_verified:false,forecast_qualified:false,series:{TCU:source()}};return p;}
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.value='';}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../boom-stage.html'),'utf8'),nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
test('whole actual native synthetic fixture renders every pair and distinct FRED source',()=>{
 const packet=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/boom-calendar-browser.json'),'utf8')),result=v.view(packet,now);
 assert.equal(result.pairs.length,20);assert.equal(result.measurements.length,6);assert.equal(result.observations,10);
 assert.equal(result.measurements.find(s=>s.sid==='ISRATIO').table[10],'ratio points');assert.equal(result.measurements.find(s=>s.sid==='TCU').table[10],'percentage points');
 assert.equal(result.measurements.find(s=>s.sid==='WGTSTUS1').table[4],'Unavailable');
});
test('all configured pairs, missing rows and extras remain identified; zero is distinct from unavailable',()=>{
 const p=legacy();p.pairs.pop();p.pairs.push({id:'EXTRA',label:'unregistered'});const x=v.view(p,now);
 assert.equal(x.pairs.length,21);assert.equal(x.present,19);assert.equal(x.extras,1);assert.equal(x.authority,false);assert.equal(x.native,false);
 assert.equal(x.pairs.find(r=>r.id==='NL-hightech').table[2],'Missing pair');assert.equal(x.pairs[0].table[4],'0');assert.equal(x.pairs[0].table[6],'Unavailable');
 assert.equal(x.measurements.length,6);assert.ok(x.measurements.every(s=>s.table[4]==='Unavailable'));
 p.pairs.push(p.pairs[0]);assert.throws(()=>v.view(p,now),/identities/);
});
test('dated units and exact prior calendar month are shown without interpreting ratios or cross-country effects',()=>{
 const p=native(),x=v.view(p,now),s=x.measurements.find(s=>s.sid==='TCU');assert.equal(s.table[4],'0');assert.equal(s.table[5],'Percent');assert.equal(s.table[10],'percentage points');
 assert.equal(s.table[7],'1.25');assert.equal(s.table[8],'2025-08-01');assert.equal(s.table[9],'-1.25');assert.equal(x.observations,2);
 for(const [key,val]of [['unit','Ratio'],['calls_eligible',true],['country','Japan']]){const forged=native();forged.fred_measurements.series.TCU[key]=val;assert.equal(v.view(forged,now).measurements.find(s=>s.sid==='TCU').table[4],'Unavailable');}
 for(const [key,val]of [['yoy_chg',99],['prior_date','2025-07-01'],['date','2026-02-30'],['returned_rows',3]]){const forged=native();forged.fred_measurements.series.TCU[key]=val;assert.throws(()=>v.view(forged,now));}
 p.fred_measurements.series.WGTSTUS1={contract:v.CONTRACT,series:'WGTSTUS1',name:'Inherited gas',status:'definition_unverified',level:999};assert.equal(v.view(p,now).measurements.find(s=>s.sid==='WGTSTUS1').table[4],'Unavailable');
});
test('invalid publication or contract cannot borrow another calculation clock',()=>{
 for(const stamp of ['2026-09-28T00:00:00Z','2026-02-30T00:00:00Z','2026-09-27']){const p=legacy();p.generated_at=stamp;assert.throws(()=>v.view(p,now));}
 const p=native();p.fred_measurements.calculated_at='2026-09-26T12:30:00Z';assert.throws(()=>v.view(p,now));
 p.fred_measurements.contract='unknown';assert.throws(()=>v.view(p,now));
 assert.equal(v.view(legacy(),now+2*86400000).overdue,true);
});
test('safe DOM preserves the whole record, filters complete population and paginates all invalid source rows',async()=>{
 const doc=document(),p=native();const s=p.fred_measurements.series.TCU;s.status='ambiguous_observation_identity';s.level=null;s.yoy_chg=null;
 s.observations=Array.from({length:503},(_,i)=>({row:i,date:'invalid',provider_value:'<img onerror=alert(1)>',value:null,status:'invalid_date'}));s.returned_rows=503;
 await v.mount(doc,async()=>p,now);assert.equal(doc.getElementById('boom-original').textContent,JSON.stringify(p,null,2));
 assert.equal(doc.getElementById('boom-pairs').children[0].children[1].children.length,20);assert.match(doc.getElementById('boom-pairs').text(),/<script>unsafe<\/script>/);
 doc.getElementById('boom-search').oninput({target:{value:'KR-semis'}});assert.match(doc.getElementById('boom-count').textContent,/1 of 20/);
 doc.getElementById('boom-series').onchange({target:{value:'TCU'}});assert.match(doc.getElementById('boom-observation-count').textContent,/1–200 of 503/);
 doc.getElementById('boom-next').onclick();doc.getElementById('boom-next').onclick();assert.match(doc.getElementById('boom-observation-count').textContent,/401–503 of 503/);assert.equal(doc.getElementById('boom-next').disabled,true);
 assert.match(doc.getElementById('boom-observations').text(),/<img onerror=alert\(1\)>/);
 assert.ok(!fs.readFileSync(path.join(__dirname,'../jh-boom-stage-review.js'),'utf8').includes('innerHTML'));
 assert.ok(!doc.getElementById('boom-pairs').text().includes('LONG'));
});
test('failed refresh clears all old values and callbacks; late older response cannot restore them',async()=>{
 const doc=document();await v.mount(doc,async()=>native(),now);const oldSearch=doc.getElementById('boom-search').oninput,oldNext=doc.getElementById('boom-next').onclick;
 let release;const first=v.mount(doc,()=>new Promise(resolve=>{release=resolve;}),now);await v.mount(doc,async()=>{throw Error('current unavailable');},now);
 release(native());await first;oldSearch({target:{value:''}});oldNext();assert.equal(doc.getElementById('boom-pairs').children.length,0);assert.equal(doc.getElementById('boom-original').textContent,'Unavailable');assert.equal(doc.getElementById('boom-next').disabled,true);assert.equal(doc.getElementById('boom-next').onclick,null);assert.match(doc.getElementById('boom-status').textContent,/current unavailable/);
});
test('reader uses same-origin no-generation path, full UTF-8 body and no redirects',async()=>{
 let requested,cancelled=0;const p=legacy(),raw=new TextEncoder().encode(JSON.stringify(p));
 const fetcher=async(url,options)=>{requested={url,options};let index=0;return{ok:true,body:{getReader:()=>({read:async()=>index++===0?{value:raw,done:false}:{done:true},cancel:async()=>{cancelled++;}})}};};
 assert.deepEqual(await v.load({fetcher}),p);assert.equal(requested.url,'/data/boom-stage.json?exact=1&nogen=1');assert.equal(requested.options.redirect,'error');assert.equal(cancelled,1);
 for(const bytes of [new TextEncoder().encode('{"pairs":[],"pairs":[]}'),new Uint8Array([255]),new TextEncoder().encode('{"x":1e400}')]){
  let i=0;await assert.rejects(v.load({fetcher:async()=>({ok:true,body:{getReader:()=>({read:async()=>i++===0?{value:bytes,done:false}:{done:true},cancel:async()=>{}})}})}));
 }
 await assert.rejects(v.load({timeout:5,fetcher:async()=>new Promise(()=>{})}),/timed out/);
});
test('whole predecessor page stays pinned and no model dials or direct S3 fetch survive',()=>{
 const crypto=require('node:crypto'),raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-research-boom-stage.html.txt'));
 assert.equal(raw.length,15284);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'613578acd1a7bf794b58c8b8cd206dca9bf73b89c348b00ebff39d2073d97f29');
 const page=fs.readFileSync(path.join(__dirname,'../boom-stage.html'),'utf8');assert.ok(!page.includes('s3.amazonaws.com'));assert.ok(!page.includes('innerHTML'));assert.match(page,/jh-boom-stage-review.js/);assert.match(page,/12:30 UTC/);
});
