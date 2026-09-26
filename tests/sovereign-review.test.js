const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {countryView,historyView,calendarChanges,captureStatus,load,mount,fmt,date,clock}=require('../jh-sovereign-review.js');
const now=Date.parse('2026-09-26T22:00:00Z');
const universe={contract:'sovereign-review-universe.v1',countries:[{country:'Alpha',slug:'alpha',region:'Region'},{country:'Beta',slug:'beta',region:'Region'}]};
const packet=()=>({generated_at:'2026-09-26T18:16:52Z',n_countries:1,countries:[{country:'Alpha',cds_bp:0,yield_10y_pct:null,as_of:'Unverified date'}],eurodollar_hub_stress_0_100:50,eurodollar_hub_history_n:1,eurodollar_hub_history:[{date:'2026-09-26',stress:50}]});
test('provider capture claims are distinguished from browser replay and retain the pending predecessor state',()=>{
 assert.match(captureStatus(packet(),universe),/pending its normal schedule/);
 const p=packet(),sha='a'.repeat(64);p.source_evidence={contract:'sovereign-source-capture.v1',configured_countries:2,coverage:[{slug:'alpha'},{slug:'beta'}],manifest:{sha256:sha,key:'audit-private/20260909-originals/global-sovereign-research/'+sha+'.bin',bytes:200},requests_attempted:3,complete_responses_retained:2,definitions_verified:false,observation_clocks_verified:false,original_source_replay_verified:false};
 assert.match(captureStatus(p,universe),/Producer reports 2 complete/);assert.match(captureStatus(p,universe),/has not replayed/);
 for(const mutate of [e=>e.coverage.push(e.coverage[0]),e=>e.coverage[0]=null,e=>e.manifest.key='data/public-original.json',e=>e.complete_responses_retained=4,e=>e.definitions_verified=true,e=>e.configured_countries=3]){const copy=structuredClone(p);mutate(copy.source_evidence);assert.match(captureStatus(copy,universe),/inconsistent/);}
});
test('country coverage preserves missing countries, genuine zero and unknown members without granting authority',()=>{
 const p=packet(),v=countryView(p,universe,now);assert.equal(v.expected,2);assert.equal(v.present,1);assert.equal(v.cds,1);assert.equal(v.yields,0);assert.equal(v.rows[1].source,null);assert.match(v.rows[1].status,/Missing/);assert.equal(v.authority,false);assert.equal(v.sourceClockQualified,0);
 p.countries.push({country:'Unregistered',cds_bp:3});p.n_countries=2;const extra=countryView(p,universe,now);assert.equal(extra.rows.length,3);assert.equal(extra.extras,1);assert.equal(extra.rows[2].provider,null);
 for(const n of [null,undefined,'',false,true,Infinity,NaN,'0'])assert.equal(fmt(n),'Unavailable');assert.equal(fmt(0),'0.00');assert.equal(fmt(-2),'-2.00');
});
test('ambiguous country identity, count drift, invalid dates and malicious source links cannot pass as reviewed membership',()=>{
 for(const edit of [p=>p.countries.push(p.countries[0]),p=>p.n_countries=9,p=>p.generated_at='2026-09-27T00:00:00Z',p=>p.generated_at='2026-02-31T12:00:00Z',p=>p.generated_at='2026-09-26T12:00:00']){const p=packet();edit(p);assert.throws(()=>countryView(p,universe,now));}
 const u=structuredClone(universe);u.countries[0].slug='https://example.com/';assert.throws(()=>countryView(packet(),u,now));
 const old=packet();old.generated_at='2026-09-24T12:00:00Z';assert.equal(countryView(old,universe,now).overdue,true);
 assert.equal(date('2026-02-31'),null);assert.equal(clock('2026-09-26'),null);
});
test('all historical rows and fields survive; no slicing, splicing or silently deduplicating history',()=>{
 const rows=Array.from({length:1500},(_,i)=>({date:new Date(Date.UTC(2020,0,1+i)).toISOString().slice(0,10),stress:i%90,extra:{whole:true}}));
 const v=historyView(rows,'daily');assert.equal(v.rows.length,1500);assert.deepEqual(v.rows,rows);assert.ok(v.fields.includes('extra'));
 assert.throws(()=>historyView([...rows,rows[0]],'daily'));assert.throws(()=>historyView({n_points:1501,history:rows},'archive'));
 const monthly=historyView({n_points:1,history:[{date:'1990-01-01',stress:80,yoy_pct:999}],generated_at:'old'},'archive');assert.equal(monthly.rows[0].yoy_pct,999);assert.ok(monthly.fields.includes('yoy_pct'));
});
test('changes require exact calendar dates and matching daily/current snapshots, never monthly or positional substitutes',()=>{
 const p=packet(),hist=[{date:'2026-09-26',stress:50},{date:'2026-09-19',stress:0},{date:'2026-08-27',stress:45}];
 assert.equal(calendarChanges(p,hist).seven.value,50);assert.equal(calendarChanges(p,hist).thirty.value,5);
 assert.equal(calendarChanges(p,hist.filter(r=>r.date!=='2026-09-19')).seven,null);
 hist[0].stress=51;assert.deepEqual(calendarChanges(p,hist),{seven:null,thirty:null});
});
test('source loader uses only exact allowlisted paths and rejects partial, malformed and stalled bodies',async()=>{
 let calls=0;await assert.rejects(load('https://example.com',{fetcher:()=>calls++}));assert.equal(calls,0);
 const out=await load('current',{fetcher:async(url,options)=>{assert.equal(url,'/data/global-sovereign.json?exact=1&nogen=1');assert.equal(options.redirect,'error');return new Response('{"ok":true}');}});assert.equal(out.ok,true);
 await assert.rejects(load('universe',{fetcher:async()=>new Response(' '.repeat(65537))}),/limit/);
 await assert.rejects(load('archive',{fetcher:async()=>new Response(new Uint8Array([255]))}));
 await assert.rejects(load('archive',{fetcher:async()=>new Response('[]',{status:403})}));
 let cancelled=false;await assert.rejects(load('archive',{timeout:10,fetcher:async()=>new Response(new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('['));},cancel(){cancelled=true;}}))}),/timed out/);assert.equal(cancelled,true);
});
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.events={};this.attrs={};}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}
 setAttribute(k,v){this.attrs[k]=v;}addEventListener(k,f){this.events[k]=f;}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
const document=()=>{const nodes=new Map();return {createElement:tag=>new Element(tag),createElementNS:(_,tag)=>new Element(tag),getElementById:id=>{if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);}};};
test('country and daily panels survive a failed monthly archive and untrusted labels stay inert',async()=>{
 const doc=document(),p=packet();p.countries[0].rating='<img src=x onerror=alert(1)>';p.countries[0].stress_0_100=98;
 await mount(doc,async kind=>{if(kind==='current')return p;if(kind==='universe')return universe;if(kind==='daily')return [{date:'2026-09-26',stress:50}];throw new Error('archive absent');},now);
 assert.match(doc.getElementById('country-table').text(),/<img src=x onerror=alert\(1\)>/);assert.match(doc.getElementById('archive-status').text(),/unavailable/);assert.match(doc.getElementById('daily-status').text(),/1 complete/);assert.match(doc.getElementById('legacy-fields').text(),/stress_0_100/);
 const search=doc.getElementById('country-search');search.events.input({target:{value:'Beta'}});assert.match(doc.getElementById('country-filter-status').text(),/1 of 2/);assert.match(doc.getElementById('country-table').text(),/Missing country response/);
});
test('failed current clears previous values while the independent monthly archive still renders',async()=>{
 const doc=document();doc.getElementById('country-count').textContent='45';doc.getElementById('country-table').appendChild(new Element('old'));
 await mount(doc,async kind=>{if(kind==='current')throw new Error('bad current');if(kind==='universe')return universe;if(kind==='daily')return [];return {history:[],n_points:0};},now);
 assert.equal(doc.getElementById('country-count').textContent,'Unavailable');assert.equal(doc.getElementById('country-table').children.length,0);assert.match(doc.getElementById('daily-status').text(),/unavailable/);assert.match(doc.getElementById('archive-status').text(),/0 complete/);
});
test('an embedded daily history cutoff fails explicitly instead of claiming complete history',async()=>{
 const doc=document(),p=packet();p.eurodollar_hub_history_n=100;
 await mount(doc,async kind=>kind==='current'?p:kind==='universe'?universe:{history:[],n_points:0},now);
 assert.match(doc.getElementById('daily-status').text(),/unavailable/);assert.equal(doc.getElementById('daily-table').children.length,0);
 assert.equal(doc.getElementById('country-count').textContent,'1 / 2');assert.match(doc.getElementById('change-seven').textContent,/Unavailable/);
});
test('checked-in browser universe exactly matches the producer country membership',()=>{
 const root=path.join(__dirname,'..'),source=fs.readFileSync(path.join(root,'aws/lambdas/justhodl-global-sovereign/source/lambda_function.py'),'utf8');
 const block=source.match(/COUNTRIES = \{([\s\S]*?)\n\}/)[1];const rows=Array.from(block.matchAll(/"([^"]+)": \("([^"]+)", "([^"]+)"\)/g),m=>({country:m[1],slug:m[2],region:m[3]})).sort((a,b)=>a.country.localeCompare(b.country));
 const actual=JSON.parse(fs.readFileSync(path.join(root,'config/global-sovereign-universe.json'),'utf8')).countries.sort((a,b)=>a.country.localeCompare(b.country));assert.equal(rows.length,45);assert.deepEqual(actual,rows);
 const html=fs.readFileSync(path.join(root,'global-sovereign.html'),'utf8');assert.ok(html.includes('jh-sovereign-review.js'));assert.ok(!html.includes('innerHTML'));assert.ok(!html.includes('id="kpi-high"'));
});
