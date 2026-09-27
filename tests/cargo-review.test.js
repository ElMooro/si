const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-cargo-review.js'),now=Date.parse('2026-09-27T07:00:00Z');
const native=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/cargo-calendar-browser.json'),'utf8'));
const legacy=()=>({engine:'port-cargo',generated_at:'2026-09-26T12:40:00Z',n_rows_window:72275,global_pulse:{total_ton:true},unknown:{preserved:[0,null,false]}});
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.value='';}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../port-cargo.html'),'utf8'),nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
test('whole actual synthetic native handler output reconciles every port, direction and observation',()=>{
 const p=native(),x=v.view(p,now);assert.equal(p._synthetic_fixture,true);assert.equal(x.native,true);assert.equal(x.authority,false);assert.equal(x.ports.length,4);assert.equal(x.countries.length,1);assert.equal(x.observations,126);
 const [a,b,c,missing]=x.ports;assert.equal(a.legs.import.current_7d.mean_metric_tons_per_day,0);assert.equal(a.legs.import.comparison.percent,null);assert.equal(a.legs.import.comparison.status,'zero_denominator');
 assert.equal(b.legs.import.current_7d.available_days,6);assert.equal(b.legs.import.current_7d.mean_metric_tons_per_day,null);assert.equal(c.legs.export.current_7d.mean_metric_tons_per_day,0);assert.equal(missing.observations.length,0);
 assert.deepEqual(x.review.covered_port_cohort.legs.import.matched_port_ids,['p0','p2']);assert.deepEqual(x.review.covered_port_cohort.legs.export.matched_port_ids,['p0','p1','p2']);
});
test('all displayed measurements must recalculate from complete observations, not inherited rankings',()=>{
 const mutations=[p=>p.measurement_review.ports[0].legs.import.current_7d.sum_metric_tons=1,p=>p.measurement_review.ports[0].legs.import.current_7d.mean_metric_tons_per_day=1,p=>p.measurement_review.ports[1].legs.import.current_7d.missing_or_invalid_dates=[],p=>p.measurement_review.ports[1].legs.import.current_7d.available_days=7,p=>p.measurement_review.ports[2].legs.import.comparison.percent+=1,p=>p.measurement_review.covered_port_cohort.legs.import.current_7d.mean_metric_tons_per_day+=1,p=>p.measurement_review.covered_port_cohort.legs.import.matched_port_ids.push('p1'),p=>p.measurement_review.countries[0].legs.export.previous_28d.sum_metric_tons+=1,p=>p.measurement_review.countries=[]];
 for(const mutate of mutations){const p=native();mutate(p);assert.throws(()=>v.view(p,now));}
 const p=native();p.global_pulse={total_ton:999999};p.impact_map={LONG:'unsupported'};assert.equal(v.view(p,now).ports.length,4);
});
test('coverage cannot silently shift its dates, source identities, memberships or units',()=>{
 const mutations=[p=>p.measurement_review.ports[0].observations[1].date=p.measurement_review.ports[0].observations[0].date,p=>p.measurement_review.ports[1].observations[0].source_object_id=1,p=>p.measurement_review.ports[0].observations[0].import_valid=false,p=>p.measurement_review.ports[0].last_observation_date='2026-09-22',p=>p.measurement_review.ports[1].port_id='p0',p=>p.measurement_review.ports[0].country_code='F1X',p=>p.measurement_review.ports[0].legs.import.previous_28d.start='2026-08-01',p=>p.measurement_review.unit='usd',p=>p.measurement_review.source_observation_lag_days=0,p=>p.acquisition_review.start='2026-08-01',p=>p.acquisition_review.queries[1].where='1=1',p=>p.acquisition_review.queries[0].declared_count=99,p=>p.acquisition_review.queries[1].membership_reconciled=false,p=>p.acquisition_review.provider_snapshot_atomic=true];
 for(const mutate of mutations){const p=native();mutate(p);assert.throws(()=>v.view(p,now));}
});
test('malformed clocks, unknown contracts and inconsistent authority or provenance declarations refuse',()=>{
 for(const stamp of ['2026-09-28T00:00:00Z','2026-02-30T00:00:00Z','2026-09-27']){const p=native();p.generated_at=stamp;assert.throws(()=>v.view(p,now));}
 const mutations=[p=>p.engine='other',p=>p.measurement_review.calculation_at='2026-09-26T00:00:00Z',p=>p.measurement_review.contract='unknown',p=>p.calls_eligible=true,p=>p.measurement_review.sizing_eligible=true,p=>p.publication_context.original_source_replay_verified=true,p=>p.publication_context.manifest.bytes=0,p=>p.publication_context.manifest.key='data/public-original.json',p=>delete p.publication_context.compiler_sha256['cargo_store.py']];
 for(const mutate of mutations){const p=native();mutate(p);assert.throws(()=>v.view(p,now));}
 assert.equal(v.view(native(),now+2*86400000).overdue,true);
});
test('safe DOM shows exact dates, distinct zero/missing coverage, direction changes and lazy complete originals',async()=>{
 const p=native(),doc=document();p.measurement_review.ports[0].names.push('<img onerror=alert(1)>');await v.mount(doc,async()=>p,now);
 assert.equal(doc.getElementById('cargo-ports').children[0].children[1].children.length,4);assert.match(doc.getElementById('cargo-window').textContent,/2026-09-17 through 2026-09-23.*2026-08-20 through 2026-09-16/);
 assert.match(doc.getElementById('cargo-ports').text(),/<img onerror=alert\(1\)>/);assert.match(doc.getElementById('cargo-ports').text(),/Unavailable/);assert.match(doc.getElementById('cargo-cohort').textContent,/2 \/ 4/);
 assert.equal(doc.getElementById('cargo-original').textContent,'Unavailable');const details=doc.getElementById('cargo-original-details');details.open=true;details.ontoggle();assert.equal(doc.getElementById('cargo-original').textContent,JSON.stringify(p,null,2));
 const direction=doc.getElementById('cargo-direction');direction.value='export';direction.onchange();assert.match(doc.getElementById('cargo-cohort').textContent,/Exports.*3 \/ 4/);
 const choice=doc.getElementById('cargo-port-choice');choice.value='p3';choice.onchange();assert.match(doc.getElementById('cargo-observation-count').textContent,/0 total observations/);assert.equal(doc.getElementById('cargo-observations').children[0].children[1].children.length,0);
 assert.ok(!fs.readFileSync(path.join(__dirname,'../jh-cargo-review.js'),'utf8').includes('innerHTML'));
});
test('full-population filtering and pagination retain catalog ports without observations',async()=>{
 const p=native(),r=p.measurement_review,cohorts=[r.covered_port_cohort,r.countries[0]];
 for(let i=4;i<405;i++){const port=structuredClone(r.ports[3]);port.port_id='p'+i;r.ports.push(port);for(const c of cohorts){c.population_port_ids.push(port.port_id);c.population_ports++;for(const d of ['import','export'])c.legs[d].excluded_port_ids.push(port.port_id);}}
 r.port_rows=r.catalog_ports=405;Object.assign(p.acquisition_review.queries[0],{declared_count:405,enumerated_ids:405,returned_rows:405});
 const doc=document();await v.mount(doc,async()=>p,now);assert.match(doc.getElementById('cargo-count').textContent,/1–200 of 405/);doc.getElementById('cargo-next').onclick();doc.getElementById('cargo-next').onclick();assert.match(doc.getElementById('cargo-count').textContent,/401–405 of 405/);assert.equal(doc.getElementById('cargo-next').disabled,true);assert.equal(doc.getElementById('cargo-port-choice').children.length,405);
 const search=doc.getElementById('cargo-search');search.value='p404';search.oninput();assert.match(doc.getElementById('cargo-count').textContent,/1–1 of 1/);assert.match(doc.getElementById('cargo-ports').text(),/p404/);search.value='unknown';search.oninput();assert.match(doc.getElementById('cargo-count').textContent,/0–0 of 0/);
});
test('legacy packets retain all originals without inheriting new coverage or comparisons',async()=>{
 const p=legacy(),doc=document();await v.mount(doc,async()=>p,now);assert.equal(v.view(p,now).native,false);assert.match(doc.getElementById('cargo-coverage').textContent,/Legacy packet.*72275/);assert.equal(doc.getElementById('cargo-ports').children.length,0);assert.equal(doc.getElementById('cargo-next').onclick,null);assert.match(doc.getElementById('cargo-cohort').textContent,/unavailable/);
 const details=doc.getElementById('cargo-original-details');details.open=true;details.ontoggle();assert.deepEqual(JSON.parse(doc.getElementById('cargo-original').textContent),p);
});
test('failed refresh clears all old numbers and callbacks; delayed responses cannot restore them',async()=>{
 const doc=document();await v.mount(doc,async()=>native(),now);const oldSearch=doc.getElementById('cargo-search').oninput,oldNext=doc.getElementById('cargo-next').onclick,oldToggle=doc.getElementById('cargo-original-details').ontoggle;
 let release;const first=v.mount(doc,()=>new Promise(resolve=>{release=resolve;}),now);await v.mount(doc,async()=>{throw Error('current unavailable');},now);release(native());await first;oldSearch();oldNext();oldToggle();
 assert.equal(doc.getElementById('cargo-ports').children.length,0);assert.equal(doc.getElementById('cargo-window').textContent,'Unavailable');assert.equal(doc.getElementById('cargo-original').textContent,'Unavailable');assert.equal(doc.getElementById('cargo-next').onclick,null);assert.match(doc.getElementById('cargo-status').textContent,/current unavailable/);
});
test('reader uses complete same-origin body, strict UTF-8 JSON, no redirects and a bounded timeout',async()=>{
 const p=native(),raw=new TextEncoder().encode(JSON.stringify(p));let requested,cancelled=0,index=0;
 const fetcher=async(url,options)=>{requested={url,options};return{ok:true,body:{getReader:()=>({read:async()=>index<raw.length?{value:raw.slice(index,(index+=139)),done:false}:{done:true},cancel:async()=>{cancelled++;}})}};};
 assert.deepEqual(await v.load({fetcher}),p);assert.equal(requested.url,'/data/port-cargo.json?exact=1&nogen=1');assert.equal(requested.options.redirect,'error');assert.equal(requested.options.cache,'no-store');assert.equal(cancelled,1);
 for(const bytes of [new TextEncoder().encode('{"x":[],"x":[]}'),new Uint8Array([255]),new TextEncoder().encode('{"x":1e400}'),new TextEncoder().encode('{"incomplete":')]){let i=0;await assert.rejects(v.load({fetcher:async()=>({ok:true,body:{getReader:()=>({read:async()=>i++===0?{value:bytes,done:false}:{done:true},cancel:async()=>{}})}})}));}
 await assert.rejects(v.load({fetcher:async()=>({ok:false})}),/unavailable/);await assert.rejects(v.load({timeout:5,fetcher:async()=>new Promise(()=>{})}),/timed out/);
});
test('whole predecessor is pinned; no direct S3 fetch, unsupported impact strip or hidden model dials',()=>{
 const crypto=require('node:crypto'),raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-research-port-cargo.html.txt'));assert.equal(raw.length,6436);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'d4e83d0a5bddec74c9c244d84402ae750a497ba54d0d51e71b567197c6e45a89');
 const html=fs.readFileSync(path.join(__dirname,'../port-cargo.html'),'utf8');assert.ok(!html.includes('s3.amazonaws.com'));assert.ok(!html.includes('jh-impact-strip'));assert.match(html,/jh-cargo-review.js/);assert.match(html,/12:40 UTC/);assert.match(html,/not customs trade values/);
});
