const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-portwatch-review.js'),now=Date.parse('2026-09-27T04:00:00Z');
const fixture=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/portwatch-calendar-browser.json'),'utf8'));
const legacy=()=>({generated_at:'2026-09-26T11:20:50Z',chokepoints:[{id:'c1',name:'<script>untrusted</script>',last_date:'2026-09-20',n_days:0,status:'DISRUPTED'}],ports:[],n_disrupted:1,exporters_slowing:['China']});
test('every complete stored entity and observation survives; legacy claims have no authority',()=>{
 const p=fixture(),w=v.view(p,now);assert.equal(w.rows.length,8);assert.equal(w.history_rows,4800);assert.equal(w.authority,false);assert.equal(w.native,true);
 assert.match(v.preservation(p),/browser has not replayed/);assert.equal(v.view(p,now+30*3600000).overdue,true);
 const old=v.view(legacy(),now);assert.equal(old.rows.length,1);assert.equal(old.native,false);assert.equal(old.authority,false);assert.match(v.preservation(legacy()),/Legacy packet/);
});
test('population, units, denominator, duplicate rows and unsupported permissions fail closed',()=>{
 const cases=[p=>p.measurement_review.history_rows++,p=>p.measurement_review.entities.push(p.measurement_review.entities[0]),p=>p.measurement_review.entities[0].source_field='capacity',p=>p.measurement_review.entities[0].current_7d.expected_days=14,p=>p.measurement_review.entities[0].observations[1].row_key=p.measurement_review.entities[0].observations[0].row_key,p=>p.sizing_eligible=true,p=>p.measurement_review.entities[0].observations[0].count=true,p=>p.measurement_review.entities[0].current_7d.available_days=6,p=>p.generated_at='2026-02-30T00:00:00Z'];
 for(const mutate of cases){const p=fixture();mutate(p);assert.throws(()=>v.view(p,now));}
 for(const raw of ['{"n":0,"n":1}','{"n":1e400}','{"n":NaN}'])assert.throws(()=>v.strictJSON(raw));assert.equal(v.strictJSON('{"n":0}').n,0);
});
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.attrs={};}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}setAttribute(k,val){this.attrs[k]=val;}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../portwatch.html'),'utf8'),nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
test('all 4800 rows remain reachable, safe text, zero and full record render',async()=>{
 const doc=document(),p=fixture();await v.mount(doc,async()=>p,now);
 assert.equal(doc.getElementById('pw-entities').children[0].children[1].children.length,8);assert.match(doc.getElementById('pw-page').textContent,/1–200 of 600/);
 doc.getElementById('pw-next').onclick();assert.match(doc.getElementById('pw-page').textContent,/201–400 of 600/);doc.getElementById('pw-next').onclick();assert.match(doc.getElementById('pw-page').textContent,/401–600 of 600/);assert.equal(doc.getElementById('pw-next').disabled,true);
 assert.ok(doc.getElementById('pw-raw').textContent.includes('portwatch-calendar-measurements.v1'));
 doc.getElementById('pw-entity').onchange({target:{value:'ports:port1'}});assert.match(doc.getElementById('pw-page').textContent,/1–200 of 600/);
 await v.mount(doc,async()=>legacy(),now);assert.match(doc.getElementById('pw-entities').text(),/<script>untrusted<\/script>/);assert.match(doc.getElementById('pw-entities').text(),/0/);assert.ok(!doc.getElementById('pw-entities').text().includes('DISRUPTED'));
 assert.ok(!fs.readFileSync(path.join(__dirname,'../jh-portwatch-review.js'),'utf8').includes('innerHTML'));
});
test('failed refresh clears all prior numbers and stale callbacks',async()=>{
 const doc=document();await v.mount(doc,async()=>fixture(),now);await v.mount(doc,async()=>{throw Error('denied');},now);
 for(const id of ['pw-entities','pw-observations','pw-entity'])assert.equal(doc.getElementById(id).children.length,0);
 assert.equal(doc.getElementById('pw-next').onclick,null);assert.equal(doc.getElementById('pw-entity').onchange,null);assert.equal(doc.getElementById('pw-raw').textContent,'Unavailable');
});
test('a late older response cannot overwrite a newer failure',async()=>{
 const doc=document();let done;const pending=v.mount(doc,()=>new Promise(r=>{done=r;}),now);
 await v.mount(doc,async()=>{throw Error('newer failed');},now);done(fixture());await pending;
 assert.match(doc.getElementById('pw-status').textContent,/newer failed/);assert.equal(doc.getElementById('pw-entities').children.length,0);
});
test('public reader rejects HTTP errors, redirects, malformed UTF8, duplicate keys and stalled bodies',async()=>{
 assert.equal((await v.load({fetcher:async(url,options)=>{assert.equal(url,'/data/portwatch.json?exact=1&nogen=1');assert.equal(options.redirect,'error');return new Response('{"zero":0}');}})).zero,0);
 for(const response of [new Response('denied',{status:403}),new Response('{"x":0,"x":1}'),new Response(new Uint8Array([255]))])await assert.rejects(v.load({fetcher:async()=>response}));
 let canceled=false;await assert.rejects(v.load({timeout:5,fetcher:async()=>new Response(new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('{'));},cancel(){canceled=true;}}))}),/timed out/);assert.equal(canceled,true);
});

test('every query and port reference is inspectable without promoting a snapshot or census',async()=>{
 const p=fixture(),q=v.acquisition(p);assert.equal(q.rows.length,5);assert.equal(q.references.length,2);
 assert.match(q.message,/not an atomic provider snapshot/);assert.match(v.preservation(p),/browser has not replayed/);
 const doc=document();await v.mount(doc,async()=>p,now);
 assert.equal(doc.getElementById('pw-queries').children[0].children[1].children.length,5);
 assert.match(doc.getElementById('pw-reference').textContent,/Shanghai/);
 await v.mount(doc,async()=>{throw Error('failed');},now);
 assert.equal(doc.getElementById('pw-queries').children.length,0);assert.equal(doc.getElementById('pw-reference').textContent,'Unavailable');
});

test('inconsistent query counts, foreign sources and duplicate reference identities fail closed',()=>{
 for(const mutate of [p=>p.acquisition_review.queries[0].returned_rows++,p=>p.acquisition_review.queries[0].url='https://example.test/query',p=>p.acquisition_review.provider_snapshot_atomic=true,p=>p.acquisition_review.partial_publication_allowed=true,p=>p.port_reference_review.push(p.port_reference_review[0])]){
  const p=fixture();mutate(p);assert.throws(()=>v.view(p,now));
 }
 const old=fixture();delete old.acquisition_review;delete old.port_reference_review;delete old.publication_context.compiler_sha256['portwatch_acquisition.py'];
 assert.equal(v.view(old,now).rows.length,8);assert.match(v.acquisition(old).message,/no reconciled/);assert.match(v.preservation(old),/browser has not replayed/);
});
