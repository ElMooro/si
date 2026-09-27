const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-business-cycle-review.js');
const now=Date.parse('2026-09-27T01:00:00Z');
const current=()=>({generated_at:'2026-09-26T12:01:10Z',countries_total:34,by_country:{USA:{iso3:'USA',cli_level:0,phase:'RECESSION',source:'<img onerror=alert(1)>',latest_date:'2026-09-25',composite:{as_of:'2026-09',confidence:0},components:[{name:'zero',value:0,source:'untrusted <script>',period:'2026-08'}]}},interpretation:{decisive_call:'BUY aggressively'},downturn_probability_6m:{probability_now:0.7}});
const history=kind=>({generated_at:'2026-09-26T12:01:10Z',countries_count:1,by_country:{USA:{n_points:1,first_date:'2026-09-25',last_date:'2026-09-25',first_period:'2026-09',last_period:'2026-09',history:[kind==='weekly'?{date:'2026-09-25',cli:0,extra:{all:true}}:{period:'2026-09',cli:100}]}},aggregate:[{date:'2026-09-25',global_avg_cli:90}],global:[{period:'2026-09',cli:100}]});
test('country universe matches the complete native 34-country map, without silently excluding missing countries',()=>{
 const source=fs.readFileSync(path.join(__dirname,'../aws/lambdas/justhodl-global-business-cycle/source/lambda_function.py'),'utf8');
 const block=source.match(/COUNTRY_MAP = \[([\s\S]*?)\n\]/)[1];const rows=Array.from(block.matchAll(/\("([A-Z]{3})",\s*"[^"]+",\s*"([^"]+)",\s*"([^"]+)"/g),m=>({iso:m[1],name:m[3],region:m[2]}));
 assert.equal(rows.length,34);assert.deepEqual(v.UNIVERSE,rows);const view=v.countryView(current(),now);assert.equal(view.rows.length,34);assert.equal(view.present,1);assert.equal(view.authority,false);assert.equal(view.rows.find(r=>r.iso==='CAN').source,null);
 const p=current();p.by_country.XXX={country_name:'Other'};assert.equal(v.countryView(p,now).extras,1);
});
test('strict parser rejects duplicate keys including escaped aliases, nonfinite numbers and truncated or trailing data',()=>{
 for(const raw of ['{"x":1,"x":2}','{"x":1,"\\u0078":2}','{"x":1e999}','{"x":NaN}','{"x":1,}','[1,]','true false','{"x":','{"x":"unclosed}','01','{"__proto__":1,"__proto__":2}'])assert.throws(()=>v.strictJSON(raw),raw);
 const input='{"__proto__":{"safe":true},"zero":0,"nested":[false,null,"escaped \\" quote"]}';const parsed=v.strictJSON(input);assert.equal(parsed.zero,0);assert.equal(Object.getPrototypeOf(parsed),Object.prototype);assert.deepEqual(parsed,JSON.parse(input));
 assert.throws(()=>v.strictJSON('['.repeat(130)+'0'+']'.repeat(130)),/nesting/);
});
test('invalid identity, count and publication clock fail instead of giving current-looking readings',()=>{
 for(const mutate of [p=>p.countries_total=33,p=>p.by_country.USA.iso3='CAN',p=>p.by_country.USA=[],p=>p.generated_at='2026-02-31T12:00:00Z',p=>p.generated_at='2026-09-27T12:00:00Z',p=>p.generated_at='2026-09-26T12:00:00']){const p=current();mutate(p);assert.throws(()=>v.countryView(p,now));}
 assert.equal(v.countryView(current(),now+2*86400000).overdue,true);assert.equal(v.fmt(0),'0.00');for(const n of [null,false,true,'0',NaN,Infinity])assert.equal(v.fmt(n),'Unavailable');
});
test('both complete histories preserve every row and nested field; duplicate periods and cutoff counts fail',()=>{
 for(const kind of ['weekly','monthly']){
  const p=history(kind),key=kind==='weekly'?'date':'period';const view=v.historyView(p,kind,now);assert.equal(view.observations,2);assert.deepEqual(view.all.USA,p.by_country.USA.history);assert.equal(view.authority,false);
  for(const mutate of [p=>p.by_country.USA.n_points=100,p=>p.by_country.USA.history.push(p.by_country.USA.history[0]),p=>p.by_country.USA.history[0][key]='2026-13',p=>p.by_country.USA.history[0][key]=kind==='weekly'?'2027-01-01':'2027-01',p=>p.by_country.USA[kind==='weekly'?'last_date':'last_period']='1999-01']){const bad=history(kind);mutate(bad);assert.throws(()=>v.historyView(bad,kind,now));}
 }
 const p=history('weekly');p.aggregate=Array.from({length:1600},(_,i)=>({date:new Date(Date.UTC(2020,0,1+i)).toISOString().slice(0,10),global_avg_cli:i%120}));assert.equal(v.historyView(p,'weekly',now).all.GLOBAL.length,1600);
});
test('retention declarations never become original-source or browser replay proof',()=>{
 const p=current();assert.match(v.publicationStatus(p),/pending the original/);p.contract='global-business-cycle-research.v1';for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])p[k]=false;
 p.publication_context={contract:'business-cycle-derived-publication.v1',public_projections_atomic:false,original_source_replay_verified:false,compiler_sha256:Object.fromEntries(['lambda_function.py','cycle_composite.py','_fred_shim.py','business_cycle_store.py','managed_secret.py'].map(k=>[k,'a'.repeat(64)]))};
 assert.match(v.publicationStatus(p),/not been replayed/);p.calls_eligible=true;assert.match(v.publicationStatus(p),/inconsistent/);
});
test('bounded loader rejects HTTP denial, malformed UTF-8, duplicate fields and hanging streams with no fallback mutation',async()=>{
 let calls=0;await assert.rejects(v.load('other',{fetcher:()=>calls++}));assert.equal(calls,0);
 const out=await v.load('current',{fetcher:async(url,options)=>{assert.equal(url,v.PATHS.current+'?exact=1&nogen=1');assert.equal(options.redirect,'error');return new Response('{"zero":0}');}});assert.equal(out.zero,0);
 for(const response of [new Response('denied',{status:403}),new Response('{"x":0,"x":1}'),new Response(new Uint8Array([255]))])await assert.rejects(v.load('weekly',{fetcher:async()=>response}));
 let closed=false;await assert.rejects(v.load('monthly',{timeout:10,fetcher:async()=>new Response(new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('{'));},cancel(){closed=true;}}))}),/timed out/);assert.equal(closed,true);
});
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.events={};this.attrs={};}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}
 setAttribute(k,v){this.attrs[k]=v;}addEventListener(k,f){this.events[k]=f;}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(file){const html=fs.readFileSync(path.join(__dirname,'..',file),'utf8'),nodes=new Map(Array.from(html.matchAll(/id="([^"]+)"/g),m=>[m[1],new Element('div')]));return {createElement:tag=>new Element(tag),createElementNS:(_,tag)=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
test('current page displays complete country and feature rows as inert text, without active legacy advice',async()=>{
 const doc=document('global-cycle.html');await v.mount(doc,async()=>current(),now);assert.equal(doc.getElementById('coverage').textContent,'1 / 34');assert.match(doc.getElementById('country-table').text(),/<img onerror=alert\(1\)>/);assert.ok(!doc.getElementById('country-table').text().includes('BUY'));
 doc.getElementById('component-country').events.change({target:{value:'USA'}});assert.match(doc.getElementById('component-table').text(),/untrusted <script>/);assert.match(doc.getElementById('raw-country').text(),/"value": 0/);
 doc.getElementById('country-search').events.input({target:{value:'USA'}});assert.match(doc.getElementById('country-filter-status').textContent,/1 of 34/);assert.match(doc.getElementById('country-table').text(),/0.00/);
 doc.getElementById('country-search').events.input({target:{value:''}});doc.getElementById('region').events.change({target:{value:'Europe'}});assert.match(doc.getElementById('country-filter-status').textContent,/20 of 34/);
});
test('each history is independent; one failed archive does not hide another or leave stale rows',async()=>{
 const doc=document('global-cycle/index.html');await v.mount(doc,async kind=>{if(kind==='monthly')throw Error('missing archive');return history(kind);},now);
 assert.match(doc.getElementById('weekly-status').textContent,/2 complete/);assert.match(doc.getElementById('monthly-status').textContent,/unavailable/);assert.equal(doc.getElementById('monthly-table').children.length,0);
 doc.getElementById('weekly-country').events.change({target:{value:'USA'}});assert.match(doc.getElementById('weekly-table').text(),/"all":true/);assert.match(doc.getElementById('weekly-rows').textContent,/1 stored rows for USA/);
});
test('failed current clears an older display and never invents a neutral allocation or zero probability',async()=>{
 const doc=document('global-cycle.html');await v.mount(doc,async()=>current(),now);await v.mount(doc,async()=>{throw Error('unavailable');},now);
 assert.equal(doc.getElementById('coverage').textContent,'Unavailable');assert.equal(doc.getElementById('country-table').children.length,0);assert.equal(doc.getElementById('raw-current').textContent,'Unavailable');
 for(const file of ['global-cycle.html','global-cycle/index.html']){const html=fs.readFileSync(path.join(__dirname,'..',file),'utf8');assert.match(html,/jh-business-cycle-review.js/);assert.ok(!html.includes('innerHTML'));assert.ok(!html.includes('id="ladder"'));assert.match(html,/WAIT/);}
});
