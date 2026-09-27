const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-recession-review.js');
const now=Date.parse('2026-09-27T02:00:00Z');
const fixture=()=>({generated_at:'2026-09-26T12:40:04Z',coverage:{n_countries_scored:1,n_excluded:1},countries:[{iso3:'USA',region:'North America',phase:'<img onerror=alert(1)>',cli_level:0,six_month_change:1.25,dist_200ma_pct:0,gdp_weight:0,latest_date:'2026-09-25',terms:{all:true},recession_prob_pct:99}],excluded:[{iso3:'CAN',phase:'UNKNOWN',reason:'missing series',gdp_weight:1}],by_region:{'North America':{n_countries:1,gdp_weight:0}},us_crosscheck:{yield_curve_probit:{t10y3m_spread_pp:0,as_of:'2026-09-25',prob_12m_pct:99},sahm_rule:{value:-0.07,as_of:'2026-08-01'}},equity_beta_guidance:{rule:'BUY 100%'}});
test('all configured countries stay visible with excluded and missing distinctions',()=>{
 const base=require('../jh-business-cycle-review.js');assert.deepEqual(v.UNIVERSE,base.UNIVERSE);
 const output=v.view(fixture(),now);assert.equal(output.rows.length,34);assert.equal(output.present,1);assert.equal(output.excluded,1);assert.equal(output.missing,32);assert.equal(output.authority,false);
 assert.equal(output.rows.find(r=>r.iso==='CAN').excluded.reason,'missing series');assert.equal(output.rows.find(r=>r.iso==='JPN').source,null);
});
test('bad clocks, duplicate identities, mismatched coverage and malformed JSON fail',()=>{
 for(const change of [p=>p.countries.push(p.countries[0]),p=>p.excluded[0].iso3='USA',p=>p.countries[0].iso3='us',p=>p.coverage.n_excluded=0,p=>p.generated_at='2026-02-31T12:00:00Z',p=>p.generated_at='2026-09-26T12:40:04',p=>p.generated_at='2027-01-01T00:00:00Z']){const p=fixture();change(p);assert.throws(()=>v.view(p,now));}
 for(const raw of ['{"a":0,"a":1}','{"a":0,"\\u0061":1}','{"a":1e999}','{"a":NaN}','{"a":','{} trailing'])assert.throws(()=>v.strictJSON(raw));
 assert.equal(v.fmt(0),'0.00');for(const value of [true,false,null,undefined,'0',NaN,Infinity])assert.equal(v.fmt(value),'Unavailable');assert.equal(v.view(fixture(),now+86400000).overdue,true);
});
test('retention declarations cannot become browser replay or forecast proof',()=>{
 const p=fixture();assert.match(v.preservation(p),/Legacy publication/);
 p.contract='global-recession-research.v1';for(const key of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])p[key]=false;
 p.publication_context={contract:'recession-retained-publication.v1',single_conditional_head_write:true,original_source_replay_verified:false,compiler_sha256:{'lambda_function.py':'a'.repeat(64),'recession_research.py':'b'.repeat(64)},acquisition_manifest:{sha256:'c'.repeat(64),key:'audit-private/20260909-originals/global-recession-research/'+'c'.repeat(64)+'.bin',bytes:100}};
 assert.match(v.preservation(p),/not been independently replayed/);p.calls_eligible=true;assert.match(v.preservation(p),/Inconsistent/);
});
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.attrs={};}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}setAttribute(k,val){this.attrs[k]=val;}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../global-recession.html'),'utf8'),nodes=new Map(Array.from(html.matchAll(/id="([^"]+)"/g),m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
test('page preserves percent units and zero, safe text, every field, and country filtering',async()=>{
 const doc=document();await v.mount(doc,async()=>fixture(),now);
 const content=doc.getElementById('recession-countries').text();assert.match(content,/1\.25/);assert.ok(!content.includes('125.00'));assert.match(content,/0\.00/);assert.match(content,/<img onerror/);assert.ok(!content.includes('99.00'));
 assert.equal(doc.getElementById('recession-spread').textContent,'0.00 pp');assert.equal(doc.getElementById('recession-sahm').textContent,'-0.07 pp');
 doc.getElementById('recession-country').onchange({target:{value:'USA'}});assert.match(doc.getElementById('recession-components').text(),/"all":true/);
 doc.getElementById('recession-search').oninput({target:{value:'USA'}});assert.match(doc.getElementById('recession-filter-status').textContent,/1 of 34/);
 doc.getElementById('recession-search').oninput({target:{value:''}});doc.getElementById('recession-region').onchange({target:{value:'Europe'}});assert.match(doc.getElementById('recession-filter-status').textContent,/20 of 34/);
 assert.equal(doc.getElementById('recession-sources').children[0].children[1].children.length,5);
});
test('failed reload clears every prior display and event handler, preserving no stale score',async()=>{
 const doc=document();await v.mount(doc,async()=>fixture(),now);await v.mount(doc,async()=>{throw Error('denied');},now);
 for(const id of ['coverage','spread','sahm','raw'])assert.equal(doc.getElementById('recession-'+id).textContent,'Unavailable');
 for(const id of ['countries','components','sources','regions'])assert.equal(doc.getElementById('recession-'+id).children.length,0);
 assert.equal(doc.getElementById('recession-country').onchange,null);assert.equal(doc.getElementById('recession-search').oninput,null);
});
test('ambiguous input identities invalidate the complete view instead of hiding a missing input',async()=>{
 const doc=document(),p=fixture();p.publication_context={input_versions:[{path:v.INPUTS[0]},{path:v.INPUTS[0]}]};
 await v.mount(doc,async()=>p,now);assert.match(doc.getElementById('recession-publication').textContent,/identities differ/);assert.equal(doc.getElementById('recession-countries').children.length,0);
});
test('bounded public-only loader rejects redirects, denials, invalid UTF8 and timeout without producer invocation',async()=>{
 const p=await v.load({fetcher:async(url,options)=>{assert.equal(url,v.PATH+'?exact=1&nogen=1');assert.equal(options.redirect,'error');return new Response('{"zero":0}');}});assert.equal(p.zero,0);
 for(const r of [new Response('denied',{status:401}),new Response('{"x":0,"x":1}'),new Response(new Uint8Array([255]))])await assert.rejects(v.load({fetcher:async()=>r}));
 let closed=false;await assert.rejects(v.load({timeout:10,fetcher:async()=>new Response(new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('{'));},cancel(){closed=true;}}))}),/timed out/);assert.equal(closed,true);
 const html=fs.readFileSync(path.join(__dirname,'../global-recession.html'),'utf8');assert.match(html,/jh-recession-review.js/);assert.ok(!html.includes('innerHTML'));assert.ok(!html.includes('id="probability"'));assert.match(html,/WAIT/);
});
