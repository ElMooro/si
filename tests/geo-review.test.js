const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-geo-review.js'),now=Date.parse('2026-09-27T04:00:00Z');
const fixture=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/geo-news-browser.json'),'utf8'));
const legacy=()=>({generated_at:'2026-09-26T11:30:17Z',version:'1.1.0',global_temp:99,top_country:'China',rankings:[{country:'China',stress_score:99,mentions_48h:0,headlines:[{title:'<img onerror=alert(1)>',src:'<script>untrusted</script>'}]}],sources:{feeds_in_corpus:164,feeds_responding:114,articles_scanned:3788},gssi_cross:{rows:[{country:'China',state:'NEWS_LEADS'}]},escalating:[{country:'China'}]});
test('complete native 22-country and 164-feed population stays visible with no authority',()=>{
 const p=fixture(),view=v.view(p,now);assert.equal(view.rows.length,22);assert.equal(view.feeds.size,164);assert.equal(view.entries.size,164);assert.equal(view.authority,false);
 const source=fs.readFileSync(path.join(__dirname,'../aws/lambdas/justhodl-geopolitical-risk/source/lambda_function.py'),'utf8');
 const names=[...source.match(/COUNTRIES = \{([\s\S]*?)\n\}/)[1].matchAll(/^    "([^"]+)":/gm)].map(m=>m[1]).sort();assert.deepEqual(v.COUNTRIES,names);
 assert.equal(v.view(p,now+27*3600000).overdue,true);assert.match(v.preservation(p),/browser has not replayed/);
});
test('legacy scores and divergent financial labels never become page or downstream risk authority',()=>{
 const p=legacy(),before=JSON.stringify(p),safe=v.context(p);assert.equal(JSON.stringify(p),before);assert.equal(safe.global_temp,null);assert.equal(safe.top_country,null);assert.deepEqual(safe.escalating,[]);assert.deepEqual(safe.gssi_cross.rows,[]);assert.deepEqual(safe.rankings,p.rankings);
 const view=v.view(p,now);assert.equal(view.rows.length,22);assert.equal(view.native,false);assert.equal(view.authority,false);assert.match(v.preservation(p),/Legacy packet/);
 const macro=fs.readFileSync(path.join(__dirname,'../macro-leads.html'),'utf8');assert.match(macro,/jh-geo-review.js/);assert.match(macro,/gr=JHGeoResearch.context\(gr\)/);
});
test('country, feed, article, denominator and permission corruption rejects the complete native view',()=>{
 for(const change of [p=>p.rankings.pop(),p=>p.rankings.push(p.rankings[0]),p=>p.entries.push(p.entries[0]),p=>p.feeds.push(p.feeds[0]),p=>p.sources.feeds_attempted=120,p=>p.sources.articles_scanned=1,p=>p.sources.feeds_responding=1,p=>p.rankings[0].entry_ids.push('unknown'),p=>p.rankings[0].raw_mentions_48h=1,p=>p.rankings[0].crisis_share=2,p=>p.calls_eligible=true,p=>p.generated_at='2026-02-31T02:00:00Z',p=>p.generated_at='2026-09-27T24:00:00Z',p=>p.generated_at='2027-01-01T00:00:00Z']){const p=fixture();change(p);assert.throws(()=>v.view(p,now));}
 const p=fixture();p.publication_context.original_source_replay_verified=true;assert.match(v.preservation(p),/Inconsistent/);
});
test('strict parser rejects duplicate and nonfinite values and never turns missing measurements into zero',()=>{
 for(const raw of ['{"a":0,"a":1}','{"a":0,"\\u0061":1}','{"a":1e999}','{"a":NaN}','{','{} trailing'])assert.throws(()=>v.strictJSON(raw));
 assert.equal(v.fmt(0),'0');for(const x of [true,false,null,undefined,'0',Infinity])assert.equal(v.fmt(x),'Unavailable');
});
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.attrs={};}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}setAttribute(k,val){this.attrs[k]=val;}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../geo-risk.html'),'utf8'),nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
test('all country/feed rows, every paginated headline, safe text and full records render',async()=>{
 const doc=document(),p=fixture();await v.mount(doc,async()=>p,now);
 assert.equal(doc.getElementById('geo-countries').children[0].children[1].children.length,22);assert.equal(doc.getElementById('geo-feeds').children[0].children[1].children.length,164);
 assert.match(doc.getElementById('geo-entry-status').textContent,/1–50 of 164/);doc.getElementById('geo-next').onclick();assert.match(doc.getElementById('geo-entry-status').textContent,/51–100 of 164/);
 doc.getElementById('geo-search').oninput({target:{value:'China'}});assert.match(doc.getElementById('geo-country-count').textContent,/1 of 22/);
 await v.mount(doc,async()=>legacy(),now);assert.match(doc.getElementById('geo-entries').text(),/<img onerror/);assert.match(doc.getElementById('geo-entries').text(),/Unverified/);
 assert.ok(!fs.readFileSync(path.join(__dirname,'../jh-geo-review.js'),'utf8').includes('innerHTML'));
});
test('failed reload clears the entire prior dataset and disables old callbacks',async()=>{
 const doc=document();await v.mount(doc,async()=>fixture(),now);await v.mount(doc,async()=>{throw Error('denied');},now);
 for(const id of ['geo-countries','geo-feeds','geo-entries'])assert.equal(doc.getElementById(id).children.length,0);
 assert.equal(doc.getElementById('geo-country').onchange,null);assert.equal(doc.getElementById('geo-next').onclick,null);assert.equal(doc.getElementById('geo-raw').textContent,'Unavailable');
});
test('an older pending response cannot overwrite a newer failed refresh',async()=>{
 const doc=document();let done;const pending=v.mount(doc,()=>new Promise(r=>{done=r;}),now);
 await v.mount(doc,async()=>{throw Error('newer denied');},now);done(fixture());await pending;
 assert.match(doc.getElementById('geo-status').textContent,/newer denied/);assert.equal(doc.getElementById('geo-countries').children.length,0);
});
test('public-only loader refuses redirects, truncation syntax, invalid UTF8, and timeout',async()=>{
 assert.equal((await v.load({fetcher:async(url,options)=>{assert.equal(url,v.PATH+'?exact=1&nogen=1');assert.equal(options.redirect,'error');return new Response('{"zero":0}');}})).zero,0);
 for(const r of [new Response('denied',{status:401}),new Response('{"x":0,"x":1}'),new Response(new Uint8Array([255]))])await assert.rejects(v.load({fetcher:async()=>r}));
 let closed=false;await assert.rejects(v.load({timeout:10,fetcher:async()=>new Response(new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('{'));},cancel(){closed=true;}}))}),/timed out/);assert.equal(closed,true);
});
