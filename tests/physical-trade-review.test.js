const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-physical-trade-review.js'),now=Date.parse('2026-09-27T05:00:00Z');
const shipping=()=>({generated_at:'2026-09-26T11:20:50Z',ports:[{id:'zero',name:'<img onerror=alert(1)>',country:'Korea',n_days:0,yoy_pct:999,status:'DISRUPTED'}],chokepoints:[],industry_exposure_summary:{unqualified:999}});
const cycle=()=>({generated_at:'2026-09-26T12:01:10Z',countries_total:34,by_country:{KOR:{iso3:'KOR',phase:'EXPANSION',cli_level:0,latest_date:'2026-09-25',source:'synthetic <script>alert(1)</script>',physical:{state:'CONFIRMED',median_yoy_pct:999}}},physical_confirmation:{counts:{CONFIRMED:1}},unrelated:{complete:[null,0,false]}});
const fixture=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/portwatch-calendar-browser.json'),'utf8'));
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.value='';}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../physical-trade.html'),'utf8'),nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
test('complete independent populations preserve missing countries, unknown extras and zero without legacy confirmation',()=>{
 const p=cycle();p.by_country.XXX={phase:'UNKNOWN'};const c=v.cycleRows(p,now),s=v.shippingRows(shipping(),now);
 assert.equal(c.rows.length,35);assert.equal(c.present,1);assert.equal(c.extras,1);assert.equal(s.rows.length,1);assert.equal(c.authority,false);assert.equal(s.authority,false);
 assert.equal(c.table.find(r=>r[0]==='KOR')[4],'0');assert.equal(c.table.find(r=>r[0]==='CAN')[4],'Unavailable');
 assert.ok(!JSON.stringify(c.table).includes('CONFIRMED'));assert.ok(!JSON.stringify(s.table).includes('999'));assert.ok(!JSON.stringify(s.table).includes('DISRUPTED'));
});
test('calendar shipping validates all source rows and exposes both seven-day denominators',()=>{
 const p=fixture(),x=v.shippingRows(p,now);assert.equal(x.rows.length,8);assert.equal(x.history_rows,4800);assert.equal(x.native,true);
 for(const row of x.table){assert.match(row[5],/n_total|portcalls/);assert.match(row[7],/\d\/7/);assert.match(row[9],/\d\/7/);}
 p.measurement_review.history_rows++;assert.throws(()=>v.shippingRows(p,now));
});
test('identity, date and population errors stay unavailable rather than becoming current',()=>{
 const p=cycle();p.countries_total=33;assert.throws(()=>v.cycleRows(p,now));
 const future=shipping();future.generated_at='2026-09-28T00:00:00Z';assert.throws(()=>v.shippingRows(future,now));
 const duplicate=shipping();duplicate.ports.push(duplicate.ports[0]);assert.throws(()=>v.shippingRows(duplicate,now));
 assert.equal(v.cycleRows(cycle(),now+2*86400000).overdue,true);
});
test('safe complete DOM render, independent filtering and every original field remain inspectable',async()=>{
 const doc=document(),s=shipping(),c=cycle();await v.mount(doc,{shipping:async()=>s,cycle:async()=>c},now);
 assert.equal(doc.getElementById('pt-shipping-table').children[0].children[1].children.length,1);
 assert.equal(doc.getElementById('pt-cycle-table').children[0].children[1].children.length,34);
 assert.match(doc.getElementById('pt-shipping-table').text(),/<img onerror=alert\(1\)>/);
 assert.ok(!doc.getElementById('pt-cycle-table').text().includes('CONFIRMED'));
 assert.equal(doc.getElementById('pt-cycle-raw').textContent,JSON.stringify(c,null,2));
 assert.equal(doc.getElementById('pt-shipping-raw').textContent,JSON.stringify(s,null,2));
 doc.getElementById('pt-cycle-search').oninput({target:{value:'KOR'}});assert.match(doc.getElementById('pt-cycle-count').textContent,/1 of 34/);
 assert.match(doc.getElementById('pt-shipping-count').textContent,/1 of 1/);
 assert.ok(!fs.readFileSync(path.join(__dirname,'../jh-physical-trade-review.js'),'utf8').includes('innerHTML'));
});
test('one unavailable source does not hide the other and refreshed failures clear previous values and handlers',async()=>{
 const doc=document();await v.mount(doc,{shipping:async()=>shipping(),cycle:async()=>cycle()},now);
 const oldHandler=doc.getElementById('pt-shipping-search').oninput;
 await v.mount(doc,{shipping:async()=>{throw Error('denied');},cycle:async()=>cycle()},now);
 assert.equal(doc.getElementById('pt-shipping-table').children.length,0);assert.equal(doc.getElementById('pt-shipping-raw').textContent,'Unavailable');
 assert.equal(doc.getElementById('pt-shipping-search').oninput,null);assert.match(doc.getElementById('pt-cycle-count').textContent,/34 of 34/);
 oldHandler({target:{value:''}});assert.equal(doc.getElementById('pt-shipping-table').children.length,0);
});
test('late older responses cannot overwrite a newer failed refresh',async()=>{
 const doc=document();let release;const first=v.mount(doc,{shipping:()=>new Promise(resolve=>{release=resolve;}),cycle:async()=>cycle()},now);
 await v.mount(doc,{shipping:async()=>{throw Error('newer unavailable');},cycle:async()=>cycle()},now);
 release(shipping());await first;assert.match(doc.getElementById('pt-shipping-status').textContent,/newer unavailable/);assert.equal(doc.getElementById('pt-shipping-table').children.length,0);
});
