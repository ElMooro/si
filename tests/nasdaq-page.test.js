const test=require('node:test'),assert=require('node:assert/strict');
const {render,display}=require('../nasdaq-page.js');
const snapshot=item=>({agent:'nasdaq-datalink-agent',status:'PARTIAL',metrics_ok:0,metrics_err:1,categories:{housing:{'FRED/TEST':item}}});
test('preserves all rows, prior values, zero and false while escaping strings',()=>{
 const view=render(snapshot({name:'<img src=x onerror=alert(1)>',latest:{value:0,flag:false},previous:{value:2},history:Array.from({length:30},(_,i)=>({value:i})),change_pct:-100}));
 assert.ok(view.html.includes('30 rows'));assert.ok(view.html.includes('29'));assert.ok(view.html.includes('false'));assert.ok(view.html.includes('Previous observation'));
 assert.ok(!view.html.includes('<img'));assert.ok(view.html.includes('&lt;img'));assert.ok(view.html.includes('Housing'));
});
test('missing values are not zero and provider failures stay visible',()=>{
 assert.equal(display(null),'Unavailable');assert.equal(display(false),'false');assert.equal(display(0),'0');
 assert.ok(render(snapshot({error:'PROVIDER_HTTP_ERROR',http_status:401})).html.includes('HTTP 401'));
});
test('wrong engine and malformed datasets rejected',()=>{
 assert.throws(()=>render({agent:'other',categories:{}}));assert.throws(()=>render(snapshot(null)));assert.throws(()=>render({agent:'nasdaq-datalink-agent',categories:[]}));
});
