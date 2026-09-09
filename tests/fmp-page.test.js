const test=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const vm=require('node:vm');
const page=require('../fmp-page.js');
function fixture(){return {agent:'fmp-fundamentals-agent',status:'PARTIAL',watchlist:['AAPL'],watchlist_quotes:{AAPL:{symbol:'AAPL',price:0,changePercentage:0,name:'<img src=x onerror=alert(1)>',timestamp:0}},index_quotes:[],sector_performance:[],movers:{gainers:Array.from({length:30},(_,i)=>({symbol:'ROW'+i,price:null}))},source_health:{quotes:{status:'UNAVAILABLE',row_count:0,http_status:403}},ts:'2026-09-09T00:00:00Z'};}
test('full quotes and every mover render with zero, null and escaped provider text',()=>{
 const view=page.render(fixture());assert.equal(view.status,'PARTIAL');assert.match(view.html,/ROW29/);assert.match(view.html,/0\.00%/);assert.match(view.html,/Unavailable/);assert.match(view.html,/&lt;img/);assert.doesNotMatch(view.html,/<img/);assert.match(view.html,/Source coverage/);
});
test('unknown or erroneous responses cannot display Ready',()=>{
 for(const doc of [{}, {...fixture(),agent:'other'}, {...fixture(),error:'bad'}])assert.throws(()=>page.render(doc));
 assert.equal(page.render({...fixture(),status:undefined}).status,'UNVERIFIED');assert.equal(page.percent(false),'Unavailable');assert.equal(page.price(null),'Unavailable');
});
test('fetch failure clears previous data and schedules exactly one retry',async()=>{
 const ids=new Map(['sb','out','fmp-payload'].map(k=>[k,{textContent:'old',innerHTML:'old'}]));let load;const timers=[];
 const context={document:{getElementById:k=>ids.get(k),addEventListener:(name,callback)=>{load=callback}},FMP_API_URL:'https://fixture.invalid/',AbortSignal:{timeout:()=>({})},fetch:async()=>{throw Error('private detail')},setTimeout:(callback,delay)=>timers.push(delay)};
 vm.runInNewContext(fs.readFileSync('fmp-page.js','utf8'),context);await load();assert.equal(ids.get('sb').textContent,'Unavailable');assert.equal(ids.get('fmp-payload').textContent,'');assert.deepEqual(timers,[60000]);assert.doesNotMatch(ids.get('out').textContent,/private detail/);
});
test('successful response retains every raw field in the complete response view',async()=>{
 const ids=new Map(['sb','out','fmp-payload'].map(k=>[k,{textContent:'',innerHTML:''}]));let load;const data={...fixture(),unknown_extra:{items:[0,false,null]}};
 vm.runInNewContext(fs.readFileSync('fmp-page.js','utf8'),{document:{getElementById:k=>ids.get(k),addEventListener:(name,callback)=>{load=callback}},FMP_API_URL:'https://fixture.invalid/',AbortSignal:{timeout:()=>({})},fetch:async()=>({ok:true,json:async()=>data}),setTimeout(){}});
 await load();assert.deepEqual(JSON.parse(ids.get('fmp-payload').textContent),data);assert.match(ids.get('sb').textContent,/PARTIAL/);
});
