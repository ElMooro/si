const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const tv='https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws';
const source=fs.readFileSync(path.join(__dirname,'../jh-warehouse-routing.js'),'utf8');
function setup({resolver={},map={},seriesStatus=200,failedMap=false}={}){
  const calls=[];
  const window={fetch:async input=>{
    const u=String(input); calls.push(u);
    if(u.includes('tv-symbol-resolver.json')) return Response.json(resolver);
    if(u.includes('symbol-map.json')) return Response.json({map},{status:failedMap?503:200});
    if(u.startsWith(tv)) return Response.json({points:[[1,2]],source:'legacy'});
    return Response.json({obs:[['2026-09-10',4.95]]},{status:seriesStatus});
  }};
  vm.runInNewContext(source,{window,location:{href:'https://justhodl.ai/chart-pro.html'},URL,Response,Promise,Date});
  return {fetch:window.fetch,calls,dailyTail:window.jhWarehouseDailyTail};
}
test('exact resolver and symbol-map IDs never call TV, including failed series',async()=>{
  for(const spec of [
    {resolver:{exact:{'TVC:US10Y':{id:'FRED:DGS10',engine:'fred'}}}},
    {map:{'TVC:US10Y':{id:'DGS10',source:'FRED'}},seriesStatus:503}
  ]){
    const s=setup(spec),r=await s.fetch(tv+'?sym=TVC%3AUS10Y');
    assert.equal(r.status,spec.seriesStatus||200);
    assert(s.calls.some(u=>u.includes('/series?id=FRED%3ADGS10')));
    assert(!s.calls.some(u=>u.startsWith(tv)));
  }
});
test('missing resolver fails closed; unknown symbol may use TV',async()=>{
  const bad=setup({failedMap:true});
  await assert.rejects(()=>bad.fetch(tv+'?sym=UNKNOWN%3AFOO'));
  assert(!bad.calls.some(u=>u.startsWith(tv)));
  const good=setup();await good.fetch(tv+'?sym=UNKNOWN%3AFOO');
  assert(good.calls.some(u=>u.startsWith(tv)));
});
test('SKIP and computed IDs remain held without invented points',async()=>{
  for(const id of ['SKIP','COMPUTE:ADS_NFCI_CURVE']){
    const s=setup({resolver:{exact:{'ECONOMICS:USLEI':{id}}}});
    const doc=await(await s.fetch(tv+'?sym=ECONOMICS%3AUSLEI')).json();
    assert.deepEqual(doc.points,[]);assert.equal(doc.status,'HELD');assert(!s.calls.some(u=>u.startsWith(tv)));
  }
});
test('non-TV requests pass through; diagnostics do not invoke TV',async()=>{
  const s=setup();await s.fetch('/unrelated');await s.fetch(tv+'/?diag=1');
  assert.deepEqual(s.calls,['/unrelated']);
});
test('daily tail has at most two bars and matches warehouse date strings',()=>{
  const s=setup();
  const rows=s.dailyTail([{time:1},{time:Date.parse('2026-09-10T14:00:00Z')/1000,close:10},{time:Date.parse('2026-09-11T14:00:00Z'),close:11}]);
  assert.equal(rows.length,2);
  assert.equal(rows[0].time,'2026-09-10');
  assert.equal(rows[1].time,'2026-09-11');
  assert(rows[1].time >= '2026-09-11');
});
