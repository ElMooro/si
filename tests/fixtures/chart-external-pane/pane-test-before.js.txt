const test=require('node:test'),assert=require('node:assert/strict');
const api=require('../jh-chart-buyback.js');
function record(){return {symbol:'QAONLY',measurement_contract:api.CONTRACT,market_cap:10000,market_cap_asof:'2026-06-30',market_cap_unit:'USD',quality:{point_in_time_availability_verified:false},measurements:{cashflow_observations:[{start_date:'2026-04-01',date:'2026-06-30',reported_currency:'USD',eligible:true,reported_calendar_duration_aligned:true,metrics:{net_common_repurchases:{value:100,status:'reported_value',source_field:'netCommonStockIssuance',sign:'negative',unit:'USD'},gross_common_repurchases:{value:120,status:'reported_value',source_field:'commonStockRepurchased',sign:'magnitude',unit:'USD'}}}]}};}
const first=r=>api.observations(r).rows[0];
test('strict typed finite and precise numeric values',()=>{for(const v of [false,true,'','1',null,undefined,NaN,Infinity,[],{},9007199254740992])assert.equal(api.number(v),null);for(const v of [0,1e-7,-1e-7,12.3])assert.equal(api.number(v),v);});
test('valid quarter amounts retain the producer sign with no second negation',()=>{const r=record();assert.equal(first(r).net,100);assert.equal(first(r).gross,120);assert.equal(first(r).ratio,1);r.measurements.cashflow_observations[0].metrics.net_common_repurchases.value=-100;assert.equal(first(r).ratio,-1);});
test('gross never replaces unavailable net',()=>{const r=record();delete r.measurements.cashflow_observations[0].metrics.net_common_repurchases;const x=first(r);assert.equal(x.net,null);assert.equal(x.gross,120);assert.equal(x.ratio,null);});
test('market-cap currency and date must both match the accounting quarter',()=>{for(const [k,v] of [['market_cap_unit','EUR'],['market_cap_asof','2026-09-30'],['market_cap',false],['market_cap',0],['market_cap',-100]]){const r=record();r[k]=v;assert.equal(first(r).ratio,null);assert.equal(first(r).net,100);}});
test('invalid or unqualified accounting rows cannot produce a metric',()=>{for(const change of [{eligible:false},{eligible:null},{eligible:'true'},{reported_calendar_duration_aligned:false},{date:'2026-02-30'},{start_date:'2026-07-01'},{reported_currency:null}]){const r=record();Object.assign(r.measurements.cashflow_observations[0],change);assert.equal(first(r).net,null);assert.equal(first(r).gross,null);assert.equal(first(r).ratio,null);}});
test('metric unit, status, definition and normalized sign must match',()=>{for(const change of [{unit:'EUR'},{status:'unknown'},{source_field:'commonStockRepurchased'},{sign:'positive'},{value:false},{value:'100'}]){const r=record();Object.assign(r.measurements.cashflow_observations[0].metrics.net_common_repurchases,change);assert.equal(first(r).net,null);assert.equal(first(r).ratio,null);}});
test('explicit measured zero survives all projections',()=>{const r=record();for(const v of Object.values(r.measurements.cashflow_observations[0].metrics))v.value=0;const x=first(r);assert.equal(x.net,0);assert.equal(x.gross,0);assert.equal(x.ratio,0);});
test('missing collections differ from an empty received array',()=>{for(const v of [null,{},[],false,{measurements:{cashflow_observations:{}}}])assert.equal(api.observations(v).status,'missing_or_invalid_observations');const r=record();r.measurements.cashflow_observations=[];assert.deepEqual(api.observations(r),{status:'reported_observations',rows:[]});});
test('all malformed received rows and long payload tails remain retained',()=>{const r=record(),tail={unknown:'x'.repeat(20000)+'TAIL'};r.measurements.cashflow_observations.push(null,false,0,tail);const out=api.observations(r);assert.equal(out.rows.length,5);assert.deepEqual(out.rows.map(x=>x.received),r.measurements.cashflow_observations);});
test('net ratio underflow cannot become fabricated zero',()=>{const r=record();r.measurements.cashflow_observations[0].metrics.net_common_repurchases.value=Number.MIN_VALUE;assert.equal(first(r).ratio,null);assert.match(first(r).issues.join(' '),/precision/);});
test('negative gross magnitude is not accepted as a gross amount',()=>{const r=record();r.measurements.cashflow_observations[0].metrics.gross_common_repurchases.value=-120;assert.equal(first(r).gross,null);assert.equal(first(r).net,100);});
test('only the active tab identifier selects a ticker',()=>{let value='NASDAQ:QAONLY';const document={querySelector:s=>{assert.equal(s,'#tabs .tab.on[data-id]');return {getAttribute:k=>{assert.equal(k,'data-id');return value;}};},getElementById:()=>{throw Error('Input box must not select a ticker');}};assert.equal(api.selected(document),'QAONLY');value='bad ticker';assert.equal(api.selected(document),'');assert.equal(api.selected({querySelector:()=>null}),'');});
test('dates reject invalid calendar dates and nontext',()=>{for(const value of [null,0,'2026-02-30','0000-01-01','2026-6-3','2026-06-30T00:00:00Z'])assert.equal(api.day(value),null);assert.equal(api.day('2024-02-29'),'2024-02-29');});

test('whole deployed predecessor retains all five reproduced faults',()=>{
 const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto'),source=fs.readFileSync(path.join(__dirname,'fixtures/buyback-pane/predecessor.js.txt'),'utf8');
 assert.equal(crypto.createHash('sha256').update(source).digest('hex'),'0cc4643e2028e70f79132354593347168380e6e0a59856e66ea1ef20ad604691');
 const marker='  window.jhBuybackDraw = draw;';assert.equal(source.split(marker).length,2);
 const context=vm.createContext({window:{OSC:[]},document:{getElementById:id=>id==='symin'?{value:'OLDINPUT'}:null,querySelector:()=>({textContent:'CURRENT',dataset:{id:'CURRENT'}})},setInterval:()=>1,clearInterval:()=>{},fetch:()=>{throw Error('Actual network forbidden');}});
 vm.runInContext(source.replace(marker,marker+'\n  window.__qa={num,quarters,held,symbol};'),context);const old=context.window.__qa;
 assert.equal(old.num(false),0);assert.equal(old.num(''),0);assert.equal(old.num(true),1);assert.equal(old.symbol(),'OLDINPUT');
 for(const key of ['net_common_repurchases','gross_common_repurchases']){
  const r={market_cap:10000,market_cap_asof:'2026-09-30',market_cap_unit:'USD',quality:{point_in_time_availability_verified:false},measurements:{cashflow_observations:[{date:'2025-03-31',reported_currency:'EUR',eligible:false,metrics:{[key]:{value:100,status:'reported_value',unit:'EUR'}}}]}};
  const quarters=old.quarters(r);assert.equal(quarters[0].y,1);assert.equal(api.observations(r).rows[0].ratio,null);
  assert.equal(old.held(quarters,[{time:Date.parse('2025-03-31T00:00:00Z')/1000}])[0].y,1);
 }
});
test('whole current source matches the separately reviewed pane replacement',()=>{
 const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
 assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,'../jh-chart-buyback.js'))).digest('hex'),'656d0bed4a4ab53021c37a9b6a7a14c8e0047576275fe4dc11a12936ce67d4ab');
});
