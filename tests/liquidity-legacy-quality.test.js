const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const quality=require('../jh-liquidity-legacy-quality.js');
const now=Date.parse('2026-09-26T02:00:00Z');
function packet(){return {generated_at:'2026-09-25T21:00:00Z',meta:{generated_at:'2026-09-25T21:00:00Z'},
 quality:{status:'fresh'},core:{fed_balance_sheet:{value_bn:6740},tga:{value_bn:883},rrp:{value_bn:5},net_liquidity:{value_bn:5852,score:72,label:'BULLISH'}},
 money_supply:{m2_bn:123},dollar:{broad_index:99},yields:{curve_status:'NORMAL'},funding:{sofr:0},soma:{total_bn:1},reserves:{total_bn:1},catalog:{one:2},part4:{signal:'BULLISH'},
 regime:{trend:'EXPANDING'},spy_signal:{direction:'BULLISH',confidence:97,validated:true},chart_data:{net_liquidity:[{date:'2026-09-17',value:123}]}};}
test('expired publication withholds every current panel, preserves history and leaves original unchanged',()=>{
 const input=packet();input.generated_at=input.meta.generated_at='2026-09-17T17:24:16Z';const before=JSON.stringify(input);
 const {view,quality:q}=quality.project(input,now);assert.equal(q.usable,false);assert.equal(q.reason,'expired_publication');
 for(const key of ['money_supply','dollar','yields','funding','soma','reserves','catalog','part4'])assert.deepEqual(view[key],{});
 assert.equal(view.core.net_liquidity.value_bn,null);assert.equal(view.core.fed_balance_sheet,undefined);assert.equal(view.core.rrp,undefined);
 assert.deepEqual(view.chart_data,input.chart_data);assert.equal(JSON.stringify(input),before);
});
test('unvalidated score and forecast never survive a recent legacy wrapper',()=>{
 const {view,quality:q}=quality.project(packet(),now);assert.equal(q.usable,true);assert.equal(view.funding.sofr,0);
 assert.equal(view.core.net_liquidity.value_bn,5852);assert.equal(view.core.net_liquidity.score,null);
 assert.equal(view.spy_signal.direction,null);assert.equal(view.spy_signal.confidence,null);assert.equal(view.sizing_eligible,false);
 assert.equal(view.regime.trend,'UNVALIDATED');
});
test('unknown malformed future or inconsistent clocks never default to the present',()=>{
 for(const mutate of [p=>{delete p.generated_at;delete p.meta;},p=>p.generated_at='2026-09-26',p=>p.generated_at='2026-09-26T01:00:00',
  p=>p.generated_at='2026-09-31T00:00:00Z',p=>p.generated_at='2026-09-27T00:00:00Z',p=>p.meta.generated_at='2026-09-17T00:00:00Z']){
  const p=packet();mutate(p);assert.equal(quality.project(p,now).quality.usable,false);
 }
});
test('an open page expires without acquisition, and a false backend fresh label cannot renew it',()=>{
 const p=packet();assert.equal(quality.assess(p,now).usable,true);
 assert.equal(quality.assess(p,now+24*3600000).usable,false);
 p.quality.status='stale';assert.equal(quality.assess(p,now).usable,false);
});
test('page binds the guard before inline rendering and clears all legacy headline measurements',()=>{
 const page=fs.readFileSync('liquidity.html','utf8');
 assert(page.indexOf('/jh-liquidity-legacy-quality.js?v=20260926')<page.indexOf('function render(data)'));
 assert.match(page,/data = projection.view/);assert.match(page,/id="liquidity-agent-status"/);
 assert.match(page,/JHLiquidityLegacyQuality.assess\(lastAgentPacket\)/);
 assert(!page.includes(' : new Date();'));assert(!page.includes('Broad DXY Proxy'));
 assert(!page.includes('Updated daily 7:30 AM ET'));
});
