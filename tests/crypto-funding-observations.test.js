const test=require('node:test'),assert=require('node:assert/strict'),{funding,runtime,scenarios}=require('./crypto-funding-observations-support.cjs');
const {runtime:marketRuntime}=require('./crypto-market-cap-support.cjs');
test('funding decimal parser compares exact decimal value without float collapse',()=>{
 const {ctx}=runtime();assert.equal(ctx.cryptoFundingDecimal('0.0100'),ctx.cryptoFundingDecimal('1e-2'));assert.notEqual(ctx.cryptoFundingDecimal('0.1000000000000000001'),ctx.cryptoFundingDecimal('0.1'));assert.equal(ctx.cryptoFundingDecimal('0.0001',2),ctx.cryptoFundingDecimal('0.01'));
 for(const value of [true,{},'NaN','Infinity','',null,' 0.1','+0.1'])assert.equal(ctx.cryptoFundingDecimal(value),null);
});
test('missing, legacy and malformed funding cannot create a neutral score or annual yield',()=>{
 const r=runtime();for(const value of [null,{},[],{rates:[{symbol:'BTC',funding_rate_pct:.1}]}]){assert.equal(r.ctx.cryptoFundingRows(value).length,0);assert.match(r.ctx.cryptoFundingTable(value),/Unavailable/);}
 const html=r.render(scenarios().legacy);assert.doesNotMatch(html,/9999|Most Longed|Most Shorted|Funding \(Long\)|50 \+ rate/);assert.match(html,/Funding market average/);assert.deepEqual(r.errors,[]);
});
test('zero remains an explicit zero event and never becomes SHORT',()=>{
 const r=runtime(),rows=r.ctx.cryptoFundingRows(funding(0));assert.equal(rows.length,10);assert.equal(rows[0].funding_rate_pct,0);const html=r.ctx.cryptoFundingTable(funding(0));assert.match(html,/>0%<\/td>/);assert.match(html,/zero reported rate/);assert.doesNotMatch(html,/SHORT|LONG/);
});
test('small signed rates retain precision and payment direction',()=>{
 const r=runtime();assert.match(r.ctx.cryptoFundingTable(funding(1e-12)),/1e-10%/);assert.match(r.ctx.cryptoFundingTable(funding(-.0001)),/-0.01%/);assert.match(r.ctx.cryptoFundingTable(funding(-.0001)),/shorts pay longs/);
});
test('current endpoint and settled history keep separate basis labels and clocks',()=>{
 const r=runtime();assert.match(r.ctx.cryptoFundingTable(funding()),/current endpoint rate/);assert.match(r.ctx.cryptoFundingTable(funding(.0001,'bybit')),/last settled history rate/);assert.match(r.ctx.cryptoFundingTable(funding()),/2020-01-01T00:00:00\+00:00/);assert.doesNotMatch(r.ctx.cryptoFundingTable(funding()),/10\.95|>8H</);
});
test('projection rejects permissions, wrong instrument, units, lossy decimal and annualization',()=>{
 const r=runtime();for(const [key,value] of [['calls_eligible',true],['independent_investment_votes',false],['unit','percent'],['funding_rate_pct',true],['funding_rate_pct','0.01'],['funding_rate_pct',NaN],['funding_rate_decimal','0.000100000000000000001'],['funding_payment_direction','SHORT'],['instrument','ETH-USDT-SWAP'],['annualized_pct',10.95],['reported_settlement_at','bad']]){const row=funding().rates[0];row[key]=value;assert.equal(r.ctx.cryptoFundingPoint(row),null,key);}
});
test('duplicate identity refuses ambiguous aggregate instead of double counting',()=>{const r=runtime(),value=funding();value.rates.push({...value.rates[0]});assert.equal(r.ctx.cryptoFundingRows(value).length,0);});
test('coverage inspector retains unavailable instrument and receipt identity without markup execution',()=>{
 const r=runtime();assert.match(r.ctx.cryptoFundingTable(scenarios().mixed),/BTC-USDT-SWAP: Unavailable/);const html=r.ctx.cryptoFundingTable(scenarios().injection);assert.doesNotMatch(html,/<img|<script|<iframe/);assert.match(html,/&lt;img/);assert.match(html,/SHA-256/);assert.match(html,/tabindex='0'/);assert.match(html,/<summary/);
});
test('all actual render scenarios complete without console errors',()=>{const r=runtime();for(const [name,value] of Object.entries(scenarios())){const html=r.render(value);assert.match(html,/crypto-funding-card/,name);assert.doesNotMatch(html,/Render Error|NaN%|undefined%/,name);}assert.deepEqual(r.errors,[]);});
test('Desk typed risk preserves zero and refuses missing, hostile or ineligible scores',()=>{
 const r=marketRuntime('desk-v2.html');assert.equal(r.ctx.cryptoRiskText({score:0}),'0');for(const score of [null,true,'0',NaN,Infinity,-1,101,{}])assert.equal(r.ctx.cryptoRiskText({score}),'Unavailable');assert.equal(r.ctx.cryptoRiskText({score:90,calls_eligible:false}),'Unavailable');
});
