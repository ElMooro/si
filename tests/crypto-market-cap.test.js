const test=require('node:test'),assert=require('node:assert/strict'),{page,ratios,runtime}=require('./crypto-market-cap-support.cjs');
for(const name of ['crypto/index.html','desk-v2.html']){
 test(name+': refuses legacy MVRV values and exposes only explicit descriptive ratio',()=>{
  const r=runtime(name);for(const v of [undefined,null,{},[],{mvrv_approx:9,signal:'OVERVALUED'}]){assert.equal(r.ctx.cryptoMarketCapRatio(v),null);assert.equal(r.ctx.cryptoMarketCapText(v),'Unavailable');}
  const html=r.render(ratios());assert.match(html,/2×/);assert.doesNotMatch(html,/900|OVERVALUED|MVRV Valuation/);assert.match(html,/descriptive/i);assert.deepEqual(r.errors,[]);
 });
 test(name+': zero, tiny and large ratios do not become missing or false zero',()=>{
  assert.equal(runtime(name).ctx.cryptoMarketCapText(ratios(0)),'0×');assert.match(runtime(name).ctx.cryptoMarketCapText(ratios(1e-10)),/e-10×$/);assert.match(runtime(name).ctx.cryptoMarketCapText(ratios(1e20)),/e\+20×$/);
 });
 test(name+': malformed values and permissions never produce a ratio',()=>{
  const r=runtime(name);for(const value of [true,false,null,'2',NaN,Infinity,-1,{},[]])assert.equal(r.ctx.cryptoMarketCapText(ratios(value)),'Unavailable');
  for(const [key,value] of [['contract','legacy'],['unit','usd'],['calls_eligible',true],['sizing_eligible',1],['forecast_qualified',null],['execution_eligible','false'],['independent_investment_votes',false]]){const v=ratios();v.market_cap_extension[key]=value;assert.equal(r.ctx.cryptoMarketCapRatio(v),null);}
 });
 test(name+': actual rendered proxy fields reject hostile markup',()=>{
  const payload='<img src=x onerror="window.injected=true"><script>bad()</script>',v=ratios(payload);v.signal=payload;v.mvrv_approx=payload;v.market_cap_extension.numerator.value_usd=payload;v.market_cap_extension.observation_window.first=payload;
  const html=runtime(name).render(v);assert.doesNotMatch(html,/<img|<script|window.injected/);assert.match(html,/Unavailable/);
 });
}
test('both pages use identical proxy contract and formatting',()=>{
 const pages=['crypto/index.html','desk-v2.html'].map(page);for(const name of ['cryptoMarketCapRatio','cryptoMarketCapText'])assert.equal(pages[0].functions.find(f=>f.name===name).code,pages[1].functions.find(f=>f.name===name).code);
});
test('Crypto inspector shows dated numerator and complete-population denominator, without claiming freshness',()=>{
 const html=runtime('crypto/index.html').ctx.cryptoMarketCapCard(ratios());assert.match(html,/<summary>Calculation and source<\/summary>/);assert.match(html,/Numerator \(USD\)<\/dt><dd>20/);assert.match(html,/Denominator \(USD\)<\/dt><dd>10/);assert.match(html,/observations<\/dt><dd>30/);assert.match(html,/2020-01-30T00:00:00\+00:00/);assert.match(html,/freshness are unverified/);assert.doesNotMatch(html,/<svg|width:50%/);
});
