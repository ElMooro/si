const vm=require('node:vm'),{page}=require('./crypto-market-cap-support.cjs');
const denied={calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false,independent_investment_votes:0};
function funding(rate=.0001,provider='okx'){
 const symbols=['BTC','ETH','SOL','XRP','DOGE','ADA','AVAX','LINK','DOT','BNB'];
 const decimal=rate===.0001?'0.0001':String(rate),percent=rate===.0001?'0.0100':String(rate*100);
 const rates=symbols.map((symbol,i)=>({...denied,contract:'crypto-reported-funding-point.v1',status:'descriptive',provider,symbol,instrument:symbol+(provider==='okx'?'-USDT-SWAP':'USDT'),unit:'ratio_per_reported_funding_event',funding_rate:rate,funding_rate_decimal:decimal,funding_rate_pct:rate*100,funding_rate_pct_decimal:percent,funding_payment_direction:rate>0?'longs_pay_shorts':rate<0?'shorts_pay_longs':'zero_reported_rate',reported_settlement_at:'2020-01-01T00:00:00+00:00',reported_next_settlement_at:'2020-01-01T01:00:00+00:00',reported_schedule_difference_ms:3600000,rate_basis:provider==='okx'?'current_endpoint_rate':'last_settled_history_rate',annualized_pct:null,source_timing_qualified:false,cross_contract_comparability_verified:false,evidence_index:i,original_response_sha256:'a'.repeat(64)}));
 return {...denied,contract:'crypto-reported-funding.v1',rates,source_attempts:rates.map(row=>({provider:row.provider,instrument:row.instrument,response_complete:true,original_response_sha256:row.original_response_sha256,acquired_completed_at:'2020-01-01T00:00:01+00:00'})),avg_funding:null,avg_rate_pct:null,leverage_sentiment:'UNAVAILABLE'};
}
function packet(f){return {funding:f,fear_greed:{current:25,score:25,label:'Invented'},risk_score:{score:null,regime:'UNAVAILABLE',action:'WAIT',...denied}};}
function runtime(){const p=page('crypto/index.html'),nodes={main:{innerHTML:''},ts:{textContent:''}},errors=[],ctx={D:{},window:{},document:{getElementById:id=>nodes[id]||null},console:{error:e=>errors.push(String(e))}};vm.createContext(ctx);vm.runInContext(p.code,ctx);return {ctx,p,nodes,errors,render(f){ctx.D=packet(f);ctx.render();return nodes.main.innerHTML;}};}
function scenarios(){
 const malformed=funding();malformed.rates[0].funding_rate_pct=true;
 const mixed=funding();mixed.rates[0].status='unavailable';mixed.rates[0].funding_rate_pct=null;mixed.rates[0].reason='missing_invalid_or_lossy_funding_rate';
 const injection=funding();injection.rates[0].instrument='<img src="https://fixture.invalid/x" onerror="window.injected=true">';injection.source_attempts[0].acquired_completed_at=injection.rates[0].instrument;
 return {normal:funding(),zero:funding(0),tiny:funding(1e-12),negative:funding(-.0001),history:funding(.0001,'bybit'),unavailable:{},legacy:{avg_funding:.02,most_longed:[{symbol:'BTC',funding_rate:999,annualized:9999}]},malformed,mixed,injection};
}
module.exports={funding,packet,runtime,scenarios,denied};
