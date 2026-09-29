/* Deterministic, assumption-driven price-shock arithmetic. No allocation model. */
(function(root) {
  'use strict';
  const CONTRACT='linear-portfolio-scenario.v1';
  const INPUT='linear-portfolio-scenario-input.v1';
  const abs=n=>n<0n?-n:n;
  function gcd(a,b){a=abs(a);b=abs(b);while(b){const r=a%b;a=b;b=r;}return a||1n;}
  function fraction(n,d=1n){if(!d)throw Error('Zero denominator');if(d<0n){n=-n;d=-d;}const g=gcd(n,d);return {n:n/g,d:d/g};}
  const add=(a,b)=>fraction(a.n*b.d+b.n*a.d,a.d*b.d);
  const neg=a=>fraction(-a.n,a.d), sub=(a,b)=>add(a,neg(b));
  const mul=(a,b)=>fraction(a.n*b.n,a.d*b.d), div=(a,b)=>fraction(a.n*b.d,a.d*b.n);
  const ZERO=fraction(0n),ONE=fraction(1n),HUNDRED=fraction(100n);
  function decimal(value,field){
    if(typeof value!=='string'||! /^-?(?:0|[1-9]\d{0,14})(?:\.\d{1,6})?$/.test(value))
      throw Error(field+': enter an exact decimal with at most six decimal places');
    const sign=value.startsWith('-')?-1n:1n,parts=value.replace(/^-/,'').split('.');
    return fraction(sign*BigInt(parts[0]+(parts[1]||'')),10n**BigInt((parts[1]||'').length));
  }
  function bounded(value,field,lo,hi){const v=decimal(value,field);if(v.n<BigInt(lo)*v.d||v.n>BigInt(hi)*v.d)throw Error(field+': outside the calculation range');return v;}
  function roundInteger(v,places=2){
    const scale=10n**BigInt(places),scaled=abs(v.n)*scale;
    let q=scaled/v.d;const r=scaled%v.d;
    if(r*2n>v.d||(r*2n===v.d&&q%2n))q++;
    return v.n<0n?-q:q;
  }
  const roundFraction=(v,places=2)=>fraction(roundInteger(v,places),10n**BigInt(places));
  function rounded(v,places=2){const q=roundInteger(v,places),s=abs(q).toString().padStart(places+1,'0');return (q<0n?'-':'')+(places?s.slice(0,-places)+'.'+s.slice(-places):s);}
  function measure(v,places=2){return v===null?null:{numerator:v.n.toString(),denominator:v.d.toString(),decimal:rounded(v,places)};}
  function keys(o,expected,field){if(!o||typeof o!=='object'||Array.isArray(o)||Object.keys(o).length!==expected.length||!expected.every(k=>Object.hasOwn(o,k)))throw Error(field+': missing or unexpected fields');}
  function text(v,field,max=120){if(typeof v!=='string'||!v.trim()||v.length>max||/[\u0000-\u001f]/.test(v))throw Error(field+': a short nonempty label is required');return v;}
  function currency(v){if(typeof v!=='string'||! /^[A-Z]{3}$/.test(v))throw Error('Use a three-letter currency label');return v;}
  function calculate(input){
    keys(input,['contract','label','horizon','base_currency','nav','cash_rate_pct','cost_pct_nav','positions'],'scenario');
    if(input.contract!==INPUT)throw Error('Unknown scenario input contract');
    text(input.label,'scenario label');text(input.horizon,'scenario horizon');currency(input.base_currency);
    const nav=input.nav===null?null:bounded(input.nav,'NAV',0,999999999999999);
    if(nav&&nav.n<=0n)throw Error('NAV must be positive, or explicitly unavailable');
    const cashRate=bounded(input.cash_rate_pct,'Cash/financing rate',-100,10000);
    const cost=bounded(input.cost_pct_nav,'Costs as percent of NAV',0,1000);
    if(!Array.isArray(input.positions)||!input.positions.length||input.positions.length>100)throw Error('Supply one to 100 explicit position assumptions');
    let net=ZERO,gross=ZERO,assetPnl=ZERO,displayed=ZERO;const ids=new Set(),rows=[];
    for(const p of input.positions){
      keys(p,['id','label','position_type','currency','weight_pct','local_price_return_pct','fx_return_pct'],'position');
      text(p.id,'position id',64);text(p.label,'position label');currency(p.currency);
      if(ids.has(p.id))throw Error('Duplicate position id');ids.add(p.id);
      if(p.position_type!=='cash_security')throw Error('Options, futures and other nonlinear positions require a different model');
      const weight=bounded(p.weight_pct,'Signed position weight',-1000,1000);
      const local=bounded(p.local_price_return_pct,'Local price shock',-100,10000);
      const fx=bounded(p.fx_return_pct,'FX shock',-100,10000);
      if(p.currency===input.base_currency&&fx.n!==0n)throw Error('A position in the base currency must have zero FX shock');
      const baseReturn=mul(sub(mul(add(ONE,div(local,HUNDRED)),add(ONE,div(fx,HUNDRED))),ONE),HUNDRED);
      const pnlPct=div(mul(weight,baseReturn),HUNDRED);
      const start=nav===null?null:div(mul(nav,weight),HUNDRED);
      const pnl=nav===null?null:div(mul(nav,pnlPct),HUNDRED);
      if(pnl!==null)displayed=add(displayed,roundFraction(pnl));
      net=add(net,weight);gross=add(gross,fraction(abs(weight.n),weight.d));assetPnl=add(assetPnl,pnlPct);
      rows.push({id:p.id,label:p.label,currency:p.currency,weight_pct:measure(weight,6),base_price_return_pct:measure(baseReturn,6),
        pnl_pct_nav:measure(pnlPct,6),starting_value:measure(start),pnl:measure(pnl),ending_value:measure(start===null?null:add(start,pnl))});
    }
    if(gross.n>1000n*gross.d)throw Error('Gross exposure exceeds this model’s 1000% computation bound; this is not a permitted risk limit');
    const cashWeight=sub(HUNDRED,net),cashPnl=div(mul(cashWeight,cashRate),HUNDRED);
    const totalPnlPct=sub(add(assetPnl,cashPnl),cost);
    const cashValue=nav===null?null:div(mul(nav,cashWeight),HUNDRED);
    const cashPnlValue=nav===null?null:div(mul(nav,cashPnl),HUNDRED);
    const costValue=nav===null?null:div(mul(nav,cost),HUNDRED);
    const pnlValue=nav===null?null:div(mul(nav,totalPnlPct),HUNDRED);
    const endingNav=nav===null?null:add(nav,pnlValue);
    let residual=null;
    if(nav!==null){
      residual=sub(roundFraction(pnlValue),sub(add(displayed,roundFraction(cashPnlValue)),roundFraction(costValue)));
    }
    return {contract:CONTRACT,label:input.label,horizon:input.horizon,base_currency:input.base_currency,
      inputs_kind:'explicit_user_assumptions',missing_input_defaults:false,
      permissions:{calls_eligible:false,sizing_eligible:false,execution_eligible:false,may_recommend_trades:false},
      scope:'Static signed cash-security exposures, local price and FX shocks, explicit cash/financing rate and aggregate costs for one stated horizon. No trades, forecasts, probabilities, dividends, margin calls or market liquidity are modeled.',
      formula:'Base return = (1 + local price return) × (1 + FX return) − 1. Position impact = signed NAV weight × base return. Cash weight = 100% − net position weight. Total impact = position impacts + cash/financing impact − explicit costs.',
      nav_status:nav===null?'UNAVAILABLE':'USER_ASSUMED_NOT_ACCOUNT_VERIFIED',
      gross_weight_pct:measure(gross,6),net_weight_pct:measure(net,6),cash_weight_pct:measure(cashWeight,6),
      cash_role:cashWeight.n<0n?'BORROWING_ASSUMPTION':'CASH_ASSUMPTION',positions:rows,
      asset_pnl_pct_nav:measure(assetPnl,6),cash_pnl_pct_nav:measure(cashPnl,6),cost_pct_nav:measure(cost,6),
      total_pnl_pct_nav:measure(totalPnlPct,6),starting_nav:measure(nav),starting_cash:measure(cashValue),
      cash_pnl:measure(cashPnlValue),costs:measure(costValue),total_pnl:measure(pnlValue),ending_nav:measure(endingNav),
      displayed_pnl_rounding_adjustment:measure(residual),
      capital_exhausted_under_assumptions:endingNav===null?null:endingNav.n<=0n};
  }
  const api={CONTRACT,INPUT,calculate};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.JHPortfolioScenario=api;
})(typeof window==='object'?window:globalThis);
