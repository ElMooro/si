(function(root, factory){
  const api = factory();
  if(typeof module === 'object' && module.exports) module.exports = api;
  else root.JHPortfolioRisk = api;
})(typeof window === 'object' ? window : this, function(){
  'use strict';
  const object = v => v !== null && typeof v === 'object' && !Array.isArray(v);
  const finite = v => typeof v === 'number' && Number.isFinite(v);
  const count = v => Number.isSafeInteger(v) && v >= 0;
  const strings = v => Array.isArray(v) && v.every(s => typeof s === 'string' && s.trim());
  const accountFields = ['portfolio_vol_annual_pct','portfolio_vol_daily_pct','portfolio_beta_spy',
    'var_1d_99_dollars','var_1d_95_dollars','var_1d_99_pct','var_1d_95_pct'];
  const holdingsFields = ['daily_pnl_std_dollars','annual_vol_pct_of_gross','beta_spy_per_gross','var_1d_99_dollars','var_1d_95_dollars'];
  function number(value, digits=2){ return finite(value) ? value.toFixed(digits) : '—'; }
  function day(value){
    if(typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
    const [y,m,d]=value.split('-').map(Number), leap=y%4===0 && (y%100!==0 || y%400===0);
    return y>=1 && m>=1 && m<=12 && d>=1 && d<=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31][m-1];
  }
  function timestamp(value){
    if(typeof value !== 'string') return NaN;
    const m=/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?(Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
    if(!m || !day(m[1]) || +m[2]>23 || +m[3]>59 || +m[4]>59 || (m[5]!=='Z' && (+m[7]>23 || +m[8]>59))) return NaN;
    return Date.parse(value);
  }
  function unavailable(reason){
    return {current:false,title:'Risk unavailable',detail:reason+' Missing data does not mean zero risk.',holdings:null};
  }
  function view(doc, now=Date.now()){
    if(!object(doc) || doc.engine!=='justhodl-portfolio-risk' || !['2.0.0','2.0.1','2.0.2'].includes(doc.schema_version)) return unavailable('A supported identified risk contract is required.');
    const stamp=timestamp(doc.generated_at), age=(now-stamp)/3600000;
    if(!finite(now) || !finite(age) || age < -5/60 || age > 4) return unavailable('The risk publication time is invalid or outside the four-hour display window.');
    if(!object(doc.permissions) || doc.permissions.sizing_eligible!==false || doc.permissions.may_recommend_trades!==false) return unavailable('Explicit research-only permissions are required.');
    if(!['AVAILABLE_HOLDINGS_MODEL','INCOMPLETE','no_positions'].includes(doc.status)) return unavailable('Risk model status is unsupported.');
    const c=doc.risk_contract, q=doc.quality, capital=doc.capital_basis, h=doc.holdings_risk, ready=doc.status==='AVAILABLE_HOLDINGS_MODEL';
    if(!object(c) || !count(c.sample_count) || !count(c.minimum_sample) || c.minimum_sample<1 || typeof c.return_basis!=='string' || !c.return_basis.trim()) return unavailable('Common-sample metadata is missing or invalid.');
    if(c.sample_count===0 ? c.sample_start!==null || c.sample_end!==null :
      !day(c.sample_start) || !day(c.sample_end) || c.sample_start>=c.sample_end || c.sample_end>new Date(stamp).toISOString().slice(0,10)) return unavailable('Common-sample dates are missing or inconsistent.');
    if(!object(q) || !strings(q.reason_codes) || q.status!==(ready?'partial':'unavailable') || (ready && (q.reason_codes.length || c.sample_count<c.minimum_sample))) return unavailable('Quality metadata conflicts with the model status or sample.');
    if(!object(capital) || capital.currency!=='USD' || !strings(capital.errors) || !['RECONCILED','UNAVAILABLE'].includes(capital.status)) return unavailable('Account capital metadata is invalid.');
    const reconciled=capital.status==='RECONCILED';
    if(reconciled ? !ready || !finite(capital.nav) || capital.nav<=0 || capital.errors.length!==0 : capital.nav!==null) return unavailable('The reported NAV does not support its reconciliation status.');
    if(accountFields.some(k => doc[k]!==null && (!finite(doc[k]) || !reconciled || (k!=='portfolio_beta_spy' && doc[k]<0)))) return unavailable('Account risk measurements are invalid or lack a reconciled NAV.');
    if(!object(h) || holdingsFields.some(k => h[k]!==null && (!finite(h[k]) || !ready || (k!=='beta_spy_per_gross' && h[k]<0))) ||
      (ready && holdingsFields.filter(k=>k!=='beta_spy_per_gross').some(k=>!finite(h[k])))) return unavailable('Holdings risk measurements conflict with the model status.');
    return {current:true,title:ready ? 'Holdings model · research only' : doc.status==='no_positions' ? 'No modeled positions · research only' : 'Incomplete risk inputs · research only',
      detail:`${c.sample_count} common return intervals · ${c.sample_start||'—'} to ${c.sample_end||'—'}. ${c.return_basis} ${reconciled ? 'Model reports reconciled USD NAV: '+number(capital.nav)+'.' : 'Account NAV unavailable; holdings exposure is not account equity.'} ${q.reason_codes.join(' · ')} Publication: ${doc.generated_at}. Metadata validation is not independent input or account verification.`,holdings:h};
  }
  function sectors(doc,snapshot){
    const no=detail=>({available:false,detail,rows:[],hhi:null,alert:false,unknownPct:null});
    const e=doc?.sector_exposure;
    if(!object(e)||e.schema_version!=='reported-sector-exposure.v1')return no('Sector coverage evidence is unavailable. Unknown classifications are not a sector.');
    if(e.classification_verified!==false||e.sizing_eligible!==false||!Array.isArray(e.records)||!Array.isArray(e.known_sectors)||
       !count(e.position_count)||e.position_count!==e.records.length||e.position_count!==doc.n_positions)return no('Sector population or permissions are inconsistent.');
    const unknown=new Set(['','unknown','unclassified','unavailable','not available','not classified','n/a','na','none','null','-','—','other','etf','fund']);
    const parse=value=>typeof value==='string'&&!unknown.has(value.trim().toLowerCase())?value.trim():null;
    const safe=v=>finite(v)&&(!Number.isInteger(v)||Number.isSafeInteger(v));
    const near=(a,b)=>a===null||b===null?a===b:safe(a)&&safe(b)&&Math.abs(a-b)<=Math.max(1e-9,Math.abs(b)*1e-12);
    const sum=values=>{let s=0,c=0;for(const x of values){const t=s+x;c+=Math.abs(s)>=Math.abs(x)?(s-t)+x:(x-t)+s;s=t;}return s+c;};
    const bySymbol=new Map();
    if(snapshot!==undefined){
      if(!object(snapshot)||!Array.isArray(snapshot.positions)||snapshot.positions.length!==e.records.length)return no('Sector records do not cover the complete loaded holdings.');
      for(let i=0;i<e.records.length;i++){
        const p=object(snapshot.positions[i])?snapshot.positions[i]:{},r=e.records[i];
        const product=safe(p.qty)&&safe(p.current_price)&&p.current_price>0?p.qty*p.current_price:null;
        if(!object(r)||r.symbol!==(typeof p.symbol==='string'?p.symbol:null)||JSON.stringify(r.reported_sector)!==JSON.stringify(p.sector??null)||JSON.stringify(r.reported_market_value)!==JSON.stringify(p.market_value??null)||!near(r.signed_marked_value,safe(product)?product:null))return no('Sector evidence differs from the corresponding loaded holding.');
      }
    }
    for(const r of e.records){if(!object(r))return no('Sector row is malformed.');const label=parse(r.reported_sector);if(typeof r.symbol==='string'&&r.symbol&&label!==null){if(!bySymbol.has(r.symbol))bySymbol.set(r.symbol,new Set());bySymbol.get(r.symbol).add(label);}}
    const groups=new Map(),unclassified=[],priced=[];
    for(let index=0;index<e.records.length;index++){
      const r=e.records[index],expected=bySymbol.get(r.symbol)?.size>1?null:parse(r.reported_sector);
      if(r.input_index!==index||r.sector!==expected||!(r.signed_marked_value===null||safe(r.signed_marked_value))||!near(r.gross_marked_value,r.signed_marked_value===null?null:Math.abs(r.signed_marked_value)))return no('Sector row identities or marked values are inconsistent.');
      if(r.gross_marked_value!==null)priced.push(r);
      if(expected===null)unclassified.push(r);else{if(!groups.has(expected))groups.set(expected,[]);groups.get(expected).push(r);}
    }
    const pricedGross=sum(priced.map(r=>r.gross_marked_value)),unknownPriced=sum(unclassified.filter(r=>r.gross_marked_value!==null).map(r=>r.gross_marked_value)),knownPriced=sum(priced.filter(r=>r.sector!==null).map(r=>r.gross_marked_value));
    if(e.priced_position_count!==priced.length||e.classified_position_count!==e.records.length-unclassified.length||e.unclassified_position_count!==unclassified.length||!near(e.priced_gross_marked_value,pricedGross)||!near(e.classified_priced_gross_marked_value,knownPriced)||!near(e.unclassified_priced_gross_marked_value,unknownPriced))return no('Sector totals or coverage counts are inconsistent.');
    const valued=e.gross_marked_value!==null;
    if(valued&&(priced.length!==e.records.length||!near(e.gross_marked_value,pricedGross)))return no('Full sector weights require all position marks.');
    const gross=e.gross_marked_value,positive=valued&&gross>0,whole=positive&&unknownPriced===0;
    if(!near(e.unclassified_gross_marked_value,valued?unknownPriced:null)||!near(e.unclassified_weight_pct,positive?unknownPriced/gross*100:null)||!near(e.classification_coverage_pct,positive?knownPriced/gross*100:null))return no('Unknown exposure or its denominator is inconsistent.');
    if(e.known_sectors.length!==groups.size)return no('Reported sector groups do not match all retained rows.');
    const seen=new Set(),weights=[];
    for(const g of e.known_sectors){
      if(!object(g)||seen.has(g.sector)||!groups.has(g.sector))return no('Reported sector group identity is inconsistent.');seen.add(g.sector);
      const rows=groups.get(g.sector),complete=rows.every(r=>r.gross_marked_value!==null),value=complete?sum(rows.map(r=>r.gross_marked_value)):null,weight=positive&&value!==null?value/gross*100:null;
      if(JSON.stringify(g.input_indices)!==JSON.stringify(rows.map(r=>r.input_index))||!near(g.gross_marked_value,value)||!near(g.signed_marked_value,complete?sum(rows.map(r=>r.signed_marked_value)):null)||!near(g.weight_pct,weight))return no('Sector membership, values or weights are inconsistent.');
      weights.push(weight);
    }
    const max=positive&&weights.every(v=>v!==null)&&weights.length?Math.max(...weights):null;
    const hhi=whole?sum(weights.map(v=>v*v)):null,alert=max!==null&&max>40?true:whole?false:null;
    const expectedStatus=!valued?'VALUATION_INCOMPLETE':!positive?'NO_GROSS_EXPOSURE':whole?'COMPLETE_REPORTED_CLASSIFICATION':'PARTIAL_CLASSIFICATION';
    if(e.status!==expectedStatus||!near(e.concentration_hhi,hhi)||!near(e.maximum_known_sector_weight_pct,max)||!near(e.maximum_sector_weight_pct,whole?max:null)||e.known_sector_above_40pct!==alert)return no('Sector status, HHI or threshold metadata is inconsistent.');
    const rows=e.known_sectors.slice().sort((a,b)=>(b.gross_marked_value??-1)-(a.gross_marked_value??-1)||a.sector.localeCompare(b.sector));
    return {available:true,rows,hhi,alert:alert===true,unknownPct:e.unclassified_weight_pct,evidence:e,
      detail:!valued?'Full-book sector weights unavailable: marked exposure is incomplete.':!positive?'No nonzero gross marked exposure; sector percentages and HHI are unavailable.':whole?'All nonzero exposure has reported sector labels. Classification and ETF look-through remain unverified.':`${number(e.unclassified_weight_pct)}% of gross marked exposure is unclassified. Whole-book sector HHI is unavailable.`,
      coveragePct:e.classification_coverage_pct};
  }
  return {number, view, timestamp, sectors};
});
