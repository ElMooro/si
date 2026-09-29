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
    if(!object(doc) || doc.engine!=='justhodl-portfolio-risk' || doc.schema_version!=='2.0.0') return unavailable('A supported identified risk contract is required.');
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
  return {number, view, timestamp};
});
