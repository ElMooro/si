(function(root, factory){
  const api = factory();
  if(typeof module === 'object' && module.exports) module.exports = api;
  else root.JHPortfolioRisk = api;
})(typeof window === 'object' ? window : this, function(){
  'use strict';
  function number(value, digits=2){ return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '—'; }
  function view(doc, now=Date.now()){
    const stamp = Date.parse(doc && doc.generated_at), age = (now-stamp)/3600000;
    const current = doc && doc.schema_version === '2.0.0' && Number.isFinite(age) && age >= -5/60 && age <= 4;
    if(!current) return {current:false, title:'Risk unavailable', detail:'A current risk contract is required. Missing data does not mean zero risk.', holdings:null};
    const c=doc.risk_contract||{}, q=doc.quality||{}, capital=doc.capital_basis||{};
    return {current:true, title:doc.status==='AVAILABLE_HOLDINGS_MODEL' ? 'Holdings model · research only' : 'Incomplete risk inputs · research only',
      detail:`${c.sample_count||0} common return intervals · ${c.sample_start||'—'} to ${c.sample_end||'—'}. ${c.return_basis||''} ${capital.status==='RECONCILED' ? 'Account NAV reconciled.' : 'Account NAV unavailable; holdings exposure is not account equity.'} ${(q.reason_codes||[]).join(' · ')}`,
      holdings:doc.holdings_risk||null};
  }
  return {number, view};
});
