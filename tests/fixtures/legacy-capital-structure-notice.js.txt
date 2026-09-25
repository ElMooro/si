/* Descriptive capital-structure evidence cannot grant investment eligibility. */
(function(root){
  'use strict';
  const note='Share Flows has no qualified dilution, buyback-direction or position-sizing signal. Weighted-average EPS share counts, reported float and cash repurchases measure different things; missing evidence does not mean stable shares.';
  function decisionView(packet){
    const p=packet&&typeof packet==='object'&&!Array.isArray(packet)?packet:{};
    return {tickers:{},by_ticker:{},rows:[],stocks:[],top:[],call:null,score:null,
      calls_eligible:false,sizing_eligible:false,execution_eligible:false,
      forecast_qualified:false,independent_investment_votes:0,
      research_context:{status:'no_qualified_capital_structure_signal',
        current_contract_recognized:p.contract==='capital-structure-original-research.v1',
        output_digest_verified:false,original_source_replay_performed:false,note}};
  }
  function mount(document){
    if(!document.body||document.getElementById('jh-capital-structure-status'))return;
    const details=document.createElement('details'),summary=document.createElement('summary'),body=document.createElement('p');
    details.id='jh-capital-structure-status';
    details.style.cssText='position:fixed;bottom:12px;right:12px;z-index:150;box-sizing:border-box;max-width:min(370px,calc(100vw - 24px));padding:9px 12px;border:1px solid #566377;border-radius:8px;background:#131b28;color:#dbe5f1;font:12px/1.5 system-ui,sans-serif;box-shadow:0 2px 10px #0004';
    summary.textContent='Share-count signals: unqualified';summary.style.cursor='pointer';
    body.textContent=note;body.style.margin='8px 0 0';details.append(summary,body);document.body.append(details);
  }
  const api={decisionView,mount,note};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  else{
    root.JHCapitalStructureContext=api;
    if(root.document){
      if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',()=>mount(root.document),{once:true});
      else mount(root.document);
    }
  }
})(typeof globalThis!=='undefined'?globalThis:this);
