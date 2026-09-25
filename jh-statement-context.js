/* Accounting data is descriptive. Legacy grades never pass to voting views. */
(function(root){
  'use strict';
  function decisionView(packet){
    const p=packet&&typeof packet==='object'?packet:{};
    return {all_results:[],rows:[],stocks:[],tickers:{},by_ticker:{},
      sector_valuation_medians:{},sector_strength_medians:{},
      cleanest_top_25:[],most_concerning_top_25:[],fortress_financials:[],problem_financials:[],
      call:null,score:null,m_score:null,grade:null,calls_eligible:false,sizing_eligible:false,
      execution_eligible:false,forecast_qualified:false,independent_investment_votes:0,
      research_context:{status:'descriptive_research_no_qualified_grade',
        current_contract_recognized:p.contract==='financial-statement-original-research.v2',
        output_digest_verified:false,original_source_replay_performed:false,
        evidence_url:'/forensic.html',note:'Inspect dated statements and source evidence. No qualified fraud probability, financial-strength grade or investment vote.'}};
  }
  const api={decisionView};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  else root.JHStatementContext=api;
})(typeof globalThis!=='undefined'?globalThis:this);
