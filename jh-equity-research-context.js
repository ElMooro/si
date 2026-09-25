/* Cached model output is inspectable history, not qualified investment advice. */
(function(root){
  'use strict';
  const fields=['business_mix_assessment','capital_allocation_assessment','catalysts_12m','competitive_position',
    'devils_advocate','earnings_call_sentiment','earnings_track_record_assessment','executive_summary',
    'financial_health_summary','forward_model','industry_comparison_assessment','institutional_activity_assessment',
    'invalidation_triggers','investment_thesis','peer_comparison_assessment','relationships','risk_factors',
    'scenarios','valuation_assessment','verdict'];
  const note='Investment forecasts and model narratives are unqualified. The current view abstains. Original company data and prior model output remain in the complete-data inspector. Check each observation date and source; legacy calculations have not all been independently validated.';
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
  function view(packet){
    if(!object(packet))return packet;
    const result={...packet},previous=packet.unqualified_model_output;
    const original=object(previous)&&object(previous.original_fields)?{...previous.original_fields}:{};
    const replacement=Object.fromEntries(fields.map(k=>[k,null]));
    replacement.verdict={rating:'WAIT',conviction_grade:null,price_target_12m:null,upside_pct:null,
      confidence_pct:null,forecast_qualified:false,verdict_rationale:'Investment forecasts require qualified evidence and out-of-sample validation.'};
    replacement.executive_summary='AI synthesis is unavailable under the no-paid-AI policy. Reported company data remains available; investment forecasts are unqualified.';
    replacement.catalysts_12m=[];replacement.invalidation_triggers=[];
    for(const [key,value] of Object.entries(replacement)){
      if(packet[key]!=null&&JSON.stringify(packet[key])!==JSON.stringify(value)&&!Object.hasOwn(original,key))original[key]=packet[key];
      result[key]=value;
    }
    result.unqualified_model_output={status:'unqualified',original_fields:original,
      calls_eligible:false,sizing_eligible:false,forecast_qualified:false};
    return Object.assign(result,{ai_policy:'no_paid_ai',model_output_guard:'equity-model-output.v1',
      calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false});
  }
  const api={view,note,fields};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  else root.JHEquityResearchContext=api;
})(typeof globalThis!=='undefined'?globalThis:this);
