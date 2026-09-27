/* Display projection only. Preserve source populations; never restore legacy scores. */
(function(root){
 'use strict';
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 function rows(packet){
  const modern=packet?.measurement_contract==='earnings-accounting-measurements.v1';
  const sourceKey=modern?'issuer_rows':packet?.all_ranked?'all_ranked':'top_20_high_quality';
  const source=modern?packet.issuer_rows:packet?.all_ranked||packet?.top_20_high_quality||[];
  if(!Array.isArray(source))return source;
  return source.map((row,i)=>{
   if(!object(row))return row;
   if(!modern)return {...row,measurement_status:'Legacy periods, currencies and formulas unverified',source_record:sourceKey+'['+i+']'};
   const amounts=object(row.amounts)?row.amounts:{},metrics=object(row.measurements)?row.measurements:{},window=row.windows?.current?.income;
   return {...row,quality_score:null,sloan_accruals_pct_assets:metrics.earnings_cash_gap_pct_end_assets,
    cash_conversion_ratio:metrics.cash_conversion_ratio,dsri_beneish:metrics.dsri_reported,gmi_beneish:metrics.gmi_reported,
    average_assets_gap_pct:metrics.cash_flow_accruals_pct_average_assets,reported_net_income:amounts.net_income,
    reported_ocf:amounts.operating_cash_flow,reported_fcf:amounts.free_cash_flow_derived,
    window_start:window?.start_date,window_end:window?.end_date,
    measurement_status:typeof row.status==='string'?row.status:'Unavailable',source_record:'issuer_rows['+i+']'};
  });
 }
 const api={rows};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHEarningsObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
