/* Display projection only; target estimates and earnings events remain separate. */
(function(root){
 'use strict';
 const CONTRACT='estimate-observations.v1';
 const obj=x=>x&&typeof x==='object'&&!Array.isArray(x)?x:{};
 const status=x=>({current_or_future_target:'Current or future target',past_target:'Historical target',unidentified:'Target date unavailable',reported_estimate_observation:'Reported estimate',unqualified_record:'Record needs verification',no_unique_comparable_prior_observation:'No unique comparable prior observation',observed_same_target_consensus_change:'Observed same-target consensus change',unavailable:'Provider unavailable',rate_limited:'Provider rate limit',not_attempted_runtime_rate_or_size_limit:'Not attempted: runtime, rate or size limit',invalid_symbol_not_requested:'Invalid symbol: not requested',received:'Empty received response'}[x]||'Unavailable');
 function rows(packet){
  if(packet.measurement_contract!==CONTRACT){
   const out=[];
   for(const key of ['estimate_strength_leaders','upward_revisions','downward_revisions','top_picks']){
    if(packet[key]!==undefined&&!Array.isArray(packet[key]))throw Error('Malformed legacy population: '+key);
    (packet[key]||[]).forEach((value,i)=>{const r=obj(value);out.push({ticker:r.ticker,event_date:r.earnings_date,eps:r.current_eps_est,
      target:null,currency:null,basis:null,cik:null,change:null,change_pct:null,status:'Legacy target identity and comparison unverified',source:key+'['+i+']'});});
   }
   if(packet.by_ticker!==undefined&&(!packet.by_ticker||typeof packet.by_ticker!=='object'||Array.isArray(packet.by_ticker)))throw Error('Malformed legacy per-ticker population');
   for(const [key,value] of Object.entries(packet.by_ticker||{})){const r=obj(value);out.push({ticker:r.ticker||key,event_date:r.earnings_date,eps:r.current_eps_est,
     target:null,currency:null,basis:null,cik:null,change:null,change_pct:null,status:'Legacy target identity and comparison unverified',source:'by_ticker.'+key});}
   return out;
  }
  if(!Array.isArray(packet.request_records)||!Array.isArray(packet.calendar_rows))throw Error('Complete observation and calendar populations required');
  return packet.request_records.flatMap((record,i)=>{
   const r=obj(record);if(!Array.isArray(r.observations)||!Array.isArray(r.comparisons))throw Error('Malformed observation record');
   const event=obj(packet.calendar_rows[r.calendar_index]);
   if(!r.observations.length)return [{ticker:r.ticker,event_date:event.date,status:status(obj(r.acquisition).status),source:'request_records['+i+']'}];
   return r.observations.map((value,j)=>{const o=obj(value),v=obj(o.values),c=obj(r.comparisons[j]);
    return {ticker:r.ticker,event_date:event.date,event_period:event.fiscal_period,target:o.target_period_end,currency:o.reported_currency,
     basis:o.eps_basis,cik:o.reported_cik,eps:v.epsAvg,low:v.epsLow,high:v.epsHigh,analysts:v.numAnalystsEps,
     received:o.received_at,change:c.eps_change,change_pct:c.eps_change_pct_positive_base,
     status:[o.target_status,o.measurement_status,c.status].filter(x=>typeof x==='string').map(status).join(' / '),
     source:'request_records['+i+'].observations['+j+']'};
   });
  });
 }
 const api={CONTRACT,rows};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHEstimateObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
