(function(root){
  'use strict';
  const FLAGS=['calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const FIELDS=[['price_change_5','Price change · 5 observations'],['price_change_20','Price change · 20 observations'],
    ['price_change_60','Price change · 60 observations'],['mean_true_range_14','Mean true range · 14'],
    ['sample_log_return_std_30','Log-return standard deviation · 30'],['mean_volume_20','Mean reported volume · 20'],['last_completed_close','Last completed-date close']];
  const UNITS={percent_price_change:'% price change',reported_price_units:'Reported price units',percent_per_reported_observation:'% per reported observation',reported_volume_units:'Reported volume units'};
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function qualifiedShape(p){return p&&p.measurement_contract==='positioning-price-observations.v1'&&p.status==='research_only'&&p.call==='WAIT'&&p.call_semantics==='abstain'&&FLAGS.every(k=>p[k]===false);}
  function identity(ref){return ref&&Number.isSafeInteger(ref.bytes)&&ref.bytes>=0&&/^[a-f0-9]{64}$/.test(ref.sha256)?esc(ref.bytes)+' bytes · SHA256 '+esc(ref.sha256):'Source identity unavailable';}
  function render(packet,ticker){
    let html='<p><strong>WAIT / abstain.</strong> Position sizes, grades, entry targets and downside limits are unavailable. WAIT is not a recommendation to hold.</p>';
    if(!qualifiedShape(packet))return html+'<p>Positioning packet unavailable, conflicting or legacy. Its allocations and scores are not qualified.</p>';
    html+='<p>Published '+esc(packet.generated_at||'unknown')+'. Publication time does not establish observation freshness. Windows use reported dates; exchange sessions, corporate actions, total returns and quote-currency binding remain unverified.</p>';
    const all=Array.isArray(packet.price_observations)&&packet.price_observations.length<=12?packet.price_observations:[];
    const names=all.map(r=>r&&r.ticker);
    const rows=all.filter(r=>r&&typeof r.ticker==='string'&&/^[A-Z0-9][A-Z0-9.\-^]{0,24}$/.test(r.ticker)&&names.filter(n=>n===r.ticker).length===1&&FLAGS.every(k=>r[k]===false)&&(!ticker||r.ticker===ticker));
    if(!rows.length)return html+'<p>No unambiguous descriptive price observations in this packet. This does not mean zero risk or an empty portfolio.</p>';
    for(const row of rows){
      html+='<details open><summary><strong>'+esc(row.ticker)+'</strong> · reported price observations</summary><p style="overflow-wrap:anywhere">'+identity(row.history_original_ref)+'</p>';
      html+='<div tabindex="0" role="region" aria-label="'+esc(row.ticker)+' price observation table" style="max-width:100%;overflow:auto;border:1px solid var(--border,#384557)"><table style="min-width:760px;width:100%"><thead><tr><th>Measurement</th><th>Value</th><th>Unit</th><th>Observation window</th><th>Source operands</th></tr></thead><tbody>';
      for(const [name,label] of FIELDS){
        const matches=(Array.isArray(row.measurements)?row.measurements:[]).filter(m=>m&&m.name===name);const m=matches.length===1?matches[0]:null;
        const unit=name.startsWith('price_change_')?'percent_price_change':name==='sample_log_return_std_30'?'percent_per_reported_observation':name==='mean_volume_20'?'reported_volume_units':'reported_price_units';
        const valid=m&&m.status==='measured'&&m.unit===unit&&FLAGS.every(k=>m[k]===false)&&typeof m.value==='number'&&Number.isFinite(m.value);
        const operands=m&&Array.isArray(m.operands)&&m.operands.length<=200?m.operands.map(o=>({
          source_pointer:typeof o?.source_pointer==='string'&&/^\/(historical\/)?\d+\/(open|high|low|close|volume)$/.test(o.source_pointer)?o.source_pointer:null,
          source_index:Number.isSafeInteger(o?.source_index)&&o.source_index>=0?o.source_index:null,
          observation_date:typeof o?.observation_date==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(o.observation_date)?o.observation_date:null,
          value_exact:typeof o?.value_exact==='string'&&o.value_exact.length<=128&&/^-?\d+(\.\d+)?([Ee][+-]?\d+)?$/.test(o.value_exact)?o.value_exact:null
        })):[];
        html+='<tr><th scope="row">'+label+'</th><td>'+ (valid?esc(m.value.toLocaleString('en-US',{maximumSignificantDigits:8})):'Unavailable')+'</td><td>'+UNITS[unit]+'</td><td>'+esc(m?.start_date||'Unknown')+' → '+esc(m?.end_date||'Unknown')+'</td><td>';
        if(m){html+='<details><summary>'+operands.length+' source operands</summary><p>'+esc(m.definition||'Definition unavailable')+'</p><p>'+esc(valid?'Reported descriptive measurement':m.reason||'Conflicting or unqualified measurement')+'</p><pre style="max-width:36rem;max-height:16rem;overflow:auto;white-space:pre-wrap">'+esc(JSON.stringify(operands,null,2))+'</pre></details>';}
        else html+='Missing or duplicate measurement';
        html+='</td></tr>';
      }
      html+='</tbody></table></div></details>';
    }
    return html;
  }
  function basket(){return '<p><strong>Portfolio risk unavailable.</strong> No allocation is produced from uncalibrated scores or default volatility. A stop price is not a maximum-loss guarantee.</p><p>Position sizing requires an explicit portfolio, valuation currency, liquidity and risk assumptions, plus validated decision evidence. Price observations below do not supply that authority.</p>';}
  const api={render,basket};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHPositioningEvidence=api;
})(typeof globalThis==='object'?globalThis:this);
