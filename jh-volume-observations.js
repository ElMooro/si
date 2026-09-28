(function(root){
  'use strict';
  const FLAGS=['calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const FIELDS=[['baseline_volume_mean_20','Baseline mean · 20 observations','reported_volume_units','Reported volume units'],
    ['last_relative_volume','Last relative volume','multiple_of_prior_baseline','Baseline multiple'],
    ['relative_volume_slope_7','Relative-volume slope · 7 observations','baseline_multiples_per_reported_observation','Baseline multiples per observation'],
    ['volume_weighted_close_direction_7','Volume-weighted close direction · 7','unitless_signed_balance','Signed balance'],
    ['relative_volume_floor_change','Relative-volume floor change','fractional_change','Fractional change']];
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const date=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)?v:'Unknown';
  const identity=r=>r&&Number.isSafeInteger(r.bytes)&&r.bytes>=0&&/^[a-f0-9]{64}$/.test(r.sha256);
  const COUNTS={baseline_volume_mean_20:20,last_relative_volume:21,relative_volume_slope_7:27,volume_weighted_close_direction_7:35,relative_volume_floor_change:27};
  const exact=v=>typeof v==='string'&&v.length<=128&&/^-?\d+(\.\d+)?([Ee][+-]?\d+)?$/.test(v)&&Number.isFinite(Number(v));
  function traceable(m,name){
    if(!Array.isArray(m?.operands)||m.operands.length!==COUNTS[name]||!exact(m.value_exact)||Number(m.value_exact)!==m.value||date(m.start_date)==='Unknown'||date(m.end_date)==='Unknown')return false;
    const pointers=new Set();
    return m.operands.every(o=>{
      const match=typeof o?.source_pointer==='string'&&o.source_pointer.match(/^\/(?:historical\/)?(\d+)\/(open|high|low|close|volume)$/);
      if(!match||!Number.isSafeInteger(o.source_index)||Number(match[1])!==o.source_index||pointers.has(o.source_pointer)||date(o.observation_date)==='Unknown'||!exact(o.value_exact))return false;
      pointers.add(o.source_pointer);return true;
    });
  }
  function render(p,ticker){
    let html='<p><strong>WAIT / abstain.</strong> Volume patterns have no validated forecast, confirmation or sizing authority. WAIT is not a recommendation to hold.</p>';
    if(!p||p.measurement_contract!=='velocity-volume-observations.v1'||p.status!=='research_only'||p.call!=='WAIT'||p.call_semantics!=='abstain'||!FLAGS.every(k=>p[k]===false)){
      return html+'<p>Volume packet unavailable, conflicting or legacy. No early-warning score is qualified.</p>';
    }
    html+='<p>Published '+esc(p.generated_at||'unknown')+'. Publication time does not establish observation freshness. Each window uses twenty baseline observations followed by seven recent observations. Exchange sessions, corporate actions and volume units remain unverified.</p>';
    const all=Array.isArray(p.volume_observations)&&p.volume_observations.length<=80?p.volume_observations:[];
    const names=all.map(r=>r?.ticker);
    const rows=all.filter(r=>r&&typeof r.ticker==='string'&&/^[A-Z0-9][A-Z0-9.\-^]{0,24}$/.test(r.ticker)&&names.filter(t=>t===r.ticker).length===1&&FLAGS.every(k=>r[k]===false)&&(!ticker||r.ticker===ticker));
    if(!rows.length)return html+'<p>No unambiguous reported observations. This does not establish zero market activity.</p>';
    for(const row of rows){
      const bound=identity(row.history_original_ref),ref=row.history_original_ref;
      html+='<details><summary><strong>'+esc(row.ticker)+'</strong> · '+date(row.end_date)+' · reported volume observations</summary><p style="overflow-wrap:anywhere">'+(bound?esc(ref.bytes)+' bytes · SHA256 '+esc(ref.sha256):'Whole-source identity unavailable')+'</p>';
      html+='<p>Observation age: '+(Number.isSafeInteger(row.observation_age_calendar_days)&&row.observation_age_calendar_days>=0?esc(row.observation_age_calendar_days)+(row.observation_age_calendar_days===1?' calendar day':' calendar days'):'unknown')+'. These transformations share one OHLCV source family; they are not independent votes.</p>';
      html+='<div tabindex="0" role="region" aria-label="'+esc(row.ticker)+' volume observation table" style="max-width:100%;overflow:auto;border:1px solid var(--border,#384557)"><table style="min-width:800px;width:100%"><thead><tr><th>Measurement</th><th>Value</th><th>Unit</th><th>Observation window</th><th>Source operands</th></tr></thead><tbody>';
      for(const [name,label,unit,displayUnit] of FIELDS){
        const matches=(Array.isArray(row.measurements)?row.measurements:[]).filter(m=>m&&m.name===name),m=matches.length===1?matches[0]:null;
        const valid=bound&&m&&m.status==='measured'&&m.unit===unit&&FLAGS.every(k=>m[k]===false)&&typeof m.value==='number'&&Number.isFinite(m.value)&&traceable(m,name);
        const operands=m&&Array.isArray(m.operands)&&m.operands.length<=64?m.operands.map(o=>({
          source_pointer:typeof o?.source_pointer==='string'&&/^\/(historical\/)?\d+\/(open|high|low|close|volume)$/.test(o.source_pointer)?o.source_pointer:null,
          source_index:Number.isSafeInteger(o?.source_index)&&o.source_index>=0?o.source_index:null,
          observation_date:date(o?.observation_date),
          value_exact:typeof o?.value_exact==='string'&&o.value_exact.length<=128&&/^-?\d+(\.\d+)?([Ee][+-]?\d+)?$/.test(o.value_exact)?o.value_exact:null
        })):[];
        html+='<tr><th scope="row">'+label+'</th><td>'+(valid?esc(m.value.toLocaleString('en-US',{maximumSignificantDigits:8})):'Unavailable')+'</td><td>'+displayUnit+'</td><td>'+date(m?.start_date)+' → '+date(m?.end_date)+'</td><td>';
        if(m)html+='<details><summary>'+operands.length+' source operands</summary><p>'+esc(m.definition||'Definition unavailable')+'</p><p>'+esc(valid?'Descriptive observation':m.reason||'Missing or conflicting measurement identity')+'</p><pre style="max-width:36rem;max-height:16rem;overflow:auto;white-space:pre-wrap">'+esc(JSON.stringify(operands,null,2))+'</pre></details>';
        else html+='Missing or duplicate measurement';
        html+='</td></tr>';
      }
      html+='</tbody></table></div></details>';
    }
    return html+'<p>Pending state remains preserved without promotion or expiration. Slope is not demonstrated advance warning; signed volume is not measured investor flow or institutional accumulation.</p>';
  }
  const api={render};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHVolumeObservations=api;
})(typeof globalThis==='object'?globalThis:this);
