(function(root){
  'use strict';
  const INPUTS={velocity:'data/velocity-acceleration.json',theme_rotation:'data/theme-momentum.json',themes:'data/momentum-themes.json',macro:'macro/regime.json',momentum:'data/momentum-leaders.json',catalysts:'data/catalysts.json',exposure:'etf-flows/stock-exposure-lookup.json'};
  const LABELS={velocity:'Volume observations',theme_rotation:'Reported theme roster',themes:'Theme classifications',macro:'Macro context',momentum:'Selected price observations',catalysts:'Catalyst context',exposure:'Provider-flow research exclusion'};
  const FLAGS=['calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const STATUS={source_read_unavailable:'Source read unavailable',invalid_json:'Invalid JSON',unrecognized_shape:'Unrecognized structure',source_error:'Source reports an error',received_object_unqualified:'Object received · unqualified',not_read_existing_flow_exclusion:'Existing flow exclusion · not read'};
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const identity=r=>r&&Number.isSafeInteger(r.bytes)&&r.bytes>=0&&typeof r.sha256==='string'&&/^[a-f0-9]{64}$/.test(r.sha256);
  const symbol=v=>typeof v==='string'&&/^[A-Z0-9][A-Z0-9.\-^]{0,24}$/.test(v);
  function render(p){
    let html='<p><strong>WAIT / abstain.</strong> No Cascade tier, forecast, position size or alert is qualified. WAIT is not a recommendation to hold.</p>';
    const ok=p&&p.measurement_contract==='theme-cascade-evidence.v1'&&p.status==='research_only'&&p.call==='WAIT'&&p.call_semantics==='abstain'&&FLAGS.every(k=>p[k]===false);
    if(!ok)return html+'<p>Unavailable, conflicting or legacy packet. Earlier rankings and allocations are withheld.</p>';
    html+='<p>Published '+esc(p.generated_at||'unknown')+'. Publication clocks do not establish observation freshness. The source table records availability, not independent investment votes.</p>';
    const sources=Array.isArray(p.sources)?p.sources:[],accepted={};let received=0;
    html+='<div role="region" tabindex="0" aria-label="Cascade source evidence table" style="max-width:100%;overflow:auto"><table style="min-width:850px;width:100%"><thead><tr><th>Declared source</th><th>Acquisition</th><th>Publication clock</th><th>Whole-source identity</th></tr></thead><tbody>';
    for(const [name,key] of Object.entries(INPUTS)){
      const matches=sources.filter(r=>r&&r.source===name&&r.source_key===key);const r=matches.length===1&&FLAGS.every(k=>matches[0][k]===false)?matches[0]:null;
      if(r)accepted[name]=r;const bound=identity(r?.original_ref);if(bound)received++;
      html+='<tr><th scope="row">'+LABELS[name]+'<br><code>'+key+'</code></th><td>'+esc(r?STATUS[r.status]||'Unrecognized status':'Missing or conflicting source')+'</td><td>'+esc(r?.source_generated_at||'Unavailable')+'<br>'+(r?.source_clock_status==='future'?'Future clock · unqualified':'Observation freshness unqualified')+'</td><td style="max-width:260px;overflow-wrap:anywhere">'+(bound?esc(r.original_ref.bytes)+' bytes<br>'+esc(r.original_ref.sha256):'Identity unavailable')+'</td></tr>';
    }
    html+='</tbody></table></div><p>'+received+' of 7 declared inputs have a complete received-body identity. The excluded flow input supplies no inferred zero, membership or vote.</p>';
    const roster=p.reported_theme_roster,ref=accepted.theme_rotation?.original_ref;
    const bound=roster&&roster.status==='reported_roster_unqualified'&&FLAGS.every(k=>roster[k]===false)&&identity(ref)&&identity(roster.source_ref)&&roster.source_ref.sha256===ref.sha256&&roster.source_ref.bytes===ref.bytes;
    const rows=bound&&Array.isArray(roster.theme_occurrences)&&roster.theme_occurrences.length<=10000?roster.theme_occurrences:null;
    const valid=rows&&rows.every((r,i)=>r&&r.source_index===i&&r.source_pointer==='/all_themes/'+i&&['reported_etf','duplicate_reported_etf','invalid_literal_etf'].includes(r.status));
    if(!valid)return html+'<p>Theme roster coordinates are unavailable or conflict with the captured source. Missing membership is not zero holdings.</p>';
    html+='<details><summary>'+rows.length+' reported theme occurrences · inspect source coordinates</summary><p>Reported symbols and repetitions describe this upstream list. They do not establish fund ownership, flows, a hot theme or historical market coverage.</p>';
    html+='<div role="region" tabindex="0" aria-label="Cascade theme roster table" style="max-width:100%;overflow:auto"><table style="min-width:600px;width:100%"><thead><tr><th>Reported ETF</th><th>Occurrence status</th><th>Source pointer</th></tr></thead><tbody>';
    for(const row of rows)html+='<tr><th scope="row">'+(symbol(row.etf)?esc(row.etf):'Invalid / missing symbol')+'</th><td>'+esc(row.status)+'</td><td><code>'+esc(row.source_pointer)+'</code></td></tr>';
    html+='</tbody></table></div></details>';
    const members=Array.isArray(roster.membership_occurrences)&&roster.membership_occurrences.length<=100000?roster.membership_occurrences:null;
    if(members){
      html+='<details><summary>'+members.length+' extracted membership occurrences · inspect source coordinates</summary><p>Partial or invalid source blocks may contain additional unknown memberships. Repeated pairs remain repeated reports.</p><div role="region" tabindex="0" aria-label="Cascade membership table" style="max-width:100%;overflow:auto"><table style="min-width:760px;width:100%"><thead><tr><th>Reported ETF</th><th>Reported symbol</th><th>Status</th><th>Source pointer</th></tr></thead><tbody>';
      for(const row of members){
        const ptr=typeof row?.source_pointer==='string'&&/^\/breadth_details\/[^/]{1,100}\/constituents_perf\/\d+$/.test(row.source_pointer)?row.source_pointer:null;
        html+='<tr><td>'+(symbol(row?.etf)?esc(row.etf):'Unavailable')+'</td><td>'+(symbol(row?.ticker)?esc(row.ticker):'Unavailable')+'</td><td>'+esc(['reported_membership','duplicate_reported_pair','invalid_literal_membership'].includes(row?.status)?row.status:'Invalid occurrence')+'</td><td>'+esc(ptr||'Coordinate unavailable')+'</td></tr>';
      }
      html+='</tbody></table></div></details>';
    }else html+='<p>Membership occurrence inventory unavailable.</p>';
    return html+'<p>All originals remain available for replay. Counts, repeated payloads and transformed prices do not establish predictive independence or portfolio authority.</p>';
  }
  const api={render};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHCascadeEvidence=api;
})(typeof globalThis==='object'?globalThis:this);
