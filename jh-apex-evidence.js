(function(root){
  'use strict';
  const INPUTS={scorecard:'data/signal-scorecard.json',cascade_validation:'data/cascade-validation-log.json',cascade_context:'data/theme-cascade-calibrated.json',positioning:'data/pump-positioning.json',momentum:'data/momentum-leaders.json',squeeze:'data/microcap-float-squeeze.json',flow:'data/options-flow-scanner.json',insider:'data/insider-clusters.json',regime:'data/report.json'};
  const LABELS={scorecard:'Validation scorecard',cascade_validation:'Cascade validation',cascade_context:'Cascade context',positioning:'Positioning observations',momentum:'Selected price observations',squeeze:'Microcap flow observations',flow:'Options flow observations',insider:'Insider clusters',regime:'Market context'};
  const FLAGS=['calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const STATUS={source_read_unavailable:'Source read unavailable',invalid_json:'Invalid JSON',unrecognized_shape:'Unrecognized structure',source_error:'Source reports an error',received_object_unqualified:'JSON object received · unqualified'};
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function present(packet){
    const ok=packet&&packet.measurement_contract==='apex-context-evidence.v1'&&packet.status==='research_only'&&packet.call==='WAIT'&&packet.call_semantics==='abstain'&&FLAGS.every(k=>packet[k]===false);
    const sourceRows=ok&&Array.isArray(packet.sources)?packet.sources:[];let received=0;const hashes=new Set();let rows='';
    for(const [name,key] of Object.entries(INPUTS)){
      const candidates=sourceRows.filter(r=>r&&r.source===name&&r.source_key===key);const r=candidates.length===1&&FLAGS.every(k=>candidates[0][k]===false)?candidates[0]:null;
      const ref=r?.original_ref;const identity=ref&&Number.isSafeInteger(ref.bytes)&&ref.bytes>=0&&typeof ref.sha256==='string'&&/^[a-f0-9]{64}$/.test(ref.sha256);
      if(identity){received++;hashes.add(ref.sha256);}
      const clock=r?.source_clock_status==='future'?'Future publication clock · unqualified':r?.source_clock_status==='reported_publication_clock'?'Reported publication clock · observation freshness unqualified':'Clock unavailable';
      rows+='<tr><th scope="row">'+LABELS[name]+'<br><code>'+key+'</code></th><td>'+esc(r?STATUS[r.status]||'Unrecognized status':'Missing or conflicting source')+'</td><td>'+esc(r?.source_generated_at||'Unavailable')+'<br>'+clock+'</td><td>'+(identity?esc(ref.bytes)+' bytes<br><code>'+esc(ref.sha256)+'</code>':'Identity unavailable')+'</td></tr>';
    }
    return {headline:'WAIT / abstain. No Apex score, probability, tier or position size is qualified. WAIT is not a recommendation to hold.',
      status:ok?'Research publication '+String(packet.generated_at||'with unknown clock'):'Unavailable, conflicting or legacy packet. Earlier rankings are withheld.',
      counts:ok?received+' of 9 declared sources have a complete-payload identity; '+hashes.size+' distinct payload hashes. These are availability counts, not independent votes.':'Source availability cannot be established from this packet.',rows};
  }
  function render(document,packet){const p=present(packet);document.getElementById('headline').textContent=p.headline;document.getElementById('publication').textContent=p.status;document.getElementById('coverage').textContent=p.counts;document.getElementById('evidence-rows').innerHTML=p.rows;}
  const api={present,render};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHApexEvidence=api;
})(typeof globalThis==='object'?globalThis:this);
