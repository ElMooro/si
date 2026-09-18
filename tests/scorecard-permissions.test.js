const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../signal-scorecard.html'),'utf8');
const script=html.match(/<script>\s*([\s\S]*?)<\/script>/)[1];

async function render(packet){
  const elements=new Map();
  const get=id=>{if(!elements.has(id))elements.set(id,{});return elements.get(id);};
  await vm.runInNewContext(script,{document:{getElementById:get},fetch:async()=>({ok:true,json:async()=>packet})});
  return get;
}

function packet(){return {integrity:{contract:'outcome-lineage.v1',scan_complete:true,price_evidence_contract:'provider-price-replay.v1',price_archive_verification_required:true},n_outcomes_quarantined:299,
  n_signals_tracked:1,n_signals_graded:0,n_insufficient:1,n_outcomes_scored:0,avg_graded_wilson_lb:null,
  scorecard:[{signal_type:'eng:crypto-emergence',grade:'—',status:'INSUFFICIENT',n_scored:0,n_neutral:0,n_legacy:0,n_unresolved:0,
    n_quarantined:299,hit_rate:null,wilson_lb:0,wilson_ub:1,edge_vs_coinflip_pct:null,avg_return_pct:null,performance_multiplier:1}],
  interpretation:'Research only; unverifiable prices are excluded.',data_quality_flags:[{signal_type:'<bad>',n_total:299,n_scored:0,n_legacy:0,n_unresolved:0,n_quarantined:299,quarantine_reasons:{instrument_or_currency_mismatch:299}}]};}

test('No verified sample shows unavailable returns and explicit research permissions',async()=>{
 const get=await render(packet());
 assert.match(get('permissions').textContent,/sizing remain disabled/);
 assert.match(get('kpis').innerHTML,/Quarantined outcomes/);
 assert.match(get('kpis').innerHTML,/299/);
 assert.doesNotMatch(get('board').innerHTML,/-50%|null%|NaN|280542/);
 assert.match(get('dqwrap').innerHTML,/instrument_or_currency_mismatch/);
 assert.match(get('dqwrap').innerHTML,/&lt;bad&gt;/);
});

test('Legacy packet cannot imply current validation authority',async()=>{
 const p=packet();delete p.integrity;
 const get=await render(p);
 assert.match(get('permissions').textContent,/Legacy scorecard/);
 assert.match(get('kpis').innerHTML,/UNVERIFIED/);
});
test('Structural hashes alone cannot display a verified-price claim',async()=>{
 const p=packet();delete p.integrity.price_archive_verification_required;
 const get=await render(p);
 assert.match(get('permissions').textContent,/Legacy scorecard/);
});
