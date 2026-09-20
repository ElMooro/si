/* Canonical USD context: exact-output verification, dates and distinct units. */
(function(root){
 'use strict';
 const PREFIX='data/eurodollar-research/',CURRENT='data/eurodollar-stress.json';
 const permissions=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=v=>typeof v==='number'&&Number.isFinite(v);
 const fmt=v=>finite(v)?v.toLocaleString('en-US',{maximumFractionDigits:6}):'Unavailable';
 const safe=key=>/^data\/eurodollar-research\/(?:runs|inputs|outputs|compilers)\/[a-f0-9]{64}\.(?:json|py)$/.test(key||'');
 const typed=p=>p?.contract==='eurodollar-native-research.v1'&&p.call===null&&p.portfolio_action==='WAIT'&&permissions.every(k=>p[k]===false)&&p.measurements&&typeof p.measurements==='object'&&!Array.isArray(p.measurements)&&Object.keys(p.measurements).length===9;
 function current(p,now=Date.now()){
  const source=Date.parse(p.source_generated_at),generated=Date.parse(p.generated_at),due=Date.parse(p.freshness?.pipeline_check_due_at);
  return Number.isFinite(source)&&Number.isFinite(generated)&&source<=generated&&generated<=now&&now<due&&due===source+26*3600000;
 }
 function rowCurrent(p,m,now=Date.now()){
  return current(p,now)&&m.quality?.status==='within_age_ceiling'&&finite(m.value)&&now<Date.parse(m.source_valid_until)&&Date.parse(m.observation_date+'T00:00:00Z')<=now;
 }
 function publisher(m){const url='https://fred.stlouisfed.org/series/'+m.series_id;return /^[A-Z0-9]+$/.test(m.series_id)&&m.source_url===url?'<a href="'+url+'">'+esc(m.label||m.series_id)+'</a>':esc(m.label||m.series_id);}
 function render(p,now=Date.now()){
  if(!typed(p))return '<p role="alert">Verified USD context unavailable.</p>';
  const ms=Object.values(p.measurements),count=ms.filter(m=>rowCurrent(p,m,now)).length,r=p.repo_comparison,d=p.yield_change_dispersion;
  const pair=r.current_comparison_available&&['SOFR','DFF'].every(k=>rowCurrent(p,p.measurements[k],now)&&p.measurements[k].observation_date===r.observation_date);
  const run=safe(p.replay?.manifest_key)?'<a href="/'+p.replay.manifest_key+'">Inspect this immutable run</a>':'Run unavailable';
  let html='<style>#eurodollar-context{font:15px/1.6 -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;color:#d7dde8;min-width:0;overflow-wrap:anywhere}#eurodollar-context .table-scroll{overflow:auto;max-width:100%}#eurodollar-context table{width:100%;border-collapse:collapse;font-size:13px}#eurodollar-context th,#eurodollar-context td{padding:12px;border-bottom:1px solid #28323f;text-align:left;vertical-align:top}#eurodollar-context th{color:#a5afbd}#eurodollar-context a{color:#d5b372}#eurodollar-context summary{cursor:pointer;min-height:44px}#eurodollar-context button{font:inherit;min-height:44px;padding:10px;background:#161e2a;color:#d7dde8;border:1px solid #4a5669;border-radius:6px}@media(max-width:600px){#eurodollar-context table{min-width:720px}#eurodollar-context h2{font-size:23px}}</style><h2>USD funding and macro context</h2><p>'+count+' / 9 within age ceilings. Compiled '+esc(p.generated_at)+' from canonical observations collected '+esc(p.source_generated_at)+'.</p><p>Research only · WAIT. Reused macro, credit and volatility measures do not count as independent investment votes.</p><div class="table-scroll" tabindex="0" role="region" aria-label="Dated USD context"><table><thead><tr><th>Source / definition</th><th>Retained value</th><th>Unit</th><th>Observed</th><th>Current use</th></tr></thead><tbody>';
  html+=ms.map(m=>'<tr><td>'+publisher(m)+'<details><summary>Definition and source window</summary><p>'+esc(m.interpretation)+'</p><p>'+fmt(m.history_coverage?.retained_rows)+' retained rows from '+esc(m.history_coverage?.first_date)+' to '+esc(m.history_coverage?.last_date)+'. Current-vintage history; historical first availability is unqualified.</p></details></td><td>'+esc(m.exact_value??'Unavailable')+'</td><td>'+esc(m.unit)+'</td><td>'+esc(m.observation_date||'Unavailable')+'<br>'+fmt(m.quality?.observation_age_days)+' days old at compilation</td><td>'+(rowCurrent(p,m,now)?'Within age ceiling':'Withheld')+'</td></tr>').join('');
  html+='</tbody></table></div><p><strong>SOFR minus effective federal funds:</strong> '+(pair?fmt(r.difference_bps)+' bp':'Withheld')+' on '+esc(r.observation_date||'Unavailable')+'. '+esc(r.interpretation)+'</p>';
  const dispersion=d.current_comparison_available&&rowCurrent(p,p.measurements.DGS10,now);
  html+='<details><summary>Ten-year yield-change dispersion and retained FX context</summary><p>Sample standard deviation: '+(dispersion?fmt(d.sample_stddev_bps)+' bp':'Withheld')+' across '+fmt(d.valid_differences)+' valid adjacent provider-row changes, '+esc(d.first_from_date)+' to '+esc(d.last_to_date)+'. Missing rows are not bridged. Maximum calendar gap '+fmt(d.maximum_calendar_gap_days)+' days.</p><p>Annualized reference: '+(dispersion?fmt(d.annualized_reference_bps)+' bp':'Withheld')+'. '+esc(d.annualization_assumption)+' This is neither MOVE nor bond-return volatility.</p><p>Upstream synthetic USD 20-day return: '+fmt(p.fx_context?.reported_usd_synthetic_20d_pct)+'% as reported, generated '+esc(p.fx_context?.source_generated_at||'unavailable')+'. Its price identity, weights and window remain unverified. It supplies no vote.</p></details>';
  html+='<p>'+run+' · <a href="/data/eurodollar-research-verification.json">Deployment acceptance</a> · <a href="/eurodollar.html#scenario-form">Enter a USD liability and rate shock</a></p><p>Rates retain percent units; multiply by 100 for basis points. Observation age is separate from collection time. These broad age ceilings are not a verified release calendar.</p><button type="button" data-usd-refresh>Refresh and verify</button>';
  return html;
 }
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved research path');const r=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!r.ok)throw Error('HTTP '+r.status);
  const raw=await r.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(packet,fetcher,signal){
  const key=packet?.replay?.manifest_key;if(!typed(packet)||!/^data\/eurodollar-research\/runs\/[a-f0-9]{64}\.json$/.test(key||''))throw Error('Native research required');
  const run=await load(key,fetcher,signal),m=run.doc;
  if(key!==PREFIX+'runs/'+await sha(run.raw)+'.json'||m.contract!=='eurodollar-native-replay.v1'||m.output_sha256!==packet.replay.output_sha256||m.generated_at!==packet.generated_at)throw Error('Run differs');
  const ref=m.output;if(ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=packet;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current packet differs');return packet;
 }

 let pending;
 async function mount(){
  const host=root.document?.getElementById('eurodollar-context');if(!host)return;
  if(pending)pending.abort();const c=new AbortController();pending=c;const timer=setTimeout(()=>c.abort(),15000);
  host.innerHTML='<p role="status">Verifying canonical USD context…</p>';
  try{const p=await verifyPacket((await load(CURRENT,root.fetch.bind(root),c.signal)).doc,root.fetch.bind(root),c.signal);
   if(c!==pending)return;host.innerHTML=render(p);host.querySelector('[data-usd-refresh]').onclick=mount;
  }catch(e){if(c===pending){host.innerHTML='<p role="alert">Verified USD context unavailable. No substitute score is shown.</p><button data-usd-refresh>Retry</button>';host.querySelector('[data-usd-refresh]').onclick=mount;}}
  finally{clearTimeout(timer);}
 }
 const api={render,verifyPacket,current,rowCurrent,typed,mount};root.JHEurodollarResearch=api;if(typeof module!=='undefined')module.exports=api;
 if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',mount);else mount();}
})(typeof globalThis!=='undefined'?globalThis:this);
