(function(root){
 'use strict';
 const PREFIX='data/capital-structure-research/',CURRENT='data/share-flows.json',HASH=/^[a-f0-9]{64}$/;
 const COMPILERS={capital_structure_source:'61b31c0a6f7e45670427c67163cbfa7599d3d72de77650bb827a63168b720404',capital_structure_measurements:'a10546e8943ef9ff0538ab47e29e2a60db0aa4b6b3efb91ec93342e09d89329e',capital_structure_research:'fb810a99a14f804f00d0cd2a7026d8793ca9382a822ed5212ed5ec9952e98ef1',statement_research_source:'fe019e9a09192db264bdec8de7e6b9b18f553a0867ef6ba471bb3c8e3c720433',statement_research_identity:'f9c56e31a43e82b913d7aaae77af84c9c2464802a2cf95a86f96efe16d20daab',statement_measurements:'6529d6bbe2236615572ea97a0e5e08d340673c6d86facbe4ebd951736f866b7c',capital_structure_store:'0af42552aa24835fe42f08082d29adcdcf71c6d36c3e06050552fda1a614971c'};
 const I='income-statement',C='cash-flow-statement';
 const FORMULAS={
  cash_repurchase_outflow:[[[C,'commonStockRepurchased',-1]],null,[[C,'commonStockRepurchased','le']]],
  cash_common_stock_issuance:[[[C,'commonStockIssuance',1]],null,[[C,'commonStockIssuance','ge']]],
  cash_common_dividends:[[[C,'commonDividendsPaid',-1]],null,[[C,'commonDividendsPaid','le']]],
  gross_common_cash_distribution:[[[C,'commonStockRepurchased',-1],[C,'commonDividendsPaid',-1]],null,[[C,'commonStockRepurchased','le'],[C,'commonDividendsPaid','le']]],
  net_common_cash_return:[[[C,'commonStockIssuance',-1],[C,'commonStockRepurchased',-1],[C,'commonDividendsPaid',-1]],null,[[C,'commonStockIssuance','ge'],[C,'commonStockRepurchased','le'],[C,'commonDividendsPaid','le']]],
  common_issuance_cash_residual:[[[C,'netCommonStockIssuance',1],[C,'commonStockIssuance',-1],[C,'commonStockRepurchased',-1]],null,[]],
  cash_repurchase_to_operating_cash_flow_pct:[[[C,'commonStockRepurchased',-1]],[C,'operatingCashFlow'],[[C,'commonStockRepurchased','le']]],
  sbc_to_operating_cash_flow_pct:[[[C,'stockBasedCompensation',1]],[C,'operatingCashFlow'],[]],
  sbc_to_revenue_pct:[[[C,'stockBasedCompensation',1]],[I,'revenue'],[]],
  weighted_diluted_over_basic_pct:[[[I,'weightedAverageShsOutDil',1],[I,'weightedAverageShsOut',-1]],[I,'weightedAverageShsOut'],[[I,'weightedAverageShsOutDil','gt']]]
 };
 const flags=v=>v&&['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'].every(k=>v[k]===false)&&v.independent_investment_votes===0&&v.call===null&&v.score===null;
 function canonical(v){let s=Array.isArray(v)?'['+v.map(canonical).join(',')+']':v&&typeof v==='object'?'{'+Object.keys(v).sort().map(k=>canonical(k)+':'+canonical(v[k])).join(',')+'}':JSON.stringify(v);return s.replace(/[\u007f-\uffff]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'));}
 async function hash(bytes){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),v=>v.toString(16).padStart(2,'0')).join('');}
 function path(key){return key===CURRENT||typeof key==='string'&&new RegExp('^'+PREFIX+'(?:runs|inputs|outputs|records)/[a-f0-9]{64}\\.json$').test(key);}
 async function get(fetcher,key,ref){
  if(!path(key))throw Error('Unrecognized recorded research path');
  const response=await fetcher('/'+key,{cache:'no-store',credentials:'same-origin'});
  if(!response.ok)throw Error('Recorded research unavailable (HTTP '+response.status+')');
  const bytes=new Uint8Array(await response.arrayBuffer());if(bytes.length>32*1024*1024)throw Error('Research artifact exceeds its size bound');
  const digest=await hash(bytes);
  if(ref&&(ref.key!==key||ref.bytes!==bytes.length||ref.sha256!==digest))throw Error('Recorded artifact bytes differ');
  if(key!==CURRENT&&key.split('/').pop().slice(0,64)!==digest)throw Error('Recorded artifact identity differs');
  return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
 }
 function valid(p){return flags(p)&&p.contract==='capital-structure-original-research.v1'&&Array.isArray(p.issuers)&&p.issuers.length===p.reported_names&&p.reported_names>0&&p.reported_names<=5000&&new Set(p.issuers.map(v=>v.symbol)).size===p.reported_names&&p.issuers.every(v=>flags(v)&&/^[A-Z0-9][A-Z0-9.-]{0,15}$/.test(v.symbol)&&Number.isInteger(v.original_rows)&&v.original_rows>=0)&&p.issuers.reduce((s,v)=>s+v.original_rows,0)===p.provider_rows&&Object.keys(p.sources||{}).length===p.provider_responses&&p.provider_responses===p.reported_names*7&&p.quality?.every_original_row_conserved===true&&p.quality.complete_planned_population===true&&p.quality.original_source_bytes_replayed===true&&p.quality.current_sec_index_replayed===true&&p.quality.historical_security_continuity_verified===false&&p.quality.split_basis_verified===false;}
 async function load(fetcher,pinned){
  let current=null,reference;
  if(pinned!=null){if(!HASH.test(pinned))throw Error('Invalid recorded run');reference=PREFIX+'runs/'+pinned+'.json';}
  else{current=await get(fetcher,CURRENT);if(!valid(current)||!current.replay)throw Error('The native Share Flows packet has not yet qualified for this research view');reference=current.replay.manifest_key;}
  const run=await get(fetcher,reference);
  if(run.contract!=='capital-structure-original-replay.v1'||!HASH.test(run.output_sha256)||canonical(Object.keys(run.compilers||{}).sort())!==canonical(Object.keys(COMPILERS).sort()))throw Error('Unsupported recorded compiler');
  for(const [name,digest] of Object.entries(COMPILERS))if(run.compilers[name].sha256!==digest||run.compilers[name].key!==PREFIX+'compilers/'+digest+'.py')throw Error('Unreviewed compiler version');
  const packet=await get(fetcher,run.output.key,run.output);
  if(!valid(packet)||run.output.sha256!==run.output_sha256||packet.generated_at!==run.generated_at)throw Error('Recorded publication differs');
  const clockKeys=['source_acquisition_started_at','source_acquisition_completed_at','generated_at'];
  const clocks=clockKeys.map(k=>typeof packet[k]==='string'&&/(Z|[+-]\d\d:\d\d)$/.test(packet[k])?Date.parse(packet[k]):NaN);
  if(clocks.some(v=>!Number.isFinite(v))||clocks[0]>clocks[1]||clocks[1]>clocks[2]||clocks[2]>Date.now())throw Error('Recorded acquisition clocks are invalid or future dated');
  if(current){const body={...current};delete body.replay;if(current.replay.output_sha256!==run.output_sha256||await hash(new TextEncoder().encode(canonical(body)))!==run.output_sha256)throw Error('Current packet differs from its recorded run');}
  const index=await get(fetcher,packet.identity_index.original.key,packet.identity_index.original),pairs=new Map();
  if(packet.identity_index.url!=='https://www.sec.gov/files/company_tickers.json'||!index||Array.isArray(index)||Object.keys(index).length<1000||Object.keys(index).length>50000)throw Error('Complete dated SEC identity index required');
  for(const v of Object.values(index)){if(!v||typeof v.ticker!=='string'||!Number.isInteger(v.cik_str)||v.cik_str<=0||v.cik_str>=1e10)throw Error('Invalid current issuer identity');if(!pairs.has(v.ticker))pairs.set(v.ticker,new Set());pairs.get(v.ticker).add(String(v.cik_str).padStart(10,'0'));}
  return {packet,run,reference,pairs,runId:reference.split('/').pop().slice(0,64)};
 }
 function fraction(v){if(typeof v!=='string'||!/^[-]?\d{1,230}(?:\.\d{1,230})?$/.test(v))throw Error('Invalid exact decimal');const [n,d='']=v.replace(/^-/,'').split('.');return [(v[0]==='-'?-1n:1n)*BigInt(n+d),10n**BigInt(d.length)];}
 const add=(a,b)=>[a[0]*b[1]+b[0]*a[1],a[1]*b[1]];
 function rounded(a){if(a[1]<=0n)throw Error('Invalid rational denominator');const neg=a[0]<0n,top=(neg?-a[0]:a[0])*10n**12n;let n=top/a[1],r=top%a[1];if(2n*r>a[1]||(2n*r===a[1]&&n%2n))n++;return (neg&&n?'-':'')+n/10n**12n+'.'+String(n%10n**12n).padStart(12,'0');}
 function rational(shown,expected){if(!shown||shown.value!==rounded(expected)||!/^[-]?\d{1,500}$/.test(shown.exact?.numerator)||!/^\d{1,500}$/.test(shown.exact?.denominator)||BigInt(shown.exact.denominator)<=0n||BigInt(shown.exact.numerator)*expected[1]!==expected[0]*BigInt(shown.exact.denominator))throw Error('Recorded arithmetic differs');}
 function metric(name,m){
  if(!FORMULAS[name]||!m||m.supports_investment_action!==false||m.period_annualized!==false)throw Error('Unsupported measurement');
  const [terms,den,signs]=FORMULAS[name],refs=terms.map(v=>v.slice(0,2));if(den&&!refs.some(v=>String(v)===String(den)))refs.push(den);
  if(!Array.isArray(m.inputs)||m.inputs.length!==refs.length)throw Error('Measurement input count differs');
  const nums=new Map();m.inputs.forEach((v,i)=>{if(v.endpoint!==refs[i][0]||v.field!==refs[i][1])throw Error('Measurement definition differs');nums.set(String(refs[i]),v.numeric_value===null?null:fraction(v.numeric_value));});
  let reason=m.inputs.some(v=>v.source_id===null)?'required_original_statement_row_missing':[...nums.values()].some(v=>v===null)?'required_provider_field_unavailable':null;
  if(!reason&&signs.some(([e,f,s])=>{const n=nums.get(String([e,f]))[0];return s==='le'?n>0n:s==='ge'?n<0n:n<=0n;}))reason='provider_cash_flow_or_share_sign_conflict';
  if(!reason&&den&&nums.get(String(den))[0]<=0n)reason='nonpositive_denominator';
  if(!reason&&name==='weighted_diluted_over_basic_pct'){const d=nums.get(String([I,'weightedAverageShsOutDil'])),b=nums.get(String([I,'weightedAverageShsOut']));if(d[0]*b[1]<b[0]*d[1])reason='diluted_denominator_below_basic';}
  if(reason){if(m.value!==null||m.exact!==null||m.status!==reason)throw Error('Unavailable source used in calculation');return;}
  if(m.status!=='descriptive_provider_row_calculation')throw Error('Unrecognized available measurement status');
  let value=terms.reduce((s,[e,f,c])=>{const v=nums.get(String([e,f]));return add(s,[v[0]*BigInt(c),v[1]]);},[0n,1n]);
  if(den){const d=nums.get(String(den));value=[value[0]*d[1]*100n,value[1]*d[0]];}rational(m,value);
  if(m.unit!==(den?'%':m.identity?.reportedCurrency))throw Error('Measurement unit differs');
 }
 async function record(fetcher,state,symbol){
  const summary=state.packet.issuers.find(v=>v.symbol===symbol);if(!summary||!flags(summary))throw Error('Choose an exact symbol from the recorded population');
  const shard=await get(fetcher,summary.record.key,summary.record);
  if(!flags(shard)||shard.contract!=='capital-structure-issuer-records.v1'||shard.requested_symbol!==symbol||!Array.isArray(shard.records)||!Array.isArray(shard.snapshots)||shard.records.length!==summary.records||shard.snapshots.length!==summary.snapshots||canonical(shard.identity_index)!==canonical(state.packet.identity_index))throw Error('Issuer record differs');
  const seen=new Set();let count=0;
  for(const row of [...shard.records,...shard.snapshots]){
   if(!flags(row)||!HASH.test(row.record_id)||!Array.isArray(row.source_rows))throw Error('Invalid issuer observation');
   for(const c of row.source_rows){const o=state.packet.sources[c.capture_id],id=c.capture_id+':'+c.source_row;if(!o||o.original_sha256!==c.source_id||o.request.symbol!==symbol||o.request.endpoint!==c.endpoint||o.request.period!==c.request_period||!Number.isInteger(c.source_row)||c.source_row<0||c.source_row>=o.rows||seen.has(id))throw Error('Source coordinate differs');seen.add(id);count++;}
   if(row.measurements?.metrics){
    const pair=state.pairs.get(symbol);if(!pair||pair.size!==1||!pair.has(row.identity?.cik)||row.identity.symbol!==symbol||!flags(row.measurements)||canonical(Object.keys(row.measurements.metrics).sort())!==canonical(Object.keys(FORMULAS).sort()))throw Error('Issuer or measurement identity differs');
    for(const [name,m] of Object.entries(row.measurements.metrics)){
     metric(name,m);
     for(const input of m.inputs){if(input.source_id===null)continue;const original=row.reported_rows.find(r=>r.coordinate.source_id===input.source_id&&r.coordinate.source_row===input.source_row&&r.coordinate.endpoint===input.endpoint);if(!original||!original.facts.some(f=>canonical(f)===canonical(input)))throw Error('Measurement cannot be traced to its reported field');}
    }
   }
  }
  if(count!==summary.original_rows||count!==shard.source_row_count)throw Error('Original row coverage differs');
  return {summary,shard,artifact:summary.record};
 }
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const plain=v=>v==null?'Unavailable':typeof v==='object'?JSON.stringify(v):String(v);
 const decimal=v=>v===null?'Unavailable':String(v).replace(/(\.\d*?)0+$/,'$1').replace(/\.$/,'');
 function sourceDetails(row){return '<details><summary>Reported fields and source coordinates</summary><pre>'+esc(JSON.stringify(row.reported_rows||row.reported,null,2))+'</pre><pre>'+esc(JSON.stringify(row.source_rows,null,2))+'</pre></details>';}
 function snapshotFields(row){return '<div class="table-scroll"><table><thead><tr><th>Reported field</th><th>Reported value</th><th>Availability</th></tr></thead><tbody>'+(row.reported?.facts||[]).map(f=>'<tr><th scope="row">'+esc(f.field)+'</th><td class="number">'+esc(f.numeric_value!==null?f.numeric_value:f.present?plain(f.reported?.value):'Unavailable')+'</td><td>'+esc(f.status.replaceAll('_',' '))+'</td></tr>').join('')+'</tbody></table></div>';}
 function renderRecord(result){
  const {summary,shard}=result;
  return '<h2>'+esc(summary.symbol)+' <span class="muted">'+summary.original_rows+' original rows</span></h2><p>Current SEC CIK: '+esc(shard.current_sec_ciks.join(', ')||'Uncorroborated')+'. Historical security continuity is unverified.</p><p><a href="/'+esc(result.artifact.key)+'">Open complete recorded issuer JSON</a></p>'+
   shard.records.map(row=>'<article class="observation"><h3>'+esc(row.identity?.date||'Unresolved period')+' · '+esc(row.identity?.period||row.request_period)+'</h3><p class="muted">'+esc(row.status)+' · reported filing '+esc(row.identity?.filingDate||'unavailable')+' · reported acceptance '+esc(row.identity?.acceptedDate||'unavailable')+' (timezone unverified)</p>'+
    (row.measurements?'<div class="table-scroll"><table><thead><tr><th>Measurement</th><th>Value</th><th>Unit</th><th>Evidence</th></tr></thead><tbody>'+Object.entries(row.measurements.metrics).map(([key,m])=>'<tr><th scope="row">'+esc(m.definition)+'</th><td class="number">'+esc(decimal(m.value))+'</td><td>'+esc(m.unit||'Unavailable')+'</td><td><details><summary>'+esc(m.status.replaceAll('_',' '))+'</summary><p>'+esc(key)+'</p><pre>'+esc(JSON.stringify({inputs:m.inputs,exact:m.exact,rounding:m.rounding},null,2))+'</pre></details></td></tr>').join('')+'</tbody></table></div>':'<p>Arithmetic withheld: '+esc(row.problem||row.status)+'</p>')+sourceDetails(row)+'</article>').join('')+
   '<h2>Provider snapshots — separate observation clocks</h2><p>Quotes, reported float and split events are retained separately. Field names and values retain the provider representation; units, security class and snapshot currency are not independently verified. No cash-flow yield, ownership change or historical split adjustment is inferred.</p>'+shard.snapshots.map(row=>'<article class="observation"><h3>'+esc(row.source_rows[0]?.endpoint)+'</h3><p>'+esc(row.status)+' · received '+esc(row.reported?.source_received_at)+'</p>'+snapshotFields(row)+sourceDetails(row)+'<details><summary>Reported snapshot calculation and limitations</summary><pre>'+esc(JSON.stringify(row.measurements,null,2))+'</pre></details></article>').join('');
 }
 async function mount(doc,fetcher,search){
  const status=doc.getElementById('research-status'),panel=doc.getElementById('issuer-record'),form=doc.getElementById('issuer-form');
  try{
   const params=new URLSearchParams(search||''),state=await load(fetcher,params.get('run'));
   status.textContent='Recorded bytes verified · descriptive research';
   doc.getElementById('research-coverage').textContent=state.packet.reported_names+' reported names · '+state.packet.provider_responses+' retained responses · '+state.packet.provider_rows+' original rows · '+state.packet.empty_responses+' empty responses';
   doc.getElementById('research-clock').textContent='Source acquisition '+state.packet.source_acquisition_started_at+' → '+state.packet.source_acquisition_completed_at+'. This is not an atomic market snapshot; statement periods appear below.';
   doc.getElementById('recorded-run').href='?run='+state.runId;
   doc.getElementById('run-json').href='/'+state.reference;
   const input=doc.getElementById('issuer-symbol'),list=doc.getElementById('issuer-options');
   list.innerHTML=state.packet.issuers.map(v=>'<option value="'+esc(v.symbol)+'"></option>').join('');form.hidden=false;
   let sequence=0;
   async function show(){const token=++sequence,symbol=input.value.trim().toUpperCase();panel.textContent='Checking recorded issuer evidence…';try{const r=await record(fetcher,state,symbol);if(token!==sequence)return;panel.innerHTML=renderRecord(r);doc.getElementById('recorded-run').href='?run='+state.runId+'&symbol='+encodeURIComponent(symbol);}catch(e){if(token===sequence)panel.textContent=e.message;}}
   form.addEventListener('submit',e=>{e.preventDefault();show();});
   if(params.get('symbol')){input.value=params.get('symbol');await show();}
  }catch(e){status.textContent='Research unavailable';panel.textContent=e.message+'. No legacy score or forecast is substituted.';form.hidden=true;}
 }
 const api={COMPILERS,FORMULAS,canonical,hash,get,valid,load,fraction,rounded,metric,record,renderRecord,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHCapitalStructureResearch=api;
 if(root.document)root.document.addEventListener('DOMContentLoaded',()=>mount(root.document,root.fetch.bind(root),root.location.search));
})(typeof window==='undefined'?globalThis:window);
