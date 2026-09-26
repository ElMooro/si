/* jh-reskin-skip */
(function(root){
 'use strict';
 const PREFIX='data/ici-research/',LIMIT=8*1024*1024;
 const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
 const NAMES={government:'Government',government_retail:'Government · retail',government_institutional:'Government · institutional',prime:'Prime',prime_retail:'Prime · retail',prime_institutional:'Prime · institutional',tax_exempt:'Tax-exempt',tax_exempt_retail:'Tax-exempt · retail',tax_exempt_institutional:'Tax-exempt · institutional',total:'Total',retail:'Total · retail',institutional:'Total · institutional',equity:'Equity',equity_domestic:'Equity · domestic',equity_world:'Equity · world',hybrid:'Hybrid',bond:'Bond · taxable + municipal',bond_taxable:'Bond · taxable',bond_municipal:'Bond · municipal',commodity:'Commodity'};
 const UNITS={mmf:'USD billions',combined_flows:'USD millions'};
 const URLS={mmf:'https://www.ici.org/research/stats/mmf',combined_flows:'https://www.ici.org/research/stats/combined_flows'};
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>typeof v==='number'&&Number.isFinite(v);
 const sorted=v=>Array.isArray(v)?v.map(sorted):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sorted(v[k])])):v;
 const same=(a,b)=>JSON.stringify(sorted(a))===JSON.stringify(sorted(b));
 const fmt=v=>number(v)?v.toLocaleString('en-US',{maximumFractionDigits:2}):'Unavailable';
 function valid(p){
   if(p?.contract!=='ici-research.v1'||!FLAGS.every(k=>p[k]===false)||p.decision?.verb!=='WAIT'||p.decision?.action!=='abstain'||p.call!==null||p.signal!==null||p.regime!==null||p.portfolio_consequences?.target_weights!==null||p.portfolio_consequences.status!=='UNAVAILABLE'||p.dependency_graph?.independent_votes!==0)throw Error('Research authority contract differs');
   if(p.quality?.status!=='dated_measurements'||p.quality.reconciliation_issues!==0)throw Error('Measurements did not reconcile');
   if(Object.keys(p.sources||{}).sort().join(',')!=='combined_flows,mmf')throw Error('Both official releases required');
   for(const kind of Object.keys(URLS)){
     const s=p.sources[kind],expected=kind==='mmf'?12:9;
     if(s.url!==URLS[kind]||s.unit!==(kind==='mmf'?'usd_bn':'usd_mn')||s.classification_rows!==expected||!Array.isArray(s.observation_dates)||!s.observation_dates.length||!Array.isArray(s.observations)||s.observations.length!==expected*s.observation_dates.length||s.original_as_known_at_history!==false||s.vintage_scope!=='current_retrieved_release')throw Error('Complete dated source classification required');
     if(!/^[a-f0-9]{64}$/.test(s.sha256)||!Number.isInteger(s.bytes)||s.bytes<=0||!/^\d{4}-\d{2}-\d{2}$/.test(s.release_date)||!Number.isFinite(Date.parse(s.acquired_at)))throw Error('Original source identity unavailable');
     if(s.observations.some(r=>!Object.hasOwn(NAMES,r.series)||!s.observation_dates.includes(r.date)||r.unit!==s.unit||(r.value!==null&&!number(r.value))||r.source_cell?.text==null))throw Error('Unexpected measurement');
   }
   return p;
 }
 async function verify(p,transport,fetcher,crypto){
   valid(p);const key=p.replay?.manifest_key;
   if(typeof key!=='string'||!/^data\/ici-research\/runs\/[a-f0-9]{64}\.json$/.test(key))throw Error('Native run identity unavailable');
   const raw=await transport.bytes('/'+key+'?exact=1&nogen=1',fetcher,LIMIT,20000);
   if(key!==PREFIX+'runs/'+await transport.hash(raw,crypto)+'.json')throw Error('Manifest bytes differ');
   const run=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
   async function artifact(ref,kind){
     if(!ref||!(/^[a-f0-9]{64}$/).test(ref.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!Number.isInteger(ref.bytes)||ref.bytes<=0||ref.bytes>LIMIT)throw Error('Artifact reference differs');
     const bytes=await transport.bytes('/'+ref.key+'?exact=1&nogen=1',fetcher,LIMIT,20000);
     if(bytes.length!==ref.bytes||await transport.hash(bytes,crypto)!==ref.sha256)throw Error('Artifact bytes differ');
     return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
   }
   if(run.contract!=='ici-replay.v1'||run.generated_at!==p.generated_at||run.output?.sha256!==p.replay.output_sha256)throw Error('Native run binding differs');
   const [output,proof]=await Promise.all([artifact(run.output,'outputs'),artifact(run.proof,'proofs')]);
   if(!same(output,Object.fromEntries(Object.entries(p).filter(([k])=>k!=='replay')))||!same(proof,p.original_arithmetic_checks)||proof.reconciliation_issues!==0||proof.forecast_qualified!==false)throw Error('Retained publication differs');
   return run;
 }
 function table(headers,rows){return '<table><thead><tr>'+headers.map(h=>'<th scope="col">'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(row=>'<tr>'+row.map(cell=>'<td>'+esc(cell)+'</td>').join('')+'</tr>').join('')+'</tbody></table>';}
 function age(p,now=Date.now()){
   const stamp=Date.parse(p.generated_at);
   if(!Number.isFinite(stamp))return 'Publication date unavailable';
   if(stamp>now)return 'Future publication clock — treat readings as unavailable';
   return 'Published '+p.generated_at+' · '+Math.floor((now-stamp)/3600000)+' hours ago. Generation time does not certify observation freshness.';
 }
 function mount(doc,transport,fetcher=root.fetch,crypto=root.crypto){
   let packet=null,generation=0;const get=id=>doc.getElementById(id);
   function clear(message){packet=null;get('ici-content').hidden=true;get('ici-status').textContent=message;get('ici-raw').textContent='';get('ici-summary').innerHTML='';get('ici-observations').innerHTML='';get('ici-checks').innerHTML='';get('ici-source').innerHTML='';}
   function render(){
     if(!packet)return;
     const kind=get('ici-kind').value==='combined_flows'?'combined_flows':'mmf',s=packet.sources[kind];
     get('ici-status').textContent='WAIT · Research only. '+age(packet);
     get('ici-source').innerHTML='<h2>'+esc(kind==='mmf'?'Money-market assets':'Combined fund flows and ETF net issuance')+'</h2><p><strong>Unit: '+esc(UNITS[kind])+'</strong> · Release '+esc(s.release_date)+' · Observations '+esc(s.observation_dates[0])+' to '+esc(s.observation_dates.at(-1))+'</p><p>Acquired '+esc(s.acquired_at)+' · Current retrieved release; earlier observations may be revised.</p><p><a href="'+URLS[kind]+'" target="_blank" rel="noopener noreferrer">Official ICI release ↗</a></p><details><summary>Complete original identity and arithmetic proof</summary><p>'+esc(s.bytes)+' original bytes · SHA-256 <code>'+esc(s.sha256)+'</code></p><p>The native producer retained the complete HTML privately and independently reconstructed every numeric cell before publishing. Browser checks bind this displayed packet to the retained run and proof; they do not independently certify the publisher or a historical release vintage.</p><p>'+esc(packet.original_arithmetic_checks.observation_checks)+' observations and '+esc(packet.original_arithmetic_checks.independent_rational_reconciliations)+' reconciliations checked across both releases.</p></details>';
     const rows=s.observations.map(r=>[NAMES[r.series],r.date,r.source_cell.text,r.unit,'table '+r.source_cell.table+', row '+r.source_cell.row+', column '+r.source_cell.column]);
     get('ici-observations').innerHTML=table(['Classification','Observation date','Printed value','Unit','Original cell'],rows);
     get('ici-count').textContent=rows.length+' of '+rows.length+' dated observations in this release';
     get('ici-checks').innerHTML=table(['Identity','Date','Residual','Rounding tolerance','Status'],packet.reconciliation[kind].map(r=>[r.identity,r.date,r.residual_decimal??'Unavailable',r.rounding_tolerance_decimal,r.status.replace(/_/g,' ')]));
     const m=packet.mmf,flow=packet.long_term;
     get('ici-summary').innerHTML=[['Total MMF assets',m.total_b,'USD billions · '+m.date],['Weekly asset change',m.wow_b,'USD billions · exact seven-day endpoints'],['Equity flows · four weeks',flow.equity_sum_4w_m,'USD millions · through '+flow.classes.total.date]].map(([title,v,note])=>'<article class="ici-stat"><h2>'+esc(title)+'</h2><strong>'+esc(fmt(v))+'</strong><p>'+esc(note)+'</p></article>').join('');
     get('ici-flow-summary').innerHTML=table(['Classification','Week ending','Latest week (USD mn)','Four weeks (USD mn)','Exact window dates'],Object.entries(flow.classes).map(([name,r])=>[({eq_dom:'Equity · domestic',eq_world:'Equity · world',bond:'Bond · includes municipal',muni:'Municipal · subset of bond',total:'Total long-term'})[name]||name,r.date,fmt(r.latest_w_m),fmt(r.sum_4w_m),r.window_dates.join(', ')]));
   }
   async function refresh(){
     const id=++generation;clear('Verifying the retained publication…');get('ici-refresh').disabled=true;
     try{
       const raw=await transport.bytes('/data/ici-flows.json?exact=1&nogen=1',fetcher,LIMIT,20000);
       const text=new TextDecoder('utf-8',{fatal:true}).decode(raw),p=JSON.parse(text);await verify(p,transport,fetcher,crypto);
       if(id!==generation)return;
       if(!Number.isFinite(Date.parse(p.generated_at))||Date.parse(p.generated_at)>Date.now())throw Error('Future or invalid publication clock');
       packet=p;get('ici-content').hidden=false;get('ici-raw').textContent=text;render();
     }catch(error){if(id===generation)clear('Publication unavailable or unverified. Measurements are withheld. Refresh only reads existing data; it does not run the engine. '+error.message);}
     finally{if(id===generation)get('ici-refresh').disabled=false;}
   }
   get('ici-refresh').addEventListener('click',refresh);get('ici-kind').addEventListener('change',render);
   refresh();return {refresh,render,destroy(){generation++;clear('Closed');}};
 }
 const api={valid,verify,age,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHICIResearch=api;
 if(root.document&&root.document.getElementById('ici-research'))mount(root.document,root.JHFIFXResearch);
})(typeof globalThis!=='undefined'?globalThis:this);
