/* Dated, hash-pinned filing evidence. No authorization or return inference. */
(function(root){
'use strict';
const SNAPSHOT='/assets/research/buyback-filings-20260925.json';
const SHA='8567669d482e707a92e560b8a9c095ca9a55c22f502a2d3c2212f35cd308aae7';
const CURRENT='/data/buyback-scanner.json?exact=1&nogen=1';
const FLAGS=['filing_population_complete','authorization_amount_qualified','buyback_execution_qualified','ticker_identity_verified',
 'ownership_dilution_qualified','forecast_qualified','sizing_qualified','calls_eligible','execution_eligible'];
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const hex=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const digest=async raw=>Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');
const clock=v=>typeof v==='string'&&/(Z|[+-]\d\d:\d\d)$/.test(v)&&Number.isFinite(Date.parse(v))?Date.parse(v):NaN;
function validate(c,now){
 if(!c||c.contract!=='buyback-reported-filing-evidence.v1'||FLAGS.some(k=>c[k]!==false)||c.all_reported_rows_conserved!==true||c.all_declared_documents_retained!==true)throw Error('Filing evidence contract differs');
 if(!Array.isArray(c.rows)||c.rows.length!==c.reported_rows||c.distinct_filings!==c.reported_rows)throw Error('Complete dated candidate set required');
 if(!Number.isFinite(now)||!(clock(c.capture_started_at)<=clock(c.capture_completed_at)&&clock(c.capture_completed_at)<=now))throw Error('Capture timing is invalid or in the future');
 if(c.source_packet?.key!=='data/buyback-scanner.json'||!hex(c.source_packet.sha256)||!Number.isInteger(c.source_packet.bytes)||c.source_packet.bytes<=0||!Number.isFinite(clock(c.source_packet.reported_as_of)))throw Error('Exact source packet identity required');
 if(!hex(c.manifest_sha256)||!hex(c.inspector_sha256)||!/^https:\/\/github\.com\/ElMooro\/si\/blob\/[a-f0-9]{40}\/aws\/ops\/reports\/latest\/ops_6109_buyback_reported_filing_capture\.md$/.test(c.acceptance_report_url))throw Error('Immutable acceptance evidence required');
 const seen=new Set();let documents=0,bytes=0;
 for(const [index,row] of c.rows.entries()){
  if(row.source_row!==index||typeof row.reported_label!=='string'||!/^\d{10}$/.test(row.issuer_cik)||!/^\d{10}-\d{2}-\d{6}$/.test(row.accession)||!hex(row.original_sha256))throw Error('Reported filing coordinates differ');
  const expected=`https://www.sec.gov/Archives/edgar/data/${Number(row.issuer_cik)}/${row.accession.replaceAll('-','')}/${row.accession}.txt`;
  const id=row.issuer_cik+':'+row.accession;
  if(row.submission_url!==expected||seen.has(id)||!Number.isInteger(row.documents)||row.documents<=0||!Number.isInteger(row.original_bytes)||row.original_bytes<=0)throw Error('Complete filing identity differs');
  if(!/^\d{4}-\d{2}-\d{2}$/.test(row.filing_date)||new Date(row.filing_date+'T00:00:00Z').toISOString().slice(0,10)!==row.filing_date||!(clock(c.capture_started_at)<=clock(row.received_at)&&clock(row.received_at)<=clock(c.capture_completed_at)))throw Error('Filing or acquisition date differs');
  seen.add(id);documents+=row.documents;bytes+=row.original_bytes;
 }
 if(documents!==c.documents||bytes!==c.original_bytes)throw Error('Complete document/byte counts differ');
 return c;
}
async function checked(raw,now){if(await digest(raw)!==SHA)throw Error('Dated evidence bytes differ');return validate(JSON.parse(new TextDecoder().decode(raw)),now);}
async function body(response){if(!response?.ok)throw Error('HTTP '+(response?.status??'unavailable'));const raw=await response.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Whole artifact exceeds display bound');return raw;}
async function load(fetcher,now=Date.now()){
 const replies=await Promise.allSettled([fetcher(SNAPSHOT,{cache:'no-store',credentials:'omit'}),fetcher(CURRENT,{cache:'no-store',credentials:'omit'})]);
 const state={catalog:null,current:null,current_sha256:null,errors:[],current_matches_capture:false};
 if(replies[0].status==='fulfilled')try{state.catalog=await checked(await body(replies[0].value),now);}catch(e){state.errors.push('Filing evidence unavailable: '+e.message);}else state.errors.push('Filing evidence request unavailable');
 if(replies[1].status==='fulfilled')try{
  const raw=await body(replies[1].value),current=JSON.parse(new TextDecoder().decode(raw));
  if(!current||typeof current!=='object'||Array.isArray(current)||!Array.isArray(current.top_opportunities)||current.top_opportunities.some(r=>!r||typeof r!=='object'||Array.isArray(r)))throw Error('Complete scanner row array unavailable');
  state.current=current;state.current_sha256=await digest(raw);
  state.current_matches_capture=!!state.catalog&&state.current_sha256===state.catalog.source_packet.sha256&&raw.byteLength===state.catalog.source_packet.bytes;
 }catch(e){state.errors.push('Current scanner packet unavailable: '+e.message);}else state.errors.push('Current scanner packet request unavailable');
 return state;
}
function selected(c,query=''){const q=query.trim().toLowerCase();return c.rows.filter(r=>[r.reported_label,r.reported_company,r.accession,r.issuer_cik].some(v=>String(v??'').toLowerCase().includes(q))).sort((a,b)=>a.reported_label.localeCompare(b.reported_label)||a.source_row-b.source_row);}
function table(c,query=''){
 const rows=selected(c,query);
 return `<p class="muted">${rows.length} of ${c.reported_rows} captured rows shown · alphabetical, not ranked</p><div class="table-scroll"><table><thead><tr><th>Reported label / company</th><th>SEC filing date</th><th>Submission</th><th>Source evidence</th></tr></thead><tbody>${rows.map(r=>`<tr><td><strong>${esc(r.reported_label)}</strong><span>${esc(r.reported_company)}</span></td><td>${esc(r.filing_date)}<span>Filing date; event date unverified</span></td><td><a href="${esc(r.submission_url)}" target="_blank" rel="noopener noreferrer">Open complete SEC submission</a><span class="mono">${esc(r.accession)}</span><span>${r.documents} documents · ${r.original_bytes.toLocaleString('en-US')} bytes</span></td><td><details><summary>Inspect provenance</summary><dl><dt>Issuer CIK from filing</dt><dd class="mono">${esc(r.issuer_cik)}</dd><dt>Captured at</dt><dd>${esc(r.received_at)}</dd><dt>Complete submission SHA-256</dt><dd class="hash">${esc(r.original_sha256)}</dd><dt>Original scanner row</dt><dd>/top_opportunities/${r.source_row}</dd><dt>Scanner authorization_usd · unverified</dt><dd>${esc(JSON.stringify(r.reported_authorization_usd))}</dd></dl><p>The source file and its document boundaries were verified. Authorization meaning, amount, issuer-to-ticker identity and execution have not been qualified.</p></details></td></tr>`).join('')}</tbody></table></div>${rows.length?'':'<p>No captured filings match this filter. The captured population remains unchanged.</p>'}`;
}
function currentStatus(state,now){
 if(!state.current)return 'Current scanner packet unavailable; dated filing evidence remains separate.';
 const stamp=clock(state.current.as_of),valid=Number.isFinite(stamp)&&stamp<=now;
 return `${state.current.top_opportunities.length} current reported rows · packet as_of ${valid?state.current.as_of:'missing, invalid or future'}${valid?' · '+Math.floor((now-stamp)/86400000)+' days old':''}. `+
  (state.current_matches_capture?'Exact packet matches the source used for this filing capture.':'Current packet differs from the captured source. New or changed claims are not joined to this dated evidence.');
}
function render(state,now=Date.now()){
 const c=state.catalog;
 return `<section class="notice"><h2>Research evidence · no qualified trade signal</h2><p>Retaining a filing does not validate a repurchase amount, completed execution, expected return or position size. Prior forecast and trade-ticket fields remain available in the complete packet inspector below.</p></section>${state.errors.map(v=>`<p role="status" class="notice">${esc(v)}</p>`).join('')}
 <section class="panel"><h2>Current scanner packet</h2><p>${esc(currentStatus(state,now))}</p><p class="muted">The scanner's observed schedule is Monday at 12:00 UTC. A capture timestamp describes evidence retrieval, not a new company announcement.</p></section>
 ${c?`<section class="panel"><div class="section-head"><div><p class="eyebrow">Dated evidence · 25 September 2026</p><h2>Reported filings, preserved in full</h2></div><a href="${esc(c.acceptance_report_url)}" target="_blank" rel="noopener noreferrer">Runner acceptance report ↗</a></div><div class="stats"><div><strong>${c.reported_rows}</strong><span>reported candidate rows</span></div><div><strong>${c.distinct_filings}</strong><span>complete submissions</span></div><div><strong>${c.documents}</strong><span>embedded documents</span></div><div><strong>${(c.original_bytes/1000000).toFixed(2)} MB</strong><span>retained original bytes</span></div></div><p>Captured ${esc(c.capture_started_at)} to ${esc(c.capture_completed_at)}. The source scanner packet was dated ${esc(c.source_packet.reported_as_of)}. This covers its reported candidates; it is not a complete market census.</p><label class="search">Find a reported label, company, CIK or accession<input id="filing-search" type="search" autocomplete="off" placeholder="For example: DHI or 0000882184"></label><div id="filing-table">${table(c)}</div></section>`:'<section class="panel"><h2>Filing evidence unavailable</h2><p>No source-verification badge or default forecast is substituted.</p></section>'}
 <section class="panel"><h2>How to use this evidence</h2><ol><li>Open the complete submission and review the primary filing and exhibits.</li><li>Separate a new authorization, remaining capacity and actual purchases. An amount near a keyword is insufficient evidence.</li><li>For reported cash flows and share counts, inspect <a href="/statement-research.html">statement research</a> and <a href="/share-flows.html">capital-structure research</a>. Reporting periods, currencies and share definitions must agree before comparison.</li></ol><p>Portfolio action: WAIT / abstain from this scanner. No validated return forecast or sizing authority is available.</p></section><section class="panel" id="filing-inspection"></section>`;
}
async function mount(document,fetcher){
 const target=document.getElementById('root');if(!target)return;
 const state=await load(fetcher);target.innerHTML=render(state);
 const search=document.getElementById('filing-search');if(search)search.addEventListener('input',()=>{document.getElementById('filing-table').innerHTML=table(state.catalog,search.value);});
 const records=[];if(state.catalog)records.push({label:'Complete verified filing-capture catalog',source:SNAPSHOT,document:state.catalog});
 if(state.current)records.push({label:'Complete current scanner packet · legacy claims unqualified',source:CURRENT,document:state.current});
 const inspector=document.getElementById('filing-inspection');
 if(root.JHResearchInspection)root.JHResearchInspection.show(inspector,records);
 else{inspector.textContent='Complete data viewer unavailable. Reload to inspect every returned field.';}
}
const api={SNAPSHOT,SHA,CURRENT,FLAGS,checked,validate,load,selected,table,currentStatus,render,mount};
if(typeof module!=='undefined'&&module.exports)module.exports=api;
else{root.JHBuybackFilings=api;root.document.addEventListener('DOMContentLoaded',()=>mount(root.document,root.fetch.bind(root)).catch(()=>{root.document.getElementById('root').textContent='Filing viewer unavailable. Reload this page.';}),{once:true});}
})(typeof globalThis!=='undefined'?globalThis:this);
