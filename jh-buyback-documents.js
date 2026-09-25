/* Inspect complete SEC document inventories. Literal matches are not signals. */
(function(root){
'use strict';
const FLAGS=['filing_population_complete','authorization_amount_qualified','buyback_execution_qualified',
 'ownership_dilution_qualified','forecast_qualified','sizing_qualified','ticker_identity_verified',
 'calls_eligible','execution_eligible','keyword_search_is_semantic_or_exhaustive'];
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const hash=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const integer=v=>Number.isSafeInteger(v)&&v>=0;
const clock=v=>typeof v==='string'&&/(Z|[+-]\d\d:\d\d)$/.test(v)&&Number.isFinite(Date.parse(v))?Date.parse(v):NaN;
const digest=async bytes=>Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),v=>v.toString(16).padStart(2,'0')).join('');
function authority(value){if(FLAGS.some(k=>value?.[k]!==false))throw Error('Document evidence cannot qualify scanner claims');}
function validate(c,base,now=Date.now()){
 authority(c);
 if(c.contract!=='buyback-submission-document-evidence.v1'||c.all_reported_rows_conserved!==true||c.all_declared_documents_retained!==true)throw Error('Complete document evidence required');
 if(!base||c.manifest_sha256!==base.manifest_sha256||c.inspector_sha256!==base.inspector_sha256||
    c.source_packet?.key!==base.source_packet.key||c.source_packet.sha256!==base.source_packet.sha256||
    c.source_packet.bytes!==base.source_packet.bytes||c.source_packet.reported_as_of!==base.source_packet.reported_as_of)throw Error('Document evidence belongs to a different captured packet');
 if(!Array.isArray(c.rows)||!Array.isArray(c.filings)||c.reported_rows!==base.reported_rows||c.rows.length!==c.reported_rows||
    c.distinct_filings!==c.filings.length||c.distinct_filings!==base.distinct_filings||c.documents!==base.documents||c.original_bytes!==base.original_bytes)throw Error('Complete row/filing population differs');
 if(!(clock(c.capture_started_at)<=clock(c.capture_completed_at)&&clock(c.capture_completed_at)<=now&&clock(c.generated_at)===clock(c.capture_completed_at)))throw Error('Document acquisition clocks differ');
 const ids=new Set(),covered=new Map();let documents=0,bytes=0,words=0;
 for(const f of c.filings){
  authority(f);
  if(!hash(f.filing_id)||ids.has(f.filing_id)||!hash(f.original_sha256)||!hash(f.capture_sha256)||!integer(f.original_bytes)||!f.original_bytes||
    !/^\d{10}$/.test(f.issuer_cik)||!/^\d{10}-\d{2}-\d{6}$/.test(f.accession)||!['8-K','8-K/A'].includes(f.form))throw Error('Exact filing source identity required');
  if(!Array.isArray(f.source_rows)||!f.source_rows.length||!Array.isArray(f.documents)||!f.documents.length||f.all_declared_documents_retained!==true)throw Error('Whole filing document array required');
  const url=`https://www.sec.gov/Archives/edgar/data/${Number(f.issuer_cik)}/${f.accession.replaceAll('-','')}/${f.accession}.txt`;
  if(f.submission_url!==url||!(clock(c.capture_started_at)<=clock(f.requested_at)&&clock(f.requested_at)<=clock(f.received_at)&&clock(f.received_at)<=clock(c.capture_completed_at)))throw Error('Filing URL or source timing differs');
  const names=new Set(),sequences=new Set();let priorEnd=0;
  for(const [i,d] of f.documents.entries()){
   if(d.source_document!==i||typeof d.type!=='string'||!d.type||typeof d.filename!=='string'||!/^[A-Za-z0-9_.-]+$/.test(d.filename)||['.','..'].includes(d.filename)||
      names.has(d.filename)||typeof d.sequence!=='string'||!/^\d+$/.test(d.sequence)||sequences.has(d.sequence)||
      d.document_url!==url.slice(0,url.lastIndexOf('/')+1)+d.filename)throw Error('Unambiguous SEC document identity required');
   for(const [start,end,size,sha] of [['byte_start','byte_end','bytes','sha256'],['text_byte_start','text_byte_end','text_bytes','text_sha256']]){
    if(!integer(d[start])||!integer(d[end])||!integer(d[size])||d[end]<d[start]||d[end]>f.original_bytes||d[end]-d[start]!==d[size]||!hash(d[sha]))throw Error('Document byte range differs');
   }
   if(d.byte_start<priorEnd||d.text_byte_start<d.byte_start||d.text_byte_end>d.byte_end||d.keyword_matches_establish_authorization!==false||
      d.document_url_current_bytes_verified!==false||d.byte_ranges_reference!=='complete_submission_original_bytes'||!Array.isArray(d.literal_keyword_occurrences))throw Error('Embedded document evidence scope differs');
   let previousWord=d.text_byte_start;
   for(const w of d.literal_keyword_occurrences){
    if(!integer(w.byte_start)||!integer(w.byte_end)||w.byte_start<previousWord||w.byte_end>d.text_byte_end||
      typeof w.reported_text!=='string'||!/^(repurchase|buyback)$/i.test(w.reported_text)||w.byte_end-w.byte_start!==w.reported_text.length)throw Error('Literal keyword coordinate differs');
    previousWord=w.byte_end;words++;
   }
   priorEnd=d.byte_end;names.add(d.filename);sequences.add(d.sequence);documents++;
  }
  for(const index of f.source_rows){
   if(!integer(index)||index>=c.rows.length||covered.has(index))throw Error('Source row assigned to multiple filings');
   const original=base.rows[index];
   if(original.issuer_cik!==f.issuer_cik||original.accession!==f.accession||original.original_sha256!==f.original_sha256||original.original_bytes!==f.original_bytes||
      original.documents!==f.documents.length||original.filing_date!==f.filing_date||original.received_at!==f.received_at)throw Error('Filing source differs from accepted row');
   covered.set(index,f.filing_id);
  }
  ids.add(f.filing_id);bytes+=f.original_bytes;
 }
 if(covered.size!==c.rows.length||documents!==c.documents||bytes!==c.original_bytes||words!==c.literal_keyword_occurrences)throw Error('Document population conservation differs');
 for(const [i,row] of c.rows.entries()){
  if(row.source_row!==i||row.filing_id!==covered.get(i)||row.reported_label!==base.rows[i].reported_label||
     row.reported_announcement_date_is_verified_event_date!==false||row.reported_authorization_amount_verified!==false)throw Error('Reported row join or claim qualification differs');
 }
 return c;
}
async function checked(raw,expected,base,now=Date.now()){
 if(!hash(expected)||await digest(raw)!==expected)throw Error('Document catalog bytes differ');
 return validate(JSON.parse(new TextDecoder().decode(raw)),base,now);
}
async function load(fetcher,reference,base,now=Date.now()){
 if(!/^\/assets\/research\/buyback-documents-[0-9]{8}\.json$/.test(reference?.path)||!hash(reference.sha256))throw Error('Reviewed dated document catalog required');
 const response=await fetcher(reference.path,{cache:'no-store',credentials:'omit'});
 if(!response?.ok)throw Error('Document evidence HTTP '+(response?.status??'unavailable'));
 const raw=await response.arrayBuffer();
 if(raw.byteLength>4*1024*1024)throw Error('Whole document catalog exceeds display bound');
 return checked(raw,reference.sha256,base,now);
}
function table(c,row){
 const join=c?.rows?.[row.source_row];
 const f=join&&c.filings.find(f=>f.filing_id===join.filing_id);
 if(!f||join.reported_label!==row.reported_label||f.original_sha256!==row.original_sha256)return '';
 return `<details class="filing-documents"><summary>Inspect all ${f.documents.length} embedded documents</summary><p>All document types are included. Literal word matches are navigation aids, not buyback classifications. Hashes and byte ranges refer to the retained complete submission; the linked SEC document has not been separately byte-verified.</p><ol>${f.documents.map(d=>`<li><a href="${esc(d.document_url)}" target="_blank" rel="noopener noreferrer">${esc(d.filename)}</a><span>${esc(d.type)} · sequence ${esc(d.sequence)} · ${d.text_bytes.toLocaleString('en-US')} text bytes · ${d.literal_keyword_occurrences.length} literal matches</span><details><summary>Inspect document byte evidence</summary><dl><dt>Raw document range · start inclusive, end exclusive</dt><dd class="mono">${d.byte_start}–${d.byte_end}</dd><dt>Raw document SHA-256</dt><dd class="hash">${esc(d.sha256)}</dd><dt>Embedded text range</dt><dd class="mono">${d.text_byte_start}–${d.text_byte_end}</dd><dt>Embedded text SHA-256</dt><dd class="hash">${esc(d.text_sha256)}</dd><dt>Literal word coordinates in the complete submission</dt><dd>${d.literal_keyword_occurrences.length?d.literal_keyword_occurrences.map(w=>`${esc(w.reported_text)} [${w.byte_start}, ${w.byte_end})`).join('; '):'No literal matches. This does not establish the absence of a buyback.'}</dd></dl></details></li>`).join('')}</ol></details>`;
}
const api={FLAGS,validate,checked,load,table};
if(typeof module!=='undefined'&&module.exports)module.exports=api;
else root.JHBuybackDocuments=api;
})(typeof globalThis!=='undefined'?globalThis:this);
