/* Complete returned research reports and source-listed daily archive copies. */
(function(global){
'use strict';
const SOURCES=new Set(['data/morning-intel.json','data/macro-surprise.json','data/yield-curve.json','data/correlation-surface.json','data/historical-analogs.json','data/event-study.json','data/ab-test-results.json','portfolio/signal-portfolio-state.json','data/13f-positions.json','data/short-interest.json','data/earnings-tracker.json','data/best-setups.json','data/opportunities.json','data/signal-board.json']);
function dateValid(day){return typeof day==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(day)&&new Date(day+'T00:00:00Z').toISOString().slice(0,10)===day;}
function archiveRows(index){
 if(!index||index.schema_version!=='daily-snapshot-index.v1'||index.complete!==true||!Array.isArray(index.snapshots))throw new Error('A complete source-generated snapshot index is not available.');
 const rows=[],seen=new Set();
 for(const row of index.snapshots){
  if(!row||!SOURCES.has(row.source_key)||!dateValid(row.capture_date))throw new Error('Snapshot index contains an invalid source/date.');
  const expected='data/snapshots/'+row.source_key.replaceAll('/','_').replace('.json','')+'-'+row.capture_date+'.json';
  if(row.key!==expected||row.immutable!==false||row.point_in_time_certified!==false)throw new Error('Snapshot path or provenance differs from its reviewed daily-copy contract.');
  if(seen.has(row.key))continue;seen.add(row.key);rows.push(row);
 }
 return rows.sort((a,b)=>a.capture_date.localeCompare(b.capture_date)||a.source_key.localeCompare(b.source_key));
}
function report(ticker,doc){
 if(!doc||typeof doc!=='object'||Array.isArray(doc)||doc.error||String(doc.ticker||'').toUpperCase()!==ticker)throw new Error('Research response does not match '+ticker+'.');
 return doc;
}
function freshReport(doc,now){const ts=Date.parse(doc&&doc.generated_at),age=now-ts;return Number.isFinite(ts)&&age>=0&&age<48*3600*1000;}
function show(container,records){
 container.replaceChildren();
 const heading=document.createElement('h2');heading.textContent='Complete returned data';container.append(heading);
 const note=document.createElement('p');note.textContent='Expand a report to inspect every returned field and row, including missing values and source metadata.';container.append(note);
 for(const record of records){
  const details=document.createElement('details'),summary=document.createElement('summary'),body=document.createElement('div');
  summary.textContent=record.label;details.append(summary,body);container.append(details);
  let loaded=false;details.addEventListener('toggle',()=>{if(!details.open||loaded)return;loaded=true;
   if(global.JHDataInspector)global.JHDataInspector.inspect(body,{source:record.source,document:record.document,error:record.error||null},record.label);
   else {const message=document.createElement('p');message.textContent='The full data viewer is unavailable. Refresh this page.';body.append(message);}
  });
 }
 if(!records.length){const empty=document.createElement('p');empty.textContent='No reports returned for this selection.';container.append(empty);}
}
const api={archiveRows,report,freshReport,show};if(typeof module!=='undefined'&&module.exports)module.exports=api;global.JHResearchInspection=api;
})(typeof window!=='undefined'?window:globalThis);
