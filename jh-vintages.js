(function(root){
'use strict';
const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function date(value){if(!/^\d{4}-\d{2}-\d{2}$/.test(value)||new Date(value+'T00:00:00Z').toISOString().slice(0,10)!==value)throw Error('Valid calendar date required');return value;}
function path(key){return typeof key==='string'&&/^data\/vintage-research\/[A-Za-z0-9_./-]+$/.test(key)&&!key.includes('..')?'/'+key:null;}
const link=(key,label)=>path(key)?`<a href="${esc(path(key))}" target="_blank" rel="noopener">${esc(label)}</a>`:'Original evidence unavailable';
async function sha(raw){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),n=>n.toString(16).padStart(2,'0')).join('');}
function canonical(value){return JSON.stringify(value,(key,v)=>v&&typeof v==='object'&&!Array.isArray(v)?Object.fromEntries(Object.keys(v).sort().map(k=>[k,v[k]])):v);}
async function bounded(stream,limit=64*1024*1024){
 const reader=stream.getReader(),chunks=[];let length=0;
 try{for(;;){const item=await reader.read();if(item.done)break;length+=item.value.byteLength;if(length>limit){await reader.cancel();throw Error('Archive exceeds bound');}chunks.push(item.value);}}
 finally{reader.releaseLock();}
 const joined=new Uint8Array(length);let offset=0;for(const chunk of chunks){joined.set(chunk,offset);offset+=chunk.byteLength;}return joined;
}
async function bytes(key,fetcher=fetch){const url=path(key);if(!url)throw Error('Unsafe archive reference');const response=await fetcher(url,{cache:'no-store'});if(!response.ok)throw Error('Archive HTTP '+response.status);return bounded(response.body);}
async function output(entry,fetcher){
 if(entry.key!=='data/vintage-research/outputs/'+entry.sha256+'.json'||!/^[a-f0-9]{64}$/.test(entry.sha256))throw Error('Output identity differs');
 const raw=await bytes(entry.key,fetcher);if(await sha(raw)!==entry.sha256)throw Error('Archived output hash differs');
 return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
}
async function loadPacket(index,sid,fetcher=fetch){
 if(index?.contract!=='fred-vintage-index.v1')throw Error('Original archive index unavailable');
 const entry=index.detail?.[sid];if(entry?.status!=='source_replayed')throw Error(entry?.error||'Series unavailable in this collection');
 const doc=await output(entry,fetcher);
 if(!['fred-vintage-periods.v1','fred-vintage-segments.v1'].includes(doc.contract)||doc.series!==sid||doc.collection_id!==index.collection_id||doc.coverage?.status!=='complete_requested_windows')throw Error('Archive contract or collection differs');
 return doc;
}
async function resolve(doc,archiveDay,fetcher=fetch){
 date(archiveDay);if(doc.contract==='fred-vintage-periods.v1')return doc;
 if(doc.contract!=='fred-vintage-segments.v1')throw Error('Original archive contract required');
 let cursor=doc.coverage.archive_start;
 for(const entry of doc.segments){if(entry.coverage.archive_start!==cursor||entry.coverage.archive_end<cursor)throw Error('Archive segment gap or overlap');const next=new Date(date(entry.coverage.archive_end)+'T00:00:00Z');next.setUTCDate(next.getUTCDate()+1);cursor=next.toISOString().slice(0,10);}
 const next=new Date(date(doc.coverage.archive_end)+'T00:00:00Z');next.setUTCDate(next.getUTCDate()+1);if(cursor!==next.toISOString().slice(0,10))throw Error('Incomplete archive coverage');
 const matches=doc.segments.filter(s=>s.coverage.archive_start<=archiveDay&&s.coverage.archive_end>=archiveDay);
 if(matches.length!==1)throw Error('Selected date outside retained archive');
 const entry=matches[0],packet=await output(entry,fetcher);
 if(packet.contract!=='fred-vintage-periods.v1'||packet.series!==doc.series||packet.coverage.scope!=='archive_segment'||canonical(packet.coverage)!==canonical(entry.coverage))throw Error('Archive segment identity differs');
 for(const field of ['replay','generated_at','acquired_at','n_vintages'])if(canonical(packet[field])!==canonical(entry[field]))throw Error('Segment descriptor differs');
 return packet;
}
function select(doc,archiveDay){
 date(archiveDay);if(doc.contract!=='fred-vintage-periods.v1')throw Error('Original archive required');
 if(archiveDay<doc.coverage.archive_start||archiveDay>doc.coverage.archive_end)return {status:'outside_retained_archive'};
 const rows=doc.vintages.map((row,index)=>({row,index})).filter(x=>x.row.date<=archiveDay&&x.row.valid_from<=archiveDay&&x.row.valid_through>=archiveDay);
 if(!rows.length)return {status:'unavailable'};
 const latest=rows.reduce((d,x)=>x.row.date>d?x.row.date:d,'');const chosen=rows.filter(x=>x.row.date===latest);
 if(chosen.length!==1)throw Error('Ambiguous original observation period');
 const defs=doc.definitions.map((row,index)=>({row,index})).filter(x=>x.row.realtime_start<=archiveDay&&x.row.realtime_end>=archiveDay);
 if(defs.length!==1)return {status:'historical_definition_unavailable'};
 return {status:chosen[0].row.value_decimal==null?'missing':'archived_value',...chosen[0],definition:defs[0].row,definitionIndex:defs[0].index};
}
function render(doc,archiveDay){
 const selected=select(doc,archiveDay),coverage=doc.coverage;
 let html=`<p class="muted">Provider archive range ${esc(coverage.archive_start)} through ${esc(coverage.archive_end)} · ${esc(coverage.observations)} observation dates · ${esc(coverage.missing_periods)} missing-value intervals · ${esc(coverage.pages)} original pages.${coverage.scope==='archive_segment'?' This is the selected segment; other dates load their matching segment.':''}</p>`;
 if(!selected.row)return html+`<p role="status">${esc(selected.status.replaceAll('_',' '))}. No substitute date or value is used.</p>`;
 const row=selected.row,meta=selected.definition,original=doc.observation_sources[row.source_page];
 const usable=new Date(archiveDay+'T12:00:00Z');usable.setUTCDate(usable.getUTCDate()+1);
 html+=`<div class="metrics"><article><span>Archived value</span><strong>${row.value_decimal==null?'Not reported':esc(row.value_decimal)}</strong><p>${esc(meta.units)}</p></article><article><span>Observation date</span><strong>${esc(row.date)}</strong><p>${esc(meta.frequency||meta.frequency_short)} · ${esc(meta.seasonal_adjustment)}</p></article><article><span>Provider archive day</span><strong>${esc(archiveDay)}</strong><p>Research policy usable from ${esc(usable.toISOString())}</p></article></div>`;
 html+=`<h2>${esc(meta.title)}</h2><p>The provider reports this value as valid from ${esc(row.valid_from)} through ${esc(row.valid_through)} within the requested archive window. ${row.start_left_censored?'The start is clipped by the query window; the initial release date is unknown.':'The interval starts at a provider archive change date; an intraday release time is not established.'}</p>`;
 html+=`<p>Original observations retrieved ${esc(original.acquired_at)} · definition retrieved ${esc(doc.definition_source.acquired_at)} · archive built ${esc(doc.generated_at)}.</p><p>${link(original.evidence.key,'Original observations (.gz)')} · ${link(doc.definition_source.evidence.key,'Historical definitions (.gz)')} · ${link(doc.replay?.manifest_key,'Replay manifest')}</p><button type="button" id="vintage-verify">Verify original value and definition</button><p id="vintage-verified" role="status"></p>`;
 const revisions=doc.vintages.filter(x=>x.date===row.date);
 html+=`<details><summary>Intervals for observation ${esc(row.date)} in this ${coverage.scope==='archive_segment'?'segment':'archive'} (${revisions.length})</summary><div class="scroll"><table><thead><tr><th>From</th><th>Through</th><th>Native value</th><th>Unit at interval start</th><th>Start boundary</th></tr></thead><tbody>${revisions.map(x=>{const def=doc.definitions.filter(d=>d.realtime_start<=x.valid_from&&d.realtime_end>=x.valid_from);return `<tr><td>${esc(x.valid_from)}</td><td>${esc(x.valid_through)}</td><td>${esc(x.value_decimal??'Not reported')}</td><td>${esc(def.length===1?def[0].units:'Unknown')}</td><td>${x.start_left_censored?'Query-clipped':'Provider archive change'}</td></tr>`;}).join('')}</tbody></table></div><p>Adjacent equal values may be separate source windows. They are not counted as new economic releases. Values across unit changes require matching dated definitions before comparison.</p></details>`;
 html+=`<details><summary>Historical definitions and unit changes (${doc.definitions.length})</summary>${doc.definitions.map(d=>`<article><h3>${esc(d.realtime_start)} — ${esc(d.realtime_end)}</h3><p>${esc(d.title)}</p><p>${esc(d.units)} · ${esc(d.frequency||d.frequency_short)} · ${esc(d.seasonal_adjustment)}</p></article>`).join('')}</details>`;
 return html;
}
async function original(ref,fetcher){
 const requestHash=await sha(new TextEncoder().encode(canonical(ref.request)));
 if(ref.evidence.request_sha256!==requestHash||ref.evidence.key!==`data/vintage-research/originals/${requestHash}/${ref.evidence.sha256}.json.gz`)throw Error('Original request identity differs');
 const zipped=await bytes(ref.evidence.key,fetcher);
 const raw=await bounded(new Blob([zipped]).stream().pipeThrough(new DecompressionStream('gzip')));
 if(raw.byteLength!==ref.evidence.bytes||await sha(raw)!==ref.evidence.sha256)throw Error('Original response hash or length differs');
 return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
}
async function verify(doc,archiveDay,fetcher=fetch){
 const selected=select(doc,archiveDay);if(!selected.row)throw Error('No original row selected');
 const row=selected.row,ref=doc.observation_sources[row.source_page];
 for(const [source,endpoint] of [[ref,'series/observations'],[doc.definition_source,'series']])if(source.request?.endpoint!==endpoint||source.request?.params?.series_id!==doc.series)throw Error('Original series request differs');
 const query=ref.request.params,metaQuery=doc.definition_source.request.params;
 if(query.units!=='lin'||query.output_type!==1||query.sort_order!=='asc'||query.observation_start!=='1776-07-04'||query.observation_end!=='9999-12-31'||'frequency' in query||'aggregation_method' in query)throw Error('Native archive request required');
 if(metaQuery.realtime_start!=='1776-07-04'||metaQuery.realtime_end!=='9999-12-31')throw Error('Complete historical definition request required');
 const observations=await original(ref,fetcher),definitions=await original(doc.definition_source,fetcher);
 const actual=observations.observations?.[row.source_row],meta=definitions.seriess?.[selected.definitionIndex];
 if(!actual||actual.date!==row.date||actual.realtime_start!==row.valid_from||actual.realtime_end!==row.valid_through||actual.value!==(row.value_decimal??'.'))throw Error('Original observation row differs');
 if(!meta||meta.id!==doc.series||canonical(meta)!==canonical(selected.definition)||meta.realtime_start>archiveDay||meta.realtime_end<archiveDay)throw Error('Original historical definition differs');
 return `Verified both original response hashes, observation row ${row.source_row}, and definition row ${selected.definitionIndex}. No first-release or intraday-availability claim.`;
}
const api={date,path,sha,canonical,bounded,loadPacket,resolve,select,render,verify};
if(typeof module==='object'&&module.exports)module.exports=api;else root.JHVintages=api;
})(typeof globalThis==='object'?globalThis:this);
