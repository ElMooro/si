/* On-demand inspection of the complete, hash-bound auction FRED input archive. */
(function(global){
'use strict';
const PREFIX='data/auction-fred-originals/',MAX=48*1024*1024;
const LIMITS={DFF:5,SOFR:5,IORB:5,DTWEXBGS:60,T10Y2Y:5,T5YIFR:5,DGS1MO:90,DGS3MO:90,DGS6MO:90,DGS1:90,DGS2:90,DGS3:90,DGS5:90,DGS7:90,DGS10:90,DGS20:90,DGS30:90};
const SERIES=Object.keys(LIMITS),CROSS=['repo_stress','dollar_strength','curve_slope','inflation_expectations'];
const DENIED=['source_definition_verified','historical_point_in_time_verified','forecast_eligible','calls_eligible','sizing_eligible','execution_eligible'];
const states=new WeakMap();
function object(v){return v!==null&&typeof v==='object'&&!Array.isArray(v);}
function count(v){return Number.isSafeInteger(v)&&v>=0;}
function day(v){if(typeof v!=='string'||!/^\d{4}-\d\d-\d\d$/.test(v)||v.startsWith('0000'))return null;const t=Date.parse(v+'T00:00:00Z');return Number.isFinite(t)&&new Date(t).toISOString().slice(0,10)===v?t:null;}
function number(v){if(!['string','number'].includes(typeof v))return null;const text=String(v).trim();if(text.length>128||!/^[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?$/.test(text))return null;const n=Number(text);return Number.isFinite(n)?n:null;}
function clock(v){
 if(typeof v!=='string'||!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|\+00:00)$/.test(v))return null;
 const t=Date.parse(v);return day(v.slice(0,10))!==null&&Number.isFinite(t)&&new Date(t).toISOString().slice(0,10)===v.slice(0,10)?t:null;
}
function same(a,b){
 if(a===b)return true;
 if(Array.isArray(a)||Array.isArray(b))return Array.isArray(a)&&Array.isArray(b)&&a.length===b.length&&a.every((x,i)=>same(x,b[i]));
 if(!object(a)||!object(b))return false;
 const left=Object.keys(a).sort(),right=Object.keys(b).sort();return same(left,right)&&left.every(k=>same(a[k],b[k]));
}
function permissions(v){return object(v)&&DENIED.every(k=>v[k]===false);}
function reference(ref,kind){
 if(!object(ref)||!same(Object.keys(ref).sort(),['bytes','key','sha256'])||typeof ref.sha256!=='string'||! /^[a-f0-9]{64}$/.test(ref.sha256)||
    ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!Number.isSafeInteger(ref.bytes)||ref.bytes<=0||ref.bytes>MAX)
  throw Error('The FRED archive reference is incomplete or outside its reviewed source namespace.');
 return Object.freeze({...ref});
}
function coverage(v){
 if(!object(v)||!same(v.expected_series,SERIES)||!count(v.raw_rows)||!count(v.selected_benchmark_rows))throw Error('FRED request coverage is incomplete.');
 const groups=['received_series','unavailable_series','not_requested_series'];
 if(groups.some(k=>!Array.isArray(v[k])||v[k].some(s=>!SERIES.includes(s))))throw Error('Unexpected FRED request population.');
 const all=groups.flatMap(k=>v[k]);
 if(all.length!==SERIES.length||new Set(all).size!==SERIES.length||v.complete_request_set!==(v.received_series.length===SERIES.length))
  throw Error('FRED request coverage overlaps or omits a series.');
 return {...v,expected_series:[...v.expected_series],...Object.fromEntries(groups.map(k=>[k,[...v[k]]]))};
}
function measurementCoverage(v){
 if(!object(v)||!object(v.series_status)||!object(v.cross_status)||!same(Object.keys(v.series_status).sort(),[...SERIES].sort())||
    !same(Object.keys(v.cross_status).sort(),[...CROSS].sort())||SERIES.some(s=>!['complete','partial','unavailable'].includes(v.series_status[s]))||
    CROSS.some(s=>!['complete','unavailable'].includes(v.cross_status[s])))throw Error('Measurement availability is not established.');
 return {...v,series_status:{...v.series_status},cross_status:{...v.cross_status}};
}
function specification(packet,publication,now=Date.now()){
 if(!object(packet)||packet.contract!=='auction-fred-originals.v1'||!permissions(packet)||packet.native_input_selection_replayed!==true||packet.whole_auction_model_replayed!==false)
  throw Error('A retained FRED input replay is not available for this publication.');
 const covered=coverage(packet.coverage),available=measurementCoverage(packet.measurement_coverage);
 const expected=covered.complete_request_set?'complete':covered.received_series.length?'partial':'unavailable';
 if(packet.status!==expected||packet.original_bytes_replayed!==(covered.received_series.length>0))throw Error('The source capture status contradicts its coverage.');
 const generated=clock(packet.generated_at),published=clock(publication),asof=day(packet.calculation_as_of);
 if(generated===null||published===null||generated!==published||asof===null||asof>published||published-asof>=2*86400000||published>now+300000||now-published>48*3600000)
  throw Error('The FRED archive does not match a current publication clock.');
 return {generated_at:packet.generated_at,calculation_as_of:packet.calculation_as_of,status:packet.status,
  coverage:covered,measurement_coverage:available,manifest:reference(packet.manifest,'runs'),measurements:reference(packet.measurements,'measurements')};
}
function policyContext(packet,publication,value){
 try{
  const spec=specification(packet,publication),row=packet.fed_funds,observed=day(row?.observation_date),today=day(spec.calculation_as_of);
  if(!spec.coverage.received_series.includes('DFF')||!object(row)||!permissions(row)||row.series_id!=='DFF'||row.unit!=='percent'||
     row.status!=='complete'||row.reason!==null||row.selection!=='latest_reported_date_without_missing_value_fallback'||row.maximum_age_calendar_days!==5||
     !Number.isSafeInteger(row.source_row_index)||row.source_row_index<0||row.source_row_index>=5||observed===null||observed>today||today-observed>5*86400000||
     typeof value!=='number'||!Number.isFinite(value)||row.value!==value)return null;
  return {value,observation_date:row.observation_date};
 }catch{return null;}
}
async function readBounded(response,expected){
 const declared=response.headers?.get?.('content-length');
 if(declared!==null&&declared!==undefined&&/^\d+$/.test(declared)&&Number(declared)>MAX)throw Error('The FRED artifact exceeds the inspection size limit.');
 if(!response.body?.getReader)throw Error('Bounded reading is unavailable in this browser.');
 const reader=response.body.getReader(),chunks=[];let length=0;
 try{while(true){const part=await reader.read();if(part.done)break;length+=part.value.byteLength;
  if(length>expected||length>MAX)throw Error('The artifact length differs from its reference.');chunks.push(part.value);}}
 catch(error){try{await reader.cancel();}catch{}throw error;}finally{reader.releaseLock?.();}
 if(length!==expected)throw Error('The artifact is incomplete.');
 const out=new Uint8Array(length);let offset=0;for(const chunk of chunks){out.set(chunk,offset);offset+=chunk.byteLength;}return out;
}
function publicUrl(sid){return 'https://api.stlouisfed.org/fred/series/observations?series_id='+sid+'&file_type=json&limit='+LIMITS[sid]+'&sort_order=desc';}
function transport(frame,sid){
 const t=frame.adapter_transport,read=clock(frame.adapter_read_at),received=clock(t?.response_received_at),started=clock(t?.request_started_at);
 if(!object(t)||t.contract!=='auction-fred-transport.v1'||t.source_url!==publicUrl(sid)||t.response_status!==200||
    !Number.isSafeInteger(t.body_bytes)||t.body_bytes<=0||t.body_bytes>2*1024*1024||typeof t.body_sha256!=='string'||! /^[a-f0-9]{64}$/.test(t.body_sha256)||
    (t.declared_content_length!==null&&t.declared_content_length!==t.body_bytes)||t.cache_ttl_seconds!==1800||
    !['network','fresh_cache'].includes(t.cache_status)||typeof t.cache_age_seconds!=='number'||!Number.isFinite(t.cache_age_seconds)||t.cache_age_seconds<0||t.cache_age_seconds>=1800||
    started===null||received===null||read===null||started>received||received>read||read-received>=1800000||Math.abs(read-received-1000*t.cache_age_seconds)>5005)
  throw Error('The acquisition record differs for '+sid+'.');
 return t;
}
function validateRows(raw,measured,sid,asof){
 if(raw.length>LIMITS[sid]){
  if(measured.status!=='unavailable'||measured.reason!=='observation_array_or_limit_invalid'||measured.rows.length||measured.excluded_rows.length||Object.keys(measured.selected_history).length)
   throw Error('An oversized response acquired selected rows.');
  return;
 }
 const seen=new Set(),rows=[],excluded=[],history={};let invalid=false;
 for(const [i,row] of raw.entries()){
  const at=object(row)?day(row.date):null,value=object(row)?number(row.value):null;
  const reason=at===null||at>day(asof)||seen.has(at)?'invalid_future_or_duplicate_date':value===null?'missing_or_nonfinite_value':null;
  if(reason==='invalid_future_or_duplicate_date')invalid=true;if(at!==null)seen.add(at);
  rows.push({source_row_index:i,date:at===null?null:row.date,value,exclusion_reason:reason});if(reason)excluded.push(i);
  else history[row.date]=value;
 }
 const status=invalid?'unavailable':raw.length?(excluded.length?'partial':'complete'):'unavailable';
 const reason=invalid?'invalid_future_or_duplicate_date':excluded.length?'missing_rows_excluded':raw.length?null:'no_observations';
 if(!same(rows,measured.rows)||!same(excluded,measured.excluded_rows)||!same(invalid?{}:history,measured.selected_history)||measured.status!==status||measured.reason!==reason)
  throw Error('The complete row selection differs for '+sid+'.');
}
function dataset(data,spec){
 if(!object(data)||data.contract!=='auction-fred-originals.v1'||data.generated_at!==spec.generated_at||data.calculation_as_of!==spec.calculation_as_of||!permissions(data)||
    !same(coverage(data.coverage),spec.coverage)||!same(measurementCoverage(data.measurement_coverage),spec.measurement_coverage)||
    !object(data.source_frames)||!object(data.observations)||!same(Object.keys(data.observations).sort(),[...SERIES].sort()))
  throw Error('The complete FRED artifact contract differs from this publication.');
 const attempted=[...spec.coverage.received_series,...spec.coverage.unavailable_series];
 if(!same(Object.keys(data.source_frames).sort(),attempted.sort()))throw Error('The artifact omits or invents a request frame.');
 let rows=0,selected=0;const summary=[];
 for(const sid of SERIES){
  const source=data.source_frames[sid],measured=data.observations[sid],received=spec.coverage.received_series.includes(sid),missing=spec.coverage.not_requested_series.includes(sid);
  const unit=sid==='DTWEXBGS'?'index_jan_2006_100':sid==='T10Y2Y'?'percentage_points':'percent';
  if(!object(measured)||measured.series_id!==sid||measured.unit!==unit||measured.status!==spec.measurement_coverage.series_status[sid]||
     !Array.isArray(measured.rows)||!Array.isArray(measured.excluded_rows)||!object(measured.selected_history))throw Error('Observation inspection is incomplete for '+sid+'.');
  const kept=Object.keys(measured.selected_history).length;
  if(Object.entries(measured.selected_history).some(([d,v])=>day(d)===null||typeof v!=='number'||!Number.isFinite(v)))throw Error('Invalid selected observation for '+sid+'.');
  if(sid.startsWith('DGS'))selected+=kept;
  if(!missing&&(!object(source)||source.series_id!==sid||source.requested_limit!==LIMITS[sid]||source.http_acquisition_time_verified!==false))throw Error('The request identity differs for '+sid+'.');
  if(!received){
   if((source&&(source.read_status!=='unavailable'||source.response!==null))||kept||measured.rows.length)throw Error('An unavailable source contains selected values.');
   summary.push({series:sid,unit,received:missing?'Not requested':'Unavailable',returned_rows:null,kept_rows:null,observation_range:'Unavailable',
    acquired:'Unavailable',reuse:'Unavailable',row_validation:measured.status});continue;
  }
  if(source.read_status!=='received'||!object(source.response)||!Array.isArray(source.response.observations))throw Error('The original parsed rows are absent for '+sid+'.');
  const t=transport(source,sid);if(clock(source.adapter_read_at)>clock(spec.generated_at))throw Error('A source was read after publication.');
  const raw=source.response.observations;rows+=raw.length;
  validateRows(raw,measured,sid,spec.calculation_as_of);
  if(kept>raw.length||measured.rows.length>raw.length||measured.excluded_rows.some(i=>!Number.isSafeInteger(i)||i<0||i>=raw.length)||new Set(measured.excluded_rows).size!==measured.excluded_rows.length)
   throw Error('The row population differs for '+sid+'.');
  const dates=raw.map(r=>object(r)&&day(r.date)!==null?r.date:null),valid=dates.filter(Boolean).sort();
  const range=valid.length?(valid[0]+' → '+valid[valid.length-1]+(dates.includes(null)?' · invalid dates present':'')):'No valid dated rows';
  summary.push({series:sid,unit,received:'Received',returned_rows:raw.length,kept_rows:kept,observation_range:valid.length&&!dates.includes(null)&&valid[0]===valid[valid.length-1]?valid[0]:range,
   acquired:new Date(clock(t.response_received_at)).toISOString().slice(0,19).replace('T',' ')+' UTC',reuse:t.cache_status==='network'?'Network':'Cache · '+t.cache_age_seconds+' s',row_validation:measured.status});
 }
 if(rows!==spec.coverage.raw_rows||selected!==spec.coverage.selected_benchmark_rows)throw Error('The complete returned or selected row count differs.');
 return summary;
}
async function loadVerified(spec,fetcher,crypto,signal){
 const response=await fetcher('/'+spec.measurements.key+'?exact=1&nogen=1',{signal,credentials:'omit'});
 if(response.status!==200||response.redirected)throw Error('The FRED artifact is unavailable or redirected (HTTP '+response.status+').');
 const bytes=await readBounded(response,spec.measurements.bytes);
 if(!crypto?.subtle)throw Error('Artifact integrity checking is unavailable.');
 const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
 if(hash!==spec.measurements.sha256)throw Error('The artifact hash differs from this publication.');
 const data=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));return {data,summary:dataset(data,spec)};
}
function node(tag,text){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;}
function render(root,packet,publication){
 if(!root)return;states.get(root)?.controller.abort();const state={controller:new AbortController()};states.set(root,state);root.replaceChildren();
 root.append(node('h3','FRED source evidence'));let spec;
 try{spec=specification(packet,publication);}catch(error){root.append(node('p',error.message));return;}
 const c=spec.coverage;root.append(node('p',c.received_series.length+' of '+SERIES.length+' existing requests received · '+c.unavailable_series.length+' unavailable · '+c.not_requested_series.length+' not requested.'));
 root.append(node('p','Capture completeness is separate from measurement availability. Current acquisition records do not establish historical first-release availability, independent evidence or a validated trade.'));
 const controls=node('div');controls.className='jdi-controls';const button=node('button','Inspect FRED sources');button.type='button';
 const manifest=node('a','Replay manifest');manifest.href='/'+spec.manifest.key;manifest.target='_blank';manifest.rel='noopener noreferrer';
 const download=node('a','Complete input JSON');download.href='/'+spec.measurements.key;download.target='_blank';download.rel='noopener noreferrer';
 controls.append(button,manifest,download);root.append(controls);
 const status=node('p','Opening this view checks the complete artifact length, hash, request coverage and recorded acquisition clocks.');status.setAttribute('role','status');
 const body=node('div');root.append(status,body);
 button.onclick=async()=>{
  button.disabled=true;body.replaceChildren();status.textContent='Loading and checking the complete FRED input archive…';
  try{
   const result=await loadVerified(spec,global.fetch.bind(global),global.crypto,state.controller.signal);if(states.get(root)!==state)return;
   body.append(node('p','Scroll the table horizontally for acquisition times and validation status. Keyboard: focus the table, then use the left and right arrow keys.'));
   const wrap=node('div');wrap.className='jaf-table-scroll';wrap.tabIndex=0;wrap.setAttribute('role','region');wrap.setAttribute('aria-label','FRED source coverage table; scroll horizontally for all columns');
   const table=node('table'),caption=node('caption','All seventeen requests · selected row counts are validation results, not independent votes');table.append(caption);
   const labels=['Series','Unit','Response','Kept / returned rows','Observation range','Adapter acquired (UTC)','Transport','Row validation'];
   const thead=node('thead'),header=node('tr');labels.forEach(label=>{const th=node('th',label);th.setAttribute('scope','col');header.append(th);});thead.append(header);table.append(thead);
   const tbody=node('tbody');for(const row of result.summary){const tr=node('tr');
    const labels={percent:'Percent',percentage_points:'Percentage points',index_jan_2006_100:'Index · Jan 2006 = 100'};
    const values=[row.series,labels[row.unit],row.received,row.returned_rows===null?'Unavailable':row.kept_rows+' / '+row.returned_rows,row.observation_range,row.acquired,row.reuse,row.row_validation];
    values.forEach((value,i)=>{const cell=node(i===0?'th':'td',value);if(i===0)cell.setAttribute('scope','row');tr.append(cell);});tbody.append(tr);}
   table.append(tbody);wrap.append(table);body.append(wrap);
   body.append(node('p','Kept rows exclude missing values; duplicate or future dates withhold the series. The complete original parsed responses, exclusions, policy-rate selection and calculation inputs remain inspectable below.'));
   if(!global.JHDataInspector?.inspect)throw Error('The complete field viewer is unavailable; use the JSON and manifest links.');
   const details=node('div');body.append(details);global.JHDataInspector.inspect(details,result.data,'Complete FRED source and calculation inputs');
   status.textContent='Artifact bytes and recorded coverage match this publication. This browser check does not replay the whole auction model or validate forecasting and portfolio use.';
  }catch(error){if(states.get(root)!==state)return;body.replaceChildren();status.textContent='Inspection unavailable: '+error.message;}
  finally{if(states.get(root)===state)button.disabled=false;}
 };
}
const api={LIMITS,SERIES,clock,day,reference,coverage,specification,policyContext,readBounded,dataset,loadVerified,render};
if(typeof module!=='undefined'&&module.exports)module.exports=api;global.JHAuctionFredEvidence=api;
})(typeof globalThis!=='undefined'?globalThis:this);
