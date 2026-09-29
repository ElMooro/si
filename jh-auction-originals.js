/* On-demand inspection of hash-bound, complete Treasury observation artifacts. */
(function(global){
'use strict';
const PREFIX='data/auction-observation-originals/',MAX=32*1024*1024;
const states=new WeakMap();
function clock(value){
  if(typeof value!=='string'||!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|\+00:00)$/.test(value))return null;
  const time=Date.parse(value);
  return Number.isFinite(time)&&new Date(time).toISOString().slice(0,10)===value.slice(0,10)?time:null;
}
function reference(ref,kind){
  if(!ref||typeof ref.sha256!=='string'||! /^[a-f0-9]{64}$/.test(ref.sha256)||
     ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!Number.isSafeInteger(ref.bytes)||ref.bytes<=0||ref.bytes>MAX)
    throw Error('The source reference is incomplete or outside the reviewed auction archive.');
  return Object.freeze({key:ref.key,sha256:ref.sha256,bytes:ref.bytes});
}
function coverage(value){
  if(!value||!Number.isSafeInteger(value.pages)||value.pages<1||value.pages>30||
     !Number.isSafeInteger(value.observations)||value.observations<0||value.observations>value.pages*200||
     value.complete_requested_window!==true||value.current_acquisition_vintage!==true||
     value.historical_publication_vintages_verified!==false||value.provider_snapshot_atomicity_verified!==false)
    throw Error('Complete acquisition coverage is not established.');
  return Object.freeze({...value});
}
function specification(packet,publication,now=Date.now()){
  if(!packet||packet.contract!=='auction-original-replay.v1'||packet.original_bytes_replayed!==true||
     packet.direct_measurements_replayed!==true||packet.historical_point_in_time_verified!==false||
     packet.calls_eligible!==false||packet.forecast_eligible!==false||packet.sizing_eligible!==false)
    throw Error('A complete direct-measurement source replay is not available for this publication.');
  const acquired=clock(packet.generated_at),published=clock(publication);
  if(acquired===null||published===null||acquired>published||published-acquired>15*60000||published>now+300000||now-published>48*3600000)
    throw Error('The source capture does not match a current publication clock.');
  return Object.freeze({generated_at:packet.generated_at,coverage:coverage(packet.coverage),
    measurements:reference(packet.measurements,'measurements'),manifest:reference(packet.manifest,'runs')});
}
async function readBounded(response,expected){
  const declared=response.headers?.get?.('content-length');
  if(declared!==null&&declared!==undefined&&/^\d+$/.test(declared)&&Number(declared)>MAX)throw Error('Source artifact exceeds the inspection size limit.');
  if(!response.body?.getReader)throw Error('Bounded artifact reading is unavailable in this browser.');
  const reader=response.body.getReader(),chunks=[];let length=0;
  try{
    while(true){
      const part=await reader.read();if(part.done)break;
      length+=part.value.byteLength;
      if(length>expected||length>MAX)throw Error('Source artifact length differs from its reference.');
      chunks.push(part.value);
    }
  }catch(error){try{await reader.cancel();}catch{}throw error;}
  finally{reader.releaseLock?.();}
  if(length!==expected)throw Error('Source artifact is incomplete.');
  const bytes=new Uint8Array(length);let offset=0;
  for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
  return bytes;
}
async function loadVerified(spec,fetcher,crypto,signal){
  const ref=spec.measurements;
  const response=await fetcher('/'+ref.key+'?exact=1&nogen=1',{signal,credentials:'omit'});
  if(!response.ok)throw Error('Source artifact unavailable (HTTP '+response.status+').');
  const raw=await readBounded(response,ref.bytes);
  if(!crypto?.subtle)throw Error('Artifact integrity checking is unavailable.');
  const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),b=>b.toString(16).padStart(2,'0')).join('');
  if(hash!==ref.sha256)throw Error('Source artifact hash differs from the publication reference.');
  const data=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
  if(!data||data.contract!=='auction-original-measurements.v1'||data.generated_at!==spec.generated_at||
     !Array.isArray(data.observations)||data.observations.length!==spec.coverage.observations||
     data.calls_eligible!==false||data.forecast_eligible!==false||data.sizing_eligible!==false||data.execution_eligible!==false)
    throw Error('Source measurement contract or complete row count differs.');
  const actual=coverage(data.coverage);
  if(actual.pages!==spec.coverage.pages||actual.observations!==spec.coverage.observations)
    throw Error('Source acquisition coverage differs from the publication.');
  return data;
}
function node(tag,text){const value=document.createElement(tag);if(text!==undefined)value.textContent=text;return value;}
function render(root,packet,publication){
  if(!root)return;
  states.get(root)?.controller.abort();
  const state={controller:new AbortController()};states.set(root,state);root.replaceChildren();
  root.append(node('h3','Treasury source evidence'));
  let spec;
  try{spec=specification(packet,publication);}catch(error){root.append(node('p',error.message));return;}
  const captured=new Date(clock(spec.generated_at)).toISOString().replace('T',' ').replace(/\.\d{3}Z$/,' UTC');
  root.append(node('p',spec.coverage.observations+' observations across '+spec.coverage.pages+' retained '+(spec.coverage.pages===1?'page':'pages')+'. Capture: '+captured+'.'));
  root.append(node('p','The producer replayed the direct auction measurements. The legacy stress score, forecasts, historical availability and portfolio consequences remain unvalidated.'));
  const actions=node('div');actions.className='jdi-controls';
  const button=node('button','Inspect every measurement');button.type='button';
  const manifest=node('a','Replay manifest');manifest.href='/'+spec.manifest.key;manifest.target='_blank';manifest.rel='noopener noreferrer';
  const download=node('a','Complete measurement JSON');download.href='/'+spec.measurements.key;download.target='_blank';download.rel='noopener noreferrer';
  actions.append(button,manifest,download);root.append(actions);
  const status=node('p','Opening the measurement viewer checks the exact artifact length and hash.');status.setAttribute('role','status');
  const body=node('div');root.append(status,body);
  button.onclick=async()=>{
    button.disabled=true;body.replaceChildren();status.textContent='Loading and checking the complete source artifact…';
    try{
      const data=await loadVerified(spec,global.fetch.bind(global),global.crypto,state.controller.signal);
      if(states.get(root)!==state)return;
      if(!global.JHDataInspector?.inspect)throw Error('The complete data viewer is unavailable. The JSON and manifest links remain available.');
      global.JHDataInspector.inspect(body,data,'Complete auction measurements · '+spec.coverage.observations+' observations');
      status.textContent='Artifact bytes match this publication’s reference. Every returned field and row is inspectable below. This browser check is not independent source replay or forecast validation.';
    }catch(error){if(states.get(root)!==state)return;body.replaceChildren();status.textContent='Inspection unavailable: '+error.message;}
    finally{if(states.get(root)===state)button.disabled=false;}
  };
}
const api={clock,reference,coverage,specification,readBounded,loadVerified,render};
if(typeof module!=='undefined'&&module.exports)module.exports=api;
global.JHAuctionOriginals=api;
})(typeof globalThis!=='undefined'?globalThis:this);
