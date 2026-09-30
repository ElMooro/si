/* Complete, unambiguous scenario replay. Uses the reviewed arithmetic model only. */
(function(root){
 'use strict';
 const EXPORT='portfolio-scenario-export.v1',LIMIT=4*1024*1024;
 const evidence=typeof module==='object'&&module.exports?require('./jh-evidence-io.js'):root.JHEvidenceIO;
 if(!evidence)throw Error('Complete evidence I/O unavailable');
 const strictJSON=evidence.strictJSON;
 function decode(raw,limit=LIMIT){
  if(!Number.isSafeInteger(limit)||limit<1||limit>LIMIT)throw Error('Invalid scenario byte bound');
  return evidence.decode(raw,limit);
 }
 function keys(v,names,label){if(!v||typeof v!=='object'||Array.isArray(v)||Object.keys(v).length!==names.length||!names.every(k=>Object.hasOwn(v,k)))throw Error(label+': missing or unexpected fields');}
 function stable(v,depth=0){
  if(depth>128)throw Error('Scenario nesting exceeds bound');
  if(v===null||typeof v==='string'||typeof v==='boolean')return v;
  if(typeof v==='number'&&Number.isFinite(v))return v;
  if(Array.isArray(v))return v.map(x=>stable(x,depth+1));
  if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k],depth+1)]));
  throw Error('Scenario contains a non-JSON value');
 }
 function same(a,b){return JSON.stringify(stable(a))===JSON.stringify(stable(b));}
 function identity(value,model){
  keys(value,['contract','sha256','bytes'],'Model identity');
  if(value.contract!==model.CONTRACT||typeof value.sha256!=='string'||! /^[a-f0-9]{64}$/.test(value.sha256)||!Number.isSafeInteger(value.bytes)||value.bytes<1||value.bytes>LIMIT)throw Error('Reviewed model identity is invalid');
  return value;
 }
 function verify(packet,expected,model){
  identity(expected,model);keys(packet,['contract','model','input','output'],'Scenario export');
  if(packet.contract!==EXPORT)throw Error('Unknown scenario export contract');
  identity(packet.model,model);
  if(!same(packet.model,expected))throw Error('Export model differs from this reviewed version. Use the matching reviewed repository version for replay.');
  const output=model.calculate(packet.input);
  if(!same(output,packet.output))throw Error('Exported results differ from complete deterministic replay');
  return output;
 }
 async function readComplete(open,options={}){
  const limit=options.limit??LIMIT;
  if(!Number.isSafeInteger(limit)||limit<1||limit>LIMIT)throw Error('Invalid scenario byte bound');
  return evidence.readComplete(open,{...options,limit});
 }
 async function readFile(file,options={}){
  if(!file||!Number.isSafeInteger(file.size)||file.size<0||file.size>LIMIT||typeof file.stream!=='function')throw Error('Export exceeds the 4 MB bound or is not a readable file');
  return decode(await readComplete(()=>({ok:true,body:file.stream()}),{...options,limit:LIMIT,expectedBytes:file.size}));
 }
 const api={EXPORT,LIMIT,strictJSON,decode,identity,same,verify,readComplete,readFile};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.JHScenarioIO=api;
})(typeof globalThis==='object'?globalThis:this);
