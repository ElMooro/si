/* Complete, unambiguous scenario replay. Uses the reviewed arithmetic model only. */
(function(root){
 'use strict';
 const EXPORT='portfolio-scenario-export.v1',LIMIT=4*1024*1024;
 function strictJSON(source){
  // Reject duplicate identities and overflow rather than accepting the last key.
  if(typeof source!=='string')throw Error('JSON text required');
  let i=0;const number=/-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function ws(){while(/[\x20\t\r\n]/.test(source[i]||'x'))i++;}
  function string(){const start=i++;for(;i<source.length;i++){if(source[i]==='\\'){i++;continue;}if(source[i]==='"'){const text=JSON.parse(source.slice(start,++i));for(let n=0;n<text.length;n++){const c=text.charCodeAt(n);if(c>=0xD800&&c<=0xDBFF){const next=text.charCodeAt(++n);if(!(next>=0xDC00&&next<=0xDFFF))throw Error('Invalid JSON Unicode');}else if(c>=0xDC00&&c<=0xDFFF)throw Error('Invalid JSON Unicode');}return text;}}throw Error('Incomplete JSON string');}
  function value(depth){
   if(depth>128)throw Error('JSON nesting exceeds bound');ws();const c=source[i];
   if(c==='"')return string();
   if(c==='{'||c==='['){const object=c==='{',out=object?{}:[],seen=new Set(),end=object?'}':']';i++;ws();if(source[i]===end){i++;return out;}
    for(;;){ws();let key;if(object){if(source[i]!=='"')throw Error('JSON key required');key=string();if(seen.has(key))throw Error('Duplicate JSON key');seen.add(key);ws();if(source[i++]!==':')throw Error('JSON colon required');}
     const item=value(depth+1);if(object)Object.defineProperty(out,key,{value:item,enumerable:true,writable:true,configurable:true});else out.push(item);
     ws();if(source[i]===end){i++;return out;}if(source[i++]!==',')throw Error('Incomplete JSON structure');}
   }
   for(const [token,v]of [['true',true],['false',false],['null',null]])if(source.startsWith(token,i)){i+=token.length;return v;}
   number.lastIndex=i;const m=number.exec(source);if(!m)throw Error('Invalid JSON value');i=number.lastIndex;const n=Number(m[0]);if(!Number.isFinite(n))throw Error('Nonfinite JSON number');return n;
  }
  const out=value(0);ws();if(i!==source.length)throw Error('Trailing JSON content');return out;
 }
 function decode(raw,limit=LIMIT){
  if(!(raw instanceof Uint8Array)||!Number.isSafeInteger(limit)||limit<1||raw.byteLength>limit)throw Error('Complete scenario bytes exceed the bound or are invalid');
  return strictJSON(new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(raw));
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
  const {limit=LIMIT,expectedBytes,timeoutMs=12000,signal}=options;
  if(!Number.isSafeInteger(limit)||limit<1||limit>LIMIT)throw Error('Invalid scenario byte bound');
  if(expectedBytes!==undefined&&(!Number.isSafeInteger(expectedBytes)||expectedBytes<0||expectedBytes>limit))throw Error('Export exceeds the 4 MB bound or has an invalid byte length');
  if(!Number.isSafeInteger(timeoutMs)||timeoutMs<1||timeoutMs>60000)throw Error('Invalid scenario read deadline');
  const aborted=()=>{const error=Error('Scenario read cancelled');error.name='AbortError';return error;};if(signal?.aborted)throw aborted();
  const controller=new AbortController();let timer,reader,response,finished=false,stop;
  const interruption=new Promise((_,reject)=>{stop=reject;timer=setTimeout(()=>{reject(Error('Scenario read timed out'));controller.abort();},timeoutMs);});
  const onAbort=()=>{stop(aborted());controller.abort();};signal?.addEventListener('abort',onAbort,{once:true});
  try{return await Promise.race([interruption,(async()=>{
   response=await open(controller.signal);
   if(finished||signal?.aborted){try{Promise.resolve(response?.body?.cancel?.()).catch(()=>{});}catch{}throw aborted();}
   if(!response?.ok)throw Error('Scenario source is unavailable');
   if(!response.body?.getReader)throw Error('Complete readable scenario body required');
   reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(finished||signal?.aborted)throw aborted();if(done)break;
    if(!(value instanceof Uint8Array))throw Error('Invalid scenario body chunk');size+=value.byteLength;
    if(size>limit||(expectedBytes!==undefined&&size>expectedBytes))throw Error('Scenario body exceeds the complete byte bound');parts.push(value);
   }
   if(expectedBytes!==undefined&&size!==expectedBytes)throw Error('Incomplete scenario bytes');
   const raw=new Uint8Array(size);let offset=0;for(const part of parts){raw.set(part,offset);offset+=part.byteLength;}return raw;
  })()]);}finally{finished=true;clearTimeout(timer);signal?.removeEventListener('abort',onAbort);controller.abort();
   if(reader){try{Promise.resolve(reader.cancel()).catch(()=>{});}catch{}try{reader.releaseLock?.();}catch{}}
   else{try{Promise.resolve(response?.body?.cancel?.()).catch(()=>{});}catch{}}
  }
 }
 async function readFile(file,options={}){
  if(!file||!Number.isSafeInteger(file.size)||file.size<0||file.size>LIMIT||typeof file.stream!=='function')throw Error('Export exceeds the 4 MB bound or is not a readable file');
  return decode(await readComplete(()=>({ok:true,body:file.stream()}),{...options,limit:LIMIT,expectedBytes:file.size}));
 }
 const api={EXPORT,LIMIT,strictJSON,decode,identity,same,verify,readComplete,readFile};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.JHScenarioIO=api;
})(typeof globalThis==='object'?globalThis:this);
