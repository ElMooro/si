/* Strict UTF-8 JSON and complete bounded byte acquisition. No routes or storage. */
(function(root){
 'use strict';
 const LIMIT=4*1024*1024,MAX_LIMIT=64*1024*1024;
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
  if(!(raw instanceof Uint8Array)||!Number.isSafeInteger(limit)||limit<1||limit>MAX_LIMIT||raw.byteLength>limit)throw Error('Complete JSON bytes exceed the bound or are invalid');
  return strictJSON(new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(raw));
 }
 async function readComplete(open,options={}){
  const {limit=LIMIT,expectedBytes,timeoutMs=12000,signal}=options;
  if(!Number.isSafeInteger(limit)||limit<1||limit>MAX_LIMIT)throw Error('Invalid JSON byte bound');
  if(expectedBytes!==undefined&&(!Number.isSafeInteger(expectedBytes)||expectedBytes<0||expectedBytes>limit))throw Error('Body exceeds the selected byte bound or has an invalid byte length');
  if(!Number.isSafeInteger(timeoutMs)||timeoutMs<1||timeoutMs>60000)throw Error('Invalid JSON read deadline');
  const aborted=()=>{const error=Error('JSON read cancelled');error.name='AbortError';return error;};if(signal?.aborted)throw aborted();
  const controller=new AbortController();let timer,reader,response,finished=false,stop;
  const interruption=new Promise((_,reject)=>{stop=reject;timer=setTimeout(()=>{reject(Error('JSON read timed out'));controller.abort();},timeoutMs);});
  const onAbort=()=>{stop(aborted());controller.abort();};signal?.addEventListener('abort',onAbort,{once:true});
  try{return await Promise.race([interruption,(async()=>{
   response=await open(controller.signal);
   if(finished||signal?.aborted){try{Promise.resolve(response?.body?.cancel?.()).catch(()=>{});}catch{}throw aborted();}
   if(!response?.ok)throw Error('JSON source is unavailable');
   if(!response.body?.getReader)throw Error('Complete readable JSON body required');
   reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(finished||signal?.aborted)throw aborted();if(done)break;
    if(!(value instanceof Uint8Array))throw Error('Invalid JSON body chunk');size+=value.byteLength;
    if(size>limit||(expectedBytes!==undefined&&size>expectedBytes))throw Error('JSON body exceeds the complete byte bound');parts.push(value);
   }
   if(expectedBytes!==undefined&&size!==expectedBytes)throw Error('Incomplete JSON bytes');
   const raw=new Uint8Array(size);let offset=0;for(const part of parts){raw.set(part,offset);offset+=part.byteLength;}return raw;
  })()]);}finally{finished=true;clearTimeout(timer);signal?.removeEventListener('abort',onAbort);controller.abort();
   if(reader){try{Promise.resolve(reader.cancel()).catch(()=>{});}catch{}try{reader.releaseLock?.();}catch{}}
   else{try{Promise.resolve(response?.body?.cancel?.()).catch(()=>{});}catch{}}
  }
 }
 const api={LIMIT,MAX_LIMIT,strictJSON,decode,readComplete};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.JHEvidenceIO=api;
})(typeof globalThis==='object'?globalThis:this);
