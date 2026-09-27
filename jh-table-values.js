/* Display values and deterministic sorting. No measurement or model qualification. */
(function(root){
 'use strict';
 function number(value){
  if(typeof value!=='number'&&typeof value!=='string')return null;
  const raw=String(value).trim();
  if(!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(raw))return null;
  const n=Number(raw);
  if(!Number.isFinite(n)||Math.abs(n)>Number.MAX_SAFE_INTEGER||(n===0&&/[1-9]/.test(raw.split(/[eE]/)[0])))return null;
  return n;
 }
 function format(value,digits=1){const n=number(value);return n===null?'':n.toFixed(digits);}
 function percent(value,digits=2){const n=number(value);return n===null?'':(n>0?'+':'')+n.toFixed(digits)+'%';}
 function integer(value){const n=number(value);return n!==null&&Number.isSafeInteger(n)&&n>=0?n:null;}
 function count(value){const n=integer(value);return n===null?'Unavailable':n.toLocaleString('en-US');}
 function sign(value,positive='pos',negative='neg'){const n=number(value);return n===null||n===0?'':n>0?positive:negative;}
 function compare(a,b,key,dir=1,kind='number'){
  function value(row){const v=row?.[key];if(kind==='text')return typeof v==='string'&&v.trim()?v:null;if(kind==='boolean')return typeof v==='boolean'?Number(v):null;return number(v);}
  const av=value(a),bv=value(b);
  if(av===null&&bv===null)return 0;
  if(av===null)return 1;if(bv===null)return -1;
  const order=kind==='text'?av.localeCompare(bv):av===bv?0:av<bv?-1:1;
  return order*(dir<0?-1:1);
 }
 function bindSort(headers,key,direction,onSort){
  for(const th of headers){
   th.tabIndex=0;th.scope='col';th.setAttribute('aria-sort',th.dataset.k===key?(direction>0?'ascending':'descending'):'none');
   const activate=()=>{const doc=th.ownerDocument,index=doc?.activeElement===th?[...doc.querySelectorAll('th[data-k]')].indexOf(th):-1;onSort(th.dataset.k);if(index>=0)doc.querySelectorAll('th[data-k]')[index]?.focus();};
   th.onclick=activate;
   th.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();activate();}};
  }
 }
 function parseJSON(source){
  let i=0;const numeric=/-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function ws(){while(/[\x20\t\r\n]/.test(source[i]||'x'))i++;}
  function string(){const start=i++;for(;i<source.length;i++){if(source[i]==='\\'){i++;continue;}if(source[i]==='"')return JSON.parse(source.slice(start,++i));}throw Error('Incomplete JSON string');}
  function value(depth){
   if(depth>128)throw Error('JSON nesting exceeds bound');ws();const c=source[i];
   if(c==='"'){string();return;}
   if(c==='{'||c==='['){const object=c==='{',seen=new Set(),end=object?'}':']';i++;ws();if(source[i]===end){i++;return;}
    for(;;){ws();if(object){if(source[i]!=='"')throw Error('JSON key required');const key=string();if(seen.has(key))throw Error('Duplicate JSON key');seen.add(key);ws();if(source[i++]!==':')throw Error('JSON colon required');}
     value(depth+1);ws();if(source[i]===end){i++;return;}if(source[i++]!==',')throw Error('Incomplete JSON structure');}
   }
   for(const token of ['true','false','null'])if(source.startsWith(token,i)){i+=token.length;return;}
   numeric.lastIndex=i;const m=numeric.exec(source);if(!m)throw Error('Invalid JSON value');i=numeric.lastIndex;const n=Number(m[0]);
   if(!Number.isFinite(n)||(n===0&&/[1-9]/.test(m[0].split(/[eE]/)[0])))throw Error('JSON numeric overflow or underflow');
  }
  value(0);ws();if(i!==source.length)throw Error('Trailing JSON content');return JSON.parse(source);
 }
 // A validation grammar, not a list of consumed feeds. Each caller owns its
 // literal source binding; merely importing this helper requests nothing.
 const PUBLIC_PATHS=/^\/data\/(?:earnings-quality|hiring-velocity|estimate-revisions|8k-filings|10kq-filings|sec-filings-intel|capex-pulse|backlog|buyback-engine|inventory-drawdown|canary-macro)\.json$/;
 async function load(path,options={}){
  if(typeof path!=='string'||!PUBLIC_PATHS.test(path))throw Error('Unreviewed public desk path');
  const controller=new AbortController();let timer,reader;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Stored desk publication timed out'));},options.timeout??15000);});
  try{return await Promise.race([deadline,(async()=>{
   const response=await(options.fetcher||root.fetch.bind(root))(path+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Stored publication unavailable'+(Number.isInteger(response.status)?' (HTTP '+response.status+')':''));
   reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const{value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>64*1024*1024)throw Error('Complete publication exceeds display bound');parts.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.byteLength;}
   const raw=new TextDecoder('utf-8',{fatal:true}).decode(bytes);
   try{const packet=parseJSON(raw);if(!packet||typeof packet!=='object'||Array.isArray(packet))throw Error('Publication object required');return{packet,raw};}
   catch(error){error.original_text=raw;throw error;}
  })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 const api={number,format,percent,integer,count,sign,compare,bindSort,parseJSON,load,PUBLIC_PATHS};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHTableValues=api;
})(typeof globalThis!=='undefined'?globalThis:this);
