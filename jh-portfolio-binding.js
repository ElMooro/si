/* Complete snapshot value identity; no account access or investment authority. */
(function(root){
 'use strict';
 const LIMIT=32*1024*1024,CONTRACT='portfolio-snapshot-value.v1',ENCODING='typed-json-binary64.v1';
 const cryptoAPI=typeof module==='object'&&module.exports?require('node:crypto').webcrypto:root.crypto;
 function encode(value){
  let buffer=new Uint8Array(1024),size=0;const utf8=new TextEncoder();
  function put(raw){
   const end=size+raw.byteLength;if(end>LIMIT)throw Error('Snapshot identity exceeds complete byte bound');
   if(end>buffer.length){let cap=buffer.length;while(cap<end)cap=Math.min(LIMIT,cap*2);const next=new Uint8Array(cap);next.set(buffer);buffer=next;}
   buffer.set(raw,size);size=end;
  }
  const ascii=s=>put(utf8.encode(s));
  function stringBytes(s){
   for(let i=0;i<s.length;i++){const c=s.charCodeAt(i);if(c>=0xd800&&c<=0xdbff){const low=s.charCodeAt(++i);if(!(low>=0xdc00&&low<=0xdfff))throw Error('Snapshot contains invalid Unicode');}else if(c>=0xdc00&&c<=0xdfff)throw Error('Snapshot contains invalid Unicode');}
   return utf8.encode(s);
  }
  function string(s){const raw=stringBytes(s);ascii('s'+raw.length+':');put(raw);}
  function visit(v,depth=0){
   if(depth>128)throw Error('Snapshot identity nesting exceeds bound');
   if(v===null)ascii('n');
   else if(typeof v==='boolean')ascii(v?'t':'f');
   else if(typeof v==='number'){
    if(!Number.isFinite(v)||(Number.isInteger(v)&&!Number.isSafeInteger(v)))throw Error('Snapshot number cannot be represented safely in both runtimes');
    ascii('d');const bytes=new Uint8Array(8);new DataView(bytes.buffer).setFloat64(0,v===0?0:v,false);put(bytes);
   }else if(typeof v==='string')string(v);
   else if(Array.isArray(v)){ascii('a'+v.length+':');for(const child of v)visit(child,depth+1);}
   else if(v&&typeof v==='object'){
    const proto=Object.getPrototypeOf(v);if(proto!==null&&proto!==Object.prototype)throw Error('Snapshot identity requires plain JSON objects');
    const keys=Object.keys(v).map(key=>({key,bytes:stringBytes(key)})).sort((a,b)=>{for(let i=0;i<Math.min(a.bytes.length,b.bytes.length);i++)if(a.bytes[i]!==b.bytes[i])return a.bytes[i]-b.bytes[i];return a.bytes.length-b.bytes.length;});
    ascii('o'+keys.length+':');for(const {key} of keys){string(key);visit(v[key],depth+1);}
   }else throw Error('Snapshot identity requires JSON values');
  }
  visit(value);return buffer.slice(0,size);
 }
 async function identity(value){const raw=encode(value),hash=new Uint8Array(await cryptoAPI.subtle.digest('SHA-256',raw));return {encoding:ENCODING,value_sha256:[...hash].map(v=>v.toString(16).padStart(2,'0')).join(''),encoded_bytes:raw.byteLength};}
 async function verify(snapshot,risk){
  const b=risk?.snapshot_binding,names=['contract','key','generated_at','encoding','value_sha256','encoded_bytes'];
  if(!b||typeof b!=='object'||Array.isArray(b)||Object.keys(b).length!==names.length||!names.every(k=>Object.hasOwn(b,k))||b.contract!==CONTRACT||b.key!=='portfolio/snapshot.json'||b.encoding!==ENCODING||typeof b.value_sha256!=='string'||!/^[a-f0-9]{64}$/.test(b.value_sha256)||!Number.isSafeInteger(b.encoded_bytes)||b.encoded_bytes<1||b.encoded_bytes>LIMIT)throw Error('Risk lacks a supported complete snapshot binding; portfolio risk is withheld.');
  if(!snapshot||typeof snapshot!=='object'||Array.isArray(snapshot)||typeof snapshot.generated_at!=='string'||b.generated_at!==snapshot.generated_at)throw Error('Risk and holdings refer to different snapshot publications.');
  const actual=await identity(snapshot);
  if(actual.value_sha256!==b.value_sha256||actual.encoded_bytes!==b.encoded_bytes)throw Error('Risk was calculated from different snapshot values; portfolio risk is withheld.');
  return actual;
 }
 const api={CONTRACT,ENCODING,LIMIT,encode,identity,verify};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHPortfolioBinding=api;
})(typeof globalThis==='object'?globalThis:this);
