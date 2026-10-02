/* Complete received bytes with explicit decimal projection checks. No polling or storage. */
(function(root){
 'use strict';
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 function decimal(token){
  const m=/^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(token);
  if(!m)throw Error('Invalid numeric token');
  let digits=(m[2]+(m[3]||'')).replace(/^0+/,'');if(!digits)return '0';
  const tail=/0+$/.exec(digits)?.[0].length||0,exponent=Number(m[4]||0);
  if(!Number.isSafeInteger(exponent))throw Error('Numeric exponent exceeds supported precision');
  if(tail)digits=digits.slice(0,-tail);
  return m[1]+digits+'e'+(exponent-(m[3]||'').length+tail);
 }
 function decode(raw,io,limit){
  const source=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(raw);
  const tokens=/"(?:[^"\\]|\\.)*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
  for(const m of source.matchAll(tokens)){
   if(m[0][0]==='"')continue;const n=Number(m[0]);
   if(!Number.isFinite(n))throw Error('Nonfinite numeric token; original retained');
   if(Number.isInteger(n)&&!Number.isSafeInteger(n))throw Error('Unsafe integer; original retained');
   if(decimal(m[0])!==decimal(String(n)))throw Error('Numeric token loses precision; original retained');
  }
  const data=io.decode(raw,limit);if(!object(data))throw Error('Expected a packet object; original retained');
  return {data,source};
 }
 async function receive(url,options={}){
  const receipt={url,status:'unavailable',received_at:null,http_status:null,raw:null,data:null,sha256:null};
  try{
   const io=options.io||root.JHEvidenceIO,fetcher=options.fetcher||root.fetch.bind(root);
   if(!io?.readComplete||!io?.decode)throw Error('Complete evidence reader unavailable');
   const limit=options.limit??io.MAX_LIMIT,target=new URL(url,root.location?.href||'https://justhodl.ai/');
   target.searchParams.set('exact','1');target.searchParams.set('nogen','1');receipt.request_url=target.href;receipt.byte_limit=limit;
   receipt.raw=await io.readComplete(async signal=>{
    const response=await fetcher(target.href,{cache:'no-store',signal});receipt.http_status=response.status;
    if(!response.ok){try{await response.body?.cancel();}catch{}throw Error('HTTP '+response.status);}return response;
   },{limit,timeoutMs:options.timeoutMs??20000});
   receipt.received_at=new Date().toISOString();
   if(root.crypto?.subtle)receipt.sha256=Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',receipt.raw)),x=>x.toString(16).padStart(2,'0')).join('');
   receipt.source=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(receipt.raw);
   Object.assign(receipt,decode(receipt.raw,io,limit),{status:'received'});
  }catch(error){receipt.reason=String(error?.message||error);}
  return receipt;
 }
 const api={decimal,decode,receive};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.JHNumericEvidence=api;
})(typeof globalThis==='object'?globalThis:this);
