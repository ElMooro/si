/* Retained portfolio research evidence: local inspection only, never an account request. */
(function(root){
 'use strict';
 const LIMIT=8*1024*1024,SCHEMA='snapshot-research.v1',SOURCE_SCHEMA='snapshot-research-source.v1';
 const known={
  'screener/alpha-score.json':'Alpha scores',
  'signals/confluence.json':'Confluence',
  'signals/regime-picks.json':'Regime picks',
  'sentiment/data.json':'Sentiment',
 };
 const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
 const integer=v=>Number.isSafeInteger(v)&&v>=0;
 const hash=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
 const clock=v=>typeof v==='string'&&v.trim()?v:null;
 function source(key,value){
  const row=object(value)?value:{};
  const metadata=row.schema_version===SOURCE_SCHEMA&&row.key===key&&row.body_complete===true&&
   integer(row.body_bytes)&&row.body_bytes<=LIMIT&&hash(row.body_sha256)&&row.body_encoding==='base64'&&
   typeof row.body==='string'&&row.body.length<=4*Math.ceil(LIMIT/3)&&row.body.length%4===0&&
   /^[A-Za-z0-9+/]*={0,2}$/.test(row.body);
  return {key,label:Object.hasOwn(known,key)?known[key]:key,status:typeof row.status==='string'?row.status:'UNAVAILABLE',
   reason:typeof row.reason_code==='string'?row.reason_code:null,
   declared:clock(row.declared_generated_at),started:clock(row.started_at),completed:clock(row.completed_at),
   complete:row.body_complete===true,bytes:integer(row.body_bytes)?row.body_bytes:null,sha:hash(row.body_sha256)?row.body_sha256:null,
   inspectable:metadata,body:metadata?row.body:null,
   detail:metadata?'Original bytes retained; verify to inspect.':row.body_complete===true?'Retained-source metadata is incomplete or inconsistent.':'No complete original body is recorded.'};
 }
 function view(snapshot){
  const research=object(snapshot)&&object(snapshot.research)?snapshot.research:null;
  if(!research||research.schema_version!==SCHEMA||!object(research.source_documents))return {
   available:false,title:'Research evidence unavailable',detail:'This snapshot does not contain the supported retained-source contract.',sources:[],joins:[]};
  const keys=[...Object.keys(known),...Object.keys(research.source_documents).filter(k=>!Object.hasOwn(known,k)).sort()];
  const sources=keys.map(key=>source(key,research.source_documents[key]));
  const rows=object(research.joins)?research.joins:{};
  const joins=['alpha','confluence_s','confluence_a','confluence_b','regime','sentiment'].map(name=>{
   const row=object(rows[name])?rows[name]:{},duplicates=object(row.duplicate_symbol_occurrences)?Object.entries(row.duplicate_symbol_occurrences):null;
   const validIndices=v=>Array.isArray(v)&&v.every(integer)&&new Set(v).size===v.length;
   const validDuplicates=duplicates!==null&&duplicates.every(([symbol,positions])=>symbol&&validIndices(positions)&&positions.length>1);
   return {name,shape:row.source_shape==='ARRAY'?'ARRAY':'UNAVAILABLE_OR_INVALID_ARRAY',
    rows:row.source_shape==='ARRAY'&&integer(row.source_row_count)?row.source_row_count:null,
    unique:integer(row.unique_usable_symbols)?row.unique_usable_symbols:null,
    invalid:validIndices(row.invalid_zero_based_occurrences)?row.invalid_zero_based_occurrences.length:null,
    duplicates:validDuplicates?duplicates.length:null,original:row};
  });
  const complete=sources.filter(row=>row.complete);
  const sum=complete.every(row=>row.bytes!==null)?complete.reduce((total,row)=>total+row.bytes,0):null;
  const bound=research.source_byte_bound===LIMIT&&integer(research.retained_complete_body_bytes)&&sum!==null&&
   sum===research.retained_complete_body_bytes&&sum<=LIMIT;
  return {available:true,title:'Retained research inputs',detail:'Source dates and byte checks are evidence of this copy; freshness, independence and investment validity remain unverified.',
   sources,joins,byteContractConsistent:bound,retainedBytes:integer(research.retained_complete_body_bytes)?research.retained_complete_body_bytes:null,
   allowsSizing:false};
 }
 async function original(row){
  if(!row||row.inspectable!==true||!integer(row.bytes)||row.bytes>LIMIT||!hash(row.sha)||typeof row.body!=='string'||
   row.body.length>4*Math.ceil(LIMIT/3)||row.body.length%4!==0||!/^[A-Za-z0-9+/]*={0,2}$/.test(row.body))
   throw Error('A complete retained source with supported metadata is required.');
  const binary=root.atob(row.body),bytes=new Uint8Array(binary.length);
  for(let i=0;i<binary.length;i++)bytes[i]=binary.charCodeAt(i);
  // Canonical round-trip also rejects nonzero base64 padding bits.
  if(bytes.length!==row.bytes||bytes.length>LIMIT||root.btoa(binary)!==row.body)throw Error('Retained source byte length or encoding does not match.');
  const cryptoAPI=typeof module==='object'&&module.exports?require('node:crypto').webcrypto:root.crypto;
  if(!cryptoAPI?.subtle)throw Error('Source byte verification is unavailable in this browser.');
  const digest=[...new Uint8Array(await cryptoAPI.subtle.digest('SHA-256',bytes))].map(v=>v.toString(16).padStart(2,'0')).join('');
  if(digest!==row.sha)throw Error('Retained source bytes do not match the recorded hash.');
  let text=null;try{text=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);}catch(_){/* Exact bytes remain downloadable. */}
  return {bytes,text,sha:digest,key:row.key,filename:(Object.hasOwn(known,row.key)?row.key.split('/').pop().replace(/\.json$/,''):'retained-source')+'.original',
   interpretation:'Exact retained bytes; no source freshness or independent provider verification.'};
 }
 const api={LIMIT,SCHEMA,source,view,original};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHPortfolioResearch=api;
})(typeof globalThis==='object'?globalThis:this);
