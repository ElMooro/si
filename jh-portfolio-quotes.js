/* Quote evidence already contained in a loaded private snapshot; no I/O. */
(function(root){
 'use strict';
 const LIMIT=128*1024,SCHEMA='portfolio-quote-collection.v1';
 const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
 const integer=v=>Number.isSafeInteger(v)&&v>=0;
 const symbol=v=>typeof v==='string'&&/^[A-Z][A-Z0-9.\-]{0,14}$/.test(v);
 const hash=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
 const text=v=>typeof v==='string'&&v.trim()?v:null;
 const clock=v=>integer(v)&&Number.isFinite(new Date(v).getTime())?new Date(v).toISOString():null;
 function source(key,value){
  const row=object(value)?value:{},trace=object(row.source_evidence)?row.source_evidence:{};
  const supported=trace.schema_version==='previous-close-source.v1';
  const metadata=supported&&trace.body_complete===true&&integer(trace.body_bytes)&&trace.body_bytes<=LIMIT&&hash(trace.body_sha256)&&
   trace.body_encoding==='base64'&&typeof trace.body==='string'&&trace.body.length===4*Math.ceil(trace.body_bytes/3)&&/^[A-Za-z0-9+/]*={0,2}$/.test(trace.body);
  const identified=symbol(key)&&trace.requested_symbol===key;
  const stated=typeof row.price==='number'&&Number.isFinite(row.price)&&row.price>0&&(!Number.isInteger(row.price)||Number.isSafeInteger(row.price))?row.price:null;
  const measured=supported&&identified&&trace.status==='MEASURED_PREVIOUS_CLOSE'&&trace.reason_code==null&&metadata&&
   row.price_basis==='SPLIT_ADJUSTED_PREVIOUS_DAY_CLOSE'&&row.price_timestamp_basis==='AGGREGATE_WINDOW_START_UTC_MS'&&clock(row.as_of_unix_ms)!==null;
  return {key,label:key,requestedSymbol:text(trace.requested_symbol),supported,identified,
   status:text(trace.status)||'UNAVAILABLE',reason:text(trace.reason_code),
   price:measured?stated:null,reportedPrice:stated,windowStart:clock(row.as_of_unix_ms),
   started:text(trace.started_at),completed:text(trace.completed_at),
   taskStarted:typeof trace.collection_task_started==='boolean'?trace.collection_task_started:null,
   requestAttempted:typeof trace.request_attempted==='boolean'?trace.request_attempted:null,
   complete:trace.body_complete===true,bytes:integer(trace.body_bytes)?trace.body_bytes:null,
   sha:hash(trace.body_sha256)?trace.body_sha256:null,inspectable:metadata,body:metadata?trace.body:null,
   record:value===undefined?null:value,allowsSizing:false};
 }
 function view(snapshot){
  const accounting=object(snapshot)&&object(snapshot.accounting)?snapshot.accounting:null;
  if(!accounting||!object(accounting.source_prices))return {available:false,title:'Quote evidence unavailable',detail:'This snapshot does not contain individual retained quote records.',rows:[],coverageConsistent:false};
  const records=accounting.source_prices,collection=object(accounting.quote_collection)?accounting.quote_collection:null;
  const requested=collection?.schema_version===SCHEMA&&Array.isArray(collection.requested_symbols)?collection.requested_symbols:null;
  const requestListValid=requested!==null&&requested.every(symbol)&&new Set(requested).size===requested.length&&requested.every((v,i)=>i===0||requested[i-1]<v);
  const keys=[...new Set([...Object.keys(records),...(requestListValid?requested:[])])].sort();
  const rows=keys.map(key=>source(key,Object.hasOwn(records,key)?records[key]:undefined));
  const observed={records:Object.keys(records).length,tasksStarted:rows.filter(r=>r.taskStarted===true).length,
   unattempted:rows.filter(r=>r.taskStarted===false).length,measured:rows.filter(r=>r.supported&&r.identified&&r.status==='MEASURED_PREVIOUS_CLOSE').length};
  const bodies=rows.filter(r=>r.complete),bodyBytes=bodies.every(r=>r.bytes!==null)?bodies.reduce((n,r)=>n+r.bytes,0):null;
  const reasons=new Map();for(const row of rows)if(row.reason!==null)reasons.set(row.reason,(reasons.get(row.reason)||0)+1);
  const expectedReasons=object(collection?.reason_counts)?Object.entries(collection.reason_counts).sort(([a],[b])=>a<b?-1:a>b?1:0):null;
  const actualReasons=[...reasons.entries()].sort(([a],[b])=>a<b?-1:a>b?1:0);
  const consistentRows=rows.every(r=>r.identified&&r.supported&&r.taskStarted!==null&&
   (r.status!=='MEASURED_PREVIOUS_CLOSE'||r.price!==null)&&(!r.complete||r.inspectable)&&
   (r.taskStarted?r.status!=='NOT_ATTEMPTED':r.status==='NOT_ATTEMPTED'&&r.requestAttempted===false&&!r.complete));
  const coverage=requestListValid&&Object.keys(records).length===requested.length&&rows.length===requested.length&&consistentRows&&
   expectedReasons!==null&&JSON.stringify(expectedReasons)===JSON.stringify(actualReasons)&&
   integer(collection.unique_requested_count)&&collection.unique_requested_count===requested.length&&
   integer(collection.tasks_started)&&collection.tasks_started===observed.tasksStarted&&
   integer(collection.unattempted_count)&&collection.unattempted_count===observed.unattempted&&
   collection.tasks_started+collection.unattempted_count===requested.length&&
   integer(collection.measured_previous_close_count)&&collection.measured_previous_close_count===observed.measured&&
   integer(collection.retained_complete_body_bytes)&&collection.retained_complete_body_bytes===bodyBytes&&
   collection.source_body_byte_bound===4*1024*1024&&bodyBytes<=collection.source_body_byte_bound&&
   collection.status===(collection.unattempted_count===0?'COMPLETE_ATTEMPT_COVERAGE':'PARTIAL_ATTEMPT_COVERAGE');
  return {available:true,title:'Previous-close quote evidence',rows,collection,observed,coverageConsistent:!!coverage,
   detail:'Split-adjusted previous-day bars are not live execution quotes. Bar-window dates differ from acquisition times. Currency and held-instrument identity remain unverified.',
   coverageText:!collection||collection.schema_version!==SCHEMA?'Collection coverage is unavailable for this snapshot.':coverage?
    `Recorded collection: ${collection.tasks_started} of ${requested.length} symbols attempted · ${collection.measured_previous_close_count} previous closes reported · ${collection.unattempted_count} not attempted.`:
    'Collection counts or identities are inconsistent with the retained quote records. Inspect the records before using them.',allowsSizing:false};
 }
 async function original(row){
  if(!row||row.inspectable!==true||!integer(row.bytes)||row.bytes>LIMIT||typeof row.body!=='string'||row.body.length!==4*Math.ceil(row.bytes/3))throw Error('A complete bounded quote body is required.');
  const research=typeof module==='object'&&module.exports?require('./jh-portfolio-research.js'):root.JHPortfolioResearch;
  if(!research)throw Error('Original-byte verifier is unavailable.');
  const result=await research.original(row);
  return {...result,filename:'quote-'+(symbol(row.key)?row.key:'unidentified')+'.original',interpretation:'Exact retained response bytes. This does not verify the provider, currency, freshness or held instrument.'};
 }
 const api={LIMIT,SCHEMA,source,view,original};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHPortfolioQuotes=api;
})(typeof globalThis==='object'?globalThis:this);
