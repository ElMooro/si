/* Complete, on-demand field inspection. Never truncates values or silently drops null/nested rows. */
(function(global){
'use strict';
const PAGE=25;
function type(v){return v===null?'null':Array.isArray(v)?'array':typeof v;}
function ptr(base,key){return base+'/'+String(key).replace(/~/g,'~0').replace(/\//g,'~1');}
function columns(rows){return [...new Set(rows.flatMap(r=>r&&typeof r==='object'&&!Array.isArray(r)?Object.keys(r):[]))];}
function leafPaths(value,path=''){
 if(value!==null&&typeof value==='object'){
  const keys=Object.keys(value);return keys.length?keys.flatMap(k=>leafPaths(value[k],ptr(path,k))):[path];
 }
 return [path];
}
function node(tag,text){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;}
function valueView(value,path){
 const kind=type(value);
 if(kind!=='array'&&kind!=='object'){
  const n=node('span',kind==='null'?'null':String(value));n.className='jdi-value jdi-'+kind;n.dataset.jsonPointer=path;n.dataset.valueType=kind;return n;
 }
 const keys=Object.keys(value),details=node('details');details.className='jdi-detail';details.dataset.jsonPointer=path;
 const summary=node('summary',kind+' · '+keys.length+(kind==='array'?' items':' fields'));details.append(summary);
 let loaded=false;
 details.addEventListener('toggle',()=>{if(!details.open||loaded)return;loaded=true;details.append(collectionView(value,path));});
 return details;
}
function collectionView(value,path){
 const outer=node('div');outer.className='jdi-collection';const keys=Object.keys(value),isArray=Array.isArray(value);
 let page=0;const body=node('div');body.className='jdi-scroll';const control=node('div');control.className='jdi-controls';
 const prev=node('button','Previous'),next=node('button','Next'),count=node('span');prev.type=next.type='button';
 prev.setAttribute('aria-label','Previous fields or rows');next.setAttribute('aria-label','Next fields or rows');control.append(prev,count,next);outer.append(control,body);
 function paint(){
  body.replaceChildren();const begin=page*PAGE,end=Math.min(begin+PAGE,keys.length),part=keys.slice(begin,end);
  count.textContent=keys.length?(begin+1)+'–'+end+' of '+keys.length:'Empty '+type(value);
  prev.disabled=page===0;next.disabled=end>=keys.length;
  const table=node('table');table.className='jdi-table';const cols=isArray?columns(value):[];
  if(isArray&&cols.length){
   const tr=node('tr');tr.append(node('th','Index'),node('th','Type / full row'));cols.forEach(c=>tr.append(node('th',c)));const head=node('thead');head.append(tr);table.append(head);
   const rows=node('tbody');part.forEach(k=>{const r=value[k],tr=node('tr');tr.append(node('td',k));const all=node('td');all.append(valueView(r,ptr(path,k)));tr.append(all);
    cols.forEach(c=>{const td=node('td');if(r&&typeof r==='object'&&Object.prototype.hasOwnProperty.call(r,c))td.append(valueView(r[c],ptr(ptr(path,k),c)));else td.append(node('span','— absent'));tr.append(td);});rows.append(tr);});table.append(rows);
  }else{
   const tr=node('tr');['JSON path','Type','Value'].forEach(c=>tr.append(node('th',c)));const head=node('thead');head.append(tr);table.append(head);
   const rows=node('tbody');part.forEach(k=>{const tr=node('tr'),td=node('td');tr.append(node('td',ptr(path,k)),node('td',type(value[k])));td.append(valueView(value[k],ptr(path,k)));tr.append(td);rows.append(tr);});table.append(rows);
  }
  body.append(table);
 }
 prev.onclick=()=>{page--;paint();};next.onclick=()=>{page++;paint();};paint();return outer;
}
function inspect(container,payload,provenance){
 container.replaceChildren();const head=node('p',provenance||'All fields from this returned engine artifact. Expand objects and use row/field pagination.');container.append(head);
 const leaves=leafPaths(payload);const info=node('p',leaves.length+' inspectable leaf paths · '+type(payload)+' · no value truncation');container.append(info);
 const search=node('input');search.type='search';search.placeholder='Find an exact field name or JSON path';search.setAttribute('aria-label','Find a JSON field');const found=node('div');container.append(search,found);
 search.addEventListener('input',()=>{
  found.replaceChildren();const q=search.value.trim().toLowerCase();if(!q)return;
  const paths=leaves.filter(p=>p.toLowerCase().includes(q));const matching=[];for(const p of paths){let v=payload;if(p)for(const bit of p.slice(1).split('/'))v=v[bit.replace(/~1/g,'/').replace(/~0/g,'~')];matching.push({json_path:p||'/',type:type(v),value:v});}
  found.append(node('p',paths.length+' matching fields'),collectionView(matching,''));
 });
 container.append(payload!==null&&typeof payload==='object'?collectionView(payload,''):valueView(payload,''));
}
async function fetchArtifact(entry,fetcher,privateClient){
 if(entry.access==='owner_authenticated'){
  if(!privateClient||privateClient.kindFor('/'+entry.key)!==entry.private_kind)throw new Error('Authenticated owner data route unavailable');
  return privateClient.fetch('/'+entry.key,{cache:'no-store',credentials:'same-origin'});
 }
 if(entry.access!=='public')throw new Error('Artifact access is not approved');
 const key=entry.key;
 if(typeof key!=='string'||!/^[A-Za-z0-9_./-]+\.json(?:\.gz)?$/.test(key)||key.startsWith('/')||key.includes('..')||key.includes('//'))throw new Error('Artifact key is not supported by the reviewed data proxy');
 // Only /data/* has a zone route on the Pages domain. Other approved
 // namespaces must use the existing read-only data proxy, verbatim; never
 // retry under /data/, strip a prefix, or fall back to an unauthenticated
 // owner artifact. Cross-origin public reads carry no browser credentials.
 const sameOrigin=key.startsWith('data/');
 const url=(sameOrigin?'/'+key:'https://justhodl-data-proxy.raafouis.workers.dev/'+key)+'?exact=1&nogen=1';
 const response=await fetcher(url,{cache:'no-store',credentials:sameOrigin?'same-origin':'omit'});
 if(response.ok&&response.headers?.get('X-JH-Artifact-Key')!==key)throw new Error('Exact artifact identity is unverified; no alternate feed is accepted');
 return response;
}
function validateProjection(entry,data){
 const required=entry.required_projection;
 if(required&&(!data||typeof data!=='object'||Array.isArray(data)))throw new Error('Required public projection is unavailable');
 const publication=data&&data.publication;
 const safe={
  'fleet-health':()=>data.privacy_version==='fleet-metadata-20260909-v1',
  'fleet-errors':()=>data.privacy_version==='fleet-errors-metadata-20260909-v1'&&data.diagnostic_text_private===true,
  'fleet-freshness':()=>publication&&publication.schema_version==='public-freshness-report.v1'&&publication.scope==='PUBLIC_ENGINE_HEALTH'&&publication.contains_private_data===false,
  'source-map':()=>data.schema_version==='public-source-map.v1'&&publication&&publication.scope==='PUBLIC_MARKET_SOURCE_METADATA'&&publication.contains_private_data===false,
  'provider-metrics':()=>publication&&publication.schema_version==='public-provider-metrics.v1'&&publication.contains_provider_response_text===false&&publication.diagnostics==='FIXED_CATEGORIES_ONLY'
 };
 if(safe[required]&&!safe[required]())throw new Error('Required public projection is unavailable');
 if(entry.required_projection==='brain-compiler'){
  if((data.claims||[]).some(row=>Object.hasOwn(row,'claim'))||(data.build_queue||[]).some(row=>Object.hasOwn(row,'sample_claims')))throw new Error('Public projection not yet redacted');
 }
 if(entry.required_projection==='sizing'&&(data.holdings!==null||data.holdings_publication!=='REDACTED_ACCOUNT_PRIVATE'))throw new Error('Public account projection not yet redacted');
 return data;
}
function indexedOutputs(entry,payload){
 const index=entry.archive_index;if(!index)return [];
 if(index.required_schema&&(!payload||payload.schema_version!==index.required_schema))throw new Error('Archive index schema does not match its reviewed publisher contract');
 if(index.require_complete&&payload.complete!==true)throw new Error('Archive listing is unavailable or incomplete; no partial archive list is certified');
 if(index.publisher_engine&&(payload.publisher_engine!==index.publisher_engine||payload.engine!==index.engine||JSON.stringify(payload.families)!==JSON.stringify(index.patterns)))throw new Error('Archive index engine or family provenance does not match its reviewed contract');
 const rawRows=payload&&payload[index.rows];
 const mapping=rawRows&&typeof rawRows==='object'&&!Array.isArray(rawRows);
 const rows=index.rows_mode==='object_values'?(mapping?Object.values(rawRows):null):index.rows_mode==='object_keys'?(mapping?Object.keys(rawRows):null):rawRows;
 if(!Array.isArray(rows))throw new Error('Archive index row schema unavailable');
 const pattern=new RegExp(index.key_regex),seen=new Set(),out=[];
 for(const row of rows){
  const value=index.key_field==='$value'?row:row&&row[index.key_field];
  if(typeof value!=='string')continue;
  const key=(index.key_prefix||'')+value+(index.key_suffix||'');
  if(key===entry.key||key.includes('..')||!pattern.test(key)||!key.endsWith('.json')||seen.has(key))continue;
  seen.add(key);out.push({engine:index.engine||entry.engine,key,access:'public',inspection_schema:'json-value.v1',indexed_by:entry.key,index_publisher:entry.engine});
 }
 return out;
}
function observeResponses(fetcher,contracts,onRecord,identity){
 return async function(input,init){
  const raw=typeof input==='string'?input:input&&input.url;
  let url;try{url=new URL(raw,global.location&&global.location.href||'https://justhodl.ai/');}catch{}
  const method=String(init&&init.method||input&&input.method||'GET').toUpperCase();
  const entry=url&&contracts.find(c=>c.origin===url.origin&&c.pathname===url.pathname&&(c.methods||['GET']).includes(method)&&Object.entries(c.query||{}).every(([key,value])=>url.searchParams.get(key)===value));
  const ownerBefore=identity();
  const response=await fetcher.apply(this,arguments);
  if(entry&&response.ok&&(!entry.owner_authenticated||ownerBefore.uid)){
   // Observation only: no new request, no request body/header retention, no persistent storage.
   try{response.clone().json().then(payload=>{
    const current=identity();if(current.epoch!==ownerBefore.epoch||current.uid!==ownerBefore.uid)return;
    const query=new URLSearchParams(url.search);['t','v','cb','_','ts'].forEach(k=>query.delete(k));
    onRecord({engine:entry.engine,endpoint:entry.origin+entry.pathname,requestKey:entry.engine+'|'+method+'|'+entry.origin+entry.pathname+'|'+query.toString(),payload,received_at:new Date().toISOString(),owner_authenticated:!!entry.owner_authenticated});
   }).catch(()=>{});}catch{}
  }
  return response;
 };
}
const api={type,columns,leafPaths,ptr,inspect,collectionView,fetchArtifact,validateProjection,observeResponses,indexedOutputs,decodeArtifactResponse};
if(typeof module!=='undefined'&&module.exports)module.exports=api;
global.JHDataInspector=api;
if(typeof document==='undefined')return;
const observed=new Map();let ownerEpoch=0;let repaintObserved=null;let clearArtifact=null;
function currentOwner(){try{const auth=global.JustHodlAuth,user=auth&&auth.getUser&&auth.getUser();return {uid:user&&user.id||null,epoch:ownerEpoch};}catch{return {uid:null,epoch:ownerEpoch};}}
const apiConfig=document.getElementById('jh-api-data-contract');
if(apiConfig){
 let contracts=[];try{contracts=JSON.parse(apiConfig.textContent);}catch{}
 if(Array.isArray(contracts)&&contracts.length)global.fetch=observeResponses(global.fetch.bind(global),contracts,record=>{observed.set(record.requestKey,record);if(repaintObserved)repaintObserved();},currentOwner);
}
function clearOwnerResponses(){ownerEpoch++;observed.clear();if(clearArtifact)clearArtifact();if(repaintObserved)repaintObserved();}
function bindOwnerChanges(){const auth=global.JustHodlAuth;if(auth&&auth.onChange)auth.onChange(clearOwnerResponses);}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',bindOwnerChanges);else bindOwnerChanges();
const style=node('style');style.textContent='.jdi-panel{margin:24px auto;padding:16px;max-width:1200px;border:1px solid #52606d;border-radius:8px;color:inherit;background:var(--jh-panel,#121820);font:13px system-ui}.jdi-panel summary{cursor:pointer;padding:8px}.jdi-controls{display:flex;gap:12px;align-items:center;margin:8px 0}.jdi-panel button,.jdi-panel select,.jdi-panel input{padding:7px;margin:4px;max-width:100%;color:inherit;background:var(--jh-panel,#18212c);border:1px solid #667788}.jdi-scroll{overflow:auto;max-height:70vh}.jdi-table{border-collapse:collapse;width:100%}.jdi-table th,.jdi-table td{border:1px solid #52606d;padding:8px;vertical-align:top;text-align:left}.jdi-value{white-space:pre-wrap;overflow-wrap:anywhere}.jdi-detail{min-width:140px}.jdi-controls button:disabled{opacity:.4}.jdi-null{font-style:italic}.jdi-error{color:#ffb5a6}';document.head.append(style);
async function decodeArtifactResponse(response){
 const bytes=new Uint8Array(await response.arrayBuffer());let decoded=bytes;
 if(bytes[0]===31&&bytes[1]===139){
  if(typeof DecompressionStream==='undefined')throw new Error('Gzip decoding unavailable in this browser');
  decoded=new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))).arrayBuffer());
 }
 return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(decoded));
}
async function install(){
 const route=decodeURI(location.pathname).replace(/^\//,'')||'index.html';const canonical=route.endsWith('/')?route+'index.html':route;
 const engine=new URLSearchParams(location.search).get('engine'),embedded=document.getElementById('jh-page-data-contract');let contract,version='page-data-contract.v1';
 if(embedded&&!(canonical==='engine-data.html'&&engine))contract=JSON.parse(embedded.textContent);
 else{
  const response=await fetch('/config/page-data-contracts.json',{cache:'no-cache'});if(!response.ok)throw new Error('Page data contract unavailable');
  const manifest=await response.json();version=manifest.schema_version;contract=canonical==='engine-data.html'&&engine?manifest.engines[engine]:manifest.pages[canonical];
 }
 if(!contract)return;
 const panel=node('details');panel.className='jdi-panel';panel.id='jh-engine-data';panel.dataset.contractVersion=version;
 panel.append(node('summary',(engine?engine+' · ':'')+'Engine data inspector · '+contract.outputs.length+' outputs'));
 if(!engine&&contract.primary_producers)panel.append(node('p',contract.primary_producers.length?'Primary engine references: '+contract.primary_producers.join(', '):'No primary engine output is declared for this page. Shared context does not establish dedicated-engine coverage.'));
 if(!engine&&contract.page_role==='NO_ENGINE_EXPECTED')panel.append(node('p','This route has no dedicated engine: '+contract.role_reason));
 if(!engine&&contract.page_role!=='NO_ENGINE_EXPECTED'&&contract.primary_output_status==='NO_PRIMARY_OUTPUT_ACCESS_CONTRACT')panel.append(node('p','Primary engine output coverage is unresolved.'));
 if(!engine&&contract.primary_output_status==='PARTIAL_PRIMARY_OUTPUT_ACCESS')panel.append(node('p','Primary output access is partial: '+contract.primary_withheld_output_count+' withheld result paths, '+contract.primary_unresolved_write_count+' unresolved writes, '+contract.primary_unindexed_family_count+' unindexed families, and '+(contract.unresolved_primary_references||[]).length+' unresolved primary references.'));
 if(contract.dedicated_coverage_note)panel.append(node('p',contract.dedicated_coverage_note));
 if(contract.primary_scope_evidence&&Object.keys(contract.primary_scope_evidence).length)panel.append(node('p','Dedicated output scope: '+Object.values(contract.primary_scope_evidence).map(row=>row.purpose).join('; ')));
 if(contract.runtime_outputs&&contract.runtime_outputs.length)panel.append(node('p','Selected outputs are inspected in this page’s data controls: '+contract.runtime_outputs.map(row=>row.engine+' — '+row.scope).join('; ')));
 if(contract.excluded_internal_outputs&&contract.excluded_internal_outputs.length){
  const inventory=node('details');inventory.append(node('summary',contract.excluded_internal_outputs.length+' source-reviewed internal storage entries'));
  inventory.append(node('p','These operational checkpoints, input caches and delivery settings remain withheld. Their purpose and source evidence are listed separately from published analytical results.'));
  inventory.append(collectionView(contract.excluded_internal_outputs,''));panel.append(inventory);
 }
 if(canonical==='engine-data.html')panel.open=true;
 const explanation=node('p','Choose an output to inspect every returned field, nested object and row. Source ownership is checked at build time. Availability and payload coverage are checked when opened.');panel.append(explanation);
 const select=node('select');select.setAttribute('aria-label','Engine output');select.append(node('option','Choose an engine output'));
 for(const o of contract.outputs){const option=node('option',o.engine+' · '+o.key+(o.access==='owner_authenticated'?' · owner sign-in':''));option.value=o.engine+'::'+o.key;select.append(option);}
 const body=node('div');panel.append(select,body);
 if(contract.api_responses&&contract.api_responses.length){
  const apiSection=node('details');apiSection.append(node('summary','Complete API responses · current page session'));
  const apiChoice=node('select');apiChoice.setAttribute('aria-label','Observed engine API response');const apiBody=node('div');apiSection.append(apiChoice,apiBody);
  apiSection.append(node('p','Responses from this page’s existing requests. All returned fields are inspectable; request parameters, headers and bodies are not displayed. A new response replaces the previous response for the same request.'));
  function showObserved(){const record=apiChoice.value===''?null:[...observed.values()][+apiChoice.value];if(record)inspect(apiBody,record.payload,record.engine+' · '+record.endpoint+' · received '+record.received_at);else apiBody.replaceChildren();}
  repaintObserved=()=>{const selected=apiChoice.value;const prompt=node('option','Choose an observed API response');prompt.value='';apiChoice.replaceChildren(prompt);[...observed.values()].forEach((record,i)=>{const option=node('option',record.engine+' · response '+(i+1)+' · '+record.received_at);option.value=String(i);apiChoice.append(option);});if(selected&&+selected<observed.size)apiChoice.value=selected;showObserved();};
  apiChoice.onchange=showObserved;repaintObserved();panel.append(apiSection);
 }

 if(contract.unresolved_count)panel.append(node('p',contract.unresolved_count+' unresolved ownership/output declarations require review; they are not labeled complete.'));
 if(contract.historical_or_dynamic_family_count)panel.append(node('p',contract.historical_or_dynamic_family_count+' historical or dynamic output families. Reviewed archive indexes expose their listed keys when opened; other families remain unresolved.'));
 if(contract.owner_authenticated_count)panel.append(node('p',contract.owner_authenticated_count+' owner outputs require sign-in and are fetched only through the authenticated account service.'));
 if(contract.restricted_count)panel.append(node('p',contract.restricted_count+' internal, sensitive or unapproved paths are withheld from this inspector.'));
 let run=0;clearArtifact=()=>{run++;body.replaceChildren();delete panel.dataset.loadedOutput;delete panel.dataset.loadedLeafPaths;};
 select.addEventListener('change',async()=>{
  const id=++run,entry=contract.outputs.find(o=>o.engine+'::'+o.key===select.value);if(!entry)return;const key=entry.key;
  body.replaceChildren(node('p','Loading '+key+'…'));
  try{
   if(entry.access==='owner_authenticated'&&!global.JustHodlPrivateArtifacts){
    await new Promise((resolve,reject)=>{const script=node('script');script.src='/private-artifacts.js?v=20260909';script.onload=resolve;script.onerror=()=>reject(new Error('Authenticated owner data service unavailable'));document.head.append(script);});
   }
   const ownerAtRequest=currentOwner();
   const r=await fetchArtifact(entry,global.fetch.bind(global),global.JustHodlPrivateArtifacts);if(!r.ok)throw new Error('HTTP '+r.status);
   const data=validateProjection(entry,await decodeArtifactResponse(r));if(id!==run)return;
   const ownerNow=currentOwner();if(entry.access==='owner_authenticated'&&(!ownerAtRequest.uid||ownerAtRequest.uid!==ownerNow.uid||ownerAtRequest.epoch!==ownerNow.epoch))throw new Error('Owner session changed; reload the authenticated output');inspect(body,data,entry.engine+' → '+key+' · retrieved '+new Date().toISOString());
   const enumerated=indexedOutputs(entry,data);
   for(const output of enumerated){if(!contract.outputs.some(o=>o.engine===output.engine&&o.key===output.key)){contract.outputs.push(output);const option=node('option',output.engine+' · '+output.key+' · indexed archive');option.value=output.engine+'::'+output.key;select.append(option);}}
   if(entry.archive_index)body.prepend(node('p',enumerated.length+' source-indexed archive keys are now available in the output selector. This covers the returned index; unindexed storage is not certified complete.'));

   panel.dataset.loadedOutput=key;panel.dataset.loadedLeafPaths=String(leafPaths(data).length);
  }catch(e){if(id!==run)return;const error=node('p','Output unavailable: '+String(e.message));error.className='jdi-error';body.replaceChildren(error);}
 });
 document.body.append(panel);
}
function contractError(){if(document.getElementById('jh-engine-data'))return;const panel=node('div','Engine data contract unavailable. Complete output inspection could not be initialized.');panel.id='jh-engine-data';panel.className='jdi-panel jdi-error';document.body.append(panel);}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>install().catch(contractError));else install().catch(contractError);
})(typeof globalThis!=='undefined'?globalThis:this);
