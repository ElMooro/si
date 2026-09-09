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
const api={type,columns,leafPaths,ptr,inspect,collectionView};
if(typeof module!=='undefined'&&module.exports)module.exports=api;
global.JHDataInspector=api;
if(typeof document==='undefined')return;
const style=node('style');style.textContent='.jdi-panel{margin:24px auto;padding:16px;max-width:1200px;border:1px solid #52606d;border-radius:8px;color:inherit;background:var(--jh-panel,#121820);font:13px system-ui}.jdi-panel summary{cursor:pointer;padding:8px}.jdi-controls{display:flex;gap:12px;align-items:center;margin:8px 0}.jdi-panel button,.jdi-panel select,.jdi-panel input{padding:7px;margin:4px;max-width:100%;color:inherit;background:var(--jh-panel,#18212c);border:1px solid #667788}.jdi-scroll{overflow:auto;max-height:70vh}.jdi-table{border-collapse:collapse;width:100%}.jdi-table th,.jdi-table td{border:1px solid #52606d;padding:8px;vertical-align:top;text-align:left}.jdi-value{white-space:pre-wrap;overflow-wrap:anywhere}.jdi-detail{min-width:140px}.jdi-controls button:disabled{opacity:.4}.jdi-null{font-style:italic}.jdi-error{color:#ffb5a6}';document.head.append(style);
async function install(){
 const route=decodeURI(location.pathname).replace(/^\//,'')||'index.html';const canonical=route.endsWith('/')?route+'index.html':route;
 const response=await fetch('/config/page-data-contracts.json',{cache:'no-cache'});if(!response.ok)throw new Error('Page data contract unavailable');
 const manifest=await response.json(),engine=new URLSearchParams(location.search).get('engine');
 const contract=canonical==='engine-data.html'&&engine?manifest.engines[engine]:manifest.pages[canonical];if(!contract)return;
 const panel=node('details');panel.className='jdi-panel';panel.id='jh-engine-data';panel.dataset.contractVersion=manifest.schema_version;
 panel.append(node('summary',(engine?engine+' · ':'')+'Complete engine data · '+contract.outputs.length+' public outputs'));
 if(canonical==='engine-data.html')panel.open=true;
 const explanation=node('p','Choose an output to inspect every returned field, nested object and row. Source ownership is checked at build time. Availability and payload coverage are checked when opened.');panel.append(explanation);
 const select=node('select');select.setAttribute('aria-label','Engine output');select.append(node('option','Choose an engine output'));
 for(const o of contract.outputs){const option=node('option',o.engine+' · '+o.key);option.value=o.key;select.append(option);}
 const body=node('div');panel.append(select,body);
 if(contract.unresolved_count)panel.append(node('p',contract.unresolved_count+' unresolved ownership/output declarations require review; they are not labeled complete.'));
 if(contract.historical_or_dynamic_family_count)panel.append(node('p',contract.historical_or_dynamic_family_count+' historical or dynamic output families need an enumerated archive index; completeness is not assumed.'));
 if(contract.restricted_count)panel.append(node('p',contract.restricted_count+' internal or sensitive outputs require their authenticated owner view.'));
 let run=0;
 select.addEventListener('change',async()=>{
  const id=++run,key=select.value,entry=contract.outputs.find(o=>o.key===key);if(!entry)return;
  body.replaceChildren(node('p','Loading '+key+'…'));
  try{const r=await fetch('/'+key,{cache:'no-store',credentials:'same-origin'});if(!r.ok)throw new Error('HTTP '+r.status);
   const data=await r.json();if(id!==run)return;inspect(body,data,entry.engine+' → '+key+' · retrieved '+new Date().toISOString());
   panel.dataset.loadedOutput=key;panel.dataset.loadedLeafPaths=String(leafPaths(data).length);
  }catch(e){if(id!==run)return;const error=node('p','Output unavailable: '+String(e.message));error.className='jdi-error';body.replaceChildren(error);}
 });
 document.body.append(panel);
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>install().catch(()=>{}));else install().catch(()=>{});
})(typeof globalThis!=='undefined'?globalThis:this);
