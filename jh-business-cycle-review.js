/* Complete derived cycle research. No forecast, allocation or execution authority. */
(function(root){
 'use strict';
 const PATHS={current:'/data/global-business-cycle.json',weekly:'/data/global-business-cycle-history.json',monthly:'/data/global-business-cycle-composite-history.json'};
 const UNIVERSE=[{"iso":"USA","name":"United States","region":"North America"},{"iso":"CHN","name":"China","region":"Asia-Pacific"},{"iso":"JPN","name":"Japan","region":"Asia-Pacific"},{"iso":"DEU","name":"Germany","region":"Europe"},{"iso":"IND","name":"India","region":"Asia-Pacific"},{"iso":"GBR","name":"United Kingdom","region":"Europe"},{"iso":"FRA","name":"France","region":"Europe"},{"iso":"ITA","name":"Italy","region":"Europe"},{"iso":"CAN","name":"Canada","region":"North America"},{"iso":"BRA","name":"Brazil","region":"Latin America"},{"iso":"KOR","name":"South Korea","region":"Asia-Pacific"},{"iso":"AUS","name":"Australia","region":"Asia-Pacific"},{"iso":"ESP","name":"Spain","region":"Europe"},{"iso":"MEX","name":"Mexico","region":"Latin America"},{"iso":"IDN","name":"Indonesia","region":"Asia-Pacific"},{"iso":"NLD","name":"Netherlands","region":"Europe"},{"iso":"TUR","name":"Turkey","region":"Europe"},{"iso":"CHE","name":"Switzerland","region":"Europe"},{"iso":"POL","name":"Poland","region":"Europe"},{"iso":"BEL","name":"Belgium","region":"Europe"},{"iso":"SWE","name":"Sweden","region":"Europe"},{"iso":"IRL","name":"Ireland","region":"Europe"},{"iso":"AUT","name":"Austria","region":"Europe"},{"iso":"NOR","name":"Norway","region":"Europe"},{"iso":"ZAF","name":"South Africa","region":"Africa"},{"iso":"DNK","name":"Denmark","region":"Europe"},{"iso":"FIN","name":"Finland","region":"Europe"},{"iso":"CZE","name":"Czech Republic","region":"Europe"},{"iso":"HUN","name":"Hungary","region":"Europe"},{"iso":"CHL","name":"Chile","region":"Latin America"},{"iso":"PRT","name":"Portugal","region":"Europe"},{"iso":"GRC","name":"Greece","region":"Europe"},{"iso":"NZL","name":"New Zealand","region":"Asia-Pacific"},{"iso":"ISR","name":"Israel","region":"Middle East"}]; // Generated from the reviewed native COUNTRY_MAP; tested for exact identity.
 const finite=n=>typeof n==='number'&&Number.isFinite(n);
 const fmt=(n,d=2)=>finite(n)?n.toFixed(d):'Unavailable';
 const text=v=>typeof v==='string'&&v.trim()?v:'Unavailable';
 function date(s){if(typeof s!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(s))return null;const n=Date.parse(s+'T00:00:00Z');return Number.isFinite(n)&&new Date(n).toISOString().slice(0,10)===s?n:null;}
 function period(s){return typeof s==='string'&&/^\d{4}-\d{2}$/.test(s)?date(s+'-01'):null;}
 function clock(s){if(typeof s!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/.test(s)||date(s.slice(0,10))===null)return null;const n=Date.parse(s);return Number.isFinite(n)?n:null;}
 function strictJSON(source){
  // Reject duplicate identities and overflow rather than accepting the last key.
  let i=0;const number=/-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function ws(){while(/[\x20\t\r\n]/.test(source[i]||'x'))i++;}
  function string(){const start=i++;for(;i<source.length;i++){if(source[i]==='\\'){i++;continue;}if(source[i]==='"')return JSON.parse(source.slice(start,++i));}throw Error('Incomplete JSON string');}
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
 function envelope(packet,now=Date.now()){
  if(!packet||typeof packet!=='object'||Array.isArray(packet)||!packet.by_country||typeof packet.by_country!=='object'||Array.isArray(packet.by_country))throw Error('Complete country structure unavailable');
  const at=clock(packet.generated_at);if(at===null||at>now)throw Error('Invalid publication clock');
  for(const [iso,row]of Object.entries(packet.by_country))if(!/^[A-Z]{3}$/.test(iso)||!row||typeof row!=='object'||Array.isArray(row)||(row.iso3!==undefined&&row.iso3!==iso))throw Error('Ambiguous country identity');
  return {at:packet.generated_at,overdue:now-at>26*3600000,authority:false};
 }
 function countryView(packet,now=Date.now()){
  const status=envelope(packet,now);if(packet.countries_total!==UNIVERSE.length)throw Error('Configured country count differs');
  const known=new Map(UNIVERSE.map(r=>[r.iso,r]));
  const rows=Array.from(new Set([...known.keys(),...Object.keys(packet.by_country)])).map(iso=>({iso,reference:known.get(iso)||null,source:packet.by_country[iso]||null}));
  rows.sort((a,b)=>(a.reference?.name||a.iso).localeCompare(b.reference?.name||b.iso));
  return {...status,rows,expected:known.size,present:rows.filter(r=>r.reference&&r.source).length,extras:rows.filter(r=>!r.reference).length};
 }
 function historyView(packet,kind,now=Date.now()){
  const status=envelope(packet,now);if(!['weekly','monthly'].includes(kind))throw Error('Unknown cycle history');
  const key=kind==='weekly'?'date':'period',check=kind==='weekly'?date:period;
  function rows(value){
   if(!Array.isArray(value))throw Error('Complete history rows unavailable');const seen=new Set();
   for(const row of value){if(!row||check(row[key])===null||check(row[key])>clock(status.at)||seen.has(row[key]))throw Error('Ambiguous history period');seen.add(row[key]);}
   return value.slice().sort((a,b)=>a[key].localeCompare(b[key]));
  }
  if(kind==='weekly'&&packet.countries_count!==Object.keys(packet.by_country).length)throw Error('History country count differs');
  const all={GLOBAL:rows(kind==='weekly'?packet.aggregate:packet.global)};
  for(const [iso,row]of Object.entries(packet.by_country)){
   all[iso]=rows(row.history);if(row.n_points!==all[iso].length)throw Error('History row count differs');
   const first=kind==='weekly'?row.first_date:row.first_period,last=kind==='weekly'?row.last_date:row.last_period;
   if(all[iso].length&&(first!==all[iso][0][key]||last!==all[iso][all[iso].length-1][key]))throw Error('History endpoints differ');
  }
  return {...status,all,key,kind,countries:Object.keys(packet.by_country).length,observations:Object.values(all).reduce((sum,r)=>sum+r.length,0)};
 }
 function publicationStatus(packet){
  const c=packet.publication_context;
  if(!c)return 'Legacy publication: complete-output retention is pending the original daily 12:00 UTC run. No provider-original replay or forecast qualification is established.';
  const names=['lambda_function.py','cycle_composite.py','_fred_shim.py','business_cycle_store.py','managed_secret.py'];
  const valid=packet.contract==='global-business-cycle-research.v1'&&['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'].every(k=>packet[k]===false)&&c.contract==='business-cycle-derived-publication.v1'&&c.public_projections_atomic===false&&c.original_source_replay_verified===false&&c.compiler_sha256&&Object.keys(c.compiler_sha256).length===names.length&&names.every(n=>/^[a-f0-9]{64}$/.test(c.compiler_sha256[n]));
  return valid?'Producer declares complete derived-output and predecessor retention. Protected originals have not been replayed by this browser. Source vintages and predictive qualification remain open.':'Publication metadata is inconsistent; retention and qualification are unverified.';
 }
 async function load(kind,options={}){
  if(!Object.hasOwn(PATHS,kind))throw Error('Unknown cycle source');const controller=new AbortController();let timer,reader;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Cycle request timed out'));},options.timeout||20000);});
  try{return await Promise.race([deadline,(async()=>{
   const response=await (options.fetcher||root.fetch.bind(root))(PATHS[kind]+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Cycle source unavailable');reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>64*1024*1024)throw Error('Complete cycle body exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let at=0;for(const part of parts){bytes.set(part,at);at+=part.byteLength;}
   return strictJSON(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 function element(doc,tag,value,parent){const el=doc.createElement(tag);if(value!==undefined)el.textContent=value;if(parent)parent.appendChild(el);return el;}
 function raw(v){return v===undefined||v===null?'Unavailable':typeof v==='object'?JSON.stringify(v):String(v);}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),head=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const name of headers){const th=element(doc,'th',name,head);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const v of row)element(doc,'td',v,tr);}}
 function series(doc,kind,view,iso){
  const rows=view.all[iso]||[],key=view.key,field=iso==='GLOBAL'&&kind==='weekly'?'global_avg_cli':'cli';
  const fields=Array.from(new Set([key,...rows.flatMap(r=>Object.keys(r))]));
  table(doc,doc.getElementById(kind+'-table'),fields.map(f=>f===field?'Synthetic index points':f),rows.map(r=>fields.map(f=>raw(r[f]))));
  doc.getElementById(kind+'-rows').textContent=rows.length+' stored rows for '+iso+' · '+(rows[0]?.[key]||'no first period')+' to '+(rows[rows.length-1]?.[key]||'no last period');
  const svg=doc.getElementById(kind+'-chart');svg.replaceChildren();const valid=rows.filter(r=>finite(r[field]));if(!valid.length)return;
  const pointDate=r=>kind==='weekly'?date(r[key]):period(r[key]);const xs=valid.map(pointDate),ys=valid.map(r=>r[field]),xmin=xs.reduce((a,b)=>Math.min(a,b),Infinity),xmax=xs.reduce((a,b)=>Math.max(a,b),-Infinity),ymin=ys.reduce((a,b)=>Math.min(a,b),Infinity),ymax=ys.reduce((a,b)=>Math.max(a,b),-Infinity);
  const add=(tag,attrs,label)=>{const n=doc.createElementNS('http://www.w3.org/2000/svg',tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);if(label!==undefined)n.textContent=label;svg.appendChild(n);return n;};
  add('title',{},iso+' synthetic '+kind+' index; stored points without connecting missing observations');add('line',{x1:55,x2:950,y1:160,y2:160,stroke:'#667898'});
  for(const row of valid){const n=add('circle',{cx:55+(pointDate(row)-xmin)/(xmax-xmin||1)*895,cy:145-(row[field]-ymin)/(ymax-ymin||1)*115,r:2,fill:'#9dbafa'});const title=doc.createElementNS('http://www.w3.org/2000/svg','title');title.textContent=row[key]+': '+row[field]+' synthetic points';n.appendChild(title);}
  add('text',{x:4,y:30,fill:'#b2c0d9','font-size':12},fmt(ymax,1));add('text',{x:4,y:150,fill:'#b2c0d9','font-size':12},fmt(ymin,1));add('text',{x:55,y:186,fill:'#b2c0d9','font-size':12},valid[0][key]);add('text',{x:950,y:186,fill:'#b2c0d9','text-anchor':'end','font-size':12},valid[valid.length-1][key]);
 }
 async function mount(doc,loader=load,now=Date.now()){
  const say=(id,v)=>{const el=doc.getElementById(id);if(el)el.textContent=v;};
  if(doc.getElementById('country-table')){
   try{
    const packet=await loader('current'),view=countryView(packet,now);let query='',region='All';
    say('publication',view.at+' · '+(view.overdue?'at page load: publication overdue (>26h)':'at page load: publication less than 26h old')+' · underlying observation freshness unverified');
    say('coverage',view.present+' / '+view.expected);say('gaps',(view.expected-view.present)+' missing · '+view.extras+' unregistered');say('preservation',publicationStatus(packet));
    say('feature-clock',text(packet.composite?.features_generated_at));say('raw-current',JSON.stringify(packet,null,2));
    const regionSelect=doc.getElementById('region');regionSelect.replaceChildren();
    for(const name of ['All',...new Set(view.rows.map(r=>r.reference?.region||'Unregistered'))]){const option=element(doc,'option',name,regionSelect);option.value=name;}
    function render(){const rows=view.rows.filter(r=>(region==='All'||r.reference?.region===region)&&((r.reference?.name||'')+' '+r.iso).toLowerCase().includes(query.toLowerCase()));
     table(doc,doc.getElementById('country-table'),['Country','Region','Coverage','Synthetic index points','Model classification (not official cycle)','Model basis','Equity source label','Equity last date','Composite period','Coverage/agreement heuristic (not probability)'],rows.map(r=>{const s=r.source;return [(r.reference?.name||'Unregistered')+' · '+r.iso,r.reference?.region||'Unregistered',s?'Source fields present; definitions unqualified':'Missing country',fmt(s?.cli_level),text(s?.phase),text(s?.phase_basis),text(s?.source),date(s?.latest_date)!==null?s.latest_date:'Unavailable',period(s?.composite?.as_of)!==null?s.composite.as_of:'Unavailable',fmt(s?.composite?.confidence)];}));
     say('country-filter-status',rows.length+' of '+view.rows.length+' countries shown. Labels describe the model, not investment opportunity.');}
    regionSelect.addEventListener('change',e=>{region=e.target.value;render();});doc.getElementById('country-search').addEventListener('input',e=>{query=e.target.value;render();});render();
    const select=doc.getElementById('component-country');select.replaceChildren();for(const row of view.rows){const option=element(doc,'option',(row.reference?.name||row.iso)+' · '+row.iso,select);option.value=row.iso;}
    function components(iso){const row=packet.by_country[iso],items=Array.isArray(row?.components)?row.components:[],fields=Array.from(new Set(items.flatMap(r=>Object.keys(r))));table(doc,doc.getElementById('component-table'),fields,items.map(r=>fields.map(k=>raw(r[k]))));say('component-status',items.length+' complete feature rows for '+iso+'; units, source labels and processing periods are producer declarations.');say('raw-country',JSON.stringify(row||{status:'missing_country'},null,2));}
    select.addEventListener('change',e=>components(e.target.value));components(view.rows[0].iso);
   }catch(e){for(const id of ['coverage','gaps','feature-clock'])say(id,'Unavailable');for(const id of ['country-table','component-table'])doc.getElementById(id).replaceChildren();say('publication','Current research unavailable: '+e.message);say('preservation','No current retention claim can be verified.');say('raw-current','Unavailable');say('raw-country','Unavailable');say('component-status','Unavailable');}
  }
  await Promise.all(['weekly','monthly'].filter(kind=>doc.getElementById(kind+'-status')).map(async kind=>{
   try{const packet=await loader(kind),view=historyView(packet,kind,now);say(kind+'-status',view.at+' · '+view.countries+' countries · '+view.observations+' complete stored country + global rows · '+(view.overdue?'at page load: publication overdue (>26h)':'at page load: publication less than 26h old'));
    say(kind+'-raw',JSON.stringify(packet,null,2));const select=doc.getElementById(kind+'-country');select.replaceChildren();
    for(const iso of Object.keys(view.all).sort((a,b)=>a==='GLOBAL'?-1:b==='GLOBAL'?1:a.localeCompare(b))){const name=UNIVERSE.find(r=>r.iso===iso)?.name||iso;const option=element(doc,'option',name+' · '+iso,select);option.value=iso;}
    select.addEventListener('change',e=>series(doc,kind,view,e.target.value));series(doc,kind,view,'GLOBAL');
   }catch(e){say(kind+'-status','History unavailable: '+e.message);say(kind+'-rows','Unavailable');say(kind+'-raw','Unavailable');for(const suffix of ['table','chart','country'])doc.getElementById(kind+'-'+suffix).replaceChildren();}
  }));
 }
 const api={PATHS,UNIVERSE,finite,fmt,date,period,clock,strictJSON,envelope,countryView,historyView,publicationStatus,load,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHBusinessCycleReview=api;if(root.document)mount(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
