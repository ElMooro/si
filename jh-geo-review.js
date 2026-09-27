/* Dated news observations. Publication freshness never grants portfolio authority. */
(function(root){
 'use strict';
 const PATH='/data/geopolitical-risk.json',CONTRACT='geopolitical-news-research.v1';
 const COUNTRIES=['China','Egypt','France','Germany','India','Iran','Israel','Japan','Lebanon','N.Korea','Pakistan','Russia','S.Korea','Saudi','Syria','Taiwan','Turkey','UK','US','Ukraine','Venezuela','Yemen'];
 const finite=n=>typeof n==='number'&&Number.isFinite(n);
 const fmt=n=>finite(n)?String(n):'Unavailable';
 function clock(s){if(typeof s!=='string'||s.length>32||!/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$/.test(s))return null;const day=Date.parse(s.slice(0,10)+'T00:00:00Z'),n=Date.parse(s);return Number.isFinite(day)&&new Date(day).toISOString().slice(0,10)===s.slice(0,10)&&Number.isFinite(n)?n:null;}
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
 function context(packet){const p=packet&&typeof packet==='object'&&!Array.isArray(packet)?packet:{};return {...p,top_country:null,global_temp:null,escalating:[],gssi_cross:{...(p.gssi_cross||{}),rows:[],news_leads:[],priced_in:[]},research_context:{status:'unqualified_news_observations',portfolio_action:'WAIT',meaning:'abstain'}};}
 function view(p,now=Date.now()){
  if(!p||typeof p!=='object'||!Array.isArray(p.rankings))throw Error('Complete country observations unavailable');
  const at=clock(p.generated_at);if(at===null||at>now)throw Error('Invalid publication clock');
  const native=p.contract===CONTRACT;
  if(p.contract&&!native)throw Error('Unknown research contract');
  const countries=new Map();for(const row of p.rankings){if(!row||!COUNTRIES.includes(row.country)||countries.has(row.country))throw Error('Ambiguous country population');countries.set(row.country,row);}
  const entries=new Map(),feeds=new Map();
  if(native){
   if(['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'].some(k=>p[k]!==false)||p.portfolio_action!=='WAIT')throw Error('Unsupported permission declaration');
   if(countries.size!==COUNTRIES.length||!Array.isArray(p.entries)||!Array.isArray(p.feeds))throw Error('Incomplete native population');
   for(const f of p.feeds){if(!f||typeof f.feed_id!=='string'||feeds.has(f.feed_id))throw Error('Ambiguous feed identity');feeds.set(f.feed_id,f);}
   for(const e of p.entries){if(!e||typeof e.entry_id!=='string'||entries.has(e.entry_id)||!feeds.has(e.feed_id))throw Error('Ambiguous article identity');entries.set(e.entry_id,e);}
   if(p.sources?.feeds_in_corpus!==feeds.size||p.sources?.feeds_attempted!==feeds.size||p.sources?.articles_scanned!==entries.size)throw Error('Complete acquisition counts differ');
   if([...feeds.values()].some(f=>!Number.isSafeInteger(f.parsed_entries)||f.parsed_entries<0)||[...feeds.values()].reduce((n,f)=>n+f.parsed_entries,0)!==entries.size||p.sources.feeds_responding!==[...feeds.values()].filter(f=>f.status==='parsed').length)throw Error('Complete parsed-feed coverage differs');
   for(const r of countries.values()){
    if(!Array.isArray(r.entry_ids)||new Set(r.entry_ids).size!==r.entry_ids.length||r.entry_ids.some(id=>!entries.has(id)))throw Error('Country evidence references differ');
    for(const k of ['mentions_24h','mentions_48h','raw_mentions_24h','raw_mentions_48h','crisis_hits','market_hits'])if(!Number.isSafeInteger(r[k])||r[k]<0)throw Error('Invalid measurement count');
    if(r.crisis_share!==null&&(!finite(r.crisis_share)||r.crisis_share<0||r.crisis_share>1))throw Error('Invalid measured share');
    if(r.raw_mentions_48h!==r.entry_ids.length||r.mentions_24h>r.mentions_48h||r.mentions_48h>r.raw_mentions_48h||r.raw_mentions_24h>r.raw_mentions_48h||r.crisis_hits>r.mentions_48h||r.market_hits>r.mentions_48h||r.crisis_share!==(r.mentions_48h?r.crisis_hits/r.mentions_48h:null))throw Error('Measurement denominators differ');
   }
  }
  return {at:p.generated_at,overdue:now-at>26*3600000,native,authority:false,entries,feeds,rows:COUNTRIES.map(country=>({country,source:countries.get(country)||null}))};
 }
 async function load(options={}){
  const controller=new AbortController();let timer,reader;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Research request timed out'));},options.timeout||20000);});
  try{return await Promise.race([deadline,(async()=>{
   const response=await(options.fetcher||root.fetch.bind(root))(PATH+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Research source unavailable');reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>64*1024*1024)throw Error('Complete body exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.byteLength;}
   return strictJSON(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 function el(doc,tag,value,parent){const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;if(parent)parent.appendChild(n);return n;}
 function table(doc,id,headers,rows){const host=doc.getElementById(id);host.replaceChildren();const t=el(doc,'table',undefined,host),tr=el(doc,'tr',undefined,el(doc,'thead',undefined,t));for(const h of headers){const th=el(doc,'th',h,tr);th.scope='col';}const body=el(doc,'tbody',undefined,t);for(const row of rows){const tr=el(doc,'tr',undefined,body);for(const item of row)el(doc,'td',item,tr);}}
 function preservation(p){
  if(p.contract!==CONTRACT)return 'Legacy packet: publisher dates, duplicate-title handling and original-response replay are unverified. The next normal producer run is daily at 11:30 UTC. No legacy score or market-pricing label is promoted.';
  const c=p.publication_context||{};
  if(c.contract!=='geopolitical-news-publication.v1'||c.publication_atomic!==false||c.original_source_replay_verified!==false||c.point_in_time_verified!==false)return 'Inconsistent preservation declaration; no replay or forecast qualification established.';
  const ref=r=>r&&/^[a-f0-9]{64}$/.test(r.sha256)&&r.key==='audit-private/20260909-originals/geopolitical-risk-research/'+r.sha256+'.bin'&&Number.isSafeInteger(r.bytes)&&r.bytes>0&&r.bytes<=64*1024*1024;
  const names=['geo_feeds.json','geo_news_model.py','geo_news_store.py','lambda_function.py'];
  if(![c.manifest,c.calculation,c.planned_complete_history].every(ref)||JSON.stringify(Object.keys(c.compiler_sha256||{}).sort())!==JSON.stringify(names)||!Object.values(c.compiler_sha256).every(h=>/^[a-f0-9]{64}$/.test(h)))return 'Inconsistent complete-retention identity; no replay or forecast qualification established.';
  return 'Producer declares complete original-response, calculation and history retention. This browser has not replayed protected originals. History and headline publish conditionally as separate objects; they are not atomic. Historical vintage availability and investment performance remain unqualified.';
 }
 async function mount(doc,loader=load,now){
  if(!doc.getElementById('geo-review'))return;
  const generation=(doc._geoGeneration||0)+1;doc._geoGeneration=generation;
  const say=(id,value)=>{const n=doc.getElementById(id);if(n)n.textContent=value;};
  const current=()=>now===undefined?Date.now():now;
  const clear=()=>{for(const id of ['geo-countries','geo-feeds','geo-entries'])doc.getElementById(id).replaceChildren();for(const id of ['geo-country','geo-search']){const n=doc.getElementById(id);n.onchange=n.oninput=null;}for(const id of ['geo-coverage','geo-entry-count','geo-raw','geo-entry-status'])say(id,'Unavailable');doc.getElementById('geo-country').replaceChildren();doc.getElementById('geo-next').onclick=doc.getElementById('geo-prev').onclick=null;};
  clear();say('geo-status','Reading the complete public packet…');
  try{
   const packet=await loader();if(doc._geoGeneration!==generation)return;const v=view(packet,current());
   say('geo-status',v.at+' · at page load: '+(v.overdue?'publication overdue (>26h)':'publication less than 26h old')+' · publisher dates and source coverage remain separate.');
   say('geo-mode',v.native?'Dated title-group observations':'Legacy observations — dates and count definitions unverified');
   say('geo-preservation',preservation(packet));say('geo-raw',JSON.stringify(packet,null,2));
   say('geo-coverage',v.native?packet.sources.feeds_responding+' parsed / '+v.feeds.size+' attempted':fmt(packet.sources?.feeds_responding)+' responding / '+fmt(packet.sources?.feeds_in_corpus)+' configured; attempts unreported');
   say('geo-entry-count',v.native?v.entries.size+' total entries; '+fmt(packet.sources.dated_title_groups_48h)+' dated title groups in 48h':fmt(packet.sources?.articles_scanned)+' legacy scanned entries');
   let query='',selected=COUNTRIES[0],page=0;
   function countryRows(){const selected=v.rows.filter(r=>r.country.toLowerCase().includes(query.toLowerCase()));table(doc,'geo-countries',['Country',v.native?'24h title groups':'Legacy 24h count',v.native?'48h title groups':'Legacy 48h count','Raw 48h entries','Conflict-term groups','Term share','First / last included publisher date'],selected.map(({country,source:r})=>[country,fmt(r?.mentions_24h),fmt(r?.mentions_48h),v.native?fmt(r?.raw_mentions_48h):'Unverified',fmt(r?.crisis_hits),v.native&&finite(r?.crisis_share)?(100*r.crisis_share).toFixed(1)+'%':'Unavailable / legacy',v.native?(r?.first_observed_publication||'None observed')+' / '+(r?.last_observed_publication||'None observed'):'Unverified']));say('geo-country-count',selected.length+' of '+v.rows.length+' countries shown. Alphabetical; no risk ranking.');}
   function entryRows(){const row=v.rows.find(r=>r.country===selected)?.source;const all=v.native?(row?.entry_ids||[]).map(id=>v.entries.get(id)):(row?.headlines||[]).map(h=>({title:h.title,source:h.src,published_at:null,window_status:'legacy_date_unverified'}));const start=page*50,shown=all.slice(start,start+50);table(doc,'geo-entries',['Publisher / feed','Complete headline','Publisher date','Date-window status'],shown.map(e=>[v.native?v.feeds.get(e.feed_id)?.name||'Unavailable':e.source||'Unavailable',typeof e.title==='string'?e.title:'Unavailable',e.published_at||'Unverified / unavailable',e.window_status]));say('geo-entry-status',all.length?(start+1)+'–'+(start+shown.length)+' of '+all.length+' matching raw entries for '+selected:'No included dated entries observed for '+selected+'; source failures and undated entries remain in the complete record.');doc.getElementById('geo-prev').disabled=page===0;doc.getElementById('geo-next').disabled=start+50>=all.length;}
   const select=doc.getElementById('geo-country');for(const country of COUNTRIES){const option=el(doc,'option',country,select);option.value=country;}
   select.onchange=e=>{selected=e.target.value;page=0;entryRows();};doc.getElementById('geo-search').oninput=e=>{query=e.target.value;countryRows();};
   doc.getElementById('geo-next').onclick=()=>{page++;entryRows();};doc.getElementById('geo-prev').onclick=()=>{page=Math.max(0,page-1);entryRows();};
   countryRows();entryRows();
   table(doc,'geo-feeds',['Feed','Acquisition state','HTTP status','Acquired at','Parsed entries','Complete original SHA-256'],v.native?[...v.feeds.values()].map(f=>[f.name,f.status,fmt(f.http_status),f.acquired_at,fmt(f.parsed_entries),f.body_sha256||'No complete response']):[['Legacy feed inventory unavailable','Current packet does not identify individual acquisition attempts','Unavailable','Unavailable','Unavailable','Unavailable']]);
  }catch(e){if(doc._geoGeneration!==generation)return;clear();say('geo-status','Research unavailable: '+e.message);say('geo-mode','Unavailable');say('geo-preservation','No current measurements or risk conclusion substituted.');}
 }
 const api={PATH,CONTRACT,COUNTRIES,clock,fmt,strictJSON,context,view,load,preservation,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHGeoResearch=api;if(root.document?.getElementById('geo-review')){mount(root.document);root.document.getElementById('geo-refresh').onclick=()=>mount(root.document);}}
})(typeof globalThis!=='undefined'?globalThis:this);
