/* Complete recession research. No forecast or portfolio authority. */
(function(root){
 'use strict';
 const PATH='/data/global-recession.json';
 const INPUTS=['data/global-business-cycle.json','data/oecd-cli.json','data/portwatch.json','data/china-liquidity.json','data/indicator-bus.json'];
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
 function view(packet,now=Date.now()){
  if(!packet||typeof packet!=='object'||Array.isArray(packet)||!Array.isArray(packet.countries)||!Array.isArray(packet.excluded))throw Error('Complete country structure unavailable');
  const at=clock(packet.generated_at);if(at===null||at>now)throw Error('Invalid publication clock');
  if(packet.coverage?.n_countries_scored!==packet.countries.length||packet.coverage?.n_excluded!==packet.excluded.length)throw Error('Country coverage counts differ');
  const known=new Map(UNIVERSE.map(r=>[r.iso,r])),included=new Map(),excluded=new Map();
  for(const [array,destination]of [[packet.countries,included],[packet.excluded,excluded]])for(const row of array){
   if(!row||typeof row!=='object'||Array.isArray(row)||!/^[A-Z]{3}$/.test(row.iso3)||included.has(row.iso3)||excluded.has(row.iso3))throw Error('Ambiguous country identity');
   destination.set(row.iso3,row);
  }
  const rows=Array.from(new Set([...known.keys(),...included.keys(),...excluded.keys()])).map(iso=>({iso,reference:known.get(iso)||null,source:included.get(iso)||null,excluded:excluded.get(iso)||null}));
  rows.sort((a,b)=>(a.reference?.name||a.iso).localeCompare(b.reference?.name||b.iso));
  return {rows,at:packet.generated_at,overdue:now-at>26*3600000,present:included.size,excluded:excluded.size,missing:rows.filter(r=>!r.source&&!r.excluded).length,authority:false};
 }
 function preservation(packet){
  const c=packet.publication_context;
  if(!c)return 'Legacy publication: input retention and the single-write contract await the original daily 12:40 UTC run. Earlier original FRED responses cannot be reconstructed.';
  const names=['lambda_function.py','recession_research.py'];
  const ref=c.acquisition_manifest;
  const valid=packet.contract==='global-recession-research.v1'&&['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'].every(k=>packet[k]===false)&&c.contract==='recession-retained-publication.v1'&&c.single_conditional_head_write===true&&c.original_source_replay_verified===false&&c.compiler_sha256&&Object.keys(c.compiler_sha256).length===names.length&&names.every(k=>/^[a-f0-9]{64}$/.test(c.compiler_sha256[k]))&&ref&&/^[a-f0-9]{64}$/.test(ref.sha256)&&ref.key==='audit-private/20260909-originals/global-recession-research/'+ref.sha256+'.bin'&&Number.isInteger(ref.bytes)&&ref.bytes>0;
  return valid?'Producer declares whole input and calculation retention with one conditional publication. Protected originals have not been independently replayed by this browser. Forecast and point-in-time qualification remain open.':'Inconsistent publication metadata: retention remains unverified.';
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
 function raw(v){return v===undefined||v===null?'Unavailable':typeof v==='object'?JSON.stringify(v):String(v);}
 function table(doc,id,headers,rows){const host=doc.getElementById(id);host.replaceChildren();const t=el(doc,'table',undefined,host),head=el(doc,'tr',undefined,el(doc,'thead',undefined,t));for(const h of headers){const n=el(doc,'th',h,head);n.scope='col';}const body=el(doc,'tbody',undefined,t);for(const row of rows){const tr=el(doc,'tr',undefined,body);for(const v of row)el(doc,'td',v,tr);}}
 function measuredDate(value,now){const n=date(value);return n!==null&&n<=now?value:'Unavailable / invalid date';}
 async function mount(doc,loader=load,now=Date.now()){
  const say=(id,value)=>{const n=doc.getElementById(id);if(n)n.textContent=value;};
  try{
   const packet=await loader(),v=view(packet,now);let query='',region='All';
   say('recession-publication',v.at+' · at page load: '+(v.overdue?'publication overdue (>26h)':'publication less than 26h old')+' · observation freshness is separate');
   say('recession-coverage',v.present+' calculated · '+v.excluded+' excluded');say('recession-gaps',v.missing+' missing configured countries');
   say('recession-preservation',preservation(packet));say('recession-raw',JSON.stringify(packet,null,2));
   const us=packet.us_crosscheck||{},curve=us.yield_curve_probit||{},sahm=us.sahm_rule||{};
   say('recession-spread',fmt(curve.t10y3m_spread_pp)+' pp');say('recession-spread-date',measuredDate(curve.as_of,now)+' · daily T10Y3M');
   say('recession-sahm',fmt(sahm.value)+' pp');say('recession-sahm-date',measuredDate(sahm.as_of,now)+' · monthly SAHMCURRENT; date denotes observation period');
   const regionSelect=doc.getElementById('recession-region');regionSelect.replaceChildren();
   for(const name of ['All',...new Set(v.rows.map(r=>r.reference?.region||'Unregistered'))]){const option=el(doc,'option',name,regionSelect);option.value=name;}
   function render(){const rows=v.rows.filter(r=>(region==='All'||r.reference?.region===region)&&((r.reference?.name||'')+' '+r.iso).toLowerCase().includes(query.toLowerCase()));
    table(doc,'recession-countries',['Country','Region','Coverage','Synthetic model label','Synthetic index points','6m index change (%)','Distance to 200d trend (%)','Configured weight','Equity observation date'],rows.map(r=>{const s=r.source;return [(r.reference?.name||'Unregistered')+' · '+r.iso,r.reference?.region||'Unregistered',s?'Calculated; unqualified':r.excluded?'Excluded: '+text(r.excluded.reason):'Missing',text(s?.phase),fmt(s?.cli_level),fmt(s?.six_month_change),fmt(s?.dist_200ma_pct),fmt((s||r.excluded)?.gdp_weight),measuredDate(s?.latest_date,now)];}));
    say('recession-filter-status',rows.length+' of '+v.rows.length+' countries shown. Alphabetical order; no investment ranking.');
   }
   doc.getElementById('recession-search').oninput=e=>{query=e.target.value;render();};regionSelect.onchange=e=>{region=e.target.value;render();};render();
   const select=doc.getElementById('recession-country');select.replaceChildren();for(const r of v.rows){const option=el(doc,'option',(r.reference?.name||r.iso)+' · '+r.iso,select);option.value=r.iso;}
   function inspect(iso){const row=v.rows.find(r=>r.iso===iso),source=row?.source||row?.excluded||{status:'missing'};table(doc,'recession-components',['Field','Complete retained diagnostic value'],Object.entries(source).map(([k,value])=>[k,raw(value)]));say('recession-selected',iso+' · all '+Object.keys(source).length+' fields; inherited scores and agreement claims are unqualified diagnostics.');}
   select.onchange=e=>inspect(e.target.value);if(v.rows.length)inspect(v.rows[0].iso);
   const regions=packet.by_region&&typeof packet.by_region==='object'&&!Array.isArray(packet.by_region)?packet.by_region:{};
   table(doc,'recession-regions',['Region','Calculated countries','Configured weight'],Object.entries(regions).sort(([a],[b])=>a.localeCompare(b)).map(([name,r])=>[name,raw(r.n_countries),fmt(r.gdp_weight)]));
   const versions=Array.isArray(packet.publication_context?.input_versions)?packet.publication_context.input_versions:[];
   if(versions.length&&(versions.length!==INPUTS.length||new Set(versions.map(r=>r.path)).size!==INPUTS.length||versions.some(r=>!INPUTS.includes(r.path))))throw Error('Derived source identities differ');
   table(doc,'recession-sources',['Derived packet','Retention state','Producer publication clock','Stored object clock','Acquisition clock','Complete SHA-256'],INPUTS.map(path=>{const r=versions.find(v=>v.path===path);return [path,r?text(r.status):'Not supplied by this legacy packet',text(r?.producer_generated_at),text(r?.last_modified),text(r?.acquired_at),text(r?.original?.sha256)];}));
  }catch(error){
   say('recession-publication','Research unavailable: '+error.message);say('recession-preservation','Unverified');
   for(const id of ['coverage','gaps','spread','spread-date','sahm','sahm-date','raw','filter-status','selected'])say('recession-'+id,'Unavailable');
   for(const id of ['countries','components','regions','sources','country','region'])doc.getElementById('recession-'+id).replaceChildren();
   doc.getElementById('recession-search').oninput=null;doc.getElementById('recession-region').onchange=null;doc.getElementById('recession-country').onchange=null;
  }
 }
 const api={PATH,INPUTS,UNIVERSE,strictJSON,finite,fmt,date,clock,view,preservation,load,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHRecessionReview=api;if(root.document)mount(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
