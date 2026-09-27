/* Complete calendar shipping observations. Retention does not grant investment authority. */
(function(root){
 'use strict';
 const PATH='/data/portwatch.json',CONTRACT='portwatch-calendar-measurements.v1';
 const units={choke:['n_total','transit_calls'],ports:['portcalls','port_calls'],choke_fallback:['portcalls','port_calls_fallback_not_transit_calls']};
 const finite=n=>typeof n==='number'&&Number.isFinite(n),integer=n=>Number.isSafeInteger(n)&&n>=0;
 const fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumFractionDigits:2}):'Unavailable';
 function day(s){return typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s+'T00:00:00Z'))&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s;}
 function clock(s){if(typeof s!=='string'||s.length>32||!/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$/.test(s)||!day(s.slice(0,10)))return null;const n=Date.parse(s);return Number.isFinite(n)?n:null;}
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
 function checkWindow(w,n){
  if(!w||w.expected_days!==n||!integer(w.available_days)||w.available_days>n||!Array.isArray(w.missing_dates)||w.missing_dates.some(d=>!day(d))||new Set(w.missing_dates).size!==w.missing_dates.length)throw Error('Invalid calendar coverage');
  if(w.start===null&&w.end===null){if(w.status!=='no_compatible_calendar_anchor'||w.mean!==null||w.sum!==null||w.available_days!==0||w.missing_dates.length!==0)throw Error('Invalid unavailable window');return;}
  if(!day(w.start)||!day(w.end)||Date.parse(w.end)-Date.parse(w.start)!==(n-1)*86400000)throw Error('Mismatched calendar window');
  if(w.missing_dates.length!==n-w.available_days||w.missing_dates.some(d=>d<w.start||d>w.end))throw Error('Incomplete calendar denominator');
  if(w.status==='complete'){if(w.available_days!==n||!finite(w.mean)||w.mean<0||!integer(w.sum)||Math.abs(w.mean*n-w.sum)>1e-9*Math.max(1,w.sum))throw Error('Invalid complete window');}
  else if(!['missing_observations','outside_exact_json_range'].includes(w.status)||w.mean!==null||w.sum!==null)throw Error('Unqualified partial window');
 }
 function view(p,now=Date.now()){
  if(!p||typeof p!=='object'||Array.isArray(p)||!Array.isArray(p.chokepoints)||!Array.isArray(p.ports))throw Error('Complete shipping packet unavailable');
  const at=clock(p.generated_at);if(at===null||at>now)throw Error('Invalid publication clock');
  const review=p.measurement_review,native=!!review,ids=new Set();let rows=[];
  if(native){
   if(p.contract!=='portwatch-preserved-calculation.v1'||review.contract!==CONTRACT||review.calculation_at!==p.generated_at||['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'].some(k=>p[k]!==false)||p.portfolio_action!=='WAIT'||review.forecast_qualified!==false||review.sizing_eligible!==false)throw Error('Unsupported measurement permission');
   if(!Array.isArray(review.entities)||review.entity_count!==review.entities.length||!integer(review.history_rows))throw Error('Incomplete retained population');
   let total=0;
   for(const r of review.entities){
    const definition=units[r.family],key=r.family+':'+r.entity_id;
    if(!definition||typeof r.entity_id!=='string'||!r.entity_id||ids.has(key)||r.source_field!==definition[0]||r.unit!==definition[1]||!Array.isArray(r.observations)||r.observation_rows!==r.observations.length||!Array.isArray(r.names)||!Array.isArray(r.countries)||[...r.names,...r.countries].some(x=>typeof x!=='string'))throw Error('Ambiguous retained entity');
    ids.add(key);total+=r.observations.length;const keys=new Set();
    for(const o of r.observations){if(!o||typeof o.row_key!=='string'||!o.row_key.startsWith(r.entity_id+'|')||keys.has(o.row_key)||o.source_field!==r.source_field||!['observed','missing_or_invalid_count','future_observation','invalid_date_or_entity_identity'].includes(o.status))throw Error('Invalid observation identity');keys.add(o.row_key);
     if(o.status==='observed'&&(!day(o.date)||!integer(o.count)||o.count!==o.source_value)||o.status!=='observed'&&o.count!==null)throw Error('Invalid observed count');}
    for(const [k,n]of [['current_7d',7],['prior_year_7d',7],['previous_30d',30],['preceding_358d',358]])checkWindow(r[k],n);
    if(r.last_observation_date!==null&&(!day(r.last_observation_date)||!integer(r.observation_lag_days)))throw Error('Invalid source age');
    if(['forecast_qualified','calls_eligible','sizing_eligible'].some(k=>r[k]!==false))throw Error('Unsupported row permission');
    rows.push({...r,key,name:r.names.join(' / ')||r.entity_id,country:r.countries.join(' / ')||'Unavailable'});
   }
   if(total!==review.history_rows)throw Error('History population differs');
  }else{
   if(p.contract&&p.contract!=='portwatch-preserved-calculation.v1')throw Error('Unknown shipping contract');
   for(const [family,group]of [['choke',p.chokepoints],['ports',p.ports]])for(const r of group){const key=family+':'+r.id;if(!r||typeof r.id!=='string'||!r.id||ids.has(key))throw Error('Ambiguous legacy identity');ids.add(key);rows.push({family,key,entity_id:r.id,name:typeof r.name==='string'?r.name:r.id,country:typeof r.country==='string'?r.country:'Unavailable',last_observation_date:r.last_date,legacy:r,observations:[]});}
  }
  rows.sort((a,b)=>a.family.localeCompare(b.family)||a.name.localeCompare(b.name)||a.entity_id.localeCompare(b.entity_id));
  return {native,rows,generated_at:p.generated_at,overdue:now-at>26*3600000,history_rows:native?review.history_rows:null,authority:false};
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
  const c=p.publication_context;
  if(!c)return 'Legacy packet: complete source acquisitions, calendar coverage and protected replay have not been declared. Original scheduled production is daily 11:20 UTC.';
  const ref=c.manifest,names=Object.keys(c.compiler_sha256||{}).sort(),expected=p.measurement_review?['lambda_function.py','portwatch_measurements.py','portwatch_store.py']:['lambda_function.py','portwatch_store.py'];
  if(c.contract!=='portwatch-preserved-calculation.v1'||['original_source_replay_verified','publication_atomic','point_in_time_verified'].some(k=>c[k]!==false)||!ref||!integer(ref.bytes)||ref.bytes<1||ref.bytes>64*1024*1024||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='audit-private/20260909-originals/portwatch-research/'+ref.sha256+'.bin'||JSON.stringify(names)!==JSON.stringify(expected)||!Object.values(c.compiler_sha256).every(x=>/^[a-f0-9]{64}$/.test(x)))return 'Inconsistent preservation declaration; no replay or economic qualification established.';
  return 'Producer declares complete stored predecessors, original HTTP responses and both native outputs retained under the displayed compiler hashes. This browser has not replayed protected originals. Older stored observations are derived inputs. Publication across history and current data is non-atomic. Full source coverage, historical vintages and investment performance remain unqualified.';
 }
 async function mount(doc,loader=load,now){
  if(!doc.getElementById('pw-review'))return;const generation=(doc._pwGeneration||0)+1;doc._pwGeneration=generation;
  const say=(id,value)=>{const n=doc.getElementById(id);if(n)n.textContent=value;};
  function clear(){for(const id of ['pw-entities','pw-observations','pw-entity'])doc.getElementById(id).replaceChildren();for(const id of ['pw-search','pw-entity']){const n=doc.getElementById(id);n.oninput=n.onchange=null;}for(const id of ['pw-next','pw-prev']){doc.getElementById(id).onclick=null;doc.getElementById(id).disabled=true;}for(const id of ['pw-population','pw-observation-count','pw-raw','pw-window','pw-page'])say(id,'Unavailable');}
  clear();say('pw-status','Reading the complete public shipping record…');
  try{
   const p=await loader();if(doc._pwGeneration!==generation)return;const v=view(p,now===undefined?Date.now():now);
   say('pw-status',v.generated_at+' · at page load: '+(v.overdue?'publication overdue (>26h)':'publication less than 26h old')+'. Observation age is shown separately.');
   say('pw-mode',v.native?'Calendar comparisons of identified vessel counts':'Legacy calculations — dates, windows and economic interpretations unverified');
   say('pw-population',v.rows.length+(v.native?' retained entities':' legacy output entities'));say('pw-observation-count',v.native?fmt(v.history_rows)+' complete stored rows':'Complete original history not exposed by this legacy packet');
   say('pw-preservation',preservation(p));say('pw-raw',JSON.stringify(p,null,2));let query='',selected=v.rows[0]?.key,page=0;
   function summaries(){const all=v.rows.filter(r=>(r.name+' '+r.country+' '+r.entity_id+' '+r.family).toLowerCase().includes(query.toLowerCase()));table(doc,'pw-entities',v.native?['Family / source field','Entity','Country','Last observation / age at calculation','Current 7d calls/day','Prior-year 7d calls/day','7d YoY','Current coverage','Preceding 358d calls/day']:['Family','Entity','Country','Last reported date','Legacy stored-observation count','Calendar qualification'],all.map(r=>v.native?[r.family+' / '+r.source_field,r.name+' ['+r.entity_id+']',r.country,(r.last_observation_date||'Unavailable')+' / '+fmt(r.observation_lag_days)+' days at calculation',fmt(r.current_7d.mean),fmt(r.prior_year_7d.mean),finite(r.year_over_year?.percent)?fmt(r.year_over_year.percent)+'%':'Unavailable',r.current_7d.available_days+'/7 days',fmt(r.preceding_358d.mean)]:[r.family,r.name+' ['+r.entity_id+']',r.country,r.last_observation_date||'Unavailable',fmt(r.legacy.n_days),'Unverified']));say('pw-entity-count',all.length+' of '+v.rows.length+' entities shown. Alphabetical within family; no disruption ranking.');}
   function observations(){const r=v.rows.find(x=>x.key===selected),all=r?.observations||[],start=page*200,shown=all.slice(start,start+200);table(doc,'pw-observations',['Stored row identity','Observation date','Exact source field','Original source value','Usable count','Status'],shown.map(o=>[o.row_key,o.date||'Unavailable',o.source_field,JSON.stringify(o.source_value),fmt(o.count),o.status]));say('pw-page',all.length?(start+1)+'–'+(start+shown.length)+' of '+all.length+' complete stored rows':'No original observation rows declared for this legacy entity.');say('pw-window',r&&v.native?JSON.stringify({entity_id:r.entity_id,unit:r.unit,current_7d:r.current_7d,prior_year_7d:r.prior_year_7d,previous_30d:r.previous_30d,preceding_358d:r.preceding_358d,year_over_year:r.year_over_year,versus_preceding_358d:r.versus_preceding_358d,invalid_identity_rows:r.invalid_identity_rows,future_rows:r.future_rows,invalid_count_rows:r.invalid_count_rows},null,2):'Legacy calendar calculations are not promoted.');doc.getElementById('pw-prev').disabled=page===0;doc.getElementById('pw-next').disabled=start+200>=all.length;}
   for(const r of v.rows){const option=el(doc,'option',r.family+' · '+r.name+' ['+r.entity_id+']',doc.getElementById('pw-entity'));option.value=r.key;}
   doc.getElementById('pw-search').oninput=e=>{query=e.target.value;summaries();};doc.getElementById('pw-entity').onchange=e=>{selected=e.target.value;page=0;observations();};doc.getElementById('pw-next').onclick=()=>{page++;observations();};doc.getElementById('pw-prev').onclick=()=>{page=Math.max(0,page-1);observations();};summaries();observations();
  }catch(e){if(doc._pwGeneration!==generation)return;clear();say('pw-mode','Unavailable');say('pw-status','Shipping research unavailable: '+e.message);say('pw-preservation','No earlier data or interpretation substituted.');}
 }
 const api={PATH,CONTRACT,clock,strictJSON,view,load,preservation,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHPortwatchReview=api;if(root.document?.getElementById('pw-review')){mount(root.document);root.document.getElementById('pw-refresh').onclick=()=>mount(root.document);}}
})(typeof globalThis!=='undefined'?globalThis:this);
