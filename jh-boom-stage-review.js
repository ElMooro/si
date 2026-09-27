/* Complete Boom Stage research. Legacy model text never becomes a portfolio instruction. */
(function(root){
 'use strict';
 const shared=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const PATH='/data/boom-stage.json',CONTRACT='boom-fred-calendar.v1';
 const PAIRS=['KR-semis','CN-broad','UAE-energy','BR-commodities','US-freight','CL-copper','PE-copper','FI-pulp','SA-oil','TW-semis','AU-iron','ID-nickel','QA-lng','DE-machinery','JP-autos','VN-electronics','MX-manufacturing','MY-semis','TH-electronics','NL-hightech'];
 const SOURCES=['CAPUTLG3344S','CAPUTLG21S','TCU','WGTSTUS1','ISRATIO','RETAILIRSA'];
 const units={CAPUTLG3344S:['Percent','percentage_points'],CAPUTLG21S:['Percent','percentage_points'],TCU:['Percent','percentage_points'],MNFCTRIRSA:['Ratio','ratio_points'],ISRATIO:['Ratio','ratio_points'],RETAILIRSA:['Ratio','ratio_points']};
 const finite=n=>typeof n==='number'&&Number.isFinite(n),fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumFractionDigits:4}):'Unavailable';
 const text=v=>typeof v==='string'&&v.trim()?v:'Unavailable';
 function view(packet,now=Date.now()){
  if(!packet||typeof packet!=='object'||Array.isArray(packet)||!Array.isArray(packet.pairs))throw Error('Complete identified pair packet required');
  const at=shared.clock(packet.generated_at);if(at===null||at>now)throw Error('Valid nonfuture publication required');
  const found=new Map();for(const row of packet.pairs){if(!row||typeof row.id!=='string'||!row.id.trim()||found.has(row.id))throw Error('Unique pair identities required');found.set(row.id,row);}
  const ids=[...PAIRS,...[...found.keys()].filter(id=>!PAIRS.includes(id))];
  const pairs=ids.map(id=>{const p=found.get(id);return {id,source:p,table:[id,text(p?.label),p?(PAIRS.includes(id)?'Present':'Unregistered pair'):'Missing pair',
   text(p?.stage),fmt(p?.value?.yoy_pct),text(p?.value?.src),fmt(p?.volume?.vs_baseline_pct),text(p?.volume?.src),text(p?.factor4?.series)]};});
  const review=packet.fred_measurements,native=review?.contract===CONTRACT;
  if(review&&!native)throw Error('Unknown FRED measurement contract');
  if(native&&(!review.series||typeof review.series!=='object'||Array.isArray(review.series)||review.calculated_at!==packet.generated_at||review.point_in_time_verified!==false||review.forecast_qualified!==false))throw Error('Inconsistent dated measurement declaration');
  const series=native?review.series:{},all=[...SOURCES,...Object.keys(series).filter(id=>!SOURCES.includes(id))];
  const measurements=all.map(sid=>{
   const s=series[sid];if(Object.hasOwn(series,sid)&&(!s||typeof s!=='object'||Array.isArray(s)||s.series!==sid||s.contract!==CONTRACT))throw Error('FRED series identity differs');
   const rows=s?.observations??[];if(!Array.isArray(rows)||rows.some(r=>!r||typeof r!=='object'||Array.isArray(r)))throw Error('Complete observation rows required');
   if(s?.returned_rows!==undefined&&(!Number.isSafeInteger(s.returned_rows)||s.returned_rows!==rows.length))throw Error('Observation population differs');
   const profile=units[sid],date=shared.date(s?.date),prior=shared.date(s?.prior_date);
   const typed=!!(s&&profile&&s.unit===profile[0]&&s.change_unit===profile[1]&&s.country==='US'&&s.country_industry_mapping_qualified===false
    &&s.read==='RESEARCH_ONLY'&&s.comparison==='same_month_previous_calendar_year'&&s.frequency==='monthly'
    &&['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'].every(k=>s[k]===false));
   let measured=typed&&s.status==='measured';
   if(measured){
    const expected=s.date.slice(0,4)-1+s.date.slice(4);
    if(date===null||prior===null||s.date.slice(8)!=='01'||date>at||s.prior_date!==expected
       ||![s.level,s.prior_level,s.yoy_chg].every(finite)||s.level<0||s.prior_level<0
       ||Math.abs((s.level-s.prior_level)-s.yoy_chg)>1e-9*Math.max(1,Math.abs(s.yoy_chg)))throw Error('Dated arithmetic is inconsistent');
   }
   const level=typed&&date!==null&&date<=at?fmt(s.level):'Unavailable';
   const status=s?(s.status==='measured'&&!typed?'Definition or permission fields inconsistent':text(s.status)):native?'Missing source':'Awaiting dated contract';
   return {sid,source:s,observations:rows,table:[sid,text(s?.name),status,
    typed?'United States; foreign mapping unqualified':'Definition unverified',level,typed?text(s.unit):'Unverified',typed?text(s.date):'Unavailable',
    measured?fmt(s.prior_level):'Unavailable',measured?s.prior_date:'Unavailable',measured?fmt(s.yoy_chg):'Unavailable',measured?s.change_unit.replaceAll('_',' '):'Unavailable',String(rows.length)]};
  });
  return {pairs,measurements,native,at:packet.generated_at,overdue:now-at>26*3600000,
   present:PAIRS.filter(id=>found.has(id)).length,extras:ids.length-PAIRS.length,observations:measurements.reduce((n,s)=>n+s.observations.length,0),authority:false};
 }
 async function load(options={}){
  const controller=new AbortController();let timer,reader;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Research request timed out'));},options.timeout||20000);});
  try{return await Promise.race([deadline,(async()=>{
   const response=await(options.fetcher||root.fetch.bind(root))(PATH+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Research source unavailable');reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>64*1024*1024)throw Error('Complete research body exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let at=0;for(const p of parts){bytes.set(p,at);at+=p.byteLength;}
   return shared.strictJSON(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 function element(doc,tag,value,parent){const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;if(parent)parent.appendChild(n);return n;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),head=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const name of headers){const th=element(doc,'th',name,head);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const value of row)element(doc,'td',value,tr);}}
 async function mount(doc,loader=load,now=Date.now()){
  if(!doc.getElementById('boom-review'))return;
  const generation=(doc._boomGeneration||0)+1;doc._boomGeneration=generation;
  const current=()=>doc._boomGeneration===generation,say=(id,value)=>{doc.getElementById(id).textContent=value;};
  function clear(){for(const id of ['boom-pairs','boom-measurements','boom-observations','boom-series'])doc.getElementById(id).replaceChildren();for(const id of ['boom-coverage','boom-count','boom-original','boom-observation-count','boom-preservation'])say(id,'Unavailable');doc.getElementById('boom-search').oninput=null;doc.getElementById('boom-series').onchange=null;for(const id of ['boom-prev','boom-next']){doc.getElementById(id).onclick=null;doc.getElementById(id).disabled=true;}}
  clear();say('boom-status','Loading complete research…');
  try{
   const packet=await loader();if(!current())return;const v=view(packet,now);
   say('boom-status',v.at+' · '+(v.overdue?'publication overdue (>26h)':'publication less than 26h old')+' at page load. Source dates are separate.');
   say('boom-coverage',v.present+' of '+PAIRS.length+' configured pairs present; '+(PAIRS.length-v.present)+' missing; '+v.extras+' unregistered. '+v.observations+' returned FRED observation rows.');
   say('boom-preservation',v.native?'The producer declares retained complete FRED responses and a bounded two-year request window. This browser has not replayed protected originals. Other providers, source vintages, model stages and portfolio effects remain unqualified.':'Legacy packet: dated FRED metadata and original-response evidence are unavailable. Existing numbers do not inherit the newer contract.');
   say('boom-original',JSON.stringify(packet,null,2));
   const input=doc.getElementById('boom-search');input.value='';
   function pairs(query){if(!current())return;const rows=v.pairs.filter(p=>p.table.join(' ').toLowerCase().includes(query.toLowerCase()));table(doc,doc.getElementById('boom-pairs'),['Pair ID','Inherited scope label','Coverage','Stored stage (unqualified)','Reported value % (unqualified)','Value source label','Reported flow % (unqualified)','Shipping source label','FRED context identity'],rows.map(p=>p.table));say('boom-count',rows.length+' of '+v.pairs.length+' rows shown. All legacy comparisons remain unqualified.');}
   input.oninput=e=>pairs(e.target.value);pairs('');
   table(doc,doc.getElementById('boom-measurements'),['Series ID','Definition','Measurement status','Geography','Latest level','Level unit','Observation month','Prior level','Prior month','Absolute change','Change unit','Returned rows'],v.measurements.map(s=>s.table));
   const select=doc.getElementById('boom-series');let selected=v.measurements[0].sid,page=0;
   for(const s of v.measurements){const option=element(doc,'option',s.sid+' · '+s.observations.length+' rows',select);option.value=s.sid;}select.value=selected;
   function observations(){if(!current())return;const rows=v.measurements.find(s=>s.sid===selected).observations,start=page*200;
    table(doc,doc.getElementById('boom-observations'),['Response row','Observation label','Provider value','Parsed value','Interpretation status'],rows.slice(start,start+200).map(r=>[String(r.row),text(r.date),r.provider_value===undefined?'Unavailable':JSON.stringify(r.provider_value),fmt(r.value),text(r.status)]));
    say('boom-observation-count',rows.length?'Rows '+(start+1)+'–'+Math.min(start+200,rows.length)+' of '+rows.length+' for '+selected:'No observation rows declared for '+selected);
    doc.getElementById('boom-prev').disabled=page===0;doc.getElementById('boom-next').disabled=start+200>=rows.length;
   }
   select.onchange=e=>{selected=e.target.value;page=0;observations();};doc.getElementById('boom-prev').onclick=()=>{page=Math.max(0,page-1);observations();};doc.getElementById('boom-next').onclick=()=>{page++;observations();};observations();
  }catch(e){if(!current())return;clear();say('boom-status','Research unavailable: '+e.message+'. Previous values cleared.');}
 }
 const api={PATH,CONTRACT,PAIRS,SOURCES,view,load,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHBoomStageReview=api;if(root.document?.getElementById('boom-review')){mount(root.document);root.document.getElementById('boom-refresh').onclick=()=>mount(root.document);}}
})(typeof globalThis!=='undefined'?globalThis:this);
