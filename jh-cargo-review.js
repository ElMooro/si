/* Complete shipment research. Source coverage never grants investment authority. */
(function(root){
 'use strict';
 const shared=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const PATH='/data/port-cargo.json',CONTRACT='port-cargo-calendar-measurements.v1',DAY=86400000;
 const finite=n=>typeof n==='number'&&Number.isFinite(n),amount=n=>Number.isSafeInteger(n)&&n>=0;
 const fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumFractionDigits:2}):'Unavailable';
 const text=v=>typeof v==='string'&&v.trim()?v:'Unavailable';
 const fail=s=>{throw Error(s);},same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
 function close(a,b){return a===b||(finite(a)&&finite(b)&&Math.abs(a-b)<=8*Number.EPSILON*Math.max(1,Math.abs(b)));}
 function windowReview(w,observations,end,days,leg){
  if(!w||w.end!==new Date(end).toISOString().slice(0,10)||w.start!==new Date(end-(days-1)*DAY).toISOString().slice(0,10)||w.expected_days!==days)fail('Exact calendar window required');
  let total=0,available=0;const missing=[];
  for(let i=days-1;i>=0;i--){const day=new Date(end-i*DAY).toISOString().slice(0,10),row=observations.get(day),value=row?.[leg+'_source_value'];if(amount(value)){total+=value;available++;}else missing.push(day);}
  const overflow=!missing.length&&!Number.isSafeInteger(total),sum=missing.length||overflow?null:total;
  if(w.available_days!==available||!same(w.missing_or_invalid_dates,missing)||w.status!==(overflow?'outside_exact_json_range':missing.length?'incomplete':'complete')||w.sum_metric_tons!==sum||!close(w.mean_metric_tons_per_day,sum===null?null:sum/days))fail('Calendar values or coverage differ from observations');
 }
 function compare(c,current,previous){
  const a=current.mean_metric_tons_per_day,b=previous.mean_metric_tons_per_day;
  const delta=a!==null&&b!==null?a-b:null,percent=a!==null&&b!==null&&b>0?100*(a/b-1):null,status=a===null||b===null?'incomplete_windows':b===0?'zero_denominator':'defined';
  if(!c||!close(c.delta_metric_tons_per_day,delta)||!close(c.percent,percent)||c.status!==status)fail('Comparison differs from complete windows');
 }
 function cohortReview(source,ports){
  if(!source||source.population_ports!==ports.length||!same(source.population_port_ids,ports.map(p=>p.port_id)))fail('Cohort population differs');
  for(const direction of ['import','export']){
   const cohort=ports.filter(p=>p.catalog_status==='registered'&&p.country_identity_consistent&&['current_7d','previous_28d'].every(w=>p.legs[direction][w].status==='complete')),ids=new Set(cohort.map(p=>p.port_id)),leg=source.legs?.[direction];
   if(!leg||leg.matched_ports!==cohort.length||!same(leg.matched_port_ids,[...ids])||!same(leg.excluded_port_ids,ports.filter(p=>!ids.has(p.port_id)).map(p=>p.port_id)))fail('Matched cohort identities differ');
   for(const [name,days] of [['current_7d',7],['previous_28d',28]]){const sum=cohort.length?cohort.reduce((n,p)=>n+p.legs[direction][name].sum_metric_tons,0):null,valid=sum!==null&&Number.isSafeInteger(sum);if(!leg[name]||leg[name].sum_metric_tons!==(valid?sum:null)||!close(leg[name].mean_metric_tons_per_day,valid?sum/days:null))fail('Cohort totals differ');}
   compare(leg.comparison,leg.current_7d,leg.previous_28d);
  }
 }
 function view(packet,now=Date.now()){
  if(!packet||typeof packet!=='object'||Array.isArray(packet)||packet.engine!=='port-cargo')fail('Complete shipment packet required');
  const at=shared.clock(packet.generated_at);if(at===null||at>now)fail('Valid nonfuture publication required');
  const result={at:packet.generated_at,overdue:now-at>26*3600000,native:false,ports:[],countries:[],authority:false};
  const r=packet.measurement_review;if(r===undefined||r===null)return result;
  if(r.contract!==CONTRACT||r.calculation_at!==packet.generated_at||r.unit!=='estimated_metric_tons'||r.rate_unit!=='estimated_metric_tons_per_day'||!Array.isArray(r.ports)||!Array.isArray(r.countries)||r.port_rows!==r.ports.length)fail('Shipment measurement contract differs');
  for(const item of [packet,r])if(!['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'].every(k=>item[k]===false))fail('Research authority declaration is inconsistent');
  const context=packet.publication_context,ref=context?.manifest;
  if(packet.contract!=='port-cargo-preserved-calculation.v1'||context?.contract!==packet.contract||!['point_in_time_verified','original_source_replay_verified','publication_atomic'].every(k=>context[k]===false)||!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='audit-private/20260909-originals/port-cargo-research/'+ref.sha256+'.bin'||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>64*1024*1024)fail('Preservation declaration differs');
  const compilers=['lambda_function.py','cargo_store.py','cargo_measurements.py','impact_mapper.py'];if(!context.compiler_sha256||Object.keys(context.compiler_sha256).length!==compilers.length||!compilers.every(k=>/^[a-f0-9]{64}$/.test(context.compiler_sha256[k])))fail('Compiler identities differ');
  const anchor=shared.date(r.source_latest_date),a=packet.acquisition_review;
  if(anchor===null||anchor>at||!a||a.contract!=='port-cargo-query-membership.v1'||shared.date(a.end)!==anchor||shared.date(a.start)!==anchor-41*DAY||a.window_days!==42||a.provider_snapshot_atomic!==false||a.source_catalog_is_world_census!==false)fail('Source query calendar differs');
  if(r.source_observation_lag_days!==Math.floor(at/DAY)-anchor/DAY)fail('Source lag differs from observation date');
  const ids=new Set(),objects=new Set(),groups=new Map();let count=0,registered=0,latest=null;
  for(const p of r.ports){
   if(!p||typeof p.port_id!=='string'||!p.port_id||ids.has(p.port_id)||!['registered','unregistered'].includes(p.catalog_status)||!Array.isArray(p.names)||p.names.some(n=>typeof n!=='string')||typeof p.country_identity_consistent!=='boolean'||!Array.isArray(p.observations)||p.observation_rows!==p.observations.length)fail('Unique complete port identities required');
   ids.add(p.port_id);if(p.catalog_status==='registered')registered++;
   if(p.country_code!==null&&(typeof p.country_code!=='string'||!/^[A-Z]{3}$/.test(p.country_code)||p.country_code!==p.country_code.toUpperCase()))fail('Country identity differs');
   const observations=new Map();let last=null;
   for(const row of p.observations){const day=shared.date(row?.date);if(day===null||day<anchor-41*DAY||day>anchor||observations.has(row.date)||!amount(row.source_object_id)||objects.has(row.source_object_id))fail('Unique dated source objects required');
    objects.add(row.source_object_id);observations.set(row.date,row);last=last===null?day:Math.max(last,day);
    for(const leg of ['import','export'])if(row[leg+'_valid']!==amount(row[leg+'_source_value']))fail('Source amount validity differs');}
   if(p.last_observation_date!==(last===null?null:new Date(last).toISOString().slice(0,10)))fail('Latest observation differs');
   count+=observations.size;if(last!==null)latest=latest===null?last:Math.max(latest,last);
   for(const leg of ['import','export']){if(!p.legs?.[leg])fail('Separate shipment directions required');const v=p.legs[leg];windowReview(v.current_7d,observations,anchor,7,leg);windowReview(v.previous_28d,observations,anchor-7*DAY,28,leg);compare(v.comparison,v.current_7d,v.previous_28d);}
   const key=p.country_code||'unassigned';if(!groups.has(key))groups.set(key,[]);groups.get(key).push(p);
  }
  if(!count||!registered||latest!==anchor||count!==r.observation_rows||registered!==r.catalog_ports)fail('Complete source population differs');
  if(!Array.isArray(a.queries)||a.queries.length!==2)fail('Both source memberships required');
  for(const [i,name] of ['PortWatch_ports_database','Daily_Ports_Data'].entries()){const q=a.queries[i],expected=i?count:registered;const where=i?"date >= timestamp '"+a.start+"' AND date <= timestamp '"+a.end+"'":'1=1';if(!q||q.where!==where||q.layer!=='https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/'+name+'/FeatureServer/0/query'||q.membership_reconciled!==true||q.provider_snapshot_atomic!==false||q.returned_rows!==expected||q.declared_count!==expected||q.enumerated_ids!==expected||!/^[a-f0-9]{64}$/.test(q.object_ids_sha256))fail('Source membership evidence differs');}
  const countries=new Set();for(const c of r.countries){if(!c||countries.has(c.country_code)||!groups.has(c.country_code))fail('Country cohort identity differs');countries.add(c.country_code);cohortReview(c,groups.get(c.country_code));}
  if(countries.size!==groups.size)fail('Missing country cohorts');cohortReview(r.covered_port_cohort,r.ports);
  return {...result,native:true,ports:r.ports,countries:r.countries,review:r,anchor:r.source_latest_date,observations:count};
 }
 async function load(options={}){
  const controller=new AbortController();let timer,reader;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Shipment research request timed out'));},options.timeout||20000);});
  try{return await Promise.race([deadline,(async()=>{const response=await(options.fetcher||root.fetch.bind(root))(PATH+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});if(!response.ok||!response.body?.getReader)fail('Shipment research source unavailable');reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>64*1024*1024)fail('Complete shipment body exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const p of parts){bytes.set(p,offset);offset+=p.byteLength;}return shared.strictJSON(new TextDecoder('utf-8',{fatal:true}).decode(bytes));})()]);
  }finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 function element(doc,tag,value,parent){const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;if(parent)parent.appendChild(n);return n;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),h=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const label of headers){const th=element(doc,'th',label,h);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const value of row)element(doc,'td',value,tr);}}
 async function mount(doc,loader=load,now=Date.now()){
  if(!doc.getElementById('cargo-review'))return;
  const generation=(doc._cargoGeneration||0)+1;doc._cargoGeneration=generation;
  const current=()=>doc._cargoGeneration===generation,say=(id,value)=>{doc.getElementById(id).textContent=value;};
  function clear(){for(const id of ['cargo-ports','cargo-countries','cargo-observations','cargo-port-choice'])doc.getElementById(id).replaceChildren();for(const id of ['cargo-coverage','cargo-count','cargo-original','cargo-observation-count','cargo-preservation','cargo-cohort','cargo-window'])say(id,'Unavailable');for(const id of ['cargo-search','cargo-direction','cargo-port-choice','cargo-original-details']){const el=doc.getElementById(id);el.oninput=el.onchange=el.ontoggle=null;}for(const id of ['cargo-prev','cargo-next','cargo-obs-prev','cargo-obs-next']){doc.getElementById(id).onclick=null;doc.getElementById(id).disabled=true;}}
  clear();say('cargo-status','Loading complete shipment research…');
  try{
   const packet=await loader();if(!current())return;const v=view(packet,now);
   say('cargo-status',v.at+' · '+(v.overdue?'publication overdue (>26h)':'publication less than 26h old')+' at page load. Publication age is separate from observation age.');
   const details=doc.getElementById('cargo-original-details');let rendered=false;
   function original(){if(!current()||!details.open||rendered)return;rendered=true;say('cargo-original',JSON.stringify(packet,null,2));}details.ontoggle=original;original();
   if(!v.native){say('cargo-coverage','Legacy packet: '+(Number.isSafeInteger(packet.n_rows_window)?packet.n_rows_window:'unavailable')+' reported source rows. Complete catalog coverage and exact calendar comparisons are not available in this packet.');say('cargo-preservation','Awaiting the normal 12:40 UTC publication of the reviewed contract. Earlier rankings, seasonal claims and impact estimates remain inspectable in the original packet; they do not inherit the new definitions.');say('cargo-cohort','Exact matched-cohort comparison unavailable');return;}
   say('cargo-coverage',v.review.catalog_ports+' catalog ports · '+(v.ports.length-v.review.catalog_ports)+' unregistered IDs · '+v.observations+' complete dated observation rows. All port and country rows remain accessible.');
   say('cargo-preservation','The producer declares reconciled source query membership and retained originals. This browser recalculates the displayed windows and cohorts from published observations; it has not replayed the protected provider bodies. Source revisions, country trade interpretation and investment effects remain unqualified.');
   const input=doc.getElementById('cargo-search'),direction=doc.getElementById('cargo-direction'),choice=doc.getElementById('cargo-port-choice');input.value='';direction.value='import';let page=0,obsPage=0;
   function render(){if(!current())return;const leg=direction.value,query=input.value.toLowerCase(),ports=v.ports.filter(p=>[p.port_id,...p.names,p.country_code||'',p.country_name||''].join(' ').toLowerCase().includes(query));page=Math.max(0,Math.min(page,Math.max(0,Math.ceil(ports.length/200)-1)));
    const anchor=shared.date(v.anchor),day=n=>new Date(anchor-n*DAY).toISOString().slice(0,10);say('cargo-window','Current: '+day(6)+' through '+day(0)+' · Previous: '+day(34)+' through '+day(7)+'. Both windows include every listed calendar date.');
    const cohort=v.review.covered_port_cohort.legs[leg];say('cargo-cohort',(leg==='import'?'Imports':'Exports')+' · '+fmt(cohort.current_7d.mean_metric_tons_per_day)+' estimated metric tons/day · '+fmt(cohort.comparison.percent)+'% vs preceding 28 days · '+cohort.matched_ports+' / '+v.ports.length+' covered ports. Latest source date '+v.anchor+' ('+v.review.source_observation_lag_days+' calendar days before publication).');
    table(doc,doc.getElementById('cargo-ports'),['Port ID','Provider names','Catalog country','Coverage / identity','Last observation','7d mean (estimated t/day)','Prior 28d mean (estimated t/day)','Change %','Valid days (7d / 28d)'],ports.slice(page*200,(page+1)*200).map(p=>{const l=p.legs[leg];return[p.port_id,p.names.join(' / ')||'Unavailable',text(p.country_code)+' · '+text(p.country_name),p.catalog_status+(p.country_identity_consistent?'':' · country unverified'),text(p.last_observation_date),fmt(l.current_7d.mean_metric_tons_per_day),fmt(l.previous_28d.mean_metric_tons_per_day),fmt(l.comparison.percent),l.current_7d.available_days+'/7 · '+l.previous_28d.available_days+'/28'];}));
    say('cargo-count',(ports.length?page*200+1:0)+'–'+Math.min((page+1)*200,ports.length)+' of '+ports.length+' matching ports. '+v.ports.length+' total source identities.');doc.getElementById('cargo-prev').disabled=page===0;doc.getElementById('cargo-next').disabled=(page+1)*200>=ports.length;
    table(doc,doc.getElementById('cargo-countries'),['Catalog country','Covered / population ports','7d mean (estimated t/day)','Prior 28d mean (estimated t/day)','Change %'],v.countries.map(c=>{const l=c.legs[leg];return[c.country_code,l.matched_ports+' / '+c.population_ports,fmt(l.current_7d.mean_metric_tons_per_day),fmt(l.previous_28d.mean_metric_tons_per_day),fmt(l.comparison.percent)];}));
   }
   input.oninput=()=>{page=0;render();};direction.onchange=()=>{page=0;render();};doc.getElementById('cargo-prev').onclick=()=>{page--;render();};doc.getElementById('cargo-next').onclick=()=>{page++;render();};
   for(const p of v.ports){const option=element(doc,'option',p.port_id+' · '+(p.names.join(' / ')||'Unnamed'),choice);option.value=p.port_id;}choice.value=v.ports[0]?.port_id||'';
   function observations(){if(!current())return;const p=v.ports.find(p=>p.port_id===choice.value),rows=p?.observations||[];obsPage=Math.max(0,Math.min(obsPage,Math.max(0,Math.ceil(rows.length/200)-1)));table(doc,doc.getElementById('cargo-observations'),['Provider object ID','Observation date','Import estimate (t)','Export estimate (t)','Value status'],rows.slice(obsPage*200,(obsPage+1)*200).map(r=>[String(r.source_object_id),r.date,r.import_valid?fmt(r.import_source_value):'Unavailable',r.export_valid?fmt(r.export_source_value):'Unavailable',(r.import_valid?'Import valid':'Import missing/invalid')+' · '+(r.export_valid?'Export valid':'Export missing/invalid')]));say('cargo-observation-count',rows.length+' total observations for '+(p?.port_id||'unavailable')+'; '+(rows.length?obsPage*200+1:0)+'–'+Math.min((obsPage+1)*200,rows.length)+' shown. No missing day is filled with zero.');doc.getElementById('cargo-obs-prev').disabled=obsPage===0;doc.getElementById('cargo-obs-next').disabled=(obsPage+1)*200>=rows.length;}
   choice.onchange=()=>{obsPage=0;observations();};doc.getElementById('cargo-obs-prev').onclick=()=>{obsPage--;observations();};doc.getElementById('cargo-obs-next').onclick=()=>{obsPage++;observations();};render();observations();
  }catch(error){if(!current())return;clear();say('cargo-status','Shipment research unavailable: '+error.message+'. Previous numbers have been cleared.');}
 }
 const api={PATH,CONTRACT,view,load,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHCargoReview=api;if(root.document)mount(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
