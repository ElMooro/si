/* Complete Korea/Taiwan export-value calendars; descriptive research only. */
(function(root){
 'use strict';
 const common=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const PROFILES={XTEXVA01KRM667N:['korea_exports','US dollars, exchange rate converted','Korea merchandise export value'],VALEXPTWM052N:['taiwan_exports','Millions of Dollars','Taiwan goods export value']};
 const fail=m=>{throw Error(m);},flags=o=>o&&FLAGS.every(k=>o[k]===false),finite=n=>typeof n==='number'&&Number.isFinite(n);
 const close=(a,b)=>a===b||(finite(a)&&finite(b)&&Math.abs(a-b)<=256*Number.EPSILON*Math.max(1,Math.abs(b)));
 const fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumSignificantDigits:12}):'Unavailable';
 function shift(s,n){const[y,m]=s.split('-').map(Number),d=new Date(0);d.setUTCFullYear(y,m-1+n,1);d.setUTCHours(0,0,0,0);return d.toISOString().slice(0,7);}
 function number(v){
  if(v===undefined||v===null||v===''||v==='.')return null;
  if(!['string','number'].includes(typeof v)||!/^\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*$/.test(String(v)))fail('Direct decimal observation required');
  const n=Number(v);if(!finite(n)||n<0||n>1e15||(n===0&&/[1-9]/.test(String(v).trim().split(/[eE]/)[0])))fail('Observation outside numeric range');return n;
 }
 function comparison(a,b){let status=a===null?'latest_missing':b===null?'prior_month_missing':b===0?'zero_denominator':'measured',percent=status==='measured'?100*(a/b-1):null;if(percent!==null&&!finite(percent)){status='outside_numeric_range';percent=null;}return{status,percent};}
 function series(id,s,at){
  const[key,unit,name]=PROFILES[id];
  if(!s||s.series_id!==id||s.key!==key||s.name!==name||s.source_url!=='https://fred.stlouisfed.org/series/'+id||s.unit!==unit||s.frequency!=='monthly'||s.seasonal_adjustment!=='NSA'||s.observation_date_kind!=='month_start_label'||!flags(s)||s.original_vintage_verified!==false||s.observation_freshness_verified!==false||!Array.isArray(s.observations)||s.returned_rows!==s.observations.length)fail('Export source identity differs: '+id);
  const rows=s.observations,usable=['measured','latest_missing'].includes(s.status);
  if(!usable){
   if(!['metadata_identity_mismatch','metadata_definition_changed','incomplete_or_transformed_response','ambiguous_or_invalid_observations','empty_observations'].includes(s.status))fail('Unknown export source failure');
   return{id,key,unit,name,s,rows,usable:false};
  }
  if(!rows.length||s.definition_status!=='reviewed_source_definition'||s.metadata?.id!==id||s.metadata.units!==unit||s.metadata.frequency_short!=='M'||s.metadata.seasonal_adjustment_short!=='NSA')fail('Export source definition differs');
  const values=new Map();
  for(let i=0;i<rows.length;i++){
   const r=rows[i],ym=r?.month,n=number(r?.original?.value),d=common.date(r?.original?.date);
   if(typeof ym!=='string'||!/^\d{4}-(0[1-9]|1[0-2])$/.test(ym)||d===null||r.original.date!==ym+'-01'||d>at||r.position!==i||values.has(ym)||!close(r.value,n)||r.status!==(n===null?'missing':'observed'))fail('Unique original export month required');
   values.set(ym,n);
  }
  const latest=[...values.keys()].sort().at(-1),current=values.get(latest);
  if(s.latest_month!==latest||!close(s.level,current)||s.status!==(current===null?'latest_missing':'measured')||s.observation_age_days!==Math.floor(at/86400000)-common.date(latest+'-01')/86400000||s.annualized_three_month_percent!==null||s.annualization_status!=='not_seasonally_adjusted')fail('Latest export month or NSA definition differs');
  for(const[name,distance]of[['mom',-1],['three_month',-3],['yoy',-12]]){
   const prior=shift(latest,distance),expected=comparison(current,values.get(prior)??null),actual=s[name];
   if(!actual||actual.status!==expected.status||!close(actual.percent,expected.percent)||actual.current_month!==latest||actual.previous_month!==prior||actual.unit!=='percent')fail('Exact export calendar comparison differs: '+name);
  }
  return{id,key,unit,name,s,rows,usable:true};
 }
 function view(packet,now=Date.now()){
  const at=common.clock(packet?.generated_at);
  if(!packet||Array.isArray(packet)||at===null||at>now)fail('Complete nonfuture Asia publication required');
  if(packet.contract===undefined)return{native:false,at:packet.generated_at,rows:[],authority:false};
  const r=packet.measurement_review,c=packet.publication_context,ref=c?.manifest;
  if(packet.contract!=='asia-leads-research.v1'||r?.contract!=='asia-export-calendar.v1'||r.calculation_at!==packet.generated_at||!flags(packet)||!flags(r)||r.sources_atomic!==false||r.original_vintage_verified!==false||packet.portfolio_action!=='WAIT')fail('Research-only Asia contract differs');
  if(!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='audit-private/20260909-originals/asia-leads-research/'+ref.sha256+'.bin'||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>64*1024*1024||c.original_source_replay_verified!==false||c.original_vintage_verified!==false||c.sources_atomic!==false||c.multiple_head_atomic!==false||!Number.isInteger(c.public_write_count)||c.public_write_count<1||c.public_write_count>4)fail('Retained Asia source context required');
  const compilers=packet.compiler_sha256||{},names=['lambda_function.py','asia_store.py','asia_measurements.py','managed_secret.py'];
  if(Object.keys(compilers).length!==names.length||names.some(k=>!/^[a-f0-9]{64}$/.test(compilers[k])))fail('Complete Asia compiler identities required');
  if(!r.series||Object.keys(r.series).length!==2||!Object.keys(PROFILES).every(k=>Object.hasOwn(r.series,k)))fail('Both separate export populations required');
  const rows=Object.keys(PROFILES).map(id=>series(id,r.series[id],at));
  return{native:true,at:packet.generated_at,overdue:now-at>26*3600000,authority:false,rows,positions:rows.reduce((n,s)=>n+s.rows.length,0)};
 }
 function element(doc,tag,text,parent){const node=doc.createElement(tag);if(text!==undefined)node.textContent=text;if(parent)parent.appendChild(node);return node;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),tr=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const h of headers){const th=element(doc,'th',h,tr);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const value of row)element(doc,'td',value,tr);}}
 function clear(doc,message='Awaiting the complete Asia calendar contract.'){
  if(!doc.getElementById('asia-calendar'))return;
  doc._asiaCalendarGeneration=(doc._asiaCalendarGeneration||0)+1;
  for(const id of ['asia-calendar-status','asia-definition','asia-row-count'])doc.getElementById(id).textContent=message;
  for(const id of ['asia-series','asia-months','asia-choice'])doc.getElementById(id).replaceChildren();
  for(const id of ['asia-prev','asia-next']){const n=doc.getElementById(id);n.disabled=true;n.onclick=null;}
  doc.getElementById('asia-choice').onchange=null;
  doc.getElementById('asia-original').textContent='Unavailable';doc.getElementById('asia-details').ontoggle=null;
 }
 function render(doc,packet,now=Date.now()){
  clear(doc);const v=view(packet,now);if(!doc.getElementById('asia-calendar'))return v;
  const generation=doc._asiaCalendarGeneration,active=()=>generation===doc._asiaCalendarGeneration;
  const details=doc.getElementById('asia-details');details.ontoggle=()=>{if(active()&&details.open)doc.getElementById('asia-original').textContent=JSON.stringify(packet,null,2);};details.ontoggle();
  if(!v.native){doc.getElementById('asia-calendar-status').textContent='Legacy publication '+v.at+'. Complete packet retained for inspection; exact monthly arithmetic and source definitions are not yet available under the repaired contract.';return v;}
  doc.getElementById('asia-calendar-status').textContent=v.at+' · 2 separate export series · '+v.positions+' monthly positions. '+(v.overdue?'Publication overdue. ':'')+'No forecast or sizing permission.';
  const pct=(row,key)=>row.usable?fmt(row.s[key].percent)+'% · '+row.s[key].status:'Unavailable · '+row.s.status;
  table(doc,doc.getElementById('asia-series'),['Source','Unit · seasonal adjustment','Latest month','Value','Month over month','3 months','Year over year'],v.rows.map(s=>[s.id,s.unit+' · NSA',s.usable?s.s.latest_month:'Unavailable',s.usable?fmt(s.s.level):'Unavailable',pct(s,'mom'),pct(s,'three_month'),pct(s,'yoy')]));
  const choice=doc.getElementById('asia-choice');for(const s of v.rows){const o=element(doc,'option',s.name+' · '+s.id,choice);o.value=s.id;}choice.value=v.rows[0].id;
  let page=0;const latest=()=>Math.max(0,Math.ceil(v.rows.find(s=>s.id===choice.value).rows.length/100)-1);page=latest();
  function observations(){
   if(!active())return;const s=v.rows.find(s=>s.id===choice.value);if(!s)return;const start=page*100;
   doc.getElementById('asia-definition').textContent=s.name+' · '+s.unit+' · not seasonally adjusted. '+(s.usable?'Latest source month '+s.s.latest_month+'; '+s.s.observation_age_days+' days since its month-start label at publication (not a release-lag estimate). MoM compares '+s.s.mom.previous_month+' to '+s.s.mom.current_month+'; 3 months '+s.s.three_month.previous_month+' to '+s.s.three_month.current_month+'; YoY '+s.s.yoy.previous_month+' to '+s.s.yoy.current_month+'.':'Source unavailable or ambiguous: '+s.s.status+'. Retained positions below are unqualified.');
   table(doc,doc.getElementById('asia-months'),['Source position','Source date','Value in source unit','Original numeric text','Status'],s.rows.slice(start,start+100).map((r,i)=>[r?.position??start+i,r?.original?.date||'Invalid date',s.usable?fmt(r?.value):'Unqualified',r?.original?.value===null?'null':String(r?.original?.value??'Missing'),r?.status||'Invalid observation']));
   doc.getElementById('asia-row-count').textContent=s.rows.length?(start+1)+'–'+Math.min(start+100,s.rows.length)+' of '+s.rows.length+' monthly positions':'No returned observations';
   doc.getElementById('asia-prev').disabled=page===0;doc.getElementById('asia-next').disabled=start+100>=s.rows.length;
  }
  doc.getElementById('asia-prev').onclick=()=>{if(active()&&page){page--;observations();}};
  doc.getElementById('asia-next').onclick=()=>{const s=v.rows.find(s=>s.id===choice.value);if(active()&&s&&(page+1)*100<s.rows.length){page++;observations();}};
  choice.onchange=()=>{if(active()){page=latest();observations();}};observations();return v;
 }
 const api={view,render,clear,number,comparison,PROFILES};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHAsiaCalendar=api;
})(typeof window!=='undefined'?window:globalThis);
