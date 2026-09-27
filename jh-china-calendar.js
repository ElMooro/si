/* Separate China monetary and price definitions; complete descriptive calendars. */
(function(root){
 'use strict';
 const common=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const PROFILES={MANMM101CNM189S:['M1','Yuan Renminbi','M','SA',true],MANMM101CNQ189S:['M1','Yuan Renminbi','Q','SA',true],MYAGM2CNM189N:['M2','National Currency','M','NSA',true],MABMM301CNM189S:['M3','Yuan Renminbi','M','SA',true],MABMM301CNQ189S:['M3','Yuan Renminbi','Q','SA',true],IR3TIB01CNM156N:['interbank_rate','Percent','M','NSA',true],IR3TIB01CNM156S:['interbank_rate_alternative',null,null,null,false],DEXCHUS:['usd_cny','Chinese Yuan Renminbi to One U.S. Dollar','D','NSA',true],PCOPPUSDM:['copper_price','U.S. Dollars per Metric Ton','M','NSA',true],IQ12260:['gold_export_price_index','Index Dec 2024=100','M','NSA',true],GOLDAMGBD228NLBM:['retired_gold_price',null,null,null,false]};
 const fail=m=>{throw Error(m);},flags=o=>o&&FLAGS.every(k=>o[k]===false),finite=n=>typeof n==='number'&&Number.isFinite(n);
 const close=(a,b)=>a===b||(finite(a)&&finite(b)&&Math.abs(a-b)<=256*Number.EPSILON*Math.max(1,Math.abs(b)));
 const fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumSignificantDigits:12}):'Unavailable';
 function shift(s,n){const[y,m,day]=s.split('-').map(Number),d=new Date(0);d.setUTCFullYear(y,m-1+n,1);d.setUTCHours(0,0,0,0);const next=new Date(d);next.setUTCMonth(next.getUTCMonth()+1);next.setUTCDate(0);d.setUTCDate(Math.min(day,next.getUTCDate()));return d.toISOString().slice(0,10);}
 function number(v,negative=false){
  if(v===undefined||v===null||v===''||v==='.')return null;
  if(!['string','number'].includes(typeof v)||!/^\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*$/.test(String(v)))fail('Direct decimal observation required');
  const n=Number(v);if(!finite(n)||(!negative&&n<0)||Math.abs(n)>1e18||(n===0&&/[1-9]/.test(String(v).trim().split(/[eE]/)[0])))fail('Observation outside numeric range');return n;
 }
 function comparison(a,b,rate=false){let status=a===null?'latest_missing':b===null?'prior_period_missing':b===0&&!rate?'zero_denominator':'measured',value=status==='measured'?(rate?a-b:100*(a/b-1)):null;if(value!==null&&!finite(value)){status='outside_numeric_range';value=null;}return{status,value};}
 function series(id,s,at){
  const[concept,unit,frequency,adjustment,reviewed]=PROFILES[id];
  if(!s||s.series_id!==id||s.concept!==concept||s.source_url!=='https://fred.stlouisfed.org/series/'+id||s.unit!==unit||s.frequency!==frequency||s.seasonal_adjustment!==adjustment||!flags(s)||s.original_vintage_verified!==false||!Array.isArray(s.observations)||s.returned_rows!==s.observations.length)fail('China source identity differs: '+id);
  const rows=s.observations,raw=s.observation_response?.observations,usable=['measured','latest_missing'].includes(s.status);
  if((rows.length&&(!Array.isArray(raw)||raw.length!==rows.length))||rows.some((r,i)=>r?.position!==i||JSON.stringify(r.original)!==JSON.stringify(raw[i])))fail('Every original source position required');
  if(!usable){
   if(!['metadata_identity_mismatch','metadata_definition_changed','definition_not_reviewed','incomplete_or_transformed_response','ambiguous_or_invalid_observations','empty_observations','incomplete_observation_period'].includes(s.status)||s.current_measurement_eligible!==false)fail('Unknown China source failure');
   return{id,concept,unit,frequency,adjustment,s,rows,usable:false};
  }
  const meta=s.metadata_response?.seriess;
  if(!reviewed||!rows.length||s.definition_status!=='reviewed_source_definition'||!Array.isArray(meta)||meta.length!==1||meta[0].id!==id||meta[0].units!==unit||meta[0].frequency_short!==frequency||meta[0].seasonal_adjustment_short!==adjustment||s.observation_response.count!==rows.length||s.observation_response.offset!==0||s.observation_response.units!=='lin'||s.observation_response.output_type!==1)fail('Exact source definition and complete response required');
  const values=new Map();
  for(const r of rows){
   const label=r.original?.date,d=common.date(label),n=number(r.original?.value,concept==='interbank_rate');
   if(d===null||d>at||r.date!==label||values.has(label)||!close(r.value,n)||r.status!==(n===null?'missing':'observed')||(['M','Q'].includes(frequency)&&(!label.endsWith('-01')||(frequency==='Q'&&![1,4,7,10].includes(Number(label.slice(5,7)))))))fail('Unique original period required');
   values.set(label,n);
  }
  const latest=[...values.keys()].sort().at(-1),current=values.get(latest),end=frequency==='D'?common.date(latest):common.date(shift(latest,frequency==='M'?1:3))-86400000,age=Math.floor(at/86400000)-end/86400000,limit={D:14,M:120,Q:220}[frequency];
  if(end>at||s.latest_date!==latest||s.latest_period_end!==new Date(end).toISOString().slice(0,10)||!close(s.level,current)||s.status!==(current===null?'latest_missing':'measured')||s.observation_age_days!==age||s.age_policy_days!==limit||s.freshness!==(age<=limit?'within_age_policy':'stale')||s.current_measurement_eligible!==(age<=limit&&current!==null)||s.annualization_status!=='not_performed')fail('Observation date, age or current-use eligibility differs');
  function check(actual,cur,prior,rate=false){const expected=comparison(values.get(cur)??null,values.get(prior)??null,rate);if(!actual||actual.status!==expected.status||!close(actual.value,expected.value)||actual.current_date!==cur||actual.comparison_date!==prior||actual.unit!==(rate?'percentage_points':'percent'))fail('Exact calendar comparison differs');}
  for(const[name,distance]of[['three_month',-3],['yoy',-12]]){
   const anchor=shift(latest,distance);let prior=anchor;
   if(frequency==='D'&&!values.has(anchor)){const candidates=[...values.keys()].filter(k=>k<=anchor&&common.date(anchor)-common.date(k)<=7*86400000).sort();prior=candidates.at(-1)||anchor;}
   const actual=s[name];check(actual,latest,prior,concept==='interbank_rate');
   if(actual.calendar_anchor!==anchor||actual.comparison_policy!==(frequency==='D'?'anchor_or_prior_observation_within_7_calendar_days; missing_value_never_backfilled':'exact_calendar_period'))fail('Comparison anchor policy differs');
  }
  if(['M1','M2','M3'].includes(concept)){
   const a=s.money_growth_acceleration,prior=shift(latest,-12);if(!a||a.is_credit_impulse!==false)fail('Money acceleration is not credit impulse');check(a.current_yoy,latest,prior);check(a.prior_yoy,prior,shift(latest,-24));
   if(!close(a.value_pp,a.current_yoy.status==='measured'&&a.prior_yoy.status==='measured'?a.current_yoy.value-a.prior_yoy.value:null))fail('Money acceleration arithmetic differs');
  }
  return{id,concept,unit,frequency,adjustment,s,rows,usable:true};
 }
 function view(packet,now=Date.now()){
  const at=common.clock(packet?.generated_at);
  if(!packet||Array.isArray(packet)||at===null||at>now)fail('Complete nonfuture China publication required');
  if(packet.contract===undefined)return{native:false,at:packet.generated_at,rows:[],authority:false};
  const r=packet.measurement_review,c=packet.publication_context,ref=c?.manifest;
  if(packet.contract!=='china-liquidity-research.v1'||r?.contract!=='china-source-calendar.v1'||r.calculation_at!==packet.generated_at||!flags(packet)||!flags(r)||r.sources_atomic!==false||r.original_vintage_verified!==false||packet.portfolio_action!=='WAIT'||packet.regime!=='WAIT'||packet.credit_impulse?.value_pp!==null||packet.dr_copper?.copper_gold_ratio!==null)fail('Research-only China contract differs');
  if(!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='audit-private/20260909-originals/china-liquidity-research/'+ref.sha256+'.bin'||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>64*1024*1024||c.original_source_replay_verified!==false||c.original_vintage_verified!==false||c.sources_atomic!==false||c.multiple_head_atomic!==false||!Number.isInteger(c.public_write_count)||c.public_write_count<2||c.public_write_count>3)fail('Retained China source context required');
  const compilers=packet.compiler_sha256||{},names=['lambda_function.py','china_store.py','china_measurements.py','_fred_shim.py'];
  if(Object.keys(compilers).length!==names.length||names.some(k=>!/^[a-f0-9]{64}$/.test(compilers[k])))fail('Complete China compiler identities required');
  if(!r.series||Object.keys(r.series).length!==11||!Object.keys(PROFILES).every(k=>Object.hasOwn(r.series,k)))fail('All original candidate populations required');
  const rows=Object.keys(PROFILES).map(id=>series(id,r.series[id],at));
  return{native:true,at:packet.generated_at,authority:false,rows,positions:rows.reduce((n,s)=>n+s.rows.length,0)};
 }
 function element(doc,tag,text,parent){const n=doc.createElement(tag);if(text!==undefined)n.textContent=text;if(parent)parent.appendChild(n);return n;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),tr=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const h of headers){const th=element(doc,'th',h,tr);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const value of row)element(doc,'td',value,tr);}}
 function clear(doc,message='Awaiting the complete China source calendar contract.'){
  if(!doc.getElementById('china-calendar'))return;
  doc._chinaCalendarGeneration=(doc._chinaCalendarGeneration||0)+1;
  for(const id of ['china-calendar-status','china-definition','china-row-count'])doc.getElementById(id).textContent=message;
  for(const id of ['china-series','china-months','china-choice'])doc.getElementById(id).replaceChildren();
  for(const id of ['china-prev','china-next']){const n=doc.getElementById(id);n.disabled=true;n.onclick=null;}
  doc.getElementById('china-choice').onchange=null;doc.getElementById('china-original').textContent='Unavailable';doc.getElementById('china-details').ontoggle=null;
 }
 function render(doc,packet,now=Date.now()){
  clear(doc);const v=view(packet,now);if(!doc.getElementById('china-calendar'))return v;
  const generation=doc._chinaCalendarGeneration,active=()=>generation===doc._chinaCalendarGeneration;
  const details=doc.getElementById('china-details');details.ontoggle=()=>{if(active()&&details.open)doc.getElementById('china-original').textContent=JSON.stringify(packet,null,2);};details.ontoggle();
  if(!v.native){doc.getElementById('china-calendar-status').textContent='Legacy publication '+v.at+'. Original fields remain inspectable. Repaired definitions and calendar measurements await the next original weekday publication.';return v;}
  doc.getElementById('china-calendar-status').textContent=v.at+' · 11 separate candidate series · '+v.positions+' original positions. No policy-regime, forecast or sizing permission.';
  table(doc,doc.getElementById('china-series'),['Source · concept','Unit · frequency · adjustment','Latest observation','Level','Age after period end','Measurement status'],v.rows.map(s=>[s.id+' · '+s.concept,(s.unit??'Unreviewed')+' · '+(s.frequency??'Unreviewed')+' · '+(s.adjustment??'Unreviewed'),s.usable?s.s.latest_date:'Unavailable',s.usable?fmt(s.s.level):'Unqualified',s.usable?s.s.observation_age_days+' days':'Unavailable',s.usable?s.s.freshness+' · '+s.s.status:s.s.status]));
  const choice=doc.getElementById('china-choice');for(const s of v.rows){const o=element(doc,'option',s.concept+' · '+s.id,choice);o.value=s.id;}choice.value=v.rows[0].id;
  let page=0;const latest=()=>Math.max(0,Math.ceil(v.rows.find(s=>s.id===choice.value).rows.length/100)-1);page=latest();
  function observations(){
   if(!active())return;const s=v.rows.find(s=>s.id===choice.value);if(!s)return;const start=page*100;
   const change=key=>fmt(s.s[key].value)+' '+s.s[key].unit+' ('+s.s[key].comparison_date+' to '+s.s[key].current_date+'; '+s.s[key].status+')';
   doc.getElementById('china-definition').textContent=s.concept+' · '+(s.unit??'Definition unreviewed')+'. '+(s.usable?'Observation '+s.s.latest_date+', period ends '+s.s.latest_period_end+'; '+s.s.observation_age_days+' days old at publication, '+s.s.freshness+'. Three-month change: '+change('three_month')+'. YoY: '+change('yoy')+'. '+(s.s.money_growth_acceleration?'Money-growth acceleration '+fmt(s.s.money_growth_acceleration.value_pp)+' percentage points; this is not a credit impulse. ':'')+'Current-use age policy is operational, not a verified release-calendar SLA.':'Source unavailable or unqualified: '+s.s.status+'. Complete original positions remain below.');
   table(doc,doc.getElementById('china-months'),['Source position','Source date','Value in source unit','Original numeric text','Status'],s.rows.slice(start,start+100).map((r,i)=>[r?.position??start+i,r?.original?.date||'Invalid date',s.usable?fmt(r?.value):'Unqualified',r?.original?.value===null?'null':String(r?.original?.value??'Missing'),r?.status||'Invalid observation']));
   doc.getElementById('china-row-count').textContent=s.rows.length?(start+1)+'–'+Math.min(start+100,s.rows.length)+' of '+s.rows.length+' original positions':'No returned observations';
   doc.getElementById('china-prev').disabled=page===0;doc.getElementById('china-next').disabled=start+100>=s.rows.length;
  }
  doc.getElementById('china-prev').onclick=()=>{if(active()&&page){page--;observations();}};
  doc.getElementById('china-next').onclick=()=>{const s=v.rows.find(s=>s.id===choice.value);if(active()&&s&&(page+1)*100<s.rows.length){page++;observations();}};
  choice.onchange=()=>{if(active()){page=latest();observations();}};observations();return v;
 }
 const api={view,render,clear,number,comparison,shift,PROFILES};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHChinaCalendar=api;
})(typeof window!=='undefined'?window:globalThis);
