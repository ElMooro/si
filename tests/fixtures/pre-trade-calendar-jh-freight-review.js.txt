/* Descriptive freight evidence. Retained source bytes do not establish forecast skill. */
(function(root){
 'use strict';
 const shared=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const SOURCES={freight:'/data/freight-pulse.json',air:'/data/air-cargo.json',trade:'/data/trade-nowcast.json',boom:'/data/boom-stage.json',shipping:'/data/portwatch.json'};
 const PROFILES={TSIFRGHT:['tsi_freight','Index 2000=100','SA','Freight Transportation Services Index'],FRGSHPUSM649NCIS:['cass_shipments','Index Jan 1990=1','NSA','Cass Freight Index: Shipments'],FRGEXPUSM649NCIS:['cass_expend','Index Jan 1990=1','NSA','Cass Freight Index: Expenditures'],TRUCKD11:['truck_tonnage','Index 2015=100','SA','Truck Tonnage Index'],RAILFRTCARLOADSD11:['rail_carloads','Carloads','SA','Rail Freight Carloads'],RAILFRTINTERMODALD11:['rail_intermodal','Containers and Trailers','SA','Rail Freight Intermodal Traffic']};
 const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'],DAY=86400000;
 const finite=n=>typeof n==='number'&&Number.isFinite(n),fail=s=>{throw Error(s);},same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
 const close=(a,b)=>a===b||(finite(a)&&finite(b)&&Math.abs(a-b)<=128*Number.EPSILON*Math.max(1,Math.abs(b)));
 const fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumSignificantDigits:12}):'Unavailable';
 const text=n=>typeof n==='string'&&n.trim()?n:'Unavailable';
 function number(raw){
  if(!['string','number'].includes(typeof raw)||!/^\s*[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\s*$/.test(String(raw)))return null;
  const n=Number(raw),mantissa=String(raw).trim().split(/[eE]/)[0];
  return finite(n)&&n>=0&&n<=1e30&&(n!==0||!/[1-9]/.test(mantissa))?n:null;
 }
 function shift(date,months){const [y,m]=date.split('-').map(Number),d=new Date(Date.UTC(y,m-1+months,1));return d.toISOString().slice(0,10);}
 function change(a,b){const value=a===null||b===null||b===0?null:100*(a/b-1);return{status:a===null||b===null?'missing_comparison':b===0?'zero_denominator':!finite(value)?'outside_numeric_range':'measured',percent:finite(value)?value:null};}
 function compare(c,a,b,current,previous,seasonal,field){
  const expected=change(a,b);
  if(!c||c.status!==expected.status||!close(c.percent,expected.percent)||c.current_date!==current||c.previous_date!==previous||!close(c.current_level,a)||!close(c.previous_level,b)||c.unit!=='percent')fail('Monthly comparison differs from dated originals');
  if(field==='six_month'){
   const value=expected.status==='measured'&&seasonal==='SA'?100*((a/b)**2-1):null;
   const status=seasonal==='NSA'?'not_seasonally_adjusted':expected.status==='measured'&&!finite(value)?'outside_numeric_range':expected.status;
   if(!close(c.annualized_percent,finite(value)?value:null)||c.annualization_status!==status)fail('Seasonal annualization differs');
  }
 }
 function seriesReview(sid,s,at){
  const [key,unit,seasonal]=PROFILES[sid];
  if(!s||s.series_id!==sid||s.key!==key||s.frequency!=='monthly'||s.source_url!=='https://fred.stlouisfed.org/series/'+sid||!FLAGS.every(k=>s[k]===false)||s.observation_freshness_verified!==false||s.point_in_time_verified!==false||s.observation_date_kind!=='month_start_label'||!Array.isArray(s.observations)||s.returned_rows!==s.observations.length)fail('Monthly series identity differs');
  const unavailable=['metadata_identity_mismatch','metadata_definition_changed','incomplete_or_transformed_response','ambiguous_observation_identity','empty_observations'];
  if(unavailable.includes(s.status))return{sid,series:s,usable:false,values:new Map()};
  if(!['measured','latest_missing'].includes(s.status)||s.definition_status!=='reviewed_source_definition'||s.unit!==unit||s.seasonal_adjustment!==seasonal||s.metadata?.id!==sid||s.metadata.units!==unit||s.metadata.frequency_short!=='M'||s.metadata.seasonal_adjustment_short!==seasonal)fail('Source definition differs');
  const values=new Map();
  for(let i=0;i<s.observations.length;i++){
   const r=s.observations[i],date=shared.date(r?.date),n=number(r?.provider_value);
   if(!r||r.row!==i||date===null||r.date.slice(-2)!=='01'||r.date<'2015-01-01'||date>at||values.has(r.date)||!close(r.value,n)||r.status!==(n===null?'missing_or_invalid_value':'observed'))fail('Unique original monthly values required');
   values.set(r.date,n);
  }
  const dates=[...values.keys()].sort(),latest=dates.at(-1),current=values.get(latest)??null;
  if(!latest||s.latest_date!==latest||!close(s.level,current)||s.status!==(current===null?'latest_missing':'measured')||s.observation_age_days!==Math.floor(at/DAY)-shared.date(latest)/DAY)fail('Latest monthly observation differs');
  for(const [field,months] of [['yoy',-12],['six_month',-6]]){const previous=shift(latest,months);compare(s[field],current,values.get(previous)??null,latest,previous,seasonal,field);}
  const baseline=s.prior_60_months,required=Array.from({length:60},(_,i)=>shift(latest,i-60)),missing=required.filter(d=>values.get(d)==null);
  let mean=null,sd=null,z=null,status=missing.length?'incomplete_baseline':current===null?'latest_missing':'measured';
  if(!missing.length){const ns=required.map(d=>values.get(d)),anchor=ns[0];mean=anchor+ns.reduce((a,b)=>a+(b-anchor),0)/60;sd=Math.sqrt(ns.reduce((a,b)=>a+(b-mean)**2,0)/60);if(sd===0)status='zero_variance';else if(current!==null){z=(current-mean)/sd;if(!finite(z)){z=null;status='outside_numeric_range';}}}
  if(!baseline||baseline.start!==required[0]||baseline.end!==required.at(-1)||baseline.expected_months!==60||!same(baseline.missing_or_invalid_dates,missing)||baseline.status!==status||!close(baseline.mean,mean)||!close(baseline.population_sd,sd)||!close(baseline.z,z))fail('Prior-calendar standardization differs');
  return{sid,series:s,usable:true,values};
 }
 function ratioReview(r,rows){
  if(!r||r.unit!=='dimensionless_index_ratio'||!FLAGS.every(k=>r[k]===false)||!Array.isArray(r.observations))fail('Cass ratio definition differs');
  const a=rows.find(x=>x.sid==='FRGEXPUSM649NCIS'),b=rows.find(x=>x.sid==='FRGSHPUSM649NCIS');
  if(!a.usable||!b.usable){if(r.status!=='unavailable'||r.current_ratio!==null||r.yoy_percent!==null||r.observations.length)fail('Unavailable Cass source cannot form a ratio');return;}
  const dates=[...new Set([...a.values.keys(),...b.values.keys()])].sort(),ratios=new Map();
  if(r.observations.length!==dates.length)fail('All Cass dates required');
  for(let i=0;i<dates.length;i++){const date=dates[i],x=a.values.get(date)??null,y=b.values.get(date)??null,value=x!==null&&y!==null&&y>0?x/y:null,n=finite(value)?value:null,obs=r.observations[i];ratios.set(date,n);if(obs.date!==date||!close(obs.expenditure_index,x)||!close(obs.shipment_index,y)||!close(obs.ratio,n))fail('Cass ratio differs from unrounded indexes');}
  const latest=dates.at(-1),previous=shift(latest,-12),a0=ratios.get(latest),b0=ratios.get(previous)??null,c=change(a0,b0);
  if(r.current_date!==latest||r.prior_date!==previous||!close(r.current_ratio,a0)||!close(r.prior_ratio,b0)||r.status!==c.status||!close(r.yoy_percent,c.percent))fail('Cass ratio comparison differs');
 }
 function view(p,now=Date.now()){
  if(!p||typeof p!=='object'||Array.isArray(p)||p.engine_class!=='physical_trade_slow_confirmation'||typeof p.version!=='string'||!p.series||typeof p.series!=='object'||Array.isArray(p.series)||(p.engine!==undefined&&p.engine!=='freight-pulse'))fail('Complete Freight Pulse packet required');
  const at=shared.clock(p.generated_at);if(at===null||at>now)fail('Nonfuture publication clock required');
  const base={at:p.generated_at,overdue:now-at>26*3600000,native:false,authority:false,rows:[]};
  if(p.measurement_review==null){if(p.contract!==undefined)fail('Declared freight contract lacks measurements');return base;}
  const review=p.measurement_review,context=p.publication_context,ref=context?.manifest;
  if(p.contract!=='freight-preserved-calculation.v1'||review.contract!=='freight-calendar-measurements.v1'||review.calculation_at!==p.generated_at||review.series_count!==6||review.complete_requested_population!==true||review.point_in_time_verified!==false||p.portfolio_action!=='WAIT'||![p,review].every(o=>FLAGS.every(k=>o[k]===false)))fail('Freight research contract differs');
  if(context?.contract!==p.contract||!['point_in_time_verified','original_source_replay_verified','publication_atomic'].every(k=>context[k]===false)||![12,13].includes(context.provider_responses)||!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='audit-private/20260909-originals/freight-native-research/'+ref.sha256+'.bin'||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>64*1024*1024)fail('Complete preservation identity required');
  const files=['lambda_function.py','freight_store.py','freight_measurements.py','impact_mapper.py','managed_secret.py'];if(!context.compiler_sha256||Object.keys(context.compiler_sha256).length!==5||!files.every(k=>/^[a-f0-9]{64}$/.test(context.compiler_sha256[k])))fail('Five compiler identities required');
  if(!review.series||!same(Object.keys(review.series).sort(),Object.keys(PROFILES).sort()))fail('Every monthly series must remain accessible');
  const rows=Object.keys(PROFILES).map(sid=>seriesReview(sid,review.series[sid],at));ratioReview(review.cass_index_ratio,rows);
  return{...base,native:true,review,rows,observations:rows.reduce((n,x)=>n+x.series.observations.length,0)};
 }
 async function load(kind,options={}){
  if(!Object.hasOwn(SOURCES,kind))fail('Undeclared research packet');
  const controller=new AbortController();let timer,reader;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Research request timed out'));},options.timeout||20000);});
  try{return await Promise.race([deadline,(async()=>{const response=await(options.fetcher||root.fetch.bind(root))(SOURCES[kind]+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});if(!response.ok||!response.body?.getReader)fail('Research source unavailable');reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>64*1024*1024)fail('Complete research body exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const p of parts){bytes.set(p,offset);offset+=p.byteLength;}return shared.strictJSON(new TextDecoder('utf-8',{fatal:true}).decode(bytes));})()]);
  }finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 function element(doc,tag,value,parent){const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;if(parent)parent.appendChild(n);return n;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),h=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const label of headers){const th=element(doc,'th',label,h);th.scope='col';}const b=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,b);for(const value of row)element(doc,'td',value,tr);}}
 async function mount(doc,loader=load,now=Date.now()){
  if(!doc.getElementById('freight-review'))return;
  const generation=(doc._freightGeneration||0)+1;doc._freightGeneration=generation;const current=()=>doc._freightGeneration===generation;
  const say=(id,value)=>{doc.getElementById(id).textContent=value;};
  function clearFreight(){for(const id of ['freight-series','freight-observations','freight-choice'])doc.getElementById(id).replaceChildren();for(const id of ['freight-count','freight-ratio','freight-definition','freight-observation-count','freight-preservation'])say(id,'Unavailable');doc.getElementById('freight-choice').onchange=null;for(const id of ['freight-prev','freight-next']){doc.getElementById(id).onclick=null;doc.getElementById(id).disabled=true;}}
  clearFreight();
  for(const key of Object.keys(SOURCES)){say(key+'-status','Reading complete packet…');say(key+'-original','Open to inspect the complete packet.');doc.getElementById(key+'-details').ontoggle=null;}
  await Promise.all(Object.keys(SOURCES).map(async key=>{
   try{
    const packet=await loader(key);if(!current())return;
    if(!packet||typeof packet!=='object'||Array.isArray(packet))fail('Complete object required');const at=shared.clock(packet.generated_at);if(at===null||at>now)fail('Valid nonfuture publication required');
    say(key+'-status',packet.generated_at+' · '+(now-at>26*3600000?'publication overdue (>26h)':'publication less than 26h old')+' at page load. Observation dates are separate.');
    const details=doc.getElementById(key+'-details');let rendered=false;function original(){if(!current()||!details.open||rendered)return;rendered=true;say(key+'-original',JSON.stringify(packet,null,2));}details.ontoggle=original;original();
    if(key!=='freight')return;
    const v=view(packet,now);
    if(!v.native){say('freight-count','Legacy publication: exact monthly comparisons have not been published yet.');say('freight-preservation','Awaiting the original daily 11:50 UTC run. All earlier fields remain in the original packet; composites, inflections and beta impacts are unqualified.');return;}
    say('freight-count','6 source identities · '+v.observations+' monthly observations · '+v.rows.filter(x=>x.usable).length+' series with validated calendar identities. No missing observation is filled with zero.');
    table(doc,doc.getElementById('freight-series'),['Series / source ID','Observation month','Level / source unit','Adjustment','Year over year %','6-month change %','6-month annualized %','Status'],v.rows.map(x=>{const s=x.series;return[PROFILES[x.sid][3]+' · '+x.sid,x.usable?s.latest_date.slice(0,7):'Unavailable',x.usable?fmt(s.level)+' · '+s.unit:'Unavailable',text(s.seasonal_adjustment),x.usable?fmt(s.yoy.percent):'Unavailable',x.usable?fmt(s.six_month.percent):'Unavailable',x.usable?(s.seasonal_adjustment==='NSA'?'Withheld · NSA':fmt(s.six_month.annualized_percent)):'Unavailable',s.status];}));
    const ratio=v.review.cass_index_ratio;say('freight-ratio','Cass expenditure / shipment index ratio: '+fmt(ratio.current_ratio)+' · '+text(ratio.current_date)+' · YoY '+fmt(ratio.yoy_percent)+'% versus '+text(ratio.prior_date)+'. Dimensionless indexes with the same January 1990 base; not dollars per shipment or a pure freight-rate index. Status: '+ratio.status+'.');
    say('freight-preservation','The producer declares whole retained inputs, provider attempts and five compiler identities. This browser checks displayed arithmetic against all published monthly observations; protected-source replay is a separate runner check. Original vintages, leading relationships and position effects are unqualified.');
    const choice=doc.getElementById('freight-choice');for(const x of v.rows){const option=element(doc,'option',PROFILES[x.sid][3]+' · '+x.sid,choice);option.value=x.sid;}choice.value=v.rows[0].sid;let page=0;
    function observations(){if(!current())return;const x=v.rows.find(r=>r.sid===choice.value);if(!x)return;const s=x.series,rows=s.observations;page=Math.max(0,Math.min(page,Math.max(0,Math.ceil(rows.length/100)-1)));
     const baseline=s.prior_60_months;say('freight-definition',x.sid+' · '+text(s.unit)+' · '+text(s.seasonal_adjustment)+'. Latest observation label: '+text(s.latest_date)+'. Provider metadata last_updated: '+text(s.metadata?.last_updated)+'. This is not an original release-vintage certificate. '+(x.usable?'Prior 60 months: '+baseline.start+' through '+baseline.end+'; mean '+fmt(baseline.mean)+', population SD '+fmt(baseline.population_sd)+', z '+fmt(baseline.z)+'. '+baseline.status+'; missing dates: '+(baseline.missing_or_invalid_dates.join(', ')||'none')+'.':'Definition/response unavailable; every returned row remains inspectable.'));
     table(doc,doc.getElementById('freight-observations'),['Returned row','Monthly label','Original provider value','Numeric value','Status'],rows.slice(page*100,(page+1)*100).map(r=>[String(r.row),text(r.date),typeof r.provider_value==='string'?r.provider_value:JSON.stringify(r.provider_value),fmt(r.value),r.status]));
     say('freight-observation-count',(rows.length?page*100+1:0)+'–'+Math.min((page+1)*100,rows.length)+' of '+rows.length+' observations; all source dates accessible.');doc.getElementById('freight-prev').disabled=page===0;doc.getElementById('freight-next').disabled=(page+1)*100>=rows.length;
    }
    choice.onchange=()=>{page=0;observations();};doc.getElementById('freight-prev').onclick=()=>{page--;observations();};doc.getElementById('freight-next').onclick=()=>{page++;observations();};observations();
   }catch(error){if(!current())return;if(key==='freight')clearFreight();say(key+'-status','Unavailable: '+error.message+'. No previous measurements remain displayed.');}
  }));
 }
 const api={SOURCES,PROFILES,number,view,load,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHFreightReview=api;if(root.document)mount(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
