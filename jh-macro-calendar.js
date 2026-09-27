/* Complete monthly truck and source GPR research; descriptive, without votes. */
(function(root){
 'use strict';
 const shared=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const REQUIRED_GPR=["GPR","GPRT","GPRA","GPRH","GPRHT","GPRHA","SHARE_GPR","N10","SHARE_GPRH","N3H","GPRH_NOEW","GPR_NOEW","GPRH_AND","GPR_AND","GPRH_BASIC","GPR_BASIC","SHAREH_CAT_1","SHAREH_CAT_2","SHAREH_CAT_3","SHAREH_CAT_4","SHAREH_CAT_5","SHAREH_CAT_6","SHAREH_CAT_7","SHAREH_CAT_8","GPRC_ARG","GPRC_AUS","GPRC_BEL","GPRC_BRA","GPRC_CAN","GPRC_CHE","GPRC_CHL","GPRC_CHN","GPRC_COL","GPRC_DEU","GPRC_DNK","GPRC_EGY","GPRC_ESP","GPRC_FIN","GPRC_FRA","GPRC_GBR","GPRC_HKG","GPRC_HUN","GPRC_IDN","GPRC_IND","GPRC_ISR","GPRC_ITA","GPRC_JPN","GPRC_KOR","GPRC_MEX","GPRC_MYS","GPRC_NLD","GPRC_NOR","GPRC_PER","GPRC_PHL","GPRC_POL","GPRC_PRT","GPRC_RUS","GPRC_SAU","GPRC_SWE","GPRC_THA","GPRC_TUN","GPRC_TUR","GPRC_TWN","GPRC_UKR","GPRC_USA","GPRC_VEN","GPRC_VNM","GPRC_ZAF","GPRHC_ARG","GPRHC_AUS","GPRHC_BEL","GPRHC_BRA","GPRHC_CAN","GPRHC_CHE","GPRHC_CHL","GPRHC_CHN","GPRHC_COL","GPRHC_DEU","GPRHC_DNK","GPRHC_EGY","GPRHC_ESP","GPRHC_FIN","GPRHC_FRA","GPRHC_GBR","GPRHC_HKG","GPRHC_HUN","GPRHC_IDN","GPRHC_IND","GPRHC_ISR","GPRHC_ITA","GPRHC_JPN","GPRHC_KOR","GPRHC_MEX","GPRHC_MYS","GPRHC_NLD","GPRHC_NOR","GPRHC_PER","GPRHC_PHL","GPRHC_POL","GPRHC_PRT","GPRHC_RUS","GPRHC_SAU","GPRHC_SWE","GPRHC_THA","GPRHC_TUN","GPRHC_TUR","GPRHC_TWN","GPRHC_UKR","GPRHC_USA","GPRHC_VEN","GPRHC_VNM","GPRHC_ZAF"];
 const fail=m=>{throw Error(m);},finite=n=>typeof n==='number'&&Number.isFinite(n),flags=o=>o&&FLAGS.every(k=>o[k]===false);
 const close=(a,b)=>a===b||(finite(a)&&finite(b)&&Math.abs(a-b)<=256*Number.EPSILON*Math.max(1,Math.abs(b)));
 const fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumSignificantDigits:12}):'Unavailable';
 function month(s){return typeof s==='string'&&/^\d{4}-(?:0[1-9]|1[0-2])$/.test(s)&&shared.date(s+'-01')!==null?s:null;}
 function shift(s,n){const[y,m]=s.split('-').map(Number);return new Date(Date.UTC(y,m-1+n,1)).toISOString().slice(0,7);}
 function number(v){
  if(v===null||v===undefined||v===''||v==='.')return null;
  if(!['string','number'].includes(typeof v)||!/^\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*$/.test(String(v)))fail('Direct decimal observation required');
  const n=Number(v);if(!finite(n)||n<0||n>1e15||(n===0&&/[1-9]/.test(String(v).trim().split(/[eE]/)[0])))fail('Observation outside numeric range');return n;
 }
 function comparison(a,b){let status=a===null?'latest_missing':b===null?'prior_month_missing':b===0?'zero_denominator':'measured',percent=status==='measured'?100*(a/b-1):null;if(percent!==null&&!finite(percent)){status='outside_numeric_range';percent=null;}return{status,percent};}
 function baseline(values,latest,count){
  const required=Array.from({length:count},(_,i)=>shift(latest,i-count)),missing=required.filter(d=>values.get(d)==null),current=values.get(latest)??null;
  const out={start:required[0],end:required.at(-1),expected_months:count,current_month_excluded:true,missing_months:missing,mean:null,population_sd:null,z:null,status:missing.length?'incomplete_baseline':current===null?'latest_missing':'measured'};
  if(missing.length)return out;
  const xs=required.map(d=>values.get(d)),anchor=xs[0],delta=xs.reduce((sum,x)=>sum+(x-anchor),0)/count,sd=Math.sqrt(xs.reduce((sum,x)=>sum+((x-anchor)-delta)**2,0)/count);
  out.mean=anchor+delta;out.population_sd=sd;
  if(sd===0)out.status='zero_variance';else if(current!==null){const z=((current-anchor)-delta)/sd;if(finite(z))out.z=z;else out.status='outside_numeric_range';}return out;
 }
 function checkBaseline(actual,values,latest,count){const expected=baseline(values,latest,count);if(!actual)fail('Monthly baseline absent');for(const key of Object.keys(expected)){if(['mean','population_sd','z'].includes(key)?!close(actual[key],expected[key]):JSON.stringify(actual[key])!==JSON.stringify(expected[key]))fail('Prior calendar baseline differs: '+key);}return expected;}
 function truck(s,at){
  if(!s||s.series_id!=='HTRUCKSSAAR'||s.source_url!=='https://fred.stlouisfed.org/series/HTRUCKSSAAR'||s.unit!=='Millions of Units'||s.seasonal_adjustment!=='SAAR'||s.frequency!=='monthly'||!flags(s)||s.original_vintage_verified!==false||s.observation_freshness_verified!==false||!Array.isArray(s.observations)||s.returned_rows!==s.observations.length)fail('Truck source identity differs');
  const usable=['measured','latest_missing'].includes(s.status),rows=s.observations;
  if(!usable){if(!['metadata_identity_mismatch','metadata_definition_changed','incomplete_or_transformed_response','ambiguous_observations','empty_observations'].includes(s.status))fail('Unknown truck failure');return{id:'HTRUCKSSAAR',label:'Heavy truck sales',definition:'Millions of units at SAAR; source unavailable or ambiguous.',rows,usable:false};}
  if(s.metadata?.id!=='HTRUCKSSAAR'||s.metadata.units!==s.unit||s.metadata.frequency_short!=='M'||s.metadata.seasonal_adjustment_short!=='SAAR')fail('Truck definition differs');
  const values=new Map();for(let i=0;i<rows.length;i++){const r=rows[i],ym=month(r?.month),n=number(r?.original?.value);if(!ym||r.position!==i||r.original?.date!==ym+'-01'||shared.date(r.original.date)>at||values.has(ym)||!close(r.value,n)||r.status!==(n===null?'missing':'observed'))fail('Unique original truck month required');values.set(ym,n);}
  const latest=[...values.keys()].sort().at(-1),current=values.get(latest),prior=shift(latest,-12),y=comparison(current,values.get(prior)??null);
  if(!latest||s.latest_month!==latest||!close(s.level,current)||s.status!==(current===null?'latest_missing':'measured')||s.yoy?.status!==y.status||!close(s.yoy?.percent,y.percent)||s.yoy?.current_month!==latest||s.yoy?.previous_month!==prior||s.yoy?.unit!=='percent'||s.observation_age_days!==Math.floor(at/86400000)-shared.date(latest+'-01')/86400000)fail('Truck calendar comparison differs');
  checkBaseline(s.prior_12_months,values,latest,12);
  return{id:'HTRUCKSSAAR',label:'Heavy truck sales',definition:'Millions of units at a seasonally adjusted annual rate; not actual trucks sold in that month.',rows,usable:true,s};
 }
 function gpr(g,at){
  if(!g||!['measured','latest_missing'].includes(g.status)||!flags(g)||g.source_url!=='https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls'||g.sheet!=='Sheet1'||![0,1].includes(g.date_mode)||g.original_vintage_verified!==false||g.workbook_contains_formulas_verified!==false||!/^[a-f0-9]{64}$/.test(g.workbook_sha256)||!Number.isSafeInteger(g.workbook_bytes)||g.workbook_bytes<1||g.workbook_bytes>16*1024*1024||!Array.isArray(g.months)||!g.months.length||!g.series||!REQUIRED_GPR.every(k=>Object.hasOwn(g.series,k)))fail('Complete source GPR matrix required');
  const keys=Object.keys(g.series).sort((a,b)=>g.series[a].column-g.series[b].column),labels=new Map();
  if(g.series_count!==keys.length||g.monthly_rows!==g.months.length||g.monthly_positions!==keys.length*g.months.length||!Array.isArray(g.source_labels)||g.source_labels.length!==keys.length+1||!Array.isArray(g.source_dates)||g.source_dates.length!==g.months.length)fail('GPR population differs');
  let lastRow=1;for(const row of g.source_labels){if(!row||typeof row.variable!=='string'||!row.variable||typeof row.label!=='string'||!row.label||labels.has(row.variable)||!Number.isSafeInteger(row.row)||row.row<=lastRow||row.row>g.monthly_rows+1)fail('Unique source label rows required');labels.set(row.variable,row.label);lastRow=row.row;}
  if(!labels.has('month')||labels.get('GPR')!=='Recent GPR (Index: 1985:2019=100)'||labels.get('GPRH')!=='Historical GPR (Index: 1900:2019=100)')fail('Source normalization differs');
  const cutoff=new Date(at).toISOString().slice(0,7);
  for(let i=0;i<g.months.length;i++){const ym=g.months[i],d=g.source_dates[i];if(ym!==shift('1900-01',i)||ym>=cutoff||d?.month!==ym||d.row!==i+2||!Number.isInteger(d.excel_serial))fail('Complete typed workbook calendar required');const epoch=g.date_mode===1?Date.UTC(1904,0,1):Date.UTC(1899,11,d.excel_serial<60?31:30);if(epoch+d.excel_serial*86400000!==shared.date(ym+'-01'))fail('Source Excel date differs');}
  const rows=keys.map((id,i)=>{const s=g.series[id];if(!s||s.column!==i+2||s.source_label!==labels.get(id)||!flags(s)||!Array.isArray(s.values)||s.values.length!==g.months.length)fail('Complete original source column required');const values=s.values.map(n=>{if(n!==null&&!finite(n))fail('Typed numeric source required');return number(n);});return{id,label:s.source_label,definition:s.source_label+' · original workbook column '+s.column+'.',usable:true,rows:values.map((value,i)=>({month:g.months[i],value,status:value===null?'missing':'observed',source_row:i+2})),s};});
  const values=new Map(g.months.map((d,i)=>[d,g.series.GPR.values[i]])),latest=g.months.at(-1),value=values.get(latest);
  if(g.headline_series_id!=='GPR'||g.headline_unit!=='Index 1985:2019=100'||g.latest_month!==latest||!close(g.level,value)||g.status!==(value===null?'latest_missing':'measured'))fail('GPR headline differs');checkBaseline(g.prior_60_months,values,latest,60);return rows;
 }
 function view(packet,now=Date.now()){
  const at=shared.clock(packet?.generated_at);if(!packet||Array.isArray(packet)||at===null||at>now)fail('Complete nonfuture macro publication required');if(packet.contract===undefined)return{native:false,at:packet.generated_at,rows:[],authority:false};
  const r=packet.measurement_review,c=packet.publication_context,ref=c?.manifest;
  if(packet.contract!=='macro-leads-research.v1'||r?.contract!=='macro-calendar-measurements.v1'||r.calculation_at!==packet.generated_at||!flags(packet)||!flags(r)||r.sources_atomic!==false||r.original_vintage_verified!==false||packet.portfolio_action!=='WAIT')fail('Research-only macro contract differs');
  if(!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='audit-private/20260909-originals/macro-leads-research/'+ref.sha256+'.bin'||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>64*1024*1024||c.original_vintage_verified!==false||c.original_source_replay_verified!==false||c.sources_atomic!==false||c.public_write_count!==1)fail('Retained source context required');
  const compilers=Object.entries(packet.compiler_sha256||{});if(compilers.length!==20||!["lambda_function.py","macro_measurements.py","macro_store.py","xlrd-2.0.1.dist-info/INSTALLER","xlrd-2.0.1.dist-info/LICENSE","xlrd-2.0.1.dist-info/METADATA","xlrd-2.0.1.dist-info/RECORD","xlrd-2.0.1.dist-info/REQUESTED","xlrd-2.0.1.dist-info/WHEEL","xlrd-2.0.1.dist-info/top_level.txt","xlrd/__init__.py","xlrd/biffh.py","xlrd/book.py","xlrd/compdoc.py","xlrd/formatting.py","xlrd/formula.py","xlrd/info.py","xlrd/sheet.py","xlrd/timemachine.py","xlrd/xldate.py"].every(k=>Object.hasOwn(packet.compiler_sha256,k))||compilers.some(([k,v])=>!k||!/^[a-f0-9]{64}$/.test(v)))fail('Complete compiler identities required');
  const rows=[truck(r.heavy_truck,at)];if(r.gpr!==null){if(r.gpr_status!==r.gpr.status)fail('GPR source status differs');rows.push(...gpr(r.gpr,at));}else if(r.gpr_status!=='source_unavailable')fail('GPR source unavailable status required');
  return{native:true,at:packet.generated_at,overdue:now-at>26*3600000,rows,review:r,authority:false,positions:rows.reduce((n,s)=>n+s.rows.length,0)};
 }
 function element(doc,tag,value,parent){const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;if(parent)parent.appendChild(n);return n;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),h=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const label of headers){const th=element(doc,'th',label,h);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const value of row)element(doc,'td',value,tr);}}
 function clear(doc,message='Awaiting the complete monthly source contract.'){
  if(!doc.getElementById('macro-calendar'))return;doc._macroCalendarGeneration=(doc._macroCalendarGeneration||0)+1;
  for(const id of ['macro-calendar-status','macro-series-count','macro-definition','macro-row-count'])doc.getElementById(id).textContent=message;
  for(const id of ['macro-series','macro-months','macro-choice'])doc.getElementById(id).replaceChildren();
  for(const id of ['macro-prev','macro-next','macro-source-prev','macro-source-next']){const n=doc.getElementById(id);n.disabled=true;n.onclick=null;}
  doc.getElementById('macro-choice').onchange=null;
  for(const id of ['gpr','heavy-truck']){const host=doc.getElementById(id);host.replaceChildren();element(doc,'h3',id==='gpr'?'Geopolitical Risk Index':'US Heavy Truck Sales',host);element(doc,'p','Monthly source calculation unavailable.',host);}
 }
 function render(doc,packet,now=Date.now()){
  if(!doc.getElementById('macro-calendar'))return;
  clear(doc);let v;try{v=view(packet,now);}catch(e){clear(doc,'Monthly research unavailable: '+e.message);throw e;}
  if(!v.native){
   clear(doc,'Legacy publication: awaiting the original daily 12:20 UTC run. Old z-scores remain unqualified.');
   for(const[id,value]of[['gpr',packet.geopolitical_risk?.gpr],['heavy-truck',packet.heavy_truck_sales?.saar_millions]]){
    const host=doc.getElementById(id);element(doc,'p','Legacy reported value '+fmt(value)+'. Read the complete original packet below; calendar, normalization and historical z-scores have not yet been published under the repaired contract.',host);
   }return v;
  }
  const generation=doc._macroCalendarGeneration,active=()=>generation===doc._macroCalendarGeneration;
  doc.getElementById('macro-calendar-status').textContent=v.at+' · '+v.rows.length+' source series · '+v.positions+' monthly positions. '+(v.overdue?'Publication overdue. ':'')+'Dates and units are source-specific; no forecast or sizing permission.';
  for(const[id,s,count]of[['heavy-truck',v.review.heavy_truck,12],['gpr',v.review.gpr,60]]){
   const host=doc.getElementById(id);host.replaceChildren();element(doc,'h3',id==='gpr'?'Geopolitical Risk Index':'US Heavy Truck Sales',host);
   if(!s){element(doc,'p','Source unavailable; no inferred value.',host);continue;}
   element(doc,'p',fmt(s.level)+' · '+(s.latest_month||'No valid month')+' · '+(id==='gpr'?s.headline_unit:'Millions of units, SAAR'),host);
   const b=s['prior_'+count+'_months'];element(doc,'p',b?'Prior '+count+' months '+b.start+' through '+b.end+'; current excluded. z '+fmt(b.z)+' ('+b.status+').':'Calendar calculation unavailable: '+s.status,host);
   if(s.yoy)element(doc,'p','YoY '+fmt(s.yoy.percent)+'% ('+s.yoy.status+'); '+s.yoy.previous_month+' to '+s.yoy.current_month+'.',host);
   element(doc,'p',id==='gpr'?'News-article index, not war probability or evidence of market pricing.':'An annualized rate, not monthly units. No demonstrated S&P 500 lead.',host);
  }
  let sourcePage=0,rowPage=0;const choice=doc.getElementById('macro-choice');
  for(const s of v.rows){const option=element(doc,'option',s.id+' · '+s.label,choice);option.value=s.id;}
  choice.value=v.rows.some(s=>s.id==='GPR')?'GPR':'HTRUCKSSAAR';
  const newestPage=()=>Math.max(0,Math.ceil(v.rows.find(s=>s.id===choice.value).rows.length/100)-1);rowPage=newestPage();
  function sources(){if(!active())return;const start=sourcePage*20;table(doc,doc.getElementById('macro-series'),['Series','Source definition','Monthly positions','Latest value'],v.rows.slice(start,start+20).map(s=>[s.id,s.label,s.rows.length,s.usable?fmt(s.rows.reduce((latest,row)=>!latest||row.month>latest.month?row:latest,null)?.value):'Unavailable']));doc.getElementById('macro-series-count').textContent=(start+1)+'–'+Math.min(start+20,v.rows.length)+' of '+v.rows.length+' sources';doc.getElementById('macro-source-prev').disabled=sourcePage===0;doc.getElementById('macro-source-next').disabled=start+20>=v.rows.length;}
  function observations(){if(!active())return;const s=v.rows.find(s=>s.id===choice.value);if(!s)return;const start=rowPage*100;doc.getElementById('macro-definition').textContent=s.definition;table(doc,doc.getElementById('macro-months'),['Source position','Month','Value in source unit','Status'],s.rows.slice(start,start+100).map((r,i)=>[r.source_row??r.position??(start+i),r.month||'Invalid month',fmt(r.value),r.status]));doc.getElementById('macro-row-count').textContent=s.rows.length?(start+1)+'–'+Math.min(start+100,s.rows.length)+' of '+s.rows.length+' monthly positions':'No source observations';doc.getElementById('macro-prev').disabled=rowPage===0;doc.getElementById('macro-next').disabled=start+100>=s.rows.length;}
  doc.getElementById('macro-source-prev').onclick=()=>{if(active()&&sourcePage){sourcePage--;sources();}};doc.getElementById('macro-source-next').onclick=()=>{if(active()&&(sourcePage+1)*20<v.rows.length){sourcePage++;sources();}};
  doc.getElementById('macro-prev').onclick=()=>{if(active()&&rowPage){rowPage--;observations();}};doc.getElementById('macro-next').onclick=()=>{const s=v.rows.find(s=>s.id===choice.value);if(active()&&s&&(rowPage+1)*100<s.rows.length){rowPage++;observations();}};choice.onchange=()=>{if(active()){rowPage=newestPage();observations();}};sources();observations();return v;
 }
 const api={view,render,clear,number,comparison,baseline,REQUIRED_GPR};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHMacroCalendar=api;
})(typeof window!=='undefined'?window:globalThis);
