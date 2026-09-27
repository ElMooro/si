/* Exact calendar transport-price and merchandise-volume research; no votes. */
(function(root){
 'use strict';
 const shared=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const FRED={PCU4831114831115:['ocean_ppi','Index Jun 1988=100','Deep sea freight prices'],PCU484121484121:['truck_ppi','Index Dec 2003=100','Long-distance truckload prices'],IR:['import_prices','Index 2000=100','All import prices'],IQ:['export_prices','Index 2000=100','All export prices']};
 const REGIONS=['w1','i1','e6','us','gb','jp','a3','r2','d1','cn','a5','t1','l1','f3'];
 const CODES=['tgz_w1_qnmi_sn','tgz_w1_pdmi_sn','hfl_w1_pdmi_nn','hpr_w1_pdmi_nn'];
 for(const flow of ['mgz','xgz'])for(const region of REGIONS)for(const measure of ['qnmi_sn','pdmi_sn'])CODES.push(flow+'_'+region+'_'+measure);
 for(const region of REGIONS)for(const weight of ['sm','sp'])CODES.push('ipz_'+region+'_qnmi_'+weight);
 const fail=m=>{throw Error(m);},finite=n=>typeof n==='number'&&Number.isFinite(n),flags=o=>o&&FLAGS.every(k=>o[k]===false);
 const close=(a,b)=>a===b||(finite(a)&&finite(b)&&Math.abs(a-b)<=128*Number.EPSILON*Math.max(1,Math.abs(b)));
 const fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumSignificantDigits:12}):'Unavailable';
 function month(s){return typeof s==='string'&&/^\d{4}-(?:0[1-9]|1[0-2])$/.test(s)&&shared.date(s+'-01')!==null?s:null;}
 function shift(s,n){const[y,m]=s.split('-').map(Number);return new Date(Date.UTC(y,m-1+n,1)).toISOString().slice(0,7);}
 function number(v){
  if(v===null||v===undefined||v===''||v==='.')return null;
  if(!['string','number'].includes(typeof v)||!/^\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*$/.test(String(v)))fail('Direct decimal observation required');
  const n=Number(v),mantissa=String(v).trim().split(/[eE]/)[0];
  if(!finite(n)||n<0||n>1e15||(n===0&&/[1-9]/.test(mantissa)))fail('Observation outside numeric range');return n;
 }
 function comparison(a,b,current,previous){
  let status=a===null?'latest_missing':b===null?'prior_month_missing':b===0?'zero_denominator':'measured';let percent=status==='measured'?100*(a/b-1):null;
  if(percent!==null&&!finite(percent)){status='outside_numeric_range';percent=null;}return{status,percent,current_month:current,previous_month:previous,unit:'percent'};
 }
 function compare(s,values){
  const latest=[...values.keys()].sort().at(-1),current=values.get(latest);
  if(!latest||s.latest_month!==latest||!close(s.level,current)||s.status!==(current===null?'latest_missing':'measured'))fail('Latest observation differs');
  for(const[field,distance]of[['mom',-1],['three_month',-3],['yoy',-12]]){
   const previous=shift(latest,distance),c=comparison(current,values.get(previous)??null,latest,previous),actual=s[field];
   if(!actual||actual.status!==c.status||!close(actual.percent,c.percent)||actual.current_month!==latest||actual.previous_month!==previous||actual.unit!=='percent')fail('Calendar change differs from original values');
  }
 }
 function fred(sid,s,at){
  const[key,unit]=FRED[sid];
  if(!s||s.series_id!==sid||s.key!==key||s.unit!==unit||s.seasonal_adjustment!=='NSA'||s.frequency!=='monthly'||s.source_url!=='https://fred.stlouisfed.org/series/'+sid||!flags(s)||s.original_vintage_verified!==false||s.observation_freshness_verified!==false||s.observation_date_kind!=='month_start_label'||!Array.isArray(s.observations)||s.returned_rows!==s.observations.length)fail('FRED source identity differs');
  const usable=['measured','latest_missing'].includes(s.status);
  if(!usable){if(!['metadata_identity_mismatch','metadata_definition_changed','incomplete_or_transformed_response','ambiguous_or_invalid_observations','empty_observations'].includes(s.status))fail('Unknown source failure');return{id:sid,s,usable:false,rows:s.observations,kind:'FRED'};}
  if(s.definition_status!=='reviewed_source_definition'||s.metadata?.id!==sid||s.metadata.units!==unit||s.metadata.frequency_short!=='M'||s.metadata.seasonal_adjustment_short!=='NSA'||s.annualized_three_month_percent!==null||s.annualization_status!=='not_seasonally_adjusted')fail('FRED definition or annualization differs');
  const values=new Map();
  for(let i=0;i<s.observations.length;i++){
   const r=s.observations[i],ym=month(r?.month),d=r?.original?.date,n=number(r?.original?.value);
   if(!r||r.position!==i||!ym||d!==ym+'-01'||shared.date(d)>at||values.has(ym)||!close(r.value,n)||r.status!==(n===null?'missing':'observed'))fail('Exact original monthly identity required');values.set(ym,n);
  }
  compare(s,values);if(s.observation_age_days!==Math.floor(at/86400000)-shared.date(s.latest_month+'-01')/86400000)fail('Observation age differs');
  return{id:sid,s,usable:true,rows:s.observations,kind:'FRED'};
 }
 function cpb(c,at){
  if(!c||c.status!=='measured'||c.original_vintage_verified!==false||!flags(c)||c.series_count!==88||!c.series||Object.keys(c.series).length!==88||!CODES.every(k=>Object.hasOwn(c.series,k))||!/^[a-f0-9]{64}$/.test(c.workbook_sha256)||!Array.isArray(c.calendar)||!c.calendar.length)fail('Complete CPB source population required');
  const report=c.report,published=shared.clock(report?.published_at),period=month(report?.period);
  const dutch='januari februari maart april mei juni juli augustus september oktober november december'.split(' '),english='january february march april may june july august september october november december'.split(' ');
  if(!period||published===null||published>at||published<shared.date(period+'-01')||report.report_url!=='https://www.cpb.nl/wereldhandelsmonitor/cpb-wereldhandelsmonitor-'+dutch[Number(period.slice(5))-1]+'-'+period.slice(0,4)||report.workbook_url!=='https://www.cpb.nl/system/files/cpbmedia/CPB-world-trade-monitor-'+english[Number(period.slice(5))-1]+'-'+period.slice(0,4)+'.xlsx'||report.description_used_for_numbers!==false)fail('CPB report identity differs');
  function column(n){let s='';while(n){const r=(n-1)%26;s=String.fromCharCode(65+r)+s;n=Math.floor((n-1)/26);}return s;}
  for(let i=0;i<c.calendar.length;i++){const row=c.calendar[i];if(row?.month!==shift('2000-01',i)||row.column!==column(i+6)||row.cell!==row.column+'4'||shared.date(row.month+'-01')>=Date.UTC(new Date(at).getUTCFullYear(),new Date(at).getUTCMonth(),1))fail('Complete contiguous CPB calendar required');}
  if(c.calendar.at(-1).month!==period)fail('Workbook period differs from report');
  const rows=CODES.map(id=>{
   const s=c.series[id],production=id.startsWith('ipz_'),vol=id.includes('_qnmi_'),section=production?(id.endsWith('_sm')?'Import weighted, seasonally adjusted':'Production weighted, seasonally adjusted'):vol?'Volumes, seasonally adjusted':'Prices / unit values in usd';
   if(!s||s.series_id!==id||s.sheet!==(production?'inpro_out':'trade_out')||s.unit!=='Index 2021=100'||s.section!==section||s.seasonal_adjustment!==(vol?'SA':'not stated in section heading')||!flags(s)||!Number.isSafeInteger(s.row)||s.row<1||!Array.isArray(s.monthly_observations)||s.monthly_count!==c.calendar.length||s.monthly_observations.length!==c.calendar.length)fail('CPB series definition differs');
   const values=new Map();
   for(let i=0;i<s.monthly_observations.length;i++){const r=s.monthly_observations[i],cal=c.calendar[i],n=number(r.provider_value);if(r.month!==cal.month||r.cell!==cal.column+s.row||!close(r.value,n)||r.status!==(n===null?'missing':'observed'))fail('CPB monthly source value differs');values.set(r.month,n);}
   compare(s,values);return{id,s,usable:true,rows:s.monthly_observations,kind:'CPB'};
  });
  if(c.monthly_positions!==rows.reduce((n,r)=>n+r.rows.length,0)||JSON.stringify(c.world_trade)!==JSON.stringify(c.series.tgz_w1_qnmi_sn))fail('CPB complete population or headline differs');
  return rows;
 }
 function view(packet,now=Date.now()){
  const at=shared.clock(packet?.generated_at);if(!packet||typeof packet!=='object'||Array.isArray(packet)||at===null||at>now)fail('Complete nonfuture trade publication required');
  if(packet.contract===undefined)return{native:false,at:packet.generated_at,rows:[],authority:false};
  const review=packet.measurement_review,context=packet.publication_context,ref=context?.manifest;
  if(packet.contract!=='trade-nowcast-research.v1'||review?.contract!=='trade-calendar-measurements.v1'||review.calculation_at!==packet.generated_at||!flags(packet)||!flags(review)||packet.portfolio_action!=='WAIT'||packet.rate_pressure!==null||packet.verdict!=='UNQUALIFIED'||review.sources_atomic!==false||review.original_vintage_verified!==false)fail('Research-only trade contract differs');
  if(!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!=='audit-private/20260909-originals/trade-nowcast-research/'+ref.sha256+'.bin'||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>64*1024*1024||context.original_vintage_verified!==false||context.original_source_replay_verified!==false||context.sources_atomic!==false||context.public_write_count!==1)fail('Complete retained-source context required');
  const files=['lambda_function.py','trade_store.py','trade_measurements.py','managed_secret.py'];if(!packet.compiler_sha256||Object.keys(packet.compiler_sha256).length!==4||!files.every(k=>/^[a-f0-9]{64}$/.test(packet.compiler_sha256[k])))fail('Four exact compiler identities required');
  if(!review.series||Object.keys(review.series).length!==4||!Object.keys(FRED).every(k=>Object.hasOwn(review.series,k)))fail('All four FRED sources required');
  const rows=Object.keys(FRED).map(sid=>fred(sid,review.series[sid],at)),cpbRows=cpb(review.cpb,at);
  if(!review.bdi||!flags(review.bdi)||!['source_unavailable','unqualified_quote'].includes(review.bdi.status)||review.bdi.level!==null||review.bdi.quote_at!==null)fail('Scraped quote cannot gain authority');
  const discovery=review.cpb_discovery;if(!discovery||discovery.selected_url!==review.cpb.report.report_url||discovery.period!==review.cpb.report.period||!Array.isArray(discovery.candidates)||!discovery.candidates.some(r=>r.url===discovery.selected_url&&r.status==='report_candidate'&&r.period===discovery.period))fail('Actual report selection differs');
  return{native:true,at:packet.generated_at,overdue:now-at>26*3600000,rows:[...rows,...cpbRows],review,authority:false,observations:rows.reduce((n,r)=>n+r.rows.length,0)+review.cpb.monthly_positions};
 }
 function element(doc,tag,value,parent){const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;if(parent)parent.appendChild(n);return n;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),h=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const label of headers){const th=element(doc,'th',label,h);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const value of row)element(doc,'td',value,tr);}}
 function clear(doc,message='Waiting for the complete trade calendar contract.'){
  if(!doc.getElementById('trade-research'))return;doc._tradeGeneration=(doc._tradeGeneration||0)+1;
  for(const id of ['trade-summary','trade-measurement-count','trade-definition','trade-observation-count','trade-source-count'])doc.getElementById(id).textContent=message;
  for(const id of ['trade-series','trade-observations','trade-choice'])doc.getElementById(id).replaceChildren();
  for(const id of ['trade-prev','trade-next','trade-source-prev','trade-source-next']){const b=doc.getElementById(id);b.disabled=true;b.onclick=null;}
  doc.getElementById('trade-choice').onchange=null;
 }
 function render(doc,packet,now=Date.now()){
  if(!doc.getElementById('trade-research'))return;
  clear(doc);const generation=doc._tradeGeneration,current=()=>generation===doc._tradeGeneration,say=(id,value)=>{doc.getElementById(id).textContent=value;};
  try{
   const v=view(packet,now);
   if(!v.native){clear(doc,'Legacy trade publication: awaiting the original daily 12:50 UTC source run. Earlier fields remain inspectable; no rate-pressure or demand signal is inferred.');return;}
   const world=v.review.cpb.world_trade;
   say('trade-summary','World merchandise volume · '+world.latest_month+' · '+fmt(world.level)+' (2021=100, seasonally adjusted). Month over month '+fmt(world.mom.percent)+'% versus '+world.mom.previous_month+'; year over year '+fmt(world.yoy.percent)+'% versus '+world.yoy.previous_month+'. Report published '+v.review.cpb.report.published_at+'.');
   say('trade-measurement-count','4 FRED price series + 88 CPB source series · '+v.observations+' monthly positions · publication '+v.at+(v.overdue?' (overdue >26h)':'')+'. All observations remain accessible. No forecast or position authority.');
   const choice=doc.getElementById('trade-choice');for(const row of v.rows){const option=element(doc,'option',row.kind+' · '+row.id+' · '+(row.s.name||'Unnamed source'),choice);option.value=row.id;}choice.value='tgz_w1_qnmi_sn';let page=0,sourcePage=0;
   function sources(){if(!current())return;sourcePage=Math.max(0,Math.min(sourcePage,Math.ceil(v.rows.length/20)-1));const first=sourcePage*20,rows=v.rows.slice(first,first+20);
    table(doc,doc.getElementById('trade-series'),['Source / ID','Measurement','Month','Level / unit','Adjustment','MoM %','3-month %','YoY %','Status'],rows.map(r=>[r.kind+' · '+r.id,FRED[r.id]?.[2]||r.s.name||'Unnamed source',r.usable?r.s.latest_month:'Unavailable',r.usable?fmt(r.s.level)+' · '+r.s.unit:'Unavailable',r.s.seasonal_adjustment,r.usable?fmt(r.s.mom.percent):'Unavailable',r.usable?fmt(r.s.three_month.percent):'Unavailable',r.usable?fmt(r.s.yoy.percent):'Unavailable',r.s.status]));
    say('trade-source-count',(first+1)+'–'+Math.min(first+20,v.rows.length)+' of '+v.rows.length+' distinct source series. The three-month column is an unannualized end-month comparison, not CPB three-month-average momentum.');doc.getElementById('trade-source-prev').disabled=sourcePage===0;doc.getElementById('trade-source-next').disabled=first+20>=v.rows.length;
   }
   function observations(){if(!current())return;const row=v.rows.find(r=>r.id===choice.value);if(!row)return;page=Math.max(0,Math.min(page,Math.max(0,Math.ceil(row.rows.length/100)-1)));const start=page*100;
    say('trade-definition',row.id+' · '+row.s.unit+' · '+row.s.seasonal_adjustment+' · '+(row.kind==='CPB'?row.s.section+'; sheet '+row.s.sheet+', row '+row.s.row+'. Base-year weights/values are separate from monthly indexes.':row.s.name+'. Monthly observation labels are not publication dates. Provider metadata last_updated: '+(row.s.metadata?.last_updated||'Unavailable')+'. NSA changes are not annualized.'));
    table(doc,doc.getElementById('trade-observations'),['Month / raw date','Original numeric text','Parsed value','Source cell / position','Status'],row.rows.slice(start,start+100).map(r=>{const original=row.kind==='CPB'?r.provider_value:r.original?.value;return[r.month||r.original?.date||'Unavailable',original==null?'Unavailable':String(original),fmt(r.value),row.kind==='CPB'?r.cell:String(r.position),r.status];}));
    say('trade-observation-count',(row.rows.length?start+1:0)+'–'+Math.min(start+100,row.rows.length)+' of '+row.rows.length+' monthly positions; missing values remain visible.');doc.getElementById('trade-prev').disabled=page===0;doc.getElementById('trade-next').disabled=start+100>=row.rows.length;
   }
   doc.getElementById('trade-source-prev').onclick=()=>{sourcePage--;sources();};doc.getElementById('trade-source-next').onclick=()=>{sourcePage++;sources();};doc.getElementById('trade-prev').onclick=()=>{page--;observations();};doc.getElementById('trade-next').onclick=()=>{page++;observations();};choice.onchange=()=>{page=0;observations();};sources();observations();
  }catch(e){clear(doc,'Trade measurements unavailable: '+e.message+'. No previous measurements were substituted.');throw e;}
 }
 const api={FRED,CODES,number,comparison,view,clear,render};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHTradeReview=api;
})(typeof globalThis!=='undefined'?globalThis:this);
