/* Current monitor reports; absence, legacy scope and incomplete measurements never mean healthy. */
(function(global){'use strict';
const ERROR_VERSION='fleet-errors-metadata-20260909-v1';
const FRESH_VERSION='public-freshness-report.v1';
const finite=v=>typeof v==='number'&&Number.isFinite(v)&&v>=0;
const count=v=>finite(v)?String(v):'unknown';
function age(doc,now){const ts=Date.parse(doc&&doc.generated_at);return Number.isFinite(ts)?now-ts:null;}
function errorReport(doc,now=Date.now()){
 if(!doc||doc.engine!=='fleet-error-monitor'||doc.privacy_version!==ERROR_VERSION||doc.diagnostic_text_private!==true)return {status:'UNAVAILABLE',reason:'A reviewed public error-monitor report is unavailable.',doc:null,alerts:[]};
 const alerts=Array.isArray(doc.alerts)?doc.alerts:[],elapsed=age(doc,now),dlq=doc.dlq_status||{};
 let status='HEALTHY',reason='No alarms in the complete current monitor report.';
 if(elapsed===null||elapsed< -300000||elapsed>20*60000){status='STALE / UNKNOWN';reason='The report timestamp is missing, future-dated or older than the 20-minute display limit.';}
 else if(doc.alerts_scope!=='ALL_CURRENT_ALARMS'||!finite(doc.n_alerts_detected)||doc.n_alerts_detected!==alerts.length||!finite(doc.n_lambdas_scanned)||doc.n_lambdas_scanned===0){status='INCOMPLETE';reason='Complete current alarm and fleet coverage is not established; legacy notification counts cannot establish health.';}
 else if(dlq.available!==true||!finite(dlq.total)||alerts.some(row=>row.metric_status==='UNAVAILABLE')){status='UNKNOWN';reason='CloudWatch or DLQ measurements are unavailable.';}
 else if(doc.n_alerts_detected>0||dlq.total>0){status='ALERTS';reason=count(doc.n_alerts_detected)+' currently detected alarms; notification deduplication does not clear an alarm.';}
 return {status,reason,doc,alerts};
}
function freshnessReport(doc,now=Date.now()){
 const publication=doc&&doc.publication;
 if(!doc||!publication||publication.schema_version!==FRESH_VERSION||publication.scope!=='PUBLIC_ENGINE_HEALTH'||publication.contains_private_data!==false)return {status:'UNAVAILABLE',reason:'A reviewed public freshness report is unavailable.',doc:null,rows:[]};
 const rows=Array.isArray(doc.results)?doc.results:[],coverage=doc.coverage||{},elapsed=age(doc,now);
 let status='HEALTHY',reason='All enumerated expected outputs meet the reported freshness checks.';
 if(elapsed===null||elapsed< -300000||elapsed>90*60000){status='STALE / UNKNOWN';reason='The report timestamp is missing, future-dated or older than the 90-minute display limit.';}
 else if(!finite(doc.n_keys_tracked)||doc.n_keys_tracked===0||doc.n_keys_tracked!==rows.length||coverage.results_complete!==true||coverage.results_returned!==rows.length||coverage.enumeration_complete!==true||doc.full_expected_coverage!==true||doc.status==='UNKNOWN'){status='INCOMPLETE';reason='Expected-output coverage or enumeration is incomplete; no all-fresh claim is available.';}
 else if(rows.some(row=>row.status!=='FRESH')||doc.status!=='HEALTHY'){status='ATTENTION';reason='The report contains stale, absent, invalid or unverified outputs.';}
 return {status,reason,doc,rows};
}
function elem(tag,text){const n=document.createElement(tag);if(text!==undefined)n.textContent=String(text);return n;}
function put(id,text){const n=document.getElementById(id);if(n)n.textContent=String(text);}
function inspect(id,payload,label){const target=document.getElementById(id);if(!target)return;target.replaceChildren();if(payload&&global.JHDataInspector)global.JHDataInspector.inspect(target,payload,label);else target.append(elem('p',payload?'Complete viewer unavailable. Refresh to retry.':'Report unavailable.'))}
function table(id,headers,rows){const target=document.getElementById(id);if(!target)return;target.replaceChildren();const t=elem('table'),head=elem('tr');headers.forEach(h=>head.append(elem('th',h)));t.append(head);for(const row of rows){const tr=elem('tr');row.forEach(v=>tr.append(elem('td',v==null?'unknown':v)));t.append(tr);}target.append(t);}
function renderReports(errorDoc,freshDoc,llmDoc,now=Date.now()){
 const error=errorReport(errorDoc,now),fresh=freshnessReport(freshDoc,now);
 put('fleet-status',error.status);put('fleet-status-meta',error.reason);
 put('updated','Error report: '+(error.doc&&error.doc.generated_at||'unavailable')+' · Freshness report: '+(fresh.doc&&fresh.doc.generated_at||'unavailable'));
 put('footer-time',new Date(now).toISOString());
 const dlq=error.doc&&error.doc.dlq_status;
 put('dlq-count',dlq&&dlq.available?count(dlq.total):'UNKNOWN');put('dlq-pct',dlq&&dlq.available?'Queue messages: '+count(dlq.visible)+' visible / '+count(dlq.inflight)+' in flight':'DLQ measurements unavailable');
 put('xray-count',error.doc?count(error.doc.n_lambdas_scanned):'UNKNOWN');put('xray-pct','Functions scanned in this report. DLQ and tracing coverage percentages are not published by these monitors.');
 put('fresh-count',fresh.doc?count(fresh.doc.n_keys_tracked):'UNKNOWN');put('fresh-detail',fresh.status+' · '+fresh.reason);
 put('alerts-meta',error.doc?count(error.doc.n_alerts_detected)+' detected / '+count(error.doc.n_alerts_raised)+' new notifications · '+error.doc.alerts_scope:'Current alarms unavailable');
 if(error.alerts.length)table('alerts-body',['Severity','Function','Errors / invocations','Error rate %','Category','Measurement status'],error.alerts.map(row=>[row.severity,row.lambda,count(row.errors)+' / '+count(row.invocations),row.error_rate_pct,row.error_category,row.metric_status||'REPORTED']));
 else put('alerts-body',error.status==='HEALTHY'?'No alarms in the complete current report.':error.reason);
 put('stale-meta',fresh.doc?fresh.rows.length+' returned rows · '+fresh.status:'Current freshness findings unavailable');
 inspect('stale-body',fresh.doc?fresh.rows:null,'Every returned freshness result, including missing, invalid, stale and unknown rows.');
 inspect('error-full',error.doc,'Complete reviewed error-monitor output, including thresholds and notification status.');
 inspect('fresh-full',fresh.doc,'Complete reviewed freshness-monitor output and coverage limitations.');
 put('llm-status',llmDoc?'Reported '+String(llmDoc.status||'UNKNOWN')+' · '+String(llmDoc.generated_at||'timestamp unavailable'):'UNAVAILABLE');
 inspect('llm-body',llmDoc,'Complete returned LLM health report; this independent feed does not establish fleet health.');
 return {error,fresh};
}
async function load(){
 const seq=++load.sequence;
 const read=async key=>{try{const response=await global.fetch('/'+key,{cache:'no-store'});return response.ok?await response.json():null;}catch{return null;}};
 const docs=await Promise.all(['data/_fleet-monitor.json','data/_freshness-monitor.json','data/llm-health.json'].map(read));
 if(seq===load.sequence)renderReports(...docs);
}
load.sequence=0;
const api={errorReport,freshnessReport,renderReports,load};global.JHObservability=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
if(typeof document!=='undefined'){if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>{load();global.setInterval(load,60000)});else{load();global.setInterval(load,60000)}}
})(typeof window==='undefined'?globalThis:window);
