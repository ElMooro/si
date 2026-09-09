const test=require('node:test');const assert=require('node:assert/strict');
const viewer=require('../jh-data-inspector.js');const app=require('../jh-observability.js');
const now=Date.parse('2026-09-09T10:00:00Z');
function errorDoc(){return {engine:'fleet-error-monitor',privacy_version:'fleet-errors-metadata-20260909-v1',diagnostic_text_private:true,generated_at:'2026-09-09T09:59:00Z',alerts_scope:'ALL_CURRENT_ALARMS',n_lambdas_scanned:883,n_alerts_detected:0,n_alerts_raised:0,dlq_status:{available:true,total:0,visible:0,inflight:0},alerts:[]}}
function freshDoc(){return {publication:{schema_version:'public-freshness-report.v1',scope:'PUBLIC_ENGINE_HEALTH',contains_private_data:false},generated_at:'2026-09-09T09:45:00Z',coverage:{results_complete:true,results_returned:1,enumeration_complete:true},full_expected_coverage:true,status:'HEALTHY',n_keys_tracked:1,results:[{key:'data/example.json',status:'FRESH'}]}}
test('missing, legacy, future and expired reports never label fleet healthy',()=>{
 assert.equal(app.errorReport(null,now).status,'UNAVAILABLE');
 assert.equal(app.errorReport({...errorDoc(),privacy_version:null},now).doc,null);
 for(const patch of [{alerts_scope:'LEGACY_NEW_ALERTS_ONLY'},{n_lambdas_scanned:0},{generated_at:'2020-01-01T00:00:00Z'},{generated_at:'2027-01-01T00:00:00Z'},{dlq_status:{available:false,total:null}},{n_alerts_detected:2}])assert.notEqual(app.errorReport({...errorDoc(),...patch},now).status,'HEALTHY');
 assert.equal(app.errorReport(errorDoc(),now).status,'HEALTHY');
});
test('deduped active alarms remain visible and unavailable measurements remain unknown',()=>{
 const doc={...errorDoc(),n_alerts_detected:1,n_alerts_raised:0,alerts:[{lambda:'engine',severity:'WARNING',errors:10,invocations:100,error_category:'TIMEOUT'}]};
 assert.equal(app.errorReport(doc,now).status,'ALERTS');
 doc.alerts[0].metric_status='UNAVAILABLE';assert.equal(app.errorReport(doc,now).status,'UNKNOWN');
});
test('freshness requires complete source coverage and every reported result',()=>{
 assert.equal(app.freshnessReport(freshDoc(),now).status,'HEALTHY');
 for(const patch of [{full_expected_coverage:false},{coverage:{results_complete:true,results_returned:1,enumeration_complete:false}},{results:[]},{results:[{key:'data/missing.json',status:'MISSING'}]},{publication:{schema_version:'old'}}])assert.notEqual(app.freshnessReport({...freshDoc(),...patch},now).status,'HEALTHY');
});
class Element{constructor(tag){this.tagName=tag;this.children=[];this.events={};this.dataset={};this._text='';}set textContent(v){this._text=String(v);this.children=[];}get textContent(){return this._text+this.children.map(n=>n.textContent).join('');}append(...n){this.children.push(...n);}replaceChildren(...n){this.children=[...n];this._text='';}setAttribute(){}addEventListener(name,fn){this.events[name]=fn;}}
function nodes(n){return [n,...n.children.flatMap(nodes)]}
test('actual page renders all alerts safely, paginates full findings, and clears healthy state on outage',()=>{
 const ids=new Map();global.document={createElement:tag=>new Element(tag),getElementById:id=>{if(!ids.has(id))ids.set(id,new Element('div'));return ids.get(id)}};global.JHDataInspector=viewer;
 const doc={...errorDoc(),n_alerts_detected:31,alerts:Array.from({length:31},(_,i)=>({lambda:i===30?'<img src=x onerror=alert(1)>':'engine'+i,severity:'WARNING',errors:i,invocations:100,error_category:'TIMEOUT'}))};
 const fresh={...freshDoc(),coverage:{results_complete:true,results_returned:402,enumeration_complete:true},n_keys_tracked:402,results:Array.from({length:402},(_,i)=>({key:'data/'+i+'.json',status:'FRESH',extra:i===401?false:null}))};
 app.renderReports(doc,fresh,null,now);assert.equal(nodes(ids.get('alerts-body')).filter(n=>n.tagName==='tr').length,32);assert.match(ids.get('alerts-body').textContent,/<img src=x onerror=alert\(1\)>/);assert.equal(nodes(ids.get('alerts-body')).some(n=>n.tagName==='img'),false);
 const next=nodes(ids.get('stale-body')).find(n=>n.tagName==='button'&&n.textContent==='Next');for(let i=0;i<16;i++)next.onclick();assert.match(ids.get('stale-body').textContent,/data\/401.json/);
 app.renderReports(null,null,null,now);assert.equal(ids.get('fleet-status').textContent,'UNAVAILABLE');assert.match(ids.get('error-full').textContent,/unavailable/);delete global.document;
});
