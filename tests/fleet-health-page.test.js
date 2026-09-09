const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const test=require('node:test');
const html=fs.readFileSync(path.join(__dirname,'../fleet-health.html'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function page(payload){
  const elements=new Map(),calls=[];
  const get=id=>{if(!elements.has(id))elements.set(id,{textContent:'',innerHTML:''});return elements.get(id);};
  const context=vm.createContext({Date,JSON,console,document:{getElementById:get},fetch:async url=>{calls.push(url);return {ok:true,json:async()=>payload()};}});
  for(const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))vm.runInContext(script[1],context);
  return {get,calls,context};
}
const fixture=()=>({engine:'fleet-monitor',schema_version:'1.0',privacy_version:'fleet-metadata-20260909-v1',generated_at:'2026-01-01T00:00:00Z',
  system_status:'yellow',elapsed_s:0,summary:{data_outputs_total:100,data_outputs_fresh:0,data_outputs_static:40,data_outputs_cadence_mapped:80,dependencies_down:0,dependencies_degraded:1,lambda_count:321},
  data_outputs:{available:true,n_red:0,n_yellow:26,n_degraded:0,n_static:40,private_content_checks_skipped:2,
    red:[],degraded:[],yellow:[{output:'fixture',age_hours:5,size:123,cadence_hours:3,issue:'aging output'}],static:[{output:'config',age_hours:40}]},
  compute:{available:false,error:'COMPUTE_INVENTORY_UNAVAILABLE',note:'inventory unavailable'},dependencies:[{name:'FRED',status:'yellow',detail:'rate limited'}],
  future_metric:{zero:0,unknown:null,flag:false}});

test('Fleet renders actual writer, missing sections, row limits and complete typed snapshot',async()=>{
  const doc=fixture(),view=page(()=>doc);await tick();
  assert.match(view.calls[0],/\/_health\/fleet\.json/);
  assert.deepEqual(JSON.parse(view.get('full-response').textContent),doc);
  assert.match(view.get('source-status').textContent,/STALE MONITOR SNAPSHOT/);
  assert.match(view.get('cards').innerHTML,/Deps Degraded|Cadence Mapped/);
  assert.match(view.get('issue-coverage').textContent,/26 aging.*Showing 1 returned rows/);
  assert.match(view.get('static-coverage').textContent,/1 of 40/);
  assert.match(view.get('compute').innerHTML,/UNKNOWN.*inventory unavailable/s);
  assert.match(view.get('issues').innerHTML,/123 bytes.*3h/);
});

test('Fleet failed refresh clears prior data and rejects old unsanitized source',async()=>{
  let doc=fixture();const view=page(()=>doc);await tick();
  doc={...doc,privacy_version:undefined};await vm.runInContext('main()',view.context);
  assert.equal(view.get('full-response').textContent,'');
  assert.equal(view.get('issue-coverage').textContent,'');
  assert.match(view.get('source-status').textContent,/Health is unknown/);
});

test('Unavailable data inventory does not imply all outputs are fresh',async()=>{
  const doc=fixture();doc.data_outputs={available:false,error:'DATA_INVENTORY_UNAVAILABLE'};
  const view=page(()=>doc);await tick();
  assert.match(view.get('issues').innerHTML,/output health is unknown/);
  assert.doesNotMatch(view.get('issues').innerHTML,/all.*fresh/);
  assert.deepEqual(JSON.parse(view.get('full-response').textContent),doc);
});
