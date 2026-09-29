const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8');
function fixture(){
 const generated_at=new Date().toISOString(),start=generated_at.slice(0,10),prefix='data/auction-calendar-originals/';
 return {generated_at,forward_calendar:[],forward_calendar_status:{status:'complete',reason:null},calendar_source:{
  contract:'treasury-upcoming-replay.v1',generated_at,status:'complete',original_bytes_replayed:true,selection_replayed:true,
  historical_point_in_time_verified:false,provider_universe_complete:false,forecast_eligible:false,calls_eligible:false,sizing_eligible:false,execution_eligible:false,
  request_window:{start,end:new Date(Date.parse(start+'T00:00:00Z')+30*86400000).toISOString().slice(0,10),days_ahead:30},
  coverage:{response_complete:true,records_received:0,selected:0,outside_window:0,invalid_records:0,selected_with_field_issues:0,duplicate_occurrences:0,
   all_occurrences_retained:true,selection_complete:true,provider_universe_complete:false},
  measurements:{key:prefix+'measurements/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64),bytes:100},
  manifest:{key:prefix+'runs/'+'b'.repeat(64)+'.json',sha256:'b'.repeat(64),bytes:100}}};
}
function page(packet){
 const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{innerHTML:'',textContent:'',className:'',style:{}});return nodes.get(id);};
 const env={Date,console,fixture:packet,document:{getElementById:get}};vm.createContext(env);
 vm.runInContext(source.slice(0,source.lastIndexOf('load();')),env);vm.runInContext('DATA=fixture',env);
 return {env,get,set(value){env.fixture=value;vm.runInContext('DATA=fixture',env);}};
}
test('failed or legacy missing calendar never claims there are no auctions',()=>{
 for(const packet of [{}, {calendar_source:{status:'unavailable',reason:'acquisition_failed'}}]){
  const {env,get}=page(packet);env.renderForwardTable();env.renderConcessionCallout();
  assert.match(get('forward-tbody').innerHTML,/unavailable or incomplete/);assert.doesNotMatch(get('forward-tbody').innerHTML,/lists no auctions/);
  assert.match(get('concession-callout').innerHTML,/unavailable or incomplete/);
 }
});
test('complete retained empty response exposes exact typed evidence without universe claims',()=>{
 const {env,get}=page(fixture());env.renderForwardTable();
 assert.match(get('forward-tbody').innerHTML,/retained Treasury response lists no auctions/);
 const html=get('forward-source-status').innerHTML;assert.match(html,/Producer replay retained 0 records/);
 assert.match(html,/Provider-universe completeness and forecasts remain unverified/);
 assert.match(html,/All retained calendar records \(JSON\)/);assert.match(html,/Replay manifest/);
});
test('invalid records, inconsistent coverage and calculation failure withhold empty inference',()=>{
 for(const change of ['partial','mismatch','boolean','calc']){
  const packet=fixture(),s=packet.calendar_source,c=s.coverage;
  if(change==='partial'){s.status='partial';c.records_received=c.invalid_records=1;c.selection_complete=false;packet.forward_calendar_status.status='partial';}
  else if(change==='mismatch')c.records_received=1;
  else if(change==='boolean')c.selected=false;
  else packet.forward_calendar_status.status='unavailable';
  const {env,get}=page(packet);env.renderForwardTable();assert.doesNotMatch(get('forward-tbody').innerHTML,/lists no auctions/);
 }
});
test('stale future mismatched window promoted permission and hostile link reject evidence',()=>{
 for(const kind of ['old','future','clock','window','permission','path','size']){
  const packet=fixture(),s=packet.calendar_source;
  if(kind==='old')packet.generated_at=s.generated_at='2000-01-01T00:00:00Z';
  else if(kind==='future')packet.generated_at=s.generated_at='2100-01-01T00:00:00Z';
  else if(kind==='clock')s.generated_at='2000-01-01T00:00:00Z';
  else if(kind==='window')s.request_window.end=s.request_window.start;
  else if(kind==='permission')s.sizing_eligible=true;
  else if(kind==='path')s.measurements.key='private/account.json';
  else s.manifest.bytes=true;
  const {env,get}=page(packet);env.renderForwardTable();
  assert.match(get('forward-source-status').innerHTML,/evidence unavailable/);
  assert.doesNotMatch(get('forward-source-status').innerHTML,/href=|account/);
 }
});
test('refresh clears prior evidence and recovers independently of heuristic quality',()=>{
 const packet=fixture(),{env,get,set}=page(packet);env.renderForwardTable();assert.match(get('forward-source-status').innerHTML,/href=/);
 set({});env.renderForwardTable();assert.doesNotMatch(get('forward-source-status').innerHTML,/href=/);
 env.showError('Unscored',packet);assert.match(get('forward-source-status').innerHTML,/href=/);
 assert.equal(get('composite-score').textContent,'—');
 env.showError('Transport failed');assert.doesNotMatch(get('forward-source-status').innerHTML,/href=/);
});
