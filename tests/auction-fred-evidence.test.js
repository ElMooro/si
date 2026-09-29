const test=require('node:test'),assert=require('node:assert/strict');
const {webcrypto,createHash}=require('node:crypto');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const api=require('../jh-auction-fred-evidence.js'),inspector=require('../jh-data-inspector.js');
const fixtures=require('./fixtures/auction-fred-evidence-view.json');
const clone=v=>JSON.parse(JSON.stringify(v));
const AT=fixtures.cases.complete.generated_at,originalNow=Date.now;
Date.now=()=>Date.parse(AT);test.after(()=>{Date.now=originalNow;});
function fixture(name='complete'){return clone(fixtures.cases[name]);}
function spec(f=fixture()){return api.specification(f.packet,f.generated_at);}
function rewrite(f,change){const data=JSON.parse(f.raw);change(data);f.raw=JSON.stringify(data);const hash=createHash('sha256').update(f.raw).digest('hex');f.packet.measurements={key:'data/auction-fred-originals/measurements/'+hash+'.json',sha256:hash,bytes:Buffer.byteLength(f.raw)};return f;}
async function load(f){return api.loadVerified(spec(f),async()=>new Response(f.raw),webcrypto);}
class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.events={};this.dataset={};this.attrs={};this.style={};this.className='';this._text='';}
 set textContent(v){this._text=String(v);this.children=[];}
 get textContent(){return this._text+this.children.map(x=>x.textContent||'').join('');}
 append(...nodes){this.children.push(...nodes);}
 replaceChildren(...nodes){this.children=[...nodes];this._text='';}
 setAttribute(k,v){this.attrs[k]=v;}
 addEventListener(name,fn){this.events[name]=fn;}
}
function all(root,predicate){return [root,...root.children.flatMap(x=>all(x,predicate))].filter(predicate);}
function dom(){global.document={createElement:tag=>new Element(tag)};global.JHDataInspector=inspector;global.crypto=webcrypto;return new Element('section');}
function button(root){return all(root,n=>n.tagName==='button'&&n.textContent==='Inspect FRED sources')[0];}

test('six complete artifacts from the actual Python producer preserve all seventeen requests',async()=>{
 assert.equal(fixtures.synthetic_only,true);
 for(const name of Object.keys(fixtures.cases)){const result=await load(fixture(name));assert.equal(result.summary.length,17);assert.deepEqual(result.summary.map(r=>r.series),api.SERIES);}
});
test('missing and unrequested are distinct from zero and complete capture',async()=>{
 const partial=await load(fixture('partial'));assert.equal(partial.summary.find(r=>r.series==='IORB').received,'Unavailable');
 assert.equal(partial.summary.find(r=>r.series==='DGS30').received,'Not requested');
 const zero=await load(fixture('zero'));assert.equal(zero.data.fed_funds.value,0);
 const empty=fixture('unavailable');assert.equal(empty.packet.original_bytes_replayed,false);assert.equal(spec(empty).status,'unavailable');
 const complete=await load(fixture());assert.equal(complete.data.observations.DGS10.status,'partial');
 assert.equal(complete.summary.find(r=>r.series==='DGS10').returned_rows,6);
 assert.equal(complete.summary.find(r=>r.series==='DGS10').kept_rows,5);
});
test('headline policy rate requires the matching dated DFF trace and preserves true zero',()=>{
 for(const name of ['complete','partial','cached','zero']){
  const f=fixture(name),value=f.packet.fed_funds.value;
  assert.equal(api.policyContext(f.packet,AT,value).value,value);
  assert.equal(api.policyContext(f.packet,AT,value+1),null);
 }
 assert.equal(api.policyContext(fixture('unavailable').packet,AT,4.25),null);
 const f=fixture();for(const change of [{unit:'bp'},{observation_date:'2000-01-01'},{value:true},{maximum_age_calendar_days:99},{source_row_index:-1},{calls_eligible:true},{selection:'skip_missing'}]){
  const packet=clone(f.packet);Object.assign(packet.fed_funds,change);assert.equal(api.policyContext(packet,AT,4.25),null);
 }
});
test('duplicate-date invalidation and cache acquisition survive inspection',async()=>{
 const bad=await load(fixture('invalid'));assert.equal(bad.summary.find(r=>r.series==='DGS10').kept_rows,0);assert.equal(bad.data.source_frames.DGS10.response.observations.length,2);
 const cached=await load(fixture('cached'));assert.match(cached.summary[0].reuse,/Cache.*300 s/);assert.notEqual(cached.summary[0].acquired,AT);
});
test('forecast or execution promotion never qualifies source inspection',()=>{
 for(const key of ['source_definition_verified','historical_point_in_time_verified','forecast_eligible','calls_eligible','sizing_eligible','execution_eligible']){
  const f=fixture();f.packet[key]=true;assert.throws(()=>spec(f));
 }
 for(const change of [{native_input_selection_replayed:1},{whole_auction_model_replayed:true},{original_bytes_replayed:false},{status:'partial'}])assert.throws(()=>spec({...fixture(),packet:{...fixture().packet,...change}}));
});
test('overlapping missing or foreign series cannot masquerade as full coverage',()=>{
 for(const change of [c=>c.received_series.pop(),c=>c.received_series.push('DFF'),c=>c.unavailable_series.push('DFF'),c=>c.expected_series[0]='other',c=>c.raw_rows=true,c=>c.complete_request_set=false]){
  const f=fixture();change(f.packet.coverage);assert.throws(()=>spec(f));
 }
});
test('private external traversal and oversized references are rejected before any fetch',()=>{
 const ref=fixture().packet.measurements;
 for(const key of ['private/account.json','data/prospective-outcomes.json','https://example.com/x','data/auction-fred-originals/../x.json',ref.key+'?x=1'])assert.throws(()=>api.reference({...ref,key},'measurements'));
 for(const bytes of [0,-1,true,'100',Infinity,48*1024*1024+1])assert.throws(()=>api.reference({...ref,bytes},'measurements'));
 assert.throws(()=>api.reference({...ref,additional:true},'measurements'));
});
test('stale future malformed and unmatched publication clocks withhold the links',()=>{
 const f=fixture();for(const at of ['2000-01-01T00:00:00Z','2999-01-01T00:00:00Z','2026-02-31T00:00:00Z','2026-09-29','0000-01-01T00:00:00Z']){
  assert.throws(()=>api.specification({...f.packet,generated_at:at},at));
 }
 assert.throws(()=>api.specification(f.packet,AT,Date.parse(AT)+49*3600000));
 assert.throws(()=>api.specification(f.packet,new Date(Date.parse(AT)+1000).toISOString()));
});
test('exact complete bytes are checked and credentials are omitted',async()=>{
 const f=fixture();let calls=0;
 const result=await api.loadVerified(spec(f),async(url,options)=>{calls++;assert.equal(url,'/'+f.packet.measurements.key+'?exact=1&nogen=1');assert.equal(options.credentials,'omit');return new Response(f.raw);},webcrypto);
 assert.equal(calls,1);assert.equal(result.summary.length,17);
 for(const raw of [f.raw.slice(0,-1),f.raw+' ',f.raw.replace('4.25','4.26')])await assert.rejects(api.loadVerified(spec(f),async()=>new Response(raw),webcrypto));
});
test('partial HTTP failures and redirects cannot supply evidence',async()=>{
 const f=fixture();for(const status of [206,401,403,500])await assert.rejects(api.loadVerified(spec(f),async()=>new Response(f.raw,{status}),webcrypto));
 const response=new Response(f.raw);Object.defineProperty(response,'redirected',{value:true});await assert.rejects(api.loadVerified(spec(f),async()=>response,webcrypto));
});
test('oversized artifact streams are cancelled before another chunk is read',async()=>{
 let reads=0,cancelled=0,released=0;const response={body:{getReader:()=>({read:async()=>{reads++;return {done:false,value:new Uint8Array(11)};},cancel:async()=>cancelled++,releaseLock:()=>released++})}};
 await assert.rejects(api.readBounded(response,10));assert.equal(reads,1);assert.equal(cancelled,1);assert.equal(released,1);
});
test('hash-correct artifacts still need complete row selections and source clocks',async()=>{
 const changes=[d=>d.observations.DGS10.rows.pop(),d=>d.observations.DGS10.selected_history[Object.keys(d.observations.DGS10.selected_history)[0]]=999,
  d=>d.observations.DGS10.excluded_rows=[],d=>d.source_frames.DFF.adapter_transport.source_url+='&api_key=secret',
  d=>d.source_frames.DFF.adapter_transport.cache_age_seconds=1800,d=>d.source_frames.DFF.adapter_read_at='2100-01-01T00:00:00Z',
  d=>d.source_frames.DFF.adapter_transport.response_status=true,d=>d.coverage.raw_rows++,d=>delete d.source_frames.DFF,
  d=>d.calls_eligible=true,d=>d.observations.DFF.unit='bp',d=>d.source_frames.extra={}];
 for(const change of changes)await assert.rejects(load(rewrite(fixture(),change)));
});
test('render creates no automatic data request and every link is typed',()=>{
 const root=dom();let calls=0;global.fetch=()=>{calls++;throw Error('Unexpected')};const f=fixture();api.render(root,f.packet,AT);
 assert.equal(calls,0);assert.match(root.textContent,/17 of 17/);assert.match(root.textContent,/separate from measurement/);
 assert.ok(all(root,n=>n.tagName==='a').every(n=>n.href.startsWith('/data/auction-fred-originals/')));
 api.render(root,{...f.packet,manifest:{...f.packet.manifest,key:'private/account.json'}},AT);assert.equal(all(root,n=>n.tagName==='a').length,0);
});
test('actual full-field inspector and complete accessible table are rendered',async()=>{
 const root=dom(),f=fixture();global.fetch=async()=>new Response(f.raw);api.render(root,f.packet,AT);await button(root).onclick();
 assert.match(root.textContent,/Artifact bytes and recorded coverage match/);const table=all(root,n=>n.tagName==='table')[0];
 const tbody=table.children.find(n=>n.tagName==='tbody');assert.equal(tbody.children.length,17);assert.match(tbody.children[16].textContent,/DGS30/);
 const region=all(root,n=>n.attrs.role==='region')[0];assert.equal(region.tabIndex,0);assert.match(region.attrs['aria-label'],/scroll horizontally/);
 assert.ok(all(root,n=>n.tagName==='details').length>0);assert.match(root.textContent,/Complete FRED source and calculation inputs/);
});
test('HTTP failure remains retryable and a later successful load recovers',async()=>{
 const root=dom(),f=fixture();let attempt=0;global.fetch=async()=>++attempt===1?new Response('denied',{status:403}):new Response(f.raw);
 api.render(root,f.packet,AT);const open=button(root);await open.onclick();assert.match(root.textContent,/Inspection unavailable/);assert.equal(open.disabled,false);
 await open.onclick();assert.match(root.textContent,/Artifact bytes and recorded coverage match/);assert.equal(open.disabled,false);
});
test('a changed or stale publication aborts loading and never repaints prior evidence',async()=>{
 const root=dom(),f=fixture();let resolve,signal;global.fetch=(url,options)=>{signal=options.signal;return new Promise(done=>resolve=done);};
 api.render(root,f.packet,AT);const running=button(root).onclick();api.render(root,null,null);assert.equal(signal.aborted,true);resolve(new Response(f.raw));await running;
 assert.doesNotMatch(root.textContent,/DGS30|Artifact bytes and recorded coverage/);assert.match(root.textContent,/not available/);
 global.fetch=async()=>new Response(f.raw);api.render(root,f.packet,AT);await button(root).onclick();assert.match(root.textContent,/DGS30/);
});
test('real auction renderer binds the FRED sidecar and publication clock',()=>{
 const elements=new Map(),calls=[];function get(id){if(!elements.has(id))elements.set(id,new Element('div'));return elements.get(id);}
 const scope={console,Date,Math,JSON,Number,String,Array,Object,Set,Map,Promise,fetch:()=>new Promise(()=>{}),setInterval:()=>{},setTimeout:()=>{},
  document:{getElementById:get,addEventListener:()=>{},querySelectorAll:()=>[],createElement:tag=>new Element(tag)},window:{addEventListener:()=>{}},
  localStorage:{getItem:()=>null},JHAuctionFredEvidence:{render:(...args)=>calls.push(args)},packet:{generated_at:AT,fred_source:fixture().packet}};
 scope.globalThis=scope;vm.createContext(scope);vm.runInContext(fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8'),scope);
 vm.runInContext('DATA=packet;renderResearchContext()',scope);assert.equal(calls.length,1);assert.equal(calls[0][0],get('auction-fred-evidence'));assert.equal(calls[0][1],scope.packet.fred_source);assert.equal(calls[0][2],AT);
 vm.runInContext('renderHero()',scope);assert.equal(get('fed-rate').textContent,'—');
 scope.JHAuctionFredEvidence.policyContext=api.policyContext;scope.packet.fed_funds_rate=4.25;
 vm.runInContext('renderHero()',scope);assert.equal(get('fed-rate').textContent,'4.25');assert.match(get('fed-rate-date').textContent,/DFF observed/);
 const html=fs.readFileSync(path.join(__dirname,'../auction-crisis.html'),'utf8');assert.ok(html.indexOf('/jh-auction-fred-evidence.js')<html.indexOf('/auction-crisis.js'));assert.match(html,/jaf-table-scroll:focus-visible/);
 assert.match(html,/auction-crisis\.js\?t=20260929-dated-concerns/);assert.match(html,/jh-auction-fred-evidence\.js\?v=20260929-fred-v1/);
});
