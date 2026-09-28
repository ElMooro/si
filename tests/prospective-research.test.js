const test=require('node:test');
const assert=require('node:assert/strict');
const {view,outcomeView}=require('../jh-prospective-research.js');
const {captureMatches,outcomeMatches,readJson,loadResearch,mount}=require('../jh-prospective-research.js');
const crypto=require('node:crypto').webcrypto;
const hash=bytes=>require('node:crypto').createHash('sha256').update(bytes).digest('hex');
const now=Date.parse('2026-09-18T20:00:00Z');
function packet(){return {schema_version:'prospective-research-summary.v1',generated_at:'2026-09-18T19:00:00Z',
  capture:{key:'data/research-forecasts/captures/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64)},
  protocol:{key:'data/research-forecasts/protocols/'+'b'.repeat(64)+'.json',sha256:'b'.repeat(64)},
  records_in_capture:0,new_records:0,rank_observations:12,ineligible_sources:3,unsupported_identity_count:2,
  coverage:{candidate_scan_complete:true},sizing_eligible:false,promotion_eligible:false};}
test('zero captured forecasts stays zero without implying a performance result',()=>{
  const v=view(packet(),now);assert.equal(v.ok,true);assert.equal(v.records,0);assert.equal(v.ranks,12);
});
test('stale counts, authority changes and arbitrary links cannot appear valid',()=>{
  for(const edit of [p=>p.generated_at='2026-09-16T00:00:00Z',p=>p.sizing_eligible=true,
    p=>p.capture.key='https://example.com',p=>p.new_records=1,p=>p.records_in_capture=null]){
    const p=packet();edit(p);assert.equal(view(p,now).ok,false);
  }
});
test('partial capture stays partial',()=>{
 const p=packet();p.coverage.candidate_scan_complete=false;assert.equal(view(p,now).complete,false);
});
test('future windows stay pending rather than a zero percent performance result',()=>{
 const p={schema_version:'prospective-outcome-batch.v1',generated_at:'2026-09-18T19:00:00Z',
  sizing_eligible:false,promotion_eligible:false,forecasts_checked:42,status_counts:{PENDING_FORWARD_WINDOW:84},
  net_return_pct:null,portfolio_pnl:null,batch:{key:'data/research-forecasts/evaluation-runs/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64)}};
 const v=outcomeView(p,now);assert.equal(v.ok,true);assert.equal(v.measured,0);assert.equal(v.pending,84);
 p.net_return_pct=0;assert.equal(outcomeView(p,now).ok,false);
 p.net_return_pct=null;p.status_counts={PROFITABLE:42};assert.equal(outcomeView(p,now).ok,false);
});

test('whole predecessor rejects a new echo exclusion while repaired view preserves its separate meaning',()=>{
 const vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
 const context={module:{exports:{}}};vm.createContext(context);
 vm.runInContext(fs.readFileSync(path.join(__dirname,'fixtures/pre-research-self-ingestion-page.js.txt'),'utf8'),context);
 const p={schema_version:'prospective-outcome-batch.v1',generated_at:'2026-09-18T19:00:00Z',
  sizing_eligible:false,promotion_eligible:false,forecasts_checked:3,status_counts:{RESEARCH_OUTPUT_ECHO:2,UNSUPPORTED_SOURCE_IDENTITY:1},
  net_return_pct:null,portfolio_pnl:null,batch:{key:'data/research-forecasts/evaluation-runs/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64)}};
 assert.equal(context.module.exports.outcomeView(p,now).ok,false);
 const result=outcomeView(p,now);assert.equal(result.ok,true);assert.equal(result.echoes,2);
 assert.equal(result.excluded,1);assert.equal(result.measured,0);assert.equal(result.pending,0);
 p.status_counts.RESEARCH_OUTPUT_ECHO=true;assert.equal(outcomeView(p,now).ok,false);
});

test('capture cannot borrow a source selection policy absent from its retained body',()=>{
 const {head,doc}=retainedFixture();
 head.source_selection_policy={contract:'research-input-selection.v1',excluded_sources:['data/prospective-research.json']};
 assert.equal(captureMatches(head,doc),false);
 doc.source_selection_policy=structuredClone(head.source_selection_policy);assert.equal(captureMatches(head,doc),true);
 doc.source_selection_policy.excluded_sources=[];assert.equal(captureMatches(head,doc),false);
});

test('unsupported identities are excluded records, never outcomes or price windows',()=>{
 const p={schema_version:'prospective-outcome-batch.v1',generated_at:'2026-09-18T19:00:00Z',
  sizing_eligible:false,promotion_eligible:false,forecasts_checked:3,
  status_counts:{UNSUPPORTED_SOURCE_IDENTITY:1,PENDING_FORWARD_WINDOW:4},
  net_return_pct:null,portfolio_pnl:null,batch:{key:'data/research-forecasts/evaluation-runs/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64)}};
 const v=outcomeView(p,now);assert.equal(v.ok,true);assert.equal(v.excluded,1);
 assert.equal(v.measured,0);assert.equal(v.pending,4);assert.equal(v.gaps,0);
 p.status_counts.UNSUPPORTED_SOURCE_IDENTITY=-1;assert.equal(outcomeView(p,now).ok,false);
});

function retainedFixture(){
 const head=packet();head.coverage={candidate_scan_complete:false};
 const doc={contract:'prospective-research-capture.v1',generated_at:head.generated_at,
  sizing_eligible:false,promotion_eligible:false,protocol_ref:head.protocol,coverage:head.coverage,
  records:[],sources:[0,1,2].map((_,i)=>({observations:Array.from({length:4},()=>({origin:'rank_observation'})),
   eligibility_reasons:['source_publication_clock_missing'],unsupported_identity_count:i===0?2:0}))};
 const raw=JSON.stringify(doc),sha=hash(raw);head.capture={key:'data/research-forecasts/captures/'+sha+'.json',sha256:sha};
 return {head,doc,raw};
}
test('capture counts reconcile every retained source without treating gaps as records',()=>{
 const {head,doc}=retainedFixture();assert.equal(captureMatches(head,doc),true);
 for(const edit of [d=>d.sources.pop(),d=>d.records.push({created:true}),d=>d.sources[0].observations.pop(),
  d=>d.generated_at='2026-09-18T18:00:00Z',d=>d.coverage={candidate_scan_complete:true},
  d=>d.protocol_ref={},d=>d.identity_policy={files:{wrong:'digest'}},d=>d.source_read_policy={reader_sha256:'changed'}]){
  const changed=structuredClone(doc);edit(changed);assert.equal(captureMatches(head,changed),false);
 }
});
test('outcome counts reconcile rows and the entire retained batch, including source identity exclusions',()=>{
 const doc={results:[{status:'UNSUPPORTED_SOURCE_IDENTITY'},{status:'PENDING_FORWARD_WINDOW'}],
  status_counts:{PENDING_FORWARD_WINDOW:1,UNSUPPORTED_SOURCE_IDENTITY:1},compiler:{original:'same'}};
 const head={...structuredClone(doc),batch:{key:'reference'}};
 assert.equal(outcomeMatches(head,doc),true);
 doc.results.pop();assert.equal(outcomeMatches(head,doc),false);
 head.results.pop();assert.equal(outcomeMatches(head,doc),false);
});
test('load verifies exact retained bytes and never follows an arbitrary or private reference',async()=>{
 const {head,raw}=retainedFixture();const urls=[];
 const fetcher=async(url,options)=>{urls.push(url);assert.equal(options.redirect,'error');
  return new Response(url.startsWith('/data/prospective-research.json')?JSON.stringify(head):raw);};
 const state=await loadResearch('capture',{fetcher,crypto,now});assert.equal(state.retainedVerified,true);
 assert.equal(urls.length,2);assert.ok(urls.every(u=>u.endsWith('?exact=1&nogen=1')));
 await assert.rejects(loadResearch('capture',{fetcher:async u=>new Response(u.startsWith('/data/prospective-research.json')?JSON.stringify(head):raw+' '),crypto,now}),/bytes differ/);
 head.capture.key='data/private/account.json';urls.length=0;
 await assert.rejects(loadResearch('capture',{fetcher,crypto,now}),/contract/);assert.equal(urls.length,1);
});
test('stream limits, invalid UTF-8 and body-inclusive deadlines fail without partial data',async()=>{
 await assert.rejects(readJson('/data/example.json',{fetcher:async()=>new Response('123456'),limit:5}),/bound/);
 await assert.rejects(readJson('/data/example.json',{fetcher:async()=>new Response(new Uint8Array([0xff]))}));
 let aborted=false,cancelled=false;
 const fetcher=async(_,options)=>{options.signal.addEventListener('abort',()=>aborted=true);
  return new Response(new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('{'));},cancel(){cancelled=true;}}));};
 await assert.rejects(readJson('/data/example.json',{fetcher,timeout:10}),/timed out/);
 assert.equal(aborted,true);assert.equal(cancelled,true);
});
test('the outcome panel renders even while capture is stalled, and failed capture shows no counts',async()=>{
 class Element{
  constructor(tag){this.tag=tag;this.children=[];this.textContent='';}
  appendChild(node){node.parent=this;this.children.push(node);return node;}
  replaceChildren(){this.children=[];}
  remove(){this.parent.children=this.parent.children.filter(n=>n!==this);}
  text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
 }
 const original=global.document;global.document={createElement:tag=>new Element(tag)};
 const host=new Element('div');let rejectCapture;
 try{
  const pending=mount(host,kind=>kind==='capture'?new Promise((_,reject)=>rejectCapture=reject):Promise.resolve({
   checked:1,measured:0,pending:0,gaps:0,excluded:1,deferred:0,batch:'/data/research-forecasts/evaluation-runs/a.json',at:'fixture'}));
  await new Promise(resolve=>setImmediate(resolve));
  assert.match(host.children[1].text(),/1 records excluded/);
  assert.match(host.children[0].text(),/Checking complete retained/);
  rejectCapture(new Error('fixture'));await pending;
  assert.match(host.children[0].text(),/failed verification/);
  assert.doesNotMatch(host.children[0].text(),/registered directions|SHA-256/);
 }finally{global.document=original;}
});
