const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path');
const G=require('../jh-genealogy-research.js'),copy=x=>structuredClone(x),P='data/signal-genealogy-research/';
function reference(value,kind='artifacts'){const text=JSON.stringify(value),sha256=crypto.createHash('sha256').update(text).digest('hex');return {ref:{key:P+kind+'/'+sha256+'.json',bytes:Buffer.byteLength(text),sha256},text};}
function head(n=1){
 const rows=Array.from({length:n},(_,i)=>({instrument_id:'equity:US:A'+String.fromCharCode(65+Math.floor(i/26))+String.fromCharCode(65+i%26),direction:'UP',source_a:'data/a.json',source_b:'data/b.json',a_lower_exclusive_utc:null,a_upper_inclusive_utc:'2026-09-18T12:00:00.123456+00:00',b_lower_exclusive_utc:null,b_upper_inclusive_utc:'2026-09-18T13:00:00+00:00',interval_order:'unresolved_prior_observation_missing',shared_presence_capture_keys:[],causal_order_qualified:false,independent_evidence_count:null}));
 return {contract:'genealogy-research-head.v1',engine:'signal-genealogy',version:'2.0.0',generated_at:'2026-09-28T18:00:00+00:00',input_sha256:'a'.repeat(64),replay:reference({},'runs').ref,
  authority:{calls_eligible:false,execution_eligible:false,forecast_qualified:false,sizing_eligible:false},independent_evidence_count:null,original_engine_replay_verified:false,
  coverage:{captures:5,complete_capture_scans:2,partial_capture_scans:3,registered_records:4,retained_records:3,excluded_records:1,ineligible_source_snapshots:7,identity_partial_source_snapshots:2,first_observed_groups:2,possible_comparisons:n},comparisons:rows,comparison_status_counts:n?{unresolved_prior_observation_missing:n}:{}};
}
function archived(h){
 const record={instrument_id:'equity:US:AAA',direction:'UP',source_key:'data/a.json',registered_at:'2026-09-18T12:00:00+00:00'};
 const output={contract:'signal-genealogy-research.v2',generated_at:h.generated_at,input_sha256:h.input_sha256,authority:h.authority,independent_evidence_count:null,original_engine_replay_verified:false,
  registration:{first_registrations:[record,record],records:[record,record,record],exclusions:[{...record,reasons:['missing']}],same_instrument_comparisons:h.comparisons},membership:{comparisons:h.comparisons,first_observations:[record,record],intervals:[record,record,record]}};
 const outputRef=reference(output),other=reference({}),manifest={contract:'genealogy-streamed-replay.v1',cutoff:h.generated_at,files:{inputs:{...other.ref,key:P+'artifacts/'+h.input_sha256+'.json',sha256:h.input_sha256},head:other.ref,membership:other.ref,output:outputRef.ref}},run=reference(manifest,'runs');h.replay=run.ref;
 const bodies=new Map([[run.ref.key,run.text],[outputRef.ref.key,outputRef.text]]);return {output,manifest,bodies};
}
function fetcher(bodies,calls=[]){return async url=>{calls.push(url);const key=url.replace(/^\//,'').split('?')[0];return bodies.has(key)?new Response(bodies.get(key)):new Response('',{status:403});};}
function document(){const nodes=new Map();return {getElementById:id=>{if(!nodes.has(id))nodes.set(id,{textContent:'',innerHTML:'',value:id==='genealogy-population'?'comparisons':'',disabled:false});return nodes.get(id);}};}

test('Complete comparison population paginates without losing source coordinates or unresolved bounds',()=>{
 const h=head(103),m=G.model(h);assert.equal(m.rows.length,103);const page=G.windowRows(m.rows,'',2);assert.equal(page.rows.length,3);assert.equal(page.start,101);assert.equal(page.rows[2].path,'/comparisons/102');assert.match(G.table(page),/prior observation unavailable/);assert.match(G.table(page),/Unresolved: prior observation missing/);
 const filtered=G.windowRows(m.rows,'AAZ',90);assert.equal(filtered.total,1);assert.equal(filtered.page,0);assert.equal(filtered.rows[0].path,'/comparisons/25');
 assert.equal(G.windowRows(m.rows,'does-not-exist',4).total,0);assert.equal(G.model(head(0)).rows.length,0);
});
test('Malformed totals, legacy packets, fake authority and invalid dates cannot render as current research',()=>{
 const mutations=[p=>p.coverage.registered_records++,p=>p.coverage.captures=false,p=>p.coverage.possible_comparisons++,p=>p.comparisons.push(copy(p.comparisons[0])),p=>p.comparisons[0].a_lower_exclusive_utc='',p=>p.comparison_status_counts.unresolved_prior_observation_missing++,p=>p.authority.sizing_eligible=true,p=>p.independent_evidence_count=1,p=>p.original_engine_replay_verified=true,p=>p.generated_at='2026-02-30T12:00:00Z',p=>p.comparisons[0].causal_order_qualified=true,p=>p.comparisons[0].source_a='https://private.invalid',p=>p.contract='legacy'];
 for(const change of mutations){const h=head();change(h);assert.throws(()=>G.model(h));}assert.throws(()=>G.model(null));
 assert.equal(G.clock('2026-09-18T12:00:00.123456+00:00'),Date.parse('2026-09-18T12:00:00.123Z'));
});
test('Every received field and record is escaped and source labels never become arbitrary URLs',()=>{
 const html=G.table({rows:[{row:{source_key:'<img src=x onerror=alert(1)>',reasons:['<script>'],registered_at:'<b>',direction:'UP'},path:'/registration/records/0'}],total:1});
 assert.ok(!html.includes('<img'));assert.ok(!html.includes('<script>'));assert.match(html,/&lt;img/);assert.ok(!html.includes('href='));assert.match(html,/registration\/records\/0/);
});
test('Retained calculation is full-byte verified through the selected head and every population is preserved',async()=>{
 const h=head(),a=archived(h),calls=[],result=await G.loadArchive(h,fetcher(a.bodies,calls));assert.equal(result.populations['registration/records'].length,3);assert.equal(result.populations['registration/exclusions'].length,1);assert.equal(result.populations['membership/intervals'].length,3);assert.equal(calls.length,2);assert.ok(calls.every(x=>x.startsWith('/'+P)&&x.endsWith('?exact=1&nogen=1')));
 const bad=new Map(a.bodies);bad.set(a.manifest.files.output.key,JSON.stringify({...a.output,generated_at:'2026-09-27T18:00:00+00:00'}));await assert.rejects(()=>G.loadArchive(h,fetcher(bad)),/artifact bytes differ/);
 const missing=copy(a.output);missing.registration.records.pop();assert.throws(()=>G.archiveModel(missing,h),/population incomplete/);
 const diff=copy(a.output);diff.membership.comparisons[0].direction='DOWN';assert.throws(()=>G.archiveModel(diff,h),/comparisons differ/);
 for(const r of [{...h.replay,key:'data/prospective-outcomes.json'},{...h.replay,bytes:true},{...h.replay,sha256:'A'.repeat(64)}])assert.throws(()=>G.ref(r,'runs'));
});
test('Bad or missing public responses retain honest errors without interpreting HTML or zero as data',async()=>{
 await assert.rejects(()=>G.read(P+'current.json',async()=>new Response('denied',{status:403})),/403/);
 try{await G.read(P+'current.json',async()=>new Response('<html>failure</html>'));assert.fail();}catch(e){assert.equal(e.original,'<html>failure</html>');}
 const h=head(),doc=document(),calls=[],app=G.mount(doc,fetcher(new Map([[P+'current.json',JSON.stringify(h)]]),calls));await app.ready;
 assert.equal(app.state.head.generated_at,h.generated_at);assert.equal(doc.getElementById('genealogy-query').disabled,false);assert.equal(calls.length,1);app.dispose();
 const badDoc=document(),bad=G.mount(badDoc,async()=>new Response('',{status:403}));await bad.ready;assert.equal(badDoc.getElementById('genealogy-archive').disabled,true);assert.match(badDoc.getElementById('genealogy-status').textContent,/Unavailable.*403/);assert.equal(bad.state.head,null);bad.dispose();
});
test('Delayed reload cannot overwrite the newest publication and failed reload clears all stale populations',async()=>{
 const old=head(),fresh=head();fresh.generated_at='2026-09-28T19:00:00+00:00';let resolve,requests=0;
 const pending=new Promise(r=>resolve=r),doc=document(),app=G.mount(doc,async()=>{requests++;return requests===1?pending:new Response(JSON.stringify(fresh));});
 await app.refresh();resolve(new Response(JSON.stringify(old)));await app.ready;assert.equal(app.state.head.generated_at,fresh.generated_at);app.dispose();
 let pass=true;const d=document(),b=G.mount(d,async()=>pass?new Response(JSON.stringify(fresh)):new Response('',{status:403}));await b.ready;d.getElementById('genealogy-archive-original').textContent='old archive';pass=false;await b.refresh();assert.equal(b.state.head,null);assert.deepEqual(b.state.groups,{});assert.equal(d.getElementById('genealogy-archive-original').textContent,'');assert.equal(d.getElementById('genealogy-query').disabled,true);b.dispose();
});
test('Whole predecessor is retained and the new accessible page fetches only reviewed chronology',()=>{
 const prior=fs.readFileSync(path.join(__dirname,'fixtures/pre-genealogy-research-page.html.txt'));
 assert.equal(crypto.createHash('sha256').update(prior).digest('hex'),'7a1199f37bed817eb14d21fd09a93c3270e733498342b7243aeec1daa4a3fc5b');assert.match(prior.toString(),/Which Signals Lead/);
 const html=fs.readFileSync(path.join(__dirname,'../signal-genealogy.html'),'utf8');assert.ok(!html.includes('data/signal-genealogy.json'));assert.ok(!html.includes('jh-page-ai.js'));assert.match(html,/jh-genealogy-research.js/);assert.match(html,/<label for="genealogy-query">/);assert.match(html,/aria-label="Genealogy research records[^>]+tabindex="0"/);assert.match(html,/data\/market-tape.json/);
 const registry=JSON.parse(fs.readFileSync(path.join(__dirname,'../config/page-explanation-sources.json'),'utf8'));assert.ok(JSON.stringify(registry).includes(P+'current.json'));
});
