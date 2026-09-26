const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto');
const ui=require('../jh-yield-curve-research.js');
// Complete local mock publication from retained public originals, not an AWS receipt.
const fixture=JSON.parse(fs.readFileSync(__dirname+'/fixtures/yield-curve-native-view.json'));
const now=Date.parse(fixture.generated_at),packet=()=>structuredClone(fixture);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function record(value,category){const body=JSON.stringify(value),hash=sha(body);return {body,ref:{key:'data/yield-curve-research/'+category+'/'+hash+'.json',sha256:hash,bytes:Buffer.byteLength(body)}};}
function retained(){
 const p=packet(),{replay,...body}=p,view=record(body,'views');
 const manifest={contract:'yield-curve-replay.v1',generated_at:p.generated_at,output_sha256:p.replay.output_sha256,view:view.ref,series:Object.fromEntries(Object.entries(p.series).map(([sid,r])=>[sid,r.complete_history_artifact]))};
 const run=record(manifest,'runs');p.replay.manifest_key=run.ref.key;
 const files={[run.ref.key]:run.body,[view.ref.key]:view.body},requests=[];
 const fetcher=async(url,options)=>{requests.push({url,options});const raw=files[url.slice(1).split('?')[0]];return new Response(raw||'missing',{status:raw?200:404});};
 return {p,files,requests,fetcher,manifest,view,run};
}
test('all 23 identities, 16 curve points and twelve declared metrics remain visible',()=>{
 const p=packet(),v=ui.view(p,now);assert(v.contract);assert.equal(v.available,23);assert.equal(v.rows.length,23);
 assert.equal(ui.curve(p,'nominal',now).length,11);assert.equal(ui.curve(p,'real',now).length,5);assert.equal(ui.curve(p,'nominal',now)[0].x,1);
 assert.equal(Object.keys(ui.METRICS).length,12);for(const name of Object.keys(ui.METRICS))assert(ui.derived(p,name,now));
 assert.match(ui.panelHTML(p,now),/all 23 remain/);assert.match(ui.panelHTML(p,now),/not calendar periods/);assert.match(ui.panelHTML(p,now,'DFII20'),/1 series shown/);
 assert.match(ui.metricsHTML(p,now),/target band width/);assert.match(ui.metricsHTML(p,now),/Formula and source legs/);
});
test('invalid authority, definitions, clocks and source coordinates withhold current measurements',()=>{
 for(const change of [p=>p.sizing_eligible=true,p=>p.signals=['LONG'],p=>p.portfolio_consequences.target_weights={SPY:1},p=>p.generated_at='2026-09-31T01:00:00Z',p=>p.source_generated_at='2026-09-01T00:00:00Z',p=>p.replay.manifest_key='data/portfolio.json']){
  const p=packet();change(p);assert.equal(ui.view(p,now).available,0);assert.equal(ui.derived(p,'2s10s',now),null);assert.equal(ui.curve(p,'nominal',now).length,0);
 }
 for(const change of [r=>r.unit='Index',r=>r.acquired_at='2026-09-01T00:00:00Z',r=>r.latest_date='2026-13-99',r=>r.source_definition.id='DGS10',r=>r.calls_eligible=true,r=>r.current.exact_decimal='123',r=>r.evidence.observations.key='data/portfolio.json']){
  const p=packet();change(p.series.DGS2);assert.equal(ui.view(p,now).rows.find(r=>r.sid==='DGS2').value,'Unavailable');assert.equal(ui.derived(p,'2s10s',now),null);assert.equal(ui.curve(p,'nominal',now).length,0);assert.equal(ui.curve(p,'real',now).length,5);
 }
 assert.equal(ui.view(packet(),now+27*3600000).available,0);
});
test('zero and negative yields survive; matched observation steps disclose actual endpoints',()=>{
 const p=packet(),r=p.series.DGS2;r.current={value:0,exact_decimal:'0'};assert.equal(ui.view(p,now).rows.find(r=>r.sid==='DGS2').value,'0');
 r.current={value:-.25,exact_decimal:'-0.25'};assert.equal(ui.view(p,now).rows.find(r=>r.sid==='DGS2').value,'-0.25');
 r.source_definition.title='<img src=x onerror=alert(1)>';assert(!ui.panelHTML(p,now).includes('<img'));
 assert.match(ui.comparison(r,'5'),/calendar days/);r.current_observation_comparisons['5'].baseline.observation_date='2099-01-01';assert.equal(ui.comparison(r,'5'),'Unavailable');
});
test('bad formula, incomplete tenors and a stale aligned date cannot become a current curve metric',()=>{
 const p=packet();p.derived['2s10s'].coefficients.DGS2=100;assert.equal(ui.derived(p,'2s10s',now),null);
 p.curves.nominal.points.pop();assert.equal(ui.curve(p,'nominal',now).length,0);
 p.curves.real.observation_date='2020-01-01';assert.equal(ui.curve(p,'real',now).length,0);
});
test('artifact timeout covers transport and streaming bodies even when they ignore abort',async()=>{
 for(const fetcher of [()=>new Promise(()=>{}),async()=>({ok:true,headers:new Headers(),body:{getReader:()=>({read:()=>new Promise(()=>{}),cancel:async()=>{}})}})])await assert.rejects(ui.bytes('/test',fetcher,100,10),/timed out/);
 await assert.rejects(ui.bytes('/test',async()=>new Response('too big'),2),/bound/);
});

test('immutable run and compact view are verified without loading the full 12 MB output',async()=>{
 const f=retained();const result=await ui.verifyView(f.p,f.fetcher,crypto.webcrypto);assert.deepEqual(result,f.manifest);
 assert.equal(f.requests.length,2);assert(f.requests.every(r=>r.options.credentials==='omit'&&r.options.cache==='no-store'));
 for(const key of [f.run.ref.key,f.view.ref.key]){const raw=f.files[key];f.files[key]='{}';await assert.rejects(ui.verifyView(f.p,f.fetcher,crypto.webcrypto));f.files[key]=raw;}
 f.p.series.DGS2.current.value=1;await assert.rejects(ui.verifyView(f.p,f.fetcher,crypto.webcrypto));
});
test('bad publication coordinates are rejected before any read',async()=>{
 const p=packet();p.replay.manifest_key='data/portfolio.json';let reads=0;
 await assert.rejects(ui.verifyView(p,async()=>{reads++;},crypto.webcrypto));assert.equal(reads,0);
 p.series.DGS2.complete_history_artifact.key='audit-private/secret';await assert.rejects(ui.verifySeries(p.series.DGS2,async()=>{reads++;},crypto.webcrypto));assert.equal(reads,0);
});
test('whole selected history and every page remain available, including explicit missing values',async()=>{
 const summary=packet().series.DGS2,{complete_history_artifact,retained_original_rows,...r}=summary;
 const full={...r,history:Array.from({length:123},(_,i)=>({original_row:i,observation_date:'2026-09-23',native_value:i===0?'.':String(i),realtime_start:'2026-09-24',realtime_end:'2026-09-24'}))};
 const artifact=record(full,'series');summary.complete_history_artifact=artifact.ref;summary.retained_original_rows=123;
 const fetched=await ui.verifySeries(summary,async()=>new Response(artifact.body),crypto.webcrypto);assert.equal(fetched.history.length,123);
 assert.match(ui.historyHTML(fetched,0),/Rows 1–50 of 123/);assert.match(ui.historyHTML(fetched,2),/Rows 101–123 of 123/);
 summary.retained_original_rows=122;await assert.rejects(ui.verifySeries(summary,async()=>new Response(artifact.body),crypto.webcrypto));
});
test('original verification binds complete definitions and exact decimals, refusing changed rows and bytes',async()=>{
 const r=packet().series.DGS2;r.original_row=0;
 const docs={definition:{seriess:[r.source_definition]},observations:{observations:[{date:r.latest_date,value:r.last_observed.exact_decimal}]}},files={};
 for(const [kind,doc] of Object.entries(docs)){const raw=JSON.stringify(doc),hash=sha(raw);r.evidence[kind].sha256=hash;r.evidence[kind].bytes=Buffer.byteLength(raw);r.evidence[kind].key='data/evidence/fred/'+'c'.repeat(64)+'/'+hash+'.bin.gz';files[r.evidence[kind].key]=raw;}
 const fetcher=async url=>new Response(require('node:zlib').gzipSync(Buffer.from(files[url.slice(1).split('?')[0]])));
 assert.match(await ui.verifyOriginal(r,fetcher,crypto.webcrypto),/Original definition and observation hashes verified/);
 r.last_observed.exact_decimal='9007199254740993';await assert.rejects(ui.verifyOriginal(r,fetcher,crypto.webcrypto));
 assert.notEqual(ui.exactDecimal('9007199254740992'),ui.exactDecimal('9007199254740993'));
 await assert.rejects(ui.verifyOriginal(r,async()=>new Response('{}'),crypto.webcrypto));
});
test('page selects native research before the legacy renderer and retains both complete predecessors',()=>{
 const page=fs.readFileSync('yield-curve.html','utf8');assert.match(page,/JHYieldCurveResearch.install\(window/);assert.match(page,/await JHYieldCurveResearch.accept\(d\)/);
 assert(page.indexOf("d?.contract==='yield-curve-research.v1'")<page.indexOf('!JHYieldCurve.validLegacy'));
 const migration=JSON.parse(fs.readFileSync('tests/fixtures/yield-curve-native-migration.json'));for(const ref of migration.complete_predecessors){const raw=fs.readFileSync(ref.predecessor);assert.equal(raw.length,ref.bytes);assert.equal(sha(raw),ref.sha256);}
});

test('failed refreshes clear every current native view and late old verification cannot revive it',async()=>{
 const f=retained(),elements=new Map(),renders=[];
 const doc={getElementById(id){if(!elements.has(id))elements.set(id,{value:id==='yc-horizon'?'5':'',innerHTML:'',textContent:'',hidden:false,addEventListener(){}});return elements.get(id);}};
 const win={document:doc,fetch:f.fetcher,crypto:crypto.webcrypto,setInterval(){},JHYieldCurveResearch:{}};
 ui.install(win,{onrender:p=>renders.push(p)});
 await win.JHYieldCurveResearch.accept(f.p);assert(renders.at(-1));
 await win.JHYieldCurveResearch.accept({...f.p,generated_at:'bad'});assert.equal(renders.at(-1),null);
 let release;const barrier=new Promise(resolve=>{release=resolve;});win.fetch=async(...args)=>{await barrier;return f.fetcher(...args);};
 const pending=win.JHYieldCurveResearch.accept(f.p);await win.JHYieldCurveResearch.accept(null);
 release();await pending;assert.equal(renders.at(-1),null);
});
