const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto');
const ui=require('../jh-liquidity-agent-research.js');
// Complete local mock publication from retained public originals, not an AWS receipt.
const fixture=JSON.parse(fs.readFileSync(__dirname+'/fixtures/liquidity-agent-native-view.json'));
const now=Date.parse(fixture.generated_at),packet=()=>structuredClone(fixture);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function record(value,category){const body=JSON.stringify(value),hash=sha(body);return {body,ref:{key:'data/liquidity-agent-research/'+category+'/'+hash+'.json',sha256:hash,bytes:Buffer.byteLength(body)}};}
function retained(){
 const p=packet(),{replay,...body}=p,view=record(body,'views');
 const manifest={contract:'liquidity-agent-replay.v1',generated_at:p.generated_at,output_sha256:p.replay.output_sha256,view:view.ref,series:Object.fromEntries(Object.entries(p.series).map(([sid,r])=>[sid,r.complete_history_artifact]))};
 const run=record(manifest,'runs');p.replay.manifest_key=run.ref.key;
 const files={[run.ref.key]:run.body,[view.ref.key]:view.body},requests=[];
 const fetcher=async(url,options)=>{requests.push({url,options});const raw=files[url.slice(1).split('?')[0]];return new Response(raw||'missing',{status:raw?200:404});};
 return {p,files,requests,fetcher,manifest,view,run};
}
test('all 73 identities survive, with 61 native-unit measurements and twelve explicit gaps',()=>{
 const p=packet(),v=ui.view(p,now);assert(v.contract);assert.equal(v.available,61);assert.equal(v.rows.length,73);
 assert.equal(v.rows.find(r=>r.sid==='WALCL').value,'6747704');assert.equal(v.rows.find(r=>r.sid==='DPCREDIT').unit,'Percent');
 assert.equal(v.rows.find(r=>r.sid==='EXCSRESNW').value,'Unavailable');
 const html=ui.panelHTML(p,now);assert.match(html,/145|WALCL/);assert.match(html,/73 remain/);assert.match(html,/no position size/);
 assert.match(ui.panelHTML(p,now,'DPCREDIT'),/1 series shown/);
});
test('definition changes, acquisition expiry, future clocks and invented authority withhold current data',()=>{
 for(const change of [p=>p.sizing_eligible=true,p=>p.generated_at='bad',p=>p.generated_at='2026-09-31T01:00:00Z',p=>p.source_generated_at='2026-09-01T00:00:00Z',p=>p.replay.manifest_key='data/portfolio.json']){
  const p=packet();change(p);assert.equal(ui.view(p,now).available,0);
 }
 for(const change of [r=>r.unit='Billions of Dollars',r=>r.acquired_at='2026-09-01T00:00:00Z',r=>r.latest_date='2026-13-99',r=>r.source_definition.id='DGS10',r=>r.calls_eligible=true,r=>r.current.exact_decimal='123',r=>r.evidence.observations.key='data/portfolio.json']){
  const p=packet();change(p.series.WALCL);assert.equal(ui.view(p,now).rows.find(r=>r.sid==='WALCL').value,'Unavailable');
 }
 assert.equal(ui.view(packet(),now+27*3600000).available,0);
});
test('zero is an observation; text is escaped and bad calendar endpoints never become a comparison',()=>{
 const p=packet(),r=p.series.WALCL;r.current={value:0,exact_decimal:'0'};assert.equal(ui.view(p,now).rows[0].value,'0');
 r.label='<img src=x onerror=alert(1)>';assert(!ui.panelHTML(p,now).includes('<img'));
 r.calendar_comparisons.month.baseline_date='2099-01-01';assert.equal(ui.comparison(r,'month'),'Unavailable');
});
test('immutable run and compact view are verified without loading the full 21 MB output',async()=>{
 const f=retained();const result=await ui.verifyView(f.p,f.fetcher,crypto.webcrypto);assert.deepEqual(result,f.manifest);
 assert.equal(f.requests.length,2);assert(f.requests.every(r=>r.options.credentials==='omit'&&r.options.cache==='no-store'));
 for(const key of [f.run.ref.key,f.view.ref.key]){const raw=f.files[key];f.files[key]='{}';await assert.rejects(ui.verifyView(f.p,f.fetcher,crypto.webcrypto));f.files[key]=raw;}
 f.p.series.WALCL.current.value=1;await assert.rejects(ui.verifyView(f.p,f.fetcher,crypto.webcrypto));
});
test('bad publication coordinates are rejected before any read',async()=>{
 const p=packet();p.replay.manifest_key='data/portfolio.json';let reads=0;
 await assert.rejects(ui.verifyView(p,async()=>{reads++;},crypto.webcrypto));assert.equal(reads,0);
 p.series.WALCL.complete_history_artifact.key='audit-private/secret';await assert.rejects(ui.verifySeries(p.series.WALCL,async()=>{reads++;},crypto.webcrypto));assert.equal(reads,0);
});
test('whole selected history and every page remain available, including explicit missing values',async()=>{
 const summary=packet().series.WALCL,{complete_history_artifact,retained_original_rows,...r}=summary;
 const full={...r,history:Array.from({length:123},(_,i)=>({original_row:i,observation_date:'2026-09-23',native_value:i===0?'.':String(i),realtime_start:'2026-09-24',realtime_end:'2026-09-24'}))};
 const artifact=record(full,'series');summary.complete_history_artifact=artifact.ref;summary.retained_original_rows=123;
 const fetched=await ui.verifySeries(summary,async()=>new Response(artifact.body),crypto.webcrypto);assert.equal(fetched.history.length,123);
 assert.match(ui.historyHTML(fetched,0),/Rows 1–50 of 123/);assert.match(ui.historyHTML(fetched,2),/Rows 101–123 of 123/);
 summary.retained_original_rows=122;await assert.rejects(ui.verifySeries(summary,async()=>new Response(artifact.body),crypto.webcrypto));
});
test('original verification binds complete definitions and exact decimals, refusing changed rows and bytes',async()=>{
 const r=packet().series.WALCL;r.original_row=0;
 const docs={definition:{seriess:[r.source_definition]},observations:{observations:[{date:r.latest_date,value:r.last_observed.exact_decimal}]}},files={};
 for(const [kind,doc] of Object.entries(docs)){const raw=JSON.stringify(doc),hash=sha(raw);r.evidence[kind].sha256=hash;r.evidence[kind].bytes=Buffer.byteLength(raw);r.evidence[kind].key='data/evidence/fred/'+'c'.repeat(64)+'/'+hash+'.bin.gz';files[r.evidence[kind].key]=raw;}
 const fetcher=async url=>new Response(require('node:zlib').gzipSync(Buffer.from(files[url.slice(1).split('?')[0]])));
 assert.match(await ui.verifyOriginal(r,fetcher,crypto.webcrypto),/Original definition and observation hashes verified/);
 r.last_observed.exact_decimal='9007199254740993';await assert.rejects(ui.verifyOriginal(r,fetcher,crypto.webcrypto));
 assert.notEqual(ui.exactDecimal('9007199254740992'),ui.exactDecimal('9007199254740993'));
 await assert.rejects(ui.verifyOriginal(r,async()=>new Response('{}'),crypto.webcrypto));
});
test('page and legacy dashboard consume the native contract without leaking a prior current reading',()=>{
 const page=fs.readFileSync('liquidity.html','utf8'),dashboard=fs.readFileSync('classic-dashboard.html','utf8');
 assert.match(page,/JHLiquidityAgentResearch\.install\(window\)/);assert.match(page,/JHLiquidityAgentResearch\.accept\(null\)/);
 assert.match(page,/mine !== agentFetchEpoch/);assert.match(page,/liquidity-legacy-panels/);
 assert.match(dashboard,/JHLiquidityAgentResearch\.view\(liq\)/);assert(!dashboard.includes('Math.abs(v) >= 100000'));
});
