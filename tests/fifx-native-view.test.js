const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {webcrypto,createHash}=require('node:crypto'),api=require('../jh-fifx-research.js'),board=require('../jh-fifx-board.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/fifx-native-view.json'),'utf8'));
const NOW=Date.parse(fixture.generated_at),copy=x=>JSON.parse(JSON.stringify(x)),sha=x=>createHash('sha256').update(x).digest('hex');
function wire(){
 const packet=copy(fixture),objects={};
 function put(value,kind,extension='json'){const raw=Buffer.isBuffer(value)?value:Buffer.from(JSON.stringify(value)),digest=sha(raw),key='data/fifx-vol-research/'+kind+'/'+digest+'.'+extension;objects[key]=raw;return {key,sha256:digest,bytes:raw.length};}
 const row=packet.series.DGS10,whole={...copy(row),contract:'fifx-source-candidate.v1',candidate_only:true};
 for(const key of ['complete_source_artifact','retained_original_rows','retained_history_rows','source_arithmetic_contract','current_expires_at'])delete whole[key];
 whole.original_rows=Array.from({length:row.retained_original_rows},(_,i)=>({original_row:i,date:new Date(NOW-86400000*(row.retained_original_rows-i)).toISOString().slice(0,10),value:i%9?'4.01':'.'}));
 whole.history=Array.from({length:row.retained_history_rows},(_,i)=>({...copy(row.last_calculated),date:whole.original_rows[i+30].date,original_row:i+30}));
 const original=Buffer.from('observation_date,DGS10\n'+whole.original_rows.map(r=>r.date+','+r.value).join('\n')+'\n'),rawRef=put(original,'originals','bin');
 const receipt={...whole.receipt,sha256:rawRef.sha256,bytes:original.length};row.receipt=whole.receipt=receipt;
 row.original_sha256=whole.original_sha256=rawRef.sha256;row.original_bytes=whole.original_bytes=rawRef.bytes;
 row.complete_source_artifact=put(whole,'series');
 const sources=Object.fromEntries(api.IDS.map(s=>[s,s==='DGS10'?{original:rawRef,receipt:put(receipt,'receipts')}:{original:null,receipt:null}]));
 const input=put({contract:'fifx-vol-inputs.v1',generated_at:packet.generated_at,sources},'inputs');
 const view={...packet};delete view.replay;
 const run={contract:'fifx-vol-replay.v1',generated_at:packet.generated_at,view:put(view,'views'),input,series:Object.fromEntries(api.IDS.map(s=>[s,packet.series[s].complete_source_artifact]))};
 packet.replay={manifest_key:put(run,'runs').key,view_sha256:run.view.sha256};
 const fetcher=async url=>{const key=url.slice(1).split('?')[0],head=key==='data/fifx-vol.json';return new Response(head?JSON.stringify(packet):objects[key],{status:head||objects[key]?200:404});};
 return {packet,objects,run,whole,rawRef,fetcher};
}
test('all sources retain units, clocks and no investment authority',()=>{
 assert.equal(api.IDS.length,18);assert.equal(api.IDS.filter(s=>api.fresh(fixture,s,NOW)).length,16);
 assert.equal(fixture.series.DGS10.specification.measurement_unit,'basis_points');assert.equal(fixture.series.DEXJPUS.specification.measurement_unit,'percent_log_return');
 assert.equal(fixture.series.VIXCLS.specification.measurement_unit,'index_points');assert.equal(fixture.series['^MOVE'].quality.status,'identity_mismatch');
 assert.equal(fixture.series['^VHSI'].quality.status,'http_error');
});
test('expiry, unreviewed identity and invalid windows cannot remain current',()=>{
 assert.equal(api.IDS.filter(s=>api.fresh(fixture,s,NOW+27*3600000)).length,0);
 for(const edit of [r=>r.current=null,r=>r.quality.status='source_regression',r=>r.source_identity.identity_reviewed=false,r=>r.current.max_interval_days=8,r=>r.specification.measurement_unit='index_points',r=>r.current.estimate.calculated_decimal='999',r=>r.current_expires_at='bad']){
  const p=copy(fixture);edit(p.series.DGS10);assert.equal(api.fresh(p,'DGS10',NOW),false);
 }
});
test('unqualified portfolio consequences and source dependencies remain enforced',()=>{
 for(const edit of [p=>p.calls_eligible=true,p=>p.dependency_graph.independent_votes=18,p=>p.portfolio_consequences.target_weights={SPY:1},p=>p.call='LONG',p=>p.series.DGS10.complete_source_artifact.key='audit-private/secret']){
  const p=copy(fixture);edit(p);assert.equal(api.qualified(p),false);
 }
 assert.equal(api.display({value:0,calculated_decimal:'0'}),'0');assert.equal(api.display({value:null,calculated_decimal:null}),'Unavailable');assert.equal(api.display({value:1,calculated_decimal:'2'}),'Unavailable');
});
test('view hash binds the complete source inventory',async()=>{
 const w=wire();await api.verifyView(w.packet,w.fetcher,webcrypto);
 const p=copy(w.packet);p.series.DGS10.current.estimate.value=999;await assert.rejects(api.verifyView(p,w.fetcher,webcrypto),/view differs/);
 w.objects[w.packet.replay.manifest_key]=Buffer.from('{}');await assert.rejects(api.verifyView(w.packet,w.fetcher,webcrypto),/hash differs/);
});
test('complete source and whole response inspection retain every row',async()=>{
 const w=wire();await api.verifyView(w.packet,w.fetcher,webcrypto);const whole=await api.verifySeries(w.packet,'DGS10',w.fetcher,webcrypto);
 assert.equal(whole.original_rows.length,650);assert.equal(whole.history.length,620);
 const proof=await api.verifyOriginal(w.packet,w.run,'DGS10',w.fetcher,webcrypto);assert.equal(proof.rows,650);assert.equal(proof.bytes,w.rawRef.bytes);
 w.objects[w.rawRef.key]=Buffer.from('wrong original');await assert.rejects(api.verifyOriginal(w.packet,w.run,'DGS10',w.fetcher,webcrypto),/bytes differ/);
});
test('missing or private history and interrupted reads are rejected',async()=>{
 const w=wire();await assert.rejects(api.verifySeries(w.packet,'../../private',w.fetcher,webcrypto));
 w.objects[w.packet.series.DGS10.complete_source_artifact.key]=Buffer.from('{}');await assert.rejects(api.verifySeries(w.packet,'DGS10',w.fetcher,webcrypto),/bytes differ/);
 await assert.rejects(api.bytes('/x',()=>new Promise(()=>{}),100,5),/timed out/);
 await assert.rejects(api.bytes('/x',async()=>new Response('123456'),3),/bound/);
});
function document(){const els={};return {els,getElementById(id){return els[id]??={hidden:false,value:'',innerHTML:'',textContent:'',disabled:false,handlers:{},addEventListener(name,fn){this.handlers[name]=fn;}};}};}
test('mounted page loads complete history, paginates and expires values',async()=>{
 const w=wire(),doc=document();doc.getElementById('fx-kind').value='original';let tick,fail=false;
 const interval=globalThis.setInterval,now=Date.now;globalThis.setInterval=fn=>{tick=fn;return 1;};Date.now=()=>NOW;
 try{
  const app=api.mount(doc,async u=>{if(fail)throw Error('offline');return w.fetcher(u);},webcrypto);await app.refresh();
  assert.equal(doc.els['fx-native'].hidden,false);assert.match(doc.els['fx-status'].textContent,/16 \/ 18/);
  await doc.els['fx-load'].handlers.click();assert.match(doc.els['fx-pagination'].textContent,/1–50 of 650/);
  doc.els['fx-older'].handlers.click();assert.match(doc.els['fx-pagination'].textContent,/51–100 of 650/);
  doc.els['fx-kind'].value='calculated';doc.els['fx-kind'].handlers.change();assert.match(doc.els['fx-pagination'].textContent,/1–50 of 620/);
  await doc.els['fx-original'].handlers.click();assert.match(doc.els['fx-history-status'].textContent,/acquisition receipt verified/);
  Date.now=()=>NOW+27*3600000;tick();assert.match(doc.els['fx-status'].textContent,/0 \/ 18/);assert.match(doc.els['fx-reading'].innerHTML,/Unavailable/);
  fail=true;await app.refresh();assert.equal(doc.els['fx-native'].hidden,true);assert.match(doc.els['fx-status'].textContent,/withheld/);app.destroy();
 }finally{globalThis.setInterval=interval;Date.now=now;}
});
test('failed original verification clears current measurements',async()=>{
 const w=wire(),doc=document(),interval=globalThis.setInterval,now=Date.now;globalThis.setInterval=()=>1;Date.now=()=>NOW;
 try{const app=api.mount(doc,w.fetcher,webcrypto);await app.refresh();w.objects[w.rawRef.key]=Buffer.from('tampered');await doc.els['fx-original'].handlers.click();assert.equal(doc.els['fx-native'].hidden,true);assert.match(doc.els['fx-status'].textContent,/withheld/);app.destroy();}
 finally{globalThis.setInterval=interval;Date.now=now;}
});
test('out-of-order refresh cannot replace a newer verified publication',async()=>{
 const w=wire(),doc=document(),interval=globalThis.setInterval;globalThis.setInterval=()=>1;let release,count=0;
 const fetcher=async u=>{if(u.startsWith('/data/fifx-vol.json')&&count++===0)return new Promise(r=>release=r);return w.fetcher(u);};
 try{const app=api.mount(doc,fetcher,webcrypto);await app.refresh();release(new Response(JSON.stringify({generated_at:'old'})));await new Promise(setImmediate);assert.equal(doc.els['fx-native'].hidden,false);app.destroy();}
 finally{globalThis.setInterval=interval;}
});
test('Signal Board never reconstructs a migration gauge from the predecessor',async()=>{
 const doc=document();await board.mount(doc,api,async()=>new Response(JSON.stringify({generated_at:'2026-09-25',migration:{state:'UPSTREAM_BREWING',spillover:99}})),webcrypto);
 assert.match(doc.els['jh-fifx-status'].textContent,/pending/);assert.ok(!doc.els['jh-fifx-status'].textContent.includes('BREWING'));
 const html=fs.readFileSync(path.join(__dirname,'../signal-board.html'),'utf8');assert.ok(html.includes('/fifx-vol.html'));assert.ok(html.includes('/jh-fifx-board.js'));assert.ok(!html.includes('hedges cheap'));assert.ok(!html.includes('function drawRibbon'));
});
test('charts show real zero and break across missing values without deleting rows',()=>{
 const rows=[{date:'2026-09-01',value:'0'},{date:'2026-09-02',value:'.'},{date:'2026-09-03',value:'-1'}];
 const svg=api.chart(rows,true,'basis points');assert.ok(!svg.includes('NaN'));assert.match(svg,/2026-09-03/);assert.equal((svg.match(/ M/g)||[]).length,2);
 const html=fs.readFileSync(path.join(__dirname,'../fifx-vol.html'),'utf8');for(const text of ['26 hours','not a point-in-time backtest','Portfolio consequence: unavailable','WAIT means abstain','Load complete history','Verify original response'])assert.ok(html.includes(text),text);
});
