const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {webcrypto,createHash}=require('node:crypto');
const api=require('../jh-bond-vol-research.js'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/bond-vol-native-view.json'),'utf8'));
const NOW=Date.parse(fixture.generated_at),copy=x=>JSON.parse(JSON.stringify(x)),sha=x=>createHash('sha256').update(x).digest('hex');
function wire(){
 const packet=copy(fixture),objects={};
 function put(value,category){const raw=Buffer.from(JSON.stringify(value)),digest=sha(raw),key='data/bond-vol-research/'+category+'/'+digest+'.json';objects[key]=raw;return {key,sha256:digest,bytes:raw.length};}
 const projection={...packet};delete projection.replay;
 const manifest={contract:'bond-vol-replay.v1',generated_at:packet.generated_at,output_sha256:packet.replay.output_sha256,view:put(projection,'views'),series:Object.fromEntries(api.IDS.map(s=>[s,packet.series[s].complete_history_artifact])),quote:packet.move.complete_quote_artifact};
 packet.replay.manifest_key=put(manifest,'runs').key;
 const fetcher=async url=>{const key=url.slice(1).split('?')[0],head=key==='data/bond-vol.json';return new Response(head?JSON.stringify(packet):objects[key],{status:head||objects[key]?200:404});};
 return {packet,objects,manifest,fetcher};
}
test('all ten rate measurements have dated units, dependencies and separate quote status',()=>{
 const v=api.view(fixture,NOW);assert.equal(v.available,10);assert.equal(v.quote,null);assert.equal(v.rows.length,10);assert.equal(fixture.move.status,'identity_mismatch');
 assert.equal(v.rows[0].value.calculated_decimal,fixture.series.DGS10.current.step_dispersion_bp.calculated_decimal);
});
test('fresh publication cannot hide acquisition expiry, missing observations or wrong units',()=>{
 const p=copy(fixture);assert.equal(api.view(p,NOW+27*3600000).available,0);
 p.generated_at=new Date(NOW+27*3600000).toISOString();p.source_generated_at=p.generated_at;assert.equal(api.view(p,NOW+27*3600000).available,0);
 const edits=[p=>p.series.DGS10.current=null,p=>p.series.DGS10.latest_observation.value='.',p=>p.series.DGS10.source_definition.units='Basis points',p=>p.series.DGS10.current.max_interval_days=8,p=>p.series.DGS10.quality.status='source_regression',p=>p.series.DGS10.latest_observation.date='2099-01-01'];
 for(const edit of edits){const x=copy(fixture);edit(x);assert.equal(api.view(x,NOW).available,9);}
});
test('investment authority and fabricated independent votes cannot qualify',()=>{
 for(const edit of [p=>p.calls_eligible=true,p=>p.regime='NORMAL',p=>p.dependency_graph.independent_votes=10,p=>p.portfolio_consequences.target_weights={SPY:1},p=>p.series.DGS10.complete_history_artifact.key='audit-private/key']){
  const p=copy(fixture);edit(p);assert.equal(api.view(p,NOW).available,0);
 }
});
test('zero dispersion remains zero and flat baseline z remains undefined',()=>{
 const p=copy(fixture),r=p.series.DGS10;r.current.step_dispersion_bp={value:0,calculated_decimal:'0'};r.current_distribution.z_score={value:null,calculated_decimal:null};
 assert.equal(api.view(p,NOW).rows[0].value.value,0);assert.equal(api.display(r.current.step_dispersion_bp),'0');assert.equal(api.display(r.current_distribution.z_score),'Unavailable');
 assert.match(api.display({value:.12345678,calculated_decimal:'0.12345678'}),/rounded/);
});
test('the exact immutable projection binds every history reference',async()=>{
 const w=wire();await api.verifyView(w.packet,w.fetcher,webcrypto);
 const p=copy(w.packet);p.series.DGS10.current.step_dispersion_bp.value=123;await assert.rejects(api.verifyView(p,w.fetcher,webcrypto),/view differs/);
 w.objects[w.packet.replay.manifest_key]=Buffer.from('{}');await assert.rejects(api.verifyView(w.packet,w.fetcher,webcrypto),/identity differs/);
});
test('missing, corrupted and private artifacts are rejected',async()=>{
 const w=wire();await assert.rejects(api.verifySeries(w.packet,'DGS10',w.fetcher,webcrypto),/unavailable/);await assert.rejects(api.verifySeries(w.packet,'../../private',w.fetcher,webcrypto));
 const ref=w.packet.series.DGS10.complete_history_artifact;w.objects[ref.key]=Buffer.from('{}');await assert.rejects(api.verifySeries(w.packet,'DGS10',w.fetcher,webcrypto),/bytes differ/);
 await assert.rejects(api.verifyQuote(w.packet,w.manifest,w.fetcher,webcrypto));assert.equal(api.identity({key:'audit-private/x',sha256:'a'.repeat(64),bytes:1},'originals','bin'),false);
});
test('provider source inspection checks all rows, not merely the latest value',async()=>{
 const row={series_id:'DGS10',source_definition:{id:'DGS10'},original_rows:[{date:'2026-09-01',value:'3.0'},{date:'2026-09-02',value:'.'}],evidence:{}},objects={};
 for(const kind of ['definition','observations']){const raw=Buffer.from(JSON.stringify(kind==='definition'?{seriess:[row.source_definition]}:{observations:row.original_rows}));const digest=sha(raw),key='data/evidence/fred/'+'a'.repeat(64)+'/'+digest+'.bin.gz';objects[key]=raw;row.evidence[kind]={captured:true,provider:'fred',key,bytes:raw.length,sha256:digest};}
 const fetcher=async u=>new Response(objects[u.slice(1).split('?')[0]]);assert.equal(await api.verifyOriginal(row,fetcher,webcrypto),2);
 row.original_rows[0].value='4.0';await assert.rejects(api.verifyOriginal(row,fetcher,webcrypto),/observations differ/);
 row.evidence.definition.key='audit-private/x';await assert.rejects(api.verifyOriginal(row,fetcher,webcrypto),/coordinates/);
});
test('transport limits and timeouts work even when a fetch ignores cancellation',async()=>{
 await assert.rejects(api.bytes('/x',()=>new Promise(()=>{}),100,5),/timed out/);
 await assert.rejects(api.bytes('/x',async()=>new Response('123456'),3),/bound/);
});
test('late refresh cannot replace a newer accepted publication',async()=>{
 let resolve;const gate=api.controller(v=>v===1?new Promise(r=>resolve=r):Promise.resolve());const old=gate.accept(1);await gate.accept(2);resolve();assert.equal(await old,false);assert.equal(gate.get(),2);gate.clear();assert.equal(gate.get(),null);
});
test('mounted page expires readings and clears them after failed refresh',async()=>{
 const w=wire(),els={};let tick,fail=false;
 const doc={getElementById(id){return els[id]??={hidden:false,value:'',innerHTML:'',textContent:'',disabled:false,querySelectorAll:()=>[]};}};
 doc.getElementById('bv-history-kind').value='original';const interval=globalThis.setInterval,now=Date.now;globalThis.setInterval=fn=>{tick=fn;return 1;};Date.now=()=>NOW;
 try{const app=api.mount(doc,async u=>{if(fail)throw Error('offline');return w.fetcher(u);},webcrypto);await app.refresh();assert.equal(els['bv-native'].hidden,false);assert.match(els['bv-status'].textContent,/10\/10/);assert.match(els['bv-quote'].innerHTML,/identity mismatch/);
 Date.now=()=>NOW+27*3600000;tick();assert.match(els['bv-status'].textContent,/0\/10/);assert.match(els['bv-reading'].innerHTML,/Unavailable/);fail=true;await app.refresh();assert.equal(els['bv-native'].hidden,true);assert.match(els['bv-status'].textContent,/withheld/);
 }finally{globalThis.setInterval=interval;Date.now=now;}
});
test('research ribbon cannot turn unqualified data into a regime or z-score',async()=>{
 const nodes={},host={innerHTML:'',children:[],appendChild(n){this.children.push(n);}},doc={getElementById:id=>id==='fixture'?host:nodes[id],head:{appendChild(n){nodes[n.id]=n;}},createElement:()=>({style:{},innerHTML:''})};
 const context={document:doc,window:{},fetch:async()=>({ok:true,json:async()=>({...fixture,regime:'CRISIS',composite_z_score:99})}),Date,requestAnimationFrame:()=>{}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../regime-ribbon.js'),'utf8'),context);context.window.RegimeRibbon.mount({target:'fixture'});await new Promise(setImmediate);
 assert.equal(host.children.length,1);assert.match(host.children[0].innerHTML,/Research only/);assert.ok(!host.children[0].innerHTML.includes('CRISIS'));assert.ok(!host.children[0].innerHTML.includes('99'));
});
test('page states calendar, vintage and portfolio limits and retains full history controls',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../bond-vol.html'),'utf8');for(const text of ['jh-bond-vol-research.js','not a point-in-time backtest','26 hours','Portfolio consequence: unavailable','WAIT means abstain','Verify provider originals','Load complete history'])assert.ok(html.includes(text),text);
 const svg=api.chart([{x:1,y:0,label:'a'},{x:2,y:null,label:'b'},{x:3,y:-1,label:'c'}],'All rows','bp');assert.ok(!svg.includes('NaN'));assert.match(svg,/c<\/text>/);
});
