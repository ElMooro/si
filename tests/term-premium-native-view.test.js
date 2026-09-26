const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {webcrypto,createHash}=require('node:crypto');
const api=require('../jh-term-premium-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/term-premium-native-view.json'),'utf8'));
const NOW=Date.parse(fixture.generated_at),copy=x=>JSON.parse(JSON.stringify(x));
const sha=x=>createHash('sha256').update(x).digest('hex');
function wire(){
  const packet=copy(fixture),objects={};
  function put(value,category){const raw=Buffer.from(JSON.stringify(value)),digest=sha(raw),key='data/term-premium-research/'+category+'/'+digest+'.json';objects[key]=raw;return {key,sha256:digest,bytes:raw.length};}
  const projected={...packet};delete projected.replay;
  const manifest={contract:'term-premium-replay.v1',generated_at:packet.generated_at,output_sha256:packet.replay.output_sha256,
    view:put(projected,'views'),tables:Object.fromEntries(Object.entries(packet.tables).map(([n,t])=>[n,t.complete_table_artifact]))};
  packet.replay.manifest_key=put(manifest,'runs').key;
  const fetcher=async url=>new Response(url.startsWith('/data/term-premium.json?')?JSON.stringify(packet):objects[url.slice(1).split('?')[0]],{status:url.startsWith('/data/term-premium.json?')||objects[url.slice(1).split('?')[0]]?200:404});
  return {packet,objects,fetcher,manifest};
}
test('all 60 daily and monthly series retain definitions and current exact values',()=>{
  const v=api.view(fixture,NOW);assert.equal(v.available,60);assert.equal(v.rows.length,60);
  assert.equal(v.rows.find(r=>r.sid==='D:ACMTP10').value,fixture.series['D:ACMTP10'].current.exact_decimal);
  assert.equal(v.rows.find(r=>r.sid==='M:ACMRNY01').table,'ACM Monthly');
});
test('acquisition, observation and open-page time expiry are independent',()=>{
  assert.equal(api.view(fixture,NOW+27*3600000).available,0);
  const p=copy(fixture);p.generated_at=new Date(NOW+27*3600000).toISOString();assert.equal(api.view(p,NOW+27*3600000).available,0);
  p.source.acquired_at=p.generated_at;p.series['D:ACMTP10'].current.observation_date='2025-01-01';assert.equal(api.view(p,NOW+27*3600000).available,59);
});
test('missing, zero, negative and future observations cannot be confused',()=>{
  const p=copy(fixture),r=p.series['D:ACMTP10'];r.current.value=0;r.current.exact_decimal='0';assert.equal(api.view(p,NOW).rows.find(r=>r.sid==='D:ACMTP10').value,'0');
  r.current.value=-1;r.current.exact_decimal='-1';assert.equal(api.view(p,NOW).available,60);
  r.current=null;assert.equal(api.view(p,NOW).available,59);
  r.current={value:1,exact_decimal:'1',observation_date:'2099-01-01'};assert.equal(api.view(p,NOW).available,59);
});
test('wrong definitions or investment authority never become eligible',()=>{
  for(const change of [p=>p.sizing_eligible=true,p=>p.dependency_graph.independent_votes=60,p=>p.source.source_url='https://example.com/file.xls']){
    const p=copy(fixture);change(p);assert.equal(api.view(p,NOW).available,0);
  }
  const p=copy(fixture);p.series['D:ACMTP10'].unit='bps';assert.equal(api.view(p,NOW).available,59);
  p.series['D:ACMTP01'].quality.max_acquisition_age_seconds=999999;assert.equal(api.view(p,NOW).available,58);
});
test('acquisition failure and source regression withhold current readings',()=>{
  const p=copy(fixture);p.acquisition.status='failed';assert.equal(api.view(p,NOW).available,0);
  p.acquisition.status='acquired';p.series['M:ACMTP10'].quality.status='source_regression';assert.equal(api.view(p,NOW).available,59);
});
test('full immutable view verifies; modified values or manifest bytes fail',async()=>{
  const w=wire();await api.verifyView(w.packet,w.fetcher,webcrypto);
  const bad=copy(w.packet);bad.series['D:ACMTP10'].current.value=100;
  await assert.rejects(api.verifyView(bad,w.fetcher,webcrypto),/view differs/);
  w.objects[w.packet.replay.manifest_key]=Buffer.from('{}');await assert.rejects(api.verifyView(w.packet,w.fetcher,webcrypto),/identity differs/);
});
test('worksheet and source inspection reject missing or changed artifacts',async()=>{
  const w=wire();await assert.rejects(api.verifyTable(w.packet,'ACM Daily',w.fetcher,webcrypto),/unavailable/);
  await assert.rejects(api.verifyTable(w.packet,'../../secret',w.fetcher,webcrypto));
  await assert.rejects(api.verifyOriginal(w.packet,w.manifest,w.fetcher,webcrypto));
  assert.equal(api.identity({key:'audit-private/x',sha256:'a'.repeat(64),bytes:1},'originals','xls'),false);
});
test('bounded transport and body timeouts do not rely on AbortSignal cooperation',async()=>{
  await assert.rejects(api.bytes('/test',()=>new Promise(()=>{}),100,5),/timed out/);
  const stalled=async()=>({ok:true,headers:new Headers(),body:{getReader:()=>({read:()=>new Promise(()=>{}),cancel:()=>Promise.resolve()})}});
  await assert.rejects(api.bytes('/test',stalled,100,5),/timed out/);
  await assert.rejects(api.bytes('/test',async()=>new Response('123456'),3),/bound/);
});
test('late and failed refreshes cannot retain old current state',async()=>{
  let release;const gate=api.controller(value=>value===1?new Promise(resolve=>release=resolve):value===3?Promise.reject(Error('invalid')):Promise.resolve());
  const first=gate.accept(1);assert.equal(await gate.accept(2),true);release();assert.equal(await first,false);assert.equal(gate.get(),2);
  await assert.rejects(gate.accept(3));assert.equal(gate.get(),null);
});
test('compact display declares rounding and preserves zero and negatives',()=>{
  assert.equal(api.display('0'),'0');assert.equal(api.display('-1.25'),'-1.25');assert.match(api.display('0.123456789'),/rounded/);
  for(const v of [null,undefined,'',NaN])assert.equal(api.display(v),'Unavailable');
  const svg=api.chart([{x:1,y:-1,label:'1y'},{x:2,y:null,label:'2y'},{x:3,y:0,label:'3y'}],'Curve');assert.match(svg,/1y/);assert.match(svg,/3y/);assert.ok(!svg.includes('NaN'));
});
test('page labels model, vintage, calendar limits and portfolio boundary',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../term-premium.html'),'utf8');
  for(const text of ['jh-term-premium-research.js','daily and monthly','not a point-in-time backtest','26 hours','Portfolio consequence: unavailable','WAIT means abstain','original workbook'])assert.ok(html.includes(text),text);
  assert.ok(!html.includes('vigilante episodes are'));assert.ok(!html.includes('board −1/−2'));
});

test('the mounted page clears current values after expiry and failed refresh',async()=>{
  const w=wire(),elements={};let tick,fail=false;
  const doc={getElementById(id){return elements[id]??=( {hidden:false,value:'',innerHTML:'',textContent:'',disabled:false,querySelectorAll:()=>[]} );}};
  doc.getElementById('term-frequency').value='D';doc.getElementById('term-family').value='ACMTP';doc.getElementById('term-tenor').value='10';
  const interval=globalThis.setInterval,originalNow=Date.now;globalThis.setInterval=fn=>{tick=fn;return 1;};Date.now=()=>NOW;
  try{
    const app=api.mount(doc,async url=>{if(fail)throw Error('offline');return w.fetcher(url);},webcrypto);
    await app.refresh();assert.equal(elements['term-native'].hidden,false);assert.match(elements['term-status'].textContent,/60\/60/);
    Date.now=()=>NOW+27*3600000;tick();assert.match(elements['term-status'].textContent,/0\/60/);assert.match(elements['term-reading'].innerHTML,/Unavailable/);
    fail=true;await app.refresh();assert.equal(elements['term-native'].hidden,true);assert.match(elements['term-status'].textContent,/withheld/);
  }finally{globalThis.setInterval=interval;Date.now=originalNow;}
});
