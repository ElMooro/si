const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),test=require('node:test'),assert=require('node:assert/strict');
const W=path.join(__dirname,'..'),api=require('../jh-squeeze-research.js');
const flags={calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function fixture(){return {contract:'squeeze-pretrigger-research.v1',state:'UNQUALIFIED',portfolio_action:'WAIT',call:null,independent_investment_votes:0,...flags,generated_at:'2026-10-01T18:00:00+00:00',inputs:Object.fromEntries(Object.entries({finra:'data/finra-short.json',short_interest:'data/short-interest-tickers.json',catalyst:'data/catalyst-calendar.json'}).map(([key,artifact])=>[key,{artifact,...flags,identity_verified:false,observation_freshness_verified:false,read_status:'parsed',body_sha256:'a'.repeat(64),body_bytes:2}]))};}
function extract(name,start,end){const source=fs.readFileSync(path.join(W,name),'utf8');const a=source.indexOf(start);assert.ok(a>=0);const b=source.indexOf(end,a+start.length);assert.ok(b>a);return source.slice(a,b);}
test('received sources remain distinct from qualified measurement or forecast',()=>{
 const packet=fixture(),v=api.view(packet);assert.equal(v.accepted,true);assert.equal(v.inputs.length,3);assert.equal(v.publication,packet.generated_at);
 const html=api.render(packet);assert.match(html,/WAIT means abstain/);assert.match(html,/not zero setups|do not mean zero setups/);assert.match(html,/freshness unverified/);assert.match(html,/SHA-256/);assert.match(html,/not an observation date/);
});
test('legacy, null, forged authority and malformed identity never enable forecasts',()=>{
 const bad=[null,{}, {state:'IMMINENT',forward_expectations:{'2m':80},imminent_setups:[{ticker:'TEST',trade_ticket:{}}]}];
 for(const key of Object.keys(flags)){const p=fixture();p[key]=true;bad.push(p);}
 let p=fixture();p.inputs.short_interest.artifact='private/account.json';bad.push(p);p=fixture();p.inputs.finra.body_bytes=true;bad.push(p);p=fixture();p.inputs.catalyst.body_sha256='evil';bad.push(p);
 for(const p of bad){assert.equal(api.view(p).accepted,false);const html=api.render(p);assert.doesNotMatch(html,/80%|IMMINENT|trade_ticket|private\/account/);assert.match(html,/unqualified|unavailable/);}
});
test('unavailable, malformed, genuine zero bytes and invalid publication dates are distinct',()=>{
 const p=fixture();p.inputs.finra={...p.inputs.finra,read_status:'unavailable'};delete p.inputs.finra.body_sha256;delete p.inputs.finra.body_bytes;
 p.inputs.short_interest.read_status='malformed';p.inputs.short_interest.body_bytes=0;
 const v=api.view(p);assert.equal(v.inputs[0].digest,null);assert.equal(v.inputs[1].bytes,0);assert.match(api.render(p),/Received 0 bytes/);
 for(const date of ['2026-02-31T12:00:00Z','0000-01-01T00:00:00Z','2026-10-01','<img src=x>']){p.generated_at=date;assert.equal(api.view(p).publication,null);assert.doesNotMatch(api.render(p),/<img/);}
});
test('untrusted narratives and private retention references are never rendered',()=>{
 const p=fixture();p.why_now_explainer='<img src=x onerror=alert(1)>';p.recommended_trade={allocation:'100%'};p.inputs.finra.original_ref={key:'private/account'};
 const html=api.render(p);assert.doesNotMatch(html,/onerror|private\/account|100%/);assert.equal((html.match(/href=/g)||[]).length,3);
});
test('actual retail and chart renderers suppress legacy tickets and scores',()=>{
 const ctx={window:{JHSqueezeResearch:api}};vm.createContext(ctx);
 vm.runInContext(extract('retail-edges.html','function renderSqueeze(d) {','\nfunction '),ctx);
 const hostile={state:'IMMINENT',signal_strength:99,imminent_setups:[{ticker:'TEST',setup_quality:'IMMINENT',trade_ticket:{}}],forward_expectations:{'2m':80}};
 ctx.p=hostile;assert.match(vm.runInContext('renderSqueeze(p)',ctx),/unqualified/);
 vm.runInContext(extract('jh-chart-tvsearch.js','  function renderSqueeze(d) {','\n  function renderPressure'),ctx);
 assert.match(vm.runInContext('renderSqueeze({sqz:{doc:p,row:{score:99}}})',ctx),/No qualified squeeze score/);
});
test('actual primary page sections do not turn missing or legacy data into zero setups',()=>{
 const elements=Object.fromEntries(['regime','kpis','boardSetups'].map(key=>[key,{}]));
 const code=extract('squeeze.html','  // Squeeze forecast stays unavailable','  // ---------- CROWDED SHORTS');
 for(const pre of [null,fixture(),{state:'IMMINENT',signal_strength:99,imminent_setups:[{ticker:'TEST'}]}]){
  vm.runInNewContext(code,{pre,window:{JHSqueezeResearch:api},document:{getElementById:id=>elements[id]}});
  assert.match(elements.regime.innerHTML,/unqualified|unavailable/);assert.equal((elements.kpis.innerHTML.match(/Unavailable/g)||[]).length,4);assert.match(elements.boardSetups.textContent,/does not establish an empty/);
 }
});

test('older whole-source gates accept only the exact reviewed squeeze renderer delta',()=>{
 const normalize=require('./helpers/squeeze-research-preservation.cjs').normalize;
 const raw=fs.readFileSync(path.join(W,'jh-chart-tvsearch.js'),'utf8');
 const item=require('./fixtures/squeeze-research/preservation.json').files['jh-chart-tvsearch.js'];
 assert.equal(normalize(raw,'jh-chart-tvsearch.js'),fs.readFileSync(path.join(W,item.predecessor),'utf8'));
 assert.throws(()=>normalize(raw.replace('No qualified squeeze score','Invented score'),'jh-chart-tvsearch.js'));
 assert.throws(()=>normalize(raw+'\nvoid 0;','jh-chart-tvsearch.js'));
 const file='tests/warehouse-observation-preservation.test.js',legacy=fs.readFileSync(path.join(W,file),'utf8');
 const normalizeTest=require('./helpers/squeeze-research-preservation.cjs').normalizeTest;
 assert.equal(normalizeTest(legacy,file),fs.readFileSync(path.join(W,require('./fixtures/squeeze-research/preservation.json').files[file].predecessor),'utf8'));
 assert.throws(()=>normalizeTest(legacy.replace('assert.equal(current,','assert.notEqual(current,'),file));
 assert.throws(()=>normalizeTest(legacy+'\nvoid 0;',file));
});
