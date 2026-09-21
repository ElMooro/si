const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
global.crypto=require('node:crypto').webcrypto;
const A=require('../jh-option-research.js'),M=require('../jh-massive-research.js'),X=require('../jh-massive-cross-asset.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/massive-native-v2.json'),'utf8'));
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
const loaded=()=>M.verifyPacket(structuredClone(fixture.publication),fetcher);
test('seven sources retain every FX pair and dated contract with zero votes',async()=>{
 const p=await loaded();assert.equal(Object.keys(p.sources).length,7);assert.equal(Object.keys(p.instruments).filter(k=>k.startsWith('FX:')).length,19);
 assert.equal(Object.keys(p.instruments).filter(k=>k.startsWith('FUTURE:')).length,14);assert.equal(p.independent_investment_votes,0);
 assert.equal(M.instrument(p,'SPY').length,5);assert.equal(M.instrument(p,'FX:MASSIVE:EUR_USD').length,1);
});
test('FX and futures links pin exact instrument and parent run',async()=>{
 const p=await loaded(),fx=M.instrument(p,'FX:MASSIVE:EUR_USD')[0];
 assert.match(M.parentUrl(fx,'FX:MASSIVE:EUR_USD'),/^\/fx-research.html\?pair=EUR_USD&run=[a-f0-9]{64}$/);
 const key=Object.keys(p.instruments).find(k=>k.startsWith('FUTURE:')&&k.includes(':ES:')),future=M.instrument(p,key)[0];
 assert.match(M.parentUrl(future,key),/^\/futures-research.html\?product=ES&ticker=ES[A-Z0-9]+&run=[a-f0-9]{64}$/);
 assert.throws(()=>M.parentUrl(fx,'FX:MASSIVE:USD_JPY'),/identity differs/);
});
test('historical composition loads only its immutable recorded parents',async()=>{
 const seen=[],id=fixture.publication.replay.manifest_key.split('/').pop().slice(0,-5);
 const p=await M.recordedRun(id,async url=>{seen.push(url);return fetcher(url);});
 assert.equal(p.contract,'massive-composite-research.v2');assert.ok(seen.every(url=>/\/(runs|outputs)\//.test(url)));
 assert.match(M.recordedUrl(p,'FX:MASSIVE:EUR_USD'),/symbol=FX%3AMASSIVE%3AEUR_USD&run=/);
});
test('instrument details distinguish window, session and acquisition clocks',async()=>{
 const p=await loaded();assert.match(M.instrumentView(p,'FX:MASSIVE:EUR_USD'),/window start/);
 const key=Object.keys(p.instruments).find(k=>k.startsWith('FUTURE:'));
 assert.match(M.instrumentView(p,key),/Latest reported session/);assert.match(M.instrumentView(p,key),/Definition date/);
 assert.match(M.instrumentView(p,'FX:MASSIVE:XAU_USD'),/quantity unit remains unverified/);
 assert.match(M.sharedView(p),/no correlation, hedge ratio or portfolio netting/);
});
test('export includes typed identity, exact source records and assurance limits',async()=>{
 const p=await loaded(),out=M.exportEvidence(p,'FX:MASSIVE:EUR_USD');
 assert.equal(out.instrument_identity.price_unit,'USD_per_EUR');assert.equal(out.measurements[0].parent.pair,'EUR_USD');
 assert.equal(out.verification.original_provider_replay_performed_by_composite,false);assert.equal(out.sizing_eligible,false);
});
test('cross-asset semantic verifier catches tampering even before publication hash gate',async()=>{
 const p=await loaded(),parents=new Map();for(const[id,n]of Object.entries(p.dependency_graph.nodes))parents.set(id,JSON.parse(fixture.artifacts[n.output.key]));
 for(const mutate of [
  b=>b.instrument_identities['FX:MASSIVE:EUR_USD'].quote_code='JPY',
  b=>b.instruments['FX:MASSIVE:EUR_USD'][0].source_capture_completed_at=b.generated_at,
  b=>b.instruments['FX:MASSIVE:EUR_USD'][0].pointer='/pairs/USD_JPY',
  b=>b.dependency_graph.shared_exposures.find(g=>g.key==='currency:USD').instruments.pop(),
  b=>b.sources.futures.source_review_due_at='2026-09-22T00:00:00Z'
 ]){const bad=structuredClone(p);mutate(bad);assert.throws(()=>X.verify(bad,parents),/differ|required/);}
});
test('unsupported identities and foreign/private artifacts cannot be followed',async()=>{
 for(const id of ['../ES','FX:EUR_USD','FUTURE:CME:ES:<script>','FX:MASSIVE:EUR_USD?x=1'])assert.equal(X.identity(id),false);
 let calls=0;await assert.rejects(()=>M.load('data/futures-research/records/'+'a'.repeat(64)+'.json',async()=>{calls++;}),/Unapproved/);assert.equal(calls,0);
});
test('absent query stays unavailable and edits invalidate previously inspected evidence',async()=>{
 const elements=new Map(),host={querySelector:key=>{if(!elements.has(key))elements.set(key,{textContent:'',innerHTML:'',value:'',disabled:false});return elements.get(key);}};
 const root={JHOptionResearch:A,JHMassiveResearch:M,location:{href:'https://justhodl.ai/market-evidence.html?symbol=ABSENT'},fetch:fetcher,setInterval:()=>0};
 require('node:vm').runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-massive-research-page.js'),'utf8'),{window:root,URL,AbortController,Blob});
 await root.JHMassiveResearchBoot(host);
 assert.equal(host.querySelector('[data-mr-symbol]').value,'ABSENT');assert.equal(host.querySelector('[data-mr-export]').disabled,true);
 assert.match(host.querySelector('[data-mr-detail]').textContent,/present in this recorded composition/);
 host.querySelector('[data-mr-symbol]').value='FX:MASSIVE:EUR_USD';host.querySelector('[data-mr-form]').onsubmit({preventDefault(){}});
 assert.equal(host.querySelector('[data-mr-export]').disabled,false);
 host.querySelector('[data-mr-symbol]').value='SPY';host.querySelector('[data-mr-symbol]').oninput();
 assert.equal(host.querySelector('[data-mr-export]').disabled,true);assert.equal(host.querySelector('[data-mr-recorded]').textContent,'');
});
