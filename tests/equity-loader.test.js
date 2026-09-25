const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'..'),html=fs.readFileSync(path.join(ROOT,'why.html'),'utf8');
const start=html.indexOf('let researchRevision = 0;'),end=html.indexOf('// ─── helpers for fetchAndRender',start);
const source=html.slice(start,end),inspection=require('../jh-research-inspection.js');
const base=Date.parse('2026-09-25T15:00:00Z');
const response=(doc,status=200)=>({ok:status===200,status,json:async()=>doc});
const doc=(ticker,stamp='2026-09-25T14:00:00Z')=>({ticker,generated_at:stamp});
function harness(fetch){
 const elements={researchBtn:{disabled:false},content:{innerHTML:''},'complete-research-data':{replaceChildren(){}}},shown=[];
 const clock={now:()=>base,parse:Date.parse};
 const context={window:{JHResearchInspection:inspection},Date:clock,$:id=>elements[id],document:{getElementById:()=>null},
  esc:s=>s,CDN_RESEARCH_BASE:'https://example.test/equity-research',fetchWithTimeout:fetch,
  setStep(){},stopStepAnimation(){},startStepAnimation(){},markStale(){},renderReport:d=>shown.push(d),
  setTimeout:fn=>{clock.now=()=>base+300000;fn();}};
 vm.runInNewContext(source,context);return {context,elements,shown,run:t=>context.fetchAndRender(t)};
}

test('a late prior ticker response cannot replace the current selection or re-enable its button',async()=>{
 const requests=[];const h=harness(url=>new Promise(resolve=>requests.push({url,resolve})));
 const first=h.run('ABC'),second=h.run('XYZ');assert.equal(requests.length,2);
 requests[0].resolve(response(doc('ABC')));await first;
 assert.equal(h.elements.researchBtn.disabled,true);assert.equal(h.shown.length,0);
 requests[1].resolve(response(doc('XYZ')));await second;
 assert.deepEqual(h.shown,[doc('XYZ')]);assert.equal(h.elements.researchBtn.disabled,false);
});

test('wrong-ticker, future and malformed cached reports cannot display or cause blind generation retries',async()=>{
 for(const packet of [doc('WRONG'),doc('ABC','2026-10-01T00:00:00Z'),doc('ABC','invalid')]){
  const calls=[];const h=harness(async url=>{calls.push(url);return url.includes('async=1')?response(null,503):response(packet);});
  await h.run('ABC');assert.equal(h.shown.length,0);assert.equal(calls.filter(u=>u.includes('async=1')).length,1);
  assert.match(h.elements.content.innerHTML,/has not been confirmed/);assert.doesNotMatch(h.elements.content.innerHTML,/will.*load|generating server-side/);
 }
});

test('an unchanged or older stale polling result is not accepted as a completed refresh',async()=>{
 for(const returned of ['2026-09-21T00:00:00Z','2026-09-20T00:00:00Z']){
  let reads=0,kickoffs=0;const old=doc('ABC','2026-09-21T00:00:00Z');
  const h=harness(async url=>{
   if(url.includes('async=1')){kickoffs++;return response(null,202);}
   return response(reads++===0?old:doc('ABC',returned));
  });
  await h.run('ABC');assert.deepEqual(h.shown,[old]);assert.equal(kickoffs,1);
  assert.match(h.elements.content.innerHTML,/Updated report unavailable/);
 }
});

test('the full predecessor remains retained and the edited page script parses',()=>{
 const record=JSON.parse(fs.readFileSync(path.join(ROOT,'tests/fixtures/equity-loader-migration.json')));
 const raw=fs.readFileSync(path.join(ROOT,record.predecessor));assert.equal(raw.length,record.bytes);
 assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),record.sha256);
 for(const script of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
  if(/\bsrc\s*=|application\/ld\+json|application\/json|type=["']module/i.test(script[1]))continue;
  new vm.Script(script[2]);
 }
});
