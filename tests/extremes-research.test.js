const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-extremes-research.js');
const f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/extremes-native.json'),'utf8')),p=f.packet,at=Date.parse(p.generated_at);
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Python synthesis verifies against exact run and output bytes',async()=>assert.deepEqual(await api.verifyPacket(p,fetcher),p));
test('edited measurement, wrong engine and external replay paths are rejected',async()=>{
 const x=structuredClone(p);x.measurements[0].value=12345;await assert.rejects(api.verifyPacket(x,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket({...p,engine:'market-extremes'},fetcher),/Retained run differs/);
 await assert.rejects(api.verifyPacket({...p,replay:{manifest_key:'https://other.invalid/data'}},fetcher),/Native synthesis required/);
 await assert.rejects(api.load('portfolio/risk.json',fetcher),/Unapproved research path/);
});
test('native and row authority cannot be enabled by the payload',()=>{
 for(const k of ['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'])assert.equal(api.typed({...p,[k]:true}),false);
 assert.equal(api.typed({...p,posture:'EUPHORIA'}),false);assert.equal(api.typed({...p,capitulation_score:99}),false);
 const x=structuredClone(p);x.measurements[0].sizing_eligible=true;assert.equal(api.typed(x),false);
});
test('expired wrapper and per-measurement deadline visibly withhold current use',()=>{
 assert.equal(api.current(p,at),true);assert.equal(api.current(p,at-1),false);assert.equal(api.current(p,at+4*3600000),false);
 const html=api.render(p,at+4*3600000);assert.match(html,/refresh overdue/);assert.match(html,/Expired context/);
 const x=structuredClone(p);x.measurements[0].valid_until=new Date(at-1).toISOString();assert.match(api.render(x,at),/Expired context/);
});
test('source overlap and both fails scopes remain visible without a combined score',()=>{
 const html=api.render(p,at);assert.match(html,/repeated series\/date groups/);assert.match(html,/zero qualified investment votes/);
 assert.match(html,/ust_ex_tips: FTD 86 \+ FTR 87 = 173 USD billion/);assert.match(html,/treasury_incl_tips: FTD 92.61 \+ FTR 97.63 = 190.24 USD billion/);
 assert.match(html,/not re-parsed/);assert.doesNotMatch(html,/GENERATIONAL_BUY|STRONG_BUY/);
});
test('HTML and evidence-link injection are escaped or withheld',()=>{
 const x=structuredClone(p);x.measurements[0].label='<img src=x onerror=alert(1)>';x.measurements[0].upstream_replay={manifest_key:'javascript:bad'};
 const html=api.render(x,at);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|javascript:/);assert.match(html,/evidence unavailable/);
});
test('signed, zero, empty and invalid price scenarios',()=>{
 assert.equal(api.scenario('100000','-10'),-10000);assert.equal(api.scenario('-100000','-10'),10000);assert.equal(api.scenario('-100','0'),0);
 for(const args of [['','10'],['100',''],['NaN','10'],['100','-101'],['1000000000001','10']])assert.throws(()=>api.scenario(...args));
});
test('editing assumptions invalidates the previous displayed consequence',()=>{
 const prior=globalThis.document,form={elements:{exposure:{value:'100000'},shock:{value:'-10'}}},out={textContent:''};globalThis.document={getElementById:id=>id==='extremes-scenario'?form:out};
 try{api.bindScenario();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/-\$10,000.00/);form.elements.shock.value='5';form.oninput();assert.match(out.textContent,/Assumptions changed/);form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$5,000.00/);}finally{globalThis.document=prior;}
});
test('both pages load the verified renderer and Intelligence links to evidence',()=>{
 for(const engine of ['capitulation','market-extremes']){const html=fs.readFileSync(path.join(__dirname,'..',engine+'.html'),'utf8');assert.match(html,/jh-extremes-research.js\?v=20260920-native2/);assert.ok(html.includes('data-extremes-engine="'+engine+'"'));assert.match(html,/id="extremes-scenario"/);}
 const intel=fs.readFileSync(path.join(__dirname,'../intelligence/index.html'),'utf8');assert.match(intel,/href="\/capitulation.html">Inspect dated evidence/);assert.doesNotMatch(intel,/score \$\{cap\?fmtNum\(cap.capitulation_score/);
});
test('declared local scripts exist in the publication tree',()=>{
 for(const engine of ['capitulation','market-extremes']){
  const html=fs.readFileSync(path.join(__dirname,'..',engine+'.html'),'utf8');
  for(const m of html.matchAll(/<script[^>]+src="(\/[^"?]+)(?:\?[^\"]*)?"/g))assert.ok(fs.existsSync(path.join(__dirname,'..',m[1])),m[1]);
 }
});
