const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {webcrypto}=require('node:crypto'),api=require('../jh-fifx-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/fifx-api-browser-whole-inputs.json'),'utf8'));
const copy=x=>JSON.parse(JSON.stringify(x)),rows=r=>Object.fromEntries(api.sourceIdentityRows(r,'DGS10'));
const fetcher=async url=>{const key=url.slice(1).split('?')[0];return key==='data/fifx-vol.json'?new Response(JSON.stringify(fixture.packet)):
  new Response(fixture.objects[key]?Buffer.from(fixture.objects[key],'base64'):'Unavailable',{status:fixture.objects[key]?200:404});};

test('whole API publication verifies every original, receipt and complete source projection',async()=>{
 const p=fixture.packet,run=await api.verifyView(p,fetcher,webcrypto);
 for(const sid of ['VIXCLS','DGS10','DEXUSEU','DEXJPUS','DEXUSUK','DTWEXBGS']){
  const whole=await api.verifySeries(p,sid,fetcher,webcrypto),proof=await api.verifyOriginal(p,run,sid,fetcher,webcrypto);
  assert.equal(whole.original_rows.length,550);assert.equal(proof.rows,550);assert.equal(proof.missing,false);
  assert.equal(whole.source_identity.population.returned_rows,whole.original_rows.length);
  assert.equal(whole.specification.provider,'fred_api');assert.equal(fixture.independent_arithmetic[sid].original_rows,550);
 }
});
test('requested observations, realtime window and exact response counts have distinct labels',()=>{
 const r=rows(fixture.packet.series.DGS10);
 assert.equal(r['Provider'],'fred_api');assert.equal(r['Original response'],'HTTP 200');
 assert.equal(r['Requested observations'],'1988-01-01 → 2026-09-26');assert.equal(r['Response realtime window'],'2026-09-26 → 2026-09-26');
 assert.equal(r['Returned / reported rows'],'550 / 550');assert.match(r['Requested-window completeness'],/^Complete according/);
 assert.equal(r['Full series history'],'Unverified');assert.equal(r['Historical first-release availability'],'Unverified');
 assert.equal(r['Forecast / sizing'],'Unqualified');
});
test('missing, fractional, boolean, partial, changed and malformed populations never claim completeness',()=>{
 for(const change of [r=>r.source_identity.population=null,r=>r.source_identity.population.returned_rows=550.5,r=>r.source_identity.population.reported_rows=true,
  r=>r.source_identity.population.reported_rows=551,r=>r.source_identity.population.offset=false,r=>r.source_identity.population.limit=4000,
  r=>r.source_identity.population.complete_requested_window='true',r=>r.retained_original_rows=549,r=>r.receipt.http_status=429,
  r=>r.source_identity.population.requested_start='2026-02-30',r=>r.source_identity.population.realtime_end='2026-09-25']){
  const row=copy(fixture.packet.series.DGS10);change(row);assert.equal(rows(row)['Requested-window completeness'],'Unverified');
 }
});
test('old CSV and quote sources never acquire invented API windows or parity claims',()=>{
 const row=copy(fixture.packet.series.DGS10);row.specification.provider='fred_csv';delete row.source_identity.population;
 assert.equal('Requested observations' in rows(row),false);row.receipt=null;assert.equal(rows(row)['Original response'],'Unavailable');
 const quote=Object.fromEntries(api.sourceIdentityRows(fixture.packet.series['^MOVE'],'^MOVE'));
 assert.equal(quote['Official quote-feed parity'],'Unverified');assert.equal('Requested observations' in quote,false);
});
test('page renders date/count evidence and clears it after a failed refresh',async()=>{
 const els={},doc={getElementById(id){return els[id]??={value:'',innerHTML:'',textContent:'',hidden:false,disabled:false,handlers:{},addEventListener(name,fn){this.handlers[name]=fn;}};}};
 const priorNow=Date.now,priorInterval=globalThis.setInterval,priorClear=globalThis.clearInterval;
 Date.now=()=>Date.parse(fixture.packet.generated_at);globalThis.setInterval=()=>1;globalThis.clearInterval=()=>{};let broken=false,app;
 try{
  app=api.mount(doc,async url=>{if(broken)throw Error('invented unavailable');return fetcher(url);},webcrypto);await app.refresh();
  assert.equal(els['fx-native'].hidden,false);assert.match(els['fx-identity'].innerHTML,/FRED observations API/);
  assert.match(els['fx-identity'].innerHTML,/1988-01-01 → 2026-09-26/);assert.match(els['fx-identity'].innerHTML,/550 \/ 550/);
  assert.match(els['fx-identity'].innerHTML,/Full series history<\/dt><dd>Unverified/);
  broken=true;await app.refresh();assert.equal(els['fx-native'].hidden,true);assert.equal(els['fx-identity'].innerHTML,'');
 }finally{app?.destroy();Date.now=priorNow;globalThis.setInterval=priorInterval;globalThis.clearInterval=priorClear;}
});
