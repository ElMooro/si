const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
globalThis.crypto=crypto.webcrypto;
const ROOT=path.join(__dirname,'..'),api=require('../jh-statement-research.js');
const code=fs.readFileSync(path.join(ROOT,'jh-equity-statements.js'),'utf8');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/statement-research-public.json')));
const packet=JSON.parse(fixture.objects[api.CURRENT]),now=Date.parse(packet.generated_at)+1;
function harness(options={}){
 const inspected=[],requested=[];let section;
 const response=raw=>new Response(raw??'{}',{status:raw===undefined?404:200});
 const fetcher=async url=>{requested.push(url);return options.fetch?options.fetch(url):response(fixture.objects[url.slice(1)]);};
 class Clock extends Date{static now(){return options.now??now;}}
 const root={Date:Clock,fetch:fetcher,encodeURIComponent,JHStatementResearch:api,
  JHResearchInspection:{show:(body,rows)=>inspected.push(...rows)},
  document:{getElementById:()=>section,createElement:()=>({})}};
 vm.runInNewContext(code,root);
 function select(ticker){const body={innerHTML:'',textContent:'',append(){}},pill={textContent:''};section={dataset:{ticker},body,pill,querySelector:s=>s==='[data-statement-content]'?body:pill};return section;}
 select('ABC');return{api:root.JHEquityStatements,root,select,section:()=>section,inspected,requested,response};
}
test('company accounting context uses the real native verifier and preserves all issuer history',async()=>{
 const h=harness();await h.api.load('ABC');const section=h.section();
 assert.equal(section.pill.textContent,'descriptive');assert.equal(h.requested.length,5);
 assert.equal(h.inspected.length,1);assert.equal(h.inspected[0].document.records.length,2);
 for(const value of ['40.000000000000','0.000000000000','EUR','2025-12-31','2026-02-15','timezone unverified','original JSON row 0','SHA-256','annual','quarter','no TTM','No health grade'])assert(section.body.innerHTML.includes(value),value);
 assert.match(section.body.innerHTML,/statement-research\.html\?run=[a-f0-9]{64}&amp;symbol=ABC&amp;period=annual&amp;record=[a-f0-9]{64}/);
 assert(h.requested.every(url=>url===('/'+api.CURRENT)||url.startsWith('/data/statement-research/')));
});
test('unverified bytes, missing dependencies and future acquisition cannot show accounting values',async()=>{
 for(const option of [{now:Date.parse(packet.generated_at)-1},{missing:true},{tamper:true}]){
  const h=harness({...option,fetch:option.tamper?async url=>new Response(fixture.objects[url.slice(1)]+(url.includes('/records/')?' ':'')):undefined});
  if(option.missing)delete h.root.JHStatementResearch;
  await h.api.load('ABC');assert.equal(h.section().pill.textContent,'unavailable');assert.equal(h.inspected.length,0);
  assert.match(h.section().body.textContent,/No legacy health score is substituted/);
 }
});
test('overdue capture, unknown ticker, empty histories and bad identity remain distinct',async()=>{
 const overdue=harness({now:now+49*3600000});await overdue.api.load('ABC');assert.equal(overdue.section().pill.textContent,'acquisition review overdue');
 for(const symbol of ['UNKNOWN','EMPTY','BADCIK','PART']){
  const h=harness();h.select(symbol);await h.api.load(symbol);const section=h.section();
  if(symbol==='UNKNOWN'){assert.equal(section.pill.textContent,'outside captured population');assert.equal(h.requested.length,4);assert.equal(h.inspected.length,0);}
  else if(symbol==='EMPTY'){assert.match(section.body.innerHTML,/No statements were returned/);assert.equal(h.inspected[0].document.records.length,0);}
  else if(symbol==='BADCIK'){assert.match(section.body.innerHTML,/Calculations unavailable/);assert.equal(h.inspected[0].document.records[0].measurements,null);}
  else {assert.match(section.body.innerHTML,/Unavailable/);assert.match(section.body.innerHTML,/nonpositive denominator/);}
 }
});
test('delayed old ticker or same-ticker panel cannot overwrite the current evidence',async()=>{
 for(const next of ['ABC','PART']){
  const queue=[];const h=harness({fetch:url=>url===('/'+api.CURRENT)?new Promise(resolve=>queue.push(resolve)):new Response(fixture.objects[url.slice(1)])});
  const first=h.api.load('ABC');h.select(next);const second=h.api.load(next);
  queue[0](h.response(fixture.objects[api.CURRENT]));await first;assert.equal(h.inspected.length,0);
  queue[1](h.response(fixture.objects[api.CURRENT]));await second;assert.equal(h.inspected.length,1);assert.equal(h.inspected[0].document.requested_symbol,next);
 }
});
test('a newer unqualified row is never replaced with an older ratio and ambiguous filings require selection',()=>{
 const h=harness(),rows=[{request_period:'annual',identity:{date:'2025-01-01'},measurements:{value:999}},
  {request_period:'annual',identity:null,identity_evidence:[{reported_identity:{date:'2026-01-01'}}],measurements:null}];
 assert.equal(h.api.newest(rows,'annual').selected,rows[1]);
 rows.push({...rows[1],record_id:'different'});assert.equal(h.api.newest(rows,'annual').selected,null);
 assert.equal(h.api.newest(rows,'quarter').selected,null);
 for(const changed of [{generated_at:null},{source_acquisition_started_at:'2027-01-01T00:00:00Z'},
  {identity_index:{...packet.identity_index,received_at:'2026-01-01T00:00:00Z'}}])assert.throws(()=>h.api.clocks({...packet,...changed},now));
 assert(h.api.section('<img onerror="boom">').includes('&lt;img'));assert(!h.api.section('<img>').includes('<img>'));
});
test('page replaces legacy scoring, loads the real dependencies and conserves the whole predecessor',()=>{
 const html=fs.readFileSync(path.join(ROOT,'why.html'),'utf8'),a=html.indexOf('function renderFinancialHealth(d){'),b=html.indexOf('function renderDilutionPillar(d){',a);
 assert.match(html.slice(a,b),/JHEquityStatements.section/);assert.doesNotMatch(html.slice(a,b),/overall_score|pillarCard|financial_health_summary/);
 assert(html.includes('window.JHEquityStatements.load(d.ticker)'));
 for(const file of ['jh-statement-research.js','jh-equity-statements.js'])assert(html.includes('src="/'+file+'?'));
 const manifest=JSON.parse(fs.readFileSync(path.join(ROOT,'tests/fixtures/equity-statement-context-migration.json')));
 for(const entry of manifest.files){const raw=fs.readFileSync(path.join(ROOT,entry.predecessor));assert.equal(raw.length,entry.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),entry.sha256);}
});
