const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const ui=require('../liquidity-pulse.js');
// Actual unpublished compiler output; this fixture is never a live release receipt.
const fixture=JSON.parse(fs.readFileSync(__dirname+'/fixtures/pulse-native-view.json','utf8'));
function packet(){const p=structuredClone(fixture);p.contract='liquidity-pulse-research.v1';delete p.candidate_only;delete p.publication_eligible;
 p.replay={manifest_key:'data/liquidity-pulse-research/runs/'+'a'.repeat(64)+'.json',output_sha256:'b'.repeat(64)};return p;}
const now=Date.parse(fixture.generated_at);

test('native view retains every identity, declared units and complete original links',()=>{
 const p=packet(),v=ui.view(p,now);assert.equal(v.rows.length,11);assert(v.current);
 assert.equal(v.rows.filter(r=>r.eligible).length,11);assert.equal(v.rows[0].value,'$6,747.704 bn');
 assert.match(v.rows.find(r=>r.sid==='HQMCB10YR').observation,/monthly period/);
 const html=ui.panelHTML(p,now);assert.match(html,/Retained definition/);assert.match(html,/Complete original observations/);
 assert.match(html,/percentage_points/);assert(!html.includes('have not been replayed'));
});
test('candidate or promoted authority cannot masquerade as a native publication',()=>{
 for(const change of [p=>p.contract='liquidity-pulse-candidate.v1',p=>p.calls_eligible=true,p=>p.replay.manifest_key='data/portfolio.json']){
  const p=packet();change(p);const v=ui.view(p,now);assert(!v.current);assert(v.rows.every(r=>r.value==='Unavailable'));
  assert(!ui.panelHTML(p,now).includes('data/portfolio.json'));
 }
});
test('fresh wrapper cannot renew stale or conflicting original identities',()=>{
 for(const change of [r=>r.acquired_at='2026-09-20T00:00:00Z',r=>r.frequency='M',r=>r.unit='Billions of Dollars',r=>r.calls_eligible=true,
  r=>r.source_definition.id='WTREGEN',r=>r.source_definition.seasonal_adjustment='Seasonally Adjusted',r=>r.latest_value_decimal='999',r=>r.evidence.observations.source_url='javascript:alert(1)']){
  const p=packet();change(p.series.WALCL);assert.equal(ui.view(p,now).rows[0].value,'Unavailable');
  assert(!ui.panelHTML(p,now).includes('javascript:'));
 }
});
test('age expiry and unavailable current rows keep explicit dated history without an actionable reading',()=>{
 const p=packet(),v=ui.view(p,now+27*3600000);assert(!v.current);assert(v.rows.every(r=>r.value==='Unavailable'));
 assert.equal(v.rows[0].reported,'6747704');assert(v.rows[0].comparisons);
 p.series.WALCL.latest_value=null;p.series.WALCL.latest_value_decimal=null;p.series.WALCL.quality.status='source_regression';
 assert.equal(ui.view(p,now).rows[0].value,'Unavailable');assert.equal(ui.view(p,now).rows[0].reported,'6747704');
});
test('text cannot inject HTML and altered comparison endpoints cannot display a valid change',()=>{
 const p=packet();p.series.WALCL.last_observed_value='<img onerror=alert(1)>';
 p.series.WALCL.historical_calendar_comparisons.month.current_date='2099-01-01';
 const html=ui.panelHTML(p,now);assert(!html.includes('<img'));assert(!html.includes('onerror='));
 assert.match(html,/No|Unavailable/);
});


function originalFixture(){
 const row=structuredClone(packet().series.WALCL);row.source_row=0;
 const documents={definition:{seriess:[row.source_definition]},observations:{observations:[{date:row.latest_date,value:row.last_observed_value}]}};
 const files={};const crypto=require('node:crypto');
 for(const [kind,doc] of Object.entries(documents)){
  const raw=JSON.stringify(doc),hash=crypto.createHash('sha256').update(raw).digest('hex');
  row.evidence[kind].sha256=hash;row.evidence[kind].bytes=Buffer.byteLength(raw);
  row.evidence[kind].key='data/evidence/fred/'+'c'.repeat(64)+'/'+hash+'.bin.gz';files['/'+row.evidence[kind].key+'?exact=1']=raw;
 }
 return {row,files};
}

test('original verifier checks full definition, response hashes and exact decimal row with anonymous bounded reads',async()=>{
 const {row,files}=originalFixture(),requests=[],crypto=require('node:crypto').webcrypto;
 const fetcher=async(url,options)=>{requests.push(options);return new Response(require('node:zlib').gzipSync(Buffer.from(files[url])));};
 const result=await ui.verifyOriginal(row,fetcher,crypto);
 assert.match(result,/Verified original definition/);assert.match(result,/row 0 on 2026-09-23/);
 assert(requests.every(r=>r.credentials==='omit'&&r.cache==='no-store'));
 assert.equal(ui.exactDecimal('6747704.00'),ui.exactDecimal('6.747704e6'));
 assert.notEqual(ui.exactDecimal('9007199254740992'),ui.exactDecimal('9007199254740993'));
});

test('original verifier refuses unbound values, definitions, dates, private paths and damaged bytes',async()=>{
 const crypto=require('node:crypto').webcrypto;
 for(const change of [r=>r.last_observed_value='9',r=>r.latest_date='2026-09-22',r=>r.source_definition.units='Billions of Dollars',r=>r.source_row=3]){
  const {row,files}=originalFixture();change(row);
  await assert.rejects(ui.verifyOriginal(row,async url=>new Response(files[url]),crypto));
 }
 const {row,files}=originalFixture();
 await assert.rejects(ui.verifyOriginal(row,async()=>new Response('{}'),crypto),/bytes/);
 row.evidence.observations.key='data/portfolio.json';let reads=0;
 await assert.rejects(ui.verifyOriginal(row,async()=>{reads++;},crypto));assert.equal(reads,0);
 assert.throws(()=>ui.exactDecimal('1e9999'),/bound/);assert.throws(()=>ui.exactDecimal('1'.repeat(513)),/bound/);
});
