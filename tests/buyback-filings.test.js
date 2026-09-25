const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
globalThis.crypto=crypto.webcrypto;
const api=require('../jh-buyback-filings.js'),ROOT=path.join(__dirname,'..');
const raw=fs.readFileSync(path.join(ROOT,api.SNAPSHOT)),catalog=JSON.parse(raw),now=Date.parse(catalog.capture_completed_at)+1;
const captured=fs.readFileSync(path.join(__dirname,'fixtures/buyback-scanner-captured-packet.json'));
function fetcher(change={}){const calls=[];const fn=async(url,options)=>{calls.push({url,options});return new Response(url===api.SNAPSHOT?(change.catalog??raw):(change.current??captured),{status:change.fail===url?503:200});};fn.calls=calls;return fn;}

test('actual accepted dated catalog and complete original scanner bytes reconcile',async()=>{
 const get=fetcher(),state=await api.load(get,now);assert.deepEqual(state.catalog,catalog);assert.equal(state.current_matches_capture,true);
 assert.equal(state.catalog.reported_rows,9);assert.equal(state.catalog.documents,116);assert.equal(state.current.top_opportunities.length,9);
 assert.equal(state.current_sha256,catalog.source_packet.sha256);assert.equal(captured.length,catalog.source_packet.bytes);
 assert.equal(get.calls.length,2);assert.deepEqual(get.calls.map(c=>c.url),[api.SNAPSHOT,api.CURRENT]);
 assert(get.calls.every(c=>c.options.credentials==='omit'&&c.options.cache==='no-store'));
 const html=api.render(state,now);for(const label of catalog.rows.map(r=>r.reported_label))assert(html.includes(label));
 for(const word of ['116','Exact packet matches','days old','Monday at 12:00','not a complete market census','WAIT / abstain','unverified'])assert(html.includes(word),word);
 assert(!html.includes('Primary Trade'));assert(!html.includes('90d expected drift'));assert(!html.includes('Signal:</span>'));
});

test('hash tampering and future captures never receive a verified source table',async()=>{
 for(const options of [{catalog:Buffer.concat([raw,Buffer.from(' ')])},{future:true}]){
  const state=await api.load(fetcher(options),options.future?now-2:now);assert.equal(state.catalog,null);assert.equal(state.current_matches_capture,false);
  assert(state.current);assert.equal(state.errors.length,1);assert(!api.render(state,now).includes('Open complete SEC submission'));
 }
});

test('changed live packets stay completely inspectable without inheriting old filing verification',async()=>{
 const current=JSON.parse(captured);current.top_opportunities.push({ticker:'NEW',authorization_usd:0,trade_ticket:{size_guidance:'INJECTED RECOMMENDATION'}});
 const state=await api.load(fetcher({current:JSON.stringify(current)}),now);assert.equal(state.current_matches_capture,false);
 assert.equal(state.current.top_opportunities.length,10);assert.equal(state.current.top_opportunities[9].authorization_usd,0);
 const html=api.render(state,now);assert(html.includes('10 current reported rows'));assert(html.includes('Current packet differs'));
 assert(!html.includes('INJECTED RECOMMENDATION'));assert.equal(state.catalog.rows.length,9);
});

test('missing current data does not erase dated originals and current clocks cannot appear falsely current',async()=>{
 const unavailable=await api.load(fetcher({fail:api.CURRENT}),now);assert(unavailable.catalog);assert.equal(unavailable.current,null);
 assert(api.render(unavailable,now).includes('Current scanner packet unavailable'));
 for(const as_of of [null,'2026-09-25T17:00:00','2027-01-01T00:00:00Z']){
  assert(api.currentStatus({current:{as_of,top_opportunities:[]}},now).includes('missing, invalid or future'));
 }
 const malformed=await api.load(fetcher({current:'{"top_opportunities":[null]}'}),now);assert.equal(malformed.current,null);
});

test('complete catalog row conservation, sorting, filtering and escaping do not rank or mutate evidence',()=>{
 const before=JSON.stringify(catalog),rows=api.selected(catalog);assert.equal(rows.length,9);assert.equal(rows[0].reported_label,'CMRC');
 assert.equal(api.selected(catalog,'0000882184').length,1);assert.equal(api.selected(catalog,'no-such-filing').length,0);
 const evil=structuredClone(catalog);evil.rows[0].reported_label='<img src=x onerror=bad>';evil.rows[0].reported_company='" onmouseover="bad';
 const html=api.table(evil);assert(html.includes('&lt;img'));assert(!html.includes('<img'));assert(html.includes('&quot;'));
 assert.equal(JSON.stringify(catalog),before);assert(api.table(catalog,'nonsense').includes('0 of 9'));
});

test('unsafe links, incomplete counts, investment permissions and mismatched source identities are rejected',()=>{
 for(const alter of [c=>c.rows[0].submission_url='javascript:bad',c=>c.rows[0].source_row=99,c=>c.rows[0].issuer_cik='123',
  c=>c.documents++,c=>c.rows.pop(),c=>c.rows[0].accession='bad',c=>c.rows[0].original_sha256='bad',
  c=>c.source_packet.key='data/account.json',c=>c.acceptance_report_url='https://example.com',...api.FLAGS.map(k=>c=>c[k]=true)]){
  const changed=structuredClone(catalog);alter(changed);assert.throws(()=>api.validate(changed,now));
 }
});

test('page loads the verified viewer and preserves whole predecessor and all legacy packet fields',()=>{
 const html=fs.readFileSync(path.join(ROOT,'buyback-scanner.html'),'utf8');
 for(const file of ['jh-buyback-filings.js','jh-data-inspector.js','jh-research-inspection.js'])assert(html.includes('src="/'+file+'?'));
 for(const old of ['renderTopTicket','tranche_priors_drift_90d_pct','jh-page-ai.js','data-bars='])assert(!html.includes(old));
 const migration=JSON.parse(fs.readFileSync(path.join(ROOT,'tests/fixtures/buyback-scanner-filing-migration.json')));
 for(const item of migration.files){const bytes=fs.readFileSync(path.join(ROOT,item.predecessor));assert.equal(bytes.length,item.bytes);assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),item.sha256);}
 assert.equal(migration.catalog.sha256,api.SHA);assert.equal(catalog.source_packet.sha256,migration.whole_scanner_packet_sha256);
 const packet=JSON.parse(captured);assert(packet.recommended_trade);assert(packet.forward_expectations);assert(packet.top_opportunities.every(r=>r.trade_ticket));
});
