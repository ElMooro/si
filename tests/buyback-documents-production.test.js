const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
globalThis.crypto=crypto.webcrypto;
globalThis.JHBuybackDocuments=require('../jh-buyback-documents.js');
const api=require('../jh-buyback-filings.js'),ROOT=path.join(__dirname,'..');
const baseRaw=fs.readFileSync(path.join(ROOT,api.SNAPSHOT)),base=JSON.parse(baseRaw);
const documentRaw=fs.readFileSync(path.join(ROOT,api.DOCUMENTS.path)),documents=JSON.parse(documentRaw);
const current=fs.readFileSync(path.join(ROOT,'tests/fixtures/buyback-scanner-captured-packet.json'));
const now=Math.max(Date.parse(base.capture_completed_at),Date.parse(documents.capture_completed_at))+1000;
function fetcher(changes={}){
 const calls=[];const get=async(url,options)=>{
  calls.push({url,options});let raw;
  if(url===api.SNAPSHOT)raw=baseRaw;
  else if(url===api.CURRENT)raw=changes.current??current;
  else if(url===api.DOCUMENTS.path)raw=changes.documents??documentRaw;
  else throw Error('Unexpected source');
  return new Response(raw);
 };get.calls=calls;return get;
}

test('actual accepted originals expose every one of 116 documents with the matching independent proof',async()=>{
 const get=fetcher(),state=await api.load(get,now);
 assert.equal(state.errors.length,0);assert.deepEqual(state.documents,documents);assert(state.current_matches_capture);
 assert.equal(documents.reported_rows,9);assert.equal(documents.distinct_filings,9);assert.equal(documents.documents,116);
 assert.deepEqual(get.calls.map(c=>c.url),[api.SNAPSHOT,api.CURRENT,api.DOCUMENTS.path]);
 assert(get.calls.every(c=>c.options.credentials==='omit'&&c.options.cache==='no-store'));
 const html=api.render(state,now);let count=0;
 for(const f of documents.filings)for(const d of f.documents){assert(html.includes(d.document_url));assert(html.includes(d.sha256));count++;}
 assert.equal(count,116);assert.equal((html.match(/Inspect document byte evidence/g)||[]).length,116);
 assert(html.includes('Independent verification report'));assert(html.includes('Offline verifier'));
 assert.match(api.DOCUMENTS.verification_report_url,/^https:\/\/github\.com\/ElMooro\/si\/blob\/[a-f0-9]{40}\/aws\/ops\/reports\/latest\/ops_6113_buyback_document_independent_acceptance\.md$/);
 const dhi=api.table(base,'DHI',documents);assert(dhi.includes('Inspect all 12 embedded documents'));assert.equal((dhi.match(/Inspect document byte evidence/g)||[]).length,12);
});

test('corrupt document evidence is withheld without erasing the verified source or whole current packet',async()=>{
 const state=await api.load(fetcher({documents:Buffer.concat([documentRaw,Buffer.from(' ')])}),now);
 assert.equal(state.documents,null);assert(state.catalog);assert(state.current);assert(state.current_matches_capture);
 assert(state.errors.some(e=>e.startsWith('Document inventory unavailable')));
 const html=api.render(state,now);assert(!html.includes('Inspect document byte evidence'));assert(html.includes('Open complete SEC submission'));
});

test('page wiring conserves both complete predecessors and binds the large reviewed catalog',()=>{
 const migration=JSON.parse(fs.readFileSync(path.join(ROOT,'tests/fixtures/buyback-documents-page-migration.json')));
 for(const item of migration.files){const old=fs.readFileSync(path.join(ROOT,item.predecessor));assert.equal(old.length,item.bytes);assert.equal(crypto.createHash('sha256').update(old).digest('hex'),item.sha256);}
 assert.equal(migration.catalog.sha256,api.DOCUMENTS.sha256);assert.equal(migration.catalog.bytes,documentRaw.length);
 const html=fs.readFileSync(path.join(ROOT,'buyback-scanner.html'),'utf8');assert(html.indexOf('/jh-buyback-documents.js')<html.indexOf('/jh-buyback-filings.js'));
 assert(html.includes('#root .stats>.jh-secbadge{grid-column:1/-1;justify-self:start}'));
});
