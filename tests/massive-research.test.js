const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
global.crypto=require('node:crypto').webcrypto;
const A=require('../jh-option-research.js'),M=require('../jh-massive-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/massive-native.json'),'utf8'));
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
const loaded=()=>M.verifyPacket(structuredClone(fixture.publication),fetcher);

test('recorded composition binds all parent outputs and five SPY references',async()=>{
  const p=await loaded(),rows=M.instrument(p,'SPY');assert.equal(rows.length,5);
  assert.equal(rows.find(r=>r.node.kind==='options').parent.reported_open_interest.calls.value,'0');
  assert.ok(Object.isFrozen(rows[0].parent));assert.equal(p.independent_investment_votes,0);
  assert.equal(p.dependency_graph.nodes[rows[0].reference.node].output.sha256,rows[0].node.output.sha256);
});
test('recorded selection never reads a mutable head and retains different options vintages',async()=>{
  const seen=[],id=fixture.publication.replay.manifest_key.split('/').pop().slice(0,-5);
  const p=await M.recordedRun(id,async url=>{seen.push(url);return fetcher(url);});
  assert.deepEqual(p,fixture.publication);assert.ok(seen.every(url=>/\/(runs|outputs)\//.test(url)));
  const family=p.dependency_graph.measurement_families.find(f=>f.family==='option_chain');assert.equal(family.nodes.length,2);
  assert.match(M.recordedUrl(p,'SPY'),/market-evidence\.html\?symbol=SPY&run=[a-f0-9]{64}$/);
});
test('exact parent navigation pins contract, population and fund research',async()=>{
  const p=await loaded(),rows=M.instrument(p,'SPY');
  assert.match(M.parentUrl(rows.find(r=>r.node.kind==='options'),'SPY'),/option-chain-research\.html\?underlying=SPY&run=[a-f0-9]{64}$/);
  assert.match(M.parentUrl(rows.find(r=>r.node.kind==='populations'),'SPY'),/gex\/\?underlying=SPY&run=[a-f0-9]{64}$/);
  assert.match(M.parentUrl(rows.find(r=>r.node.kind==='etf_desk'),'SPY'),/etf\.html\?fund=SPY&run=[a-f0-9]{64}$/);
});
test('copied unverified packets cannot generate rows, links or exported evidence',async()=>{
  const p=await loaded(),copy=structuredClone(p);
  for(const fn of [()=>M.instrument(copy,'SPY'),()=>M.exportEvidence(copy,'SPY'),()=>M.recordedUrl(copy,'SPY')])assert.throws(fn,/Verify/);
});
test('altered composition and parent bytes cannot pass verification',async()=>{
  const altered=structuredClone(fixture.publication);altered.sources.options.instrument_count=999;
  await assert.rejects(()=>M.verifyPacket(altered,fetcher),/publication differs/);
  const p=fixture.publication,parent=Object.values(p.dependency_graph.nodes)[0].output.key;
  await assert.rejects(()=>M.verifyPacket(structuredClone(p),async url=>new Response((fixture.artifacts[url.slice(1)]||'')+(url==='/'+parent?' ':''))),/bytes differ/);
});
test('foreign URLs, private originals, accounts and request records cannot be fetched',async()=>{
  let calls=0;for(const key of ['https://example.com/data','data/trade-tickets.json','audit-private/source.bin',M.PREFIX+'requests/'+'a'.repeat(64)+'.json'])
    await assert.rejects(()=>M.load(key,async()=>{calls++;}),/Unapproved/);
  assert.equal(calls,0);
});
test('missing and unknown review clocks cannot become fresh, and boundary expires',()=>{
  assert.equal(M.review({node:null}),'Unavailable / unqualified');
  assert.equal(M.review({node:'x'}),'Source review deadline unknown');
  const row={node:'x',source_review_due_at:'2026-09-21T17:00:00Z'};
  assert.equal(M.review(row,Date.parse(row.source_review_due_at)),'Source review overdue');
});
test('source board and export keep exact values, pointers, clocks and qualification scope',async()=>{
  const p=await loaded(),html=M.instrumentView(p,'SPY',Date.parse('2026-09-21T17:00:00Z')),out=M.exportEvidence(p,'SPY');
  assert.match(html,/Call open interest · contracts/);assert.match(html,/>0<\/td>/);assert.match(html,/OI observation date/);assert.match(html,/Unknown/);
  assert.match(html,/Source review overdue/);assert.match(html,/\/chains\/SPY/);assert.match(M.sourcesView(p),/not independent investment votes/);
  assert.equal(out.composition.output_sha256,p.replay.output_sha256);assert.equal(out.verification.original_provider_replay_performed_by_composite,false);
  assert.equal(out.measurements.length,5);assert.equal(out.sizing_eligible,false);
});
test('empty or malformed recorded identifiers cannot silently load latest',async()=>{
  let calls=0;await assert.rejects(()=>M.recordedRun('',async()=>{calls++;}),/Exact recorded/);assert.equal(calls,0);
  const seen=[];await assert.rejects(()=>M.recordedRun('a'.repeat(64),async url=>{seen.push(url);return new Response('',{status:404});}),/unavailable/);
  assert.equal(seen.length,1);assert.ok(seen[0].includes('/runs/'));
});
test('a self-claimed investment permission is rejected',async()=>{
  const p=structuredClone(fixture.publication);p.calls_eligible=true;
  await assert.rejects(()=>M.verifyPacket(p,fetcher),/descriptive composite/);
});
