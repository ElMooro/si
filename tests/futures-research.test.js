const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
global.crypto=require('node:crypto').webcrypto;
const A=require('../jh-option-research.js'),F=require('../jh-futures-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/futures-native.json'),'utf8'));
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
const loaded=()=>F.verifyPacket(structuredClone(fixture.publication),fetcher);
const assumptions={quantity:'2',entry:'100',future:'101.5',cost:'25'};
async function rewritten(mutate){const p=structuredClone(fixture.publication);mutate(p);const {replay,...output}=p,bytes=A.bytes(output),hash=await A.sha(bytes),key=F.PREFIX+'outputs/'+hash+'.json';
  const run=JSON.parse(fixture.artifacts[replay.manifest_key]);run.output={key,sha256:hash,bytes:bytes.length};run.output_sha256=hash;
  const runBytes=A.bytes(run),runHash=await A.sha(runBytes);p.replay={manifest_key:F.PREFIX+'runs/'+runHash+'.json',output_sha256:hash};
  const blobs={...fixture.artifacts,[key]:new TextDecoder().decode(bytes),[p.replay.manifest_key]:new TextDecoder().decode(runBytes)};
  const fetch=async url=>new Response(blobs[url.slice(1)]||'',{status:blobs[url.slice(1)]?200:404});return {p:await F.verifyPacket(p,fetch),fetch};}
test('publication, source rows, calendar events and dated price arithmetic verify',async()=>{
  const p=await loaded(),rows=await F.records(p,'ES',fetcher);assert.equal(rows.ESZ6.length,23);assert.equal(F.value(rows.ESZ6[0],'window_start'),'1785542400000000001');
  assert.equal(F.session(p.products.ES,rows.ESZ6.at(-1)).scheduled_session_ended_by_capture,false);
  assert.equal(F.evidence(p,'ES').datasets['ES:schedules'].length,138);assert.ok(Object.isFrozen(rows.ESZ6[0].values));
});
test('all seven product record families verify',async()=>{const p=await loaded();for(const name of Object.keys(F.PRODUCTS))await F.records(p,name,fetcher);});
test('recorded navigation never fetches latest',async()=>{const id=fixture.publication.replay.manifest_key.split('/').pop().slice(0,-5),seen=[];
  const p=await F.recordedRun(id,async url=>{seen.push(url);return fetcher(url);});assert.deepEqual(p,fixture.publication);assert.ok(seen.every(url=>/\/(runs|outputs)\//.test(url)));
  assert.equal(F.recordedUrl(p,'ES','ESZ6'),'/futures-research.html?product=ES&ticker=ESZ6&run='+id);});
test('unverified data cannot export or calculate',async()=>{const p=await loaded();assert.throws(()=>F.scenario(p,'ES','ESZ6',assumptions),/record blocks/);
  await F.records(p,'ES',fetcher);assert.throws(()=>F.scenario(structuredClone(p),'ES','ESZ6',assumptions),/Verify/);});
test('retained record bytes and output hash tampering are rejected',async()=>{
  const copy=structuredClone(fixture.publication);copy.products.ES.contracts[0].comparisons.close['1'].absolute_change_decimal='999';await assert.rejects(()=>F.verifyPacket(copy,fetcher),/differs/);
  const p=await loaded(),key=p.datasets['ES:bars:ESZ6'].records[0].key;await assert.rejects(()=>F.records(p,'ES',async url=>new Response((fixture.artifacts[url.slice(1)]||'')+(url==='/'+key?' ':''))),/bytes differ/);});
test('rehashed wrong arithmetic, endpoints, coverage and quantity multiplier are rejected',async()=>{
  for(const mutate of [p=>p.products.ES.contracts[0].comparisons.close['5'].absolute_change_decimal='999',p=>p.products.ES.contracts[0].comparisons.close['5'].percent_change_exact.numerator='999',
    p=>p.products.ES.contracts[0].comparisons.close['5'].elapsed_calendar_days=5,p=>p.products.ES.contracts[0].comparisons.close['5'].from.ordinal=0,
    p=>p.products.ES.specification.usd_value_per_price_unit_per_contract_decimal='999',p=>p.products.ES.contracts[0].coverage.scheduled_ended_rows=23,
    p=>p.products.ES.matched_curves[0].far_minus_near_decimal='999',p=>p.products.ES.contracts[0].latest_reported_row.values.volume.decimal='999']){
    const {p,fetch}=await rewritten(mutate);await assert.rejects(()=>F.records(p,'ES',fetch),/differ/);}});
test('a rehashed premature calendar close is rejected against captured event rows',async()=>{
  const {p,fetch}=await rewritten(p=>p.products.ES.session_calendar.sessions['2026-09-21'].scheduled_close_utc='2026-09-21T18:00:00+00:00');await assert.rejects(()=>F.records(p,'ES',fetch),/calendar end differs/);});
test('private, foreign, request and account paths cannot issue fetches',async()=>{let calls=0;for(const key of ['data/trade-tickets.json','https://example.com/a','audit-private/a.bin',F.PREFIX+'requests/'+'a'.repeat(64)+'.json'])await assert.rejects(()=>F.load(key,async()=>calls++),/Unapproved/);assert.equal(calls,0);});
test('self-promoted votes or missing products fail publication validation',async()=>{for(const edit of [p=>p.calls_eligible=true,p=>delete p.products.NG,p=>p.products.ES.sizing_eligible=true]){const p=structuredClone(fixture.publication);edit(p);await assert.rejects(()=>F.verifyPacket(p,fetcher),/Descriptive/);}});
test('explicit long and short contracts use verified provider multiplier and costs',async()=>{const p=await loaded();await F.records(p,'ES',fetcher);
  const long=F.scenario(p,'ES','ESZ6',assumptions),short=F.scenario(p,'ES','ESZ6',{...assumptions,quantity:'-2'});assert.equal(long.gross_usd,'3000');assert.equal(long.net_usd,'2975');assert.equal(short.net_usd,'-3025');assert.equal(long.currency,'USD');assert.deepEqual(long.run,p.replay);});
test('negative prices and tiny exact differences remain expressible',async()=>{const p=await loaded();await F.records(p,'CL',fetcher);
  const negative=F.scenario(p,'CL','CLZ6',{quantity:'1',entry:'0',future:'-37',cost:'1'});assert.equal(negative.net_usd,'-37001');
  const tiny=F.scenario(p,'CL','CLZ6',{quantity:'1',entry:'1',future:'1.00000001',cost:'0'});assert.equal(tiny.net_usd,'0.00001');});
test('fractional contracts, zero position, implicit costs and malformed prices fail',async()=>{const p=await loaded();await F.records(p,'ES',fetcher);
  for(const [k,v] of [['quantity','0'],['quantity','0.5'],['quantity','1000001'],['quantity','1e3'],['cost',''],['cost','-1'],['entry',''],['future','Infinity'],['entry','1.000000001']])assert.throws(()=>F.scenario(p,'ES','ESZ6',{...assumptions,[k]:v}));});
test('tables keep close and settlement separate and label offsets as rows',async()=>{const p=await loaded();await F.records(p,'ES',fetcher);assert.match(F.comparisonsView(p,'ES','ESZ6'),/settlement_price/);assert.match(F.comparisonsView(p,'ES','ESZ6'),/not trading days/);assert.match(F.rowsView(p,'ES','ESZ6'),/of 23/);});
