const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
global.crypto=require('node:crypto').webcrypto;
const A=require('../jh-option-research.js'),F=require('../jh-fx-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/fx-native.json'),'utf8'));
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
const loaded=()=>F.verifyPacket(structuredClone(fixture.publication),fetcher);
const assumptions={quantity:'10000',entry:'1.10',future:'1.05',cost:'25'};
async function rewritten(mutate){const p=structuredClone(fixture.publication);mutate(p);const {replay,...output}=p,bytes=A.bytes(output),hash=await A.sha(bytes),key=F.PREFIX+'outputs/'+hash+'.json';
  const run=JSON.parse(fixture.artifacts[replay.manifest_key]);run.output={key,sha256:hash,bytes:bytes.length};run.output_sha256=hash;
  const runBytes=A.bytes(run),runHash=await A.sha(runBytes);p.replay={manifest_key:F.PREFIX+'runs/'+runHash+'.json',output_sha256:hash};
  const blobs={...fixture.artifacts,[key]:new TextDecoder().decode(bytes),[p.replay.manifest_key]:new TextDecoder().decode(runBytes)};
  const fetch=async url=>new Response(blobs[url.slice(1)]||'',{status:blobs[url.slice(1)]?200:404});return {p:await F.verifyPacket(p,fetch),fetch};}

test('exact publication, all pair identities and original row references verify',async()=>{
  const p=await loaded(),rows=await F.records(p,'EUR_USD',fetcher);assert.equal(Object.keys(p.pairs).length,19);assert.equal(rows.length,23);
  assert.equal(rows[0].source.pointer,'/results/0');assert.equal(rows[0].values.v.decimal,'0');assert.equal(rows[0].bar_finality_verified,false);
  assert.ok(Object.isFrozen(rows[0].source));assert.equal(F.evidence(p,'EUR_USD').run.output_sha256,p.replay.output_sha256);
});
test('recorded navigation stays on immutable run and does not fetch latest',async()=>{
  const seen=[],id=fixture.publication.replay.manifest_key.split('/').pop().slice(0,-5);
  const p=await F.recordedRun(id,async url=>{seen.push(url);return fetcher(url);});assert.deepEqual(p,fixture.publication);
  assert.ok(seen.every(url=>/\/(runs|outputs)\//.test(url)));assert.equal(F.recordedUrl(p,'USD_JPY'),'/fx-research.html?pair=USD_JPY&run='+id);
});
test('unverified copies cannot export evidence or calculate exposure',async()=>{
  const p=await loaded();await F.records(p,'EUR_USD',fetcher);const copy=structuredClone(p);
  for(const fn of [()=>F.evidence(copy,'EUR_USD'),()=>F.scenario(copy,'EUR_USD',assumptions),()=>F.recordedUrl(copy,'EUR_USD')])assert.throws(fn,/Verify/);
});
test('verifying output alone does not qualify source rows for a scenario',async()=>{
  const p=await loaded();assert.throws(()=>F.scenario(p,'EUR_USD',assumptions),/bar blocks/);
});
test('mutable publication or retained block tampering cannot be hidden',async()=>{
  const p=structuredClone(fixture.publication);p.pairs.EUR_USD.comparisons['1'].quoted_rate_change_decimal='99';
  await assert.rejects(()=>F.verifyPacket(p,fetcher),/publication differs/);
  const current=await loaded(),key=current.pairs.EUR_USD.bar_blocks[0].key;
  await assert.rejects(()=>F.records(current,'EUR_USD',async url=>new Response((fixture.artifacts[url.slice(1)]||'')+(url==='/'+key?' ':''))),/bytes differ/);
});
test('even a rehashed output must reproduce exact source-row arithmetic',async()=>{
  for(const mutate of [p=>p.pairs.EUR_USD.comparisons['5'].quoted_rate_change_exact.numerator='999',
    p=>p.pairs.EUR_USD.comparisons['5'].quoted_rate_change_decimal='99',p=>p.pairs.EUR_USD.comparisons['5'].elapsed_calendar_days_decimal='99',
    p=>p.pairs.EUR_USD.comparisons['5'].from.ordinal=0,p=>p.pairs.EUR_USD.latest_reported_row.reported_close_decimal='999']){
    const {p,fetch}=await rewritten(mutate);await assert.rejects(()=>F.records(p,'EUR_USD',fetch),/differ/);
  }
});
test('private originals, account paths and foreign URLs never issue fetches',async()=>{
  let calls=0;for(const key of ['data/trade-tickets.json','https://example.com/test','audit-private/a.bin',F.PREFIX+'requests/'+'a'.repeat(64)+'.json'])
    await assert.rejects(()=>F.load(key,async()=>{calls++;}),/Unapproved/);assert.equal(calls,0);
});
test('self-proclaimed investment authority or missing configured pairs fails',async()=>{
  for(const mutate of [p=>p.calls_eligible=true,p=>delete p.pairs.USD_TRY,p=>p.pairs.EUR_USD.sizing_eligible=true]){
    const p=structuredClone(fixture.publication);mutate(p);await assert.rejects(()=>F.verifyPacket(p,fetcher),/Descriptive/);
  }
});
test('long and short exposure use explicit quote currency and exact costs',async()=>{
  const p=await loaded();await F.records(p,'EUR_USD',fetcher);
  const long=F.scenario(p,'EUR_USD',assumptions),short=F.scenario(p,'EUR_USD',{...assumptions,quantity:'-10000'});
  assert.equal(long.gross_quote,'-500');assert.equal(long.net_quote,'-525');assert.equal(short.net_quote,'475');
  assert.equal(long.quote_currency,'USD');assert.equal(long.assumption_units.quantity,'signed_EUR_units');assert.deepEqual(long.run,p.replay);
});
test('fractional exposure retains product precision instead of floating rounding',async()=>{
  const p=await loaded();await F.records(p,'EUR_USD',fetcher);
  const out=F.scenario(p,'EUR_USD',{quantity:'0.00000001',entry:'1',future:'1.00000001',cost:'0'});
  assert.equal(out.net_quote,'0.0000000000000001');
});
test('scenario uses the selected pair quote currency, not an assumed USD label',async()=>{
  const p=await loaded();await F.records(p,'USD_JPY',fetcher);
  const out=F.scenario(p,'USD_JPY',{quantity:'100',entry:'150',future:'145',cost:'25'});
  assert.equal(out.quote_currency,'JPY');assert.equal(out.net_quote,'-525');assert.equal(out.assumption_units.quantity,'signed_USD_units');
});
test('missing inputs, negative costs, zero rates and oversized assumptions fail',async()=>{
  const p=await loaded();await F.records(p,'EUR_USD',fetcher);
  for(const [key,value] of [['cost',''],['cost','-1'],['entry','0'],['future','0'],['quantity','0'],['quantity','1e6'],['quantity','1000000000001'],['future','<script>'],['entry','1.000000001']])
    assert.throws(()=>F.scenario(p,'EUR_USD',{...assumptions,[key]:value}));
});
test('unverified metal quantity units cannot be used for FX sizing arithmetic',async()=>{
  const p=await loaded();await F.records(p,'XAU_USD',fetcher);assert.throws(()=>F.scenario(p,'XAU_USD',assumptions),/quantity units are unverified/);
});
test('review deadline never converts source acquisition to an observation date',()=>{
  const f={source_review_due_at:'2026-09-21T18:00:00Z'};assert.equal(F.review(f,Date.parse(f.source_review_due_at)),'Acquisition review overdue');assert.equal(F.review({}),'Review deadline unknown');
});
test('comparison and row tables label row offsets and preserve true zero values',async()=>{
  const p=await loaded();await F.records(p,'EUR_USD',fetcher);assert.match(F.comparisonsView(p,'EUR_USD'),/Observed row offset/);
  assert.match(F.rowsView(p,'EUR_USD'),/of 23/);assert.match(F.comparisonsView(p,'EUR_USD'),/window starts do not establish bar finality/);
});
