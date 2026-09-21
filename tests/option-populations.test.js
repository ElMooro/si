const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
global.crypto=require('node:crypto').webcrypto;
const A=require('../jh-option-research.js'),P=require('../jh-option-populations.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/option-population-native.json'),'utf8'));
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
async function loaded(){const p=await P.verifyPacket(structuredClone(fixture.publication),fetcher),c=await P.chain(p,'SPY',fetcher),groups=await P.groupBlock(c,0,fetcher);return {p,c,groups};}

test('native populations bind source capture, exact groups and true zeros',async()=>{
  const {p,c,groups}=await loaded();assert.equal(p.universe.length,10);assert.equal(c.group_count,2);
  assert.equal(groups[0].sides.call.reported_open_interest.value,'0');assert.equal(groups[0].sides.put.reported_open_interest.value,'3');
  assert.equal(groups[0].sides.put.gamma_oi_shares.value,'3.00');assert.equal(groups[1].sides.call.gamma_oi_shares.value,null);
  assert.equal(groups[1].sides.call.reported_open_interest.value,'5');assert.ok(Object.isFrozen(groups[0].sides.call));
});
test('contributing records match exact pages, row indices, contracts and field membership',async()=>{
  const {groups}=await loaded(),items=await P.sourceRows(groups[0],fetcher);
  assert.equal(items.length,2);assert.equal(items[0].row.evidence.row_index,0);assert.equal(items[1].row.evidence.row_index,1);
  assert.match(items[0].url,/&run=[a-f0-9]{64}&page=1&row=0$/);assert.deepEqual(items[0].fields,['reported_open_interest','gamma_oi_shares','delta_oi_shares']);
  const partial=await P.sourceRows(groups[1],fetcher);assert.deepEqual(partial[0].fields,['reported_open_interest','delta_oi_shares']);
});
test('recorded population loads pinned source runs without either mutable head',async()=>{
  const id=fixture.publication.replay.manifest_key.split('/').pop().slice(0,-5),paths=[];
  const p=await P.recordedRun(id,async url=>{paths.push(url);return fetcher(url);});
  assert.deepEqual(p,fixture.publication);assert.ok(paths.length>0);assert.ok(paths.every(k=>k!=='/'+P.CURRENT&&k!=='/'+A.CURRENT));
  assert.match(P.recordedUrl(p,'SPY','2026-09-25'),/\/gex\/\?underlying=SPY&run=[a-f0-9]{64}&expiry=2026-09-25$/);
});
test('unverified populations, clones and groups cannot supply source evidence',async()=>{
  await assert.rejects(()=>P.chain(fixture.publication,'SPY',fetcher),/Verified/);
  const {p,c,groups}=await loaded();await assert.rejects(()=>P.groupBlock(structuredClone(c),0,fetcher),/Verified/);
  await assert.rejects(()=>P.sourceRows(structuredClone(groups[0]),fetcher),/Verified/);
  assert.throws(()=>P.recordedUrl(p,'OTHER'),/Verified/);
});
test('forbidden paths and malformed IDs are rejected before fetching',async()=>{
  let n=0;for(const key of ['data/trade-tickets.json','audit-private/original.bin','https://example.com',P.PREFIX+'inputs/'+'a'.repeat(64)+'.json'])await assert.rejects(()=>P.load(key,()=>{n++;}),/Unapproved/);
  await assert.rejects(()=>P.recordedRun('../latest',()=>{n++;}),/Exact recorded/);assert.equal(n,0);
});
test('altered publication and run bytes cannot pass verification',async()=>{
  const p=structuredClone(fixture.publication);p.underlyings.SPY.totals.sides.call.reported_open_interest.value='999';
  await assert.rejects(()=>P.verifyPacket(p,fetcher),/body differs/);
  await assert.rejects(()=>P.verifyPacket(structuredClone(fixture.publication),async url=>new Response(fixture.artifacts[url.slice(1)]+' ')),/run differs/);
});
test('authority, observation-time and field coverage drift are rejected',()=>{
  for(const key of ['calls_eligible','sizing_eligible','forecast_qualified','execution_eligible']){const p=structuredClone(fixture.publication);p[key]=true;assert.equal(P.typed(p),false);}
  const p=structuredClone(fixture.publication);p.underlyings.SPY.totals.sides.call.gamma_oi_shares.complete_field_coverage=true;assert.equal(P.typed(p),false);
  const q=structuredClone(fixture.publication);q.underlyings.SPY.totals.sides.call.gamma_oi_shares.observation_time=q.generated_at;assert.equal(P.typed(q),false);
});
test('changed group and contributing record bodies fail their retained digest',async()=>{
  const {c,groups}=await loaded();await assert.rejects(()=>P.groupBlock(c,0,async url=>new Response(fixture.artifacts[url.slice(1)]+' ')),/bytes differ/);
  await assert.rejects(()=>P.sourceRows(groups[0],async url=>new Response(fixture.artifacts[url.slice(1)]+' ')),/bytes differ/);
});
test('capture-date expiry never substitutes a nearby date or current calendar date',async()=>{
  const {p,c}=await loaded();assert.equal(P.captureDate(p),'2026-09-21');assert.deepEqual(P.expiries(c),['2026-09-25']);
  assert.deepEqual(P.indices(c,P.captureDate(p)),[]);assert.deepEqual(P.indices(c,'2026-09-25'),[0,1]);
});
test('acquisition aging remains distinct from unknown Greek and OI clocks',async()=>{
  const {p}=await loaded();const text=P.overview(p,Date.parse(p.source_capture_completed_at)+3*3600000);
  assert.match(text,/review overdue/);assert.match(text,/OI and Greek observation dates are not supplied/);
});
test('source evidence text is escaped and the units remain explicit',async()=>{
  const {c,groups}=await loaded();assert.match(P.summary(c),/shares_per_USD_underlying_move/);
  assert.match(P.groupTable(groups),/shares per USD underlying move/);
  const items=await P.sourceRows(groups[0],fetcher),changed=structuredClone(items);changed[0].row.contract_id='<script>bad</script>';
  assert.doesNotMatch(P.evidence(changed),/<script>/);assert.match(P.evidence(changed),/&lt;script/);
});
test('readable decimal display preserves true zero and tiny nonzero values',()=>{
  assert.equal(P.display('4520873.247499084439705564299489000'),'≈ 4,520,873.247499');
  assert.equal(P.display('123.9999999'),'≈ 124');assert.equal(P.display('-1.0000005'),'≈ -1.000001');
  assert.equal(P.display('0'),'0');assert.equal(P.display('0.000000001'),'0 < value < 0.000001');
  assert.equal(P.display('-0.000000001'),'-0.000001 < value < 0');assert.equal(P.display(null),'Unavailable');
});
test('whole predecessor pages stay preserved and native pages use source drill-down',()=>{
  for(const file of ['gex/index.html','0dte/index.html']){const s=fs.readFileSync(path.join(__dirname,'..',file),'utf8');assert.match(s,/jh-option-populations-page\.js/);assert.match(s,/data-op-evidence/);assert.doesNotMatch(s,/SpotGamma-grade|forced to hedge/);}
  const hub=fs.readFileSync(path.join(__dirname,'..','options.html'),'utf8');assert.match(hub,/data-option-populations-hub/);assert.doesNotMatch(hub,/posG\?'POSITIVE GAMMA':'NEGATIVE GAMMA'/);
});
