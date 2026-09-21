const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-option-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/option-native.json'),'utf8'));
global.crypto=require('node:crypto').webcrypto;
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
async function loaded(){const p=await api.verifyPacket(structuredClone(fixture.publication),fetcher),c=await api.chain(p,'SPY',fetcher),rows=await api.records(c,0,fetcher);return {p,c,rows};}

test('publication, chain and exact records bind retained bytes',async()=>{
  const {p,c,rows}=await loaded();assert.equal(p.contract,api.CONTRACT);assert.equal(rows.length,2);
  assert.equal(rows[0].metrics.open_interest.value,'0');assert.equal(rows[0].clocks.daily_bar_updated.raw_nanoseconds,'1789759800000000001');
  assert.equal(rows[0].open_interest_date,null);assert.equal(c.total_session_volume,null);
  assert.ok(Object.isFrozen(rows[0].metrics.open_interest));
});
test('only approved evidence paths are requested',async()=>{
  let calls=0;for(const key of ['data/trade-tickets.json','audit-private/original.bin','https://example.com/data',api.PREFIX+'requests/'+'a'.repeat(64)+'.json'])await assert.rejects(()=>api.load(key,async()=>{calls++;}),/Unapproved/);
  assert.equal(calls,0);
});
test('changed numeric body, authorities and run bytes rejected',async()=>{
  const p=structuredClone(fixture.publication);p.chains.SPY.coverage.returned_rows=3;
  await assert.rejects(()=>api.verifyPacket(p,fetcher),/differs/);
  for(const key of ['calls_eligible','sizing_eligible','forecast_qualified']){const changed=structuredClone(fixture.publication);changed[key]=true;assert.equal(api.typed(changed),false);}
  await assert.rejects(()=>api.verifyPacket(fixture.publication,async url=>new Response(fixture.artifacts[url.slice(1)]+' ')),/run differs/);
});
test('unverified chains cannot supply calculation rows',async()=>{
  await assert.rejects(()=>api.chain(fixture.publication,'SPY',fetcher),/verified publication/);
  const {c}=await loaded();await assert.rejects(()=>api.records(structuredClone(c),0,fetcher),/verified source page/);
});
test('a changed record page fails its hash',async()=>{
  const {p}=await loaded(),c=await api.chain(p,'SPY',fetcher);
  await assert.rejects(()=>api.records(c,0,async url=>new Response(fixture.artifacts[url.slice(1)]+' ')),/bytes differ/);
});
test('codec cannot grant authority or overwrite shared definitions',async()=>{
  const {c}=await loaded(),raw=fixture.artifacts[c.record_blocks[0].artifact.key],block=JSON.parse(raw);
  block.rows[0].record.sizing_eligible=true;await assert.rejects(()=>api.unpack(block),/identity differs|authority differs/);
  const altered=JSON.parse(raw);altered.rows[0].cells.open_interest.unit='USD';await assert.rejects(()=>api.unpack(altered),/definition differs/);
});
test('acquisition expiry never changes the OI or Greek observation clocks',async()=>{
  const {p}=await loaded(),fresh=api.overview(p,Date.parse(p.generated_at)),old=api.overview(p,Date.parse(p.generated_at)+3*3600000);
  assert.match(fresh,/within acquisition review window/);assert.match(old,/review overdue/);
  assert.match(old,/OI, IV and Greek observation times are not supplied/);
});
test('exact decimals and invalid reported values remain visible',()=>{
  assert.equal(api.exact('123456789012345678.123456789'),'123,456,789,012,345,678.123456789');assert.equal(api.exact('0'),'0');assert.equal(api.exact(null),'Unavailable');
  assert.equal(api.metric({value:null,reported_value:'-0.001',state:'outside_domain'}),'Unqualified (-0.001)');
});
test('source text cannot inject markup',async()=>{
  const {rows}=await loaded(),row=structuredClone(rows[0]);row.contract_id='<img src=x onerror=bad()>';row.metrics.open_interest.unit='<script>bad</script>';
  const html=api.rowView(row);assert.doesNotMatch(html,/<img|<script>/);assert.match(html,/&lt;img/);
});
test('payoff exact arithmetic includes multiplier, direction and costs',async()=>{
  const {p,rows}=await loaded();
  const out=api.scenario(p,rows[0],{quantity:'2',premium:'1.125',spot:'102.25',cost:'3.50'});
  assert.equal(out.premium_cash_usd,'-225');assert.equal(out.intrinsic_value_usd,'450');assert.equal(out.net_usd,'221.5');assert.equal(out.maximum_loss_usd,'228.5');
  assert.deepEqual(out.evidence,rows[0].evidence);assert.deepEqual(out.run,p.replay);
});
test('short call has unbounded modeled loss; put downside is explicit',async()=>{
  const {p,rows}=await loaded();
  const call=api.scenario(p,rows[0],{quantity:'-1',premium:'2',spot:'110',cost:'1'});
  assert.equal(call.net_usd,'-801');assert.equal(call.maximum_loss_usd,null);assert.match(call.loss_scope,/Unbounded/);
  const put=api.scenario(p,rows[1],{quantity:'-1',premium:'2',spot:'0',cost:'1'});
  assert.equal(put.net_usd,'-9801');assert.equal(put.maximum_loss_usd,'9801');
});
test('no binary floating-point loss in small premiums',async()=>{
  const {p,rows}=await loaded(),out=api.scenario(p,rows[0],{quantity:'3',premium:'0.1',spot:'100.3',cost:'0.1'});
  assert.equal(out.net_usd,'59.9');
});
test('invalid assumptions and unverified or obsolete row selection fail',async()=>{
  const {p,rows}=await loaded(),good={quantity:'1',premium:'2',spot:'102',cost:'1'};
  for(const [key,value] of [['quantity','0'],['quantity','1.5'],['quantity','1000001'],['premium','-1'],['spot','NaN'],['cost','Infinity'],['cost','0.000000001']])assert.throws(()=>api.scenario(p,rows[0],{...good,[key]:value}));
  assert.throws(()=>api.scenario(p,structuredClone(rows[0]),good),/verified standard/);
  const next=await api.verifyPacket(structuredClone(fixture.publication),fetcher);assert.throws(()=>api.scenario(next,rows[0],good),/this publication/);
});
