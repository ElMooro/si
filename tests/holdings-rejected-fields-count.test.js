// Synthetic retained evidence only; no production/provider/diagnostic requests.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const H=require('../jh-etf-holdings.js'),D=require('../jh-etf-desk-research.js');
const holdings=JSON.parse(fs.readFileSync(__dirname+'/fixtures/etf-holdings-native.json','utf8'));
const desk=JSON.parse(fs.readFileSync(__dirname+'/fixtures/etf-desk-native.json','utf8'));
const ownership=require('./ownership-summary-fixture.cjs');
const packet=kind=>{const replay=holdings.publications[kind].replay,run=JSON.parse(holdings.artifacts[replay.manifest_key]);return {...JSON.parse(holdings.artifacts[run.output.key]),replay};};
const at=Date.parse(packet('holdings').generated_at),label='rows with rejected fields: ';
const examples=[['holdings',packet('holdings'),'SPY'],['lookthrough',packet('lookthrough'),'SPY'],['desk',desk.publication,'BND']];
const artifacts={...holdings.artifacts,...desk.artifacts};
function fetcher(reads){return async key=>{reads.push(key);const raw=artifacts[key.slice(1)];return {ok:typeof raw==='string',arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};}

test('verified current/prior snapshots expose the existing count in holdings, lookthrough and desk inspectors',async()=>{
  for(const [name,p,ticker] of examples){
    const reads=[],fetch=fetcher(reads);
    await (name==='desk'?D:H).verifyPacket(p,fetch);
    const wrapped=name==='desk'?{funds:{[ticker]:p.funds[ticker].holdings}}:p;
    for(const role of ['current','prior']){
      const doc=await H.snapshot(wrapped,ticker,role,fetch),before=reads.length,copy=structuredClone(doc);
      const value=doc.quality.rows_with_field_errors,html=H.snapshotView(doc,role,at);
      assert.equal(Number.isSafeInteger(value)&&value>=0,true);
      assert.ok(html.includes(label+value+'.'));
      assert.match(html,role==='current'?/current collection/:/earlier query cutoff/);
      assert.ok(html.includes('Missing ticker: '+doc.quality.missing_ticker_rows));
      assert.ok(html.includes('missing identity: '+doc.quality.missing_identity_rows));
      assert.ok(html.includes('duplicate identity rows: '+doc.quality.duplicate_identity_rows));
      assert.match(html,/reconstructed rows/);assert.match(html,/Current fund ownership is not confirmed/);
      assert.deepEqual(doc,copy);assert.equal(reads.length,before);
    }
    assert.equal(reads.some(k=>k.includes('/directories/')),false);
  }
});

test('measured zero and nonnegative safe integers display without coercing missing or malformed counts',async()=>{
  const invalid=[undefined,null,true,false,-1,-0.5,0.5,NaN,Infinity,-Infinity,'0','2','',[],{},Number.MAX_SAFE_INTEGER+1,'<img src=x onerror=alert(1)>'];
  for(const [name,p,ticker] of examples)for(const role of ['current','prior']){
    const wrapped=name==='desk'?{funds:{[ticker]:p.funds[ticker].holdings}}:p;
    const original=await H.snapshot(wrapped,ticker,role,fetcher([]));
    for(const value of [0,1,17,Number.MAX_SAFE_INTEGER]){
      const doc=structuredClone(original);doc.quality.rows_with_field_errors=value;
      assert.ok(H.snapshotView(doc,role,at).includes(label+value+'.'));
    }
    const absent=structuredClone(original);delete absent.quality.rows_with_field_errors;
    assert.ok(H.snapshotView(absent,role,at).includes(label+'Unavailable.'));
    for(const value of invalid){
      const doc=structuredClone(original);doc.quality.rows_with_field_errors=value;
      const html=H.snapshotView(doc,role,at);
      assert.ok(html.includes(label+'Unavailable.'));assert.doesNotMatch(html,/<img|onerror=/);
      assert.equal(doc.quality.rows_with_field_errors,value);
    }
  }
});

test('field-error-only exclusion gains an explanation without changing qualification or row-level detail',async()=>{
  const p=packet('holdings'),doc=await H.snapshot(p,'SPY','current',fetcher([]));
  doc.quality.missing_identity_rows=0;doc.quality.duplicate_identity_rows=0;doc.quality.rows_with_field_errors=1;
  const date=Object.keys(doc.effective_dates)[0],before=structuredClone(doc);
  assert.equal(H.heatReason(p,doc,date,at),'Identity or field coverage unresolved');
  const html=H.snapshotView(doc,'current',at);
  assert.match(html,/missing identity: 0; duplicate identity rows: 0; rows with rejected fields: 1/);
  assert.equal(H.heatReason(p,doc,date,at),'Identity or field coverage unresolved');assert.deepEqual(doc,before);
  const f=ownership(),m=f.manifest(),g=m.cohorts.find(c=>c.kind==='current_membership');
  m.funds.SPY.qualification_exclusion='identity_or_field_coverage_unresolved';
  assert.match(H.ownershipCoverage(m,g,Date.parse(f.p.generated_at)),/identity_or_field_coverage_unresolved/);
  const rows=await H.rowPart(doc,0,fetcher([])),row={...rows[0],field_errors:['shares_held','market_value']};
  assert.match(H.rowDetail(row),/Rejected fields: shares_held, market_value/);
  assert.match(H.rowDetail(row),/a change does not establish a trade/);
});

test('desk snapshot explanation does not redefine its complete-returned source check',async()=>{
  const p=structuredClone(desk.publication),fund=p.funds.BND,wrapped={funds:{BND:fund.holdings}};
  for(const role of ['current','prior']){
    const doc=await H.snapshot(wrapped,'BND',role,fetcher([]));doc.quality.rows_with_field_errors=3;
    assert.ok(H.snapshotView(doc,role,at).includes(label+'3.'));
  }
  const before=D.eligibility(fund,at);fund.holdings.current.quality.rows_with_field_errors=3;
  assert.deepEqual(D.eligibility(fund,at),before);
  assert.match(D.summary(p,at),/complete returned holdings/);
});
