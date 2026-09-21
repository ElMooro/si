const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-etf-holdings.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/etf-holdings-native.json'),'utf8'));
const fetcher=async key=>{const raw=fixture.artifacts[key.slice(1)];return {ok:typeof raw==='string',arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
function packet(kind){const ref=fixture.publications[kind].replay,run=JSON.parse(fixture.artifacts[ref.manifest_key]);return {...JSON.parse(fixture.artifacts[run.output.key]),replay:ref};}
const holdings=packet('holdings'),lookthrough=packet('lookthrough'),at=Date.parse(holdings.generated_at);

test('Both holdings views bind their actual Python run and output bytes',async()=>{
  assert.deepEqual(await api.verifyPacket(holdings,fetcher),holdings);
  assert.deepEqual(await api.verifyPacket(lookthrough,fetcher),lookthrough);
  const wrong=structuredClone(holdings);wrong.funds.SPY.current.processed_date='2099-01-01';
  await assert.rejects(api.verifyPacket(wrong,fetcher),/Current holdings body differs/);
  await assert.rejects(api.verifyPacket({...holdings,replay:lookthrough.replay},fetcher),/run path differs/);
  await assert.rejects(api.verifyPacket(holdings,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Holdings run differs/);
  for(const key of ['private/positions.json','https://provider.invalid/','data/etf-holdings-research/rows/../../private.json'])await assert.rejects(api.load(key,fetcher),/Unapproved/);
  for(const flag of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'])assert.equal(api.typed({...holdings,[flag]:true}),false);
});

test('Full snapshot indexes and rows remain searchable beyond the first page',async()=>{
  const doc=await api.snapshot(holdings,'SPY','current',fetcher);
  assert.equal(doc.indexed_rows,307);assert.equal(doc.row_index.length,307);assert.equal(doc.parts.length,2);
  const last=api.searchRows(doc,'FIX304');assert.equal(last.length,1);assert.equal(last[0].part,1);
  const rows=await api.rowPart(doc,last[0].part,fetcher);assert.ok(rows.some(r=>r.row_id===last[0].row_id));
  const cash=api.searchRows(doc,'cash');assert.equal(cash.length,1);assert.equal(cash[0].constituent_ticker,null);
  assert.equal(api.searchRows(doc,'FIXTURE00001').length,1);
  const bad=structuredClone(holdings);bad.funds.VOO.current.snapshot=holdings.funds.SPY.current.snapshot;
  await assert.rejects(api.snapshot(bad,'VOO','current',fetcher),/snapshot differs/);
  await assert.rejects(api.snapshot(holdings,'SPY','invented',fetcher),/snapshot basis/);
  await assert.rejects(api.snapshot(holdings,'SPY','current',async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/artifact bytes differ/);
});

test('Membership uses exact identity, complete directory and separately dated rows',async()=>{
  const dir=await api.directory(holdings,fetcher);assert.equal(dir.security_count,307);
  assert.equal(api.searchSecurities(dir,'FIXTURE00001').length,1);
  const found=api.searchSecurities(dir,'EXAMPLE');assert.equal(found.length,1);
  const members=await api.memberships(dir,found[0].identity_key,fetcher);
  assert.equal(members.observed_funds.length,300);assert.equal(members.memberships.length,300);
  assert.equal(members.portfolio_weight,null);assert.equal(members.inferred_trade_usd,null);
  assert.equal(members.memberships.find(r=>r.fund==='VOO').effective_date,'2026-08-31');
  assert.match(api.memberView(members),/not independent votes or portfolio allocation weights/);
  await assert.rejects(api.memberships(dir,'unreviewed',fetcher),/verified provider identity/);
});

test('Position comparisons verify every page and pass a real cancellation signal',async()=>{
  const doc=await api.snapshot(holdings,'SPY','current',fetcher),row=(await api.rowPart(doc,0,fetcher))[0];
  const controller=new AbortController();let calls=0;
  const checked=async(key,options)=>{assert.equal(options.signal,controller.signal);new Request('https://fixture.invalid'+key,options);calls++;return fetcher(key);};
  const result=await api.comparison(holdings,'SPY',row.identity_key,checked,controller.signal);
  assert.equal(result.row.status,'observed_in_both');assert.equal(result.row.shares_held_change_raw_decimal,'20');assert.equal(result.row.inferred_trade_usd,null);
  assert.ok(calls>=3);assert.equal(result.summary.compared_identities,306);
  assert.equal((await api.comparison(holdings,'SPY',null,checked,controller.signal)).row,null);
});

test('Missing ticker, raw leverage and source clocks are explicit',async()=>{
  const spy=await api.snapshot(holdings,'SPY','current',fetcher),soxl=await api.snapshot(holdings,'SOXL','current',fetcher);
  assert.equal(soxl.weight_audit.raw_observed_sum_decimal,'3.2');
  assert.match(api.snapshotView(soxl,'current',at),/not a certified NAV fraction/);
  assert.match(api.snapshotView(spy,'current',Date.parse(spy.source_valid_until)),/source check is overdue/);
  const rows=await api.rowPart(spy,0,fetcher),cash=rows.find(r=>r.constituent_name==='Synthetic cash row');
  assert.equal(cash.weight_raw_decimal,'0');assert.equal(cash.shares_held_raw_decimal,null);
  assert.match(api.rowTable([cash]),/No ticker/);assert.match(api.rowTable([cash]),/Unavailable/);
  assert.match(api.rowDetail(rows[0]),/SHA-256/);assert.match(api.rowDetail(rows[0]),/does not establish a trade/);
});

test('Portfolio scenario uses user assumptions and never the unqualified weight',async()=>{
  const doc=await api.snapshot(holdings,'SPY','current',fetcher),row=(await api.rowPart(doc,0,fetcher))[0];
  const a=api.scenario(holdings,'SPY',doc,row,100000,10,-5,25,at);
  assert.equal(a.assumed_holding_exposure_usd,10000);assert.equal(a.gross_change_usd,-500);assert.equal(a.net_change_usd,-525);assert.equal(a.source_weight_used,false);
  assert.equal(api.scenario(holdings,'SPY',doc,row,-100000,10,-5,25,at).net_change_usd,475);
  assert.equal(api.scenario(holdings,'SPY',doc,row,100000,-10,-5,25,at).net_change_usd,475);
  const changed={...row,weight_raw_decimal:'999999'};assert.equal(api.scenario(holdings,'SPY',doc,changed,100000,10,-5,25,at).net_change_usd,-525);
  for(const values of [[true,1,1,0],[100,'',1,0],[NaN,1,1,0],[0,1,1,0],[100,1001,1,0],[100,1,-101,0],[100,1,1,-1]])assert.throws(()=>api.scenario(holdings,'SPY',doc,row,...values,at));
  assert.throws(()=>api.scenario(holdings,'SPY',doc,row,100,1,1,0,Date.parse(doc.source_valid_until)),/currently verified/);
  const prior=await api.snapshot(holdings,'SPY','prior',fetcher);assert.throws(()=>api.scenario(holdings,'SPY',prior,row,100,1,1,0,at),/currently verified/);
});

test('Scenario edits and basis changes invalidate results',async()=>{
  const doc=await api.snapshot(holdings,'SPY','current',fetcher),row=(await api.rowPart(doc,0,fetcher))[0];
  const form={elements:{exposure:{value:'100000'},fraction:{value:'10'},shock:{value:'-5'},cost:{value:'25'}}},out={innerHTML:'',textContent:''};
  let state={packet:holdings,ticker:'SPY',basis:'current',snapshot:doc,row};
  const invalidate=api.bindScenario({querySelector:s=>s==='[data-hd-scenario]'?form:out},()=>state),realNow=Date.now;Date.now=()=>at;
  try{form.onsubmit({preventDefault(){}});assert.match(out.innerHTML,/-\$525.00/);assert.match(out.innerHTML,/raw weight was not used/);
    form.oninput();assert.match(out.textContent,/recalculate/);state.basis='prior';invalidate();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/current collection/);
    state.basis='current';form.elements.fraction.value='';form.onsubmit({preventDefault(){}});assert.match(out.textContent,/Complete every/);
  }finally{Date.now=realNow;}
});

test('Escaping, complete page content and preserved predecessor stay intact',async()=>{
  const doc=await api.snapshot(holdings,'SPY','current',fetcher),row=(await api.rowPart(doc,0,fetcher))[0];
  const html=api.rowTable([{...row,constituent_ticker:'<img src=x onerror=evil()>'}]);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
  for(const page of ['etf-holdings.html','flow-lookthrough.html']){
    const source=fs.readFileSync(path.join(__dirname,'..',page),'utf8');assert.match(source,/jh-etf-holdings.js\?v=20260921-native1/);assert.doesNotMatch(source,/jh-page-ai.js|force mechanical buying/);
    for(const match of source.matchAll(/href="(\/[^"?#]*)"/g)){const route=match[1],target=route.endsWith('/')?route+'index.html':route;assert.ok(fs.existsSync(path.join(__dirname,'..',target.slice(1))),route);}
  }
  const old=fs.readFileSync(path.join(__dirname,'../docs/legacy/etf-holdings-flow-lookthrough-pre-native-20260921.html.txt'),'utf8');assert.ok(old.includes('force mechanical buying'));
});

test('Streaming byte bound rejects oversized evidence before parsing',async()=>{
  let cancelled=false,reads=0;
  await assert.rejects(api.load('data/etf-holdings-research.json',async()=>({ok:true,body:{getReader:()=>({
    read:async()=>{reads++;return {done:false,value:new Uint8Array(9*1024*1024)};},cancel:async()=>{cancelled=true;}})}})),/byte bound/);
  assert.equal(cancelled,true);assert.equal(reads,2);
});
