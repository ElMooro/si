// Synthetic source data only; no production or private source access.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const A=require('../jh-etf-holdings.js');
const fixture=JSON.parse(fs.readFileSync(__dirname+'/fixtures/etf-holdings-native.json'));
const replay=fixture.publications.holdings.replay,run=JSON.parse(fixture.artifacts[replay.manifest_key]);
const p={...JSON.parse(fixture.artifacts[run.output.key]),replay},at=Date.parse(p.generated_at);
const fetcher=async key=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode(fixture.artifacts[key.slice(1)]).buffer});
const row=(id='id',extra={})=>({row_id:id,identity_key:id,figi:id,constituent_ticker:'COLLISION',asset_class:'Equity',effective_date:'2026-09-17',processed_date:'2026-09-18',shares_held_raw_decimal:'0',weight_raw_decimal:null,...extra});
const snapshot=(extra={})=>({rows:[row()],indexed_rows:1,processed_date:'2026-09-18',effective_dates:{'2026-09-17':1},source_acquired_at:'2026-09-21T06:00:00Z',source_valid_until:'2026-09-22T08:00:00Z',quality:{status:'complete_returned_snapshot',pagination_complete:true,missing_identity_rows:0,duplicate_identity_rows:0,rows_with_field_errors:0},...extra});
const measure=(a,b)=>A.heatMeasure(p,['SPY','VOO'],{SPY:a,VOO:b},'2026-09-17',at);
test('cohort denominator changes with partial, stale, future and incompatible source dates',()=>{
 const s=snapshot();assert.equal(measure(s,s).eligible,2);
 for(const bad of [snapshot({source_valid_until:'2020-01-01'}),snapshot({source_acquired_at:'2099-01-01'}),snapshot({effective_dates:{'2026-08-31':1}}),snapshot({quality:{...s.quality,status:'incomplete'}})]){
  const m=measure(s,bad);assert.equal(m.eligible,1);assert.equal(m.configured,300);assert.equal(m.rows[0].raw.length,2);assert.equal(m.rows[0].qualified.length,1);assert.ok(m.coverage[1].reason);
 }
 assert.equal(A.heatMeasure({...p,generated_at:'2099-01-01'},['SPY'],{SPY:s},'2026-09-17',at).eligible,0);
 assert.throws(()=>A.heatMeasure(p,['SPY','VOO'],{SPY:s},'2026-09-17',at),/Whole selected/);
});
test('identity ambiguity, duplicates and ticker collisions never inflate qualified counts',()=>{
 const s=snapshot();assert.throws(()=>measure(s,snapshot({rows:[row('id',{figi:'different'})]})),/collision/);
 assert.throws(()=>measure(s,snapshot({rows:[row(),row()],indexed_rows:2})),/Duplicate/);
 assert.throws(()=>measure(s,snapshot({rows:[row('a',{identity_key:null})]})),/unidentified/);
 const m=measure(s,snapshot({rows:[row('other')]}));assert.equal(m.rows.length,2);assert.equal(m.rows[0].qualified.length,1);
 const unknown=snapshot({rows:[row('x',{identity_key:null})],quality:{...s.quality,missing_identity_rows:1}});assert.equal(measure(s,unknown).unidentified,1);
});
test('zero/null and unknown units are not transformed to exposure or trades',()=>{
 const s=snapshot(),before=JSON.stringify(s),m=measure(s,s);assert.equal(JSON.stringify(s),before);
 const html=A.heatView(m);assert.match(html,/No aggregate dollars/);assert.match(html,/same-date revisions and corporate actions are not trades/);
 assert.equal(s.rows[0].shares_held_raw_decimal,'0');assert.equal(s.rows[0].weight_raw_decimal,null);
 assert.match(A.heatView(measure(snapshot({source_valid_until:'2020-01-01'}),snapshot({source_valid_until:'2020-01-01'}))),/Qualified ranking unavailable/);
});
test('bounded complete artifact loading caches repeat interactions and never reads shards/history',async()=>{
 let calls=0;const keys=[],f=async(k,o)=>{calls++;keys.push(k);return fetcher(k,o);};
 const session=A.heatSession(),first=await session.run(p,['ARKK','SPY'],'2026-09-17',f,()=>{},at),n=calls;
 assert.ok(n>0&&n<=40);assert.ok(first.bytes<=A.heatLimits.bytes);assert.ok(keys.every(k=>/\/(snapshots|rows)\//.test(k)));
 const again=await session.run(p,['ARKK','SPY'],'2026-09-17',f,()=>{},at);assert.equal(calls,n);assert.equal(again.requests,0);assert.deepEqual(first.rows,again.rows);
 console.log('Synthetic ARKK+SPY cold cost:',first.requests,'objects',first.bytes,'bytes');
});
test('cancellation rejects completion and retry succeeds',async()=>{
 const session=A.heatSession();let release;
 const task=session.run(p,['ARKK'],'2026-09-17',async(k,o)=>{await new Promise(r=>release=r);assert.equal(o.signal.aborted,true);return fetcher(k);},()=>{},at);
 session.cancel();release();await assert.rejects(task,/Cancelled/);
 await session.run(p,['ARKK'],'2026-09-17',fetcher,()=>{},at);
});
test('selection and declared-byte bounds fail closed without a fetch',async()=>{
 assert.deepEqual(A.heatSelection(p,'SPY, spy QQQ'),['QQQ','SPY']);assert.throws(()=>A.heatSelection(p,'unknown'),/configured/);
 const bad=structuredClone(p);bad.funds.SPY.current.snapshot.bytes=A.heatLimits.bytes+1;
 await assert.rejects(A.heatSession().run(bad,['SPY'],'2026-09-17',()=>{throw Error('must not fetch');}),/byte budget/);
});
test('same-date revisions and changed raw quantities/weights never create trading deltas',()=>{
 const s=snapshot(),revised=snapshot({rows:[row('id',{shares_held_raw_decimal:'999',weight_raw_decimal:'0.5'})]});
 const a=measure(s,s),b=measure(s,revised);assert.deepEqual(a.rows,b.rows);
 assert.equal('trades' in b,false);assert.equal('weight' in b.rows[0],false);assert.equal('quantityChange' in b.rows[0],false);
});
