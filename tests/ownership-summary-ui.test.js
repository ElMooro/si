const test=require('node:test'),assert=require('node:assert/strict');
const A=require('../jh-etf-holdings.js'),fixture=require('./ownership-summary-fixture.cjs');
const current=m=>m.cohorts.find(g=>g.kind==='current_membership'&&g.eligible_fund_count),time=p=>Date.parse(p.generated_at);
test('old, unavailable and unknown-policy publications leave the existing panels intact',async()=>{
 const f=fixture();delete f.p.ownership_summary;await assert.rejects(A.ownershipManifest(f.p,f.fetcher),/not available/);
 assert.match(A.render(f.p),/data-hd-heat/);assert.match(A.render(f.p),/data-hd-fund/);
 for(const status of ['unavailable','partial','complete']){f.p.ownership_summary={status,policy:'wrong',reason:'summary_record_bound'};await assert.rejects(A.ownershipManifest(f.p,f.fetcher),/unavailable/);}
});
test('new Python-produced summary binds root/run/output and lazily loads exact cohort pages',async()=>{
 const f=fixture(),s=A.ownershipSession(),keys=[],fetch=async(k,o)=>{keys.push(k);return f.fetcher(k,o);};
 const m=await s.open(f.p,fetch);assert.equal(keys.length,3);assert.equal(m.configured_fund_count,300);
 await assert.rejects(s.next('qualified',fetch,time(f.p)),/Choose/);
 const g=current(m);s.select(g.cohort_id);const r=await s.next('qualified',fetch,time(f.p));assert.equal(r.rows.length,200);assert.equal(keys.length,4);
 assert.match(A.ownershipView(m,g,r,'qualified',time(f.p)),/1 \/ 1/);assert.match(A.ownershipCoverage(m,g,time(f.p)),/1 eligible \/ 300 configured/);
 const next=await s.next('qualified',fetch,time(f.p));assert.equal(next.loaded,307);await assert.rejects(s.next('qualified',fetch,time(f.p)),/No more/);
 assert.equal(keys.some(k=>/\/(memberships|rows|snapshots)\//.test(k)),false);
 s.select(g.cohort_id);const before=keys.length;await s.next('raw',fetch,time(f.p));assert.equal(keys.length,before);
 console.log('Synthetic summary metadata',f.p.ownership_summary.manifest.bytes,'bytes; first page',g.parts[0].bytes,'bytes; 3 proof/metadata requests + 1 lazy page');
});
test('economic dates are explicit and old dates warn without choosing a cohort',()=>{
 const f=fixture(),m=f.manifest(),g=current(m);g.effective_dates=['2019-01-01'];
 assert.match(A.ownershipCoverage(m,g,time(f.p)),/Historical economic date/);
 assert.match(A.ownershipPanel(),/Choose a date explicitly/);assert.doesNotMatch(A.ownershipPanel(),/selected/);
 const partial=m.cohorts.find(g=>!g.eligible_fund_count);assert.match(A.ownershipState(m,partial,'qualified',time(f.p)),/unavailable/);
 assert.equal(A.ownershipState(m,partial,'lower',time(f.p)),null);
});
test('lower-bound expiry and ranking expiry are independent and exclusive including null',()=>{
 const f=fixture(),m=f.manifest(),g=current(m),at=time(f.p);
 g.source_valid_until=new Date(at+7200000).toISOString();g.lower_bound_valid_until=new Date(at+3600000).toISOString();
 assert.equal(A.ownershipState(m,g,'lower',at+3599999),null);
 assert.match(A.ownershipState(m,g,'lower',at+3600000),/expired/);assert.equal(A.ownershipState(m,g,'qualified',at+3600000),null);
 assert.match(A.ownershipState(m,g,'qualified',at+7200000),/expired/);assert.equal(A.ownershipState(m,g,'raw',at+7200000),null);
 g.lower_bound_valid_until=null;assert.match(A.ownershipState(m,g,'lower',at),/missing/);
 assert.match(A.ownershipState({...m,generated_at:'2099-01-01'},g,'raw',at),/future/);
});
test('manifest corruption of hashes, sizes, totals, cohorts and denominators is rejected',async()=>{
 for(const change of [m=>m.record_count++,m=>m.page_count++,m=>current(m).record_count++,m=>current(m).eligible_fund_count++,m=>current(m).cohort_id='a'.repeat(64),m=>current(m).parts[0].bytes=262145,m=>current(m).parts.push(current(m).parts[0]),m=>m.funds.SPY.current_snapshot.bytes++]){
  const f=fixture(),m=f.manifest();change(m);f.resign(m);await assert.rejects(A.ownershipSession().open(f.p,f.fetcher));
 }
 const f=fixture();f.artifacts[f.p.ownership_summary.manifest.key]+=' ';await assert.rejects(A.ownershipSession().open(f.p,f.fetcher),/byte/);
 const p=fixture();p.p.ownership_summary.manifest.bytes=524289;let calls=0;await assert.rejects(A.ownershipManifest(p.p,async()=>{calls++;}),/size/);assert.equal(calls,0);
});
test('correctly hashed corrupt pages cannot present count, order, duplicate or truncated records',async()=>{
 for(const change of [d=>d.row_offset++,d=>d.rows.pop(),d=>d.rows.reverse(),d=>d.rows[1]=d.rows[0],d=>d.rows[0].qualified_fund_count=301,d=>d.rows[0].raw_observed_fund_count=null,d=>d.rows[0].known_presence_lower_bound=null]){
  const f=fixture(),m=f.manifest(),g=current(m),d=JSON.parse(f.artifacts[g.parts[0].key]);change(d);g.parts[0]=f.put(d);f.resign(m);
  const s=A.ownershipSession();await s.open(f.p,f.fetcher);s.select(g.cohort_id);await assert.rejects(s.next('qualified',f.fetcher,time(f.p)));
 }
});
test('cross-page ordering and duplicates fail even with individually valid hashes',()=>{
 const f=fixture(),m=f.manifest(),g=current(m),d=JSON.parse(f.artifacts[g.parts[0].key]),state=A.ownershipRows(d,g,0,new Set(),null),second=JSON.parse(f.artifacts[g.parts[1].key]);
 second.rows[0]=d.rows[0];assert.throws(()=>A.ownershipRows(second,g,1,state.seen,state.previous),/contract|ordering/);
});
test('zero remains zero, null class unknown, no inferred exposure and raw/lower views unranked',()=>{
 const f=fixture(),m=f.manifest(),g=current(m),r={rows:[{identity_key:'a'.repeat(64),reported_tickers:['<script>'],source_asset_class:null,qualified_fund_count:0,raw_observed_fund_count:0,known_presence_lower_bound:0}],page:1,loaded:1,total:1};
 for(const view of ['raw','lower']){const html=A.ownershipView(m,g,r,view,time(f.p));assert.match(html,/not ranked/);assert.match(html,/Unknown/);assert.match(html,/>0</);assert.doesNotMatch(html,/<script>/);}
 assert.match(A.ownershipView(m,g,r,'qualified',time(f.p)),/0 \/ 1/);
});
test('access errors, cancellation and repeated interaction do not advance the page or poison cache',async()=>{
 const f=fixture(),s=A.ownershipSession(),m=await s.open(f.p,f.fetcher),g=current(m);s.select(g.cohort_id);
 await assert.rejects(s.next('raw',async()=>({ok:false}),time(f.p)),/request failed/);
 let release;const pending=s.next('raw',async(k,o)=>{await new Promise(r=>release=r);assert.equal(o.signal.aborted,true);return f.fetcher(k);},time(f.p));
 s.cancel();release();await assert.rejects(pending,/Cancelled/);
 assert.equal((await s.next('raw',f.fetcher,time(f.p))).page,1);
 s.select(g.cohort_id);assert.equal((await s.next('raw',async()=>{throw Error('cached');},time(f.p))).page,1);
});
test('expiry is reevaluated after the page request, including cached pages',async()=>{
 const f=fixture(),s=A.ownershipSession(),m=await s.open(f.p,f.fetcher),g=current(m),real=Date.now;let clock=time(f.p);Date.now=()=>clock;
 try{s.select(g.cohort_id);await assert.rejects(s.next('lower',async(k,o)=>{const r=await f.fetcher(k,o);clock=Date.parse(g.lower_bound_valid_until);return r;}),/expired/);
  s.select(g.cohort_id);await assert.rejects(s.next('lower',f.fetcher),/expired/);assert.equal((await s.next('raw',f.fetcher)).page,1);
 }finally{Date.now=real;}
});
test('32-page request ceiling spans cohorts; cache hits do not spend or reset it',async()=>{
 const f=fixture(),m=f.manifest(),g=current(m);g.record_count=6600;g.parts=[];const other=m.cohorts.find(c=>c!==g);m.cohorts=[g,other];m.record_count=6600+other.record_count;m.page_count=33+other.parts.length;
 for(let page=0;page<33;page++)g.parts.push(f.put({contract:'etf-qualified-membership-rows.v1',cohort_id:g.cohort_id,row_offset:page*200,rows:Array.from({length:200},(_,i)=>({identity_key:(page*200+i).toString(16).padStart(64,'0'),reported_tickers:[],source_asset_class:null,source_security_type:null,raw_observed_fund_count:1,known_presence_lower_bound:1,qualified_fund_count:1,observed_in_both_count:null,observed_only_in_current_count:null,observed_only_in_prior_count:null}))}));
 m.limits={pages:999999,total_bytes:999999999};f.resign(m);const s=A.ownershipSession();await s.open(f.p,f.fetcher);s.select(g.cohort_id);
 for(let i=0;i<32;i++)assert.equal((await s.next('qualified',f.fetcher,time(f.p))).page,i+1);
 let calls=0;await assert.rejects(s.next('qualified',async()=>{calls++;},time(f.p)),/32-page/);assert.equal(calls,0);
 // Changing the cohort resets the cursor, never the network budget.
 const counted=async(k,o)=>{calls++;return f.fetcher(k,o);};
 for(let i=0;i<3;i++){
  s.select(other.cohort_id);await assert.rejects(s.next('raw',counted,time(f.p)),/budget/);assert.equal(calls,0);
  s.select(g.cohort_id);const cached=await s.next('qualified',counted,time(f.p));assert.equal(cached.requests,32);assert.equal(calls,0);
 }
 // Only explicit metadata retry starts a new budget; immutable cache survives.
 await s.open(f.p,f.fetcher);s.select(g.cohort_id);
 assert.equal((await s.next('qualified',counted,time(f.p))).requests,0);assert.equal(calls,0);
 s.select(other.cohort_id);const fresh=await s.next('raw',counted,time(f.p));assert.equal(fresh.requests,1);assert.equal(calls,1);
 s.select(other.cohort_id);assert.equal((await s.next('raw',counted,time(f.p))).requests,1);assert.equal(calls,1);
});
test('reviewed PR14 UI-only downgrade verifies new publications and keeps selected-fund inspectors',async()=>{
 const fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
 const source=fs.readFileSync(__dirname+'/fixtures/pr14-holdings-ui-rollback.js.txt','utf8');
 assert.equal(crypto.createHash('sha256').update(source).digest('hex'),'a562a1d9fa6352b8c8b918a677d76f924fb6eb7511a302141e7ac3c18254c69b');
 const ctx={crypto:globalThis.crypto,TextEncoder,TextDecoder,AbortController,DOMException,module:{exports:{}}};
 vm.runInNewContext(source,ctx);const old=ctx.module.exports,f=fixture();
 for(const p of Object.values(f.packets)){
  await old.verifyPacket(p,f.fetcher);const snap=await old.snapshot(p,'SPY','current',f.fetcher);
  assert.equal(snap.indexed_rows,306);const html=old.render(p);
  assert.match(html,/data-hd-heat-load/);assert.doesNotMatch(html,/data-hd-ownership/);
 }
});
