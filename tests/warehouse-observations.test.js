const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),core=require('../jh-observation-series.js'),cacheCore=require('../jh-observation-cache.js'),view=require('../jh-chart-stock-desk.js');
const read=p=>fs.readFileSync(path.join(R,p),'utf8'),clone=v=>JSON.parse(JSON.stringify(v));
const doc=(id='FRED:invented',obs=[['2026-01-01',-2],['2026-01-02',0]])=>({id,provider:id.split(':')[0],provider_name:'Invented provider',freq:'D',unit:'invented',source:'invented:test',as_of:'2026-01-03T00:00:00Z',obs,extra:{whole:true}});
const flush=async()=>{for(let i=0;i<40;i++)await Promise.resolve();};
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const source=read('jh-chart-engine.js'),top=acorn.exports.parse(source,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const named=Object.fromEntries(top.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,source.slice(n.start,n.end)]));
function setup(){
 let utc=Date.parse('2026-01-03'),mono=1000,next=0;const requests=[],timers=new Map(),docs=new Map(),modes=new Map(),pending=new Map();
 class Clock extends Date{constructor(...a){super(...(a.length?a:[utc]));}static now(){return utc;}}
 const c={Date:Clock,performance:{now:()=>mono},AbortController,setTimeout:(f,ms)=>{timers.set(++next,{f,ms});return next;},clearTimeout:id=>timers.delete(id)};c.window=c;
 c.fetch=async(url,options)=>{
  const u=new URL(url);assert.equal(u.origin,'https://justhodl-data-proxy.raafouis.workers.dev');assert.equal(u.pathname,'/series');
  const id=u.searchParams.get('id');requests.push({id,url,options});const mode=modes.get(id);
  if(mode==='pending')return new Promise(resolve=>pending.set(id,resolve));
  if(mode==='http')return {ok:false,status:503,json(){throw Error('Do not read HTTP error bodies');}};
  return {ok:true,status:200,json:async()=>structuredClone(docs.get(id)||doc(id))};
 };
 for(const p of ['jh-observation-series.js','jh-observation-cache.js','jh-chart-catalog.js'])vm.runInNewContext(read(p),c,{filename:p});
 return {c,requests,timers,docs,modes,pending,advance:ms=>{utc+=ms;mono+=ms;},tick:async()=>{for(const [id,t]of [...timers]){timers.delete(id);t.f();}await flush();}};
}
function engine(h){
 Object.assign(h.c,{barEvidence:new WeakMap(),barCache:{},spec:id=>[id],lastSource:'original',toast(){throw Error('No scalar market fallback');},warehouseSpec(){throw Error('No scalar market endpoint');},fetchJson(){throw Error('No scalar network fallback');}});
 vm.runInNewContext(['bare','yahooSym','resolveSym','observationId','identifyBars','resampleToTf','klines'].map(n=>named[n]).join('\n'),h.c);return h.c;
}

test('received warehouse rows preserve signed and zero scalars and reject coercion and impossible calendars',()=>{
 const p=doc('FRED:invented',[['2026-01-01',null],['2026-01-02',false],['2026-01-03',true],['2026-01-04',''],['2026-01-05',[]],['2026-01-06','-2.3e-8'],['2026-01-07',0],['2026-02-30',8]]),before=JSON.stringify(p),r=core.warehouse(p,p.id);
 assert.deepEqual(r.d.map(b=>b.close),[-2.3e-8,0]);assert.equal(r.evidence.records.length,8);assert.equal(r.evidence.rejected_records,6);assert.equal(r.evidence.records[7].reason,'invalid_period_or_frequency');
 assert.ok(r.d.every(b=>b.volume===null));assert.deepEqual(r.evidence.records.map(x=>x.source_path),p.obs.map((_,i)=>'$.obs['+i+']'));assert.equal(r.evidence.whole_packet,p);assert.equal(JSON.stringify(p),before);
});
test('normalized warehouse days do not become guessed month ends or publication times',()=>{
 for(const freq of ['M','Q','A','unknown']){const p=doc('ECB:invented',[['2024-02-01',0],['2024-03-01',-1],['2024-04',8]]);p.freq=freq;const r=core.warehouse(p,p.id);
  assert.deepEqual(r.d.map(b=>new Date(b.time*1000).toISOString().slice(0,10)),['2024-02-01','2024-03-01']);assert.equal(r.evidence.packet_reported_as_of,p.as_of);assert.equal(r.evidence.source_acquired_at,null);assert.equal(r.evidence.source_published_at,null);assert.equal(r.evidence.upstream_originals_verified,false);
 }
});
test('warehouse identity is the full native ID and never a suffix, provider neighbor or blank',()=>{
 for(const id of ['invented','NYFED:invented','FRED:invented_extra','',null]){const p=doc(id===null?'FRED:invented':id);p.id=id;const r=core.warehouse(p,'FRED:invented');assert.equal(r.evidence.reason,'packet_identity_mismatch');assert.equal(r.evidence.whole_packet,p);assert.equal(r.d.length,0);}
 const r=core.warehouse(doc('fred:INVENTED'),'FRED:invented');assert.equal(r.d.length,2);assert.equal(r.evidence.selected_id,'fred:INVENTED');assert.equal(r.evidence.identity_resolution.requested,'FRED:invented');
});
test('all received duplicate rows remain inspectable while conflicting periods are withheld',()=>{
 const p=doc('NYFED:invented',[['2026-01-02',2],['2026-01-01',0],['2026-01-02',3],['2026-01-01','0'],['2026-01-03'],null]);const r=core.warehouse(p,p.id);
 assert.deepEqual(r.d.map(b=>b.close),[0]);assert.deepEqual(r.d[0].observation_ordinals,[1,3]);assert.equal(r.evidence.records.length,6);assert.equal(r.evidence.records[0].reason,'conflicting_duplicate_period');assert.equal(r.evidence.records[4].reason,'unpaired_or_malformed_row');
});
test('absent, malformed and empty warehouse histories retain distinct complete evidence',()=>{
 assert.equal(core.warehouse(null,'FRED:x').evidence.reason,'warehouse_packet_unavailable');for(const obs of [null,{},'bad']){const p=doc('FRED:x',obs),r=core.warehouse(p,p.id);assert.equal(r.evidence.reason,'malformed_points');assert.equal(r.evidence.whole_packet,p);assert.equal(r.evidence.plotted_points,undefined);}
 const r=core.warehouse(doc('FRED:x',[]),'FRED:x');assert.equal(r.evidence.reason,'no_valid_observations');assert.equal(r.evidence.plotted_points,0);
});
test('warehouse labels and alias text are escaped and no evidence grants freshness or trade authority',()=>{
 const p=doc();p.provider='<img src=x>';p.source='<script>bad()</script>';p.as_of='<iframe>';const r=core.warehouse(p,p.id);r.evidence.chart_alias={requested:'<img>',resolved:'<svg>',rule:'<script>'};
 const html=view.markup({kind:'scalar_observations',symbol:p.id,bars:r.d,observations:r.evidence,valid:true});assert.doesNotMatch(html,/<img|<script|<iframe|<svg/);assert.match(html,/&lt;img/);assert.match(html,/packet construction metadata; not first publication/);
 for(const field of ['calls_eligible','sizing_eligible','source_freshness_verified','source_equivalence_verified'])assert.equal(r.evidence[field],false);
});
test('custom cache definitions are validated, isolated and copied without changing the seven defaults',async()=>{
 for(const sources of [null,[],{}, {x:{}},{x:{paths:[],valid:()=>true}},{x:{paths:Array(1),valid:()=>true}},{x:{paths:['',null],valid:()=>true}},{x:{paths:['/invented'],valid:false}}])assert.throws(()=>cacheCore.create({sources}),/Invalid observation source/);
 const paths=['/invented'],sources=Object.create(null);sources.constructor={paths,valid:v=>v?.ok===true};const requests=[];
 const cache=cacheCore.create({sources,fetch:async p=>{requests.push(p);return {ok:true,json:async()=>({ok:true})};}});paths[0]='/mutated';delete sources.constructor;
 assert.equal((await cache.read('constructor')).packet.ok,true);assert.deepEqual(requests,['/invented']);assert.throws(()=>cache.read('series'),/Unknown/);assert.throws(()=>cacheCore.create().read('constructor'),/Unknown/);
 for(const id of ['series','onchain','feed','catalog','spec','universe','ciss'])assert.equal(cacheCore.create().status(id).source_id,id);
});
test('whole catalog rejects inherited provider names and caches exact identified warehouse packets',async()=>{
 const h=setup();for(const id of ['constructor:any','__proto__:any','toString:any','NYSE:DGS10'])assert.equal(h.c.JHChartCatalog.isWarehouse(id),false,id);
 for(const id of ['FRED:invented','NYFED:invented','ecb:INVENTED']){const [a,b]=await Promise.all([h.c.JHChartCatalog.klines(id),h.c.JHChartCatalog.klines(id)]);assert.deepEqual(clone(a.d.map(x=>x.close)),[-2,0]);assert.deepEqual(clone(a),clone(b));}
 assert.equal(h.requests.length,3);assert.equal(h.timers.size,0);
});
test('failed replacements cannot replace another identity or erase a valid cached history',async()=>{
 const h=setup(),id='FRED:invented',initial=await h.c.JHChartCatalog.klines(id);h.advance(300000);const wrong=doc('NYFED:invented');h.docs.set(id,wrong);
 const failed=await h.c.JHChartCatalog.klines(id);assert.deepEqual(clone(failed.d),clone(initial.d));assert.equal(failed.evidence.transport_cache.state,'cached_after_failure');assert.deepEqual(clone(failed.evidence.transport_cache.attempts[0].rejected_packet),wrong);
 h.advance(30000);h.docs.set(id,doc(id,[['2026-01-03',5]]));const recovered=await h.c.JHChartCatalog.klines(id);assert.equal(recovered.d[0].close,5);assert.equal(recovered.evidence.transport_cache.state,'checked_within_interval');
});
test('initial schema failures retain the complete rejected packet and never invent a selected series',async()=>{
 const h=setup(),id='FRED:invented',wrong=doc('FRED:other');h.docs.set(id,wrong);const r=await h.c.JHChartCatalog.klines(id);
 assert.equal(r.d.length,0);assert.equal(r.evidence.selected_id,undefined);assert.equal(r.evidence.reason,'warehouse_packet_unavailable');assert.deepEqual(clone(r.evidence.transport_cache.attempts[0].rejected_packet),wrong);
});
test('pending warehouse downloads time out, abort, ignore late bodies and recover on the bounded retry',async()=>{
 const h=setup(),id='FRED:invented';h.modes.set(id,'pending');const p=h.c.JHChartCatalog.klines(id);await flush();h.advance(10000);await h.tick();const r=await p;
 assert.equal(r.evidence.transport_cache.last_error.kind,'timeout');assert.equal(h.requests[0].options.signal.aborted,true);h.pending.get(id)({ok:true,json:async()=>doc(id,[['2026-01-01',999]])});await flush();
 h.advance(30000);h.modes.delete(id);const recovered=await h.c.JHChartCatalog.klines(id);assert.equal(recovered.d[0].close,-2);assert.equal(h.requests.length,2);
});
test('eight-entry warehouse capacity evicts the least recently used packet and aborts its pending generation',async()=>{
 const h=setup();for(let i=0;i<8;i++)await h.c.JHChartCatalog.klines('FRED:i'+i);await h.c.JHChartCatalog.klines('FRED:i0');await h.c.JHChartCatalog.klines('FRED:i8');assert.equal(h.requests.length,9);
 await h.c.JHChartCatalog.klines('FRED:i0');assert.equal(h.requests.length,9);await h.c.JHChartCatalog.klines('FRED:i1');assert.equal(h.requests.length,10);
 const g=setup();g.modes.set('FRED:old','pending');const pending=g.c.JHChartCatalog.klines('FRED:old');await flush();for(let i=0;i<8;i++)await g.c.JHChartCatalog.klines('FRED:i'+i);const old=await pending;
 assert.equal(old.evidence.transport_cache.state,'superseded');assert.equal(g.requests[0].options.signal.aborted,true);g.pending.get('FRED:old')({ok:true,json:async()=>doc('FRED:old',[['2026-01-01',999]])});await flush();g.modes.delete('FRED:old');assert.equal((await g.c.JHChartCatalog.klines('FRED:old')).d[0].close,-2);
});
test('equity resolution terminates and named exchanges cannot inherit unqualified macro aliases',()=>{
 const c=engine(setup());for(const id of ['AAPL','NYSE:DGS10','NASDAQ:US10Y','NYSE:GOLD']){assert.equal(c.observationId(id),false);assert.equal(c.resolveSym(id).engine,'equity');}
 assert.equal(c.resolveSym('NYSE:DGS10').ticker,'DGS10');for(const id of ['DGS10','US10Y','TVC:US10Y','TVC:US02Y','FRED:DGS10','NYFED:invented','ECB:invented'])assert.equal(c.observationId(id),true,id);
});
test('general scalar charts retain exact evidence and never write into the unused market bar cache',async()=>{
 const h=setup(),c=engine(h);for(const id of ['FRED:invented','NYFED:invented','ECB:invented'])for(const tf of ['1d','1w','2w','1M']){const bars=await c.klines(id,tf);assert.ok(bars.length);const e=c.barEvidence.get(bars);assert.equal(e.symbol,id);assert.equal(e.observations.requested_id,id);assert.equal(e.observations.calls_eligible,false);}
 assert.deepEqual(Object.keys(c.barCache),[]);assert.equal(h.requests.length,3);
});
test('explicit legacy aliases retain both requested and canonical IDs with full source evidence',async()=>{
 const h=setup(),c=engine(h),bars=await c.klines('TVC:US10Y','1d'),e=c.barEvidence.get(bars).observations;
 assert.equal(h.requests[0].id,'FRED:DGS10');assert.equal(e.requested_id,'TVC:US10Y');assert.equal(e.selected_id,'FRED:DGS10');assert.equal(e.chart_alias.resolved,'FRED:DGS10');assert.equal(e.identity_resolution.requested,'FRED:DGS10');
});
test('missing catalog/parser cannot turn a warehouse measurement into an exchange ticker',async()=>{
 for(const missing of ['catalog','parser']){const h=setup(),c=engine(h);if(missing==='catalog')c.JHChartCatalog=undefined;else c.JHObservationSeries=undefined;
  for(const id of ['FRED:invented','NYFED:invented','ECB:invented'])assert.equal((await c.klines(id,'1d')).length,0,missing+':'+id);assert.equal(h.requests.length,0);
 }
});
