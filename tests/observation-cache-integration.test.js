const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const R=path.join(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
const clone=v=>JSON.parse(JSON.stringify(v));
const initial=()=>({'/data/cryptoquant-series.json':{series:{x:{d:['2026-01-01'],v:[-5],unit:'invented'}},twins:{x:{d:['2010-01-01'],v:[999]}}},
 '/data/cryptoquant-onchain.json':{metrics:{},composite_onchain_risk_z:null},'/data/cq-feed.json':{metrics:{}},'/data/cq-catalog.json':{catalog:{}},'/data/config/cryptoquant-spec.json':{metrics:[]},'/cq-universe.json':{rows:[]},
 '/data/ciss-stress.json':{series:[{key:'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX',points:[['2026-01-01',-2]]}]}});
function setup(){let utc=Date.parse('2026-01-01'),mono=1000,n=0;const docs=initial(),timers=new Map(),requests=[],fail=new Set();
 class Clock extends Date{constructor(...a){super(...(a.length?a:[utc]));}static now(){return utc;}}
 const c={Date:Clock,performance:{now:()=>mono},AbortController,setTimeout:(f,ms)=>{timers.set(++n,{f,ms});return n;},clearTimeout:i=>timers.delete(i)};c.window=c;
 c.fetch=async(url,opts)=>{requests.push({url,opts});assert.ok(Object.hasOwn(docs,url)||url==='/assets/cq-universe.json',url);return {ok:!fail.has(url),status:fail.has(url)?503:200,json:async()=>structuredClone(docs[url]??docs['/cq-universe.json'])};};
 for(const p of ['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js','jh-chart-catalog.js'])vm.runInNewContext(read(p),c,{filename:p});
 return {c,docs,timers,requests,fail,advance:ms=>{utc+=ms;mono+=ms;}};
}
test('history queries request only their own source and share the bounded cache across complete modules',async()=>{
 const h=setup(),one=await h.c.JHChartCatalog.klines('CQ:x');assert.equal(one.d[0].close,-5);assert.equal(h.requests.length,1);assert.equal(one.evidence.transport_cache.source_freshness_verified,false);
 await h.c.JHCqFuse.klines('CQ:x');assert.equal(h.requests.length,1);await h.c.JHChartCatalog.klines('CISS:ea');assert.equal(h.requests.length,2);
 h.docs['/data/cryptoquant-series.json'].series.x.v[0]=8;h.advance(300000);assert.equal((await h.c.JHChartCatalog.klines('CQ:x')).d[0].close,8);assert.equal(h.requests.length,3);
});
test('fuse recovers only missing sources, distinguishes unknown counts, and retains every decoded original',async()=>{
 const h=setup();h.fail.add('/data/cq-feed.json');h.fail.add('/data/cryptoquant-series.json');let p=await h.c.JHCqFuse.load();
 assert.equal(p.n_series,null);assert.equal(p.n_snaps,null);assert.equal(p.n_armed,0);assert.equal(p.n_v1,null);assert.equal(p.coverage_complete,false);assert.equal(p.source_cache.series.state,'unavailable');
 assert.match(h.c.JHCqFuse.paneHTML({}),/SOURCE DOWNLOAD STATUS/);assert.doesNotMatch(h.c.JHCqFuse.paneHTML({}),/twins to 2010/);
 h.advance(30000);h.fail.clear();const count=h.requests.length;p=await h.c.JHCqFuse.load();assert.equal(h.requests.length-count,2);assert.equal(p.n_series,1);assert.equal(p.n_chartable,1);assert.equal(p.source_cache.series.state,'checked_within_interval');
 assert.equal(p.calls_eligible,false);assert.equal(p.sizing_eligible,false);assert.deepEqual(clone(p.source_documents.series),h.docs['/data/cryptoquant-series.json']);
 h.advance(300000);h.docs['/data/cryptoquant-series.json']={bad:['whole rejected packet']};const result=await h.c.JHChartCatalog.klines('CQ:x');assert.equal(result.d[0].close,-5);assert.equal(result.evidence.transport_cache.state,'cached_after_failure');
 assert.deepEqual(clone(result.evidence.transport_cache.attempts[0].rejected_packet),{bad:['whole rejected packet']});assert.match(result.src,/cached_after_failure/);
});
test('one valid point is chartable; all-invalid and ambiguous casefold IDs are not',async()=>{
 const h=setup();h.docs['/data/cryptoquant-series.json'].series.bad={d:['2026-01-01'],v:[null]};
 let p=await h.c.JHCqFuse.load();assert.equal(h.c.JHCqFuse.isChartable('CQ:x'),true);assert.equal(h.c.JHCqFuse.isChartable('CQ:bad'),false);
 assert.equal(h.c.JHCqFuse.searchHits('bad')[0].chartable,false);assert.equal(h.c.JHCqFuse.searchHits('CQ:x')[0].chartable,true);
 h.docs['/data/cryptoquant-series.json'].series.X=structuredClone(h.docs['/data/cryptoquant-series.json'].series.x);h.c.JHCqFuse.reset();p=await h.c.JHCqFuse.load();assert.equal(h.c.JHCqFuse.isChartable('CQ:x'),false);assert.equal(h.c.JHCqFuse.searchHits('CQ:x')[0].chartable,false);
});
test('snapshot and forecast formatting cannot convert null, booleans or containers into measurements',async()=>{
 const h=setup();for(const value of [null,false,true,[],[1],{},'',' ','1e-999'])assert.equal(h.c.JHCqFuse.fmt(value),'—');assert.equal(h.c.JHCqFuse.fmt(0),'0');
 h.docs['/data/cryptoquant-onchain.json']={metrics:{x:{value:false,z365:null,pctl_1y:[]}},composite_onchain_risk_z:false,forecasts:{method:'invented',btc:{h30:{exp_pct:null},h90:{exp_pct:false},h180:{exp_pct:[]},h365:{exp_pct:0}}}};
 h.docs['/data/cq-feed.json'].metrics.path={path:'invented/path',fields:{a:false,b:'0'},prev:{a:0,b:'-1'}};
 const p=await h.c.JHCqFuse.load();assert.equal(p.snaps.find(x=>x.field==='a').dlt,null);assert.equal(p.snaps.find(x=>x.field==='b').dlt,1);
 const html=h.c.JHCqFuse.paneHTML({});assert.match(html,/z —/);assert.doesNotMatch(html,/null%|false%|>0\.00<|0th pctl|LEDGER-GRADED|twins to 2010/);assert.match(html,/0%/);assert.match(html,/QUALIFICATION UNVERIFIED/);
});
test('prototype-like source categories remain ordinary escaped rows',async()=>{
 const h=setup();h.docs['/data/cryptoquant-onchain.json'].metrics.x={category:'__proto__',label:'<img src=x>',value:0};await h.c.JHCqFuse.load();const html=h.c.JHCqFuse.paneHTML({});assert.match(html,/&lt;img src=x&gt;/);assert.doesNotMatch(html,/<img/);
});
test('malformed descriptive metadata cannot crash the catalog or manufacture a unit',async()=>{
 const h=setup();h.docs['/data/cryptoquant-onchain.json'].metrics.x={label:{unexpected:true},category:[]};h.docs['/data/cryptoquant-series.json'].series.x.unit={bad:'unit'};
 const pack=await h.c.JHCqFuse.load(),meta=h.c.JHCqFuse.seriesMeta();assert.equal(meta[0].name,'X');assert.equal(meta[0].unit,'');assert.equal(h.c.JHCqFuse.searchHits('CQ:x')[0].chartable,true);
 assert.deepEqual(clone(pack.source_documents.series.series.x.unit),{bad:'unit'});assert.match(h.c.JHCqFuse.paneHTML({}),/unit unverified/);
});
test('a complete fuse reset cannot let an old aggregate overwrite the new packet',async()=>{
 const h=setup(),original=h.c.fetch;let release;const oldResponses=new Promise(r=>{release=r;});
 h.c.fetch=url=>oldResponses.then(()=>({ok:true,json:async()=>structuredClone(initial()[url])}));
 const old=h.c.JHCqFuse.load().then(()=>{throw Error('Cancelled load must not publish');},error=>error.message);
 for(let i=0;i<8;i++)await Promise.resolve();h.c.JHCqFuse.reset();h.c.fetch=original;h.docs['/data/cryptoquant-series.json'].series.x.v[0]=77;
 const newer=await h.c.JHCqFuse.load();assert.equal(newer.series.x.v[0],77);assert.equal(await old,'Observation load superseded');
 release();for(let i=0;i<30;i++)await Promise.resolve();assert.equal(h.c.JHCqFuse.pack(),newer);assert.equal(h.timers.size,0);
});
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const tv=read('jh-chart-tvsearch.js'),nodes=acorn.exports.parse(tv,{ecmaVersion:'latest'}).body[0].expression.callee.body.body;
const functions=Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,tv.slice(n.start,n.end)]));
test('the companion on-chain desk shares packet recovery and never advertises a proxy-extended primary span',async()=>{
 const h=setup();Object.assign(h.c,{CQ_PROXIES:{BTC:1},CQ_ONCHAIN:null,CQ_SERIES:null,jhFundTicker:s=>s,esc:h.c.JHCqFuse.esc,kpi:(a,b)=>a+':'+b,blk:(a,b)=>a+' '+b});
 vm.runInNewContext(['cqPack','cqMetric','cqFmt','renderChain'].map(n=>functions[n]).join('\n'),h.c);
 const p=await h.c.cqPack('BTC');assert.equal(h.requests.filter(x=>x.url==='/data/cryptoquant-series.json').length,1);const html=h.c.renderChain({chain:p});assert.doesNotMatch(html,/2010-01-01|2010|LIVE CQ/);assert.match(html,/2026-01-01 → 2026-01-01/);assert.match(html,/source freshness/);
 for(const value of [false,[],null,' '])assert.equal(h.c.cqFmt(value),'—');
 h.advance(300000);h.docs['/data/cryptoquant-series.json'].series.x.v[0]=6;assert.equal((await h.c.cqPack('BTC')).series.series.x.v[0],6);
});
