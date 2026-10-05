const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto'),zlib=require('node:zlib');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8'),cat=JSON.parse(read('config/regional-fed-series.json')),review=JSON.parse(read('docs/audit/2026-10-04/chart-regional-macro-watchlist-review.json')),plain=v=>JSON.parse(JSON.stringify(v)),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function context(fetch){const c={window:{},Set,Map,URLSearchParams,URL,TextDecoder,Uint8Array,AbortController,Blob,setTimeout,clearTimeout,fetch};c.window=c;for(const p of ['jh-watchlist-quotes.js','jh-observation-series.js','jh-observation-cache.js','jh-chart-catalog.js','jh-chart-provider-browser.js'])vm.runInNewContext(read(p),c);return c;}
test('all 658 regional Fed definitions remain exact across reference comparisons and adjustments',()=>{
 const c=context();assert.equal(Object.keys(cat.series).length,658);assert.equal(Object.keys(cat.datasets).length,15);
 for(const d of Object.values(cat.series)){
  assert.equal(c.JHChartCatalog.chartId(d.id),d.id);assert.equal(c.JHChartCatalog.chartId(d.id.toUpperCase()),d.id);assert.equal(c.JHChartCatalog.isWarehouse(d.id),true);assert.equal(c.JHChartProviderBrowser.action({id:d.id,provider:'regionalfed',kind:'series',chartable:true}),'chart');assert.ok(c.JHWatchlistQuotes.resolve(d.id,{}).reason);
 }
 for(const id of ['regionalfed:kc-manufacturing:composite','regionalfed:chicago-cfnai:CFNAI:extra','regionalfed:chicago-cfnai:UNKNOWN','regionalfed:kc-manufacturing:month-sa:production?x=1']){assert.equal(c.JHChartCatalog.chartId(id),'');assert.equal(c.JHChartProviderBrowser.action({id,provider:'regionalfed',kind:'series',chartable:true}),'inspect');}
});
test('two qualified macro alternatives preserve requested symbols without an ambiguous manufacturing guess',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 for(const [requested,d] of Object.entries(review.alternatives)){
  const c=scope.make(),r=c.chartRoute(requested);assert.equal(r.requested,requested);assert.equal(r.handoff,d.alternative);assert.match(r.reason,/equivalence unverified/);if(/dallas-wei/.test(d.alternative))assert.match(r.reason,/not realized GDP/);else assert.match(r.reason,/CC BY-SA 4.0/);assert.equal(c.observationId(r.handoff),true);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);c.openSym(requested);assert.deepEqual(Array.from(c.loads),[d.alternative.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);
 }
 const c=scope.make();assert.equal(c.providerRest('ECONOMICS:USKFMI'),'');c.openSym('regionalfed:kc-manufacturing:composite');assert.deepEqual(Array.from(c.loads),[]);
});
test('discovery adds one adapter with fifteen datasets without inventing stored-file access or coverage',()=>{
 const c=context(),p={providers:[{slug:'fred',custom:17}]},rows=plain(c.JHChartProviderBrowser.providerEntries(p)),d=rows.find(r=>r.slug==='regionalfed');assert.deepEqual(rows[0],p.providers[0]);assert.equal(d.discovery_origin,'chart_adapter');assert.equal(d.catalogue_slug,null);assert.equal(c.JHChartProviderBrowser.storedProvider('regionalfed'),null);assert.throws(()=>c.JHChartProviderBrowser.route({provider:'regionalfed',view:'files'}),/No published stored-file/);
 for(const key of ['as_of','freshest_h','coverage_pct','series_count','history_verified','calls_eligible'])assert.equal(Object.hasOwn(d,key),false);
 for(const ds of Object.values(cat.datasets)){const u=new URL(c.JHChartProviderBrowser.route({provider:'regionalfed',dataset:ds.id,query:'production',offset:50}));assert.equal(u.pathname,'/browse');assert.equal(u.searchParams.get('ds'),ds.id);assert.equal(u.searchParams.get('offset'),'50');}
 const original={slug:'regionalfed',custom:3};assert.deepEqual(plain(c.JHChartProviderBrowser.providerEntries({providers:[original]})).filter(r=>r.slug==='regionalfed'),[original]);
});
test('direct canonical loading preserves every original source observation and its non-price unit',async()=>{
 const ids=JSON.parse(read("tests/fixtures/chart-regional-macro/data/browser-targets.json"));
 for(const id of ids){
  const packet=JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(R,'tests/fixtures/chart-regional-macro/data/packets',hash(id.toLowerCase())+'.json.gz')))),requests=[];
  const c=context(async url=>{requests.push(url);assert.equal(new URL(url).pathname,'/series');assert.equal(new URL(url).searchParams.get('id'),id);return {ok:true,json:async()=>packet};}),parsed=await c.JHChartCatalog.klines(id.toUpperCase());assert.equal(requests.length,1);assert.equal(parsed.evidence.whole_packet,packet);assert.equal(parsed.evidence.requested_id,id.toUpperCase());assert.deepEqual(Array.from(parsed.d,r=>[r.time,r.close]),packet.obs.filter(r=>r[1]!==null).map(r=>[Date.parse(r[0]+'T00:00:00Z')/1000,r[1]]));assert.equal(parsed.evidence.calls_eligible,false);assert.equal(parsed.evidence.sizing_eligible,false);assert.match(parsed.src,/source|observations/i);
 }
});
test('unavailable history does not fall back to market candles or create zeros',async()=>{
 const id='regionalfed:chicago-cfnai:CFNAI',packet={id,provider:'regionalfed',unit:'CFNAI standard-deviation units',obs:[],n:0,quality:{status:'unavailable',error:'source refused'}};let calls=0;
 const c=context(async url=>{calls++;assert.equal(new URL(url).pathname,'/series');return {ok:true,json:async()=>packet};});const p=await c.JHChartCatalog.klines(id);assert.equal(p.d.length,0);assert.equal(p.evidence.whole_packet,packet);const invalid=await c.JHChartCatalog.klines(id+':extra');assert.equal(invalid.d.length,0);assert.equal(calls,1);
});
test('all previous source bytes and original tests are exactly reconstructable; twins match',()=>{
 const {normalize,transition}=require('./helpers/chart-regional-macro-preservation.cjs');for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 assert.equal(read('config/regional-fed-series.json'),read('aws/lambdas/justhodl-symdir/source/regional-fed-series.json'));
});
