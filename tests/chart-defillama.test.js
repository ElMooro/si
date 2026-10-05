const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto'),zlib=require('node:zlib');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8'),cat=JSON.parse(read('config/defillama-tvl.json')),plain=v=>JSON.parse(JSON.stringify(v));
function context(fetch){const c={window:{},Set,Map,URLSearchParams,URL,TextDecoder,Uint8Array,AbortController,Blob,setTimeout,clearTimeout,fetch};c.window=c;for(const p of ['jh-watchlist-quotes.js','jh-observation-series.js','jh-observation-cache.js','jh-chart-catalog.js','jh-chart-provider-browser.js'])vm.runInNewContext(read(p),c);return c;}
test('all 469 exact default TVL definitions bind without token, chain or market substitutions',()=>{
 const c=context();assert.equal(Object.keys(cat.series).length,469);
 for(const d of Object.values(cat.series)){
  assert.equal(c.JHChartCatalog.chartId(d.id),d.id);assert.equal(c.JHChartCatalog.chartId(d.id.toUpperCase()),d.id);assert.equal(c.JHChartCatalog.isWarehouse(d.id),true);
  assert.equal(c.JHChartProviderBrowser.action({id:d.id,provider:'defillama',kind:'series',chartable:true}),'chart');assert.ok(c.JHWatchlistQuotes.resolve(d.id,{}).reason);
  assert.equal(c.JHChartProviderBrowser.action({id:d.id+':extra',provider:'defillama',kind:'series',chartable:true}),'inspect');
 }
 for(const id of ['defillama:tvl:ETH','defillama:tvl:../all','defillama:tvl:all?x=1','defillama:tvl:Ethereum/other']){assert.equal(c.JHChartCatalog.chartId(id),'');assert.equal(c.JHChartProviderBrowser.action({id,provider:'defillama',kind:'series',chartable:true}),'inspect');}
 assert.equal(c.JHChartCatalog.chartId('DEFILLAMA:TOTAL_TVL'),'defillama:tvl:all');assert.equal(c.JHChartCatalog.chartId('defillama:tvl:EOS%20EVM'),'defillama:tvl:EOS%20EVM');
});
test('watchlist handoff retains requested identity and makes the valuation-stock qualification visible',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 for(const requested of ['DEFILLAMA:TOTAL_TVL','defillama:tvl:Ethereum','defillama:tvl:EOS%20EVM']){
  const c=scope.make(),r=c.chartRoute(requested);assert.equal(r.requested,requested);assert.equal(r.handoff,requested==='DEFILLAMA:TOTAL_TVL'?'defillama:tvl:all':requested);assert.match(r.reason,/not net deposits or capital inflows/);assert.match(r.reason,/double-counted TVL/);assert.match(r.reason,/equivalence unverified/);assert.equal(c.observationId(r.handoff),true);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);c.openSym(requested);assert.deepEqual(Array.from(c.loads),[r.handoff.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);
 }
 const c=scope.make();c.openSym('defillama:tvl:ETH');assert.deepEqual(Array.from(c.loads),[]);
});
test('provider discovery adds a separate adapter without inventing a warehouse catalogue',()=>{
 const c=context(),p={providers:[{slug:'fred',name:'Original',custom:17}]},before=JSON.stringify(p),rows=plain(c.JHChartProviderBrowser.providerEntries(p));assert.equal(JSON.stringify(p),before);assert.deepEqual(rows[0],p.providers[0]);const d=rows.find(r=>r.slug==='defillama');assert.equal(d.discovery_origin,'chart_adapter');assert.equal(d.catalogue_slug,null);assert.equal(c.JHChartProviderBrowser.storedProvider('defillama'),null);assert.throws(()=>c.JHChartProviderBrowser.route({provider:'defillama',view:'files'}),/No published stored-file/);
 for(const key of ['as_of','freshest_h','coverage_pct','series_count','history_verified','calls_eligible'])assert.equal(Object.hasOwn(d,key),false);
 const original={slug:'defillama',name:'Publisher',custom:3};assert.deepEqual(plain(c.JHChartProviderBrowser.providerEntries({providers:[original]})).filter(r=>r.slug==='defillama'),[original]);
 const u=new URL(c.JHChartProviderBrowser.route({provider:'defillama',dataset:'defillama:reviewed-tvl-history',query:'EOS',offset:50}));assert.equal(u.pathname,'/browse');assert.equal(u.searchParams.get('ds'),'defillama:reviewed-tvl-history');assert.equal(u.searchParams.get('offset'),'50');
});
test('direct alias loading uses the exact scalar endpoint and retains all original values',async()=>{
 const raw=zlib.gunzipSync(fs.readFileSync(path.join(R,'tests/fixtures/chart-defillama/data/total-tvl.json.gz'))),original=JSON.parse(raw),obs=original.map(r=>[new Date(r.date*1000).toISOString().slice(0,10),r.tvl]);
 const packet={id:'defillama:tvl:all',provider:'defillama',unit:'USD',freq:'D',obs,n:obs.length,name:'Default TVL',history:{market_ohlc_qualified:false,traded_volume_qualified:false,full_upstream_history_verified:false},calls_eligible:false,sizing_eligible:false},requests=[];
 const c=context(async url=>{requests.push(url);assert.equal(new URL(url).pathname,'/series');assert.equal(new URL(url).searchParams.get('id'),'defillama:tvl:all');return {ok:true,json:async()=>packet};});
 const parsed=await c.JHChartCatalog.klines('DEFILLAMA:TOTAL_TVL');assert.equal(requests.length,1);assert.equal(parsed.evidence.whole_packet,packet);assert.equal(parsed.evidence.requested_id,'DEFILLAMA:TOTAL_TVL');assert.equal(parsed.evidence.chart_alias.resolved,'defillama:tvl:all');assert.deepEqual(plain(parsed.d.map(r=>[r.time,r.close])),original.map(r=>[r.date,r.tvl]));assert.equal(parsed.evidence.calls_eligible,false);assert.equal(parsed.evidence.sizing_eligible,false);
 const invalid=await c.JHChartCatalog.klines('defillama:tvl:ETH');assert.equal(invalid.d.length,0);assert.equal(requests.length,1);
});
test('empty or rejected DefiLlama packets cannot become market quotes or synthetic zero history',async()=>{
 for(const packet of [{id:'defillama:tvl:all',provider:'defillama',unit:'USD',obs:[],n:0,quality:{status:'unavailable',error:'refused'}},{id:'defillama:tvl:all',provider:'defillama',unit:'USD',obs:[['2026-01-01',null],['2026-01-02',false],['2026-01-03','']],n:0}]){
  const c=context(async url=>{assert.equal(new URL(url).pathname,'/series');return {ok:true,json:async()=>packet};});const result=await c.JHChartCatalog.klines('defillama:tvl:all');assert.equal(result.d.length,0);assert.equal(result.evidence.whole_packet,packet);
 }
});
test('whole preceding modules, tests and original assertions remain reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-defillama-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 assert.equal(read('config/defillama-tvl.json'),read('aws/lambdas/justhodl-symdir/source/defillama-tvl.json'));
});
