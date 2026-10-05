const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8'),cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/cboe-indices.json'));
test('Cboe picker supports exact reviewed index identities without consuming exchange namespaces',()=>{
 const scope={window:{},Set,URLSearchParams,TextDecoder,Uint8Array,AbortController,URL,Blob};vm.runInNewContext(read('jh-chart-provider-browser.js'),scope);const api=scope.window.JHChartProviderBrowser;assert.equal(Object.keys(cat.series).length,77);
 for(const symbol of Object.keys(cat.series)){const row={id:'cboeindex:'+symbol,provider:'cboeindex',kind:'series',chartable:true};assert.equal(api.action(row),'chart');assert.equal(api.action({...row,id:'CBOE:'+symbol}),'inspect');assert.equal(api.action({...row,id:row.id+':extra'}),'inspect');}
 for(const id of ['cboeindex:VX1!','cboeindex:SPX','cboeindex:UNKNOWN'])assert.equal(api.action({id,provider:'cboeindex',kind:'series',chartable:true}),'inspect');
});
test('Cboe watchlist choices preserve exact requests and explicitly qualify close-only index observations',()=>{
 const text=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(text+'\nglobalThis.make=harness;',scope);const rows=JSON.parse(read('docs/audit/2026-10-04/chart-cboe-watchlist-review.json')).alternatives;assert.equal(Object.keys(rows).length,77);
 for(const [requested,row] of Object.entries(rows)){const c=scope.make(),r=c.chartRoute(requested);assert.equal(r.requested,requested);assert.equal(r.handoff,row.alternative);assert.match(r.reason,/source equivalence unverified/);assert.match(r.reason,/settlement close/);assert.match(r.reason,/market OHLC and traded volume unqualified/);assert.equal(c.observationId(r.handoff),true);c.openSym(requested);assert.deepEqual(Array.from(c.loads),[row.alternative.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);assert.equal(row.calls_eligible,false);assert.equal(row.sizing_eligible,false);}
});
test('whole preceding sources and all existing aliases remain reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-cboe-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 assert.equal(read('aws/lambdas/justhodl-symdir/source/cboe-indices.json'),read('aws/lambdas/justhodl-symdir/config/cboe-indices.json'));
});
