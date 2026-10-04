const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
test('OECD picker charts only reviewed flow versions and exact dimension keys',()=>{
 const scope={window:{},Set,URLSearchParams,TextDecoder,Uint8Array,AbortController,URL,Blob};vm.runInNewContext(read('jh-chart-provider-browser.js'),scope);const api=scope.window.JHChartProviderBrowser,cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/oecd-series.json'));assert.equal(Object.keys(cat.series).length,1511);
 for(const key of Object.keys(cat.series))assert.equal(api.action({id:'oecd:'+cat.flow+':'+key,provider:'oecd',kind:'series',chartable:true}),'chart');
 for(const id of ['oecd:unknown:USA','oecd:'+cat.flow+':ZZZ.M.PRVM.IX.BTE.Y._Z._Z.N','oecd:'+cat.flow+':USA.A.PRVM.IX.BTE.Y._Z._Z.N:GY','oecd:'+cat.flow+':USA.M.PRVM.IX.BTE.Y._Z._Z.N:FAKE'])assert.equal(api.action({id,provider:'oecd',kind:'series',chartable:true}),'inspect');
});
test('78 source alternatives retain the original request and explicitly qualify computed growth',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 const definitions=JSON.parse(read('docs/audit/2026-10-04/chart-oecd-source-review.json')).alternatives;assert.equal(Object.keys(definitions).length,78);
 for(const [requested,d] of Object.entries(definitions)){
  const c=scope.make(),r=c.chartRoute(requested);assert.equal(r.handoff,d.alternative);assert.equal(r.requested,requested);assert.equal(r.relation,'mapped-route');assert.equal(c.observationId(d.alternative),true);assert.match(r.reason,/computed by JustHodl/);assert.match(r.reason,/equivalence unverified/);
  c.openSym(requested);assert.deepEqual(Array.from(c.loads),[d.alternative.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);
 }
});
test('complete preceding source and ledger remain reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-oecd-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row]of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 assert.equal(normalize(read(transition.ledger.path),transition.ledger.path),read(transition.ledger.before_path));
});
