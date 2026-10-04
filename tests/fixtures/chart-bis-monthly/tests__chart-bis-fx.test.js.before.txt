const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
test('picker charts exactly the reviewed 1234 FX definitions; unknown currencies and collections stay inspect-only',()=>{
 const scope={window:{},Set,URLSearchParams,TextDecoder,Uint8Array,AbortController,URL,Blob};vm.runInNewContext(read('jh-chart-provider-browser.js'),scope);const api=scope.window.JHChartProviderBrowser;
 const cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/bis-fx-series.json'));
 assert.equal(Object.keys(cat.series).length,1234);
 for(const key of Object.keys(cat.series))assert.equal(api.action({id:'bis:WS_XRU:'+key,provider:'bis',kind:'series',chartable:true}),'chart');
 assert.equal(api.action({id:'bisfx:XDR:JPY:D',provider:'bisfx',kind:'series',chartable:true}),'chart');assert.equal(api.action({id:'bisfx:FAK:BAD:D',provider:'bisfx',kind:'series',chartable:true}),'inspect');
 for(const id of ['bis:WS_XRU:D.JP.USD.A','bis:WS_XRU:D.JP.JPY.E','bis:WS_XRU:D.XX.JPY.A'])assert.equal(api.action({id,provider:'bis',kind:'series',chartable:true}),'inspect');
});
test('59 reference alternatives preserve the exact request, scalar route, direction, and qualification',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 const definitions=JSON.parse(read('docs/audit/2026-10-04/chart-bis-fx-source-review.json')).alternatives;assert.equal(Object.keys(definitions).length,59);
 for(const [requested,d] of Object.entries(definitions)){
  const c=scope.make(),r=c.chartRoute(requested);assert.equal(r.handoff,d.id);assert.equal(r.requested,requested);assert.equal(r.relation,'mapped-route');assert.equal(c.observationId(d.id),true);assert.match(r.reason,/Different fixing times/);assert.match(r.reason,/equivalence unverified/);
  c.openSym(requested);assert.deepEqual(Array.from(c.loads),[d.id.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);
 }
});
test('complete preceding code and ledger remain reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-bis-fx-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row]of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 assert.equal(normalize(read(transition.ledger.path),transition.ledger.path),read(transition.ledger.before_path));
});
