const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
test('four monthly reference alternatives preserve the exact request, scalar route, direction, and qualification',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 const definitions=JSON.parse(read('docs/audit/2026-10-04/chart-bis-monthly-source-review.json')).alternatives;assert.equal(Object.keys(definitions).length,4);
 for(const [requested,d] of Object.entries(definitions)){
  const c=scope.make(),r=c.chartRoute(requested);assert.equal(r.handoff,d.id);assert.equal(r.requested,requested);assert.equal(r.relation,'mapped-route');assert.equal(c.observationId(d.id),true);assert.match(r.reason,/Historical monthly end-of-period/);assert.match(r.reason,/equivalence unverified/);
  c.openSym(requested);assert.deepEqual(Array.from(c.loads),[d.id.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);
 }
});
test('complete preceding code and ledger remain reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-bis-monthly-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row]of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 assert.equal(normalize(read(transition.ledger.path),transition.ledger.path),read(transition.ledger.before_path));
});


test('quote absence is separate from qualified reference routing and never invents a quote',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;globalThis.quoteSource=fn(watch,"quoteStatus");',scope);
 const c=scope.make();c.quotes={};c.quoteIdentity=()=>({reason:'unresolved namespace'});vm.runInContext(scope.quoteSource,c);
 assert.match(c.quoteStatus('FX_IDC:JPYETB'),/^Quote unavailable.*monthly BIS reference alternative route.*unqualified/);
 assert.match(c.quoteStatus('FX_IDC:XDRJPY'),/^Quote unavailable.*daily BIS reference alternative route.*unqualified/);
 assert.match(c.quoteStatus('FX_IDC:JPYWCU'),/^Unavailable.*unresolved namespace/);assert.equal(Object.keys(c.quotes).length,0);
});
