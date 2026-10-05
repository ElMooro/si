const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
test('six Census alternatives preserve requested identity, exact dimensions and qualification',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 const definitions=JSON.parse(read('docs/audit/2026-10-04/chart-census-watchlist-source-review.json')).alternatives,cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/census-series.json'));assert.equal(Object.keys(definitions).length,6);
 for(const [requested,d] of Object.entries(definitions)){
  const c=scope.make(),r=c.chartRoute(requested),definition=cat.series[d.alternative.substring(7)];assert.ok(definition);assert.equal(definition.sampling_error,false);assert.equal(r.handoff,d.alternative);assert.equal(r.requested,requested);assert.equal(r.relation,'mapped-route');assert.equal(c.observationId(d.alternative),true);assert.match(r.reason,/Census published-measurement alternative/);assert.match(r.reason,/equivalence unverified/);assert.doesNotMatch(r.reason,/same indicator|computed by JustHodl/);
  c.openSym(requested);assert.deepEqual(Array.from(c.loads),[d.alternative.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);
 }
 for(const request of ['ECONOMICS:USRSMM','ECONOMICS:USRSEA','ECONOMICS:USCONSTS','ECONOMICS:USRIEA'])assert.equal(cat.series[definitions[request].alternative.substring(7)].unit,'Percent');
 for(const request of ['ECONOMICS:USHST','ECONOMICS:USNHS'])assert.equal(cat.series[definitions[request].alternative.substring(7)].unit,'Thousands of units (seasonally adjusted annual rate)');
 assert.equal(definitions['ECONOMICS:USRI'],undefined);assert.equal(definitions['ECONOMICS:USRCNSMSPND'],undefined);
});
test('Census source lookup refuses conflicting source names and preserves canonical identities',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 const id='census:resconst:TOTAL:ASTARTS:yes:US',c=scope.make({[id.toUpperCase()]:{source:'FRED',id:'WRONG'}}),r=c.chartRoute(id);assert.equal(r.handoff,id);assert.equal(r.relation,'exact');c.openSym(id);assert.deepEqual(Array.from(c.loads),[id.toUpperCase()]);
});
test('the whole previous chart, routing functions and tests remain reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-census-watchlist-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unrelated edit',file));}
});
