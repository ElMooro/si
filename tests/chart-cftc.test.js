const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=f=>fs.readFileSync(path.join(R,f),'utf8'),catalog=JSON.parse(read('aws/lambdas/justhodl-symdir/source/cftc-series.json'));
function boot(){const window={};vm.runInNewContext(read('jh-chart-cftc.js'),{window});return window;}
test('every COT watchlist alias agrees with the native exact definition and returns a fresh object',()=>{
 const window=boot();assert.equal(Object.keys(catalog.aliases).length,346);
 for(const [id,row] of Object.entries(catalog.aliases)){const got=window.JHChartCFTC.resolve(id);assert.equal(got.id,row.canonical);assert.equal(got.field,row.field);assert.equal(got.unit,row.unit);assert.equal(got.scope,row.scope);assert.equal(got.history_verified,false);got.field='mutated';assert.equal(window.JHChartCFTC.resolve(id).field,row.field);assert.equal(window.JHChartCFTC.resolve(row.canonical).id,row.canonical);}
 for(const id of [null,42,true,'COT3:wrong','cftc:yw9f-hn96|132741|constructor','cftc:yw9f-hn96|132741|noncomm_short'])assert.equal(window.JHChartCFTC.resolve(id),null);
});
test('catalogue routes exact CFTC identities and prevents unknown COT market inference',()=>{
 const window=boot();vm.runInNewContext(read('jh-chart-catalog.js'),{window,Set,Map});
 for(const [id,row] of Object.entries(catalog.aliases)){assert.equal(window.JHChartCatalog.chartId(id),row.canonical);assert.equal(window.JHChartCatalog.isWarehouse(id),true);}
 assert.equal(window.JHChartCatalog.chartId('COT3:missing'),'');assert.equal(window.JHChartCatalog.isWarehouse('COT3:missing'),true);
});
test('actual watchlist route ignores the legacy noncommercial substitution for every COT definition',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],context={require,process,console,AbortController,setTimeout,clearTimeout};
 vm.createContext(context);vm.runInContext(source+'\nglobalThis.makeHarness=harness;',context);const window=boot();
 for(const [id,row]of Object.entries(catalog.aliases)){
  const c=context.makeHarness({[id]:{source:'COT',id:'jun7-fc8e|132741|noncomm_short'}});c.window.JHChartCFTC=window.JHChartCFTC;
  const route=c.chartRoute(id);assert.equal(route.handoff,row.canonical);assert.equal(route.relation,'schema-defined');assert.equal(route.definition.unit,row.unit);c.openSym(id);assert.deepEqual(JSON.parse(JSON.stringify(c.loads)),[row.canonical]);assert.equal(c.chartSelection.frame,row.canonical);
 }
});
test('all reviewed CFTC edits retain complete predecessors and append every old watchlist edit',()=>{
 const {normalize,transition}=require('./helpers/chart-cftc-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row]of Object.entries(transition.changes)){const current=read(file);assert.equal(hash(current),row.after_sha256);assert.equal(normalize(current,file),read(row.before_path));assert.throws(()=>normalize(current+'\n// unreviewed',file));}
 const old=JSON.parse(read(transition.ledger.before_path)),now=JSON.parse(read(transition.ledger.path));
 for(const [file,row]of Object.entries(old)){const current=structuredClone(now[file]);if(['chart.html','jh-chart-engine.js','jh-chart-tvwatch.js'].includes(file)){assert.deepEqual(current.edits.slice(row.edits.length),transition.changes[file].edits);current.edits=current.edits.slice(0,row.edits.length);current.after_sha256=row.after_sha256;}assert.deepEqual(current,row);}
});
