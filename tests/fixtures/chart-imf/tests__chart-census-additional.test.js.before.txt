const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
const cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/census-series.json'));
test('all 13858 exact definitions across 20 Census datasets can open, partial or invented dimensions cannot',()=>{
 const scope={window:{},Set,URLSearchParams,TextDecoder,Uint8Array,AbortController,URL,Blob};vm.runInNewContext(read('jh-chart-provider-browser.js'),scope);const api=scope.window.JHChartProviderBrowser;
 assert.equal(Object.keys(cat.series).length,13858);assert.equal(Object.keys(cat.dataset_definitions).length,20);
 for(const [key,d] of Object.entries(cat.series)){const row={id:'census:'+key,provider:'census',kind:'series',chartable:true};assert.equal(api.action(row),'chart',key);assert.equal(api.action({...row,id:row.id+':extra'}),'inspect',key);assert.ok(['M','Q','A'].includes(d.freq));assert.ok(d.unit);assert.ok(d.measure_definition);}
 for(const dataset of Object.keys(cat.dataset_definitions)){assert.equal(api.action({id:'census:'+dataset,provider:'census',kind:'dataset'}),'dataset');assert.equal(api.action({id:'census:'+dataset+':invented:no:US',provider:'census',kind:'series',chartable:true}),'inspect');}
});
test('seven additional watchlist alternatives preserve identity, source scope and unverified equivalence',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 const aliases=JSON.parse(read('docs/audit/2026-10-04/chart-census-additional-watchlist-review.json')).alternatives;assert.equal(Object.keys(aliases).length,7);
 for(const [requested,row] of Object.entries(aliases)){const c=scope.make(),r=c.chartRoute(requested),d=cat.series[row.alternative.slice(7)];assert.deepEqual(d,row.definition);assert.equal(d.sampling_error,false);assert.equal(r.requested,requested);assert.equal(r.handoff,row.alternative);assert.equal(r.relation,'mapped-route');assert.match(r.reason,/equivalence unverified/);assert.match(r.reason,/Advance and revised publication vintages are not interchangeable/);assert.equal(c.observationId(row.alternative),true);c.openSym(requested);assert.deepEqual(Array.from(c.loads),[row.alternative.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);assert.equal(row.vendor_equivalence_verified,false);assert.equal(row.calls_eligible,false);assert.equal(row.sizing_eligible,false);}
 assert.equal(cat.series[aliases['ECONOMICS:USFOET'].alternative.slice(7)].category,'MXT');
 assert.equal(cat.series[aliases['ECONOMICS:USDGOET'].alternative.slice(7)].category,'DXT');
 for(const [k,d] of Object.entries(aliases))assert.equal(d.definition.unit,k==='ECONOMICS:USGTB'?'Millions of US dollars':'Percent');
 assert.equal(aliases['ECONOMICS:USGTB'].definition.category,'CBG');
});
test('whole predecessor and all existing Census definitions remain exactly reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-census-additional-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 const old=JSON.parse(read('tests/fixtures/chart-census-additional/census-series.before.json'));assert.equal(Object.keys(old.series).length,3402);
 for(const [k,v] of Object.entries(old.series))assert.deepEqual(cat.series[k],v);
 assert.equal(read('aws/lambdas/justhodl-symdir/source/census-series.json'),read('aws/lambdas/justhodl-symdir/config/census-series.json'));
});
test('projected formations, duration units, annual rates and source marker meanings remain distinguishable',()=>{
 for(const d of Object.values(cat.series)){if(d.dataset==='bfs'){assert.match(d.reference_period_scope,/Application cohort month/);if(d.data_type.startsWith('BF_DUR'))assert.equal(d.unit,'Quarters');if(d.data_type.startsWith('BF_PBF'))assert.equal(d.observation_kind,'projected_formations');if(d.data_type.startsWith('BF_SBF'))assert.match(d.measurement_basis,/boundary not verified/);}}
 assert.equal(cat.series['mhs2:SH:T:yes:US'].unit,'Thousands of units (seasonally adjusted annual rate)');
 assert.equal(cat.dataset_definitions.mhs2.source_markers.Z,'estimate_below_50_units');assert.equal(cat.dataset_definitions.bfs.source_markers.S,'estimate_fails_publication_quality_standard');assert.equal(cat.dataset_definitions.mhs,undefined);
 assert.equal(Object.values(cat.series).filter(d=>d.snapshot_numeric_rows===0).length,4);
});
