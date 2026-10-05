const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
const cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/imf-series.json'));
test('all 10113 reviewed IMF definitions open; unknown flows, scaled series and partial keys remain inspect-only',()=>{
 const scope={window:{},Set,URLSearchParams,TextDecoder,Uint8Array,AbortController,URL,Blob};vm.runInNewContext(read('jh-chart-provider-browser.js'),scope);const api=scope.window.JHChartProviderBrowser;
 assert.equal(Object.keys(cat.series).length,10113);assert.deepEqual(Object.keys(cat.flows).sort(),['LS','MFS_IR','PI','PPI']);
 for(const [key,d] of Object.entries(cat.series)){const row={id:'imf:'+key,provider:'imf',kind:'series',chartable:true};assert.equal(api.action(row),'chart',key);assert.equal(api.action({...row,id:row.id+':extra'}),'inspect',key);assert.equal(d.fixed_attributes.SCALE,'0');assert.ok(['M','Q','A'].includes(d.freq));assert.ok(d.unit);}
 for(const id of ['imf:UNKNOWN:USA.U.PT.M','imf:LS:USA.UP.PE.M','imf:LS:USA.U.PT','imf:MFS_CBS:USA.M1.M'])assert.equal(api.action({id,provider:'imf',kind:'series',chartable:true}),'inspect');
});
test('68 IMF alternatives retain requested identities and source qualifications through selection',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 const review=JSON.parse(read('docs/audit/2026-10-04/chart-imf-watchlist-review.json'));assert.equal(Object.keys(review.alternatives).length,68);
 for(const [requested,row] of Object.entries(review.alternatives)){const c=scope.make(),r=c.chartRoute(requested),d=cat.series[row.alternative.slice(4)];assert.equal(r.requested,requested);assert.equal(r.handoff,row.alternative);assert.equal(r.relation,'mapped-route');assert.match(r.reason,/equivalence unverified/);assert.match(r.reason,/not live policy decisions/);assert.equal(c.observationId(row.alternative),true);c.openSym(requested);assert.deepEqual(Array.from(c.loads),[row.alternative.toUpperCase()]);assert.equal(c.chartSelection.requested,requested);assert.doesNotMatch(c.chartRouteText(r,null,[]),/supplementary Yahoo/);assert.equal(row.equivalence_verified,false);assert.equal(row.definition.fixed_attributes.SCALE,'0');assert.equal(row.definition.freq,'M');assert.equal(d.key,row.definition.key);assert.equal(d.unit,row.definition.unit);assert.equal(row.country_reference.id,row.definition.dimensions.COUNTRY);}
});
test('the exact predecessor survives and configuration twins match byte for byte',()=>{
 const {normalize,transition}=require('./helpers/chart-imf-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unreviewed',file));}
 assert.equal(read('aws/lambdas/justhodl-symdir/source/imf-series.json'),read('aws/lambdas/justhodl-symdir/config/imf-series.json'));
});
test('published transformations, index bases and rate units remain distinct',()=>{
 assert.equal(cat.series['LS:USA.U.PT.M'].unit,'Percent of labor force');
 assert.equal(cat.series['MFS_IR:USA.MFS166_RT_PT_A_PT.M'].unit,'Percent per annum');
 assert.match(cat.series['PI:USA.IND.IX.M'].unit,/2010=100/);
 assert.match(cat.series['PI:USA.IND.YOY_PCH_PT.M'].unit,/same period one year earlier/);
 for(const [key,d] of Object.entries(cat.series)){assert.ok(d.seasonal_adjustment);if(key.includes('.YOY_'))assert.match(d.unit,/one year earlier/);}
});
