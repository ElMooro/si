const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
test('every additional OECD key charts only its reviewed complete flow and dimensions',()=>{
 const scope={window:{},Set,URLSearchParams,TextDecoder,Uint8Array,AbortController,URL,Blob};vm.runInNewContext(read('jh-chart-provider-browser.js'),scope);const api=scope.window.JHChartProviderBrowser,cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/oecd-additional-series.json'));let count=0;
 for(const [flow,d] of Object.entries(cat.flows))for(const key of Object.keys(d.series)){count++;const id='oecd:'+flow+':'+key;assert.equal(api.action({id,provider:'oecd',kind:'series',chartable:true}),'chart');for(const bad of [id+':G1',id+':GY',id.replace(flow,flow.slice(0,-1)+'9')])assert.equal(api.action({id:bad,provider:'oecd',kind:'series',chartable:true}),'inspect');}
 assert.equal(count,2338);for(const flow of ['__proto__','constructor','toString','UNKNOWN'])assert.equal(api.action({id:'oecd:'+flow+':USA',provider:'oecd',kind:'series',chartable:true}),'inspect');
});
test('all catalogue config twins preserve exact definitions, status and units',()=>{
 for(const name of ['oecd-series.json','oecd-additional-series.json'])assert.equal(read('aws/lambdas/justhodl-symdir/source/'+name),read('aws/lambdas/justhodl-symdir/config/'+name));
 const cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/oecd-additional-series.json'));assert.deepEqual(Object.values(cat.flows).map(d=>Object.keys(d.series).length),[875,1050,413]);
 for(const d of Object.values(cat.flows))for(const [key,row] of Object.entries(d.series)){assert.equal(key,d.dimension_order.map(f=>row.dimensions[f]).join('.'));assert.equal(row.unit_mult,'0');assert.ok(['M','Q','A'].includes(row.freq));}
});
test('complete previous modules, tests and every assertion remain reconstructable',()=>{
 const {normalize,transition}=require('./helpers/chart-oecd-additional-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row] of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256,file);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unrelated',file));}
});
