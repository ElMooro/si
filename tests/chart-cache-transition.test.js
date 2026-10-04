// Synthetic source/cache-contract acceptance. No provider or production requests.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),crypto=require('node:crypto'),vm=require('node:vm'),cp=require('node:child_process');
const ROOT=path.join(__dirname,'..'),P=require('./helpers/chart-cache-transition-preservation.cjs'),read=p=>fs.readFileSync(path.join(ROOT,p),'utf8');
test('two reviewed chart sites restore every preceding HTML byte',()=>{
 const t=P.manifest.sources.html,actual=read(t.path);assert.equal(P.hash(actual),t.after_sha256);assert.equal(P.restoreHtml(actual),read(t.before_path));
 assert.equal(crypto.createHash('md5').update(read('jh-chart-engine.js')).digest('hex').slice(0,8),'3a70581b');
 assert.equal(actual.split('/jh-chart-engine.js?v=3a70581b&watch=watchlist-transaction-v1').length,2);
});
for(const [key,t]of Object.entries(P.manifest.hooks))test('complete prior '+key+' reader and all assertions preserved',()=>{
 assert.equal(P.restoreHook(read(t.path),key),read(t.before_path));
 assert.throws(()=>P.restoreHook(read(t.before_path),key),/exact reviewed cache transition/);
});
for(const [name,before,after]of[
 ['engine old token','v=3a70581b','v=20261002ac-shelves'],
 ['watch identity','/jh-chart-engine.js?v=3a70581b&watch=watchlist-transaction-v1','/jh-chart-engine.js?v=3a70581b&watch=changed'],
 ['reload','/* jh-tvux-36 retired:','location.reload(); /* jh-tvux-36 retired:'],
 ['quantity mutation','/* jh-tvux-36 retired:','window.lastBars[0].volume=0; /* jh-tvux-36 retired:'],
 ['cache deletion','/* jh-tvux-36 retired:','caches.delete("invented-history"); /* jh-tvux-36 retired:'],
 ['navigation','id="symchip"','id="changed-symbol-chip"'],
 ['layout','--watch-w:320px','--watch-w:321px'],
])test('exact inverse rejects unreviewed '+name+' change',()=>{const raw=read('chart.html');assert.equal(raw.split(before).length,2);assert.throws(()=>P.restoreHtml(raw.replace(before,after)),/exact reviewed cache transition/);});
for(const marker of [null,'old','jh-tvux-36'])for(const denied of [false,true])test('retired startup preserves histories and worker with marker '+marker+' storage denial '+denied,()=>{
 const migration=P.manifest.sources.html.edits[0].after.replace(/^<script>\n|\n<\/script>$/g,''),calls=[];
 const storage={getItem(){if(denied)throw Error('synthetic denied');return marker;},setItem(){calls.push('write');},removeItem(){calls.push('remove');},clear(){calls.push('clear');}};
 vm.runInNewContext(migration,{window:{},localStorage:storage,sessionStorage:storage,caches:{keys(){calls.push('keys');},delete(){calls.push('delete');}},navigator:{serviceWorker:{getRegistrations(){calls.push('registrations');}}},location:{reload(){calls.push('reload');}}});
 assert.deepEqual(calls,[]);
});
test('actual unchanged stamper retains compound engine pin and rolls helper distribution graph',()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'jh-cache-transition-'));
 try{
  for(const file of ['chart.html','jh-chart-engine.js','jh-chart-distribution.js','jh-chart-bbfix.js'])fs.copyFileSync(path.join(ROOT,file),path.join(dir,file));
  const run=cp.spawnSync('python3',[path.join(ROOT,'scripts/stamp_assets.py'),dir],{encoding:'utf8'});assert.equal(run.status,0,run.stdout+run.stderr);
  const html=fs.readFileSync(path.join(dir,'chart.html'),'utf8'),engine=fs.readFileSync(path.join(dir,'jh-chart-engine.js'));
  const pin=crypto.createHash('md5').update(engine).digest('hex').slice(0,8);assert.equal(pin,'3a70581b');assert.ok(html.includes('/jh-chart-engine.js?v='+pin+'&watch=watchlist-transaction-v1'));
  const helper=fs.readFileSync(path.join(dir,'jh-chart-bbfix.js')),hv=crypto.createHash('md5').update(helper).digest('hex').slice(0,8),dist=fs.readFileSync(path.join(dir,'jh-chart-distribution.js'));
  assert.equal(hv,'87629d9a');assert.ok(dist.toString().includes('/jh-chart-bbfix.js?v='+hv));assert.ok(html.includes('/jh-chart-distribution.js?v='+crypto.createHash('md5').update(dist).digest('hex').slice(0,8)));
  const before=fs.readFileSync(path.join(dir,'chart.html'));assert.equal(cp.spawnSync('python3',[path.join(ROOT,'scripts/stamp_assets.py'),dir]).status,0);assert.deepEqual(fs.readFileSync(path.join(dir,'chart.html')),before);
 }finally{fs.rmSync(dir,{recursive:true,force:true});}
});
