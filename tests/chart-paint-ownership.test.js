const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../jh-chart-engine.js'),'utf8'),parser={exports:{}};
Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
const nodes=parser.exports.parse(source,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const named=Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,source.slice(n.start,n.end)]));
const rows=(volume=100)=>Array.from({length:60},(_,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i,high:102+i,low:99+i,close:101+i,volume}));
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
const plain=x=>JSON.parse(JSON.stringify(x));
function setup(names,extra={}){const c={window:{},lastSpxDaily:[{sentinel:'original'}],lastBenchName:'original',spyBars:[{sentinel:'original'}],tf:'1d',spec:x=>[x],resampleToTf:x=>x,...extra};vm.createContext(c);if(names.includes('vsSpy')&&!extra.alignSpy)names=['alignSpy',...names];vm.runInContext(names.map(n=>named[n]).join('\n'),c);return c;}

test('cancelled primary benchmark response cannot overwrite daily state or start a fallback',async()=>{
 const wait=deferred(),calls=[];let current=true;const c=setup(['loadSpxDaily'],{klines:async(...args)=>{calls.push(args);return wait.promise;}}),before=c.lastSpxDaily;
 const pending=c.loadSpxDaily(()=>current);current=false;wait.resolve(rows().slice(0,2));assert.deepEqual(plain(await pending),[]);assert.equal(c.lastSpxDaily,before);assert.equal(calls.length,1);
});
test('cancelled fallback benchmark response cannot overwrite daily state',async()=>{
 const wait=deferred(),entered=deferred();let current=true;const c=setup(['loadSpxDaily'],{klines:async s=>{if(s==='^GSPC')return [];entered.resolve();return wait.promise;}}),before=c.lastSpxDaily;
 const pending=c.loadSpxDaily(()=>current);await entered.promise;current=false;wait.resolve(rows());assert.deepEqual(plain(await pending),[]);assert.equal(c.lastSpxDaily,before);
});
test('current benchmark retains original primary and fallback selection with complete inputs',async()=>{
 for(const primary of [rows(),[]]){const calls=[],fallback=rows(200),c=setup(['loadSpxDaily'],{klines:async s=>{calls.push(s);return s==='^GSPC'?primary:fallback;}});
  const result=await c.loadSpxDaily(()=>true);assert.equal(result,primary.length?primary:fallback);assert.equal(c.lastSpxDaily,result);assert.deepEqual(calls,primary.length?['^GSPC']:['^GSPC','SPX']);}
});
test('cancelled daily handoff cannot rename a benchmark or start intraday acquisition',async()=>{
 const wait=deferred();let current=true;const c=setup(['loadBench'],{loadSpxDaily:()=>wait.promise,klines:()=>{throw Error('Obsolete request');}});
 const pending=c.loadBench('1h',()=>current);current=false;wait.resolve(rows());assert.deepEqual(plain(await pending),[]);assert.equal(c.lastBenchName,'original');
});
test('cancelled intraday response cannot publish the wrong benchmark identity',async()=>{
 const wait=deferred(),entered=deferred();let current=true;const c=setup(['loadBench'],{loadSpxDaily:async()=>rows(),klines:()=>{entered.resolve();return wait.promise;}});
 const pending=c.loadBench('1h',()=>current);await entered.promise;current=false;wait.resolve(rows());assert.deepEqual(plain(await pending),[]);assert.equal(c.lastBenchName,'original');
});
test('current daily, intraday and grouped benchmark branches retain their original choices',async()=>{
 for(const interval of ['1d','1h','1w']){const daily=rows(),intra=rows(200),groups=rows(300),c=setup(['loadBench'],{loadSpxDaily:async()=>daily,klines:async()=>intra,resampleToTf:(d,tf)=>{assert.equal(d,daily);assert.equal(tf,'1w');return groups;}});
  const result=await c.loadBench(interval,()=>true);assert.equal(result,interval==='1d'?daily:interval==='1h'?intra:groups);assert.equal(c.lastBenchName,interval==='1h'?'SPY':'SPX');}
});
test('cancelled relative-return benchmark does not replace the selected comparison frame',async()=>{
 const wait=deferred();let current=true;const c=setup(['vsSpy'],{loadBench:()=>wait.promise}),before=c.spyBars;
 const pending=c.vsSpy(rows(),()=>current);current=false;wait.resolve(rows(200));assert.deepEqual(plain(await pending),[]);assert.equal(c.spyBars,before);
});
test('cancelled relative fallback never computes a result for an obsolete frame',async()=>{
 const wait=deferred(),entered=deferred();let current=true;const c=setup(['vsSpy'],{loadBench:async()=>[],loadSpxDaily:()=>{entered.resolve();return wait.promise;},alignSpy:()=>{throw Error('Obsolete computation');}});
 const pending=c.vsSpy(rows(),()=>current);await entered.promise;current=false;wait.resolve(rows());assert.deepEqual(plain(await pending),[]);
});

function painter(){
 const effects=[],adds=[],rafs=[],elements=new Map(),el=id=>{if(!elements.has(id))elements.set(id,{textContent:'',style:{}});return elements.get(id);};
 const chart={timeScale:()=>({getVisibleLogicalRange:()=>null,fitContent(){},setVisibleLogicalRange(){}}),priceScale:()=>({applyOptions(){}}),applyOptions(){},addHistogramSeries:o=>{const s={options:o,setData:d=>{s.data=d;}};adds.push(s);return s;}};
 const c=setup(['paint'],{paintSeq:0,loadGen:0,active:'OLD',tf:'1d',barEvidence:new WeakMap(),observationId:()=>false,lastBars:[],INDS:[],OSC:[],series:[],mode:'ytd',lastSource:'invented',compare:[],COLORS:[],tape:{prints:[]},preserveView:false,volOn:true,watermark:true,UP:'up',DN:'down',chart,window:{},document:{getElementById:el},
  wipe(){c.series=[];},pal:()=>({}),volScore:()=>0,chartLook:()=>({}),rightScaleMode:()=>0,computeChange:d=>d.map(b=>({time:b.time,value:b.close})),requestAnimationFrame:f=>rafs.push(f),refreshHiLoVP:()=>effects.push(['refresh']),fmtVol:String,loadBench:async()=>rows(),lastSpxDaily:[],lastBenchName:'SPX',lastVsSpx:null});
 for(const name of ['paintOsc','drawSVG','paintPat','quoteUI','renderDetail','checkAlerts','countdown','renderLegend','syncLivePill','renderTech','renderOver','renderSeason','renderFin','paintMini','writeState'])c[name]=(...args)=>effects.push([name,...args]);
 return {c,effects,adds,rafs,el};
}
test('an older successful paint cannot replace the newer frame after its benchmark resolves',async()=>{
 const h=painter(),wait=deferred(),old=rows(100),fresh=rows(null);let calls=0;h.c.loadBench=()=>++calls===1?wait.promise:Promise.resolve(rows(200));
 const pending=h.c.paint(old);h.c.active='NEW';h.c.loadGen++;await h.c.paint(fresh);const before=h.effects.slice(),bench=h.c.spyBars;
 wait.resolve(rows(300));await pending;assert.deepEqual(h.effects,before);assert.equal(h.c.lastBars,fresh);assert.equal(h.c.spyBars,bench);assert.equal(h.adds.length,2);
});
test('an obsolete benchmark rejection cannot bypass cancellation through the catch block',async()=>{
 const h=painter(),wait=deferred(),old=rows(),fresh=rows(null);let calls=0;h.c.loadBench=()=>++calls===1?wait.promise:Promise.resolve(rows(200));
 const pending=h.c.paint(old);h.c.active='NEW';h.c.loadGen++;await h.c.paint(fresh);const before=h.effects.slice();wait.reject(Error('invented failure'));await pending;assert.deepEqual(h.effects,before);
});
test('a newly started acquisition invalidates an old paint before the new data arrives',async()=>{
 const h=painter(),wait=deferred();h.c.loadBench=()=>wait.promise;const pending=h.c.paint(rows());h.c.loadGen++;wait.resolve(rows(200));await pending;
 assert.deepEqual(h.effects,[]);assert.equal(h.c.spyBars[0].sentinel,'original');
});
test('same-symbol repaint ownership is distinguished by paint sequence',async()=>{
 const h=painter(),wait=deferred();let calls=0;h.c.loadBench=()=>++calls===1?wait.promise:Promise.resolve(rows(200));
 const pending=h.c.paint(rows());const fresh=rows(0);await h.c.paint(fresh);const before=h.effects.slice();wait.resolve(rows(300));await pending;assert.deepEqual(h.effects,before);assert.equal(h.c.lastBars,fresh);
});
test('an obsolete relative-return result cannot add a series to the replacement chart',async()=>{
 const h=painter(),wait=deferred();h.c.mode='vsspy';h.c.vsSpy=()=>wait.promise;
 const pending=h.c.paint(rows());h.c.mode='ytd';h.c.active='NEW';h.c.loadGen++;await h.c.paint(rows(null));const before=h.effects.slice();wait.resolve([{time:1,value:999}]);await pending;
 assert.equal(h.adds.length,1);assert.deepEqual(h.effects,before);
});
test('scheduled view refreshes belong to their original paint',async()=>{
 const h=painter();await h.c.paint(rows());h.c.active='NEW';await h.c.paint(rows(null));h.effects.length=0;h.rafs.forEach(fn=>fn());assert.deepEqual(h.effects,[['refresh']]);
});
test('current paints still publish their complete tail after a usable or failed optional benchmark',async()=>{
 for(const fail of [false,true]){const h=painter(),frame=rows();h.c.loadBench=async()=>{if(fail)throw Error('invented failure');return rows(200);};await h.c.paint(frame);
  assert.equal(h.effects.find(x=>x[0]==='quoteUI')[1],frame);assert.equal(h.effects.filter(x=>x[0]==='writeState').length,1);assert.equal(h.c.lastBars,frame);}
});

function loader(names){
 const wait=deferred(),entered=deferred(),effects=[],frame=rows(),c=setup(names,{loadGen:0,active:'OLD',tf:'1d',lastGoodTf:'1d',barEvidence:new WeakMap(),lastBars:[],preserveView:false,layout:2,TABS:[],liveOn:true,replay:{on:false},barCache:{},resolveSym:s=>({ticker:s}),observationId:()=>false,
  klines:async()=>frame,paint:d=>{c.lastBars=d;entered.resolve();return wait.promise;}});
 for(const name of ['paintPanes','loadTape','loadDraw','renderTabs','renderTf','setWatch','showInfo','closeSymSearch','renderWtabs','renderList','toast','syncLivePill'])c[name]=(...args)=>effects.push([name,...args]);
 return {c,wait,entered,effects,frame};
}
test('load has no obsolete post-paint pane or tape side effects',async()=>{
 const h=loader(['load']),pending=h.c.load();await h.entered.promise;h.c.active='NEW';h.c.loadGen++;h.c.lastBars=rows(null);const before=h.effects.slice();h.wait.resolve();await pending;
 assert.deepEqual(h.effects,before);assert.equal(h.c.preserveView,false);
});
test('ticker loading cannot open an obsolete destination after a newer acquisition',async()=>{
 const h=loader(['loadTicker']),pending=h.c.loadTicker('FIRST','fin');await h.entered.promise;h.c.active='NEW';h.c.loadGen++;h.c.lastBars=rows(null);const before=h.effects.slice();h.wait.resolve();await pending;
 assert.deepEqual(h.effects,before);assert.equal(h.c.preserveView,false);
});
test('obsolete ticker paint errors cannot close the current search or replace its status',async()=>{
 const h=loader(['loadTicker']),pending=h.c.loadTicker('FIRST','fin');await h.entered.promise;h.c.loadGen++;const before=h.effects.slice();h.wait.reject(Error('invented stale rejection'));await pending;assert.deepEqual(h.effects,before);
});
test('live refresh does not initiate tape work after its frame was superseded',async()=>{
 const h=loader(['tickLive']),pending=h.c.tickLive();await h.entered.promise;h.c.active='NEW';h.c.loadGen++;h.wait.resolve();await pending;assert.deepEqual(h.effects,[]);
});
