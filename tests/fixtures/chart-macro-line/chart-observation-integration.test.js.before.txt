const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),core=require('../jh-observation-series.js');
const read=p=>fs.readFileSync(path.join(R,p),'utf8');
const dates=Array.from({length:40},(_,i)=>new Date(Date.UTC(2026,7,1+i)).toISOString().slice(0,10));
function fixture(){return {'/data/cryptoquant-series.json':{generated_at:'2026-10-01T00:00:00Z',series:{invented_metric:{d:dates,v:dates.map((x,i)=>i%7===0?null:i-18),unit:'invented_ratio',freq:'D'}},twins:{invented_metric:{d:['2010-01-01'],v:[999]}}},'/data/cryptoquant-onchain.json':{metrics:{}},'/data/cq-feed.json':{metrics:{}},'/data/cq-catalog.json':{catalog:{}},'/data/config/cryptoquant-spec.json':{metrics:[]},'/cq-universe.json':{rows:[]},'/data/ciss-stress.json':{series:[{id:'ea_exact',key:'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX',freq:'D',unit:'dimensionless_index',points:dates.map((d,i)=>[d,i%7===0?false:(i-18)/10])}]}};}
function setup(fuse=false,helper=true){
 const docs=fixture(),requests=[],c={setTimeout,clearTimeout};c.window=c;
 c.fetch=async url=>{assert.ok(Object.hasOwn(docs,url),url);requests.push(url);return {ok:true,json:async()=>structuredClone(docs[url])};};
 if(helper){vm.runInNewContext(read('jh-observation-series.js'),c);vm.runInNewContext(read('jh-observation-cache.js'),c);}
 if(fuse)vm.runInNewContext(read('jh-cq-fuse.js'),c);
 vm.runInNewContext(read('jh-chart-catalog.js'),c);return {c,docs,requests};
}
test('both complete modules keep exact primary history, missing values and signed measurements',async()=>{
 for(const withFuse of [false,true]){
  const h=setup(withFuse),r=await h.c.JHChartCatalog.klines('CQ:invented_metric');
  assert.equal(r.d.length,34);assert.ok(r.d.some(b=>b.close===0));assert.ok(r.d.some(b=>b.close<0));assert.ok(r.d.every(b=>b.volume===null));
  assert.equal(r.evidence.rejected_records,6);assert.equal(r.evidence.records.length,40);assert.equal(r.evidence.proxy_histories[0].joined,false);
  assert.deepEqual(JSON.parse(JSON.stringify(r.evidence.whole_packet)),h.docs['/data/cryptoquant-series.json']);
  assert.equal((await h.c.JHChartCatalog.klines('CQ:invented')).d.length,0);
  const ciss=await h.c.JHChartCatalog.klines('CISS:ea');assert.equal(ciss.d.length,34);assert.equal(ciss.evidence.records.length,40);
  assert.equal((await h.c.JHChartCatalog.klines('CISS:not-real')).d.length,0);
 }
});
test('missing shared parser is unavailable and cannot revive permissive legacy decoders',async()=>{
 for(const fuse of [false,true]){const h=setup(fuse,false);assert.equal((await h.c.JHChartCatalog.klines('CQ:invented_metric')).d.length,0);assert.equal((await h.c.JHChartCatalog.klines('CISS:ea')).d.length,0);assert.equal(h.requests.length,0);}
});
const parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
const engine=read('jh-chart-engine.js'),top=parser.exports.parse(engine,{ecmaVersion:'latest'}).body.find(x=>x.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const named=Object.fromEntries(top.filter(x=>x.type==='FunctionDeclaration').map(x=>[x.id.name,engine.slice(x.start,x.end)]));
function engineContext(catalog){
 const requests=[],elements=new Map(),lineData=[];
 const el=id=>{if(!elements.has(id))elements.set(id,{textContent:'',innerHTML:'',className:'',style:{}});return elements.get(id);};
 const c={Date,Math,Map,WeakMap,barEvidence:new WeakMap(),observationAxes:new WeakMap(),barCache:{},active:'CQ:invented_metric',tf:'1d',lastSource:'old',
  window:{JHChartCatalog:catalog},resolveSym:s=>({ticker:s,yahoo:s}),spec:id=>[id],warehouseSpec:()=>{throw Error('No market warehouse for observations');},fetchJson:async u=>{requests.push(u);throw Error('No market endpoint');},
  document:{getElementById:el},series:[],oscCharts:[],oscSeries:[],preserveView:false,ACC:'#ff9900',
  chart:{applyOptions(){},priceScale:()=>({applyOptions(){}}),addLineSeries:()=>({applyOptions(){},setData:d=>lineData.push(d)}),timeScale:()=>({fitContent(){},setVisibleLogicalRange(){}})},
  tape:{prints:[]},renderQR(){},paintMini(){},miniSeries:null,TFS:[['1d'],['1w'],['1M']],lastBars:[],lastVolShow:true,dwinOn:true};
 vm.createContext(c);vm.runInContext(['escHtml','medianGap','expectedGap','barsFitTf','uniq','resampleToTf','identifyBars','klines','observationId','scalarPanel','clearObservationFrame','observationText','observationAxisFormatter','bindObservationAxis','paintObservations','quoteUI','fillTape','lastPx','countdown','renderLegend','renderDwin','loadTape','paint','load','startReplay','renderCorr'].map(n=>named[n]).join('\n'),c);
 return {c,requests,elements,lineData,el};
}
test('failed or absent catalog never sends a scalar ID to market-price endpoints',async()=>{
 for(const catalog of [null,{klines:async()=>null},{klines:async()=>({d:[]})},{klines:async()=>{throw Error('invented source failure');}}]){
  const h=engineContext(catalog);assert.equal((await h.c.klines('CQ:invented_metric','1d')).length,0);assert.equal((await h.c.klines('CISS:unknown','1d')).length,0);assert.equal(h.requests.length,0);
 }
});
test('cache and aggregation retain full evidence, contributing ordinals and unavailable volume',async()=>{
 const doc=fixture()['/data/cryptoquant-series.json'],h=engineContext({klines:async s=>core.cq(doc,s)});
 const native=await h.c.klines('CQ:invented_metric','1d'),cached=await h.c.klines('CQ:invented_metric','1d');
 assert.deepEqual(cached,native);assert.equal(h.c.barEvidence.get(cached).observations.whole_packet,doc);
 const projected=h.c.resampleToTf(native,'1w');assert.ok(projected.length<native.length);assert.ok(projected.every(b=>b.volume===null));
 assert.deepEqual(Array.from(projected.flatMap(b=>b.observation_ordinals)).sort((a,b)=>a-b),Array.from(native.flatMap(b=>b.observation_ordinals)).sort((a,b)=>a-b));
});
test('scalar render preserves raw precision and signs, never rounds to market ticks or shows traded volume',async()=>{
 const doc=fixture()['/data/cryptoquant-series.json'];doc.series.invented_metric.v[1]=-0.0000000123456789;
 const h=engineContext({klines:async s=>core.cq(doc,s)}),bars=await h.c.klines('CQ:invented_metric','1d');h.c.lastBars=bars;
 h.c.paintObservations(bars,null);assert.equal(h.lineData[0][0].value,-0.0000000123456789);assert.equal(h.c.lastVolShow,false);
 assert.match(h.el('quote').textContent,/market OHLC, trade volume and release time unavailable/);
 assert.match(h.c.observationText(bars,bars[0].time),/-1.23456789e-8/);
 assert.equal(await h.c.lastPx('CQ:invented_metric'),null);await h.c.loadTape(true);h.c.fillTape(h.el('qr'));
 assert.match(h.el('qr').textContent,/unavailable for scalar/);assert.equal(h.requests.length,0);
 h.c.active='CISS:other';assert.match(h.c.observationText(bars),/unavailable for the selected/);
});
test('short monthly histories and pre-epoch scalar groups retain every contributing observation',async()=>{
 const doc={series:[{id:'old',freq:'M',points:[['0001-01',-2],['0001-02',0],['0001-03',1]]}]};
 const h=engineContext({klines:async s=>core.ciss(doc,s)}),bars=await h.c.klines('CISS:old','1d');
 assert.equal(bars.length,3);assert.equal(new Date(bars[0].time*1000).toISOString().slice(0,7),'0001-01');
 const grouped=h.c.resampleToTf(bars,'3M');assert.equal(grouped.length,1);
 assert.equal(new Date(grouped[0].time*1000).toISOString().slice(0,10),'0001-01-01');
 assert.deepEqual(Array.from(grouped[0].observation_ordinals),[0,1,2]);assert.equal(grouped[0].volume,null);
});
test('identically named CQ, CISS and equity symbols cannot share the scalar cache',async()=>{
 const cq={series:{X:{d:['2026-09-01'],v:[-5]}}},ciss={series:[{id:'X',points:[['2026-09-01',9]]}]};
 const h=engineContext({klines:async s=>s.startsWith('CQ:')?core.cq(cq,s):core.ciss(ciss,s)});
 h.c.resolveSym=s=>({ticker:'X',yahoo:'X'});h.c.barCache['X|1d']={at:Date.now(),d:Array(10).fill({close:123}),src:'equity'};
 assert.equal((await h.c.klines('CQ:X','1d'))[0].close,-5);assert.equal((await h.c.klines('CISS:X','1d'))[0].close,9);
 assert.equal((await h.c.klines('CQ:X','1d'))[0].close,-5);assert.equal(h.c.barCache['X|1d'].d[0].close,123);
});
test('unidentified scalar replay arrays and failed selections clear the previous chart frame',async()=>{
 const h=engineContext({klines:async()=>({d:[]})});let wipes=0,refreshes=0;
 Object.assign(h.c,{paintSeq:0,loadGen:0,replay:{on:true,full:[{time:1,close:99}],timer:123},
  clearInterval(){},wipe(){wipes++;},syncLivePill(){},toast(){throw Error('No market fallback or replay');}});
 h.c.window.JHStockDeskController={refresh(){refreshes++;}};
 await h.c.paint([{time:1,close:99}]);assert.equal(wipes,1);assert.equal(h.c.lastBars.length,0);assert.equal(h.c.window.jhChartEvidence,null);
 assert.equal(h.c.replay.on,false);assert.equal(h.c.replay.full.length,0);
 await h.c.load();assert.equal(wipes,3);assert.equal(refreshes,3);assert.match(h.el('quote').textContent,/no other series is substituted/);
 let message;h.c.toast=s=>{message=s;};h.c.startReplay();assert.match(message,/source availability times/);
 assert.equal(h.requests.length,0);
});

test('mixed-tab correlation cells with scalar IDs remain unavailable, including the diagonal',async()=>{
 const h=engineContext(null);let marketReads=0;
 Object.assign(h.c,{active:'SPY',TABS:['CQ:X','SPY'],loadGen:1,bare:s=>s,retsByTime:()=>[],alignedRets:()=>{throw Error('No scalar returns');},pearson:()=>{throw Error('No scalar correlation');},klines:async s=>{assert.equal(s,'SPY');marketReads++;return [];}});
 await h.c.renderCorr();assert.equal(marketReads,1);assert.equal((h.el('corr').innerHTML.match(/>1\.00</g)||[]).length,1);
 assert.equal((h.el('corr').innerHTML.match(/>—</g)||[]).length,3);
});
test('a delayed market correlation cannot overwrite a newly selected scalar view',async()=>{
 const h=engineContext(null);let resolve;
 Object.assign(h.c,{active:'SPY',TABS:['SPY'],loadGen:1,bare:s=>s,retsByTime:()=>[],klines:()=>new Promise(r=>{resolve=r;})});
 const pending=h.c.renderCorr();h.c.active='CQ:X';h.c.loadGen++;h.c.scalarPanel('corr');const message=h.el('corr').textContent;
 h.el('corr').innerHTML='selected scalar state';resolve([]);await pending;assert.equal(h.el('corr').innerHTML,'selected scalar state');assert.equal(h.el('corr').textContent,message);
});
