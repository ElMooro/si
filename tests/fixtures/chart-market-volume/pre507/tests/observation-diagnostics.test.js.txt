const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),core=require('../jh-observation-series.js'),view=require('../jh-chart-stock-desk.js');
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const source=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8');
const top=acorn.exports.parse(source,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const named=Object.fromEntries(top.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,source.slice(n.start,n.end)]));
function setup(doc={series:{}},catalog){
 const elements=new Map(),requests=[],frames=[],el=id=>{if(!elements.has(id))elements.set(id,{textContent:'',style:{},className:'',innerHTML:''});return elements.get(id);};
 const c={Date,Math,Array,Map,WeakMap,setTimeout,clearTimeout,clearInterval(){},barEvidence:new WeakMap(),barCache:{},active:'CQ:x',tf:'1d',loadGen:0,paintSeq:0,
  lastBars:[],lastSource:'unavailable',lastVolShow:false,series:[],oscCharts:[],oscSeries:[],miniSeries:null,replay:{on:false,full:[],timer:null},
  chart:{applyOptions(value){c.lastChartOptions=value;}},window:{JHChartCatalog:catalog||{klines:async s=>core.cq(doc,s)},JHStockDeskController:{refresh(){frames.push(c.window.jhChartEvidence);}}},
  resolveSym:s=>({ticker:s,yahoo:s}),spec:id=>[id],document:{getElementById:el},wipe(){},syncLivePill(){},
  toast(){throw Error('No market fallback');},warehouseSpec(){throw Error('No market endpoint');},fetchJson:async u=>{requests.push(u);throw Error('No network');}};
 vm.createContext(c);vm.runInContext(['identifyBars','observationId','resampleToTf','klines','clearObservationFrame','observationText','paint','load'].map(n=>named[n]).join('\n'),c);
 return {c,frames,requests,el};
}
function scalar(dates){return dates.map((day,i)=>({time:Date.parse(day+'T00:00:00Z')/1000,open:i-1,high:i-1,low:i-1,close:i-1,volume:null,observation_ordinals:[i]}));}
function date(t){return new Date(t*1000).toISOString().slice(0,10);}
function model(e){return {kind:'scalar_observations',symbol:e.requested_id,observations:e,bars:[],valid:false};}
test('unknown exact identity survives load as inspectable empty evidence without substituting another series',async()=>{
 const doc={series:{unrelated:{d:['2026-01-01'],v:[99]}},whole_extra:{invented:true}},h=setup(doc);
 h.el('wm').textContent='OLD TICKER';await h.c.load();assert.equal(h.c.lastBars.length,0);const e=h.c.window.jhChartEvidence;
 assert.equal(e.bars,h.c.window.lastBars);assert.equal(e.symbol,'CQ:x');assert.equal(e.interval,'1d');assert.equal(e.observations.reason,'unknown_series_identity');
 assert.equal(e.observations.whole_packet,doc);assert.equal(e.observations.calls_eligible,false);assert.equal(h.requests.length,0);
 assert.match(h.el('quote').textContent,/no other series is substituted/);
 assert.equal(h.c.lastChartOptions.watermark.visible,false);
 assert.equal(h.el('wm').textContent,'');
 const html=view.markup(model(e.observations));assert.match(html,/No exact matching source series/);assert.match(html,/Unavailable valid source points/);assert.doesNotMatch(html,/undefined|0 valid source points/);
});
test('all-invalid received records remain complete in an identified diagnostic frame',async()=>{
 const doc={series:{x:{freq:'D',unit:'invented',d:['2026-01-01','not-date','2026-01-03'],v:[null,3,false]}}},h=setup(doc);
 await h.c.load();const e=h.c.window.jhChartEvidence.observations;
 assert.equal(e.records.length,3);assert.equal(e.rejected_records,3);assert.equal(e.plotted_points,0);assert.equal(e.whole_packet,doc);
 assert.match(view.markup(model(e)),/No source observations passed validation/);assert.match(view.markup(model(e)),/Inspect all 3 received source records/);
});
test('ambiguous and malformed source identities retain distinct reasons and do not invent counts',async()=>{
 for(const doc of [{series:{x:{},X:{}}},{series:{x:{d:'not-array',v:[]}}}]){
  const h=setup(doc);await h.c.load();const e=h.c.window.jhChartEvidence.observations;
  assert.ok(['ambiguous_series_identity','malformed_parallel_arrays'].includes(e.reason));assert.equal(e.whole_packet,doc);
  const html=view.markup(model(e));assert.match(html,/Unavailable valid source points/);assert.doesNotMatch(html,/undefined/);
 }
});
test('a parser result for another requested symbol cannot be relabeled for the active series',async()=>{
 const h=setup({}, {klines:async()=>core.cq({series:{}},'CQ:other')});await h.c.load();assert.equal(h.c.window.jhChartEvidence,null);assert.equal(h.requests.length,0);
});
test('late unavailable responses and failures cannot replace a newer symbol or interval',async()=>{
 for(const change of ['generation','symbol','interval'])for(const reject of [false,true]){
  let finish;const h=setup({}, {klines:()=>new Promise((resolve,rejectFn)=>{finish=reject?()=>rejectFn(Error('invented')):()=>resolve(core.cq({series:{}},'CQ:x'));})});
  const pending=h.c.load();if(change==='generation')h.c.loadGen++;if(change==='symbol')h.c.active='CQ:new';if(change==='interval')h.c.tf='2w';
  const current={symbol:'newer'};h.c.window.jhChartEvidence=current;h.c.lastSource='newer source';finish();await pending;assert.equal(h.c.window.jhChartEvidence,current);assert.equal(h.c.lastSource,'newer source');
 }
});
test('unidentified, mismatched and nonempty arrays cannot republish stale diagnostics',async()=>{
 const h=setup();for(const [sym,tf,bars]of [['CQ:other','1d',[]],['CQ:x','2w',[]],['CQ:x','1d',scalar(['2026-01-01'])]]){
  h.c.identifyBars(bars,sym,tf,'invented',core.cq({series:{}},'CQ:x').evidence);h.c.clearObservationFrame('unavailable',bars);assert.equal(h.c.window.jhChartEvidence,null);
 }
 const empty=await h.c.klines('CQ:x','1d');await h.c.paint(empty);assert.equal(h.c.window.jhChartEvidence.bars,empty);assert.equal(h.c.lastBars.length,0);
});
test('scalar fortnight groups are Monday anchored and never label earlier observations with a future start',()=>{
 const h=setup(),rows=scalar(['0001-01-01','1969-12-29','1969-12-30','1970-01-04','1970-01-05','1970-01-18','1970-01-19','2026-10-01']);
 const out=h.c.resampleToTf(rows,'2w');assert.ok(out.length<rows.length);
 for(const b of out){assert.equal(new Date(b.time*1000).getUTCDay(),1);for(const ordinal of b.observation_ordinals){assert.ok(b.time<=rows[ordinal].time);assert.ok(rows[ordinal].time<b.time+14*86400);}assert.equal(b.volume,null);}
 assert.equal(date(out.find(b=>b.observation_ordinals.includes(1)).time),'1969-12-22');assert.equal(date(out.find(b=>b.observation_ordinals.includes(4)).time),'1970-01-05');
 assert.deepEqual(Array.from(out.flatMap(b=>b.observation_ordinals)),rows.map((_,i)=>i));
});
test('single and sparse scalar observations follow the same declared calendar as dense input',()=>{
 // 2026-09-28 is exactly 20,720 days (1,480 fortnights) after 1970-01-05.
 const h=setup();for(const [tf,input,expected]of [['2w','2026-10-01','2026-09-28'],['1w','2026-10-01','2026-09-28'],['1M','2026-10-31','2026-10-01'],['3M','0001-02-28','0001-01-01']]){
  const one=h.c.resampleToTf(scalar([input]),tf);assert.equal(date(one[0].time),expected);assert.deepEqual(Array.from(one[0].observation_ordinals),[0]);
 }
 const sparse=h.c.resampleToTf(scalar(['2026-01-31','2026-06-30','2026-12-31']),'1M');assert.deepEqual(Array.from(sparse,b=>date(b.time)),['2026-01-01','2026-06-01','2026-12-01']);
});
test('scalar legend distinguishes last source period from the display group coordinate',async()=>{
 const doc={series:{x:{freq:'D',unit:'invented',d:['2026-09-28','2026-10-01'],v:[-2,0]}}},h=setup(doc);h.c.tf='2w';
 const bars=await h.c.klines('CQ:x','2w'),text=h.c.observationText(bars);
 assert.match(text,/last scalar from 2026-10-01/);assert.match(text,/display group 2026-09-28/);assert.match(text,/source periods 2026-09-28 → 2026-10-01/);assert.equal(bars[0].close,0);
});
test('calendar repair leaves all legacy market grouping outputs unchanged',()=>{
 const old=fs.readFileSync(path.join(__dirname,'fixtures/observation-diagnostics/pre501/jh-chart-engine.js.txt'),'utf8');
 const body=acorn.exports.parse(old,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
 const node=body.find(n=>n.type==='FunctionDeclaration'&&n.id.name==='resampleToTf');
 const previous={Date,Math,Array,spec:x=>[x],uniq:x=>x},current=setup().c;current.uniq=x=>x;vm.runInNewContext(old.slice(node.start,node.end),previous);
 const rows=Array.from({length:70},(_,i)=>({time:Date.UTC(2026,0,1+i)/1000,open:i+1,high:i+3,low:i,close:i+2,volume:i%5?i:0}));
 for(const tf of ['1d','2d','3d','5d','1w','2w','1M','3M'])assert.deepEqual(JSON.parse(JSON.stringify(current.resampleToTf(rows,tf))),JSON.parse(JSON.stringify(previous.resampleToTf(rows,tf))),tf);
});
test('prototype-like and markup category names are escaped plain labels in complete CQ modules',async()=>{
 const docs={'/data/cryptoquant-series.json':{series:{x:{d:['2026-01-01'],v:[0]}}},'/data/cryptoquant-onchain.json':{metrics:{x:{category:'__proto__'}}},'/data/cq-feed.json':{metrics:{}},'/data/cq-catalog.json':{catalog:{}},'/data/config/cryptoquant-spec.json':{metrics:[]},'/cq-universe.json':{rows:[]}};
 for(const label of ['__proto__','constructor','<img id="injected">']){
  docs['/data/cryptoquant-onchain.json'].metrics.x.category=label;
  const c={setTimeout,clearTimeout,AbortController,fetch:async u=>{assert.ok(Object.hasOwn(docs,u));return {ok:true,json:async()=>structuredClone(docs[u])};}};c.window=c;
  for(const file of ['jh-observation-series.js','jh-observation-cache.js','jh-cq-fuse.js'])vm.runInNewContext(fs.readFileSync(path.join(R,file),'utf8'),c);
  await c.JHCqFuse.load();const html=c.JHCqFuse.paneHTML({});assert.doesNotMatch(html,/\[object Object\]|function Object|<img id="injected">/);if(label.includes('<'))assert.match(html,/&lt;img/);
 }
});
