const test=require('node:test'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict');
const R=fs.existsSync(path.join(__dirname,'../jh-chart-engine.js'))?path.resolve(__dirname,'..'):path.resolve(__dirname,'../../candidate');
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const raw=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),prior=fs.readFileSync(path.join(R,'tests/fixtures/volume-containment/engine-before.js.txt'),'utf8');
function funcs(s){const body=acorn.exports.parse(s,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;return Object.fromEntries(body.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,s.slice(n.start,n.end)]));}
const fn=funcs(raw),oldFn={...fn,...funcs(prior)};
const rows=()=>Array.from({length:80},(_,i)=>({time:1609459200+i*300,open:100,high:103,low:98,close:101+i/100,volume:[null,0,undefined,NaN,Infinity,-1,true,'5',100000,120][i%10]}));
function snapshot(d){return d.map(x=>({...x}));}
function setup({legacy=false}={}){
 const f=legacy?oldFn:fn,d=rows(),before=snapshot(d),bench=[],calls={wipes:0,quotes:0,clear:0,scalar:0},scripts=[],timers=[];
 const chart={timeScale:()=>({getVisibleLogicalRange:()=>null,fitContent(){},setVisibleLogicalRange(){}}),applyOptions(){},priceScale:()=>({applyOptions(){}}),addHistogramSeries:()=>({setData(){}}),addLineSeries:()=>({setData(){},applyOptions(){}})};
 const c={window:{lastBars:d,jhActive:'BTC-USD',tf:'5m',INDS:[],OSC:[]},paintSeq:0,active:'BTC-USD',tf:'5m',loadGen:1,barEvidence:new WeakMap(),resolveSym:()=>({engine:'yahoo'}),mode:'dod',lastBars:d,series:[],INDS:[],OSC:[],compare:[],tape:{prints:[]},COLORS:[],UP:'up',DN:'down',ACC:'#123',lastSource:'synthetic; unit unknown',volOn:false,watermark:false,chart,preserveView:false,lastSpxDaily:[],BARS:{dod:1},isFinite,console,localStorage:{getItem:()=>null,setItem(){}},document:{readyState:'loading',getElementById:()=>null,addEventListener(){},write(){},createElement:()=>({}),head:{appendChild:e=>scripts.push(e)}},miniSeries:null,oscCharts:[],oscSeries:[],replay:{on:false,timer:null,full:[]},setInterval:f=>(timers.push(f),timers.length),clearInterval(){},wipe(){calls.wipes++;c.series=[];},pal:()=>({}),chartLook:()=>({}),rightScaleMode:()=>0,requestAnimationFrame(){},loadBench:()=>new Promise(resolve=>bench.push(resolve)),fmtVol:String};
 for(const name of ['paintOsc','drawSVG','paintPat','renderDetail','checkAlerts','countdown','renderLegend','syncLivePill','renderTech','renderOver','renderSeason','renderFin','paintMini','writeState','bindObservationAxis','scalarPanel','toast'])c[name]=()=>{};
 c.quoteUI=()=>calls.quotes++;c.observationText=()=>'';
 vm.createContext(c);vm.runInContext(['identifyBars','sliceIdentifiedBars','repaintOwnedBars','reportedVolume','computeChange','observationId','clearObservationFrame','paintObservations','paint'].map(n=>f[n]).join('\n'),c);
 const realClear=c.clearObservationFrame;c.clearObservationFrame=(...args)=>{calls.clear++;return realClear(...args);};
 const realScalar=c.paintObservations;c.paintObservations=(...args)=>{calls.scalar++;return realScalar(...args);};
 c.identifyBars(d,'BTC-USD','5m','synthetic quantity unit unknown');
 // Execute the exact current initialization/publication statement; the complete
 // canonical paint executes the other publication itself on accepted input.
 const init=legacy?'try{ window.INDS=INDS; window.OSC=OSC; window.volOn=volOn; window.paint=paint; }catch(e){}':'try{ window.INDS=INDS; window.OSC=OSC; window.volOn=volOn; window.paint=repaintOwnedBars; }catch(e){}';
 assert.ok((legacy?prior:raw).includes(init));vm.runInContext(init,c);
 return {c,d,before,bench,calls,scripts,timers,flush(){bench.splice(0).forEach(resolve=>resolve([]));}};
}
async function settle(s){s.flush();for(let i=0;i<8;i++)await Promise.resolve();}
const triggers={
 'direct public paint':s=>s.c.window.paint(s.c.window.lastBars),
 'complete structure second 400ms callback':s=>{vm.runInContext(fs.readFileSync(R+'/jh-chart-structure.js','utf8'),s.c);assert.equal(s.timers.length,1);s.timers[0]();s.timers[0]();},
 'complete distribution dynamic BB helper onload':s=>{s.c.document.readyState='complete';vm.runInContext(fs.readFileSync(R+'/jh-chart-distribution.js','utf8'),s.c);assert.equal(s.scripts.length,2);s.scripts.find(e=>e.id==='jh-bb-src').onload();},
 'complete distribution UI toggle':s=>{vm.runInContext(fs.readFileSync(R+'/jh-chart-distribution.js','utf8'),s.c);s.c.window.jhDistToggle();}
};

// These synthetic fixtures supply explicit acquisition identity. Unknown quantities
// remain unknown; no test packet or coordinate is a provider/unit qualification.
for(const [entry,trigger]of Object.entries(triggers)){
 for(const [reason,mutate]of [['pending private timeframe',s=>s.c.tf='30m'],['pending private symbol',s=>s.c.active='ETH-USD'],['unidentified current array',s=>s.c.barEvidence.delete(s.d)],['public array differs from private lastBars',s=>s.c.lastBars=s.d.slice()]]){
  test(entry+' rejects '+reason+' before sequence/source effects',async()=>{
   const s=setup();mutate(s);const seq=s.c.paintSeq,last=s.c.lastBars;trigger(s);await settle(s);
   assert.equal(s.c.paintSeq,seq);assert.deepEqual(s.calls,{wipes:0,quotes:0,clear:0,scalar:0});assert.equal(s.c.lastBars,last);assert.deepEqual(s.d,s.before);
  });
 }
 test(entry+' retains one matching accepted current repaint',async()=>{
  const s=setup();trigger(s);await settle(s);assert.equal(s.c.paintSeq,1);assert.equal(s.calls.wipes,1);assert.equal(s.calls.quotes,1);assert.equal(s.c.window.paint,s.c.repaintOwnedBars);assert.deepEqual(s.d,s.before);
 });
 test(entry+' complete frozen predecessor reproduces pending-frame mislabel',async()=>{
  const old=setup({legacy:true});old.c.tf='30m';trigger(old);await settle(old);assert.equal(old.c.paintSeq,1);assert.equal(old.calls.quotes,1);assert.equal(old.c.window.tf,'30m');assert.equal(old.c.window.jhChartEvidence,null);assert.equal(old.d[1].time-old.d[0].time,300);assert.deepEqual(old.d,old.before);
 });
}
for(const [reason,mutate]of [['known wrong interval',s=>s.c.tf='30m'],['known wrong symbol',s=>s.c.active='ETH-USD']]){
 test('private canonical paint refuses '+reason+' before sequence',async()=>{
  const s=setup();mutate(s);const pending=s.c.paint(s.d);await settle(s);await pending;assert.equal(s.c.paintSeq,0);assert.equal(s.calls.wipes,0);assert.deepEqual(s.d,s.before);
 });
}
for(const kind of ['fresh acquisition','appended history','corrected interior history','replay prefix','replay full']){
 test('private canonical paint accepts matching '+kind+' without current-array inference',async()=>{
  const s=setup();let next=kind.startsWith('replay')?s.c.sliceIdentifiedBars(s.d,kind==='replay prefix'?37:undefined):rows();
  if(kind==='appended history')next.push({...next.at(-1),time:next.at(-1).time+300,volume:0});
  if(kind==='corrected interior history')next[22]={...next[22],volume:null};
  if(!kind.startsWith('replay'))s.c.identifyBars(next,s.c.active,s.c.tf,'synthetic accepted source; quantity unit unknown');
  assert.notEqual(next,s.c.lastBars);const saved=snapshot(next);assert.equal(s.c.window.paint(next),null,'Public current-array API is intentionally narrower than acquisition');
  const pending=s.c.paint(next);await settle(s);await pending;assert.equal(s.c.lastBars,next);assert.equal(s.c.window.jhChartEvidence,s.c.barEvidence.get(next));assert.equal(s.calls.quotes,1);assert.deepEqual(next,saved);assert.deepEqual(s.d,s.before);
  if(kind.startsWith('replay'))assert.equal(next[0],s.d[0]);
 });
}
test('refused invalid concurrent private call does not cancel accepted async frame',async()=>{
 const s=setup(),p=s.c.paint(s.d),before=s.c.paintSeq,wrong=rows();s.c.identifyBars(wrong,'ETH-USD','5m','synthetic');await s.c.paint(wrong);assert.equal(s.c.paintSeq,before);await settle(s);await p;assert.equal(s.calls.quotes,1);assert.equal(s.c.lastBars,s.d);
});
test('unidentified private compatibility stays admitted with null public evidence',async()=>{
 const s=setup();s.c.barEvidence.delete(s.d);const p=s.c.paint(s.d);await settle(s);await p;assert.equal(s.calls.quotes,1);assert.equal(s.c.window.jhChartEvidence,null);
});
for(const reason of ['unknown scalar evidence','known mismatched scalar evidence','identified empty scalar diagnostic']){
 test('original complete scalar clear path retains '+reason,async()=>{
  const s=setup();s.c.active='CQ:SYNTHETIC';s.c.tf='1d';let next=s.d;
  if(reason==='unknown scalar evidence')s.c.barEvidence.delete(next);
  if(reason==='identified empty scalar diagnostic'){next=[];s.c.identifyBars(next,s.c.active,s.c.tf,'synthetic',{contract:'chart-observations.v1',records:[],source:'synthetic'});}
  await s.c.paint(next);assert.equal(s.calls.clear,1);assert.equal(s.calls.wipes,1);assert.equal(s.c.lastBars.length,0);assert.equal(s.c.paintSeq,1);assert.deepEqual(s.d,s.before);
  if(reason==='identified empty scalar diagnostic')assert.equal(s.c.window.jhChartEvidence,s.c.barEvidence.get(next));else assert.equal(s.c.window.jhChartEvidence,null);
 });
}
test('original observation renderer retains non-scalar catalog observation metadata',async()=>{
 const s=setup();s.c.identifyBars(s.d,s.c.active,s.c.tf,'synthetic catalog',{contract:'chart-observations.v1',records:[],source:'synthetic'});await s.c.paint(s.d);assert.equal(s.calls.scalar,1);assert.equal(s.calls.clear,0);assert.equal(s.c.lastBars,s.d);assert.deepEqual(s.d,s.before);
});
