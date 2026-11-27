// chart.html, 2026-11-23: every study closes, hides and shows, and stays that way.
// Khalid: "the indicators are good but some of them wont close and remove out of the chart and visible and invisible doesnt always work".
// What was wrong is in docs/audit/2026-11-23/chart-indicator-controls.md; each test here fails on the files as they were.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.join(__dirname,'..'),read=f=>fs.readFileSync(path.join(R,f),'utf8'),hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const engine=read('jh-chart-engine.js'),html=read('chart.html'),ledger=require('./helpers/chart-macro-line-preservation.cjs');
const parser={exports:{}};
Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
const nodes=parser.exports.parse(engine,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const named=Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,engine.slice(n.start,n.end)]));
const BARS=JSON.parse(read('tests/fixtures/campaign-scan-bars.json')).bars.NVDA.map(b=>({time:Math.round(b[0]/1000),open:b[1],high:b[2],low:b[3],close:b[4],volume:b[5]}));
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
const flush=async(n=12)=>{for(let i=0;i<n;i++)await Promise.resolve();};
function sandbox(names,extra={}){const c={window:{},JSON,Object,Math,Array,String,Number,parseFloat,...extra};vm.createContext(c);vm.runInContext(names.map(n=>{assert.ok(named[n],'engine function '+n);return named[n];}).join('\n'),c);return c;}

test('the byte ledger takes the change back out: engine and chart.html return to the files before it',()=>{
 // the Credit Before Equity button (2026-11-25) and the Risk Map button (2026-11-24) are later changes: peeled first, newest first
 const before=ledger.peelIndicatorControls(ledger.peelRiskMapEngine(ledger.peelCreditBoardEngine(ledger.peelMoneyFlowEngine(ledger.peelChartTabsEngine(ledger.peelHeldByFundsEngine(engine, 'jh-chart-engine.js'), 'jh-chart-engine.js'), 'jh-chart-engine.js'),'jh-chart-engine.js'),'jh-chart-engine.js'),'jh-chart-engine.js');
 assert.equal(hash(before),'dcefd93abba4c5523277b7342d8b29a22b59d789904c671f8ace098c07674913');
 assert.equal(hash(ledger.peelIndicatorControls(ledger.peelRiskMapHtml(ledger.peelCreditBoardHtml(ledger.peelMoneyFlowHtml(ledger.peelChartTabsHtml(ledger.peelHeldByFundsHtml(html, 'chart.html'), 'chart.html'), 'chart.html'),'chart.html'),'chart.html'),'chart.html')),'46a52f2ac304b333056d4b0a7b0ece2c5b91dbbab093e2864d896dfff450bbf2');
 assert.ok(!before.includes('installIndicatorControls'),'nothing of the change is left in the predecessor');
 // a browser holding the old engine fetches this one: the tag sits before &ldr, which older gates require at the end
 assert.match(html,/jh-chart-engine\.js\?v=[^"]*&sov=20261122&ind=20261123c&ldr=20261105"><\/script>/);
});

test('the eye hides what it could not: the VWAP NY line and the candle pattern labels',()=>{
 assert.ok(engine.includes('INDS.some(function(i){return i.id==="vwap"&&i.on&&!i.hide;})'),'VWAP NY is drawn only while VWAP is on and not hidden');
 assert.ok(!engine.includes('INDS.some(function(i){return i.id==="vwap"&&i.on;})'));
 assert.ok(engine.includes('if(patInd&&patInd.on&&!patInd.hide&&lastBars.length){'),'candle pattern labels are drawn only while the study is on and not hidden');
 assert.ok(!engine.includes('if(patInd&&patInd.on&&lastBars.length){'));
 // found by the live sweep (ops 6930): the studio file draws its RS / beta markers whenever that study is on
 assert.ok(engine.includes('if(window.jhRsReady && !INDS.some(function(i){ return i.id==="rs"&&i.hide; })){'),'the RS / beta markers are asked for only while that study is not hidden');
});

function painter(extra={}){
 const effects=[],adds=[],rafs=[],timers=[],elements=new Map(),el=id=>{if(!elements.has(id))elements.set(id,{textContent:'',style:{}});return elements.get(id);};
 const chart={timeScale:()=>({getVisibleLogicalRange:()=>null,fitContent(){},setVisibleLogicalRange(){}}),priceScale:()=>({applyOptions(){}}),applyOptions(){},addHistogramSeries:o=>{const s={options:o,setData:d=>{s.data=d;}};adds.push(s);return s;}};
 const c=sandbox(['reportedVolume','paint'],{toastT:42,clearTimeout(){},paintSeq:0,loadGen:0,active:'NVDA',tf:'1d',barEvidence:new WeakMap(),observationId:()=>false,lastBars:[],INDS:[],OSC:[],series:[],mode:'ytd',lastSource:'test',compare:[],COLORS:[],tape:{prints:[]},preserveView:false,volOn:true,volHide:false,watermark:true,UP:'up',DN:'down',chart,document:{getElementById:el},
  wipe(){c.series=[];},rightPriceFormatter:String,pal:()=>({}),volScore:()=>0,chartLook:()=>({}),rightScaleMode:()=>0,computeChange:d=>d.map(b=>({time:b.time,value:b.close})),requestAnimationFrame:f=>rafs.push(f),refreshHiLoVP(){},fmtVol:String,loadBench:async()=>BARS,lastSpxDaily:[],lastBenchName:'SPX',lastVsSpx:null,spyBars:[],
  setTimeout:f=>{timers.push(f);return timers.length;},...extra});
 for(const name of ['paintOsc','drawSVG','paintPat','quoteUI','renderDetail','checkAlerts','countdown','renderLegend','syncLivePill','renderTech','renderOver','renderSeason','renderFin','paintMini','writeState'])c[name]=()=>effects.push(name);
 return {c,effects,adds,timers};
}

test('Volume: the eye hides the bars without switching Volume off',async()=>{
 const shown=painter();await shown.c.paint(BARS);assert.equal(shown.c.lastVolShow,true);
 const hidden=painter({volHide:true});await hidden.c.paint(BARS);assert.equal(hidden.c.lastVolShow,false,'hidden: no volume band');assert.equal(hidden.c.volOn,true,'still switched on');
 const tape=painter({volOn:false,volHide:true,INDS:[{id:'voltape',on:true}]});await tape.c.paint(BARS);assert.equal(tape.c.lastVolShow,false,'the eye also wins while Volume Tape is on');
 const forced=painter({volOn:false,INDS:[{id:'voltape',on:true}]});await forced.c.paint(BARS);assert.equal(forced.c.lastVolShow,true,'Volume Tape still brings the bars with it, as before');
});

test('a slow benchmark no longer holds the panes and the legend back',async()=>{
 const h=painter(),wait=deferred();h.c.loadBench=()=>wait.promise;
 const pending=h.c.paint(BARS);await flush();
 assert.deepEqual(h.effects,[],'nothing yet: the benchmark may come from the cache within this tick');
 assert.equal(h.timers.length,1);h.timers[0]();await flush();
 assert.deepEqual(h.effects,['paintOsc','drawSVG','paintPat','renderLegend'],'panes and legend are drawn while the benchmark downloads');
 wait.resolve(BARS);await pending;
 assert.deepEqual(h.effects.slice(4,8),['paintOsc','drawSVG','paintPat','quoteUI'],'and drawn again, with the rest of the tail, when it lands');
 assert.equal(h.effects.filter(x=>x==='writeState').length,1);assert.equal(h.c.spyBars,BARS);
});

test('a benchmark already in hand is one pass, as before',async()=>{
 const h=painter();await h.c.paint(BARS);
 assert.equal(h.effects.filter(x=>x==='paintOsc').length,1);assert.equal(h.effects.filter(x=>x==='renderLegend').length,1);assert.equal(h.effects[0],'paintOsc');
});

test('a paint that was superseded while its benchmark downloaded draws nothing early or late',async()=>{
 const h=painter(),wait=deferred();let calls=0;h.c.loadBench=()=>++calls===1?wait.promise:Promise.resolve(BARS);
 const old=h.c.paint(BARS);await flush();h.c.active='AAPL';h.c.loadGen++;
 h.timers[0]();await flush();assert.deepEqual(h.effects,[],'the early pass belongs to the paint that asked for it');
 wait.reject(Error('late failure of a request nobody waits for'));await old;assert.deepEqual(h.effects,[]);
});

test('the retired studio store follows the saved layout, so a reload cannot bring a closed study back',()=>{
 const store={},localStorage={getItem:k=>k in store?store[k]:null,setItem:(k,v)=>{store[k]=v;}};
 const c=sandbox(['mirrorStudio'],{localStorage,INDS:[{id:'sma20',on:false},{id:'ema9',on:true},{id:'bb',on:false}]});
 c.mirrorStudio({inds:[]});assert.deepEqual(store,{},'a browser that never used the studio gets no store');
 store['jh-chart-ind-layout-v1']=JSON.stringify({sma20:{on:true,c:'#f23645'},ema9:{on:false,c:'#2962ff'},bb:{on:false,c:'#089981'},tape:{on:true,c:'#2962ff'},rs:{on:true,c:'#089981'}});
 c.mirrorStudio(null);
 let st=JSON.parse(store['jh-chart-ind-layout-v1']);
 assert.equal(st.sma20.on,false,'closed since the snapshot: stays closed');assert.equal(st.ema9.on,true,'added since the snapshot: stays');assert.equal(st.sma20.c,'#f23645','colours are left alone');
 assert.equal(st.tape.on,true,'a study the engine has not loaded yet is left alone on a save');
 c.mirrorStudio({inds:[{id:'ema9',on:true},{id:'rs',on:true}]});
 st=JSON.parse(store['jh-chart-ind-layout-v1']);
 assert.equal(st.tape.on,false,'at start-up the layout decides: not in it, not on');assert.equal(st.rs.on,true,'in it and on: on');
 assert.ok(/applySnap\(OSC, lay\.osc\); mirrorStudio\(lay\); \}/.test(engine),'run at start-up, before the studio applies its store');
 assert.ok(/inds:snap\(INDS\),osc:snap\(OSC\)\}\);\n    mirrorStudio\(null\);/.test(engine),'and on every save');
});

// a small page: a column holding the price chart (stage), other rows, and the pane area
function page(kinds,opt={}){
 const mk=(cls,h,attrs={})=>{const e={cls,style:{height:h==null?'':h+'px'},dataset:{},attrs,children:[],classList:{contains:c=>cls.split(' ').includes(c)},getAttribute:n=>attrs[n]??null};
  Object.defineProperty(e,'offsetHeight',{get:()=>e.style.height==='auto'?22:Math.round(parseFloat(e.style.height)||0)});e.getBoundingClientRect=()=>({height:e.offsetHeight});return e;};
 const stage=mk('stage',null),wrap=mk('wrap',null),other=mk('quote',opt.other??60),col={children:[other,stage,wrap],clientHeight:opt.height??950,style:{},dataset:opt.short?{jhShort:'1'}:{}};
 stage.parentNode=col;wrap.offsetWidth=1500;wrap.clientWidth=opt.bar?1492:1500;
 kinds.forEach(k=>{wrap.children.push(mk('pane-split',5));const p=mk('osc',k.h??240,{'data-oid':k.id});if(k.hid){p.dataset.hid='1';p.style.height='auto';}wrap.children.push(p);});
 if(opt.note)wrap.children.push(mk('osc-more',26));
 wrap.querySelectorAll=sel=>wrap.children.filter(e=>e.classList.contains(sel.slice(1)));
 const document={getElementById:id=>id==='oscwrap'?wrap:id==='stage'?stage:null};
 const c=sandbox(['fitPanes'],{document,OSC:kinds.map(k=>({id:k.id,on:true,h:k.user,externalPane:k.ext})),window:{getComputedStyle:()=>({display:'block',position:'static'})}});
 c.fitPanes();
 const panes=wrap.children.filter(e=>e.classList.contains('osc'));
 return {wrap,stage,col,panes,heights:panes.map(p=>p.offsetHeight),room:parseFloat(wrap.style.maxHeight),avail:col.clientHeight-other.offsetHeight};
}

test('indicator windows share the room: the price chart always keeps at least two fifths of the column',()=>{
 for(const n of [1,2,3,4,5,6]){
  const p=page(Array.from({length:n},(_,i)=>({id:'o'+i})));
  assert.ok(p.avail-p.room>=Math.max(220,Math.round(p.avail*0.4)),n+' windows: the price chart keeps '+(p.avail-p.room)+' of '+p.avail+' px');
  assert.equal(p.wrap.style.overflowY,'auto');p.heights.forEach(h=>assert.ok(h>=160&&h<=240,'a window is between 160 and 240 px: '+h));
 }
 const three=page([{id:'rsi'},{id:'macd'},{id:'stoch'}]);
 assert.deepEqual(three.heights,[173,173,173],'three fit: they share the room evenly');
 assert.ok(three.heights.reduce((a,b)=>a+b,0)+15<=three.room,'with nothing to scroll');
 const six=page(Array.from({length:6},(_,i)=>({id:'o'+i})));
 assert.deepEqual(six.heights,[160,160,160,160,160,160],'six do not: each keeps a readable 160 px and the area scrolls, every window in reach');
 assert.ok(six.heights.reduce((a,b)=>a+b,0)>six.room);
});

test('a height the user set is kept; a hidden window takes only its title row; an external window is not resized',()=>{
 const p=page([{id:'rsi',h:300,user:300},{id:'macd'},{id:'stoch',hid:true},{id:'effspread',h:220,ext:true}]);
 assert.equal(p.heights[0],300,'set by hand: untouched');assert.equal(p.heights[3],220,'drawn by its own file: untouched');assert.equal(p.heights[2],22,'hidden: its title row');
 assert.equal(p.heights[1],160,'the window still at its default takes what is left, never under 160 px');
 const none=page([]);assert.equal(none.wrap.style.maxHeight,'');assert.equal(none.stage.style.paddingRight,'');
 const bar=page(Array.from({length:6},(_,i)=>({id:'o'+i})),{bar:true});assert.equal(bar.stage.style.paddingRight,'8px','when the area scrolls, the price chart gives up the same 8 px so bars stay in line');
});

test('a window follows the price chart and never leads it; it carries every bar; hidden means folded away',()=>{
 assert.ok(engine.includes('if(!r || syncing || (c!==chart && oscCharts.indexOf(c)>=0)) return;'),'a new window no longer pushes its own default zoom onto the price chart');
 assert.ok(engine.includes('padS.setData((d||[]).map(function(b){ return {time:b.time}; }))'),'each window holds the price chart\'s bar times, so the same bar sits at the same place in both');
 assert.ok(/function afterPanes\(wrap\)\{[\s\S]*?x\.timeScale\(\)\.setVisibleLogicalRange\(mr\)/.test(named.afterPanes),'and takes the price chart\'s range when it is built');
 assert.ok(!engine.includes('host.style.opacity="0.12"'),'a hidden window is no longer a faint copy of itself');
 assert.ok(engine.includes('if(o.hide){ pane.dataset.hid="1"; pane.classList.add("osc-hid"); pane.style.height="auto";'));
 assert.ok(engine.includes('#oscwrap .osc-head .leg-ops{opacity:1}'),'the eye and the cross of a window are on screen without hovering');
 assert.ok(engine.includes('if(paneHeld){ paneLate=true; return; }'),'a window is not rebuilt while one of its buttons is held down');
});

test('a seventh oscillator is named with a way to close it, in place of being on and nowhere',()=>{
 const made=[],OSC=['rsi','macd','stoch','cci','mfi','obv','atr','ext'].map(id=>({id,n:id.toUpperCase(),on:true,externalPane:id==='ext'}));
 const calls=[],c=sandbox(['paneNote'],{OSC,escHtml:String,saveLay:()=>calls.push('save'),paint:()=>calls.push('paint'),lastBars:[1],document:{createElement:()=>{const e={className:'',innerHTML:'',onclick:null};made.push(e);return e;}}});
 const wrap={kids:[],appendChild(e){this.kids.push(e);}};
 c.paneNote(wrap);
 assert.equal(wrap.kids.length,1);assert.match(made[0].innerHTML,/Six indicator windows fit under the chart\. Switched on, not shown:/);assert.match(made[0].innerHTML,/data-more='atr'/);assert.ok(!/data-more='(obv|ext)'/.test(made[0].innerHTML),'only what is not drawn');
 made[0].onclick({target:{closest:()=>({getAttribute:()=>'atr'})}});
 assert.equal(OSC.find(o=>o.id==='atr').on,false);assert.deepEqual(calls,['save','paint']);
 const six={kids:[],appendChild(e){this.kids.push(e);}};OSC[6].on=false;c.paneNote(six);assert.equal(six.kids.length,0,'six or fewer: no note');
});

test('a column too short to share (a phone held upright): the price chart keeps 220 px and the column scrolls to the windows',()=>{
 const phone=page([{id:'rsi'},{id:'macd'}],{height:307,other:197});
 assert.equal(phone.avail,110,'what the page leaves the chart on a 390 x 844 screen');
 assert.equal(phone.stage.style.minHeight,'220px','the price chart had no height at all with a window open');assert.equal(phone.col.style.overflowY,'auto');
 assert.deepEqual(phone.heights,[240,240],'the windows keep their height');assert.equal(phone.wrap.style.maxHeight,'');
 const back=page([],{height:307,other:197,short:true});assert.equal(back.stage.style.minHeight,'');assert.equal(back.col.style.overflowY,'','with the windows closed the column is as it was');
});

// --- the two add-on files -------------------------------------------------------------------------------------------
function dom(){
 const mk=tag=>{const e={tag,children:[],style:{},dataset:{},attrs:{},listeners:{},className:'',textContent:'',parent:null,
  appendChild(k){k.parent=e;e.children.push(k);return k;},remove(){if(e.parent)e.parent.children=e.parent.children.filter(x=>x!==e);},replaceChildren(...k){e.children.forEach(x=>{x.parent=null;});e.children=[];k.forEach(x=>e.appendChild(x));},
  addEventListener(t,f){(e.listeners[t]=e.listeners[t]||[]).push(f);},click(){(e.listeners.click||[]).forEach(f=>f({preventDefault(){},stopPropagation(){}}));},setAttribute(k,v){e.attrs[k]=v;},getAttribute:k=>e.attrs[k]??null,
  classList:{toggle(){},contains:()=>false,add(){},remove(){}},
  all(){return e.children.flatMap(k=>[k,...k.all()]);},
  querySelector(sel){const m=/^\[data-k='(\w+)'\]$/.exec(sel);return e.all().find(k=>sel==='div'?k.tag==='div':sel==='.jh-desk'?k.className==='jh-desk':m?k.dataset.k===m[1]:false)||null;}};return e;};
 return {mk};
}
function auction(inds,saved){
 const {mk}=dom(),root=mk('div'),store={'jh-desk-pack-v2':JSON.stringify(saved||{avwap:0,vp:0,ref:0})},painted=[];
 const window={INDS:inds,lastBars:BARS,LightweightCharts:null,jhSaveLay(){},paint(){painted.push(1);}};
 const chart={chartElement:()=>root,timeScale:()=>({getVisibleRange:()=>null,subscribeVisibleLogicalRangeChange(){}})},series={priceToCoordinate:p=>p};
 window.jhDeskChart=chart;window.jhDeskSeries=series;
 const c={window,document:{createElement:mk},localStorage:{getItem:k=>store[k]??null,setItem:(k,v)=>{store[k]=v;}},setInterval(){},Date,JSON,Math,Infinity,isFinite};
 vm.createContext(c);vm.runInContext(read('jh-chart-auction.js'),c);
 const draw=()=>{window.jhDeskDraw();const host=root.querySelector('.jh-desk');return {host,lines:host.children.slice(1).length,chip:k=>host.querySelector("[data-k='"+k+"']")};};
 return {draw,store,window};
}

test('desk levels (Session VWAP bands, Volume profile, PDH / PWH / OR) leave the chart when their study is hidden',()=>{
 for(const [id,key] of [['avwap','avwap'],['vprof','vp'],['reflevels','ref']]){
  const inds=[{id:'avwap',n:'Session VWAP',on:0},{id:'vprof',n:'Volume profile',on:0},{id:'reflevels',n:'PDH / PWH / OR',on:0}],row=inds.find(i=>i.id===id),a=auction(inds);
  assert.equal(a.draw().lines,0,id+': off, nothing drawn');
  row.on=1;const on=a.draw();assert.ok(on.lines>=4,id+': on, '+on.lines+' pieces drawn');assert.equal(on.chip(key).style.color,'#C9942E');
  row.hide=true;const hid=a.draw();assert.equal(hid.lines,0,id+': on and hidden, nothing drawn');assert.equal(hid.chip(key).style.color,'#C9942E','its chip stays lit: the study is still on');
  row.hide=false;assert.equal(a.draw().lines,on.lines,id+': shown again, the same pieces');
 }
});

test('a desk chip switches its study with one press after the study was closed from the legend',()=>{
 const inds=[{id:'avwap',on:0},{id:'vprof',on:0},{id:'reflevels',on:0}],a=auction(inds),vp=inds[1];
 a.draw().chip('vp').click();assert.equal(vp.on,1,'chip: on');
 vp.on=false;                                   // the cross on the legend row
 const after=a.draw();assert.equal(JSON.parse(a.store['jh-desk-pack-v2']).vp,0,'the chip knows');assert.equal(after.chip('vp').style.color,'#787b86');
 after.chip('vp').click();assert.equal(vp.on,1,'one press switches it on again (it took two)');
});

function sdui(show,row){
 const store=show?{'jh-sd-show':JSON.stringify(show)}:{},painted=[],window={INDS:[row],lastBars:[1],paint(){painted.push(1);},localStorage:{getItem:k=>store[k]??null,setItem:(k,v)=>{store[k]=v;}}};
 const {mk}=dom(),document={readyState:'complete',head:mk('head'),getElementById:()=>null,createElement:mk,documentElement:mk('html')};
 const observers=[],c={window,document,setTimeout(){},MutationObserver:function(f){observers.push(f);this.observe=()=>{};},Date,JSON};
 vm.createContext(c);vm.runInContext(read('jh-chart-sd-ui.js'),c);
 let clock=1e6;c.Date={now:()=>(clock+=1000)};
 return {mutate:()=>observers.forEach(f=>f()),show:()=>JSON.parse(store['jh-sd-show']||'null'),window};
}

test('Supply & Demand: the cross on its legend row closes it while the Demand button is lit',()=>{
 const row={id:'sdmd',on:false,hide:false},s=sdui({demand:true,supply:false},row);
 assert.equal(row.on,true,'the buttons set the study when the page opens');
 row.on=false;row.hide=false;s.mutate();s.mutate();
 assert.equal(row.on,false,'closed from the legend: it stays closed (it was switched straight back on)');
 assert.deepEqual(s.show(),{demand:false,supply:false},'and both buttons go dark');
});

test('Supply & Demand: the Indicators list can switch it on; the buttons still work',()=>{
 const row={id:'sdmd',on:false,hide:false},s=sdui(null,row);
 row.on=true;s.mutate();
 assert.equal(row.on,true,'switched on from the list: it stays on (it was switched straight back off)');
 assert.deepEqual(s.show(),{demand:true,supply:true},'and both buttons light');
 s.window.jhToggleSupply();assert.deepEqual(s.show(),{demand:true,supply:false});assert.equal(row.on,true);
 s.window.jhToggleDemand();assert.deepEqual(s.show(),{demand:false,supply:false});assert.equal(row.on,false,'both buttons off: the study is off');
 row.hide=true;s.window.jhToggleDemand();assert.equal(row.on,true);assert.equal(row.hide,false,'a button brings a hidden study back in view');
});

test('Effort & Spread has an eye; hidden, its window folds to its title row instead of vanishing',()=>{
 const evr=read('jh-chart-evr.js');
 assert.ok(evr.includes('return !!(item && (item.on === 1 || item.on === true));'),'a hidden window still counts as on');
 assert.ok(!evr.includes('(item.on === 1 || item.on === true) && !item.hide'),'(it used to count as off: the window vanished and nothing on screen brought it back)');
 assert.ok(evr.includes("data-act=eye title='Visibility'"));
 assert.ok(evr.includes('if (hid) { pane.dataset.hid = "1"; pane.style.height = "auto"; pane.style.minHeight = "0"; }'));
 assert.ok(evr.includes('item.on = 0; item.hide = false;'),'closing it clears the hidden flag');
});

// ---- found by the live sweep on justhodl.ai (ops 6930, NVDA daily, 6,990 bars) ----

function controls(){
 // installIndicatorControls with a clock that only moves when the test moves it
 let now=0,seq=0;const timers=new Map(),inner=[],leg={},pane={},win={},doc={};
 const host=store=>({addEventListener:(type,fn)=>{store[type]=fn;}});
 const c=sandbox(['installIndicatorControls'],{setTimeout:(fn,ms)=>{timers.set(++seq,{at:now+(ms||0),fn});return seq;},clearTimeout:id=>{timers.delete(id);},
  document:{head:{appendChild(){}},createElement:()=>({}),getElementById:id=>id==='legend'?host(leg):id==='oscwrap'?host(pane):null,addEventListener:(type,fn)=>{doc[type]=fn;}},dressLegend(){},paintOsc(){inner.push('panes');},drawSVG(){}});
 c.window.addEventListener=(type,fn)=>{win[type]=fn;};
 vm.runInContext('var ctlInstalled=false,legendHeld=false,legendLate=null,paneHeld=false,paneLate=false,heldT=null,lastBars=[1];var renderLegend=function(at){record(at);};',Object.assign(c,{record:at=>inner.push(at)}));
 c.installIndicatorControls();
 const advance=ms=>{now+=ms;for(let guard=0;guard<50;guard++){const due=[...timers].filter(([,t])=>t.at<=now).sort((a,b)=>a[1].at-b[1].at)[0];if(!due)break;timers.delete(due[0]);due[1].fn();}};
 const button={target:{closest:sel=>sel==='button'?{}:null}};
 return {c,inner,leg,pane,win,doc,advance,button};
}

test('a legend button held down for four seconds is still held: the legend is not rebuilt under it (a 1.5 s timer used to let go first)',()=>{
 const h=controls();
 h.leg.pointerdown(h.button);h.advance(4000);
 h.c.renderLegend('while held');assert.deepEqual(h.inner,[],'no rebuild while the button is down - on a 6,990-bar chart a repaint alone takes longer than 1.5 s');
 h.win.pointerup();h.advance(0);
 assert.deepEqual(h.inner,['while held'],'the rebuild it was owed comes when the button is released, after the click');
 h.c.renderLegend('after');assert.deepEqual(h.inner,['while held','after']);
});

test('a window button the same; and a release the page never heard of ends the hold at the next pointer move with no button down',()=>{
 const h=controls();
 h.pane.pointerdown(h.button);h.advance(4000);
 assert.equal(vm.runInContext('paneHeld',h.c),true);
 h.doc.pointermove({buttons:1});h.advance(0);assert.equal(vm.runInContext('paneHeld',h.c),true,'moving with the button down is still a hold');
 h.doc.pointermove({buttons:0});h.advance(0);assert.equal(vm.runInContext('paneHeld',h.c),false,'the button is up: let go');
 h.leg.pointerdown(h.button);h.win.blur();h.advance(0);assert.equal(vm.runInContext('legendHeld',h.c),false,'so does the window losing focus');
 h.leg.pointerdown(h.button);h.advance(11000);assert.equal(vm.runInContext('legendHeld',h.c),true);
 h.advance(1500);assert.equal(vm.runInContext('legendHeld',h.c),false,'and, last of all, twelve seconds');
 h.leg.pointerdown({target:{closest:()=>null}});assert.equal(vm.runInContext('legendHeld',h.c),false,'a press on the legend that is not on a button holds nothing');
});

test('a study with a download of its own has its legend row, and the chart its zoom, before the download is asked for',()=>{
 for(const line of ['            legendFirst(); try{ await loadCalendar(); }catch(e3){}','              legendFirst(); var dv=await loadDiv(active);','              legendFirst(); var hv=await loadInstHarvest();',
  '          legendFirst(); try{ intraSrc=await klines(active, "5m", true); }catch(e5){ intraSrc=[]; }','        legendFirst(); try{ await loadCalendar(); }catch(eAv){}'])
  assert.equal(engine.split(line).length,2,'once, on the line of the download: '+line.trim());
 const at=engine.indexOf('    function legendFirst(){'),src=engine.slice(at,engine.indexOf('\n    }\n',at)+7);
 assert.ok(at>engine.indexOf('  async function paint(d){')&&at<engine.indexOf('wipe(); lastBars=d;'),'it belongs to the repaint and is defined before the chart is wiped');
 const run=(preserveView,saved)=>{const got=[];const c={preserveView,saved,legendDrawn:false,chart:{timeScale:()=>({setVisibleLogicalRange:r=>got.push(['zoom',r.from,r.to])})},renderLegend:()=>got.push(['legend'])};vm.createContext(c);vm.runInContext(src+'\nlegendFirst();legendFirst();',c);return got;};
 assert.deepEqual(run(true,{from:6830,to:6995}),[['zoom',6830,6995],['legend']],'the zoom the chart had, then the legend - once for a repaint however many downloads it makes');
 assert.deepEqual(run(false,{from:6830,to:6995}),[['legend']],'a new symbol has no zoom to keep');
 assert.deepEqual(run(true,null),[['legend']]);
});

