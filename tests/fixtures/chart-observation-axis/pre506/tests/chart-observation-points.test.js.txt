const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),raw=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),parser={exports:{}};
Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
const nodes=parser.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const funcs=Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)])),core=require('../jh-observation-series.js');
const serial=x=>JSON.parse(JSON.stringify(x));
function setup(){
 const elements=new Map(),charts=[],removed=[];
 const element=id=>{if(!elements.has(id))elements.set(id,{id,textContent:'',innerHTML:'',className:'',style:{},width:0});return elements.get(id);};
 const make=()=>{const chart={series:[],priceScale:()=>({applyOptions(){}}),applyOptions(){},subscribeClick(fn){this.click=fn;},timeScale:()=>({fitContent(){},setVisibleLogicalRange(){}}),remove(){this.removed=true;},removeSeries(s){removed.push(s);chart.series=chart.series.filter(x=>x!==s);}};
  for(const type of ['Line','Area','Candlestick'])chart['add'+type+'Series']=options=>{const row={options,type,seriesType:()=>type,setData(data){this.data=serial(data);}};chart.series.push(row);return row;};charts.push(chart);return chart;};
 const context={active:'FRED:invented',tf:'1d',ACC:'#abc',barEvidence:new WeakMap(),series:[],oscCharts:[],oscSeries:[],lastVolShow:true,preserveView:false,lastBars:[],
  window:{},document:{getElementById:element},chart:make(),scalarPanel(){},observationId:s=>s.startsWith('FRED:'),miniOn:true,miniChart:null,miniSeries:null,pal:()=>({bg:'#fff'}),LW:{createChart:make},
  TABS:[],compare:[],layout:2,chart2:null,chart3:null,chart4:null,mkChart:make,bindSync(){},paneEls:()=>[['p2','c2'],['p3','c3'],['p4','c4']]};
 vm.createContext(context);vm.runInContext(['observationText','paintObservations','paintMini','paintPanes','resampleToTf','uniq'].map(n=>funcs[n]).join('\n'),context);
 return{c:context,charts,removed,element};
}
function points(options){assert.equal(options.lineVisible,false);assert.equal(options.pointMarkersVisible,true);assert.ok(options.pointMarkersRadius>0);assert.equal(options.priceLineVisible,false);}
test('scalar main view plots exact accepted observations without joining rejected periods',()=>{
 const p={id:'FRED:invented',unit:'invented',freq:'D',obs:[['2026-01-01',0],['2026-01-02',null],['2026-01-03',2],['2026-01-04',3],['2026-01-04',4],['2026-01-05',-1],['invalid',7]]};
 const original=JSON.stringify(p),result=core.warehouse(p,p.id),h=setup();h.c.barEvidence.set(result.d,{symbol:p.id,interval:'1d',observations:result.evidence});h.c.paintObservations(result.d,null);
 const plotted=h.c.chart.series[0];points(plotted.options);assert.deepEqual(plotted.data,result.d.map(b=>({time:b.time,value:b.close})));assert.deepEqual(plotted.data.map(p=>p.value),[0,2,-1]);assert.equal(result.evidence.records.length,7);assert.equal(result.evidence.rejected_records,4);assert.equal(JSON.stringify(p),original);assert.match(h.element('cd').textContent,/no interpolation/);
});
test('a single measured zero retains a visible scalar marker',()=>{
 const result=core.warehouse({id:'FRED:invented',obs:[['2026-01-01',0]]},'FRED:invented'),h=setup();h.c.barEvidence.set(result.d,{symbol:'FRED:invented',interval:'1d',observations:result.evidence});h.c.paintObservations(result.d,null);points(h.c.chart.series[0].options);assert.deepEqual(h.c.chart.series[0].data.map(p=>p.value),[0]);
});
test('navigator preserves market area style and switches scalar point types without retaining old geometry',()=>{
 const h=setup(),bars=[{time:1,close:-2},{time:3,close:0}];h.c.paintMini(bars);const first=h.c.miniSeries;points(first.options);assert.equal(first.type,'Line');
 h.c.paintMini(bars);assert.equal(h.c.miniSeries,first);h.c.active='SPY';h.c.paintMini(bars);assert.equal(h.c.miniSeries.type,'Area');assert.equal(h.c.miniSeries.options.topColor,'rgba(41,98,255,.25)');assert.equal(h.c.miniSeries.options.lineWidth,1);
 h.c.active='FRED:invented';h.c.paintMini(bars);points(h.c.miniSeries.options);assert.equal(h.c.miniSeries.type,'Line');assert.equal(h.removed.length,2);assert.equal(h.c.miniChart.series.length,1);assert.deepEqual(h.c.miniSeries.data,[{time:1,value:-2},{time:3,value:0}]);
});
test('single-observation split panes render points instead of dropping genuine short history',async()=>{
 const h=setup();h.c.active='SPY';h.c.TABS=['SPY','FRED:invented'];h.c.klines=async()=>[{time:1,close:0}];await h.c.paintPanes();assert.equal(h.c.chart2.series.length,1);points(h.c.chart2.series[0].options);assert.deepEqual(h.c.chart2.series[0].data,[{time:1,value:0}]);
});
test('split scalar points keep signed precision and omit no accepted value',async()=>{
 const h=setup(),d=[{time:1,close:-0.0000000123456789},{time:5,close:0},{time:9,close:2}];h.c.active='SPY';h.c.TABS=['SPY','FRED:invented'];h.c.klines=async()=>d;await h.c.paintPanes();points(h.c.chart2.series[0].options);assert.deepEqual(h.c.chart2.series[0].data,d.map(b=>({time:b.time,value:b.close})));
});
