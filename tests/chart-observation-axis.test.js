const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),raw=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),p={exports:{}};
Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(p.exports,p);
const nodes=p.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const functions=Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)]));
function context(){const c={observationAxes:new WeakMap()};vm.createContext(c);vm.runInContext(['observationAxisFormatter','bindObservationAxis','applyTheme','wipe'].map(n=>functions[n]).join('\n'),c);return c;}
function fmt(values,coordinate=x=>x*100){return context().observationAxisFormatter(values.map(close=>({close})),{priceToCoordinate:coordinate});}

test('generated scalar ticks remove binary noise without modifying exact source labels',()=>{
 const f=fmt([0,2,-1]);assert.deepEqual([1.4000000000000001,1.2000000000000002,-2.7755575615628914e-17,-0.39999999999999974].map(f),['1.4','1.2','0','-0.4']);assert.deepEqual([0,2,-1].map(f),['0','2','-1']);
});
test('all finite source measurements retain complete represented precision at any scale',()=>{
 const values=[Number.MIN_VALUE,-Number.MIN_VALUE,1.4000000000000001,1.2345678901234567e-8,1e12+0.001,Number.MAX_VALUE,0,-0];
 const f=fmt(values,()=>0);assert.deepEqual(values.map(f),values.map(String));
});
test('invalid scalar labels are unavailable without numeric coercion',()=>{
 const f=fmt([]);for(const value of [null,undefined,true,false,'0','',NaN,Infinity,-Infinity,{},[]])assert.equal(f(value),'');
});
test('pixel guard preserves distinguishable near-zero and tightly zoomed large ticks',()=>{
 assert.equal(fmt([],x=>x*1e20)(-2.7755575615628914e-17),'-2.77555756156e-17');
 assert.equal(fmt([],x=>x*1e32)(-2.7755575615628914e-17),'-2.7755575615628914e-17');
 assert.equal(fmt([],x=>(x-1e12)*1e6)(1e12+0.001),String(1e12+0.001));
});
test('exact 0.05-pixel differences and unavailable scale conversions cannot authorize rounding',()=>{
 for(const coord of [x=>x===0?0:0.05,()=>null,()=>NaN,()=>Infinity,()=>undefined,()=>{throw Error('removed');}])assert.equal(fmt([],coord)(1e-16),'1e-16');
});
test('each formatter snapshots its own source values and responds to scale changes',()=>{
 const c=context(),rows=[{close:1.4000000000000001}];let zoom=100;
 const f=c.observationAxisFormatter(rows,{priceToCoordinate:x=>x*zoom});rows[0].close=999;
 assert.equal(f(1.4000000000000001),'1.4000000000000001');assert.equal(f(-2.7755575615628914e-17),'0');zoom=1e32;assert.equal(f(-2.7755575615628914e-17),'-2.7755575615628914e-17');
});
test('pane binding gives chart and series one independent source formatter',()=>{
 const c=context(),make=()=>({applyOptions(o){this.options=o;}}),a=make(),b=make(),sa=make(),sb=make();sa.priceToCoordinate=sb.priceToCoordinate=x=>x*100;
 c.bindObservationAxis(a,sa,[{close:1.4000000000000001}]);c.bindObservationAxis(b,sb,[{close:0}]);
 assert.equal(a.options.localization.priceFormatter,sa.options.priceFormat.formatter);assert.equal(b.options.localization.priceFormatter,sb.options.priceFormat.formatter);
 assert.equal(c.observationAxes.get(a)(1.4000000000000001),'1.4000000000000001');assert.equal(c.observationAxes.get(b)(1.4000000000000001),'1.4');
});
test('theme keeps scalar units per pane and preserves the original market formatter',()=>{
 const c=context(),market=v=>'market '+v,scalar=v=>String(v),opts={localization:{priceFormatter:market,locale:'en-US'},rightPriceScale:{}};
 const make=()=>({applyOptions(o){this.options=o;},timeScale:()=>({getVisibleLogicalRange:()=>null}),priceScale:()=>({applyOptions(){}})});
 Object.assign(c,{document:{documentElement:{setAttribute(){}},querySelector:()=>null},dark:true,pal:()=>({bg:'#fff',border:'#111'}),lastVolShow:false,chart:make(),chart2:make(),chart3:null,chart4:null,oscCharts:[],mainSeries:null,invert:false,chartLook:()=>opts,lastBars:[],drawSVG(){},paintPat(){},saveLay(){}});
 c.observationAxes.set(c.chart2,scalar);c.applyTheme();assert.equal(c.chart.options.localization.priceFormatter,market);assert.equal(c.chart2.options.localization.priceFormatter,scalar);assert.equal(opts.localization.priceFormatter,market);assert.equal(c.chart2.options.localization.locale,'en-US');
});
test('wiping a scalar frame removes its formatter ownership before market reuse',()=>{
 const c=context(),chart={removeSeries(){}};Object.assign(c,{chart,series:[{}],mainSeries:{},volSeries:{}});c.observationAxes.set(chart,()=>String(1));c.wipe();assert.equal(c.observationAxes.has(chart),false);assert.equal(c.mainSeries,null);assert.equal(c.series.length,0);
});
