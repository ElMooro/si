const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),read=f=>fs.readFileSync(path.join(R,f),'utf8');
const T=d=>Date.UTC(...d.split('-').map((x,i)=>i===1?x-1:+x))/1000;
function load(file){const c={window:{},globalThis:{},console,Math,Date,JSON,isFinite,Number,String,Array,Object,RegExp,parseFloat,parseInt};c.window=c;c.globalThis=c;vm.createContext(c);vm.runInContext(read(file),c);return c;}
test('chart.html loads transforms before the engine and the auto-hide/import modules after the watchlist',()=>{
 const h=read('chart.html'),t=h.indexOf('/jh-chart-transforms.js?'),e=h.indexOf('/jh-chart-engine.js?v=20261002ac-shelves'),w=h.indexOf('/jh-tv-watchlist.js?'),a=h.indexOf('/jh-chart-autohide.js?'),i=h.indexOf('/jh-chart-import.js?');
 assert.ok(t>0&&e>t&&w>e&&a>w&&i>a);
});
test('calendar change modes use exact calendar bases and omit periods without one',()=>{
 const X=load('jh-chart-transforms.js').JHChartTransforms;
 const m=[];for(let i=0;i<26;i++){const y=2020+Math.floor(i/12),mo=i%12+1;if(i===14)continue;m.push({time:T(`${y}-${String(mo).padStart(2,'0')}-01`),value:100+i});}
 const yoy=X.change(m,'yoy');assert.equal(yoy[0].time,T('2021-01-01'));assert.ok(Math.abs(yoy[0].value-(112/100-1)*100)<1e-9);
 assert.ok(!yoy.some(p=>p.time===T('2022-03-01')),'base 2021-03 is missing so 2022-03 YoY is omitted');
 const mom=X.change(m,'mom');assert.ok(!mom.some(p=>p.time===T('2021-04-01')),'no prior month for the month after the gap');
 const d=X.change(m,'yoyd');assert.equal(d[0].value,12);
});
test('macro candles open at the prior observation and never invent highs or lows',()=>{
 const X=load('jh-chart-transforms.js').JHChartTransforms;
 const c=X.candles([{time:1,close:5},{time:2,close:7},{time:3,close:6}]);
 assert.deepEqual(JSON.parse(JSON.stringify(c.slice(-2).map(b=>[b.open,b.high,b.low,b.close]))),[[5,7,5,7],[7,7,6,6]]);
});
test('CSV/JSON import parses common layouts exactly',()=>{
 const I=load('jh-chart-import.js').JHChartImport;
 const t=I.parseCSV('Title\nDATE,CPIAUCSL,UNRATE\n2020-01-01,258.687,3.5\n2020-02-01,259.246,.\n2020-03-01,258.150,4.4\n'),d=I.detect(t),b=I.build(t,d);
 assert.equal(d.dateCol,0);assert.equal(b.freq,'M');assert.deepEqual(JSON.parse(JSON.stringify(b.series.map(s=>s.n))),[3,2]);
 const e=I.parseCSV('Datum;Wert\n01.02.2020;1,5\n13.02.2020;2,25\n'),de=I.detect(e);assert.equal(de.decimalComma,true);assert.deepEqual(JSON.parse(JSON.stringify(I.build(e,de).series[0].obs)),[['2020-02-01',1.5],['2020-02-13',2.25]]);
 const y=I.parseJSON(JSON.stringify({chart:{result:[{timestamp:[1704153600,1704240000,1704326400],indicators:{quote:[{open:[1,2,3],high:[2,3,4],low:[0.5,1,2],close:[1.5,2.5,3.5],volume:[10,20,30]}]}}]}})),dy=I.detect(y);
 assert.ok(dy.ohlc);assert.equal(I.build(y,dy).ohlc.length,3);
 for(const [s,day] of [['2020Q3','2020-07-01'],['Jan 2020','2020-01-01'],['2020M03','2020-03-01'],['1704153600','2024-01-02'],['20200105','2020-01-05']])assert.equal(I.parseDate(s,undefined,true).day,day,s);
});
test('watchlist routes TradingView ECONOMICS codes to the reviewed map and labels the equivalence',()=>{
 const s=read('jh-tv-watchlist.js');assert.match(s,/jh-econ-map\.js/);assert.match(s,/Mapped equivalent: /);assert.match(s,/TradingView feed not available/);
 assert.match(read('jh-chart-engine.js'),/window\.jhSetMode=function/);
});
