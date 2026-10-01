const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),source=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8');
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const nodes=acorn.exports.parse(source,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const fns=Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,source.slice(n.start,n.end)]));
const rows=v=>v.map((volume,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100,high:101,low:99,close:100,volume}));
function boot(){
 const elements=new Map(),el=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',style:{}});return elements.get(id);};
 const c={Number,Math,UP:'up-default',DN:'down-default',window:{},active:'INVENTED',tf:'1d',mode:'price',lastVP:{poc:null},lastAdr:null,lastOrLv:null,lastVsSpx:null,tape:{delta:0,prints:[]},
  observationId:()=>false,periodVwap:()=>[],twap:()=>[],atr:()=>[],fmt:x=>x===null?'Unavailable':String(x),fmtVol:x=>String(x),document:{getElementById:el}};
 vm.runInNewContext(['reportedVolume','rvolAt','rvolSeries','volCandlePaint','quoteUI'].map(n=>fns[n]).join('\n'),c);return {c,el};
}
test('RVOL excludes the current bar and requires exactly the preceding complete window',()=>{
 const {c}=boot(),d=rows([...Array(20).fill(100),1000]);assert.equal(c.rvolAt(d,20,20),10);assert.equal(c.rvolSeries(d,20).at(-1).value,10);
 assert.equal(c.rvolAt(d,19,20),null);assert.ok(c.rvolSeries(d.slice(0,20),20).every(p=>p.value===undefined));
});
test('a measured zero numerator survives; zero denominators never invent normal or spike ratios',()=>{
 const {c}=boot();assert.equal(c.rvolAt(rows([...Array(20).fill(100),0]),20,20),0);
 for(const end of [0,100]){const d=rows([...Array(20).fill(0),end]);assert.equal(c.rvolAt(d,20,20),null);assert.equal(c.rvolSeries(d,20).at(-1).value,undefined);}
 assert.equal(c.rvolAt(rows([0,...Array(19).fill(100),190]),20,20),2);
});
test('invalid operands withhold the exact window without dropping or borrowing earlier observations',()=>{
 const {c}=boot();for(const value of [null,undefined,false,true,'','100',[],{},-1,NaN,Infinity])for(const index of [4,20]){
  const d=rows(Array(21).fill(100));d[index].volume=value;assert.equal(c.rvolAt(d,20,20),null,String(value));
 }
 const d=rows(Array(25).fill(100));d[2].volume=null;assert.equal(c.rvolAt(d,22,20),null);assert.equal(c.rvolAt(d,23,20),1);
});
test('invalid lengths, indexes and arithmetic overflow remain unavailable',()=>{
 const {c}=boot(),d=rows(Array(25).fill(100));for(const n of [0,-1,1.5,'20',null,Infinity])assert.equal(c.rvolAt(d,20,n),null);
 for(const i of [-1,1.5,25,Infinity])assert.equal(c.rvolAt(d,i,20),null);
 assert.equal(c.rvolAt(rows([...Array(20).fill(1e308),100]),20,20),null);assert.equal(c.rvolAt(rows([...Array(20).fill(Number.MIN_VALUE),1e308]),20,20),null);
});
test('the complete plotted calendar retains unavailable positions instead of compressing the sample',()=>{
 const {c}=boot(),d=rows(Array(44).fill(100));d[21].volume=null;const series=c.rvolSeries(d,20);assert.equal(series.length,d.length);
 assert.deepEqual(Array.from(series,p=>p.time),d.map(b=>b.time));assert.equal(series[20].value,1);assert.equal(series[21].value,undefined);assert.equal(series[41].value,undefined);assert.equal(series[42].value,1);
});
test('prefix calculations exclude future rows and ratios are invariant to constant positive unit scaling',()=>{
 const {c}=boot(),d=rows(Array.from({length:80},(_,i)=>i%7?100+i:0));const before=JSON.stringify(d);
 for(let i=20;i<d.length;i++){const value=c.rvolAt(d,i,20);assert.equal(c.rvolAt(d.slice(0,i+1),i,20),value);assert.ok(Math.abs(c.rvolAt(d.map(b=>({...b,volume:b.volume*1000})),i,20)-value)<1e-12);}
 assert.equal(JSON.stringify(d),before);
});
test('unknown relative volume uses ordinary price direction styling, not the thin-volume bucket',()=>{
 const {c}=boot(),up=rows([100])[0],down={...up,close:99};for(const value of [null,undefined,NaN,Infinity,-1,'0']){assert.equal(c.volCandlePaint(up,value).color,'up-default');assert.equal(c.volCandlePaint(down,value).color,'down-default');}
 assert.notEqual(c.volCandlePaint(up,0).color,'up-default');
});
test('quote and detail labels preserve zero and explicitly identify insufficient history',()=>{
 const {c,el}=boot();c.quoteUI(rows([...Array(20).fill(100),0]));assert.match(el('quote').innerHTML,/RVOL 0\.00x/);assert.match(el('detail').innerHTML,/0\.00x/);
 c.quoteUI(rows(Array(20).fill(100)));assert.match(el('quote').innerHTML,/RVOL Unavailable/);assert.doesNotMatch(el('quote').innerHTML,/RVOL[^<]*thin/);
 c.quoteUI(rows([...Array(20).fill(100),1000]));assert.match(el('quote').innerHTML,/RVOL 10\.00x/);assert.match(el('quote').innerHTML,/preceding 20/);
});

test('unrepresentable ratios never become fabricated measured zero',()=>{
 const {c}=boot();assert.equal(c.rvolAt(rows([...Array(20).fill(1e305),Number.MIN_VALUE]),20),null);
 assert.equal(c.rvolAt(rows([...Array(20).fill(100),0]),20),0);
 assert.equal(c.rvolAt(rows([...Array(20).fill(100),100]),20),1);
});

test('malformed, duplicate and reversed times cannot reach the chart library',()=>{
 const {c}=boot();for(const value of [null,{},[],false])assert.deepEqual(Array.from(c.rvolSeries(value)),[]);
 for(const bad of [null,undefined,'1',NaN,Infinity]){const d=rows(Array(22).fill(100));d[3].time=bad;assert.equal(c.rvolSeries(d).length,0);}
 for(const delta of [0,-1]){const d=rows(Array(22).fill(100));d[3].time=d[2].time+delta;assert.equal(c.rvolSeries(d).length,0);}
 const d=rows(Array(22).fill(100));delete d[3];assert.equal(c.rvolAt(d,21),null);assert.equal(c.rvolSeries(d).length,0);
});

test('descriptive stock-desk prior mean agrees on ordinary retained numeric frames',()=>{
 const {c}=boot(),research=require('../jh-stock-desk-research.js');
 for(const volume of [0,100,1000]){const d=rows([...Array(20).fill(100),volume]);const result=research.volumeRatio(d,true);
  assert.equal(result.ratio,c.rvolAt(d,20));
 }
});
