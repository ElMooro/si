const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),source=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'),p={exports:{}};
Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(p.exports,p);
const fn=p.exports.parse(source,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body.find(n=>n.type==='FunctionDeclaration'&&n.id.name==='obv');
const context={};vm.runInNewContext(source.slice(fn.start,fn.end),context);const obv=d=>Array.from(context.obv(d),x=>({...x}));
const rows=(closes,volumes)=>closes.map((close,i)=>({time:Date.UTC(2026,0,i+1)/1000,close,volume:volumes[i]}));
test('unchanged closes add no volume to the explicit zero anchor',()=>{assert.deepEqual(obv(rows([100,100,100],[100,100,100])).map(p=>p.value),[0,0,0]);});
test('mixed closes produce signed cumulative volume with the initial anchor unchanged',()=>{assert.deepEqual(obv(rows([100,101,101,99,99,102],[100,200,999,300,999,50])).map(p=>p.value),[0,200,200,-100,-100,-50]);});
test('measured zero volume and unchanged closes remain zero increments',()=>{assert.deepEqual(obv(rows([100,101,99,99],[100,0,0,100])).map(p=>p.value),[0,0,0,0]);});
test('broken volume evidence withholds the whole remaining cumulative chain',()=>{
 for(const bad of [undefined,null,false,true,'100','',[],{},-1,NaN,Infinity]){const d=rows([100,101,102,103,103],[100,100,bad,200,300]);const actual=obv(d);assert.deepEqual(actual.map(p=>p.time),d.map(b=>b.time));assert.deepEqual(actual.map(p=>p.value),[0,100,undefined,undefined,undefined]);}
});
test('invalid closes cannot borrow another direction or silently restart the sum',()=>{
 for(const bad of [null,undefined,false,'100',NaN,Infinity]){const d=rows([100,101,bad,103],[100,100,100,100]);assert.deepEqual(obv(d).map(p=>p.value),[0,100,undefined,undefined]);}
});
test('the first bar is an explicit zero anchor, not an inferred signed first volume',()=>{
 assert.deepEqual(obv(rows([100],[999999])).map(p=>p.value),[0]);assert.deepEqual(obv(rows([100,101],[null,100])).map(p=>p.value),[0,100]);
 assert.deepEqual(obv(rows([null,101],[100,100])).map(p=>p.value),[undefined,undefined]);assert.deepEqual(obv([]),[]);
});
test('nonfinite and absorbed nonzero cumulative increments remain unavailable',()=>{
 assert.deepEqual(obv(rows([100,101,102,103],[100,1e308,1e308,1])).map(p=>p.value),[0,1e308,undefined,undefined]);
 assert.deepEqual(obv(rows([100,101,102],[100,1e20,1])).map(p=>p.value),[0,1e20,undefined]);
});
test('malformed and unordered time coordinates cannot reach the drawing library',()=>{
 for(const v of [null,false,{},'x'])assert.deepEqual(obv(v),[]);
 for(const v of [null,'1',NaN,Infinity]){const d=rows([100,101],[100,100]);d[1].time=v;assert.deepEqual(obv(d),[]);}
 for(const delta of [0,-1]){const d=rows([100,101],[100,100]);d[1].time=d[0].time+delta;assert.deepEqual(obv(d),[]);}
 const d=rows([100,101],[100,100]);delete d[1];assert.deepEqual(obv(d),[]);
});
test('prefix values exclude future bars and integer examples reproduce an independent exact sum',()=>{
 const d=rows(Array.from({length:250},(_,i)=>100+(i*17)%11),Array.from({length:250},(_,i)=>(i*101)%503));const raw=JSON.stringify(d),actual=obv(d);let sum=0n;
 for(let i=0;i<d.length;i++){if(i){if(d[i].close>d[i-1].close)sum+=BigInt(d[i].volume);else if(d[i].close<d[i-1].close)sum-=BigInt(d[i].volume);}assert.equal(actual[i].value,Number(sum));assert.equal(obv(d.slice(0,i+1)).at(-1).value,Number(sum));}
 assert.equal(JSON.stringify(d),raw);
});
test('constant positive volume-unit scaling and price translation preserve signed flow meaning',()=>{
 const d=rows([100,101,101,99,102],[100,200,500,300,50]),a=obv(d);
 assert.deepEqual(obv(d.map(b=>({...b,volume:b.volume*1000}))).map(p=>p.value),a.map(p=>p.value*1000));
 assert.deepEqual(obv(d.map(b=>({...b,close:b.close-200}))),a);
});

test('stock-desk volume ratios also withhold nonzero numerators rounded to zero',()=>{
 const research=require('../jh-stock-desk-research.js'),d=rows(Array(21).fill(100),[...Array(20).fill(1e305),Number.MIN_VALUE]);
 for(const inclusive of [true,false]){const result=research.volumeRatio(d,inclusive);assert.equal(result.ratio,null);assert.equal(result.latest,Number.MIN_VALUE);assert.ok(result.mean>0);}
 d.at(-1).volume=0;assert.equal(research.volumeRatio(d,true).ratio,0);
});
