const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),p={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(p.exports,p);
function functions(raw){const nodes=p.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;return Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)]));}
const now=functions(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8')),old=functions(fs.readFileSync(path.join(__dirname,'fixtures/chart-market-volume/pre507/jh-chart-engine.js.txt'),'utf8'));
const basic=['reportedVolume','volumeTotal','volumeFields','completeVolumes','volumeMean','toBars','sanitizeBars','uniq','rvolAt','rvolSeries','resampleToTf','utcMidnight','asDaily','mergeByDay','fillCandleBodies','weekBars','toRange','roundBar','roundBars','roundTick','tickSize','tickPrec','tickFromBars','fmtVol','obv'];
function context(extra=[],prior=false){const f=prior?old:now,c={spec:id=>[id],periodKey:t=>new Date(t*1000).toISOString().slice(0,10),observationAxes:new WeakMap(),document:{getElementById:()=>null}};vm.createContext(c);vm.runInContext([...new Set(basic.concat(extra))].filter(n=>f[n]).map(n=>f[n]).join('\n'),c);return c;}
const serial=x=>JSON.parse(JSON.stringify(x)),rows=values=>values.map((volume,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i,high:102+i,low:99+i,close:101+i,volume}));

test('reported volume requires an explicit finite nonnegative number, including genuine zero',()=>{
 const c=context();for(const v of [0,1,.1,1e9,Number.MIN_VALUE,Number.MAX_VALUE])assert.equal(c.reportedVolume(v),v);
 for(const v of [null,undefined,true,false,'0','100','',[],{},NaN,Infinity,-Infinity,-1])assert.equal(c.reportedVolume(v),null);
});
test('explicit volume aliases must agree; a price-like value field cannot become volume',()=>{
 const c=context();for(const r of [{volume:0},{v:1},{vol:2},{Volume:3},{volume:10,v:10,vol:10,Volume:10}])assert.equal(c.volumeFields(r),Object.values(r)[0]);
 for(const r of [{},{value:999},{volume:null,v:1},{volume:1,v:2},{volume:false},{volume:'100'}])assert.equal(c.volumeFields(r),null);
});
test('all supported market packet shapes retain unknown and measured-zero volumes',()=>{
 const c=context(),bars=rows([null,0,100]),ts=bars.map(b=>b.time),prices=bars.map(b=>b.close),volumes=[null,0,100];
 const packets=[{bars},{ohlc:bars},{results:bars},{obs:bars},{points:bars},{data:bars},bars,bars.map(b=>[b.time,b.open,b.high,b.low,b.close,b.volume]),{timestamp:ts,close:prices,volume:volumes},{chart:{result:[{timestamp:ts,indicators:{quote:[{open:prices,high:prices,low:prices,close:prices,volume:volumes}]}}]}}];
 for(const packet of packets){const before=structuredClone(packet);assert.deepEqual(Array.from(c.toBars(packet),b=>b.volume),volumes);assert.deepEqual(packet,before);}
});
test('an absent Yahoo volume array does not discard otherwise usable prices',()=>{
 const c=context(),packet={chart:{result:[{timestamp:[1767225600,1767312000],indicators:{quote:[{close:[100,101],open:[99,100],high:[101,102],low:[98,99]}]}}]}};
 assert.deepEqual(Array.from(c.toBars(packet),b=>b.volume),[null,null]);assert.deepEqual(Array.from(c.toBars(packet),b=>b.close),[100,101]);
});
test('decoder-to-RVOL cannot classify absent, invalid or aliased price data as thin or extreme volume',()=>{
 const c=context(),whole=require('./fixtures/chart-market-volume/whole-decoder-predecessor.json');
 for(const entry of whole.cases){const packet=structuredClone(entry.whole_packet),before=structuredClone(packet),decoded=c.toBars(packet);assert.equal(c.rvolAt(decoded,40,20),entry.name==='measured-zero'?0:null,entry.name);assert.deepEqual(packet,before);}
});
test('same-day totals retain uncertainty and preserve genuine zeros',()=>{
 const c=context(),base=rows([0,100]);base[1].time=base[0].time+3600;assert.equal(c.asDaily(base)[0].volume,100);
 base[0].volume=null;assert.equal(c.asDaily(base)[0].volume,null);base[0].volume=100;base[1].volume=null;assert.equal(c.asDaily(base)[0].volume,null);
});
test('calendar grouping cannot heal missing volume and preserves every scalar ordinal',()=>{
 const c=context(),d=rows([100,null,0,100,100,100,100,100]);
 for(const tf of ['2d','3d','5d','1w','2w','1M','3M'])assert.ok(c.resampleToTf(d,tf).some(b=>b.volume===null),tf);
 const scalar=d.map((b,i)=>({...b,volume:null,observation_ordinals:[i]})),result=c.resampleToTf(scalar,'1w');assert.deepEqual(Array.from(result.flatMap(b=>b.observation_ordinals)).sort((a,b)=>a-b),d.map((b,i)=>i));assert.ok(result.every(b=>b.volume===null));
});
test('replacement rows and price-only transforms do not synthesize zero volume',()=>{
 const c=context(),d=rows([null,0,100]),flat=d.map(b=>({...b,open:b.close,high:b.close,low:b.close}));
 for(const result of [c.sanitizeBars(d),c.roundBars(d),c.fillCandleBodies(flat),c.mergeByDay([],d)])assert.deepEqual(Array.from(result,b=>b.volume),[null,0,100]);
 assert.equal(c.mergeByDay(rows([100]),rows([null]))[0].volume,null);
});
test('volume totals and means reject overflow, absorbed increments and division underflow',()=>{
 const c=context();for(const [a,b]of [[null,1],[1,null],[1e308,1e308],[1e308,1],[Number.MIN_VALUE,1e308]])assert.equal(c.volumeTotal(a,b),null);
 assert.equal(c.volumeTotal(0,0),0);assert.equal(c.volumeTotal(0,100),100);assert.equal(c.volumeTotal(100,0),100);
 assert.equal(c.volumeMean(rows([Number.MIN_VALUE,0]),0,1),null);assert.equal(c.volumeMean(rows([0,0]),0,1),0);assert.equal(c.volumeMean(rows([100,null]),0,1),null);
});
test('moving-average windows recover exactly when the missing observation leaves their inclusive window',()=>{
 const c=context(),d=rows(Array(65).fill(100));d[25].volume=null;
 assert.equal(c.volumeMean(d,5,24),100);for(let end=25;end<=44;end++)assert.equal(c.volumeMean(d,end-19,end),null);assert.equal(c.volumeMean(d,26,45),100);
});
test('invalid window indexes never coerce or borrow an alternate sample',()=>{
 const c=context(),d=rows([1,2,3]);for(const [from,to]of [[-1,1],[0,3],[2,1],[0.5,2],['0',2],[0,null]])assert.equal(c.volumeMean(d,from,to),null);
});
test('volume labels separate unavailable, zero, fractional quantity and signed derived quantities',()=>{
 const c=context();for(const v of [null,undefined,false,'0',NaN,Infinity])assert.equal(c.fmtVol(v),'Unavailable');assert.equal(c.fmtVol(0),'0');assert.equal(c.fmtVol(.1),'0.1');assert.equal(c.fmtVol(-.1),'-0.1');assert.equal(c.fmtVol(1000),'1.0K');
});
test('the OBV anchor remains explicit and missing subsequent volume breaks the cumulative chain',()=>{
 const c=context(),d=rows([null,100,null,100]),result=c.obv(d);assert.equal(result[0].value,0);assert.equal(result[1].value,100);assert.equal(result[2].value,undefined);assert.equal(result[3].value,undefined);
});
test('legacy volume-dependent overlays are explicitly unavailable for incomplete retained frames',()=>{
 const names=['vwma','vwap','cvd','mfi','adline','cmf','force','volOsc','eom','periodVwap','avwapFromAth','swingVwap'],c=context(names),d=rows(Array(80).fill(100));d[30].volume=null;
 for(const n of names)assert.deepEqual(serial(c[n](d,14,20)),n==='swingVwap'?{fromH:[],fromL:[]}:[],n);
});
test('complete-frame legacy arithmetic stays byte-for-byte numerically compatible',()=>{
 const names=['vwma','vwap','cvd','mfi','adline','cmf','force','volOsc','eom','periodVwap','avwapFromAth','swingVwap','ema','sma','swingPts','fractals'],a=context(names),b=context(names,true),d=rows(Array.from({length:80},(_,i)=>100+i));
 for(const name of names.slice(0,12))assert.deepEqual(serial(a[name](d,14,20)),serial(b[name](d,14,20)),name);
});
test('frame clearing disables redraw ownership before removeSeries callbacks can recreate an old profile',()=>{
 const c=context(['wipe']),elements=new Map([['hud',{textContent:'old',style:{display:'block'}}],['ohlc',{textContent:'old',style:{}}]]),removed=[];
 Object.assign(c,{document:{getElementById:id=>elements.get(id)},chart:{removeSeries(s){assert.equal(c.mainSeries,null);removed.push(s);}},mainSeries:{},volSeries:{},series:[{id:1},{id:2}],lastVP:{poc:999},lastHudVwap:[1],lastAtrPts:[1]});
 c.wipe();assert.equal(removed.length,2);assert.equal(c.lastVP.poc,null);assert.equal(c.lastHudVwap.length,0);assert.equal(c.lastAtrPts.length,0);assert.equal(elements.get('hud').textContent,'');assert.equal(elements.get('hud').style.display,'none');assert.equal(elements.get('ohlc').textContent,'');
});
