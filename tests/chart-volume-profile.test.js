const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),test=require('node:test');
const W=__dirname,R=path.join(W,'..'),D=path.join(W,'fixtures/chart-volume-profile'),acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
function functions(raw){const body=acorn.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;return Object.fromEntries(body.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)]));}
const prior=functions(fs.readFileSync(path.join(D,'engine-before.js.txt'),'utf8')),current=functions(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8'));
function run(fns){
 const canvas={style:{},width:0,height:0,getContext:()=>new Proxy({},{get:(o,k)=>Object.hasOwn(o,k)?o[k]:()=>{},set:(o,k,v)=>(o[k]=v,true)})};
 const lines={},context={console,Number,Math,Date,lastVP:{poc:null,vah:null,val:null},namedLines:lines,lastBars:[],lastVolShow:true,vpOn:true,hiLo:false,active:'QAONLY',tape:{src:'invented',prints:[]},ACC:'#000',pal:()=>({fg:'#fff'}),observationId:()=>false,chart:{timeScale:()=>({getVisibleLogicalRange:()=>null})},document:{getElementById:id=>id==='vp'?canvas:id==='chart'?{clientHeight:500}:null},mainSeries:{priceToCoordinate:n=>n},addPriceLine:(v,c,t,k)=>{lines[k]=v==null?null:{price:v,title:t};}};
 vm.createContext(context);vm.runInContext(['reportedVolume','completeVolumes','visibleSlice','drawVP','refreshHiLoVP'].map(n=>fns[n]).join('\n'),context);return context;
}
const rows=volume=>Array.from({length:61},(_,i)=>({time:Date.UTC(2026,0,i+1)/1000,open:100+i*.1,high:102+i*.1,low:99+i*.1,close:101+i*.1,volume}));
const plain=x=>JSON.parse(JSON.stringify(x));
test('whole ordinary finite profile arithmetic and input rows remain unchanged',()=>{
 for(const volume of [100,.000001,1e-300,1e100]){
  const before=run(prior),after=run(current),data=rows(volume),original=plain(data);before.drawVP(data);after.drawVP(data);
  assert.deepEqual(plain(after.lastVP),plain(before.lastVP));assert.deepEqual(data,original);assert.ok(after.lastVP.poc>0);
 }
});
test('zero-volume viewport cannot publish invented POC VAH or VAL',()=>{
 const before=run(prior),after=run(current);before.drawVP(rows(0));after.drawVP(rows(0));assert.ok(before.lastVP.poc>0);assert.deepEqual(plain(after.lastVP),{poc:null,vah:null,val:null});
 for(const name of ['POC','VAH','VAL'])assert.equal(after.namedLines[name],null);
});
test('a missing-volume viewport removes every old profile price line',()=>{
 const before=run(prior),after=run(current);for(const c of [before,after]){c.drawVP(rows(100));assert.ok(c.namedLines.POC);const data=rows(100);data[25].volume=null;c.drawVP(data);}
 assert.ok(before.namedLines.POC);for(const name of ['POC','VAH','VAL'])assert.equal(after.namedLines[name],null);assert.deepEqual(plain(after.lastVP),{poc:null,vah:null,val:null});
});
test('disable clears narrative state as well as drawn lines',()=>{
 const before=run(prior),after=run(current);for(const c of [before,after]){c.lastBars=rows(100);c.drawVP(c.lastBars);c.vpOn=false;c.refreshHiLoVP();}
 assert.ok(before.lastVP.poc>0);assert.deepEqual(plain(after.lastVP),{poc:null,vah:null,val:null});for(const name of ['POC','VAH','VAL'])assert.equal(after.namedLines[name],null);
});
test('infinite sums and invalid quantities withhold the profile after valid output',()=>{
 for(const volume of [Number.MAX_VALUE,NaN,Infinity,null,-1]){const c=run(current);c.drawVP(rows(100));c.drawVP(rows(volume));assert.deepEqual(plain(c.lastVP),{poc:null,vah:null,val:null});for(const name of ['POC','VAH','VAL'])assert.equal(c.namedLines[name],null);}
});
test('empty frames clear a previously valid profile',()=>{const c=run(current);c.drawVP(rows(100));c.drawVP([]);assert.deepEqual(plain(c.lastVP),{poc:null,vah:null,val:null});assert.equal(c.namedLines.POC,null);});

test('whole chart source preserves every byte outside five reviewed replacements',()=>require('./helpers/chart-volume-profile-preservation.cjs').normalizeSource(fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8')));
test('profile preservation rejects unrelated price arithmetic changes',()=>{const source=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8');assert.throws(()=>require('./helpers/chart-volume-profile-preservation.cjs').normalizeSource(source.replace('function rvolAt(d, i, n){','function rvolAt(d, i, n){return 1;')));});
test('all previous dock preservation assertions remain behind two explicit hooks',()=>{const crypto=require('node:crypto'),hash=x=>crypto.createHash('sha256').update(x).digest('hex'),t=JSON.parse(fs.readFileSync(path.join(D,'helper-transition.json'),'utf8'));let expected=fs.readFileSync(path.join(D,'docked-helper-before.cjs.txt'),'utf8');assert.equal(hash(expected),t.before_sha256);for(const e of t.replacements){assert.equal(expected.split(e.before).length,2);expected=expected.replace(e.before,e.after);}const actual=fs.readFileSync(path.join(R,t.path),'utf8');assert.equal(actual,expected);assert.equal(hash(actual),t.after_sha256);});
