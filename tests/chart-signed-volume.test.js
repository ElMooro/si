const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const raw=fs.readFileSync(path.join(__dirname,'../jh-chart-engine.js'),'utf8'),p={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(p.exports,p);
const nodes=p.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body,named=Object.fromEntries(nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)]));
const c={};vm.createContext(c);vm.runInContext(['reportedVolume','fmtVol','closeLocationVolume','fmtSignedVol'].map(n=>named[n]).join('\n'),c);
const bar=(close,volume=100)=>({open:15,high:20,low:10,close,volume});
test('close-location weighted volume retains the sign at low, midpoint and high',()=>{
 for(const [close,expected] of [[10,-100],[12.5,-50],[15,0],[17.5,50],[20,100]])assert.equal(c.closeLocationVolume(bar(close)),expected);
});
test('estimate depends on close location and reported volume, not the open or previous close',()=>{
 for(const open of [10,15,20])assert.equal(c.closeLocationVolume({...bar(12.5),open}),-50);
});
test('zero-range bars are unavailable, including measured zero volume',()=>{
 for(const volume of [0,100])assert.equal(c.closeLocationVolume({high:10,low:10,close:10,volume}),null);
});
test('measured zero remains zero on a valid nonflat range',()=>{
 for(const close of [10,15,20])assert.equal(c.closeLocationVolume(bar(close,0)),0);
});
test('missing, typed and invalid volume is never treated as zero',()=>{
 for(const volume of [null,undefined,true,false,'100',[],{},NaN,Infinity,-1])assert.equal(c.closeLocationVolume({...bar(15),volume}),null);
});
test('prices must be finite primitive numbers inside an ordered range',()=>{
 for(const name of ['close','high','low'])for(const value of [null,undefined,true,'15',NaN,Infinity,-Infinity])assert.equal(c.closeLocationVolume({...bar(15),[name]:value}),null);
 for(const row of [bar(9),bar(21),{high:10,low:20,close:15,volume:100},null])assert.equal(c.closeLocationVolume(row),null);
});
test('negative price levels remain arithmetically supported without reinterpreting the measurement',()=>{
 assert.equal(c.closeLocationVolume({high:-10,low:-20,close:-17.5,volume:100}),-50);
});
test('overflowing price ranges and underflowing nonzero estimates remain unavailable',()=>{
 assert.equal(c.closeLocationVolume({high:1e308,low:-1e308,close:0,volume:100}),null);
 assert.equal(c.closeLocationVolume({high:4,low:0,close:1,volume:Number.MIN_VALUE}),null);
 assert.equal(c.closeLocationVolume({high:1,low:0,close:Number.MIN_VALUE,volume:100}),-100);
});
test('all ordinary complete nonflat frames preserve the preceding arithmetic',()=>{
 const whole=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/chart-signed-volume/whole-sign-predecessor.json'),'utf8'));
 for(const rows of Object.values(whole.whole_inputs))for(const b of rows){if(typeof b.volume!=='number'||b.high<=b.low)continue;const before=structuredClone(b);assert.equal(c.closeLocationVolume(b),((b.close-b.low)/(b.high-b.low)*2-1)*b.volume);assert.deepEqual(b,before);}
});
test('signed presentation retains a visible minus and the existing magnitude units',()=>{
 for(const [value,label] of [[-100,'-100'],[100,'+100'],[-1000,'-1.0K'],[1000,'+1.0K'],[-.1,'-0.1'],[.1,'+0.1'],[0,'0'],[-0,'0']])assert.equal(c.fmtSignedVol(value),label);
});
test('unavailable signed quantities do not gain a plus sign or numeric value',()=>{
 for(const value of [undefined,null,false,'-100',NaN,Infinity,-Infinity])assert.equal(c.fmtSignedVol(value),'Unavailable');
});
