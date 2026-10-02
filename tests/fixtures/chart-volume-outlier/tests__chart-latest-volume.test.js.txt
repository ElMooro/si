const test=require('node:test'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-latest-volume'),current=path.join(R,'jh-chart-engine.js'),prior=path.join(D,'predecessor.js.txt');
const p={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(p.exports,p);
const body=raw=>p.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const functions=raw=>Object.fromEntries(body(raw).filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)]));
const hash=raw=>crypto.createHash('sha256').update(raw).digest('hex');
function setup(file){const raw=fs.readFileSync(file,'utf8'),named=functions(raw),els={quote:{},detail:{}};
 const c={active:'INVENTED',tf:'1D',mode:'candles',window:{},lastAdr:null,lastOrLv:null,lastVsSpx:null,lastVP:{poc:null},tape:{delta:0,prints:[]},observationId:()=>false,periodVwap:()=>[],twap:()=>[],atr:()=>[],escHtml:String,fmt:String,document:{getElementById:id=>els[id]||null}};
 vm.createContext(c);vm.runInContext(['reportedVolume','fmtVol','rvolAt','closeLocationVolume','fmtSignedVol','quoteUI'].map(n=>named[n]).join('\n'),c);return {c,els};}
const bars=volume=>Array.from({length:22},(_,i)=>({time:1577836800+i*86400,open:10,high:20,low:5,close:12,volume:i===21?volume:100}));
for(const [volume,label,ratio] of [[8000,'8.0K','80.00x'],[100,'100','1.00x'],[0,'0','0.00x'],[null,'Unavailable','Unavailable'],[undefined,'Unavailable','Unavailable'],[true,'Unavailable','Unavailable'],[false,'Unavailable','Unavailable'],['100','Unavailable','Unavailable'],[-1,'Unavailable','Unavailable'],[Infinity,'Unavailable','Unavailable'],[NaN,'Unavailable','Unavailable']])test('latest volume '+typeof volume+':'+String(volume)+' retains its clock and typed value',()=>{
 const h=setup(current),d=bars(volume),before=structuredClone(d);h.c.quoteUI(d);assert.deepEqual(d,before);assert.ok(h.els.quote.innerHTML.includes('>Vol '+label+' · 2020-01-22T00:00:00Z</span>'));assert.ok(h.els.quote.innerHTML.includes('>RVOL '+ratio));assert.ok(h.els.detail.innerHTML.includes('RVOL 20'));assert.ok(!h.els.quote.innerHTML.includes('same unit'));
});
for(const clock of [null,undefined,true,'1577836800',Infinity,NaN,1e100])test('invalid volume clock '+typeof clock+':'+String(clock)+' is unavailable',()=>{const h=setup(current),d=bars(100);d.at(-1).time=clock;h.c.quoteUI(d);assert.ok(h.els.quote.innerHTML.includes('Vol 100 · time unavailable'));});
for(const missing of [0,10,20])test('relative volume denominator preserves required bar '+missing,()=>{const h=setup(current),d=bars(8000);d[missing].volume=null;h.c.quoteUI(d);assert.ok(h.els.quote.innerHTML.includes(missing===0?'>RVOL 80.00x':'>RVOL Unavailable'));});
for(const volume of [8000,null])test('whole predecessor reproduces older-volume substitution for '+String(volume),()=>{const h=setup(prior);h.c.quoteUI(bars(volume));assert.ok(h.els.quote.innerHTML.includes('>Vol 100</span>'));assert.ok(h.els.quote.innerHTML.includes('>RVOL 1.00x'));});
test('only the two complete reviewed functions change and all outer statements survive',()=>{
 const old=fs.readFileSync(prior,'utf8'),raw=fs.readFileSync(current,'utf8'),transition=JSON.parse(fs.readFileSync(path.join(D,'transition.json'),'utf8'));
 assert.equal(hash(old),transition.source_sha256);assert.equal(hash(raw),transition.candidate_sha256);
 const a=functions(old),b=functions(raw);assert.deepEqual(Object.keys(a),Object.keys(b));assert.deepEqual(Object.keys(a).filter(k=>a[k]!==b[k]),transition.changes.map(x=>x.name));
 for(const change of transition.changes){assert.equal(a[change.name],change.before);assert.equal(b[change.name],change.after);}
 assert.equal(Object.keys(a).length-transition.changes.length,transition.unchanged_functions);
 const outer=x=>body(x).filter(n=>n.type!=='FunctionDeclaration').map(n=>x.slice(n.start,n.end));assert.deepEqual(outer(old),outer(raw));
});
test('prior complete preservation manifest changes only the two reviewed function hashes',()=>{
 const old=JSON.parse(fs.readFileSync(path.join(D,'source-delta-before.json'),'utf8')),actual=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-signed-volume/source-delta.json'),'utf8')),transition=JSON.parse(fs.readFileSync(path.join(D,'transition.json'),'utf8'));
 for(const change of transition.changes){const row=old.entries['jh-chart-engine.js'].changed.find(row=>row.name===change.name);assert.ok(row);assert.equal(row.sha256,hash(change.before));row.sha256=hash(change.after);}
 assert.deepEqual(actual,old);
});

test('paint race and cancellation assertions are unchanged while loading the real volume dependency',()=>{
 const old=fs.readFileSync(path.join(D,'paint-ownership-before.js.txt'),'utf8'),needle="const c=setup(['paint'],{paintSeq:0";assert.equal(old.split(needle).length,2);
 assert.equal(fs.readFileSync(path.join(R,'tests/chart-paint-ownership.test.js'),'utf8'),old.replace(needle,"const c=setup(['reportedVolume','paint'],{paintSeq:0"));
});
