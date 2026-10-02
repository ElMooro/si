const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict'),test=require('node:test');
const W=__dirname,current=path.join(W,'../jh-chart-engine.js'),prior=path.join(W,'fixtures/chart-warehouse-volume/engine-before.js.txt');
const parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
function boot(file,packets=[]){
 const raw=fs.readFileSync(file,'utf8'),nodes=parser.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
 const fns=nodes.filter(n=>n.type==='FunctionDeclaration').map(n=>raw.slice(n.start,n.end)),tfs=nodes.find(n=>n.type==='VariableDeclaration'&&n.declarations.some(d=>d.id.name==='TFS'));
 const calls=[],c={window:{},barCache:{},barEvidence:new WeakMap(),lastSource:'',PROXY:'https://invented.proxy.test',LIVE:'https://invented.live.test'};vm.createContext(c);vm.runInContext(fns.join('\n')+'\n'+raw.slice(tfs.start,tfs.end),c);
 c.fetchJson=async url=>{calls.push(url);assert.ok(packets.length>=calls.length,'unexpected fixture request '+url);return structuredClone(packets[calls.length-1]);};
 return {c,calls};
}
const bar=(i,volume)=>({time:Date.UTC(2020,0,i+1)/1000,open:100+i*.1,high:102+i*.1,low:99+i*.1,close:101+i*.1,volume});
const aliases=['volume','v','vol','Volume'];
for(const alias of aliases)for(const value of [null,undefined,false,true,'100',-1,Infinity,NaN])test(alias+' invalid '+String(value)+' cannot fall back to warehouse value',()=>{
 const row={...bar(0,100),value:12345};delete row.volume;row[alias]=value;const packet={source:'warehouse',bars:[row]},copy=structuredClone(packet),h=boot(current);
 assert.equal(h.c.toBars(packet)[0].volume,null);assert.deepEqual(packet,copy);
 assert.equal(boot(prior).c.toBars(packet)[0].volume,12345);
});
for(const packetKey of [{source:'warehouse'},{source:'warehouse+yahoo',warehouse_key:'invented/crypto-bars/BTC'}])for(const volume of [0,100,12345])test('legacy warehouse OHLC volume '+volume+' remains valid even when equal to close '+JSON.stringify(packetKey),()=>{
 const row={...bar(0,100),close:100,value:volume};delete row.volume;
 assert.equal(boot(current).c.toBars({...packetKey,bars:[row]})[0].volume,volume);
});
test('conflicts remain unavailable; valid named volume wins; generic scalar is not volume',()=>{
 const h=boot(current),row={...bar(0,100),v:200,value:12345};assert.equal(h.c.toBars({source:'warehouse',bars:[row]})[0].volume,null);
 for(const v of [0,100])assert.equal(h.c.toBars({source:'warehouse',bars:[{...row,volume:v,v}]})[0].volume,v);
 const scalar={time:bar(0,100).time,value:100};assert.equal(h.c.toBars({source:'warehouse',bars:[scalar]})[0].volume,null);
 const plain={...bar(0,100),value:12345};delete plain.volume;assert.equal(h.c.toBars({source:'unknown',bars:[plain]})[0].volume,null);
});
for(const leading of [0,null])for(const spike of [8000,1000000])test('complete crypto path preserves leading '+leading+' and latest '+spike,async()=>{
 const primary=Array.from({length:40},(_,i)=>bar(i,i<3?leading:i===39?spike:100)),older=Array.from({length:48},(_,i)=>({...bar(i-8,50),close:90+i*.1,open:90+i*.1,high:92+i*.1,low:89+i*.1}));
 const packets=[{source:'warehouse',warehouse_key:'invented/crypto-bars/BTC.json',bars:primary},{source:'invented:yahoo',bars:older}],input=structuredClone(packets);
 for(const [file,fixed] of [[prior,false],[current,true]]){
  const h=boot(file,packets),out=await h.c.klines('BTCUSDT','1d',false);assert.equal(out.length,48);assert.equal(h.calls.length,2);assert.ok(h.calls[1].endsWith('&nowarehouse=1'));assert.equal(out.at(-1).volume,fixed?spike:null);
  for(let i=0;i<primary.length;i++){const result=out.find(r=>r.time===primary[i].time);assert.equal(result.close,fixed||i>=3?primary[i].close:older[i+8].close);assert.equal(result.volume,fixed?primary[i].volume:i<3?50:i===39?null:100);}
  assert.equal(h.c.barEvidence.get(out).symbol,'BTCUSDT');assert.equal(h.c.barEvidence.get(out).interval,'1d');
  const cached=await h.c.klines('BTCUSDT','1d',false);assert.equal(cached,out);assert.equal(h.calls.length,2);
 }
 assert.deepEqual(packets,input);
});
test('equity path and failed crypto supplement keep the primary frame intact',async()=>{
 const primary=Array.from({length:40},(_,i)=>bar(i,i===39?8000:100)),packet={source:'warehouse',warehouse_key:'invented/bank',bars:primary};
 for(const sym of ['AAPL','BTCUSDT']){const h=boot(current,[packet]);const out=await h.c.klines(sym,'1d',false);assert.equal(JSON.stringify(out),JSON.stringify(primary));assert.equal(h.calls.length,sym==='AAPL'?1:2);}
});
test('weekly crypto aggregation keeps zero and propagates missingness without restoring erased tails',async()=>{
 for(const leading of [0,null]){
  const primary=Array.from({length:80},(_,i)=>bar(i,i<3?leading:i===79?8000:100)),supplement=Array.from({length:80},(_,i)=>bar(i,10));
  const h=boot(current,[{warehouse_key:'invented/crypto-bars/BTC',bars:primary},{bars:supplement}]);const out=await h.c.klines('BTCUSDT','1w',false),expected=h.c.resampleToTf(primary,'1w');assert.equal(JSON.stringify(out),JSON.stringify(expected));assert.ok(out.at(-1).volume>=8000);assert.equal(out[0].volume,leading===null?null:200);
 }
});

test('whole predecessor, all 398 unrelated functions and outer bytes survive',()=>require('./helpers/chart-warehouse-volume-preservation.cjs').normalizeSource(fs.readFileSync(current,'utf8')));
test('prior assertions survive exact reviewed dependency and fixed-defect expectations',()=>{const root=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-warehouse-volume'),h=x=>require('node:crypto').createHash('sha256').update(x).digest('hex');for(const [file,r] of Object.entries(JSON.parse(fs.readFileSync(path.join(D,'test-transitions.json'),'utf8')))){const old=fs.readFileSync(path.join(root,r.before_path),'utf8');assert.equal(h(old),r.before_sha256);let expected=old;for(const e of r.replacements){assert.equal(expected.split(e.before).length,2);expected=expected.replace(e.before,e.after);}const actual=fs.readFileSync(path.join(root,file),'utf8');assert.equal(actual,expected);assert.equal(h(actual),r.after_sha256);}});
