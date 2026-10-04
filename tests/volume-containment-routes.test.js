// All inputs are invented; the real complete Worker module and retained frontend
// functions execute. No request is forwarded and no stored application data is read.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto'),{pathToFileURL}=require('node:url');
const ROOT=path.resolve(__dirname,'..'),BASE=path.join(ROOT,'tests/fixtures/volume-containment/worker-before'),FIX=ROOT;
const sha=s=>crypto.createHash('sha256').update(s).digest('hex'),copy=x=>structuredClone(x),DAY=86400,T=1767225600;
const b=(i,v,c=100)=>({time:T+i*DAY,open:c*.998,high:c*1.003,low:c*.997,close:c,volume:v});
async function route(root,{volumes=[null,0,1,1000,30000],mode='yahoo',pathname='/ohlc',symbol='BTC-USD',interval='day',historyAdds=true,primaryUnit,historyUnit}={}){
 const saved={fetch:global.fetch,caches:global.caches,now:Date.now},calls=[];
 const primary=volumes.map((v,i)=>b(i,v,100+i)),hist=Array.from({length:volumes.length+(historyAdds?3:0)},(_,i)=>b(i-(historyAdds?3:0),1000,80+i));
 primary.forEach(row=>{if(primaryUnit!==undefined)row.volume_unit=primaryUnit;});
 const receipt={warehouse_empty:false,warehouse_key:symbol==='AAPL'?'invented/equity':'invented/crypto-bars/BTC.json',source_span:'day',source_mult:1,last_modified:'2026-01-06T00:00:00Z',volume_unit:primaryUnit,bars:primary};
 const yahoo={chart:{result:[{meta:{volume_unit:historyUnit},timestamp:hist.map(r=>r.time),indicators:{quote:[{open:hist.map(r=>r.open),high:hist.map(r=>r.high),low:hist.map(r=>r.low),close:hist.map(r=>r.close),volume:hist.map(r=>r.volume)}]}}]}};
 const binance=hist.map(r=>[r.time*1000,String(r.open),String(r.high),String(r.low),String(r.close),String(r.volume),(r.time+DAY)*1000-1,'900000000',2,'400','500','0']);
 const before=copy({receipt,yahoo,binance});
 Date.now=()=>Date.UTC(2026,0,7);global.caches={default:{match:async()=>null,put:async()=>{}}};
 global.fetch=async input=>{const u=new URL(typeof input==='string'?input:input.url);calls.push(u.href);
  if(u.hostname==='invented.bank.test')return Response.json(receipt);
  if(u.hostname==='query1.finance.yahoo.com')return Response.json(mode==='yahoo'?yahoo:{chart:{result:[]}});
  if(u.hostname==='api.binance.com')return Response.json(mode==='none'?[]:binance);
  throw Error('No real network permitted: '+u.hostname);
 };
 try{
  const worker=(await import(pathToFileURL(path.join(root,root===BASE?'index.js':'cloudflare/workers/justhodl-data-proxy/src/index.js')))).default;
  const url=pathname==='/ohlc'?`https://invented.worker.test/ohlc?ticker=${symbol}&span=${interval}&days=12000`:`https://invented.worker.test/yf-ohlc?symbol=${symbol}&interval=${interval==='week'?'1wk':'1d'}&range=max`;
  const response=await worker.fetch(new Request(url),{SYMDIR_URL:'https://invented.bank.test'},{waitUntil:()=>{}}),packet=await response.json();
  assert.deepEqual({receipt,yahoo,binance},before,'source fixture objects remain unchanged');
  return {status:response.status,packet,sourceHeader:response.headers.get('X-Source'),calls,primary,hist};
 }finally{global.fetch=saved.fetch;global.caches=saved.caches;Date.now=saved.now;}
}
test('retained current-source predecessor Worker replaces null, zero and both 20x-boundary quantities, preserving warehouse prices',async()=>{
 const r=await route(BASE);assert.equal(r.status,200);assert.equal(r.packet.bars.length,8);
 const overlap=r.packet.bars.slice(3);assert.deepEqual(overlap.map(r=>r.value),[1000,1000,1000,1000,1000]);
 assert.deepEqual(overlap.map(r=>r.close),r.primary.map(r=>r.close));
});
for(const mode of ['yahoo','binance'])for(const pathname of ['/ohlc','/yf-ohlc'])test(`current containment ${pathname}/${mode} retains primary values, OHLC, all history dates and source counts`,async()=>{
 const base=await route(BASE,{mode,pathname}),fixed=await route(FIX,{mode,pathname});assert.equal(fixed.status,200);
 assert.deepEqual(fixed.calls,base.calls);assert.deepEqual(fixed.packet.bars.map(r=>r.time),base.packet.bars.map(r=>r.time));
 assert.deepEqual(fixed.packet.bars.map(({time,open,high,low,close})=>({time,open,high,low,close})),base.packet.bars.map(({time,open,high,low,close})=>({time,open,high,low,close})));
 assert.deepEqual(fixed.packet.bars.slice(3).map(r=>r.value),fixed.primary.map(r=>r.volume));assert.deepEqual(fixed.packet.bars.slice(0,3).map(r=>r.value),[1000,1000,1000]);
 assert.equal(fixed.packet.source,'warehouse+'+mode);assert.equal(fixed.sourceHeader,fixed.packet.source);assert.equal(fixed.packet.history_source,mode);assert.equal(fixed.packet.history_n,8);assert.equal(fixed.packet.history_added_n,3);assert.equal(fixed.packet.warehouse_n,5);assert.equal(fixed.packet.yahoo_n,mode==='yahoo'?8:0);assert.equal(fixed.packet.binance_n,mode==='binance'?8:0);assert.equal(fixed.packet.last_modified,'2026-01-06T00:00:00Z');assert.equal(fixed.packet.warehouse_key,base.packet.warehouse_key);
 assert.match(fixed.packet.history_join_policy,/No volume unit is inferred or converted/);assert.match(fixed.packet.history_join_policy,/remain unverified/);
});
for(const value of [null,0,1,49,50,51,999,1000,19999,20000,20001,30000,1e-9,Number.MAX_VALUE])test('accepted primary source quantity survives '+String(value),async()=>{const r=await route(FIX,{volumes:[value,value]});assert.equal(r.packet.bars.at(-1).value,value);});
for(const options of [{mode:'none'},{symbol:'AAPL'},{interval:'week'},{historyAdds:false}])test('candidate keeps complete existing no-extension path '+JSON.stringify(options),async()=>{
 const base=await route(BASE,options),fixed=await route(FIX,options);assert.deepEqual(fixed,base);assert.equal(fixed.packet.source,'warehouse');
});
for(const [primaryUnit,historyUnit]of[[undefined,undefined],['invented-base-asset','invented-quote-asset'],['invented-same-unit','invented-same-unit']])for(const mode of['yahoo','binance'])test('native quantities stay unchanged for '+mode+' with unit annotations '+String(primaryUnit)+'/'+String(historyUnit),async()=>{
 const r=await route(FIX,{mode,primaryUnit,historyUnit,volumes:[null,0,1,50,20000,30000]});
 assert.deepEqual(r.packet.bars.slice(3).map(row=>row.value),[null,0,1,50,20000,30000]);
 assert.deepEqual(r.packet.bars.slice(0,3).map(row=>row.value),[1000,1000,1000]);
 assert.match(r.packet.history_join_policy,/No volume unit is inferred or converted/);assert.match(r.packet.history_join_policy,/remain unverified/);
 // Existing codecs drop supplied unit ancestry. This limitation is retained,
 // explicitly observed, and must not become a claim of unit transport repair.
 assert.ok(r.packet.bars.every(row=>row.volume_unit===undefined&&row.unit===undefined));
});
for(const value of[undefined,false,true,'12',-1])test('unavailable or invalid typed primary volume stays null instead of supplementary fill '+String(value),async()=>{
 const r=await route(FIX,{volumes:[value,value]});assert.deepEqual(r.packet.bars.slice(3).map(row=>row.value),[null,null]);assert.equal(r.packet.bars.at(-1).close,101);
});
