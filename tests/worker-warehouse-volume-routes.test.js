const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),{pathToFileURL}=require('node:url');
const workerURL=pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'));
const bar=(i,volume)=>({time:Date.UTC(2020,0,i+1)/1000,open:100+i,high:102+i,low:99+i,close:101+i,volume});
async function run(route,rows){
 const original=global.fetch,calls=[],saved=global.caches;global.caches={default:{match:async()=>null,put:async()=>{}}};
 global.fetch=async input=>{const u=new URL(typeof input==='string'?input:input.url);calls.push(u.href);assert.equal(u.hostname,'invented.bank.test');return Response.json({warehouse_empty:false,warehouse_key:'invented/warehouse',source_span:'day',source_mult:1,bars:rows});};
 try{const worker=(await import(workerURL)).default,r=await worker.fetch(new Request('https://invented.worker.test'+route),{SYMDIR_URL:'https://invented.bank.test'},{waitUntil:()=>{}});return{status:r.status,packet:await r.json(),calls};}finally{global.fetch=original;global.caches=saved;}
}
for(const volume of [null,false,'100',0,100])test('actual Worker daily route preserves '+typeof volume+':'+String(volume),async()=>{const rows=[bar(0,volume),bar(1,100)],copy=structuredClone(rows),r=await run('/ohlc?ticker=INVENTED&span=day',rows);assert.equal(r.status,200);assert.equal(r.packet.bars[0].value,typeof volume==='number'?volume:null);assert.equal(r.packet.count,2);assert.equal(r.calls.length,1);assert.deepEqual(rows,copy);});
test('actual Worker monthly Yahoo route does not publish partial volume',async()=>{const r=await run('/yf-ohlc?symbol=INVENTED&range=max&interval=1mo',[bar(0,100),bar(1,null),bar(2,200)]);assert.equal(r.status,200);assert.equal(r.packet.count,1);assert.equal(r.packet.bars[0].value,null);assert.equal(r.packet.bars[0].close,103);assert.equal(r.calls.length,1);});
test('actual Worker alias conflicts remain unavailable on the wire',async()=>{const r=await run('/ohlc?ticker=INVENTED&span=day',[{...bar(0,100),value:200}]);assert.equal(r.status,200);assert.equal(r.packet.bars[0].value,null);assert.equal(r.calls.length,1);});
