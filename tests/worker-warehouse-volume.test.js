const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),{pathToFileURL}=require('node:url');
const currentURL=pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/warehouse-ohlc.js')),oldURL='data:text/javascript;base64,'+require('node:fs').readFileSync(path.join(__dirname,'fixtures/worker-warehouse-volume/before.mjs.txt')).toString('base64');
const b=(i,volume)=>({time:Date.UTC(2020,0,i+1)/1000,open:100+i,high:102+i,low:99+i,close:101+i,volume});
for(const value of [null,undefined,false,true,'',' ','100',-1,Infinity,NaN])test('warehouse typed missing/invalid quantity '+typeof value+':'+String(value),async()=>{
 const current=await import(currentURL),old=await import(oldURL),rows=[b(0,value)],copy=structuredClone(rows);
 assert.equal(current.normalizeBars(rows)[0].value,null);assert.deepEqual(rows,copy);
 assert.equal(old.normalizeBars(rows)[0].value,Number(value)||0);
 assert.equal(current.normalizeBars([[rows[0].time,100,102,99,101,value]])[0].value,null);
});
for(const name of ['volume','value','v','vol','Volume'])test('alias '+name+' keeps measured zero and reports conflict',async()=>{
 const h=await import(currentURL),row=b(0,100);delete row.volume;row[name]=0;assert.equal(h.normalizeBars([row])[0].value,0);
 const other=name==='volume'?'value':'volume';row[other]=100;assert.equal(h.normalizeBars([row])[0].value,null);row[other]=0;assert.equal(h.normalizeBars([row])[0].value,0);row[other]=null;assert.equal(h.normalizeBars([row])[0].value,null);
});
for(const bad of [null,undefined,false,'100',Infinity,-1])test('weekly aggregate with '+String(bad)+' is not a partial sum',async()=>{
 const h=await import(currentURL),rows=[b(0,100),b(1,bad),b(2,200)],bars=h.normalizeBars(rows),copy=structuredClone(bars),out=h.aggregateBars(bars,'week');assert.equal(out.length,1);assert.equal(out[0].value,null);assert.deepEqual(bars,copy);assert.equal(out[0].open,rows[0].open);assert.equal(out[0].close,rows[2].close);
});
test('zero adds, overflow and absorbed positive quantities do not produce plausible totals',async()=>{
 const h=await import(currentURL);
 for(const [a,bv,want] of [[0,0,0],[0,200,200],[100,200,300],[Number.MAX_VALUE,Number.MAX_VALUE,null],[1e30,1,null]])assert.equal(h.aggregateBars(h.normalizeBars([b(0,a),b(1,bv)]),'month')[0].value,want);
 assert.equal((await import(oldURL)).aggregateBars((await import(oldURL)).normalizeBars([b(0,100),b(1,null)]),'month')[0].value,100);
});
test('primary crypto row remains authoritative for all fields including unavailable volume',async()=>{
 const h=await import(currentURL),old=await import(oldURL);
 for(const volume of [null,undefined,false,'100',0,8000]){const primary=b(0,volume),older={...b(0,500),close:90},input=structuredClone([older,primary]);const out=h.mergeBarsPrefer([older],[primary]);assert.equal(out[0].close,101);assert.equal(out[0].value,typeof volume==='number'?volume:null);assert.deepEqual([older,primary],input);}
 assert.equal(old.mergeBarsPrefer([],[b(0,null)])[0].value,0);
});
test('Yahoo volume remains typed without changing complete price frames',async()=>{
 const h=await import(currentURL);
 for(const volume of [null,undefined,false,true,'100',0,100]){const input={chart:{result:[{timestamp:[1577836800],indicators:{quote:[{open:[100],high:[102],low:[99],close:[101],volume:[volume]}]}}]}},copy=structuredClone(input),out=h.yahooResultToBars(input);assert.equal(out[0].value,typeof volume==='number'?volume:null);assert.equal(out[0].close,101);assert.deepEqual(input,copy);}
});
for(const value of ['', ' ',null,undefined,true,false,'-1','0x10','1e3','Infinity','0.'+'.','0.'+'0'.repeat(400)+'1','9'.repeat(400)])test('Binance invalid decimal '+typeof value+':'+String(value).slice(0,20),async()=>{
 const h=await import(currentURL);const row=[1577836800000,'100','102','99','101',value,1577923199999,'700',2,'3','4','0'],copy=structuredClone(row);assert.equal(h.binanceKlinesToBars([row])[0].value,null);assert.deepEqual(row,copy);
});
for(const [value,expected] of [['0',0],['0.00000000',0],['001.2500',1.25],['148976.11427815',148976.11427815],[0,0],[100,100]])test('Binance documented ordinal 5 '+value,async()=>{const h=await import(currentURL),row=[1577836800000,'100','102','99','101',value,1577923199999,'999999999',2,'3','4','0'];assert.equal(h.binanceKlinesToBars([row])[0].value,expected);});
test('warehouse response preserves missingness through the actual monthly request',async()=>{
 const h=await import(currentURL),original=global.fetch,calls=[];
 global.fetch=async u=>{calls.push(String(u));assert.equal(new URL(u).hostname,'invented.bank.test');return Response.json({warehouse_empty:false,warehouse_key:'invented/warehouse',source_span:'day',source_mult:1,last_modified:'2020-01-04T00:00:00Z',bars:[b(0,100),b(1,null),b(2,200)]});};
 try{const out=await h.warehouseOHLC('https://invented.base.test','INVENTED','month',1,'https://invented.bank.test');assert.equal(calls.length,1);assert.equal(out.bars[0].value,null);assert.equal(out.bars[0].close,103);assert.equal(out.warehouse_key,'invented/warehouse');}finally{global.fetch=original;}
});
test('malformed rows do not crash otherwise usable warehouse frames',async()=>{const h=await import(currentURL),out=h.normalizeBars([null,false,7,'bad',b(0,100)]);assert.equal(out.length,1);assert.equal(out[0].value,100);});

test('whole predecessor and exact reviewed volume changes preserve every other byte',()=>{const fs=require('node:fs'),crypto=require('node:crypto'),D=path.join(__dirname,'fixtures/worker-warehouse-volume'),t=JSON.parse(fs.readFileSync(path.join(D,'transition.json'),'utf8')),old=fs.readFileSync(path.join(D,'before.mjs.txt'),'utf8'),hash=x=>crypto.createHash('sha256').update(x).digest('hex');assert.equal(hash(old),t.source_sha256);let expected=old;for(const e of t.replacements){assert.equal(expected.split(e.before).length,2);expected=expected.replace(e.before,e.after);}const current=fs.readFileSync(path.join(__dirname,'..',t.path),'utf8');assert.equal(current,expected);assert.equal(hash(current),t.candidate_sha256);});
