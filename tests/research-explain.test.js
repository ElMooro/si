const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const R=require('../jh-research-explain.js'),io=require('../jh-evidence-io.js');
const packet=data=>({status:'received',data});
test('actual Compound handler public packet reaches the consumer',()=>{
 const {execFileSync}=require('node:child_process');
 const output=JSON.parse(execFileSync(process.env.PYTHON||'python3',['-X','utf8','tests/compound_public_fixture.py'],{cwd:path.join(__dirname,'..'),encoding:'utf8'}));
 const model=R.crossModel({compound:packet(output)},'QAONLY')[0];
 assert.equal(model.matches.length,1);assert.equal(model.matches[0].pointer,'/compound/0');assert.equal(model.matches[0].value.compound_score,150);
});
test('canonical Compound empty, malformed and null never revive ranked alias',()=>{
 for(const value of [[],{},null]){
  const model=R.crossModel({compound:packet({compound:value,ranked:[{symbol:'OLD',compound_score:999}]})},'OLD')[0];
  assert.equal(model.matches.length,0);assert.equal(model.groups[0].key,'compound');
 }
 assert.equal(R.crossModel({compound:packet({ranked:[{symbol:'OLD',compound_score:0}]})},'OLD')[0].matches.length,1);
});
test('canonical ranker zero and complete duplicate records are retained',()=>{
 const rows=[{ticker:'Q',score:0},{ticker:'Q',score:13}],p={top_tickers:rows,ranked:[{ticker:'OLD'}],unranked_tickers:[{ticker:'Q',score:null,reasons:['missing']}]};
 const m=R.crossModel({master:packet(p)},'Q')[0];assert.equal(m.matches.length,3);assert.equal(m.matches[0].value,rows[0]);assert.equal(R.numberField(rows[0],'score').value,0);assert.equal(m.matches[2].collection,'unranked_tickers');
});
for(const canonical of [[],null,{},false])test('present canonical '+JSON.stringify(canonical)+' never restores stale alias',()=>{
 const m=R.select({top_tickers:canonical,ranked:[{ticker:'OLD'}]},['top_tickers','ranked'],'OLD');assert.deepEqual(m.matches,[]);assert.equal(m.key,'top_tickers');assert.equal(m.status,Array.isArray(canonical)?'available':'unavailable');
});
test('absent canonical retains documented legacy input and original pointers',()=>{
 const m=R.select({ranked:[{ticker:'Q'},null,{ticker:'Q'}]},['top_tickers','ranked'],'Q');assert.equal(m.status,'partial');assert.deepEqual(m.matches.map(x=>x.pointer),['/ranked/0','/ranked/2']);assert.deepEqual(m.malformed,['/ranked/1']);
});
test('ambiguous, blank and non-string identity cannot join; punctuation is preserved',()=>{
 for(const row of [{ticker:0},{ticker:''},{ticker:'Q',symbol:'X'},{symbol:null}])assert.equal(R.identity(row),null);
 assert.equal(R.identity({ticker:' brk.b ',symbol:'BRK.B'}),'BRK.B');
});
test('numeric fields never coerce missing, bool, string or nonfinite to zero',()=>{
 for(const score of [null,undefined,false,true,'0','',NaN,Infinity,-Infinity,Number.MAX_SAFE_INTEGER+1])assert.equal(R.numberField({compound_score:score,score:99},'compound_score',['score']).value,null);
 for(const score of [0,-2,1.25,1e-20])assert.equal(R.numberField({score},'compound_score',['score']).value,score);
});
test('ranker aggregate counts and best setups remain snapshots; no new event invented',()=>{
 const model=R.nowModel({master:packet({alerts:{n_tier_3:4}}),bestSetups:packet({top_setups:[{ticker:'Q',score:0}]})});
 assert.equal(model.events.length,0);assert.equal(model.snapshots.length,2);assert.equal(model.snapshots[0].pointer,'/alerts');assert.ok(model.diagnostics.some(x=>x.includes('aggregate counters')));
});
test('whole-packet snapshot uses the JSON pointer root, not an empty-key member',()=>{
 const value={regime:'invented','':{regime:'different'}};
 const model=R.nowModel({funding:packet(value)});assert.equal(model.snapshots[0].pointer,'');assert.equal(model.snapshots[0].value,value);
});
test('explicit event order does not borrow generated_at; missing clocks sort last',()=>{
 const a={reason:'undated'},b={event_at:'2026-01-02T00:00:00Z'},c={event_at:'2026-01-03T00:00:00Z'},bad={event_at:'2026-01-04'};
 const model=R.nowModel({compound:packet({new_alerts:[a,b,c,bad],generated_at:'2099-01-01T00:00:00Z'})});
 assert.deepEqual(model.events.map(x=>x.value),[c,b,a,bad]);assert.equal(model.events[2].time,null);assert.equal(model.events[3].time,null);
});
test('invalid calendar rollovers and missing zones cannot become dated events',()=>{
 for(const s of ['2026-02-30T00:00:00Z','2026-02-29T00:00:00Z','2026-01-01T24:00:00Z','2026-01-01T00:00:00','2026-01-01','',null])assert.equal(R.eventTime(s),null);
 assert.equal(R.eventTime('2024-02-29T01:00:00+01:00'),Date.parse('2024-02-29T00:00:00Z'));
});
test('research request records precede emptied legacy signal populations',()=>{
 for(const key of ['eps','revenue','microcap','pead']){const m=R.crossModel({[key]:packet({request_records:[{ticker:'Q',call:null}],all_qualifying:[]})},'Q')[0];assert.equal(m.matches.length,1);assert.equal(m.matches[0].pointer,'/request_records/0');}
 assert.equal(R.crossModel({nobrainers:packet({all_scored:[{ticker:'Q'}]})},'Q')[0].matches.length,1);
 assert.equal(R.crossModel({insiders:packet({transactions:[{ticker:'Q'}],sell_transactions:[{symbol:'Q'}],clusters:[],big_buys:[]})},'Q')[0].matches.length,2);
});
test('whole packet decoder rejects duplicate keys, underflow, unsafe integers and primitives',()=>{
 for(const text of ['{"x":1,"x":2}','{"x":1e-999}','{"x":1e999}','{"x":9007199254740993}','[]','null'])assert.throws(()=>R.checkedDecode(new TextEncoder().encode(text),io));
 const source=' { "x":0,"small":1e-200,"label":"1e-999 \\\" 9007199254740993","end":"COMPLETE" }\n';
 assert.equal(R.checkedDecode(new TextEncoder().encode(source),io).source,source);
});
test('one complete bounded request retains original and exact source options',async()=>{
 let calls=0;const source=' {"top_tickers":[{"ticker":"Q","score":0}],"end":"COMPLETE"}\n';
 const received=await R.receive('https://example.invalid/data/test.json',{io,fetcher:async(url,options)=>{calls++;assert.equal(new URL(url).searchParams.get('nogen'),'1');assert.equal(new URL(url).searchParams.get('exact'),'1');assert.equal(options.cache,'no-store');return new Response(source);}});
 assert.equal(calls,1);assert.equal(received.status,'received');assert.equal(received.source,source);assert.deepEqual(received.raw,new TextEncoder().encode(source));assert.equal(received.data.top_tickers[0].score,0);
});
test('access denial never retries a proxy or becomes a quiet/empty result',async()=>{
 let calls=0;const receipt=await R.receive('https://example.invalid/x',{io,fetcher:async()=>{calls++;return new Response('denied',{status:401});}});
 assert.equal(calls,1);assert.equal(receipt.status,'unavailable');assert.equal(receipt.http_status,401);assert.equal(receipt.data,null);assert.match(R.receivedSummary({x:receipt}),/0\/1 packets received.*1 unavailable/);
});
test('malformed original stays downloadable; no parsed records leak out',async()=>{
 const source='{"score":1e-999}';const receipt=await R.receive('https://example.invalid/x',{io,fetcher:async()=>new Response(source)});
 assert.equal(receipt.status,'unavailable');assert.equal(receipt.source,source);assert.equal(receipt.data,null);assert.ok(receipt.raw);assert.match(receipt.reason,/underflow/);
});
test('stalled body aborts and oversized body never exposes a partial result',async()=>{
 let cancelled=false;
 const slow=await R.receive('https://example.invalid/x',{io,timeoutMs:20,fetcher:async()=>new Response(new ReadableStream({cancel(){cancelled=true;}}))});
 assert.equal(slow.status,'unavailable');assert.match(slow.reason,/timed out/);assert.ok(cancelled);
 const large=await R.receive('https://example.invalid/x',{io,fetcher:async()=>new Response(' '.repeat(io.LIMIT+1))});assert.equal(large.status,'unavailable');assert.equal(large.raw,null);
});
test('all original companion references and explicit momentum abstention remain',()=>{
 for(const name of ['why-now.html','why-cross-signal.html']){
  const base=fs.readFileSync(path.join(__dirname,'fixtures/research-explain-predecessor',name),'utf8'),now=fs.readFileSync(path.join(__dirname,'..',name),'utf8');
  const paths=[...base.matchAll(/(?:\/)?(?:data\/[a-z0-9-]+|flow-data)\.json/g)].map(x=>x[0].replace(/^\//,''));
  for(const p of new Set(paths))assert.ok(now.includes(p),p);
 }
 const cross=fs.readFileSync(path.join(__dirname,'../why-cross-signal.html'),'utf8');assert.match(cross,/k === "momentum" \? \{status:"research_only_abstain",investment_votes:0\}/);
});
test('retained whole predecessors bind the actually reproduced defects',()=>{
 const hashes={'why-now.html':'9b88aaf4cdf90e1e6c3b5bce4d0edf3be5fa94993aa6cb00d635f74e0f30a54f','why-cross-signal.html':'07a5381d222e5087a46587bb138974786b7b5ee3b07d5c63484d42666c2ca84c'};
 for(const [name,hash] of Object.entries(hashes))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,'fixtures/research-explain-predecessor',name))).digest('hex'),hash);
});
