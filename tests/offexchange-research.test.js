const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const O=require('../jh-offexchange-research.js'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/offexchange-native.json'),'utf8'));
const run=fixture.run.split('/').pop().slice(0,64),flush=()=>new Promise(r=>setImmediate(r));
function network(change){const calls=[];return {calls,fetch:async key=>{calls.push(key);const raw=fixture.objects[key.slice(1)];assert.equal(typeof raw,'string','Unreviewed read '+key);return new Response(change?change(key,raw):raw);}};}
test('recorded output and selected original locators verify without private or mutable reads',async()=>{
 const n=network(),state=await O.load(n.fetch,run),record=await O.record(n.fetch,state,'AAPL');assert.equal(record.row.weekly[0].ats_pct_of_reported_offexchange,'50.000000000000');assert.equal(record.row.daily[0].source_line,2);
 assert.equal(record.row.monthly[0].reported_activity_hhi_upper_bound,'5200.000000000000');assert(n.calls.every(k=>k.startsWith('/data/offexchange-research/')));
});
test('current publication must equal its immutable output and cannot promote authority',async()=>{
 const n=network(),state=await O.load(n.fetch,null);assert.equal(state.runId,run);
 for(const change of [p=>p.calls_eligible=true,p=>p.generated_at='2026-09-25T03:00:00Z'])await assert.rejects(()=>O.load(network((k,s)=>{if(k.endsWith('/dark-pool.json')){const p=JSON.parse(s);change(p);return JSON.stringify(p);}return s;}).fetch,null));
});
test('invalid pin cannot fetch latest and altered bytes fail exact artifact checks',async()=>{
 for(const pin of ['', 'invalid', '../'+run]){const n=network();await assert.rejects(()=>O.load(n.fetch,pin));assert.equal(n.calls.length,0);}
 await assert.rejects(()=>O.load(network((k,s)=>k.includes('/outputs/')?s+' ':s).fetch,run),/verification|identity/);
 const n=network(),state=await O.load(n.fetch,run);await assert.rejects(()=>O.record(n.fetch,state,'MISSING'),/not in/);
 const altered=network((k,s)=>k.includes('/records/')?s+' ':s);await assert.rejects(()=>O.record(altered.fetch,state,'AAPL'),/verification/);
});
test('signed exposure uses exact decimal arithmetic with explicit costs and boundaries',()=>{
 const x=O.scenario({shares:'3.125',price:'391.69',shock:'-10.005',cost:'0.003'});assert.equal(x.pnl_usd,'-122.4673265625');assert.equal(x.signed_notional_usd,'1224.03125');assert.equal(x.forecast,false);
 assert.equal(O.scenario({shares:'-2.5',price:'100',shock:'-10',cost:'1'}).pnl_usd,'24');
 assert.equal(O.scenario({shares:'0',price:'1',shock:'0',cost:'0'}).pnl_usd,'0');
 for(const [field,value] of [['price','0'],['shares','NaN'],['shares','1e5'],['shock','-100.001'],['cost','-1'],['shares','1.000000001']])assert.throws(()=>O.scenario({shares:'1',price:'2',shock:'0',cost:'0',[field]:value}));
});
function dom(api,search='?run='+run+'&symbol=AAPL'){
 class El{constructor(tag){this.tag=tag;this.children=[];this.textContent='';this.value='';this.disabled=false;this.style={};this.handlers={};}append(...items){this.children.push(...items);}replaceChildren(...items){this.textContent='';this.children=items;}addEventListener(name,fn){this.handlers[name]=fn;}click(){this.clicked=true;}remove(){this.removed=true;}}
 const map=new Map(),get=name=>{if(!map.has(name))map.set(name,new El(name));return map.get(name);},location={search,href:'https://justhodl.ai/offexchange-research.html'+search};
 const document={querySelector:()=>({querySelector:s=>get(s.slice(9,-1))}),createElement:tag=>new El(tag),createTextNode:text=>text,getElementById:get,body:new El('body')};
 const history={replaceState(a,b,url){location.href=String(url);location.search=url.search;}};let blob,cleanup,revoked=false;
 class U extends URL{}U.createObjectURL=b=>{blob=b;return 'blob:record';};U.revokeObjectURL=()=>{revoked=true;};
 class F{constructor(form){this.form=form;}entries(){return Object.entries(this.form.values||{});}}
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-offexchange-page.js'),'utf8'),{window:{JHOffexchangeResearch:api,fetch:network().fetch},document,location,history,URLSearchParams,URL:U,Blob,FormData:F,setTimeout:fn=>{cleanup=fn;}});
 return {get,location,document,blob:()=>blob,cleanup:()=>cleanup(),revoked:()=>revoked};
}
test('failed pinned run clears prior evidence and keeps export/scenario disabled',async()=>{
 const d=dom({...O,load:async()=>{throw Error('Missing recorded run');}});await flush();assert(d.get('export').disabled);assert(d.get('calculate').disabled);assert.match(d.get('status').textContent,/Missing recorded/);assert.equal(d.get('daily').children.length,0);
});
test('selection, refresh and assumptions remain on exact run and clear old scenario/export state',async()=>{
 const state=await O.load(network().fetch,run),r=await O.record(network().fetch,state,'AAPL'),requested=[];
 const api={...O,load:async(_,pin)=>{assert.equal(pin,run);return state;},record:async(_,s,sym)=>{requested.push(sym);return {artifact:r.artifact,row:{...r.row,symbol:sym}};}};
 const d=dom(api);await flush();assert(!d.get('export').disabled);d.get('form').values={shares:'1',price:'100',shock:'-10',cost:'1'};
 d.get('form').handlers.submit({preventDefault(){}});assert.match(d.get('scenario').textContent,/-11 USD/);
 d.get('form').handlers.input();assert.equal(d.get('scenario').textContent,'');
 d.get('symbol').value='MSFT';d.get('symbol').handlers.input();assert(d.get('export').disabled);d.get('search').handlers.submit({preventDefault(){}});await flush();
 assert.match(d.location.search,/symbol=MSFT/);d.get('refresh').handlers.click();await flush();assert.equal(requested.at(-1),'MSFT');
 d.get('export').handlers.click();const data=JSON.parse(await d.blob().text());assert.equal(data.record.symbol,'MSFT');assert.equal(data.run,fixture.run);assert.equal(data.scenario,null);assert(!d.revoked());d.cleanup();assert(d.revoked());
});
test('late selected record cannot replace a newer requested selection',async()=>{
 const state=await O.load(network().fetch,run),r=await O.record(network().fetch,state,'AAPL');let finish;
 const d=dom({...O,load:async()=>state,record:async(_,s,sym)=>sym==='AAPL'?new Promise(resolve=>{finish=()=>resolve(r);}):{...r,row:{...r.row,symbol:sym}}});await flush();
 d.get('symbol').value='MSFT';d.get('symbol').handlers.input();d.get('search').handlers.submit({preventDefault(){}});await flush();finish();await flush();assert.match(d.get('identity').textContent,/^MSFT/);
});
