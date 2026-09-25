const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const O=require('../jh-short-volume-research.js'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/short-volume-native.json'),'utf8'));
const run=fixture.run.split('/').pop().slice(0,64),flush=()=>new Promise(r=>setImmediate(r));
function network(change){const calls=[];return {calls,fetch:async key=>{calls.push(key);const raw=fixture.objects[key.slice(1)];assert.equal(typeof raw,'string','Unreviewed read '+key);return new Response(change?change(key,raw):raw);}};}

test('exact recorded output and every selected daily point retain source locators',async()=>{
 const n=network(),state=await O.load(n.fetch,run),record=await O.record(n.fetch,state,'AAPL');
 assert.equal(record.row.points.length,61);assert.equal(record.row.windows['60'].observed_rows,60);
 const observation=O.observation(state,record,60);assert.equal(observation.date,'2026-09-24');assert.equal(observation.point[1],2);assert.equal(observation.short_volume_pct,'60.99999978');
 assert.equal(observation.source.original.sha256.length,64);assert(n.calls.every(k=>k.startsWith('/data/short-volume-research/')));
});
test('current must equal immutable output; invalid or missing pins never fall back to latest',async()=>{
 assert.equal((await O.load(network().fetch,null)).runId,run);
 for(const pin of ['', 'invalid', '../'+run]){const n=network();await assert.rejects(()=>O.load(n.fetch,pin));assert.equal(n.calls.length,0);}
 const calls=[];await assert.rejects(()=>O.load(async key=>{calls.push(key);return new Response('',{status:404});},'0'.repeat(64)));assert.equal(calls.length,1);assert(!calls[0].endsWith('/finra-short.json'));
 for(const change of [p=>p.calls_eligible=true,p=>p.counts.source_rows++,p=>p.days_to_cover=20])await assert.rejects(()=>O.load(network((key,raw)=>{if(key.endsWith('/finra-short.json')){const p=JSON.parse(raw);change(p);return JSON.stringify(p);}return raw;}).fetch,null));
});
test('changed output and selected bucket reject the record',async()=>{
 await assert.rejects(()=>O.load(network((key,raw)=>key.includes('/outputs/')?raw+' ':raw).fetch,run),/verification|identity/);
 const n=network(),state=await O.load(n.fetch,run);await assert.rejects(()=>O.record(network((key,raw)=>key.includes('/records/')?raw+' ':raw).fetch,state,'AAPL'),/verification/);
 await assert.rejects(()=>O.record(n.fetch,state,'UNKNOWN'),/not in/);
});
test('daily ratio uses exact fractional shares and half-even rounding, never adds exempt twice',()=>{
 assert.equal(O.percentage('0.100001','0.200002'),'50');assert.equal(O.percentage('1','3'),'33.333333333333');
 assert.equal(O.percentage('0','0'),null);assert.equal(O.percentage('999999999999999999999999999999','999999999999999999999999999999'),'100');
 assert.equal(O.percentage('1','200000000000000'),'0');assert.equal(O.percentage('3','200000000000000'),'0.000000000002');
 for(const pair of [['2','1'],['NaN','1'],['1e5','1'],['-1','2']])assert.throws(()=>O.percentage(...pair));
});
test('missing symbol date stays absent without substituting a previous observation',async()=>{
 const state=await O.load(network().fetch,run),record=await O.record(network().fetch,state,'AAPL');record.row.points.pop();
 const o=O.observation(state,record,60);assert.equal(o.point,null);assert.equal(o.short_volume_pct,null);assert.match(o.missing_reason,/absent/);
});
test('legacy browser consumers never restore squeeze scores, positions or covering states',()=>{
 const value=O.decisionView({squeeze_candidates:[{symbol:'AAPL',squeeze_score:99}],tickers:{AAPL:{days_to_cover:20}},names:[{state:'SHORTS COVERING'}]});
 assert.deepEqual(value.squeeze_candidates,[]);assert.deepEqual(value.tickers,{});assert.deepEqual(value.names,[]);assert.equal(value.calls_eligible,false);
});
test('complete predecessor pages and assets retain their exact bytes',()=>{
 const crypto=require('node:crypto'),manifest=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/short-volume-pages-migration.json'),'utf8'));
 for(const entry of Object.values(manifest)){const raw=fs.readFileSync(path.join(__dirname,'..',entry.file));assert.equal(raw.length,entry.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),entry.sha256);}
});
test('explicit signed exposure arithmetic keeps exact decimals and rejects invalid assumptions',()=>{
 assert.equal(O.scenario({shares:'3.125',price:'100.01',shock:'-10.005',cost:'0.003'}).pnl_usd,'-31.2717515625');
 assert.equal(O.scenario({shares:'-2.5',price:'100',shock:'-10',cost:'1'}).pnl_usd,'24');
 for(const [field,value] of [['price','0'],['shares','NaN'],['shock','-100.01'],['cost','-1']])assert.throws(()=>O.scenario({shares:'1',price:'100',shock:'0',cost:'0',[field]:value}));
});

function dom(api,search='?run='+run+'&symbol=AAPL'){
 class El{constructor(tag){this.tag=tag;this.children=[];this.textContent='';this.value='';this.disabled=false;this.style={};this.handlers={};this.attributes={};}append(...items){this.children.push(...items);}replaceChildren(...items){this.textContent='';this.children=items;}addEventListener(name,fn){this.handlers[name]=fn;}setAttribute(k,v){this.attributes[k]=v;}click(){this.clicked=true;}remove(){this.removed=true;}}
 const map=new Map(),get=name=>{if(!map.has(name))map.set(name,new El(name));return map.get(name);},location={search,href:'https://justhodl.ai/short-volume-research.html'+search};
 const document={querySelector:()=>({querySelector:s=>get(s.slice(9,-1))}),createElement:tag=>new El(tag),createElementNS:(_,tag)=>new El(tag),createTextNode:text=>text,getElementById:get,body:new El('body')};
 const history={replaceState(a,b,url){location.href=String(url);location.search=url.search;}};let blob,cleanup,revoked=false;
 class U extends URL{}U.createObjectURL=b=>{blob=b;return 'blob:record';};U.revokeObjectURL=()=>{revoked=true;};
 class F{constructor(form){this.form=form;}entries(){return Object.entries(this.form.values||{});}}
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-short-volume-page.js'),'utf8'),{window:{JHShortVolumeResearch:api,fetch:network().fetch},document,location,history,URLSearchParams,URL:U,Blob,FormData:F,setTimeout:fn=>{cleanup=fn;}});
 return {get,location,document,blob:()=>blob,cleanup:()=>cleanup(),revoked:()=>revoked};
}
test('failed pin leaves no stale record or enabled export/scenario',async()=>{
 const d=dom({...O,load:async()=>{throw Error('Missing recorded run');}});await flush();assert(d.get('export').disabled);assert(d.get('calculate').disabled);assert.match(d.get('status').textContent,/Missing recorded/);assert.equal(d.get('observation').children.length,0);
});
test('date, symbol and assumption changes clear scenarios; export contains exact selected lineage',async()=>{
 const state=await O.load(network().fetch,run),r=await O.record(network().fetch,state,'AAPL');
 const d=dom({...O,load:async()=>state,record:async()=>r});await flush();assert(!d.get('export').disabled);assert.equal(d.get('date').children.length,61);
 d.get('form').values={shares:'1',price:'100',shock:'-10',cost:'1'};d.get('form').handlers.submit({preventDefault(){}});assert.match(d.get('scenario').textContent,/-11 USD/);
 d.get('date').value='0';d.get('date').handlers.change();assert.equal(d.get('scenario').textContent,'');assert.match(d.get('observation-status').textContent,/2026-07-26/);
 d.get('next').handlers.click();d.get('next').handlers.click();d.get('next').handlers.click();assert.equal(d.get('page').textContent,'Published files 61–61 of 61');assert(d.get('next').disabled);
 d.get('export').handlers.click();const data=JSON.parse(await d.blob().text());assert.equal(data.run,fixture.run);assert.equal(data.selected_observation.date_index,0);assert.equal(data.daily_sources.length,61);assert.equal(data.record.points.length,61);assert.equal(data.scenario,null);assert(!d.revoked());d.cleanup();assert(d.revoked());
 d.get('symbol').value='MSFT';d.get('symbol').handlers.input();assert(d.get('export').disabled);assert.equal(d.get('observation').children.length,0);
});
test('slow old symbol response cannot replace a newer selected record',async()=>{
 const state=await O.load(network().fetch,run),r=await O.record(network().fetch,state,'AAPL');let finish;
 const d=dom({...O,load:async()=>state,record:async(_,s,sym)=>sym==='AAPL'?new Promise(resolve=>{finish=()=>resolve(r);}):{...r,row:{...r.row,symbol:sym}}});await flush();
 d.get('symbol').value='MSFT';d.get('symbol').handlers.input();d.get('search').handlers.submit({preventDefault(){}});await flush();finish();await flush();assert.match(d.get('identity').textContent,/^MSFT/);
});
test('existing consumer pages load the boundary and native heads are noncached at the edge',()=>{
 for(const page of ['chart.html','chart-pro.html','why.html','squeeze.html'])assert(fs.readFileSync(path.join(__dirname,'..',page),'utf8').includes('src="/jh-short-volume-research.js"'));
 assert.match(fs.readFileSync(path.join(__dirname,'../jh-chart-instvol.js'),'utf8'),/JHShortVolumeResearch\.decisionView/);
 for(const page of ['short/index.html','short-pressure.html','short-volume-research.html']){
   const html=fs.readFileSync(path.join(__dirname,'..',page),'utf8');assert(html.includes('data-short-volume'));assert(html.includes('/jh-short-volume-page.js'));assert(!html.includes('No setups today'));
 }
 const code=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(code,/\['dollar-radar.json'[^\n]+finra-short.json[^\n]+short-pressure.json/);
});
