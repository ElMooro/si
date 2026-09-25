const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const api=require('../jh-sec-ftd-research.js'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/sec-ftd-native.json'),'utf8'));
const run=fixture.run.split('/').pop().slice(0,64);
async function ready(app){
 const deadline=Date.now()+5000;
 while(!/CUSIP verified|no date was substituted/.test(app.get('status').textContent)){
  assert(Date.now()<deadline,'Page verification did not settle: '+app.get('status').textContent);
  await new Promise(resolve=>setTimeout(resolve,2));
 }
}
function network(change){const calls=[];return {calls,fetch:async(key,options)=>{calls.push(key);assert.equal(options.cache,'no-store');const raw=fixture.objects[key.slice(1)];assert.equal(typeof raw,'string','Unreviewed read '+key);return new Response(change?change(key,raw):raw);}};}

test('all observations retain source lines, precise balances and missing labels',async()=>{
 const n=network(),state=await api.load(n.fetch,null),selected=await api.record(n.fetch,state,'001234567');
 assert.equal(state.runId,run);assert.equal(state.packet.counts.original_rows,36);assert.equal(selected.row.observations.length,24);
 const value=api.observation(state,selected,'2026-08-18');assert.equal(value.point.fail_balance_shares,'150');assert.equal(value.point.adjacent_balance_change_pct,'50.000000000000');assert.equal(value.point.source_line,3);assert.equal(value.source.original.sha256.length,64);
 const other=await api.record(n.fetch,state,'901234567');assert.equal(api.observation(state,other,'2026-08-18').point.reported_symbol,'');
 assert.equal(api.observation(state,other,'2026-08-17').point,null);assert.match(api.observation(state,other,'2026-08-17').missing_reason,/no zero/);
 assert(n.calls.every(key=>!key.includes('audit-private')));
});
test('shared reported labels require explicit CUSIP selection',async()=>{
 const n=network(),state=await api.load(n.fetch,run);assert.equal(api.findIssues(state,'abc').length,2);
 assert.equal(api.findIssues(state,'001234567').length,1);assert.equal(api.findIssues(state,'MISSING').length,0);
 await assert.rejects(()=>api.record(n.fetch,state,'ABC'),/exact reported CUSIP/);
});
test('missing pins, altered current, output and selected buckets fail without fallback',async()=>{
 for(const pin of ['', '../'+run,'wrong']){const n=network();await assert.rejects(()=>api.load(n.fetch,pin));assert.equal(n.calls.length,0);}
 const calls=[];await assert.rejects(()=>api.load(async key=>{calls.push(key);return new Response('',{status:403});},'0'.repeat(64)));assert.equal(calls.length,1);assert(!calls[0].endsWith('squeeze-fuel.json'));
 for(const mutate of [p=>p.calls_eligible=true,p=>p.counts.original_rows++,p=>p.score=99])await assert.rejects(()=>api.load(network((key,raw)=>{if(key.endsWith('squeeze-fuel.json')){const p=JSON.parse(raw);mutate(p);return JSON.stringify(p);}return raw;}).fetch,null));
 await assert.rejects(()=>api.load(network((key,raw)=>key.includes('/outputs/')?raw+' ':raw).fetch,run),/verification|identity/);
 const state=await api.load(network().fetch,run);await assert.rejects(()=>api.record(network((key,raw)=>key.includes('/records/')?raw+' ':raw).fetch,state,'001234567'),/verification/);
});
test('hash-valid unqualified compiler manifests are rejected',async()=>{
 const value=JSON.parse(fixture.objects[fixture.run]);value.compilers.sec_ftd_source.sha256='f'.repeat(64);value.compilers.sec_ftd_source.key=api.PREFIX+'compilers/'+'f'.repeat(64)+'.py';
 const body=api.canonical(value),digest=await api.hash(new TextEncoder().encode(body));
 await assert.rejects(()=>api.load(async()=>new Response(body),digest),/Unqualified calculation version/);
});
test('exposure effects use exact signed decimal assumptions and have no forecast',()=>{
 const value=api.scenario({shares:'3.125',price:'100.01',shock:'-10.005',cost:'0.003'});assert.equal(value.pnl_usd,'-31.2717515625');assert.equal(value.signed_notional_usd,'312.53125');assert.equal(value.forecast,false);
 assert.equal(api.scenario({shares:'-100',price:'50',shock:'20',cost:'7'}).pnl_usd,'-1007');
 for(const change of [{price:'0'},{cost:'-1'},{shock:'-101'},{shares:'1e9'},{price:'Infinity'}])assert.throws(()=>api.scenario({shares:'1',price:'1',shock:'1',cost:'0',...change}));
});

function ui(search,customApi=api){
 class El{constructor(tag){this.tagName=tag;this.children=[];this.events={};this.value='';this.disabled=false;this.textContent='';this.clientWidth=760;this.classList={add(){}};}append(...items){this.children.push(...items);}replaceChildren(...items){this.children=[...items];this.textContent='';}addEventListener(name,fn){this.events[name]=fn;}setAttribute(name,value){this[name]=value;}click(){this.clicked=true;}remove(){this.removed=true;}}
 const nodes=new Map(),get=name=>{if(!nodes.has(name))nodes.set(name,new El(name));return nodes.get(name);},location={search,href:'https://justhodl.ai/squeeze-fuel.html'+search};
 const document={querySelector:()=>({querySelector:selector=>get(selector.slice(10,-1))}),createElement:tag=>new El(tag),createElementNS:(_,tag)=>new El(tag),getElementById:get,body:new El('body')};
 const history={replaceState(a,b,url){location.href=String(url);location.search=url.search;}};let blob;
 class U extends URL{}U.createObjectURL=value=>{blob=value;return 'blob:record';};U.revokeObjectURL=()=>{};
 class F{constructor(form){this.form=form;}entries(){return Object.entries(this.form.values||{});}}
 const net=network();vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-sec-ftd-page.js'),'utf8'),{window:{JHSecFtdResearch:customApi,addEventListener(){}},fetch:net.fetch,document,location,history,URLSearchParams,URL:U,Blob,FormData:F,setTimeout(){}});
 return {get,location,blob:()=>blob};
}
test('actual page shows a missing date as a gap, withholds scenario and exports exact selection',async()=>{
 const app=ui('?run='+run+'&cusip=901234567&date=2026-08-17');await ready(app);
 assert.match(app.get('observation-status').textContent,/no zero/);assert.equal(app.get('calculate').disabled,true);
 app.get('export').events.click();const exported=JSON.parse(await app.blob().text());assert.equal(exported.selected_observation.point,null);assert.equal(exported.record.reported_cusip,'901234567');assert.equal(exported.run,fixture.run);
 app.get('date').value='2026-08-18';app.get('date').events.change();assert.equal(app.get('calculate').disabled,false);
 app.get('form').values={shares:'3.125',price:'100.01',shock:'-10.005',cost:'0.003'};app.get('form').events.submit({preventDefault(){}});assert.match(app.get('scenario').textContent,/-31\.2717515625 USD/);
 app.get('query').value='ABC';app.get('query').events.input();assert.equal(app.get('export').disabled,true);assert.equal(app.get('scenario').textContent,'');assert.equal(app.get('observation').children.length,0);
});
test('invalid requested date stays unselected until an explicit valid date',async()=>{
 const app=ui('?run='+run+'&cusip=001234567&date=2020-01-01');await ready(app);
 assert.match(app.get('status').textContent,/no date was substituted/);assert.equal(app.get('date').selectedIndex,-1);assert.equal(app.get('export').disabled,true);
 app.get('date').value='2026-08-18';app.get('date').events.change();assert.equal(app.get('export').disabled,false);assert.match(app.get('status').textContent,/CUSIP verified/);
});
test('page readiness follows completed verification even when crypto or transport is delayed',async()=>{
 const delayed={...api,record:async(...args)=>{await new Promise(resolve=>setTimeout(resolve,30));return api.record(...args);}};
 const app=ui('?run='+run+'&cusip=901234567&date=2026-08-17',delayed);
 await ready(app);assert.match(app.get('observation-status').textContent,/no zero/);assert.equal(app.get('calculate').disabled,true);
});
test('page references complete reviewed assets and retains its full predecessor',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../squeeze-fuel.html'),'utf8');
 for(const asset of ['jh-sec-ftd-research.js','jh-sec-ftd-page.js','jh-sec-ftd-research.css'])assert(html.includes('/'+asset));
 assert(!html.includes('jh-page-ai.js'));assert(!html.includes('Lit-fuse picks'));assert(html.includes('data-ftd-source'));
 const migration=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/sec-ftd-consumer-migration.json'),'utf8'));
 assert.equal(fs.readFileSync(path.join(__dirname,'../',migration.archives.page.file)).byteLength,migration.archives.page.bytes);
});
