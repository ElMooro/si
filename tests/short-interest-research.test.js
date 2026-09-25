const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const O=require('../jh-short-interest-research.js'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/short-interest-native.json'),'utf8'));
const run=fixture.run.split('/').pop().slice(0,64),flush=()=>new Promise(r=>setImmediate(r));
function network(change){const calls=[];return {calls,fetch:async key=>{calls.push(key);const raw=fixture.objects[key.slice(1)];assert.equal(typeof raw,'string','Unreviewed read '+key);return new Response(change?change(key,raw):raw);}};}

test('recorded settlement positions keep complete source identity and separate ratios',async()=>{
 const n=network(),s=await O.load(n.fetch,run),r=await O.record(n.fetch,s,'ABC'),p=O.observation(s,r,'2026-09-15');
 assert.equal(r.row.observations.length,4);assert.equal(p.point.currentShortPositionQuantity,'100');assert.equal(p.point.daysToCoverQuantity,'1');assert.equal(p.point.reconstructed_position_to_reported_adv_days,'0.500000000000');assert.equal(p.source.pass,1);assert.equal(p.point.source_row,0);assert.equal(p.source.original.sha256.length,64);assert.equal(s.packet.counts.source_rows_each_scan,8);
});
test('current must equal immutable output and a missing pin never falls back',async()=>{
 assert.equal((await O.load(network().fetch,null)).runId,run);
 for(const pin of ['', '../'+run,'not-a-run']){const n=network();await assert.rejects(()=>O.load(n.fetch,pin));assert.equal(n.calls.length,0);}
 const calls=[];await assert.rejects(()=>O.load(async key=>{calls.push(key);return new Response('',{status:403});},'0'.repeat(64)));assert.equal(calls.length,1);assert(!calls[0].endsWith('/short-interest.json'));
 for(const change of [p=>p.calls_eligible=true,p=>p.counts.issues++,p=>p.short_float_pct=20])await assert.rejects(()=>O.load(network((key,raw)=>{if(key.endsWith('/short-interest.json')){const p=JSON.parse(raw);change(p);return JSON.stringify(p);}return raw;}).fetch,null));
});
test('tampered output and issue bucket fail; multiple identities require a choice',async()=>{
 await assert.rejects(()=>O.load(network((key,raw)=>key.includes('/outputs/')?raw+' ':raw).fetch,run),/verification|identity/);
 const s=await O.load(network().fetch,run);await assert.rejects(()=>O.record(network((key,raw)=>key.includes('/records/')?raw+' ':raw).fetch,s,'ABC'),/verification/);
 await assert.rejects(()=>O.record(network().fetch,s,'MISSING'),/not in/);
 s.packet.symbols[0].issues.push({...s.packet.symbols[0].issues[0],record_id:'0'.repeat(64)});
 await assert.rejects(()=>O.record(network().fetch,s,'ABC'),/exact reported issue/);
});
test('missing issue observations stay absent and invalid settlement locators fail',async()=>{
 const s=await O.load(network().fetch,run),r=await O.record(network().fetch,s,'ABC');r.row.observations.shift();
 const p=O.observation(s,r,'2026-07-31');assert.equal(p.point,null);assert.equal(p.source,null);assert.match(p.missing_reason,/absent/);
 assert.throws(()=>O.observation(s,r,'2026-01-01'));
 r.row.observations.at(-1)[0]=100000;assert.throws(()=>O.observation(s,r,'2026-09-15'),/locator/);
});
test('explicit exposure arithmetic preserves decimals and rejects invalid assumptions',()=>{
 assert.equal(O.scenario({shares:'3.125',price:'100.01',shock:'-10.005',cost:'0.003'}).pnl_usd,'-31.2717515625');
 assert.equal(O.scenario({shares:'-2.5',price:'100',shock:'-10',cost:'1'}).pnl_usd,'24');
 for(const [field,value] of [['price','0'],['shares','NaN'],['shock','-100.01'],['cost','-1']])assert.throws(()=>O.scenario({shares:'1',price:'100',shock:'0',cost:'0',[field]:value}));
 assert.deepEqual(O.decisionView().by_ticker,{});assert.equal(O.decisionView().calls_eligible,false);
});
function dom(api,search='?run='+run+'&symbol=ABC'){
 class El{constructor(tag){this.tag=tag;this.children=[];this.textContent='';this.value='';this.disabled=false;this.style={};this.handlers={};this.attributes={};}append(...items){this.children.push(...items);}replaceChildren(...items){this.textContent='';this.children=items;}addEventListener(name,fn){this.handlers[name]=fn;}setAttribute(k,v){this.attributes[k]=v;}click(){this.clicked=true;}remove(){this.removed=true;}}
 const map=new Map(),get=name=>{if(!map.has(name))map.set(name,new El(name));return map.get(name);},location={search,href:'https://justhodl.ai/short-interest-research.html'+search};
 const document={querySelector:()=>({querySelector:s=>get(s.slice(9,-1))}),createElement:tag=>new El(tag),createElementNS:(_,tag)=>new El(tag),createTextNode:text=>text,getElementById:get,body:new El('body')};
 const history={replaceState(a,b,url){location.href=String(url);location.search=url.search;}};let blob,cleanup,revoked=false;
 class U extends URL{}U.createObjectURL=b=>{blob=b;return 'blob:record';};U.revokeObjectURL=()=>{revoked=true;};
 class F{constructor(form){this.form=form;}entries(){return Object.entries(this.form.values||{});}}
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-short-interest-page.js'),'utf8'),{window:{JHShortInterestResearch:api,fetch:network().fetch},document,location,history,URLSearchParams,URL:U,Blob,FormData:F,setTimeout:fn=>{cleanup=fn;}});
 return {get,location,document,blob:()=>blob,cleanup:()=>cleanup(),revoked:()=>revoked};
}
test('missing pinned record disables actions and clears all old observations',async()=>{
 const d=dom({...O,load:async()=>{throw Error('Missing recorded run');}});await flush();assert(d.get('export').disabled);assert(d.get('calculate').disabled);assert.match(d.get('status').textContent,/Missing recorded/);assert.equal(d.get('observation').children.length,0);
});
test('selection changes clear old scenarios and evidence export binds selected row and assumptions',async()=>{
 const s=await O.load(network().fetch,run),r=await O.record(network().fetch,s,'ABC');const d=dom({...O,load:async()=>s,record:async()=>r});await flush();assert(!d.get('export').disabled);assert.equal(d.get('date').children.length,4);
 d.get('form').values={shares:'1',price:'100',shock:'-10',cost:'1'};d.get('form').handlers.submit({preventDefault(){}});assert.match(d.get('scenario').textContent,/-11 USD/);
 d.get('date').value='2026-07-31';d.get('date').handlers.change();assert.equal(d.get('scenario').textContent,'');assert.match(d.get('observation-status').textContent,/2026-07-31/);
 d.get('export').handlers.click();const out=JSON.parse(await d.blob().text());assert.equal(out.run,fixture.run);assert.equal(out.selected_observation.date,'2026-07-31');assert.equal(out.scenario,null);assert.equal(out.source_pages.length,16);d.cleanup();assert(d.revoked());
 d.get('symbol').value='XYZ';d.get('symbol').handlers.input();assert(d.get('export').disabled);assert(d.get('calculate').disabled);assert.equal(d.get('history').children.length,0);
});
test('a missing selected settlement disables calculation and leaves no previous result',async()=>{
 const s=await O.load(network().fetch,run),r=await O.record(network().fetch,s,'ABC');r.row.observations.shift();const d=dom({...O,load:async()=>s,record:async()=>r});await flush();d.get('date').value='2026-07-31';d.get('date').handlers.change();assert(d.get('calculate').disabled);assert.match(d.get('observation-status').textContent,/absent/);
});
test('complete prior pages remain intact and every decision reader loads the boundary',()=>{
 const crypto=require('node:crypto'),archives=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/short-interest-pages-migration.json'),'utf8'));
 for(const entry of Object.values(archives)){const raw=fs.readFileSync(path.join(__dirname,'..',entry.file));assert.equal(raw.length,entry.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),entry.sha256);}
 for(const page of ['chart.html','chart-pro.html','ticker.html','why.html','signal-replay.html'])assert(fs.readFileSync(path.join(__dirname,'..',page),'utf8').includes('src="/jh-short-interest-research.js"'));
 for(const file of ['jh-chart-instvol.js','ticker.html','why.html','signal-replay.html'])assert.match(fs.readFileSync(path.join(__dirname,'..',file),'utf8'),/JHShortInterestResearch\.decisionView/);
});
test('shared links reproduce the selected settlement and never substitute an invalid one',async()=>{
 const s=await O.load(network().fetch,run),r=await O.record(network().fetch,s,'ABC'),api={...O,load:async()=>s,record:async()=>r};
 const d=dom(api,'?run='+run+'&symbol=ABC&date=2026-07-31');await flush();assert.equal(d.get('date').value,'2026-07-31');assert.match(d.get('observation-status').textContent,/2026-07-31/);
 d.get('date').value='2026-08-31';d.get('date').handlers.change();assert.equal(new URL(d.location.href).searchParams.get('date'),'2026-08-31');
 const broken=dom(api,'?run='+run+'&symbol=ABC&date=2026-01-01');await flush();assert(broken.get('export').disabled);assert(broken.get('calculate').disabled);assert.match(broken.get('identity').textContent,/Requested settlement date is unavailable/);
});
test('an old issue response cannot replace a more recently selected issue',async()=>{
 const s=await O.load(network().fetch,run),r=await O.record(network().fetch,s,'ABC');let finish;
 const d=dom({...O,load:async()=>s,record:async(_,state,symbol)=>symbol==='ABC'?new Promise(resolve=>{finish=()=>resolve(r);}):{...r,row:{...r.row,identity:{...r.row.identity,symbolCode:symbol}}}});await flush();
 d.get('symbol').value='XYZ';d.get('symbol').handlers.input();d.get('search').handlers.submit({preventDefault(){}});await flush();finish();await flush();assert.match(d.get('identity').textContent,/^XYZ/);
});
