const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto'),cp=require('node:child_process');
const io=require('../jh-portfolio-scenario-io.js'),model=require('../jh-portfolio-scenario.js'),cli=require('../scripts/replay_portfolio_scenario.cjs');
const root=path.join(__dirname,'..'),raw=fs.readFileSync(path.join(root,'jh-portfolio-scenario.js')),sha=crypto.createHash('sha256').update(raw).digest('hex');
const identity={contract:model.CONTRACT,sha256:sha,bytes:raw.length};
const packet=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/pre-scenario-replay-export.json'),'utf8'));
const encoded=v=>Buffer.from(JSON.stringify(v));
const reordered=v=>Array.isArray(v)?v.map(reordered):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).reverse().map(([k,x])=>[k,reordered(x)])):v;

test('scenario strict decoding rejects duplicate identities, invalid UTF-8 and overflow',()=>{
 const p=JSON.stringify(packet());
 for(const text of [p.replace('"nav":"100000"','"nav":"12","nav":"100000"'),p.replace('"input":','"input":null,"input":'),'{"x":{"a":0,"\\u0061":1}}','[1e400]','{"x":"\\ud800"}','\ufeff{}','{} trailing'])assert.throws(()=>io.decode(Buffer.from(text)));
 assert.throws(()=>io.decode(Buffer.from([0xc3,0x28])));assert.throws(()=>io.decode(new Uint8Array(5),4));
 assert.deepEqual(io.decode(encoded(packet())),packet());
 const p2=io.strictJSON('{"__proto__":{"private":1},"text":"\\ud83c\\udfe6"}');assert.equal(Object.getPrototypeOf(p2),Object.prototype);assert.equal({}.private,undefined);assert.equal(p2.text,'🏦');
});

test('one complete replay contract accepts semantic key order in both browser and CLI',()=>{
 const p=reordered(packet());assert.deepEqual(io.verify(p,reordered(identity),model),packet().output);assert.deepEqual(cli.verify(p),packet().output);
 assert.deepEqual(p,reordered(packet()));
});

test('replay refuses extra or missing envelope/model fields and every result mutation',()=>{
 for(const change of [p=>p.extra='ignored before',p=>delete p.output,p=>p.model.extra=0,p=>p.model.bytes=true,p=>p.model.sha256='a'.repeat(64),p=>p.output.permissions.sizing_eligible=true,p=>p.output.positions[0].pnl.numerator='99',p=>delete p.output.positions[0].ending_value,p=>p.output.extra=0,p=>p.output.total_pnl=null]){
  const p=packet();change(p);assert.throws(()=>io.verify(p,identity,model));assert.throws(()=>cli.verify(p));
 }
 assert.throws(()=>io.same({n:undefined},{n:null}),/non-JSON/);assert.throws(()=>io.same({n:Infinity},{n:null}),/non-JSON/);
});

test('the entire genuine synthetic predecessor export replays without a model change',()=>{
 const p=packet();assert.deepEqual(p.model,identity);assert.deepEqual(io.verify(p,identity,model),p.output);
 assert.deepEqual(raw,fs.readFileSync(path.join(root,'aws/lambdas/justhodl-position-sizer/source/scenario_model.js')));
 assert.deepEqual(raw,fs.readFileSync(path.join(__dirname,'fixtures/pre-scenario-replay-jh-portfolio-scenario.js.txt')));
});

test('local file replay reads complete bytes, preserves non-ASCII labels and rejects false sizes',async()=>{
 const p=packet();p.input.label='Synthetic café 🏦';p.output=model.calculate(p.input);const blob=new Blob([JSON.stringify(p)]);
 assert.deepEqual(await io.readFile(blob),p);
 for(const size of [blob.size-1,blob.size+1])await assert.rejects(io.readFile({size,stream:()=>blob.stream()}),/byte|bound/);
 for(const file of [{size:io.LIMIT+1,stream(){}},{size:true,stream(){}},{size:2}])await assert.rejects(io.readFile(file),/bound|readable/);
});

test('bounded stream fails without a prefix and closes on HTTP, chunk and reader errors',async()=>{
 let cancelled=0,released=0;
 const response={ok:true,body:{getReader:()=>({read:async()=>({value:new Uint8Array(4),done:false}),cancel(){cancelled++;},releaseLock(){released++;}})}};
 await assert.rejects(io.readComplete(async()=>response,{limit:3}),/bound/);assert.equal(cancelled,1);assert.equal(released,1);
 await assert.rejects(io.readComplete(async()=>({ok:false,body:{cancel(){cancelled++;}}})),/unavailable/);assert.equal(cancelled,2);
 await assert.rejects(io.readComplete(async()=>({ok:true,body:{getReader:()=>({read:async()=>({value:'not bytes'}),cancel(){cancelled++;},releaseLock(){}})}})),/chunk/);
 await assert.rejects(io.readComplete(async()=>({ok:true})),/readable/);
 assert.deepEqual(await io.readComplete(async()=>new Response('abcd'),{limit:4,expectedBytes:4}),new TextEncoder().encode('abcd'));
});

test('deadline covers ignored fetch cancellation and disposes a late body',async()=>{
 let resolve,cancelled=0;const pending=new Promise(done=>resolve=done);
 await assert.rejects(io.readComplete(()=>pending,{timeoutMs:5}),/timed out/);
 resolve({ok:true,body:{cancel(){cancelled++;}}});await new Promise(setImmediate);assert.equal(cancelled,1);
});

test('stalled local readers settle after abort without awaiting their cancellation',async()=>{
 const controller=new AbortController();let cancelled=0,released=0;
 const loading=io.readFile({size:1,stream:()=>({getReader:()=>({read:()=>new Promise(()=>{}),cancel(){cancelled++;return new Promise(()=>{});},releaseLock(){released++;}})})},{signal:controller.signal});
 setTimeout(()=>controller.abort(),5);await assert.rejects(loading,{name:'AbortError'});assert.equal(cancelled,1);assert.equal(released,1);
 let opened=0;await assert.rejects(io.readComplete(()=>{opened++;},{signal:controller.signal}),{name:'AbortError'});assert.equal(opened,0);
});

test('CLI decodes exact complete files and rejects duplicate JSON, malformed bytes and oversized files',()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'jh-scenario-replay-'));const file=path.join(dir,'synthetic.json'),script=path.join(root,'scripts/replay_portfolio_scenario.cjs');
 try{
  fs.writeFileSync(file,encoded(reordered(packet())));assert.deepEqual(cli.readExport(file),reordered(packet()));
  let r=cp.spawnSync(process.execPath,[script,file],{encoding:'utf8'});assert.equal(r.status,0,r.stderr);assert.equal(JSON.parse(r.stdout).verified,true);
  for(const content of [Buffer.from(JSON.stringify(packet()).replace('"nav":"100000"','"nav":"123","nav":"100000"')),Buffer.from([0xc3,0x28]),Buffer.alloc(io.LIMIT+1,32)]){
   fs.writeFileSync(file,content);r=cp.spawnSync(process.execPath,[script,file],{encoding:'utf8'});assert.equal(r.status,1);assert.match(r.stderr,/rejected/);assert.equal(r.stdout,'');
  }
  assert.throws(()=>cli.readExport(dir),/regular file|illegal operation/);
 }finally{if(fs.existsSync(file))fs.unlinkSync(file);fs.rmdirSync(dir);}
});

class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.dataset={};this.value='';this.handlers={};this.files=[];this.disabled=false;}
 append(...items){for(const item of items){item.parent=this;this.children.push(item);}}
 replaceChildren(...items){this.children=[];this.append(...items);}
 setAttribute(k,v){this[k]=v;}
 addEventListener(k,v){this.handlers[k]=v;}
 querySelectorAll(tag){return this.children.flatMap(c=>[...(c.tagName===tag?[c]:[]),...c.querySelectorAll(tag)]);}
 remove(){this.parent.children=this.parent.children.filter(c=>c!==this);}
 click(){}
}
async function page(change=()=>{},options={}){
 const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,new Element('div'));return elements.get(id);};
 get('scenario-model').dataset={sha256:sha,bytes:String(raw.length)};
 const publication={contract:'portfolio-scenario-availability.v1',model_contract:model.CONTRACT,input_contract:model.INPUT,call:'WAIT',permissions:{calls_eligible:false,sizing_eligible:false,execution_eligible:false,may_recommend_trades:false},generated_at:'2026-09-29T12:00:00Z',scenario_model:{key:'data/scenario-model/models/'+sha+'.js',sha256:sha,bytes:raw.length}};change(publication);
 const calls=[],downloads=[];const fetch=async(url,opts)=>{calls.push({url,opts});if(options.fetch)return options.fetch(url,opts,publication);return new Response(url==='/data/position-sizing.json'?JSON.stringify(publication):raw);};
 const scope=vm.createContext({window:{JHPortfolioScenario:model,JHScenarioIO:io},document:{getElementById:get,createElement:tag=>new Element(tag)},fetch,crypto:crypto.webcrypto,Blob,URL:{createObjectURL:b=>{downloads.push(b);return 'blob:synthetic';},revokeObjectURL(){}},AbortController,Uint8Array,console,setTimeout:fn=>{fn();return 1;}});
 vm.runInContext(fs.readFileSync(path.join(root,'jh-portfolio-scenario-page.js'),'utf8'),scope);
 for(let i=0;i<100&&!get('model-status').textContent;i++)await new Promise(done=>setTimeout(done,2));
 const importFile=async file=>{get('import').files=[file];await get('import').onchange();};
 return {get,calls,downloads,importFile,importPacket:p=>importFile(new Blob([JSON.stringify(p)]))};
}

test('actual page replays reordered complete exports and downloads exactly reproducible evidence',async()=>{
 const p=await page();await p.importPacket(reordered(packet()));assert.match(p.get('error').textContent,/completely recalculated/);assert.equal(p.get('download').disabled,false);
 assert.equal(p.get('positions').children.length,1);assert.equal(p.get('label').value,packet().input.label);
 p.get('download').onclick();assert.equal(p.downloads.length,1);const exported=await io.readFile(p.downloads[0]);assert.deepEqual(cli.verify(exported),packet().output);assert.deepEqual(exported.input,packet().input);
 assert.equal(p.calls.length,2);assert.deepEqual(p.calls.map(c=>c.url),['/data/position-sizing.json','/jh-portfolio-scenario.js']);assert.ok(p.calls.every(c=>c.opts.credentials==='omit'&&c.opts.redirect==='error'));
});

test('actual page refuses ambiguous/extra exports without replacing the previous assumptions',async()=>{
 const p=await page();await p.importPacket(packet());const label=p.get('label').value;
 for(const text of [JSON.stringify({...packet(),extra:1}),JSON.stringify(packet()).replace('"nav":"100000"','"nav":"2","nav":"100000"')]){
  await p.importFile(new Blob([text]));assert.equal(p.get('label').value,label);assert.equal(p.get('download').disabled,true);assert.equal(p.get('results').children.length,0);assert.match(p.get('error').textContent,/Duplicate|unexpected/);
 }
});

function delayedFile(text){let finish;const raw=Buffer.from(text);return {file:{size:raw.length,stream:()=>({getReader:()=>({read:()=>new Promise(done=>{finish=()=>done({value:raw,done:false});}),cancel(){},releaseLock(){}})})},finish:()=>finish?.()};}
test('a delayed import cannot overwrite newer form edits or resurrect results',async()=>{
 const p=await page(),delay=delayedFile(JSON.stringify(packet()));const pending=p.importFile(delay.file);await new Promise(setImmediate);
 p.get('label').value='Newer synthetic edit';p.get('scenario').handlers.input();delay.finish();await pending;
 assert.equal(p.get('label').value,'Newer synthetic edit');assert.equal(p.get('download').disabled,true);assert.equal(p.get('results').children.length,0);assert.equal(p.get('error').textContent,'');
});

test('newer imports, synthetic examples and row changes supersede a slow import',async()=>{
 for(const action of ['import','example','add']){
  const p=await page(),delay=delayedFile(JSON.stringify(packet()));const pending=p.importFile(delay.file);await new Promise(setImmediate);
  if(action==='import'){const newer=packet();newer.input.label='NEWEST synthetic scenario';newer.output=model.calculate(newer.input);await p.importPacket(newer);}
  else p.get(action).onclick();
  const expected=p.get('label').value,count=p.get('positions').children.length;delay.finish();await pending;
  assert.equal(p.get('label').value,expected);assert.equal(p.get('positions').children.length,count);
  if(action==='import')assert.match(p.get('error').textContent,/completely recalculated/);
 }
});

test('published model identity or permission mismatches disable export but keep local arithmetic',async()=>{
 for(const mutate of [p=>p.permissions.sizing_eligible=true,p=>p.scenario_model.bytes++,p=>p.call='LONG']){
  const p=await page(mutate);assert.match(p.get('model-status').textContent,/versions or permissions do not match/);
  p.get('example').onclick();p.get('scenario').onsubmit({preventDefault(){}});assert.ok(p.get('results').children.length>0);assert.equal(p.get('download').disabled,true);
 }
 const p=await page(()=>{},{fetch:async(url,opts,publication)=>new Response(url.endsWith('.json')?JSON.stringify(publication):Buffer.concat([raw,Buffer.from('\n')]))});assert.match(p.get('model-status').textContent,/page-bound/);
});

test('page-bound model SRI is exactly the unchanged reviewed native model',()=>{
 const html=fs.readFileSync(path.join(root,'position-sizer.html'),'utf8'),tag=html.match(/<script id="scenario-model"[^>]+>/)[0];
 assert.match(tag,new RegExp('data-sha256="'+sha+'"'));assert.match(tag,new RegExp('data-bytes="'+raw.length+'"'));
 assert.ok(tag.includes('integrity="sha256-'+crypto.createHash('sha256').update(raw).digest('base64')+'"'));
 assert.ok(html.indexOf('jh-portfolio-scenario-io.js')<html.indexOf('jh-portfolio-scenario-page.js'));
});
