const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const evidence=require('../jh-evidence-io.js');
const ROW={symbol:'AAA',qty:10,cost_basis_per_share:100,cost_basis_total:1000,computed_cost_basis_total:1000,basis_reconciliation_status:'MATCHED',record_etag:'a'.repeat(64),notes:'invented note'};
const book=rows=>({ok:true,list_complete:true,positions:rows,counts:{positions:rows.length},list_consistency:'STRONGLY_CONSISTENT_PAGES_NOT_ATOMIC_SNAPSHOT'});

function page(){
 const elements=new Map(),requests=[],storage=new Map(),events={};let active=null;
 const decode=s=>s.replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&');
 function element(id){
  if(elements.has(id))return elements.get(id);
  let html='';const el={value:'',textContent:'',style:{},className:'',disabled:false,focus(){active=id;},getAttribute(){return null;},addEventListener(name,fn){events[id+':'+name]=fn;}};
  Object.defineProperty(el,'innerHTML',{get(){return html;},set(value){html=value;for(const m of value.matchAll(/<input\b[^>]*\bid="([^"]+)"[^>]*\bvalue="([^"]*)"/g))element(m[1]).value=decode(m[2]);}});
  elements.set(id,el);return el;
 }
 const context=vm.createContext({console,document:{getElementById:element,querySelectorAll(){return [];},addEventListener(){}},
  sessionStorage:{getItem:k=>storage.get(k)||'',setItem:(k,v)=>storage.set(k,v)},JHEvidenceIO:{...evidence,readComplete:(open,opts)=>evidence.readComplete(open,{...opts,timeoutMs:100})},
  AbortController,Date,confirm:()=>false,fetch:(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,payload:JSON.parse(options.body),resolve,reject}))});
 const html=fs.readFileSync('portfolio-manager.html','utf8');const source=[...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).find(s=>s.includes('const ADMIN_URL='));
 assert.ok(source);vm.runInContext(source,context);
 return {context,elements,requests,run:code=>vm.runInContext(code,context),load(rows){context.supplied=structuredClone(book(rows));return vm.runInContext('acceptBook(supplied)',context);},input(id,value){element(id).value=value;},reply(index,value,status=200){requests[index].resolve(new Response(JSON.stringify(value),{status}));},active:()=>active};
}
const tick=()=>new Promise(resolve=>setTimeout(resolve,0));

test('manager requires a complete list and never converts failed refresh into an empty book',async()=>{
 const p=page();p.load([ROW]);const pending=p.run('refresh()');p.reply(0,{ok:false,err:'invented list failure'},503);await pending;
 assert.match(p.elements.get('book').innerHTML,/Book unavailable/);assert.doesNotMatch(p.elements.get('book').innerHTML,/contains no positions/);
 assert.equal(p.elements.get('book-status').textContent,'invented list failure');assert.equal(p.run('POSITIONS'),null);
 for(const bad of [{ok:true,positions:[]},{ok:true,list_complete:true,positions:[],counts:{positions:true}}]){
  p.context.bad=bad;assert.throws(()=>p.run('acceptBook(bad)'));
 }
});

test('explicit complete empty list preserves measured zero without unknown input coercion',()=>{
 const p=page();p.load([]);assert.match(p.elements.get('book').innerHTML,/complete response contains no positions/);
 assert.match(p.elements.get('kpis').innerHTML,/\$0.00/);
 p.load([{...ROW,qty:true,computed_cost_basis_total:null,target_weight_pct:false}]);
 assert.match(p.elements.get('kpis').innerHTML,/0\/1 computed/);assert.doesNotMatch(p.elements.get('kpis').innerHTML,/\$0.00/);
 assert.match(p.elements.get('book').innerHTML,/<td class="num">—<\/td>/);
});

test('current editor sends only changed canonical fields and the complete source token',async()=>{
 const p=page();p.load([ROW]);p.run('editRow(0)');p.input('e_qty','20');
 const pending=p.run('saveRow()');assert.deepEqual(p.requests[0].payload,{action:'update_position',symbol:'AAA',expected_record_etag:'a'.repeat(64),qty:20});
 p.reply(0,{ok:true,snapshot_refresh:'queued'});await tick();assert.equal(p.requests[1].payload.action,'list');p.reply(1,book([{...ROW,qty:20,record_etag:'b'.repeat(64)}]));await pending;
 assert.match(p.elements.get('toast').textContent,/completion is not yet verified/);
});

test('unchanged form never rewrites rounded numbers or old unsupported fields',async()=>{
 const p=page();p.load([{...ROW,qty:0.12345678901234568,stop_loss:'legacy unsupported text'}]);p.run('editRow(0)');
 await p.run('saveRow()');assert.equal(p.requests.length,0);assert.match(p.elements.get('toast').textContent,/No changes/);
 p.input('e_notes','changed note');const pending=p.run('saveRow()');
 assert.deepEqual(p.requests[0].payload,{action:'update_position',symbol:'AAA',expected_record_etag:'a'.repeat(64),notes:'changed note'});
 p.reply(0,{ok:false,err:'synthetic stop'},409);await pending;
});

test('clearing an existing stop sends null without echoing quantity or cost',async()=>{
 const p=page();p.load([{...ROW,stop_loss:90}]);p.run('editRow(0)');p.input('e_stop','');
 const pending=p.run('saveRow()');assert.deepEqual(p.requests[0].payload,{action:'update_position',symbol:'AAA',expected_record_etag:'a'.repeat(64),stop_loss:null});
 p.reply(0,{ok:false,err:'invented conflict'},409);await pending;
});

test('stale-edit rejection keeps the form and never automatically retries a mutation',async()=>{
 const p=page();p.load([ROW]);p.run('editRow(0)');p.input('e_qty','20');
 const pending=p.run('saveRow()');p.reply(0,{ok:false,error_code:'STALE_EDIT',err:'Refresh before editing'},409);await pending;
 assert.equal(p.requests.length,1);assert.equal(p.elements.get('e_qty').value,'20');assert.match(p.elements.get('toast').textContent,/Refresh before editing/);
});

test('duplicate submit produces one mutation request while acknowledgement is pending',async()=>{
 const p=page();p.load([]);p.input('f_sym','BBB');p.input('f_qty','0');p.input('f_cost','0');
 const pending=p.run('addPos()');await p.run('addPos()');assert.equal(p.requests.length,1);
 assert.deepEqual(p.requests[0].payload,{action:'add_position',symbol:'BBB',qty:0,cost_basis_per_share:0});
 p.reply(0,{ok:true,snapshot_refresh:'unconfirmed'});await tick();p.reply(1,book([{...ROW,symbol:'BBB',qty:0}]));await pending;
 assert.match(p.elements.get('addMsg').textContent,/snapshot refresh is unconfirmed/);assert.doesNotMatch(p.elements.get('addMsg').textContent,/queued/);
});

test('invalid form numbers fail before sending and omitted optional values stay omitted',async()=>{
 const p=page();p.load([]);p.input('f_sym','AAA');p.input('f_qty','Infinity');p.input('f_cost','10');await p.run('addPos()');assert.equal(p.requests.length,0);
 p.input('f_qty','1');p.input('f_cost','');await p.run('addPos()');assert.equal(p.requests.length,0);
 assert.match(p.elements.get('addMsg').textContent,/required/);
});

test('late earlier list response cannot overwrite the newest complete book',async()=>{
 const p=page();p.load([ROW]);const older=p.run('refresh()'),newer=p.run('refresh()');
 p.reply(1,book([{...ROW,symbol:'BBB'}]));await newer;p.reply(0,book([ROW]));await older;
 assert.match(p.elements.get('book').innerHTML,/BBB/);assert.doesNotMatch(p.elements.get('book').innerHTML,/>AAA</);
});

test('hostile records remain text, invalid identity remains a visible row, no dynamic inline handlers',()=>{
 const p=page();const rows=[{...ROW,notes:'"><img src=x onerror=alert(1)>',sector:"<script>"},null,{...ROW,symbol:"AAA');alert(1)//"}];p.load(rows);
 const html=p.elements.get('book').innerHTML;assert.doesNotMatch(html,/<img|<script|onclick=/);assert.match(html,/&lt;img/);assert.match(html,/Record 2 unavailable/);assert.match(html,/Record 3 unavailable/);
 assert.equal(JSON.stringify(p.run('POSITIONS')),JSON.stringify(rows));p.run('editRow(0)');assert.doesNotMatch(p.elements.get('row_0').innerHTML,/<img|onclick=/);
});

test('missing edit token and duplicated identities disable ambiguous mutations',()=>{
 const p=page();p.load([{...ROW,record_etag:null}]);assert.match(p.elements.get('book').innerHTML,/data-invalid="true" disabled/);
 p.run('editRow(0)');assert.equal(p.run('EDITING'),null);
 p.load([ROW,{...ROW}]);p.run('editRow(1)');assert.equal(p.run('EDITING'),null);
});

test('malformed, duplicate and interrupted API responses never confirm a write',async()=>{
 for(const raw of ['{"ok":true,"ok":false}','{"ok":"true"}','{"ok":true']){
  const p=page();const pending=p.run('api("update_position",{symbol:"AAA",qty:20})');p.requests[0].resolve(new Response(raw));const result=await pending;
  assert.equal(result.body.ok,false);assert.equal(result.body.error_code,'RESPONSE_UNCONFIRMED');assert.equal(p.requests.length,1);
 }
 const p=page();const pending=p.run('api("update_position",{symbol:"AAA",qty:20})');p.reply(0,{ok:true},500);assert.equal((await pending).body.ok,false);
});

test('request stays on the fixed endpoint with redirect denial and no-store',async()=>{
 const p=page();const pending=p.run('api("list",{filter:"POSITION"})');const request=p.requests[0];
 assert.equal(request.url,'https://api.justhodl.ai/portfolio-admin');assert.equal(request.options.redirect,'error');assert.equal(request.options.cache,'no-store');assert.equal(request.options.method,'POST');
 p.reply(0,book([]));assert.equal((await pending).body.ok,true);
});
