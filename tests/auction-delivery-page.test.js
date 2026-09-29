const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../auctions.html'),'utf8');
const code=html.slice(html.indexOf('  function commitSnapshot(D) {'),html.indexOf('  document.addEventListener("DOMContentLoaded", () => {$("desk-refresh")'));
function harness() {
  const controls=new Map(),phases=[],state={shell:null,trigger:null,previous:{generated_at:'2026-09-28T12:00:00Z'}};
  function node(kind){return {kind,rows:[],hidden:false,cloneNode:()=>node(kind),replaceWith(next){state[kind]=next;}};}
  state.shell=node('shell');state.shell.rows=['whole previous snapshot'];state.trigger=node('trigger');state.trigger.rows=['previous triggers'];
  const get=id=>id==='auction-desk-shell'?state.shell:id==='triggers'?state.trigger:controls.get(id);
  for(const id of ['desk-refresh','desk-delivery-status','desk-complete-packet','gx-overlay'])controls.set(id,{disabled:false,textContent:'',classList:{remove:()=>{}}});
  const scope={document:{getElementById:get},window:{__AUCTION_DESK:state.previous,renderTriggers:(d,target)=>target.rows.push(d.id)},$:get,
    renderRoot:null,tapeFilter:'All',loadGeneration:0,FEED:'fixture-full-packet',wireExplainer:()=>{},wireEvidence:()=>{},client:()=>state.client};
  for(const name of ['renderBanner','renderReactions','renderOps','renderBuybacks','renderCalendar','renderDays','renderTape','renderTenors']) {
    scope[name]=d=>{phases.push(name);scope.renderRoot.rows.push(d.id+':'+name);};
  }
  state.released=[];
  state.client={load:async()=>({id:'new',generated_at:'2026-09-29T12:00:00Z'}),source:()=>null,release:view=>state.released.push(view)};
  vm.createContext(scope);vm.runInContext(code,scope);return {scope,state,controls,phases};
}
test('all sections render offscreen before one complete snapshot becomes current',async()=>{
  const h=harness(),old=h.state.shell;
  await h.scope.load();assert.notEqual(h.state.shell,old);assert.equal(h.state.shell.rows.length,8);
  assert.deepEqual(old.rows,['whole previous snapshot']);assert.equal(h.scope.window.__AUCTION_DESK.id,'new');
  assert.equal(h.state.trigger.rows[0],'new');assert.equal(h.controls.get('desk-refresh').disabled,false);
});
test('late render failure leaves all previous sections and window snapshot unchanged',async()=>{
  const h=harness(),old=h.state.shell,trigger=h.state.trigger;
  h.scope.renderTenors=()=>{throw Error('synthetic late renderer failure');};
  await h.scope.load();assert.equal(h.state.shell,old);assert.equal(h.state.trigger,trigger);
  assert.equal(h.scope.window.__AUCTION_DESK,h.state.previous);assert.equal(h.scope.renderRoot,null);
  assert.match(h.controls.get('desk-delivery-status').textContent,/Keeping the complete snapshot/);
  assert.equal(h.controls.get('desk-refresh').disabled,false);
});
test('listener binding failure rolls back both desk and companion calendar',async()=>{
  const h=harness(),old=h.state.shell,trigger=h.state.trigger;
  h.scope.wireEvidence=()=>{throw Error('synthetic binding failure');};
  await h.scope.load();assert.equal(h.state.shell,old);assert.equal(h.state.trigger,trigger);
  assert.equal(h.scope.window.__AUCTION_DESK,h.state.previous);
});
test('failed transport keeps old dated values and does not call a renderer',async()=>{
  const h=harness();h.state.client.load=async()=>{throw Error('synthetic unavailable feed');};
  await h.scope.load();assert.equal(h.phases.length,0);assert.equal(h.scope.window.__AUCTION_DESK,h.state.previous);
  assert.match(h.controls.get('desk-delivery-status').textContent,/2026-09-28T12:00:00Z/);
});
test('late response from an older request cannot repaint a newer snapshot or status',async()=>{
  const h=harness();let finish;
  h.state.client.load=()=>new Promise(resolve=>{finish=resolve;});const older=h.scope.load();
  h.state.client.load=async()=>({id:'newer',generated_at:'2026-09-29T12:01:00Z'});await h.scope.load();
  const current=h.state.shell,status=h.controls.get('desk-delivery-status').textContent;
  finish({id:'older',generated_at:'2026-09-29T12:00:00Z'});await older;
  assert.equal(h.state.shell,current);assert.equal(h.scope.window.__AUCTION_DESK.id,'newer');
  assert.equal(h.controls.get('desk-delivery-status').textContent,status);
});
test('first-load failure leaves the shell hidden and enables explicit retry',async()=>{
  const h=harness();h.scope.window.__AUCTION_DESK=null;h.state.shell.hidden=true;
  h.state.client.load=async()=>{throw Error('synthetic unavailable feed');};await h.scope.load();
  assert.equal(h.state.shell.hidden,true);assert.equal(h.controls.get('desk-refresh').disabled,false);
  assert.match(h.controls.get('desk-delivery-status').textContent,/Desk snapshot unavailable/);
});

test('failed rendering releases only the incoming download and preserves old link and snapshot',async()=>{
  const h=harness(),link=h.controls.get('desk-complete-packet');link.href='blob:old';link.hidden=false;
  h.state.client.source=()=>({url:'blob:new',encoding:'json',bytes:20,sha256:'a'.repeat(64),download:'exact.json',integrity_basis:'received_legacy_bytes'});
  h.scope.wireEvidence=()=>{throw Error('listener failure');};
  await h.scope.load();assert.equal(link.href,'blob:old');assert.equal(link.hidden,false);
  assert.equal(h.state.released.length,1);assert.equal(h.state.released[0].id,'new');
  assert.equal(h.scope.window.__AUCTION_DESK,h.state.previous);
});

test('successful refresh binds download and releases the previous snapshot exactly once',async()=>{
  const h=harness(),link=h.controls.get('desk-complete-packet');
  h.state.client.source=()=>({url:'blob:exact',encoding:'json',bytes:20,sha256:'a'.repeat(64),download:'exact.json',integrity_basis:'received_legacy_bytes'});
  await h.scope.load();assert.equal(link.href,'blob:exact');assert.equal(link.download,'exact.json');assert.equal(link.hidden,false);
  assert.match(link.title,/Received bytes: 20/);assert.match(link.textContent,/exact packet/);
  assert.deepEqual(h.state.released,[h.state.previous]);
  assert.match(h.controls.get('desk-delivery-status').textContent,/Original-provider and manifest verification unavailable/);
});

test('unavailable download stays hidden and never points at a mutable packet',async()=>{
  const h=harness();await h.scope.load();const link=h.controls.get('desk-complete-packet');
  assert.equal(link.hidden,true);assert.equal(link.href,'');assert.match(h.controls.get('desk-delivery-status').textContent,/download unavailable/);
});

test('a late response reusing the cached displayed object cannot release its evidence context',async()=>{
  const h=harness(),shared={id:'same',generated_at:'2026-09-29T12:01:00Z'};let finish;
  h.state.client.load=()=>new Promise(resolve=>{finish=resolve;});const older=h.scope.load();
  h.state.client.load=async()=>shared;await h.scope.load();finish(shared);await older;
  assert.equal(h.scope.window.__AUCTION_DESK,shared);
  assert.deepEqual(h.state.released,[h.state.previous]);
});
