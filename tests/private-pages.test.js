const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
function section(file,start,end){const s=fs.readFileSync(path.join(__dirname,'..',file),'utf8');const a=s.indexOf(start),b=s.indexOf(end,a);assert.ok(a>=0&&b>a);return s.slice(a,b);}
test('Brain sign-out clears rendered personal state and performs no public fallback fetch',async()=>{
  const c=vm.createContext({STATE:{notes:[{id:'prior-user'}]},window:{JustHodlAuth:{getUser(){return null}}},brainUid(){return null},dbg(){},renderStatus(){},renderCats(){},renderFilters(){},renderNotes(){},fetch(){throw new Error('signed-out request')}});
  c.JustHodlAuth=c.window.JustHodlAuth;vm.runInContext(section('brain.html','async function load(){','async function renderStatus(){'),c);await c.load();
  assert.equal(c.STATE.notes.length,0);assert.equal(c.STATE.readonly,true);
});
test('Brain retry uses authenticated private mirror and never a raw S3 fallback',async()=>{
  const calls=[];const c=vm.createContext({STATE:{notes:[]},window:{JustHodlAuth:{getUser(){return {id:'fixture'}}}},brainUid(){return 'fixture'},uidQ(){return 'uid=fixture'},authHdr:async()=>({Authorization:'Bearer fixture'}),dbg(){},renderStatus(){},renderCats(){},renderFilters(){},renderNotes(){},PROXY:'https://api.invalid',PROXY_FALLBACK:'https://worker.invalid',AbortController,setTimeout,clearTimeout,Date,
    fetch:async(url,options)=>{calls.push({url,options});return calls.length===1?new Response('{}',{status:401}):Response.json({notes:[{id:'fixture'}],generated_at:new Date().toISOString()});}});
  c.JustHodlAuth=c.window.JustHodlAuth;vm.runInContext(section('brain.html','async function load(){','async function renderStatus(){'),c);await c.load();
  assert.equal(calls.length,2);assert.ok(calls[1].url.includes('/private-artifact?kind=brain'));assert.equal(calls[1].options.headers.Authorization,'Bearer fixture');assert.equal(c.STATE.notes.length,1);assert.equal(c.STATE.readonly,true);
});
test('Journal sign-out clears entries and grades without retrieving a shared mirror',async()=>{
  const c=vm.createContext({ENTRIES:[{id:'old'}],GRADED:{private:true},BASE_REVISION:9,READONLY:false,brainSession(){return false},renderTrack(){},renderEntries(){},badge(){},fetch(){throw new Error('signed-out request')}});
  vm.runInContext(section('journal.html','async function load(){','function openPin()'),c);await c.load();
  assert.equal(c.ENTRIES.length,0);assert.equal(c.GRADED,null);assert.equal(c.READONLY,true);
});
test('Journal concurrent revision merges only new additions and resubmits the server revision',async()=>{
  const calls=[],server={id:'existing',locked:true,thesis:'locked server decision'},draft={id:'draft',thesis:'new decision'};
  const c=vm.createContext({_jsaving:false,_jpending:false,ENTRIES:[draft],BASE_REVISION:0,brainSession(){return true},badge(){},renderEntries(){},PROXY_FALLBACK:'https://worker.invalid',uidQ(){return ''},authHdr:async()=>({Authorization:'Bearer fixture'}),setTimeout(){throw new Error('unbounded automatic retry')},Set,
    fetch:async(url,opts)=>{const body=JSON.parse(opts.body);calls.push(body);return calls.length===1?Response.json({error:'revision conflict',current:{revision:1,entries:[server]}},{status:409}):Response.json({ok:true,revision:2,entries:body.entries});}});
  vm.runInContext(section('journal.html','async function _jdoSave(){','function save(){'),c);await c._jdoSave();
  assert.equal(calls.length,2);assert.equal(calls[1].baseRevision,1);assert.deepEqual(calls[1].entries,[server,draft]);assert.equal(c.BASE_REVISION,2);assert.equal(c._jsaving,false);
});

test('Brain late load after sign-out cannot restore personal notes',async()=>{
  let current='user-a',finish;const c=vm.createContext({STATE:{notes:[]},window:{JustHodlAuth:{getUser(){return current?{id:current}:null}}},brainUid(){return current},uidQ(){return 'uid='+current},authHdr:async()=>({Authorization:'Bearer fixture'}),dbg(){},renderStatus(){},renderCats(){},renderFilters(){},renderNotes(){},PROXY:'https://api.invalid',PROXY_FALLBACK:'https://worker.invalid',AbortController,setTimeout,clearTimeout,Date,
    fetch:()=>new Promise(resolve=>finish=resolve)});c.JustHodlAuth=c.window.JustHodlAuth;
  vm.runInContext(section('brain.html','async function load(){','async function renderStatus(){'),c);const prior=c.load();await new Promise(setImmediate);current=null;await c.load();finish(Response.json({notes:[{id:'prior-personal-note'}]}));await prior;assert.equal(c.STATE.notes.length,0);
});
test('Journal late load and save cannot populate a replacement account',async()=>{
  let identity='user-a',finish;const c=vm.createContext({ENTRIES:[],GRADED:null,BASE_REVISION:0,READONLY:false,brainSession(){return !!identity},uidQ(){return 'uid='+identity},renderTrack(){},renderEntries(){},badge(){},authHdr:async()=>({Authorization:'Bearer fixture'}),PROXY_FALLBACK:'https://worker.invalid',Date,
    fetch:()=>new Promise(resolve=>finish=resolve)});
  vm.runInContext(section('journal.html','async function load(){','function openPin()'),c);const pending=c.load();await new Promise(setImmediate);identity=null;await c.load();finish(Response.json({entries:[{id:'old-private'}],revision:7}));await pending;assert.equal(c.ENTRIES.length,0);assert.equal(c.GRADED,null);
});
test('Brain queued notes retain originating UID and cannot acquire replacement-account credentials',async()=>{
  let identity='user-a',finish,fetches=0;const c=vm.createContext({brainUid(){return identity},uidQ(){return 'uid='+identity},authHdr:()=>new Promise(resolve=>finish=resolve),PROXY:'https://api.invalid',PROXY_FALLBACK:'https://worker.invalid',fetch(){fetches++;throw new Error('cross-account write')},dbg(){}});
  vm.runInContext(section('brain.html','async function _putBrain(', 'async function _runQueue(){'),c);
  const pending=c._putBrain({ownerUid:'user-a',payload:{note:{id:'draft',text:'synthetic private draft'}}});await new Promise(setImmediate);identity='user-b';finish({Authorization:'Bearer user-b'});const result=await pending;assert.equal(result._status,401);assert.equal(fetches,0);
});
