const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),cp=require('node:child_process');
const root=path.resolve(__dirname,'..');
function actual(){
  for(const python of [process.env.PYTHON,'python3','python'].filter(Boolean)) {
    try{return JSON.parse(cp.execFileSync(python,[path.join(__dirname,'calls_consumer_boundary.py')],{encoding:'utf8',maxBuffer:2*1024*1024,timeout:30000}));}
    catch(e){if(e.code!=='ENOENT')throw e;}
  }
  throw new Error('Python required for actual producer boundary');
}
const fixture=actual(),now=Date.parse(fixture.at);
const element=()=>({style:{},textContent:'',innerHTML:'',querySelectorAll(){return[];}});
function document(){const nodes=new Map();return {nodes,getElementById(id){if(!nodes.has(id))nodes.set(id,element());return nodes.get(id);},querySelector(){return element();}};}
function date(){class Clock extends Date {static now(){return now;}}return Clock;}
async function brief(publicPacket,file='brief.html'){
  const html=fs.readFileSync(path.join(root,file),'utf8');
  const code=[...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).find(s=>s.includes('async function load()'));
  assert.ok(code,'actual Brief loader required');const doc=document();let rendered;
  const context={document:doc,Date:date(),marked:{parse(md){rendered=md;return 'rendered complete markdown';}},setInterval(){},
    fetch:async url=>{assert.match(url,/^\/data\/ai-brief-public\.json\?t=\d+$/);return {ok:true,json:async()=>structuredClone(publicPacket)};}};
  vm.runInNewContext(code,context);await new Promise(r=>setImmediate(r));
  return {doc,rendered};
}
function history(row,file='calls-page.js'){
  const doc=document(),context={document:doc,Date:date(),module:{exports:{}}};
  vm.runInNewContext(fs.readFileSync(path.join(root,file),'utf8'),context);
  context.module.exports.render({snapshots:[row]});return doc.getElementById('now-verb').textContent;
}
test('actual frozen producer reaches public evidence, Brief page, Calls history and Morning boundary',async()=>{
  const view=require('../jh-public-brief.js');
  for(const method of [fixture.public.generation_method,'warehouse_deterministic_v1']){
    const packet={...fixture.public,generation_method:method};
    assert.equal(view.state(packet,now).valid,true);assert.equal(view.state(packet,now).overdue,false);
    const result=await brief(packet);assert.equal(result.doc.getElementById('status').textContent,'updated · WAIT');
    assert.equal(result.rendered,fixture.public.brief_md);
    assert.equal(history({...fixture.row,generation_method:method}),'WAIT');
  }
});
test('unreviewed method versions are not accepted as compatible',async()=>{
  const view=require('../jh-public-brief.js');
  for(const method of ['warehouse_deterministic_v999','warehouse_deterministic_v2x',null,{},[]]){
    const packet={...fixture.public,generation_method:method};assert.equal(view.state(packet,now).valid,false);
    const result=await brief(packet);assert.equal(result.doc.getElementById('status').textContent,'error');
    assert.equal(result.rendered,undefined);assert.equal(history({...fixture.row,generation_method:method}),'ABSTAIN');
  }
});
test('WAIT presentation never conceals producer errors or invalid qualification',()=>{
  for(const method of [fixture.public.generation_method,'warehouse_deterministic_v1']){
    assert.equal(history({...fixture.row,generation_method:method,decision_status:'ERROR'}),'ERROR');
    assert.equal(history({...fixture.row,generation_method:method,decision_status:'VALID'}),'INVALID');
    assert.equal(history({...fixture.row,generation_method:method,call_verb:'LONG'}),'ABSTAIN');
  }
});
test('whole predecessor fixtures reproduce all three browser compatibility failures',async()=>{
  const context={module:{exports:{}}};vm.runInNewContext(fs.readFileSync(path.join(root,'tests/fixtures/pre-calls-v2-consumers-jh-public-brief.js.txt'),'utf8'),context);
  assert.equal(context.module.exports.state(fixture.public,now).valid,false);
  const old=await brief(fixture.public,'tests/fixtures/pre-calls-v2-consumers-brief.html.txt');
  assert.equal(old.doc.getElementById('status').textContent,'error');
  assert.equal(history(fixture.row,'tests/fixtures/pre-calls-v2-consumers-calls-page.js.txt'),'ABSTAIN');
});
