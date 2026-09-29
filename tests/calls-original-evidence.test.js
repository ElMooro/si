const test=require('node:test'),assert=require('node:assert/strict');
const cp=require('node:child_process'),path=require('node:path');
const view=require('../jh-public-brief.js');
function actual(){
  for(const python of [process.env.PYTHON,'python3','python'].filter(Boolean)){
    try{return JSON.parse(cp.execFileSync(python,[path.join(__dirname,'test_calls_original_binding.py'),'--fixture'],{encoding:'utf8',maxBuffer:2*1024*1024,timeout:60000}));}
    catch(e){if(e.code!=='ENOENT')throw e;}
  }throw new Error('Python required for native original-source fixture');
}
const packet=actual();
function element(tag){return {tag,children:[],attrs:{},style:{},textContent:'',appendChild(n){this.children.push(n);},replaceChildren(){this.children=[];},setAttribute(k,v){this.attrs[k]=v;}};}
function flatten(n){return [n,...n.children.flatMap(flatten)];}
test('actual original-bound producer exposes all three sources and six immutable originals safely',()=>{
  const box=element('section');view.render({createElement:element},box,packet,Date.parse(packet.generated_at));
  const nodes=flatten(box),texts=nodes.map(n=>n.textContent),links=nodes.filter(n=>n.tag==='a'&&n.href.includes('/evidence/fred/'));
  assert.equal(links.length,6);
  for(const sid of ['WALCL','WTREGEN','RRPONTSYD'])assert.ok(texts.some(t=>t.startsWith(sid+' · ')));
  const data=packet.original_source_lineage.liquidity_flow;
  for(const source of Object.values(data.sources))assert.ok(texts.some(t=>t.includes(source.acquired_at)));
  assert.ok(texts.some(t=>t.includes('No allocation permission')));
  assert.ok(!nodes.some(n=>Object.hasOwn(n,'innerHTML')));
});
test('failed or expired originals never appear as current research and private links are not emitted',()=>{
  const copy=structuredClone(packet);copy.original_source_lineage.liquidity_flow.current_use.eligible=false;
  copy.original_source_lineage.liquidity_flow.sources.WALCL.originals.definition.key='private/account.json';
  const box=element('section');view.render({createElement:element},box,copy,Date.parse(copy.generated_at));
  assert.ok(flatten(box).some(n=>n.textContent.includes('Historical reconstruction only')));
  assert.equal(flatten(box).filter(n=>n.tag==='a'&&n.href.includes('/evidence/fred/')).length,5);
  copy.original_source_lineage.liquidity_flow={status:'unavailable'};view.render({createElement:element},box,copy,Date.parse(copy.generated_at));
  assert.ok(flatten(box).some(n=>n.textContent.includes('Original-source replay unavailable')));
  assert.equal(flatten(box).filter(n=>n.tag==='a'&&n.href.includes('/evidence/fred/')).length,0);
});
