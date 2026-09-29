const test=require('node:test'),assert=require('node:assert/strict');
const cp=require('node:child_process'),path=require('node:path'),view=require('../jh-public-brief.js');
function actual(){
  for(const python of [process.env.PYTHON,'python3','python'].filter(Boolean)){
    try{return JSON.parse(cp.execFileSync(python,[path.join(__dirname,'test_calls_fails_binding.py'),'--fixture'],{encoding:'utf8',maxBuffer:2*1024*1024,timeout:60000}));}
    catch(e){if(e.code!=='ENOENT')throw e;}
  }throw new Error('Python required for actual native compiler fixture');
}
const packet=actual();
function element(tag){return {tag,children:[],attrs:{},style:{},textContent:'',appendChild(n){this.children.push(n);},replaceChildren(){this.children=[];},setAttribute(k,v){this.attrs[k]=v;}};}
function flatten(n){return [n,...n.children.flatMap(flatten)];}
test('actual FR2004-bound Calls producer renders six fields, four source rows, eight originals and complete history link',()=>{
  const box=element('section');view.render({createElement:element},box,packet,Date.parse(packet.generated_at));
  const nodes=flatten(box),texts=nodes.map(n=>n.textContent),links=nodes.filter(n=>n.tag==='a');
  assert.equal(links.filter(n=>n.href.includes('/evidence/fr2004/')).length,8);
  assert.equal(links.filter(n=>n.href.includes('/fails-research/outputs/')).length,1);
  assert.equal(texts.filter(t=>t.includes(' · source row (zero-based) ')).length,4);
  assert.ok(texts.some(t=>t.includes('FTD 100 + FTR 101 = gross 201')));
  assert.ok(texts.some(t=>t.includes('FTD 202 + FTR 204 = gross 406')));
  assert.ok(texts.some(t=>t.includes('Never add these scopes')));
  assert.ok(texts.some(t=>t.includes('1272 original rows across 12 series')));
  assert.ok(!nodes.some(n=>Object.hasOwn(n,'innerHTML')));
});
test('expired or failed FR2004 originals withhold current use and reject unsafe links',()=>{
  const p=structuredClone(packet),original=p.original_source_lineage.settlement_fails;
  for(const scope of Object.values(original.scopes))scope.current_use.eligible=false;
  original.complete_source_output_key='data/prospective-outcomes.json';original.original_sources.observations.evidence.key='private/account.json';
  const box=element('section');view.render({createElement:element},box,p,Date.parse(p.generated_at));
  assert.equal(flatten(box).filter(n=>n.textContent==='Historical report only; current research use is withheld.').length,2);
  assert.equal(flatten(box).filter(n=>n.tag==='a'&&n.href.includes('/evidence/fr2004/')).length,7);
  assert.ok(!flatten(box).some(n=>n.href?.includes('private/')||n.href?.includes('prospective-outcomes')));
  p.original_source_lineage.settlement_fails={status:'unavailable'};view.render({createElement:element},box,p,Date.parse(p.generated_at));
  assert.ok(flatten(box).some(n=>n.textContent.startsWith('FR2004 original replay unavailable')));
  assert.equal(flatten(box).filter(n=>n.tag==='a'&&n.href.includes('/evidence/fr2004/')).length,0);
  assert.equal(view.originalPath({key:'javascript:alert(1)',sha256:'a'.repeat(64)},'fr2004'),null);
});
