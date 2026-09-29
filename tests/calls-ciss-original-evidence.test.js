const test=require('node:test'),assert=require('node:assert/strict');
const cp=require('node:child_process'),path=require('node:path'),view=require('../jh-public-brief.js');
function actual(){
  for(const python of [process.env.PYTHON,'python3','python'].filter(Boolean)){
    try{return JSON.parse(cp.execFileSync(python,[path.join(__dirname,'test_calls_ciss_binding.py'),'--fixture'],{encoding:'utf8',maxBuffer:2*1024*1024,timeout:60000}));}
    catch(e){if(e.code!=='ENOENT')throw e;}
  }throw new Error('Python required for actual native compiler fixture');
}
const packet=actual();
function element(tag){return {tag,children:[],attrs:{},style:{},textContent:'',appendChild(n){this.children.push(n);},replaceChildren(){this.children=[];},setAttribute(k,v){this.attrs[k]=v;}};}
function flatten(n){return [n,...n.children.flatMap(flatten)];}
test('actual CISS compiler exposes seven originals, all 28 comparison baselines and complete manifest',()=>{
  const box=element('section');view.render({createElement:element},box,packet,Date.parse(packet.generated_at));
  const nodes=flatten(box),texts=nodes.map(n=>n.textContent),links=nodes.filter(n=>n.tag==='a');
  assert.equal(links.filter(n=>n.href.includes('/evidence/ecb/')).length,7);
  assert.equal(links.filter(n=>n.href.includes('/ciss-research/runs/')).length,1);
  assert.equal(texts.filter(t=>t.includes(' · source row (zero-based) ')).length,7);
  assert.equal(texts.filter(t=>t.includes(' index points; target ')).length,28);
  assert.ok(texts.some(t=>t.includes('All seven headline and contribution legs pass')));
  assert.ok(texts.some(t=>t.includes('1 matched, 1 mismatched, 0 unavailable')));
  assert.ok(texts.some(t=>t.includes('not count as seven independent votes')));
  assert.ok(texts.some(t=>t.includes('Correlation contribution')));
  assert.ok(!nodes.some(n=>Object.hasOwn(n,'innerHTML')));
});
test('expired or failed CISS withholds current use and rejects unsafe original and manifest links',()=>{
  const p=structuredClone(packet),original=p.original_source_lineage.ciss;
  original.current_headline_research_eligible=false;original.source_replay.manifest_key='data/prospective-outcomes.json';
  Object.values(original.original_sources)[0].key='private/account.json';
  const box=element('section');view.render({createElement:element},box,p,Date.parse(p.generated_at));
  assert.ok(flatten(box).some(n=>n.textContent==='Historical reconstruction only; current CISS research use is withheld.'));
  assert.equal(flatten(box).filter(n=>n.tag==='a'&&n.href.includes('/evidence/ecb/')).length,6);
  assert.ok(!flatten(box).some(n=>n.href?.includes('private/')||n.href?.includes('prospective-outcomes')));
  p.original_source_lineage.ciss={status:'unavailable'};view.render({createElement:element},box,p,Date.parse(p.generated_at));
  assert.ok(flatten(box).some(n=>n.textContent.startsWith('ECB original replay unavailable')));
  assert.equal(flatten(box).filter(n=>n.tag==='a'&&n.href.includes('/evidence/ecb/')).length,0);
  assert.equal(view.originalPath({key:'javascript:alert(1)',sha256:'a'.repeat(64)},'ecb'),null);
});
