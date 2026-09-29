const test=require('node:test'),assert=require('node:assert/strict');
const cp=require('node:child_process'),path=require('node:path'),view=require('../jh-public-brief.js');
function actual(){
  for(const python of [process.env.PYTHON,'python3','python'].filter(Boolean)){
    try{return JSON.parse(cp.execFileSync(python,[path.join(__dirname,'test_calls_tic_binding.py'),'--fixture'],{encoding:'utf8',maxBuffer:2*1024*1024,timeout:60000}));}
    catch(e){if(e.code!=='ENOENT')throw e;}
  }throw new Error('Python required for actual native compiler fixture');
}
const packet=actual();
function element(tag){return {tag,children:[],attrs:{},style:{},textContent:'',appendChild(n){this.children.push(n);},replaceChildren(){this.children=[];},setAttribute(k,v){this.attrs[k]=v;}};}
function flatten(n){return [n,...n.children.flatMap(flatten)];}
test('actual TIC compiler renders every selected month, full original, holder mix and net sign',()=>{
  const box=element('section');view.render({createElement:element},box,packet,Date.parse(packet.generated_at));
  const nodes=flatten(box),texts=nodes.map(n=>n.textContent),links=nodes.filter(n=>n.tag==='a');
  assert.equal(links.filter(n=>n.href.includes('/evidence/tic/')).length,1);
  assert.equal(links.filter(n=>n.href.includes('/tic-research/runs/')).length,1);
  assert.equal(links.filter(n=>n.href.includes('/tic-research/outputs/')).length,1);
  assert.equal(texts.filter(t=>t.includes(' · native value ')).length,120);
  assert.equal(texts.filter(t=>t.startsWith('Exact window: ')).length,10);
  assert.ok(texts.some(t=>t.includes('official 12; private 36 USD billions')));
  assert.ok(texts.some(t=>t.includes('same twelve months: 18 USD billions')));
  assert.ok(texts.some(t=>t.includes('Short-term Treasuries · 0 USD billions')));
  assert.ok(texts.some(t=>t.includes('363 windows and 72 component reconciliations')));
  assert.ok(texts.some(t=>t.includes('99 incomplete historical windows')));
  assert.ok(texts.some(t=>t.includes('Never add them as separate flows')));
  assert.ok(!nodes.some(n=>Object.hasOwn(n,'innerHTML')));
});
test('TIC expiry, missing months and invalid links cannot look like qualified current evidence',()=>{
  const p=structuredClone(packet),original=p.original_source_lineage.tic;
  original.current_use.eligible=false;original.source_replay.manifest_key='data/prospective-outcomes.json';
  original.original_sources.bulk.evidence.key='private/account.json';original.complete_source_output_key='javascript:alert(1)';
  const window=original.windows.total,month=window.months[0];window.original_observations[month]=null;window.missing_months=[month];window.total_usd_bn_decimal=null;
  const box=element('section');view.render({createElement:element},box,p,Date.parse(p.generated_at));
  const nodes=flatten(box);
  assert.ok(nodes.some(n=>n.textContent==='Historical reconstruction only; current TIC research use is withheld.'));
  assert.ok(nodes.some(n=>n.textContent.startsWith(month+' · native value missing')));
  assert.ok(nodes.some(n=>n.textContent.includes('Missing months: '+month)));
  assert.ok(!nodes.some(n=>n.href?.includes('private/')||n.href?.includes('prospective-outcomes')||n.href?.startsWith('javascript:')));
  p.original_source_lineage.tic={status:'unavailable'};view.render({createElement:element},box,p,Date.parse(p.generated_at));
  assert.ok(flatten(box).some(n=>n.textContent.startsWith('TIC original replay unavailable')));
  assert.equal(flatten(box).filter(n=>n.tag==='a'&&n.href.includes('/tic-research/')).length,0);
});
