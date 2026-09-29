const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const inspector=require('../jh-data-inspector.js');
const registry=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'../config/page-data-contracts.json'),'utf8'));
const names=['capitulation','market-extremes'];

test('each dedicated Extremes page exposes its actual primary current packet',()=>{
 const manifest=registry();
 for(const name of names){
  const contract=inspector.selectedContract(manifest,inspector.inspectionSelection(name+'.html',''));
  assert.deepEqual(contract.primary_producers,['justhodl-'+name]);
  const entry=contract.outputs.find(x=>x.engine==='justhodl-'+name&&x.key==='data/'+name+'.json');
  assert.ok(entry,'Current primary packet is inspectable');assert.equal(entry.access,'public');
  assert.equal(entry.entrypoint_reachability.status,'reachable_in_source');assert.equal(entry.entrypoint_reachability.runtime_verified,false);
  assert.ok(entry.ownership_evidence.some(x=>x.repository_path==='aws/shared/extremes_native_store.py'&&x.entrypoint_reachability==='reachable_in_source'));
 }
});

test('primary access preserves legacy history uncertainty and incomplete runtime qualification',()=>{
 for(const name of names){
  const contract=registry().pages[name+'.html'];
  const history=contract.outputs.find(x=>x.key==='data/'+name+'-history.json');
  assert.ok(history,'Retain the legacy history candidate');assert.equal(history.entrypoint_reachability.status,'unproven');
  assert.equal(contract.runtime_coverage,'unverified_until_opened');assert.match(contract.primary_output_status,/PARTIAL/);
  assert.ok(contract.primary_unresolved_write_count>0||contract.primary_unindexed_family_count>0);
 }
});

test('primary dependency inspection renders complete source references without fetching packets',()=>{
 class Element{
  constructor(tag){this.tagName=tag;this.children=[];this._text='';this.attrs={};}
  set textContent(value){this._text=String(value);this.children=[];}
  get textContent(){return this._text+this.children.map(n=>n.textContent||'').join('');}
  append(...nodes){this.children.push(...nodes);}
  setAttribute(k,v){this.attrs[k]=v;}
 }
 const previousDocument=global.document,previousFetch=global.fetch;let requests=0;
 global.document={createElement:tag=>new Element(tag)};global.fetch=()=>{requests++;throw Error('No packet read permitted');};
 try{
  for(const name of names){
   const groups=registry().pages[name+'.html'].dependency_groups;
   assert.equal(groups.length,1);assert.equal(groups[0].engine,'justhodl-'+name);
   assert.equal(groups[0].calls_eligible,false);assert.equal(groups[0].sizing_eligible,false);assert.equal(groups[0].independent_evidence_count,null);
   const view=inspector.dependencyView(groups);assert.match(view.textContent,/runtime reads|runtime consumption|total input coverage/);
   for(const ref of groups[0].public_references)assert.ok(view.textContent.includes(ref.key),ref.key);
  }
  assert.equal(requests,0);
 }finally{global.document=previousDocument;global.fetch=previousFetch;}
});
