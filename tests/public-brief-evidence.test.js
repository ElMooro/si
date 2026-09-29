const test = require('node:test');
const assert = require('node:assert/strict');
const view = require('../jh-public-brief.js');
const now = Date.parse('2026-09-18T17:00:00Z');
const sha = 'a'.repeat(64), ref = {payload_sha256:sha,run_id:'calls-research-'+sha,bundle_key:'data/calls-research-runs/'+sha+'.json',bundle_sha256:'b'.repeat(64)};
function element(tag) {return {tag,children:[],attrs:{},style:{},textContent:'',appendChild(n){this.children.push(n);},replaceChildren(){this.children=[];},setAttribute(k,v){this.attrs[k]=v;}};}
function flatten(node) {return [node,...node.children.flatMap(flatten)];}
function packet(){return {generated_at:'2026-09-18T17:00:00Z',generation_method:'warehouse_deterministic_v1',call_verb:'WAIT',sizing_eligible:false,brief_md:'# Brief\n## DATA\n<img src=x>\n'+'Public observations. '.repeat(10),research_replay:ref,evidence:[{series_id:'data/ciss-stress.json#<img src=x>',source:'data/ciss-stress.json',value:0,unit:'index',observation_date:'2026-09-17',quality_status:'fresh',root_ids:['ECB:CISS']}],evidence_inventory:{root_groups:[{root_id:'ECB:CISS'}]}};}
test('Fresh pages cannot renew an old brief and unsafe replay paths are rejected',()=>{
 assert.equal(view.state(packet(),now).overdue,false);
 assert.equal(view.state({...packet(),generated_at:'2026-09-18T01:00:00Z'},now).overdue,true);
 assert.equal(view.bundlePath(ref),'/'+ref.bundle_key);
 assert.equal(view.bundlePath({...ref,payload_sha256:'../../private'}),null);
 assert.equal(view.bundlePath({...ref,bundle_key:'https://example.com'}),null);
});
test('Renderer preserves zero, dates, field identities and escaped text',()=>{
 const box=element('section');view.render({createElement:element},box,packet(),now);
 const nodes=flatten(box),texts=nodes.map(n=>n.textContent);
 assert.ok(texts.includes('0 index'));assert.ok(texts.includes('2026-09-17'));assert.ok(texts.includes('<img src=x>'));
 assert.ok(nodes.some(n=>n.tag==='a'&&n.href==='/data/ciss-stress.json'));
 assert.ok(nodes.some(n=>n.attrs['aria-label']==='Brief evidence table'));
 assert.ok(texts.some(t=>t.includes('0 eligible votes')));
 assert.ok(!nodes.some(n=>n.tag==='img'||Object.hasOwn(n,'innerHTML')));
});
test('A legacy proof without displayed-byte identity remains unverified',async()=>{
 assert.equal(await view.proofMatches({...ref,status:'reproduced'},new TextEncoder().encode(JSON.stringify(packet())).buffer,now),false);
});
