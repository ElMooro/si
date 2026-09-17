const assert=require('node:assert/strict');
const view=require('../calls-page.js');
const now=Date.parse('2026-09-17T21:00:00Z');
const base={schema_version:'calls.v2',timestamp:'2026-09-17T20:00:00Z',expires_at:'2026-09-18T00:00:00Z',
  decision_status:'ABSTAIN',decision_reason:'decision_model_not_validated',call_verb:'WAIT',sizing_eligible:false};
assert.equal(view.state(base,now).label,'ABSTAIN');
assert.equal(view.state({...base,decision_status:'ERROR',decision_reason:'empty_or_stub_brief'},now).label,'ERROR');
assert.equal(view.state({call_verb:'WAIT'},now).label,'LEGACY');
assert.equal(view.state({...base,timestamp:'2026-09-18T20:00:00Z'},now).label,'INVALID');
const valid={...base,decision_status:'VALID',decision_eligible:true,sizing_eligible:true,validation_status:'validated',
  model_version:'fixture',evidence_ids:['root'],call_verb:'LONG'};
assert.equal(view.state(valid,now).eligible,true);
assert.equal(view.state({...valid,expires_at:'2026-09-17T20:30:00Z'},now).label,'EXPIRED');
assert.equal(view.state({...valid,evidence_ids:[]},now).eligible,false);
assert.match(view.health({...base,timestamp:'2026-09-17T10:00:00Z'},now),/^overdue/);
const nodes=new Map();
global.document={getElementById:id=>{if(!nodes.has(id))nodes.set(id,{style:{},textContent:'',innerHTML:''});return nodes.get(id);}};
const originalNow=Date.now;Date.now=()=>now;
view.render({snapshots:[{...base,khalid_score:0,phase:'<img src=x onerror=alert(1)>',weighted_mean_accuracy:null}]});
assert.equal(nodes.get('now-verb').textContent,'ABSTAIN');
assert.match(nodes.get('now-ctx').innerHTML,/Khalid Index<\/span><span class="val">0<\/span>/);
assert.ok(!nodes.get('now-ctx').innerHTML.includes('<img'));
assert.equal(nodes.get('status').textContent,'abstain');
assert.equal(nodes.get('changes-section').style.display,'none');
view.render({snapshots:[{...valid,expires_at:'2026-09-17T20:30:00Z'}]});
assert.equal(nodes.get('now-verb').textContent,'EXPIRED');
Date.now=originalNow;
console.log('Calls page contract and rendering tests passed: 14 assertions');
