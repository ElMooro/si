const {test}=require('node:test');
const assert=require('node:assert/strict');
const ui=require('../jh-research-brief.js');
const now=Date.parse('2026-09-18T22:00:00Z');
const row={series_id:'ICSA',metric:'Initial Claims',value:'196000',last_observed_value:'196000',unit:'Number',status:'fresh',observed_at:'2026-09-12',acquired_at:'2026-09-18T20:00:00Z',frequency:'W',current_row_index:0,changes:{month:{change_decimal:'-16000',change_unit:'Number',baseline_date:'2026-08-08',baseline_decimal:'212000',target_date:'2026-08-12'}},evidence:{observations:{key:'data/evidence/actual.bin.gz'},definition:{key:'data/evidence/definition.bin.gz'}}};
const packet={contract:'research-intelligence.v1',generated_at:'2026-09-18T21:00:00Z',source_generated_at:'2026-09-18T20:00:00Z',metrics_table:[row],brief_items:[row],replay:{manifest_key:'data/research-intelligence/runs/proof.json'}};
test('dated exact claims and baseline appear without invented regime or portfolio weights',()=>{
  const html=ui.render(packet,now);
  assert.match(html,/196000.*Number/);assert.doesNotMatch(html,/196000K|BULL|50.*100|raise cash/);
  assert.match(html,/WAIT — abstain/);assert.match(html,/2026-08-08/);assert.match(html,/observation row 0/);
  assert.match(html,/Replay this brief/);assert.match(html,/private portfolio/);
});
test('browser independently withholds stale values even when packet still calls them fresh',()=>{
  const html=ui.render(packet,now+48*3600000);
  assert.match(html,/0 \/ 1 SERIES FRESH/);assert.match(html,/Last observed: 196000/);
  assert.doesNotMatch(html,/class="brief-value">196000/);
  assert.equal(ui.status({...row,observed_at:'2026-09-21'},packet,now),'invalid_clock');
});
test('provider content is escaped and evidence links cannot leave the allowed public namespaces',()=>{
  const input=structuredClone(packet);input.brief_items[0].metric='<img onerror=alert(1)>';
  const html=ui.render(input,now);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
  for(const key of ['javascript:bad','data/private/owner.json','data/evidence/../private.json','https://evil.test'])assert.equal(ui.link(key,'x'),'Evidence unavailable');
});
test('old legacy packet or failed refresh clears previous recommendations',async()=>{
  const host={innerHTML:'old allocation'};
  assert.equal(await ui.refresh(host,async()=>({ok:true,json:async()=>({version:'3.0',scores:{khalid_index:99}})}),now),null);
  assert.match(host.innerHTML,/cleared/);assert.doesNotMatch(host.innerHTML,/old allocation/);
  assert.equal(await ui.refresh(host,async()=>({ok:false}),now),null);
});
