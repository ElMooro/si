const {test}=require('node:test');
const assert=require('node:assert/strict');
const ui=require('../jh-report-research.js');
const now=Date.parse('2026-09-18T20:00:00Z');
const row={contract:'report-observations.v1',series_id:'ICSA',name:'Initial Claims',current_decimal:'196000',date:'2026-09-12',
  acquired_at:'2026-09-18T19:00:00Z',unit:'Number',frequency:'W',quality:{status:'fresh'},seasonal_adjustment:'Seasonally Adjusted',current_row_index:0,
  changes:{month:{change_decimal:'2000',change_unit:'Number',current_date:'2026-09-12',baseline_date:'2026-08-08',baseline_decimal:'194000',pct_change:1.030928}},
  evidence:{observations:{key:'data/evidence/source.bin.gz'},definition:{key:'data/evidence/definition.bin.gz'}}};
const packet={contract:'report-observations.v1',catalog:{ICSA:{category:'macro',display_name:'Initial Claims'},OTHER:{display_name:'Other'}},measurements:{ICSA:row},errors:{OTHER:'HTTPError'},generated_at:'2026-09-18T19:10:00Z'};
test('claims retain Number units, dated baseline and original row; missing series are explicit',()=>{
  const html=ui.render(packet,'','month',now);
  assert.match(html,/196000/);assert.doesNotMatch(html,/196000K/);assert.match(html,/Baseline 2026-08-08/);assert.match(html,/observation row 0/);
  assert.match(html,/Source unavailable/);assert.match(html,/1 \/ 2/);assert.doesNotMatch(html,/NEUTRAL|50\/100|probability/i);
});
test('search matches exact identity and description without injecting provider content',()=>{
  const p=structuredClone(packet);p.measurements.ICSA.name='<img onerror=alert(1)>';
  const html=ui.render(p,'ICSA','month',now);assert.doesNotMatch(html,/<img|Other<\/h3/);assert.match(html,/&lt;img/);
  assert.equal(ui.link('data/../private','x'),'Evidence unavailable');assert.equal(ui.link('javascript:bad','x'),'Evidence unavailable');
});
test('old source and observation cannot inherit a fresh wrapper',()=>{
  assert.equal(ui.status({...row,acquired_at:'2026-09-16T19:00:00Z'},now),'stale source');
  assert.equal(ui.status({...row,date:'2025-09-12'},now),'stale observation');
  assert.equal(ui.status({...row,current_decimal:null},now),'unavailable');
});
test('failed refresh clears prior values and returns no cached packet',async()=>{
  const host={innerHTML:'old number'};
  assert.equal(await ui.refresh(host,async()=>({ok:false})),null);
  assert.doesNotMatch(host.innerHTML,/old number/);assert.match(host.innerHTML,/cleared/);
});
