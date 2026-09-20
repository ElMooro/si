const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const ui=require('../jh-risk-regime-research.js');
const packet=()=>({contract:'risk-regime-research.v1',generated_at:'2026-09-20T07:00:00Z',measurements:{VIXCLS:{series_id:'VIXCLS',value:null,unit:'Index',quality:{status:'unavailable'},history:[]}},option_cohorts:{},context:{},pd_settlement_fails:{},source_failures:{}});
test('missing or legacy data cannot render a neutral score or size',()=>{
 assert.match(ui.render({risk_regime_score:99}),/contract has not loaded/);
 const html=ui.render(packet(),Date.parse('2026-09-20T07:01:00Z'));
 assert.match(html,/WAIT/);assert.match(html,/Unavailable/);assert.doesNotMatch(html,/size ×|MILD_RISK_ON|NaN|undefined/);
});
test('HTML and source links are escaped and constrained',()=>{
 const p=packet();p.measurements.VIXCLS.title='<img src=x onerror=alert(1)>';
 p.measurements.VIXCLS.originals={definition:{key:'javascript:alert(1)'}};
 const html=ui.render(p);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|href="javascript/);
 assert.equal(ui.link('data/../secret','x'),'x unavailable');assert.equal(ui.fmt(false),'Unavailable');assert.equal(ui.fmt(0),'0');
});
test('stale wrapper warns separately from source age',()=>{
 assert.match(ui.render(packet(),Date.parse('2026-09-23T00:00:00Z')),/Packet stale/);
 assert.doesNotMatch(ui.render(packet(),Date.parse('2026-09-20T08:00:00Z')),/Packet stale/);
});
test('incomplete options cannot imply whole-market ratio or synchronized skew',()=>{
 const p=packet();p.option_cohorts.SPY={symbol:'SPY',pagination_complete:false,contracts:250,pages:1,expiries:[{expiry:'2026-10-16',contracts:250,volume_rows_in_session:150,volume_coverage:.6,put_call_volume_ratio:null,skew_25delta_vol_points:.8}],universe:{}};
 const html=ui.render(p);assert.match(html,/Incomplete \/ unavailable/);assert.match(html,/60% \(150\/250\)/);assert.match(html,/0.8 · undated/);assert.match(html,/never replaced by open interest/);
});
test('history preserves visible gaps and dates',()=>{
 const html=ui.history([{date:'2026-09-01',value:0},{date:'2026-09-02',value:null},{date:'2026-09-03',value:1}],'Index');
 assert.equal((html.match(/ M /g)||[]).length,2);assert.doesNotMatch(html,/ L /);assert.match(html,/Missing rows break/);
});
test('history uses elapsed calendar time instead of equal-width provider rows',()=>{
 const html=ui.history([{date:'2026-09-01',value:0},{date:'2026-09-02',value:1},{date:'2026-09-11',value:2}],'index points');
 assert.match(html,/L 66\.40 /);assert.doesNotMatch(html,/L 300\.00 /);
});
test('open-interest timing is visible before expanding methodology',()=>{
 const p=packet();p.option_cohorts.HYG={symbol:'HYG',pagination_complete:true,contracts:2,pages:1,expiries:[{expiry:'2026-10-16',contracts:2,volume_rows_in_session:1,volume_coverage:.5,put_call_open_interest_ratio:4.6}],universe:{}};
 const html=ui.render(p);assert.match(html,/4.6 · undated/);assert.match(html,/1 page · expiry/);assert.match(html,/OI and IV observation timestamps are not supplied/);
});
test('page loads the reviewed renderer and coherent single packet',()=>{
 const html=fs.readFileSync('risk-regime.html','utf8');assert.match(html,/jh-risk-regime-research\.js\?v=50/);assert.match(html,/aria-live="polite"/);
 assert.doesNotMatch(html,/Loading ~40|size multiplier|fetch\(/);
});
