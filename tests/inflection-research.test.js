const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const api=require('../jh-inflection-research.js');
const now=Date.parse('2026-09-19T06:00:00Z'),stamp='2026-09-19T05:00:00Z';
function packet(){return {contract:api.CONTRACT,generated_at:stamp,source_generated_at:stamp,series:Object.fromEntries(['WALCL','WTREGEN','RRPONTSYD'].map(sid=>[sid,{name:sid,current_decimal:'0',last_observed_value:'0',date:'2026-09-18',frequency:'D',acquired_at:stamp,available:true,quality:{status:'fresh'},unit:'Billions of US Dollars'}])),net_liquidity:{net:0,components:{}},calendar_research:{weekly_observations:14,archive_references:Object.fromEntries(['WALCL','WTREGEN','RRPONTSYD'].map(sid=>[sid,{generated_at:stamp}])),latest:{start:'2026-06-19',end:'2026-09-18',present_weekly_samples:14,slope_usd_mn_per_week:0,acceleration_usd_mn_per_week2:0,z_3y:{z:null,status:'zero_variance'}},history:{'2026-09-18':{slope_usd_mn_per_week:0}},archive_collection_key:'data/vintage-research/collections/'+'a'.repeat(64)+'.json'},quality:{fresh_series:3,expected_series:20},decision:{reason:'Unqualified'},retained_context:{}};}
test('legacy scores rejected and page has no paid/AI panel route',()=>{
 assert.throws(()=>api.render({usd:{impulse_z:9}}));
 const html=fs.readFileSync('inflection.html','utf8');assert.match(html,/WAIT means|Calls vote/);assert.ok(!html.includes('jh-page-ai'));assert.ok(!html.includes('aiPanel('));
});
test('zero is rendered as data and exact native units survive',()=>{
 const v=api.render(packet(),now);assert.match(v.summary,/<strong>0<\/strong>/);assert.match(v.series,/Billions of US Dollars/);assert.match(v.summary,/week²/);
 assert.equal(api.num(null),'—');assert.equal(api.num('0'),'0');
});
test('expired observations cannot remain current but dated research is inspectable',()=>{
 const p=packet();let v=api.render(p,now+10*86400000);
 assert.ok(!v.summary.includes('<strong>0</strong>'));assert.match(v.archive,/expired/);assert.match(v.series,/Last observed: 0/);
 p.generated_at='2099-01-01T00:00:00Z';assert.equal(api.fresh(p.series.WALCL,p,now),'invalid_clock');
});
test('evidence paths and provider text are escaped',()=>{
 const p=packet();p.series.WALCL.name='<img onerror=alert(1)>';p.series.WALCL.evidence={observations:{key:'javascript:alert(1)'}};
 const v=api.render(p,now);assert.ok(!v.series.includes('<img'));assert.ok(!v.series.includes('href="javascript:'));
 assert.equal(api.path('data/evidence/../secret'),null);assert.equal(api.path('data/evidence/a.bin.gz'),'/data/evidence/a.bin.gz');
});
test('calendar chart preserves missing weeks as gaps and inspector pins collection',()=>{
 const chart=api.chart({'2026-09-04':{slope_usd_mn_per_week:1},'2026-09-11':{slope_usd_mn_per_week:null},'2026-09-18':{slope_usd_mn_per_week:2}});
 assert.match(chart,/d="M[^"]+ M|d="M[^L"]+M/);
 const html=api.inspect(packet(),'2026-09-18');assert.match(html,/day=2026-09-17/);assert.match(html,new RegExp('run='+'a'.repeat(64)));
});
