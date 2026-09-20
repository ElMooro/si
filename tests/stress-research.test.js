const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const ui=require('../jh-stress-research.js');
const packet=()=>({contract:'stress-research.v1',generated_at:'2026-09-20T08:00:00Z',measurements:{KCFSI:{series_id:'KCFSI',group:'Published financial-condition indices',value:null,frequency_short:'M',frequency:'Monthly',observation_period:'2026-08',quality:{status:'unavailable'},history:[]}},spreads:{},context:{},source_failures:{}});
test('legacy score and missing measurements cannot become neutral decisions',()=>{
 assert.match(ui.render({jsi:99}),/Verified stress research unavailable/);const html=ui.render(packet());
 assert.match(html,/WAIT/);assert.match(html,/Unavailable/);assert.doesNotMatch(html,/NaN|undefined|size ×|buy QQQ/);
 assert.equal(ui.fmt(false),'Unavailable');assert.equal(ui.fmt(0),'0');
});
test('monthly period is visibly distinct from daily observation',()=>{
 assert.match(ui.render(packet()),/Reference month 2026-08/);
});
test('source and provider links are constrained and content escaped',()=>{
 const p=packet();p.measurements.KCFSI.title='<img src=x onerror=alert(1)>';p.measurements.KCFSI.method_url='javascript:alert(1)';
 const html=ui.render(p);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|href="javascript/);
 assert.equal(ui.link('data/../private','x'),'x unavailable');
});
test('stale packet clock is visible separately from source age',()=>{
 assert.match(ui.render(packet(),'2',Date.parse('2026-09-22T00:00:00Z')),/Packet stale/);
 assert.doesNotMatch(ui.render(packet(),'2',Date.parse('2026-09-20T09:00:00Z')),/Packet stale/);
});
test('calendar chart preserves missing rows and irregular spacing',()=>{
 const gap=ui.chart([{date:'2026-09-01',value:0},{date:'2026-09-02',value:null},{date:'2026-09-11',value:2}],'index points','all');
 assert.equal((gap.match(/ M /g)||[]).length,2);assert.doesNotMatch(gap,/ L /);
 const line=ui.chart([{date:'2026-09-01',value:0},{date:'2026-09-02',value:1},{date:'2026-09-11',value:2}],'index points','all');
 assert.match(line,/L 66\.40 /);assert.match(line,/Calendar spacing/);
});
test('longer ranks with insufficient history remain unavailable',()=>{
 const p=packet();p.measurements.KCFSI.percentiles={'5y':{value:null,n_finite:36,n_missing:0,complete_span:false}};
 const html=ui.render(p);assert.match(html,/5y historical rank<\/dt><dd>Unavailable/);assert.doesNotMatch(html,/null\/100/);
});
test('page loads one native public packet and offers explicit scenarios',()=>{
 const html=fs.readFileSync('jsi.html','utf8'),script=fs.readFileSync('jh-stress-research.js','utf8');
 assert.match(html,/jh-stress-research\.js\?v=51/);assert.match(html,/aria-live="polite"/);
 assert.doesNotMatch(html,/percentile since 1990|atlas:|fetch\(/);assert.match(script,/fetch\('\/data\/jsi.json'/);
 assert.match(ui.render(packet()),/position-sizer.html/);
});
test('chart window selection is bounded',()=>{
 assert.match(ui.render(packet(),'all'),/value="all" selected/);
 assert.match(ui.render(packet(),'nonsense'),/value="2" selected/);
});
