const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const ui=require('../jh-crisis-research.js');
const stamp='2026-09-20T09:30:00Z',now=Date.parse(stamp)+1000;
function packet(){return {contract:'crisis-research.v1',generated_at:stamp,calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_eligible:false,
 master_crisis_score:null,defcon_level:null,playbook:null,replay:{manifest_key:'data/crisis-research/runs/'+'a'.repeat(64)+'.json'},
 measurements:{SOFR:{series_id:'SOFR',label:'Secured Overnight Financing Rate',value_decimal:'0',unit:'Percent',observation_date:'2026-09-17',quality:{status:'fresh'},history:[]}},
 channels:{funding:{series_ids:['SOFR']}},comparisons:{sofr_iorb:{label:'SOFR minus IORB',value_decimal:'-5.00',unit:'basis_points',observation_date:'2026-09-17',source_latest_dates:{SOFR:'2026-09-17',IORB:'2026-09-20'},source_rows:{SOFR:0,IORB:1},formula:'(SOFR - IORB) * 100'}},
 quality:{status:'fresh',fresh_native_series:23,expected_native_series:23,ecb_headline_status:'fresh'}};}
test('legacy scores and producer self-qualification cannot enter native research UI',()=>{
 assert.match(ui.render({master_crisis_score:99}),/Crisis research unavailable/);
 for(const key of ['calls_eligible','sizing_eligible','execution_eligible','forecast_eligible']){const p=packet();p[key]=true;assert.match(ui.render(p),/Crisis research unavailable/);}
 const p=packet();p.defcon_level=5;assert.match(ui.render(p),/Crisis research unavailable/);
});
test('zero is observed while missing boolean and invalid strings remain unavailable',()=>{
 assert.equal(ui.fmt(0),'0');for(const x of [false,true,'',null,{},'NaN','Infinity'])assert.equal(ui.fmt(x),'Unavailable');
 const html=ui.render(packet(),{now});assert.match(html,/0<small>Percent/);assert.match(html,/-5 <small>bp/);assert.doesNotMatch(html,/NaN|undefined|\[object Object\]/);
});
test('same-date spread details expose differently dated latest inputs and original row zero',()=>{
 const html=ui.render(packet(),{now});assert.match(html,/IORB: latest 2026-09-20; matched original row 1/);assert.match(html,/SOFR: latest 2026-09-17; matched original row 0/);
 assert.match(html,/not seven or six independent confirmations/);
});
test('expired or future packets withhold current comparisons while retaining history',()=>{
 for(const time of [now+9*3600000,now-100000]){const html=ui.render(packet(),{now:time});assert.match(html,/Packet stale or clock invalid/);assert.match(html,/Withheld/);assert.doesNotMatch(html,/>-5 <small>bp/);assert.match(html,/Current provider vintage/);}
});
test('dates and missing source rows determine chart spacing and gaps',()=>{
 const h=[{date:'2026-09-01',value_decimal:'1'},{date:'2026-09-02',value_decimal:null},{date:'2026-09-11',value_decimal:'2'}];
 const a=ui.plot(h,63,'Percent');assert.equal((a.match(/ M /g)||[]).length,2);assert.doesNotMatch(a,/ L /);
 h[1].value_decimal='1.5';assert.match(ui.plot(h,63,'Percent'),/L 125\.00 /);
});
test('untrusted source text and paths are escaped and bounded to public artifacts',()=>{
 const p=packet();p.measurements.SOFR.label='<img src=x onerror=alert(1)>';
 const html=ui.render(p,{now});assert.doesNotMatch(html,/<img/);assert.match(html,/&lt;img/);
 for(const key of ['javascript:alert(1)','data/../private','https://evil.test'])assert.equal(ui.link(key,'Source'),'Source unavailable');
});
test('scenario links carry no automatic target weights, trade or crisis probability',()=>{
 const html=ui.render(packet(),{now});assert.match(html,/href="\/position-sizer.html"/);assert.match(html,/research abstains/);assert.match(html,/instruction to liquidate/);assert.match(html,/No target weight/);
});
test('DEFCON page uses one native source controller and preserves related-source links',()=>{
 const page=fs.readFileSync('defcon.html','utf8');assert.match(page,/jh-crisis-research.js\?v=53/);assert.match(page,/aria-live="polite"/);assert.doesNotMatch(page,/s3.us-east-1|ai-brief-kit|master_crisis_score\|\|0/);
 assert.match(ui.render(packet(),{now}),/Capitulation/);assert.match(ui.render(packet(),{now}),/China liquidity/);
});
test('native Plumbing page links to Crisis without ingesting its old DEFCON score',()=>{
 const plumbing=require('../jh-plumbing-research.js');
 const p={contract:'plumbing-research.v1',calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_eligible:false,
  decision:{verb:'WAIT'},composite:{composite_stress_score:null},measurements:Object.fromEntries(Array.from({length:53},(_,i)=>['S'+i,{}])),
  context:{crisis_composite:{master_crisis_score:99,defcon_level:1,playbook:'FORCED EXIT'}}};
 const html=plumbing.render(p,now);assert.match(html,/href="\/defcon.html"/);assert.doesNotMatch(html,/FORCED EXIT|master_crisis_score|>99</);
 assert.doesNotMatch(fs.readFileSync('crisis.html','utf8'),/pick\(d,NK\)|arguments.callee.caller/);
});
