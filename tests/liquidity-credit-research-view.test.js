const test=require('node:test'),assert=require('node:assert/strict');
const ui=require('../liquidity-credit.js'),now=Date.parse('2026-09-25T21:00:00Z');
const key='data/evidence/fred/'+'a'.repeat(64)+'/'+'b'.repeat(64)+'.bin.gz';
function row(sid='WALCL',unit='Millions of U.S. Dollars',value=0){return {series_id:sid,_label:sid,_units:unit,_category:'balance_sheet',
 available:true,quality:{status:'fresh'},latest_value:value,latest_date:'2026-09-23',frequency:'W',acquired_at:'2026-09-25T18:00:00Z',
 source_definition:{id:sid,units:unit,frequency:'Weekly, As of Wednesday'},evidence:{definition:{key,captured:true},observations:{key,captured:true}},
 calendar_comparisons:{month:{change:0,change_unit:unit,source_unit:unit,baseline_date:'2026-08-19',current_date:'2026-09-23',target_date:'2026-08-23'}},history:[],statistics:{}};}
function packet(){return {contract:'liquidity-credit-research.v1',call:null,calls_eligible:false,sizing_eligible:false,execution_eligible:false,
 replay:{manifest_key:'data/lce-research/runs/'+'c'.repeat(64)+'.json'},generated_at:'2026-09-25T18:22:00Z',source_generated_at:'2026-09-25T18:21:00Z',
 composite:{score:null},series:{WALCL:row()}};}

test('native measurement retains real zero and actual calendar comparison endpoints without zero-risk conversion',()=>{
 const d=packet(),v=ui.view(d,'balance_sheet',now);assert.equal(v.fresh,1);assert.equal(v.rows[0].value,'0 Millions of U.S. Dollars');
 assert.match(v.rows[0].changes.month,/2026-08-19.*2026-09-23.*2026-08-23/);
 const html=ui.panelHTML(d,'balance_sheet',now);assert(!html.includes('0/100'));assert.match(html,/WAIT \/ abstain/);
 assert.match(html,/definition original/);assert.match(html,/replay manifest/);
});

test('percent comparisons use percentage points and monthly/quarterly dates are periods',()=>{
 const r=row('HQMCB10YR','Percent',5.58);r.frequency='M';r.latest_date='2026-08-01';
 r.calendar_comparisons.month={change:.13,change_unit:'percentage_points',source_unit:'Percent',baseline_date:'2026-07-01',current_date:r.latest_date,target_date:'2026-07-01'};
 assert.match(ui.period(r),/2026-08 \(monthly period/);assert.match(ui.comparison(r,'month',true),/0.13 percentage points/);
 r.frequency='Q';r.latest_date='2026-07-01';assert.match(ui.period(r),/2026 Q3/);
 r.calendar_comparisons.month.change_unit='Percent';assert.equal(ui.comparison(r,'month',true),'Unavailable');
 r.latest_date='2026-08-01';assert.equal(ui.comparison(r,'month',true),'Comparison unit mismatch');
});

test('old source or acquisition clocks, mismatched definition and absent evidence withhold current amounts',()=>{
 for(const change of [d=>d.source_generated_at='2026-09-20T00:00:00Z',d=>d.series.WALCL.acquired_at='2026-09-26T00:00:00Z',
  d=>d.series.WALCL.latest_date='2026-02-30',d=>d.series.WALCL._units='Billions',d=>d.series.WALCL.evidence.definition.key='https://example.com/',
  d=>d.series.WALCL.latest_value=null,d=>d.series.WALCL.quality.status='unavailable']){
  const d=packet();change(d);const v=ui.view(d,'balance_sheet',now);assert.equal(v.fresh,0);assert.equal(v.rows[0].value,'Unavailable');assert.equal(v.rows[0].changes.month,'Unavailable');
 }
});

test('legacy and directional packets cannot qualify; arbitrary text and evidence paths cannot inject markup',()=>{
 for(const d of [{},null,{...packet(),calls_eligible:true},{...packet(),replay:{manifest_key:'https://example.com'}}])assert.equal(ui.view(d,'balance_sheet',now).status,'Unavailable');
 const d=packet();d.series.WALCL._label='<img onerror=alert(1)>';d.series.WALCL.history=[{value:'</pre><img src=x>'}];
 const html=ui.panelHTML(d,'balance_sheet',now);assert(!html.includes('<img'));assert.match(html,/&lt;img/);
});

test('all series determine categories even when legacy category index omits them',()=>{
 const d=packet();d.series.CREDIT={...row('CREDIT','Percent',5),_category:'credit_spreads'};d.by_category={balance_sheet:['WALCL']};
 const v=ui.view(d,'credit_spreads',now);assert.deepEqual(v.categories,['balance_sheet','credit_spreads']);assert.equal(v.rows[0].sid,'CREDIT');assert.equal(v.total,2);
});

test('failed refresh replaces prior content and all loads omit credentials',async()=>{
 const host={innerHTML:'',querySelectorAll:()=>[]},timers=[],requests=[];let good=true;
 const win={JUSTHODL_LCE_NO_PILL:true,document:{readyState:'complete',head:{appendChild(){}},createElement:()=>({}),getElementById:id=>id==='liquidity-credit-panel'?host:null},
  setInterval:(f,t)=>timers.push({f,t}),fetch:async(url,options)=>{requests.push({url,options});return {ok:good,json:async()=>packet()};}};
 ui.install(win);await new Promise(resolve=>setImmediate(resolve));assert.match(host.innerHTML,/WALCL/);assert.equal(requests[0].options.credentials,'omit');
 good=false;await timers.find(t=>t.t===300000).f();assert(!host.innerHTML.includes('WALCL'));assert.match(host.innerHTML,/Unavailable/);assert(timers.some(t=>t.t===60000));
});
