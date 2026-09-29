const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8');
const fixtures=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/auction-cross-observations.json'),'utf8'));
const copy=value=>JSON.parse(JSON.stringify(value));
function page(name='valid'){
  const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{innerHTML:'',textContent:'',className:'',style:{}});return nodes.get(id);};
  class FixedDate extends Date { constructor(...args){super(...(args.length?args:[fixtures.clock]));} static now(){return Date.parse(fixtures.clock);} }
  const env={Date:FixedDate,console,document:{getElementById:get},fixture:{cross_signals:copy(fixtures.cases[name])}};
  vm.createContext(env);vm.runInContext(source.slice(0,source.lastIndexOf('load();')),env);vm.runInContext('DATA=fixture',env);
  return {env,get,data:env.fixture.cross_signals,html(){env.renderCrossSignals();return get('cross-strip').innerHTML;}};
}
test('actual-model fixture shows all four dated measurements and correct units',()=>{
  const p=page(),html=p.html();assert.equal((html.match(/class="cross-card"/g)||[]).length,4);
  assert.match(html,/5.00bp/);assert.match(html,/25.00bp/);assert.match(html,/2.60%/);assert.match(html,/Observed 2026-09-29/);
  assert.match(html,/2026-08-30 → 2026-09-29 \(30 calendar days\)/);
  assert.equal(p.env.checkedCross('repo_stress').value,5);assert.equal(p.env.checkedCross('dollar_strength').change,10);
});
test('mismatched dates cannot render a collateral squeeze or invented rate spread',()=>{
  const p=page('mismatched');assert.equal(p.env.checkedCross('repo_stress'),null);
  assert.match(p.html(),/Dated observations are unavailable/);assert.doesNotMatch(p.html(),/Collateral squeeze|ACUTE/);
});
test('latest common date is distinct from each series latest date',()=>{
  const p=page('common'),row=p.env.checkedCross('repo_stress');assert.equal(row.date,'2026-09-28');assert.equal(row.value,10);
  assert.match(p.html(),/Latest source dates: SOFR 2026-09-28; IORB 2026-09-29/);
});
test('measured zero survives in rates and dollar comparison without anchoring claims',()=>{
  const p=page('zero');assert.equal(p.env.checkedCross('repo_stress').value,0);assert.equal(p.env.checkedCross('inflation_expectations').value,0);
  assert.equal(p.env.checkedCross('dollar_strength').change,0);assert.match(p.html(),/\+0.00%/);assert.match(p.html(),/0.00bp/);
  assert.doesNotMatch(p.html(),/UNANCHORED|Above Fed target|Collateral squeeze/);
});
test('single dollar observation retains level while withholding the change',()=>{
  const p=page('single_dollar'),row=p.env.checkedCross('dollar_strength');assert.equal(row.value,100);assert.equal(row.change,null);
  assert.match(p.html(),/Comparison unavailable/);assert.doesNotMatch(p.html(),/\+0.00%/);
  p.data.source_frames.DTWEXBGS.response.observations[0].value='-1';p.data.dollar_strength.level=-1;
  assert.equal(p.env.checkedCross('dollar_strength'),null);
});
test('bounded comparison exposes actual 32-day interval and cannot relabel legacy key',()=>{
  const p=page('bounded_dollar');assert.equal(p.env.checkedCross('dollar_strength').change,10);
  assert.match(p.html(),/2026-08-28 → 2026-09-29 \(32 calendar days\)/);
  p.data.dollar_strength.change_30d_pct=10;assert.equal(p.env.checkedCross('dollar_strength').change,null);
});
test('unknown contract, promoted permission and old or future calculation date reject all cards',()=>{
  for(const kind of ['legacy','permission','item_permission','old','future']){
    const p=page();if(kind==='legacy')p.data.measurement_contract='legacy';
    else if(kind==='permission')p.data.sizing_eligible=true;
    else if(kind==='item_permission')for(const key of ['repo_stress','dollar_strength','curve_slope','inflation_expectations'])p.data[key].calls_eligible=true;
    else p.data.calculation_as_of=kind==='old'?'2026-09-20':'2026-09-30';
    for(const key of ['repo_stress','dollar_strength','curve_slope','inflation_expectations'])assert.equal(p.env.checkedCross(key),null,kind+key);
  }
});
test('raw dates, duplicate dates, non-numeric values and response identity are checked independently',()=>{
  for(const kind of ['future','invalid','duplicate','bool','missing','series','limit','status']){
    const p=page(),f=p.data.source_frames.SOFR,rows=f.response.observations;
    if(kind==='future')rows[0].date='2026-09-30';else if(kind==='invalid')rows[0].date='2026-02-30';
    else if(kind==='duplicate')rows.push(copy(rows[0]));else if(kind==='bool')rows[0].value=false;
    else if(kind==='missing')rows[0].value='.';else if(kind==='series')f.series_id='WRONG';
    else if(kind==='limit')f.requested_limit=6;else f.read_status='unavailable';
    assert.equal(p.env.checkedCross('repo_stress'),null,kind);
  }
});
test('trace arithmetic row positions and units must bind to retained response',()=>{
  for(const kind of ['spread','source_value','trace_date','trace_index','unit','observed']){
    const p=page(),m=p.data.repo_stress;
    if(kind==='spread')m.spread_bp=6;else if(kind==='source_value')p.data.source_frames.SOFR.response.observations[0].value='9';
    else if(kind==='trace_date')m.trace.sofr.date='2026-09-28';else if(kind==='trace_index')m.trace.sofr.source_row_index=7;
    else if(kind==='unit')m.unit='percent';else m.observation_date='2026-09-28';
    assert.equal(p.env.checkedCross('repo_stress'),null,kind);
  }
});
test('corrupt dollar comparison clears change while separately verified level survives',()=>{
  for(const kind of ['date','span','value','index','start','status','unit']){
    const p=page(),m=p.data.dollar_strength,t=m.comparison;
    if(kind==='date')t.target_date='2026-08-29';else if(kind==='span')t.elapsed_calendar_days=31;
    else if(kind==='value')m.change_30d_target_pct=t.change_pct=11;else if(kind==='index')t.start.source_row_index=0;
    else if(kind==='start')t.start.value=99;else if(kind==='status')t.status='unavailable';else t.unit='bp';
    const row=p.env.checkedCross('dollar_strength');assert.equal(row.value,110);assert.equal(row.change,null,kind);
  }
});
test('inflation and curve arithmetic cannot be promoted by a supplied narrative',()=>{
  const p=page();p.data.inflation_expectations.interpretation='<img src=x onerror=alert(1)> UNANCHORED';
  assert.doesNotMatch(p.html(),/<img|UNANCHORED|onerror/);
  p.data.curve_slope.spread_bp=0.25;p.data.inflation_expectations.rate_pct=3;
  assert.equal(p.env.checkedCross('curve_slope'),null);assert.equal(p.env.checkedCross('inflation_expectations'),null);
});
test('missing response clears previously rendered data and recovery is complete',()=>{
  const p=page();assert.match(p.html(),/5.00bp/);const prior=copy(p.data.source_frames);p.data.source_frames={};
  assert.doesNotMatch(p.html(),/5.00bp|25.00bp|2.60%/);p.data.source_frames=prior;assert.match(p.html(),/5.00bp/);
});
