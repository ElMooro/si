const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const contract=require('../jh-portfolio-risk-contract.js');
const now=Date.parse('2026-09-18T18:00:00Z');
const fixtures=JSON.parse(fs.readFileSync('tests/fixtures/risk-browser-native-synthetic.json','utf8'));
const copy=name=>structuredClone(fixtures.packets[name||'holdings_without_nav']);

test('risk 2.0.1 complete calculation repairs remain displayable without loosening permissions',()=>{
  const updated=JSON.parse(fs.readFileSync('tests/fixtures/risk-calculation-native-synthetic.json','utf8'));
  for(const packet of Object.values(updated.packets)){
    const before=JSON.stringify(packet),result=contract.view(packet,now);
    assert.equal(packet.schema_version,'2.0.1');assert.equal(result.current,true,result.detail);
    assert.match(result.title,/research only/);assert.equal(JSON.stringify(packet),before);
    assert.equal(contract.view({...packet,permissions:{sizing_eligible:true,may_recommend_trades:false}},now).current,false);
    assert.equal(contract.view({...packet,schema_version:'2.0.3'},now).current,false);
  }
  assert.equal(updated.packets.rounded_scenario_lots.historical_scenarios.sector_shock.projected_pnl_dollars,-26.5);
});

test('incomplete malformed positions show an explanation rather than measured zero risk',()=>{
  const packets=JSON.parse(fs.readFileSync('tests/fixtures/risk-calculation-native-synthetic.json','utf8')).packets;
  for(const name of ['boolean_multiplier','null_position','string_positions']){
    const result=contract.view(packets[name],now);
    assert.match(result.title,/Incomplete risk inputs/);
    assert.equal(contract.number(result.holdings.var_1d_99_dollars),'—');
  }
  const result=contract.view(packets.null_book_reasons,now);
  assert.match(result.detail,/Account NAV unavailable/);
  assert.ok(result.holdings.var_1d_99_dollars>0);
});
test('risk UI preserves zero but refuses missing, stale and legacy contracts',()=>{
  assert.equal(contract.number(0),'0.00'); assert.equal(contract.number(null),'—');
  const doc=copy();
  assert.match(contract.view(doc,now).detail,/NAV unavailable/);
  assert.equal(contract.view({...doc,generated_at:'2026-09-17T17:00:00Z'},now).current,false);
  assert.equal(contract.view({...doc,schema_version:'1.0'},now).current,false);
  assert.equal(contract.view({...doc,generated_at:'2026-09-19T17:00:00Z'},now).current,false);
});

test('all complete native synthetic outputs remain usable without granting account or sizing authority',()=>{
  for(const packet of Object.values(fixtures.packets)){
    const original=JSON.stringify(packet), result=contract.view(packet,now);
    assert.equal(result.current,true,result.detail);assert.equal(JSON.stringify(packet),original);
    assert.match(result.title,/research only/);assert.match(result.detail,/not independent input or account verification/);
    assert.equal(packet.permissions.sizing_eligible,false);assert.equal(packet.permissions.may_recommend_trades,false);
    assert.deepEqual(result.holdings,packet.holdings_risk);
  }
  const zero=contract.view(copy('measured_zero_risk'),now);
  assert.equal(contract.number(zero.holdings.var_1d_99_dollars),'0.00');
  assert.match(contract.view(copy('reported_reconciled_nav'),now).detail,/Model reports reconciled USD NAV: 2000.00/);
});

test('impossible dates, timezone-free dates and invalid offsets cannot be current',()=>{
  for(const date of ['2026-02-30T12:00:00Z','2025-02-29T12:00:00Z','2026-09-18T24:00:00Z','2026-09-18T17:00:60Z','2026-09-18T17:00:00','2026-09-18T17:00:00+00:60','2026-09-18T17:00:00+24:00']){
    assert.equal(Number.isNaN(contract.timestamp(date)),true,date);
    assert.equal(contract.view({...copy(),generated_at:date},now).current,false,date);
  }
  assert.equal(contract.timestamp('2024-02-29T18:00:00.123456+01:00'),Date.parse('2024-02-29T17:00:00.123Z'));
});

test('exact display-age boundaries preserve grace but never coerce a supplied clock',()=>{
  const p=copy(),t=contract.timestamp(p.generated_at);
  assert.equal(contract.view(p,t+4*3600000).current,true);
  assert.equal(contract.view(p,t+4*3600000+1).current,false);
  assert.equal(contract.view(p,t-300000).current,true);
  assert.equal(contract.view(p,t-300001).current,false);
  for(const n of [null,true,String(t),NaN,Infinity]) assert.equal(contract.view(p,n).current,false);
});

test('missing, boolean, fractional and negative common samples are unavailable rather than zero',()=>{
  for(const sample of [undefined,null,false,'128',1.5,-1]){
    const p=copy();p.risk_contract.sample_count=sample;const v=contract.view(p,now);
    assert.equal(v.current,false);assert.match(v.detail,/sample/);assert.doesNotMatch(v.detail,/0 common/);
  }
  const zero=copy('incomplete');zero.risk_contract.sample_count=0;zero.risk_contract.sample_start=null;zero.risk_contract.sample_end=null;
  assert.match(contract.view(zero,now).detail,/0 common return intervals/);
});

test('common-sample date range and minimum must agree with an available model',()=>{
  for(const alter of [c=>c.sample_start='2026-02-30',c=>c.sample_end=c.sample_start,c=>c.sample_end='2026-09-19',c=>c.sample_count=1,c=>c.minimum_sample=false]){
    const p=copy();alter(p.risk_contract);assert.equal(contract.view(p,now).current,false);
  }
});

test('null, nonfinite, wrong currency and error-bearing NAV cannot claim reconciliation',()=>{
  for(const nav of [null,undefined,0,-1,true,'2000',Infinity]){
    const p=copy('reported_reconciled_nav');p.capital_basis.nav=nav;assert.equal(contract.view(p,now).current,false);
  }
  for(const alter of [c=>c.currency='EUR',c=>c.errors=['FAILED'],c=>c.status='UNAVAILABLE']){
    const p=copy('reported_reconciled_nav');alter(p.capital_basis);assert.equal(contract.view(p,now).current,false);
  }
});

test('malformed quality metadata has a visible unavailable result and never throws',()=>{
  for(const q of [null,[],true,{status:'partial',reason_codes:true},{status:'partial',reason_codes:[0]},{status:'fresh',reason_codes:[]},{status:'partial',reason_codes:['INCOMPLETE']}]){
    const p=copy();p.quality=q;const v=contract.view(p,now);assert.equal(v.current,false);assert.match(v.detail,/Quality/);
  }
});

test('wrong engine and missing or upgraded permissions cannot be rendered as research risk',()=>{
  for(const change of [{engine:'other'},{permissions:null},{permissions:{sizing_eligible:true,may_recommend_trades:false}},{permissions:{sizing_eligible:false,may_recommend_trades:'false'}},{status:'READY'}]) assert.equal(contract.view({...copy(),...change},now).current,false);
});

test('account metrics cannot leak into a no-NAV contract; malformed measurements remain unavailable',()=>{
  for(const value of [0,true,'0',NaN,Infinity]){const p=copy();p.var_1d_99_pct=value;assert.equal(contract.view(p,now).current,false);}
  for(const value of [null,false,'0',NaN,Infinity,-1]){const p=copy();p.holdings_risk.var_1d_99_dollars=value;assert.equal(contract.view(p,now).current,false);}
  const p=copy('incomplete');p.holdings_risk.var_1d_99_dollars=0;assert.equal(contract.view(p,now).current,false);
});
function context(path){
  const html=fs.readFileSync(path,'utf8'), elements=new Map();
  const document={addEventListener(){},getElementById(id){if(!elements.has(id)) elements.set(id,{innerHTML:'',textContent:'',style:{}});return elements.get(id);},querySelector(){return {style:{}};}};
  const scripts=[...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n');
  const scope=vm.createContext({document,window:{},console,JHPortfolioRisk:contract,Date,AbortController,
    JHScenarioIO:{decode(){},readComplete(){return new Promise(()=>{});}},setInterval(){},fetch(){return new Promise(()=>{});}});
  vm.runInContext(scripts,scope);return {scope,elements};
}

test('actual page keeps malformed supplied rows visible and never invents an empty portfolio',()=>{
  const cases=JSON.parse(fs.readFileSync('tests/fixtures/pre-risk-calculation/complete-synthetic.json','utf8'));
  const outputs=JSON.parse(fs.readFileSync('tests/fixtures/risk-calculation-native-synthetic.json','utf8')).packets;
  for(const [name,row] of Object.entries(cases)){
    const {scope,elements}=context('portfolio/index.html');
    scope.suppliedSnapshot=row.inputs.snapshot;scope.suppliedRisk=outputs[name];
    const before=JSON.stringify(row.inputs.snapshot);
    vm.runInContext('snapshot=suppliedSnapshot;risk=suppliedRisk;renderPositions();renderScenarios();',scope);
    assert.equal(JSON.stringify(row.inputs.snapshot),before);
    if(name==='string_positions'){
      assert.match(elements.get('positions-body').innerHTML,/Holdings list unavailable/);
      assert.doesNotMatch(elements.get('positions-body').innerHTML,/No positions yet/);
    }
    if(name==='null_position'){
      assert.equal(elements.get('pos-count').textContent,'2 supplied records · 1 invalid');
      assert.match(elements.get('positions-body').innerHTML,/Position record 2 unavailable/);
      assert.match(elements.get('positions-body').innerHTML,/AAA/);
    }
    assert.match(elements.get('scenarios-grid').innerHTML,/unrounded position contributions/);
  }
});

test('actual page recovers from invalid position lists and escapes scenario metadata',()=>{
  const {scope,elements}=context('portfolio/index.html');
  for(const positions of [null,false,true,0,'AAA',{}]){
    scope.supplied=positions;vm.runInContext('snapshot={positions:supplied};renderPositions();',scope);
    assert.match(elements.get('positions-body').innerHTML,/Holdings list unavailable/);
    assert.equal(elements.get('pos-count').textContent,'Holdings count unavailable');
  }
  vm.runInContext('snapshot={positions:[]};renderPositions();',scope);
  assert.equal(elements.get('pos-count').textContent,'0 holdings');assert.match(elements.get('positions-body').innerHTML,/No positions yet/);
  vm.runInContext(`risk={historical_scenarios:{x:{name:'Synthetic',rounding:'<img src=x onerror=bad>'}}};renderScenarios();`,scope);
  assert.match(elements.get('scenarios-grid').innerHTML,/&lt;img/);assert.doesNotMatch(elements.get('scenarios-grid').innerHTML,/<img/);
});
test('actual portfolio page renders absent NAV as unavailable, not 0% risk',()=>{
  const {scope,elements}=context('portfolio/index.html');
  vm.runInContext(`snapshot={portfolio_summary:{},counts:{}};risk={var_1d_99_pct:null,var_1d_99_dollars:null,portfolio_beta_spy:0,portfolio_vol_annual_pct:null,concentration_hhi:0};renderTopMetrics();`,scope);
  assert.equal(elements.get('mv-beta').textContent,'0.00');
  assert.equal(elements.get('mv-vol').textContent,'—');
  assert.equal(elements.get('mv-var').textContent,'—');
  assert.match(elements.get('mv-var-pct').textContent,/NAV unavailable/);
  vm.runInContext('snapshot=null;risk=null;renderTopMetrics();renderAlerts();',scope);
  assert.equal(elements.get('mv-beta').textContent,'—');
  assert.equal(elements.get('alert-zone').innerHTML,'');
});

test('actual page clears failed snapshot metadata and distinguishes missing counts from measured zero',()=>{
  const {scope,elements}=context('portfolio/index.html');
  vm.runInContext(`snapshot={generated_at:'2026-09-18T17:00:00Z',counts:{positions:0,watchlist:0},positions:[],watchlist:[]};setUpdated();renderTopMetrics();renderPositions();renderWatchlist();`,scope);
  assert.equal(elements.get('mv-positions').textContent,'0');assert.match(elements.get('updated').textContent,/2026-09-18T17/);
  vm.runInContext(`snapshot=null;risk=null;setUpdated();renderTopMetrics();renderPositions();renderWatchlist();renderSectors();`,scope);
  assert.equal(elements.get('updated').textContent,'unavailable');assert.equal(elements.get('mv-positions').textContent,'—');
  assert.match(elements.get('pos-count').textContent,/unavailable/);assert.match(elements.get('watch-count').textContent,/unavailable/);
  vm.runInContext(`snapshot={counts:{}};renderTopMetrics();`,scope);assert.equal(elements.get('mv-positions').textContent,'—');
  assert.equal(vm.runInContext(`fmtPct('0')`,scope),'—');assert.equal(vm.runInContext(`fmtPct(0)`,scope),'0.00%');
});

test('correlation section recovers after a single-instrument result',()=>{
  const {scope,elements}=context('portfolio/index.html');
  vm.runInContext(`risk={correlation_matrix:{AAA:{AAA:1}}};renderCorrelations();`,scope);
  assert.equal(elements.get('corr-section').style.display,'none');
  vm.runInContext(`risk={correlation_matrix:{AAA:{AAA:1,BBB:0},BBB:{AAA:0,BBB:1}}};renderCorrelations();`,scope);
  assert.equal(elements.get('corr-section').style.display,'');assert.match(elements.get('corr-matrix').innerHTML,/0.00/);
});

test('scenario navigation points to the existing private portfolio page',()=>{
  const html=fs.readFileSync('position-sizer.html','utf8');assert.match(html,/href="\/portfolio\/index.html">Portfolio risk/);
  assert.equal(fs.existsSync('portfolio/index.html'),true);assert.doesNotMatch(html,/href="\/portfolio-risk.html"/);
});
test('sizing page renders WAIT and no allocation rather than empty or invented percent',()=>{
  const {scope,elements}=context('sizing/index.html');
  vm.runInContext(`data={summary:{regime:'RESEARCH_ONLY',nav:null,current_invested_pct:null,kelly_invested_pct:null,kelly_cash_pct:null,actionable_count:0,entry_candidates_count:0},reason_codes:['NO_APPROVED_SIZING_PROTOCOL'],positions:[{symbol:'AAA',action:'WAIT',kelly_weight_pct:null,shares_delta:null,dollar_delta:null}],entry_candidates:[]};renderAll();`,scope);
  assert.match(elements.get('summary-grid').innerHTML,/—/);
  assert.match(elements.get('positions-body').innerHTML,/WAIT/);
  assert.match(elements.get('entries-body').innerHTML,/withheld/);
});
test('PM page does not render unpermitted legacy actions',()=>{
  const {scope,elements}=context('pm-decision.html');
  vm.runInContext(`renderActions({actions:{add:[{target:'FAKE_BUY',reason:'not approved'}]}});`,scope);
  assert.doesNotMatch(elements.get('actions').innerHTML,/FAKE_BUY/);
  assert.match(elements.get('actions').innerHTML,/No authorized actions/);
});
