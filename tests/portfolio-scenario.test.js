const test=require('node:test'),assert=require('node:assert/strict');
const m=require('../jh-portfolio-scenario.js');
const row=(id,weight,local,fx='0',currency='USD')=>({id,label:'Synthetic '+id,position_type:'cash_security',currency,weight_pct:weight,local_price_return_pct:local,fx_return_pct:fx});
const example=()=>({contract:m.INPUT,label:'Synthetic scenario',horizon:'One hypothetical day',base_currency:'USD',nav:'100000',cash_rate_pct:'0',cost_pct_nav:'0.1',positions:[row('A','60','-10'),row('B','30','2')]});
const decimal=(out,key)=>out[key]?.decimal;

test('explicit scenario reconciles signed positions, cash, costs and NAV',()=>{
  const out=m.calculate(example());assert.equal(decimal(out,'net_weight_pct'),'90.000000');assert.equal(decimal(out,'cash_weight_pct'),'10.000000');
  assert.equal(decimal(out,'asset_pnl_pct_nav'),'-5.400000');assert.equal(decimal(out,'total_pnl_pct_nav'),'-5.500000');
  assert.equal(decimal(out,'total_pnl'),'-5500.00');assert.equal(decimal(out,'ending_nav'),'94500.00');assert.equal(decimal(out,'displayed_pnl_rounding_adjustment'),'0.00');
  assert.equal(out.permissions.may_recommend_trades,false);assert.equal(out.nav_status,'USER_ASSUMED_NOT_ACCOUNT_VERIFIED');
});
test('FX compounds with local prices instead of adding percentage points',()=>{
  const x=example();x.positions=[row('A','100','10','-10','EUR')];x.cost_pct_nav='0';
  const out=m.calculate(x);assert.equal(out.positions[0].base_price_return_pct.decimal,'-1.000000');assert.equal(decimal(out,'total_pnl'),'-1000.00');
});
test('short exposure, borrowing and insolvency keep their signs',()=>{
  const x=example();x.positions=[row('short','-100','200'),row('long','250','-20')];x.cash_rate_pct='1';x.cost_pct_nav='0';
  const out=m.calculate(x);assert.equal(decimal(out,'gross_weight_pct'),'350.000000');assert.equal(decimal(out,'cash_weight_pct'),'-50.000000');
  assert.equal(decimal(out,'cash_pnl'),'-500.00');assert.equal(decimal(out,'total_pnl_pct_nav'),'-250.500000');assert.equal(decimal(out,'ending_nav'),'-150500.00');
  assert.equal(out.capital_exhausted_under_assumptions,true);assert.equal(out.cash_role,'BORROWING_ASSUMPTION');
});
test('missing NAV leaves currency P&L unavailable while exact percentage impact remains',()=>{
  const x=example();x.nav=null;const out=m.calculate(x);assert.equal(out.total_pnl,null);assert.equal(out.positions[0].pnl,null);
  assert.equal(out.ending_nav,null);assert.equal(out.capital_exhausted_under_assumptions,null);assert.equal(decimal(out,'total_pnl_pct_nav'),'-5.500000');
});
test('zeros are valid but missing inputs, duplicate identities and unsupported products fail',()=>{
  const x=example();x.positions=[row('zero','0','0')];x.cost_pct_nav='0';assert.equal(decimal(m.calculate(x),'total_pnl'),'0.00');
  for(const change of [v=>delete v.cash_rate_pct,v=>v.positions[0].weight_pct=null,v=>v.positions[0].local_price_return_pct=10,
    v=>v.positions[0].fx_return_pct='NaN',v=>v.positions[0].position_type='option',v=>v.positions.push({...v.positions[0]}),
    v=>v.positions[0].fx_return_pct='1',v=>v.positions[0].local_price_return_pct='-100.1',v=>v.cost_pct_nav='-1',
    v=>v.nav='0',v=>v.positions[0].weight_pct='1001',v=>v.positions[0].weight_pct='0.0000001',v=>v.unknown='extra']){
    const bad=example();change(bad);assert.throws(()=>m.calculate(bad));
  }
});
test('exact small amounts reconcile through an explicit display-rounding adjustment',()=>{
  const x=example();x.nav='1';x.cost_pct_nav='0';x.positions=[row('a','50','1'),row('b','50','1')];
  const out=m.calculate(x);assert.deepEqual(out.positions.map(r=>r.pnl.decimal),['0.00','0.00']);
  assert.equal(decimal(out,'total_pnl'),'0.01');assert.equal(decimal(out,'displayed_pnl_rounding_adjustment'),'0.01');
});
test('calculation is deterministic, does not mutate inputs, and retains every position',()=>{
  const x=example(),copy=structuredClone(x),a=m.calculate(x),b=m.calculate(copy);
  assert.deepEqual(a,b);assert.deepEqual(x,copy);assert.equal(a.positions.length,x.positions.length);
  assert.equal(a.missing_input_defaults,false);assert.ok(Object.values(a.permissions).every(v=>v===false));
});

test('valid large outputs remain exact beyond the input decimal bound',()=>{
  const x=example();x.nav='999999999999999';x.cost_pct_nav='0';x.positions=[row('large','1000','10000','10000','EUR')];
  const out=m.calculate(x);assert.equal(out.total_pnl.decimal,'101999999999999898000.00');
  assert.equal(out.displayed_pnl_rounding_adjustment.decimal,'0.00');
});

test('joining field names cannot impersonate the required schema',()=>{
  const x=example();x['cash_rate_pct|contract']=x.cash_rate_pct;delete x.cash_rate_pct;delete x.contract;
  assert.throws(()=>m.calculate(x),/missing or unexpected/);
});

test('offline replay checks model bytes and every result without running supplied code',()=>{
  const fs=require('node:fs'),crypto=require('node:crypto'),verify=require('../scripts/replay_portfolio_scenario.cjs').verify;
  const raw=fs.readFileSync(require.resolve('../jh-portfolio-scenario.js'));
  const input=example(),packet={contract:'portfolio-scenario-export.v1',model:{contract:m.CONTRACT,sha256:crypto.createHash('sha256').update(raw).digest('hex'),bytes:raw.length},input,output:m.calculate(input)};
  assert.deepEqual(verify(packet),packet.output);
  const changed=structuredClone(packet);changed.output.positions[1].pnl.numerator='42';assert.throws(()=>verify(changed));
  const code=structuredClone(packet);code.model.sha256='0'.repeat(64);assert.throws(()=>verify(code));
});

test('browser and bundled model have byte-identical reviewed sources',()=>{
  const fs=require('node:fs'),path=require('node:path');
  assert.deepEqual(fs.readFileSync(require.resolve('../jh-portfolio-scenario.js')),fs.readFileSync(path.join(__dirname,'../aws/lambdas/justhodl-position-sizer/source/scenario_model.js')));
});
