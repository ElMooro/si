const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const contract=require('../jh-portfolio-risk-contract.js');
const now=Date.parse('2026-09-18T18:00:00Z');
test('risk UI preserves zero but refuses missing, stale and legacy contracts',()=>{
  assert.equal(contract.number(0),'0.00'); assert.equal(contract.number(null),'—');
  const doc={schema_version:'2.0.0',generated_at:'2026-09-18T17:00:00Z',status:'AVAILABLE_HOLDINGS_MODEL',
    capital_basis:{status:'UNAVAILABLE'},risk_contract:{sample_count:100,sample_start:'2026-05-01',sample_end:'2026-09-17'}};
  assert.match(contract.view(doc,now).detail,/NAV unavailable/);
  assert.equal(contract.view({...doc,generated_at:'2026-09-17T17:00:00Z'},now).current,false);
  assert.equal(contract.view({...doc,schema_version:'1.0'},now).current,false);
  assert.equal(contract.view({...doc,generated_at:'2026-09-19T17:00:00Z'},now).current,false);
});
function context(path){
  const html=fs.readFileSync(path,'utf8'), elements=new Map();
  const document={addEventListener(){},getElementById(id){if(!elements.has(id)) elements.set(id,{innerHTML:'',textContent:'',style:{}});return elements.get(id);},querySelector(){return {style:{}};}};
  const scripts=[...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n');
  const scope=vm.createContext({document,window:{},console,JHPortfolioRisk:contract,Date,setInterval(){},fetch(){return new Promise(()=>{});}});
  vm.runInContext(scripts,scope);return {scope,elements};
}
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
