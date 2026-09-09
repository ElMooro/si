"use strict";
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const assert=require('node:assert/strict');
class Element {
  constructor(){this.children=[];this.textContent='';}
  appendChild(child){this.children.push(child);return child;}
  replaceChildren(...children){this.children=children;}
}
const ids={};
for(const prefix of ['honest','walkforward']) for(const suffix of ['notice','kpi-row','stats-body','nav-chart']) ids[prefix+'-'+suffix]=new Element();
ids["daily-ledger-status"]=new Element();ids["daily-ledger-table"]=new Element();
const document={getElementById:id=>ids[id]||null,createElement:()=>new Element()};
const html=fs.readFileSync(path.resolve(__dirname,'../../../../backtest.html'),'utf8');
const code=html.split('/* AUDIT PUBLICATION GATE START */')[1].split('/* AUDIT PUBLICATION GATE END */')[0];
const context={document};vm.createContext(context);vm.runInContext(code,context);
const fixture={data_sufficient:true,coverage_pct:100,headline_eligible:false,tradable_portfolio_nav:false,annualized_return_pct:120473.55,final_nav:123456789};
for(const prefix of ['honest','walkforward']){
  context.renderAttributionGate(fixture,prefix);
  assert.match(ids[prefix+'-notice'].textContent,/PERFORMANCE BLOCKED/);
  assert.doesNotMatch(ids[prefix+'-kpi-row'].textContent,/120473|123456789|publishable/);
  const pre=ids[prefix+'-stats-body'].children[0].children[0].children[0].children[1];
  assert.deepEqual(JSON.parse(pre.textContent),fixture);
}
assert.doesNotMatch(html,/Becomes the headline|number to publish|✓ publishable|future headline/);
console.log('backtest publication DOM regression passed');

context.renderDailyLedger({status:'BLOCKED',reason:'WTREGEN or fills missing'});
assert.match(ids['daily-ledger-status'].textContent,/BLOCKED/);
assert.equal(ids['daily-ledger-table'].children.length,0);
context.renderDailyLedger({status:'READY',schema_version:'research-capital-ledger-1.0',input_contract:{hash_verified:true},curve_semantics:'daily_marked_capital_ledger',n_sessions:1,n_fills:1,nav_curve:[{date:'2026-09-01',equity_nav:999,cash:499,gross_exposure:500,external_flow:0}]});
assert.match(ids['daily-ledger-status'].textContent,/Publication blocked/);
assert.equal(ids['daily-ledger-table'].children[0].children[1].children[1].textContent,'999');
