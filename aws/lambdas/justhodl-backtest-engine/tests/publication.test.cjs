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
