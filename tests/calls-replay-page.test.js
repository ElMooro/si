const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../backtest.html'), 'utf8');
const start = html.indexOf('async function loadCallsBacktest(){');
const end = html.indexOf('function renderCallsNavChart(curve){', start);
assert.ok(start > 0 && end > start);

test('Calls replay clears old performance on legacy, unavailable or error output', async () => {
  const inputs = [
    {ok:true, json:async()=>({status:'no_eligible_calls'})},
    {ok:true, json:async()=>({status:'error'})},
    {ok:true, json:async()=>({summary:{total_return_pct:100}, nav_curve:[{nav:200000}]})},
    {ok:false},
    new Error('network unavailable'),
  ];
  for (const input of inputs) {
    const elements = new Map();
    const document = {getElementById(id) {
      if (!elements.has(id)) elements.set(id, {innerHTML:'OLD PERFORMANCE'});
      return elements.get(id);
    }};
    let curve = ['old'];
    const context = vm.createContext({document, console:{error(){}}, Date,
      fetch:async()=>{if(input instanceof Error) throw input; return input;},
      renderCallsNavChart:values=>{curve=values;}});
    vm.runInContext(html.slice(start, end), context);
    await context.loadCallsBacktest();
    assert.equal(document.getElementById('calls-kpi-row').innerHTML, '');
    assert.equal(curve.length, 0);
    assert.doesNotMatch(document.getElementById('calls-body').innerHTML, /OLD PERFORMANCE/);
  }
});
