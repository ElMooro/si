const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const root = path.join(__dirname, '..');

function app(page, needle, docs) {
  const text = fs.readFileSync(path.join(root, page), 'utf8');
  const script = [...text.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]).find(s => s.includes(needle));
  const elements = new Map();
  const calls = [];
  const context = vm.createContext({ console, Date, setInterval() {},
    document: {getElementById(id) {if (!elements.has(id)) elements.set(id, {innerHTML: '', textContent: '',value:'',querySelectorAll(){return []}}); return elements.get(id);}},
    fetch: async url => {const key = new URL(url).pathname.split('/').pop(); calls.push(key); return {ok: key in docs, json: async () => docs[key]};},
  });
  context.window = context;
  if(page==='options-scanner.html'){
    context.JHTableValues={compare:()=>0,format:v=>String(v),bindSort(){},load:async url=>{const key=url.split('/').pop();calls.push(key);if(!(key in docs))throw Error('unavailable');return {packet:docs[key],raw:JSON.stringify(docs[key])};}};
    vm.runInContext(fs.readFileSync(path.join(root,'jh-option-observations.js'),'utf8'),context);
  }
  vm.runInContext(script, context);
  return {elements, calls};
}

test('ECB detail renders independently fetched derived ESI and keeps base liquidity', async () => {
  const view = app('ecb-detail.html', 'async function main()', {
    'ecb-detail.json': {ok: true, liquidity: {excess_liquidity_eur_bn: 2468}, cross_reference: {eurodollar_stress_score: 11}},
    'ecb-derived.json': {generated_at: 'derived-time', indicators: {eurodollar_stress_index: {esi_0_100: 70.5, tier: 'CRITICAL'}}},
  });
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(view.calls.sort(), ['ecb-derived.json', 'ecb-detail.json']);
  assert.match(view.elements.get('elq').innerHTML, /2,468/);
  assert.match(view.elements.get('edollar').innerHTML, /70\.5\/100.*CRITICAL.*derived-time/);
});

test('scanner displays dedicated schema and rejects the old macro shape', async () => {
  for (const valid of [true, false]) {
    const doc = valid ? {method: 'options_flow_scanner_v1', stats: {n_evaluated: 1}, summary: {tier_a: ['NVDA']},
      all_qualifying: [{symbol: 'NVDA', tier: 'TIER_A_BULLISH_FLOW', score: 72, flags: [], metrics: {spot: 123}}]} : {success: true, data: {put_call: {}}};
    const view = app('options-scanner.html', 'JHOptionObservations.start()', {'options-flow-scanner.json': doc});
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(view.calls, ['options-flow-scanner.json']);
    if(valid){assert.match(view.elements.get('board').innerHTML,/NVDA/);assert.match(view.elements.get('status').textContent,/unqualified/);}
    else assert.match(view.elements.get('status').textContent,/unavailable/);
  }
});
