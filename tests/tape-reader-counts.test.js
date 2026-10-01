const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const {execFileSync} = require('node:child_process');
const root = path.join(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'tape-reader.html'), 'utf8');
const source = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]).find(s => s.includes('const FEED_URL'));
const fixture = JSON.parse(execFileSync('python3', ['-B', 'aws/lambdas/justhodl-tape-reader/tests/run_tests.py', '--fixture'], {cwd: root, encoding: 'utf8'}));
const flush = () => new Promise(resolve => setImmediate(resolve));

async function page(packet) {
  const elements = {};
  const context = vm.createContext({console: {error(){}}, setInterval(){},
    fetch: async () => ({ok:true, json:async () => packet}),
    document: {getElementById: id => elements[id] ||= {innerHTML:'', textContent:''}, querySelectorAll: () => []}});
  vm.runInContext(source, context); await flush();
  return {elements, context, run: code => vm.runInContext(code, context)};
}

test('actual producer packet shows unavailable size, honest tags and unchanged other scores', async () => {
  const p = await page(fixture), table = p.elements.tableHost.innerHTML;
  assert.match(table, /Avg size×/); assert.match(table, /LARGE AVG TRADE SIZE/);
  assert.doesNotMatch(table, /BLOCK PRINTS|Block×|block prints/);
  assert.match(table, /<td>—<\/td>/); assert.match(table, />54<\/span>/);
  assert.equal(p.elements.nSize.textContent, '1 / 2');
});

test('typed formatting preserves measured zero and excludes null, booleans, strings and nonfinite numbers', async () => {
  const p = await page(fixture);
  assert.equal(p.run("fmt(0, 2, '×')"), '0.00×');
  for (const expr of ['null','undefined','true','false','NaN','Infinity','-Infinity','"0"','""']) {
    assert.equal(p.run(`fmt(${expr}, 2, '×')`), '—', expr);
    assert.equal(p.run(`fmtBig(${expr})`), '—', expr);
  }
  assert.equal(p.run('fmtBig(0)'), '0');
});

test('missing-size rows sort last in both directions and cannot acquire size tags', async () => {
  const data = structuredClone(fixture);
  data.top_loud_tape[1].classifications.push('BLOCK_PRINTS', 'LARGE_AVG_TRADE_SIZE');
  const p = await page(data);
  assert.equal((p.elements.tableHost.innerHTML.match(/LARGE AVG TRADE SIZE/g) || []).length, 1);
  assert.doesNotMatch(p.elements.tableHost.innerHTML, /BLOCK PRINTS/);
  for (const dir of ['asc','desc']) {
    assert.equal(p.run(`applySort(DATA.top_loud_tape, 'block_ratio', '${dir}').at(-1).ticker`), 'UNKNOWN');
  }
});

test('legacy packet clears table and summary rather than reusing unsupported scores', async () => {
  const data = structuredClone(fixture); delete data.measurement_contract;
  const p = await page(data);
  assert.match(p.elements.tableHost.innerHTML, /Updated aggregate-activity data unavailable/);
  assert.equal(p.elements.topScore.textContent, '—'); assert.equal(p.run('DATA'), null);
});

async function enhancement(packet, attrs = {}) {
  const elements = {'jhviz-body': {innerHTML:''}};
  const configured = {'data-feed':'data/tape-reader.json','data-bars':'top_loud_tape:ticker:score',
    'data-contract':'tape-reader-activity.v2','data-strict-numbers':'1',
    'data-footnote':'Published daily aggregate activity', ...attrs};
  vm.runInNewContext(fs.readFileSync(path.join(root, 'jh-enhance.js'), 'utf8'), {
    fetch: async () => ({ok:true, json:async () => packet}),
    document: {currentScript:{getAttribute:k=>configured[k] || null},
      createElement: () => ({style:{}, innerHTML:''}), querySelector:()=>null,
      body:{insertBefore(){}}, getElementById:k=>elements[k]}});
  await flush(); return elements['jhviz-body'].innerHTML;
}

test('enhancement contract rejects old scores and strict mode omits unavailable without inventing zero', async () => {
  assert.match(await enhancement(fixture), /Published daily aggregate activity/);
  const old = structuredClone(fixture); delete old.measurement_contract;
  assert.match(await enhancement(old), /Updated aggregate-activity data unavailable/);
  const rows = [null, undefined, false, true, '25', NaN, Infinity, 0, 54].map((score,i)=>({ticker:'CASE'+i,score}));
  const chart = await enhancement({...fixture,top_loud_tape:rows});
  for(let i=0;i<7;i++) assert.doesNotMatch(chart, new RegExp('CASE'+i));
  assert.match(chart, /CASE7/); assert.match(chart, /CASE8/);
  assert.doesNotMatch(chart, /live data/);
});

test('shared enhancement retains legacy behavior for other pages that do not opt in', async () => {
  const chart = await enhancement({top_loud_tape:[{ticker:'OTHER',score:'25'}]},
    {'data-contract':null,'data-strict-numbers':null,'data-footnote':null});
  assert.match(chart, /OTHER/); assert.match(chart, /25/); assert.match(chart, /live data/);
});

test('page wires the same contract and strict enhancement; no institutional activity claim', () => {
  assert.match(html, /data-contract="tape-reader-activity.v2"/);
  assert.match(html, /data-strict-numbers="1"/);
  assert.doesNotMatch(html, /institutional footprint detection|institutions are crossing|intraday tape intensity/);
});
