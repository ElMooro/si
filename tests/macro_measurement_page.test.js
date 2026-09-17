const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../macro-data.html'),'utf8');
const script=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m=>m[1]).find(s=>s.includes('function renderMetricsTable'));

function page(){
  const elements={};
  const context=vm.createContext({console,Date,AbortController,setTimeout,clearTimeout,
    document:{getElementById:id=>elements[id]??=( {innerHTML:'',textContent:'',className:'',insertAdjacentHTML(_where,text){this.innerHTML+=text;}} )}});
  // Load the page's actual functions; tests control network and initial load.
  vm.runInContext(script.slice(0,script.lastIndexOf('\nupdateClock();')),context);
  return {context,elements};
}

test('survey and utilization changes render percentage points; production renders index points',()=>{
  const {context:c,elements}=page();
  c.renderMetricsTable('metrics',{survey:{current:7.6,changes:{'1M':-13},change_unit:'survey_balance_pct'},
    utilization:{current:76.29,changes:{'1M':0.08},change_unit:'percent'},
    production:{current:102.99,changes:{'1M':0.21},change_unit:'index_source_base'}},{cols:['1M']});
  assert.match(elements.metrics.innerHTML,/-13.00 pp/);
  assert.match(elements.metrics.innerHTML,/\+0.08 pp/);
  assert.match(elements.metrics.innerHTML,/\+0.21 index pts/);
  assert.doesNotMatch(elements.metrics.innerHTML,/-13.00%|\+0.21%/);
});

test('legacy changes without unit metadata are not rendered as percent returns',()=>{
  const {context:c,elements}=page();
  c.renderMetricsTable('metrics',{loans:{current:14028,changes:{'1W':119.93}}},{cols:['1W']});
  assert.match(elements.metrics.innerHTML,/Unvalidated/);
  assert.doesNotMatch(elements.metrics.innerHTML,/119.93%/);
});

test('manufacturing renders while unrelated banking request remains pending',async()=>{
  const {context:c,elements}=page();
  let finishBanking;
  c.loadNowcast=()=>{};
  c.fetchAgent=url=>url.includes('manufacturing')?Promise.resolve({methodology_version:'manufacturing-measurement.v2',
    generated_at:new Date().toISOString(),quality:{status:'fresh'},us_manufacturing:{EMPIRE_STATE:{current:7.6,changes:{'1M':-13},change_unit:'survey_balance_pct'}}}):
    url.includes('banking')?new Promise(resolve=>{finishBanking=resolve;}):Promise.resolve({bea_data:{}});
  const pending=c.load();
  await new Promise(resolve=>setImmediate(resolve));
  assert.match(elements.manufBody.innerHTML,/EMPIRE STATE/);
  assert.match(elements.manufBody.innerHTML,/-13.00 pp/);
  finishBanking({banking_metrics:{}});
  await pending;
  assert.equal(elements.statusPill.textContent,'PARTIAL');
});

test('measurement page does not load the removed decisive-call and tenor inference widgets',()=>{
  assert.doesNotMatch(html,/JHAIBrief\.mount|src="\/tenor-signals\.js"|src="\/liquidity-credit\.js"/);
});
