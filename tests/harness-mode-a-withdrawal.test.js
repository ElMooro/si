const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.join(__dirname, '..');
const current = fs.readFileSync(path.join(root, 'backtests.html'), 'utf8');
const before = fs.readFileSync(path.join(__dirname, 'fixtures/harness-mode-a/backtests.html.txt'), 'utf8');
const code = html => html.match(/<script>([\s\S]*?)<\/script>/)[1];
const live = [{signal_type:'fixture',graded:3,pending:0,hit_pct:0,avg_excess_pct:0,median_excess_pct:0,artifacts:0}];
const meta = {model:{uplift_pp:1,test_take_precision:50,test_base_hit:49,n_train:70,n_test:30},
  gates:[{type:'fixture',ticker:'TEST',conf:.5,meta_p:.6,verdict:'TAKE'}],n_take:1,n_pending_gated:1,threshold:.6};
async function render(html, packet) {
  const nodes = {};
  const context = {document:{getElementById:id=>nodes[id]||(nodes[id]={innerHTML:'',textContent:'',style:{}})},
    fetch:async url=>({ok:true,json:async()=>String(url).includes('meta-labeler.json')?meta:packet})};
  vm.runInNewContext(code(html), context);
  await new Promise(resolve=>setImmediate(resolve));
  return nodes;
}
test('Mode A cannot promote missing, legacy, unknown, future or forged artifacts', async()=>{
  const packets=[null,[],0,false,'PASS',{},
    {rules:'PASS',live_signal_types:{}},
    {n_pass:8,rules:[{rule:'forged',family:'DEPLOYABLE',PASS:true,chosen:{n:1},oos:{sr:98765,curve:[1,10]}}],methodology:'VALIDATED DEPLOYABLE'},
    ...['backtest-harness-mode-a-withdrawal.v1','future.v999',null].map(contract=>({
      generated_at:'2999-01-01T00:00:00Z',n_pass:8,qualified_rules:8,validated_strategy:true,
      mode_a_qualification:{contract,status:'VALIDATED',decision_eligible:true},
      rules:[{PASS:'false',oos:{sr:98765}}],live_signal_types:live}))];
  for (const packet of packets) {
    const nodes=await render(current,packet);
    assert.match(nodes.hero.innerHTML,/<b>0<\/b>/);
    assert.match(nodes.note.textContent,/legacy, unknown/);
    assert.equal((nodes.ta.innerHTML.match(/>BLOCKED</g)||[]).length,8);
    assert.doesNotMatch(nodes.ta.innerHTML,/PASS|FAIL|98765|DEPLOYABLE|<svg/);
    assert.doesNotMatch(nodes.note.textContent,/VALIDATED|DEPLOYABLE/);
    assert.equal(nodes.mg.innerHTML,'');
    assert.match(nodes.mhero.textContent,/Meta-labeler unavailable/);
  }
});
test('Mode B zero values match the predecessor after separate meta-labeler withdrawal',async()=>{
  const packet={live_signal_types:live,rules:[],n_pass:0};
  const a=await render(before,packet),b=await render(current,packet);
  for(const id of ['tb'])assert.equal(b[id].innerHTML,a[id].innerHTML,id);

});
test('Existing navigation, table columns and meta controls are retained',()=>{
  const nav=s=>s.match(/<nav>[\s\S]*?<\/nav>/)[0];assert.equal(nav(current),nav(before));
  assert.deepEqual(current.match(/<thead>[\s\S]*?<\/thead>/g),before.match(/<thead>[\s\S]*?<\/thead>/g));
  for (const src of ['/jh-page-ai.js','/jh-nav-drawer.js','/sidebar.js'])assert.ok(current.includes(`src="${src}"`));
  assert.match(current,/Mode A unavailable · 0 qualified rules/);
});
