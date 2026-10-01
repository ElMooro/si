const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(require('node:path').join(__dirname,'../backtests.html'),'utf8');
const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
test('actual page masks missing, stale, malformed and self-promoted meta-labeler packets',async()=>{
  const forged={status:'active',validated_strategy:true,decision_eligible:true,
    generated_at:'2999-01-01',model:{uplift_pp:98765},gates:[{verdict:'TAKE',ticker:'FORGED'}],
    methodology:'VALIDATED STRATEGY'};
  for(const packet of [null,[],true,'TAKE',{},forged,
    {...forged,contract:'meta-labeler-withdrawal.v1'},
    {...forged,contract:'future-causal-model.v999'}]){
    const nodes={};
    vm.runInNewContext(script,{
      document:{getElementById:id=>nodes[id]||(nodes[id]={innerHTML:'',textContent:'',style:{}})},
      fetch:async()=>({ok:true,json:async()=>packet})
    });
    assert.match(nodes.mhero.textContent,/Meta-labeler unavailable/,'blocked before fetch resolves');
    await new Promise(r=>setImmediate(r));
    assert.match(nodes.mhero.textContent,/recommendations withheld/);
    assert.doesNotMatch(nodes.mhero.textContent,/98765|FORGED|VALIDATED STRATEGY/);
    assert.equal(nodes.mg.innerHTML,'');assert.equal(nodes.mt.innerHTML,'');
    assert.equal(nodes.mgrid.style.display,'none');
  }
});
