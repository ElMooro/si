const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const source=fs.readFileSync(require('node:path').join(__dirname,'../jh-pd-fails-strip.js'),'utf8');
async function render(sf,flow){
 const node={style:{},textContent:''},main={appendChild(value){assert.equal(value,node);}};
 const document={getElementById(){return null;},createElement(){return node;},querySelector(){return main;},body:{appendChild(){throw Error('Expected document flow in main');}}};
 vm.runInNewContext(source,{window:{},document,fetch:async(url)=>({ok:true,json:async()=>url.includes('settlement-fails')?sf:flow})});
 await new Promise(resolve=>setImmediate(resolve));return node;
}
test('settlement context is in document flow and retains separate dates and decimals',async()=>{
 const node=await render({headline:{scope_id:'ust_ex_tips',as_of:'2026-09-02',ftd_bn:86,ftr_bn:87,combined_bn:173}},
 {pd_settlement_fails:{scope_id:'treasury_incl_tips',unit:'usd_bn',as_of:'2026-09-09',combined_bn:190.24}});
 assert.doesNotMatch(node.style.cssText,/position:fixed|z-index/);assert.match(node.textContent,/190.24bn/);
 assert.match(node.textContent,/ex-TIPS · 2026-09-02/);assert.match(node.textContent,/including TIPS · 2026-09-09/);
});
test('missing headline never borrows the Treasury gross scope',async()=>{
 const node=await render({treasury:{as_of:'2026-09-02',ftd_bn:92.61,ftr_bn:97.63,gross_bn:190.24}},null);
 assert.doesNotMatch(node.textContent,/92.61|97.63|190.24/);assert.match(node.textContent,/FTD —/);
});
test('an unscoped old flow projection uses the declared canonical Treasury scope',async()=>{
 const node=await render({treasury:{scope_id:'treasury_incl_tips',as_of:'2026-09-09',gross_bn:262.569,field_units:{gross_bn:'usd_bn'}}},
 {pd_settlement_fails:{as_of:'2026-09-02',combined_bn:190.24,unit:'usd_bn'}});
 assert.match(node.textContent,/including TIPS · 2026-09-09/);assert.match(node.textContent,/262.569bn/);assert.doesNotMatch(node.textContent,/190.24/);
});
test('invalid scope, booleans, nonfinite values and source markup cannot become displayed observations',async()=>{
 const node=await render({headline:{scope_id:'ust_ex_tips',as_of:'<img onerror=x>',ftd_bn:false,ftr_bn:Infinity,combined_bn:NaN}},
 {pd_settlement_fails:{scope_id:'other',unit:'usd_bn',combined_bn:777}});
 assert.doesNotMatch(node.textContent,/<img|777|NaN|Infinity|\$0bn/);assert.match(node.textContent,/observation date unavailable/);
});
