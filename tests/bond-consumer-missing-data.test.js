const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
async function render(file,packet){
 const html=fs.readFileSync(path.join(__dirname,'..',file),'utf8'),scripts=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m=>m[1]);
 const source=scripts.find(s=>s.includes(file==='crypto-risk.html'?'function col(':'const sc='));assert.ok(source);
 const elements={},doc={getElementById:id=>elements[id]??={innerHTML:'',textContent:'',appendChild(){}},createElement:()=>({setAttribute(){}})};
 const scope={document:doc,fetch:async()=>({ok:true,json:async()=>packet}),Date,Number,Math,console};vm.runInNewContext(source,scope);await new Promise(setImmediate);return {elements,scope};
}
test('actual crypto page renders abstention without a green zero or null progress bar',async()=>{
 const {elements,scope}=await render('crypto-risk.html',{calls_eligible:false,dump_risk_score:null,risk_level:'UNAVAILABLE',factors:{macro_regime:{risk:null,weight:.06,note:'Unqualified'}}});
 const text=elements.content.innerHTML;assert.match(text,/WAIT · ABSTAIN/);assert.match(text,/Unavailable/);assert.ok(!text.includes('width:null'));assert.ok(!text.includes('width:0'));assert.ok(!text.includes('color:#26ffaf'));assert.equal(scope.col(null),'#6f7b91');assert.equal(scope.col(0),'#26ffaf');
});
test('actual bond page suppresses partial-world inference, null ranking and billing prompt',async()=>{
 const {elements}=await render('bond-desk.html',{calls_eligible:false,world_anxiety:null,regime:'UNAVAILABLE',hottest_region:null,regions:{us:{fresh:false,score:null}},world_map:[],crisis_analogs:{},ai_brief:{interpretation:'must not render'},ai_status:'LIVE'});
 const text=elements.app.innerHTML;assert.match(text,/WAIT · ABSTAIN/);assert.match(text,/Required regional votes incomplete/);assert.ok(!text.includes('HOTTEST: undefined'));assert.ok(!text.includes('must not render'));assert.ok(!text.includes('top up LLM billing'));assert.ok(!text.includes('>null<'));
});
test('a real zero is distinct from unavailable on both legacy views',async()=>{
 const crypto=await render('crypto-risk.html',{dump_risk_score:0,risk_level:'LOW',factors:{}});assert.match(crypto.elements.content.innerHTML,/>0<\/div>/);
 const bond=await render('bond-desk.html',{world_anxiety:0,regime:'CALM',regions:{},world_map:[],crisis_analogs:{}});assert.match(bond.elements.app.innerHTML,/>0<\/div>/);
});
