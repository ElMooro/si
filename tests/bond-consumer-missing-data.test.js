const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
async function render(file,packet,extra={}){
 const html=fs.readFileSync(path.join(__dirname,'..',file),'utf8'),scripts=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m=>m[1]);
 const source=scripts.find(s=>s.includes(file==='crypto-risk.html'?'function col(':'const sc='));assert.ok(source);
 const elements={},doc={getElementById:id=>elements[id]??={innerHTML:'',textContent:'',appendChild(){}},createElement:()=>({setAttribute(){}})};
 const transport=require('../jh-fifx-research.js');
 const requests=[];
 const scope={document:doc,fetch:async url=>{requests.push(url);return Response.json(packet);},JHFIFXResearch:transport,TextDecoder,Date,Number,Math,console,...extra};vm.runInNewContext(source,scope);await new Promise(setImmediate);return {elements,scope,requests};
}
test('actual crypto page renders abstention without a green zero or null progress bar',async()=>{
 const {elements,scope}=await render('crypto-risk.html',{calls_eligible:false,dump_risk_score:null,risk_level:'UNAVAILABLE',factors:{macro_regime:{risk:null,weight:.06,note:'Unqualified'}}});
 const text=elements.content.innerHTML;assert.match(text,/WAIT · ABSTAIN/);assert.match(text,/Unavailable/);assert.ok(!text.includes('width:null'));assert.ok(!text.includes('width:0'));assert.ok(!text.includes('color:#26ffaf'));assert.equal(scope.col(null),'#6f7b91');assert.equal(scope.col(0),'#26ffaf');
});
test('actual bond page suppresses partial-world inference, null ranking and billing prompt',async()=>{
 const {elements}=await render('bond-desk.html',{calls_eligible:false,world_anxiety:null,regime:'UNAVAILABLE',hottest_region:null,regions:{us:{fresh:false,score:null}},world_map:[],crisis_analogs:{},ai_brief:{interpretation:'must not render'},ai_status:'LIVE'});
 const text=elements.app.innerHTML;assert.match(text,/WAIT · ABSTAIN/);assert.match(text,/Research qualification incomplete/);assert.ok(!text.includes('HOTTEST: undefined'));assert.ok(!text.includes('must not render'));assert.ok(!text.includes('top up LLM billing'));assert.ok(!text.includes('>null<'));
});
test('a real zero is distinct from unavailable on both legacy views',async()=>{
 const crypto=await render('crypto-risk.html',{dump_risk_score:0,risk_level:'LOW',factors:{}});assert.match(crypto.elements.content.innerHTML,/>0<\/div>/);
 const bond=await render('bond-desk.html',{world_anxiety:0,regime:'CALM',regions:{us:{score:0}},world_map:[],crisis_analogs:{}});
 assert.match(bond.elements.app.innerHTML,/WAIT · ABSTAIN/);assert.match(bond.elements.app.innerHTML,/>0<\/div>/);
 assert.equal(JSON.parse(bond.elements['bond-original'].textContent).world_anxiety,0);
});

test('missing or asserted legacy eligibility cannot revive an unsupported global trade inference',async()=>{
 for(const eligibility of [undefined,true,false]){
  const packet={world_anxiety:64.5,regime:'ANXIOUS',regions:{us:{score:73.4},japan:{score:null,fresh:false}},world_map:[],crisis_analogs:{},equity_read:'trade-now',ai_brief:{interpretation:'unsupported-advice'},calls_eligible:eligibility};
  const {elements}=await render('bond-desk.html',packet),text=elements.app.innerHTML;
  assert.match(text,/WAIT · ABSTAIN/);assert.ok(!text.includes('trade-now'));assert.ok(!text.includes('unsupported-advice'));
  assert.equal(JSON.parse(elements['bond-original'].textContent).world_anxiety,64.5);assert.match(text,/not validated investment performance/);
 }
});

test('untrusted predecessor strings remain inert while exact source text is retained',async()=>{
 const dangerous='<img src=x onerror=alert(1)>';
 const packet={regions:{us:{score:0,credit:{composite_regime:dangerous}}},world_map:[{code:'us',label:dangerous,score:0,metric:dangerous}],crisis_analogs:{all:[{name:dangerous,date:'2026-01-01',similarity_pct:0,fwd:{}}]},method:dangerous};
 const {elements}=await render('bond-desk.html',packet);
 assert.ok(!elements.app.innerHTML.includes('<img'));assert.ok(elements.app.innerHTML.includes('&lt;img'));
 assert.equal(JSON.parse(elements['bond-original'].textContent).method,dangerous);
});

test('Bond Desk uses one read-only packet and retains every reported historical row',async()=>{
 const history=Array.from({length:600},(_,i)=>({date:'reported-'+i,value:i===0?0:i===1?null:i}));
 const {elements,requests}=await render('bond-desk.html',{regions:{},world_map:[],crisis_analogs:{},chart_ccc_bb:history});
 assert.deepEqual(requests,['/data/bond-desk.json?exact=1&nogen=1']);
 assert.match(elements['chart-anchor'].innerHTML,/All 600 observations/);
 assert.equal((elements['chart-anchor'].innerHTML.match(/<tr>/g)||[]).length,601);
 assert.match(elements['chart-anchor'].innerHTML,/reported-599/);assert.match(elements['chart-anchor'].innerHTML,/<td>0<\/td>/);
 assert.equal(JSON.parse(elements['bond-original'].textContent).chart_ccc_bb.length,600);
});

test('a delayed optional cohort cannot block credit, whole packet or reported history',async()=>{
 const calls=[];
 const {elements}=await render('bond-desk.html',{regions:{},chart_ccc_bb:[{date:'2026-09-24',value:0}]},{
  JHBondCohorts:{mount(){calls.push('cohorts');return new Promise(()=>{});}},
  JHBondCredit:{mount(){calls.push('credit');return Promise.resolve();}}
 });
 assert.deepEqual(calls,['cohorts','credit']);assert.match(elements['chart-anchor'].innerHTML,/All 1 observations/);
 assert.equal(JSON.parse(elements['bond-original'].textContent).chart_ccc_bb[0].value,0);
});
