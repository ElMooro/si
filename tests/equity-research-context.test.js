const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'..'),read=p=>fs.readFileSync(path.join(ROOT,p),'utf8');
const gate=require('../jh-equity-research-context.js');
const legacy={ticker:'ABC',generated_at:'2026-09-24T12:00:00Z',quote:{price:125},statements:{income_annual:[{revenue:500}]},
  verdict:{rating:'BUY',price_target_12m:999,confidence_pct:99},scenarios:{expected_value_upside_pct:500},
  forward_model:{future_return:99},executive_summary:'Prior model narrative',catalysts_12m:['Prior catalyst']};

test('cached or self-qualified model output cannot authorize a forecast; original company data survives',()=>{
 for(const input of [legacy,{...legacy,forecast_qualified:true,sizing_eligible:true},{...legacy,unqualified_model_output:{original_fields:'invalid'}}]){
  const before=structuredClone(input),out=gate.view(input);
  assert.deepEqual(input,before);assert.deepEqual(out.quote,input.quote);assert.deepEqual(out.statements,input.statements);
  assert.equal(out.generated_at,input.generated_at);assert.equal(out.verdict.rating,'WAIT');assert.equal(out.verdict.price_target_12m,null);
  assert.equal(out.scenarios,null);assert.equal(out.forward_model,null);assert.deepEqual(out.catalysts_12m,[]);
  for(const key of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'])assert.equal(out[key],false);
  assert.deepEqual(out.unqualified_model_output.original_fields.verdict,input.verdict);
  assert.deepEqual(gate.view(out),out);assert.match(gate.note,/observation date/);
 }
 for(const value of [null,[],42])assert.equal(gate.view(value),value);
});

test('the actual Why renderer projects cached documents before every main section and blocks if helper is absent',()=>{
 const code=read('why.html'),a=code.indexOf('function renderReport(d){'),b=code.indexOf('\nfunction aiUnavailable(',a),script=code.slice(a,b);
 for(const loaded of [true,false]){
  const content={},seen=[],inspected=[],window=loaded?{JHEquityResearchContext:gate,JHResearchInspection:{show:(el,rows)=>inspected.push(...rows)}}:{};
  const context={window,document:{},$:(id)=>content,esc:s=>String(s),console:{warn(){}},CDN_RESEARCH_BASE:'https://example.test/equity-research'};
  for(const match of script.matchAll(/\b((?:render|fetch)[A-Z]\w+)\(/g)){
   if(match[1]==='renderReport')continue;
   context[match[1]]=doc=>{if(doc&&typeof doc==='object')seen.push(doc);return '';};
  }
  vm.runInNewContext(script+'\nrenderReport(input)',{...context,input:structuredClone(legacy)});
  if(!loaded){assert.match(content.textContent,/qualification controls are unavailable/);assert.equal(seen.length,0);continue;}
  assert(seen.length>20);assert(seen.every(d=>d.verdict.rating==='WAIT'&&d.verdict.price_target_12m===null));
  assert.equal(window.__rd.scenarios,null);assert.match(content.innerHTML,/Research qualification/);
  assert.equal(inspected[0].document.unqualified_model_output.original_fields.verdict.price_target_12m,999);
 }
});

test('actual comparison renders guarded cards and does not elect a winner from unqualified metrics',()=>{
 const code=read('compare.html'),a=code.indexOf('function renderComparison(docs){'),b=code.indexOf('\nfunction renderTickerCard(',a);
 const w=code.indexOf('function renderWinnersTable(docs){'),end=code.indexOf('\n</script>',w);
 const context={window:{JHEquityResearchContext:gate},esc:s=>s,$:()=>context.content,content:{},seen:[],
  renderTickerCard:(t,d)=>{context.seen.push(d);return d.verdict.rating;},input:[{ticker:'ABC',doc:legacy},{ticker:'XYZ',doc:legacy}]};
 vm.runInNewContext(code.slice(a,b)+code.slice(w,end)+'\nrenderComparison(input)',context);
 assert.equal(context.seen.length,2);assert(context.seen.every(d=>d.verdict.rating==='WAIT'));
 assert.match(context.content.innerHTML,/Investment ranking unavailable/);assert.doesNotMatch(context.content.innerHTML,/999|Best in show/);
 assert.equal(legacy.verdict.rating,'BUY');
});

test('second-model critique does not make a request for the guarded report',async()=>{
 const code=read('why.html'),a=code.indexOf('async function fetchCritique('),b=code.indexOf('\nfunction paintCritiqueBlock(',a);
 const elements={'critique-section':{},'critique-pill':{},'critique-body':{}};let requests=0;
 const result=vm.runInNewContext(code.slice(a,b)+'\nfetchCritique("ABC",input)',{
  input:gate.view(legacy),document:{getElementById:id=>elements[id]},fetchWithTimeout:()=>{requests++;throw Error('unexpected request');}});
 await result;assert.equal(requests,0);assert.equal(elements['critique-pill'].textContent,'unqualified');
 assert.match(elements['critique-body'].textContent,/current AI policy/);
});

test('browser model-field boundary matches native code and unavailable copy makes no freshness promise',()=>{
 const python=read('aws/lambdas/justhodl-equity-research/source/lambda_function.py');
 const match=/replacement = dict\.fromkeys\(\(([^\n]+)\)\)/.exec(python);assert(match);
 const fields=[...match[1].matchAll(/'([^']+)'/g)].map(m=>m[1]);assert.deepEqual(gate.fields,fields);
 const code=read('why.html'),a=code.indexOf('function aiPending('),b=code.indexOf('\nfunction renderScenarios(',a);
 const output=vm.runInNewContext(code.slice(a,b)+'\naiPending("", "Scenarios")');
 assert.match(output,/Unqualified/);assert.doesNotMatch(output.replace(/<[^>]*>/g,''),/live and current|populate automatically|pending/);
});

test('whole page predecessors survive and current inline scripts parse',()=>{
 const record=JSON.parse(read('tests/fixtures/equity-model-context-migration.json'));
 for(const item of record.files){
  const raw=fs.readFileSync(path.join(ROOT,item.predecessor));assert.equal(raw.length,item.bytes);
  assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),item.sha256);
  const code=read(item.target);assert(code.indexOf('/jh-equity-research-context.js')<code.indexOf('</head>'));
  let i=0;for(const script of code.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
   if(/\bsrc\s*=|application\/ld\+json|application\/json|type=["']module/i.test(script[1]))continue;
   new vm.Script(script[2],{filename:item.target+':'+i++});
  }
 }
});

test('comparison preserves a reported zero and labels debt-to-equity with the actual denominator',()=>{
 const code=read('compare.html'),helpers=code.slice(code.indexOf('const esc ='),code.indexOf('// ─── URL-driven init'));
 const a=code.indexOf('function renderTickerCard('),b=code.indexOf('function renderWinnersTable(',a);
 const input=gate.view({...legacy,financial_health:{overall_score:0,leverage:{debt_to_equity:2}}});
 const html=vm.runInNewContext(helpers+code.slice(a,b)+'\nrenderTickerCard("ABC",input)',{input});
 assert.match(html,/Health Score<\/span><span class="val">0\/100/);
 assert.match(html,/Debt \/ Equity<\/span><span class="val warn">2\.00x/);
 assert.doesNotMatch(html,/Net Debt \/ EBITDA|999|Position Size/);
});

test('packet generation time is not a claim that every underlying observation is fresh',()=>{
 const code=read('why.html'),a=code.indexOf('function renderMeta('),b=code.indexOf('// ─── On load',a);
 const input=gate.view({...legacy,metadata:{data_sources_loaded:0,data_sources_total:0,fmp_endpoints_failed:['<script>unsafe</script>']}});
 const esc=s=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
 const html=vm.runInNewContext(code.slice(a,b)+'\nrenderMeta(input)',{input,esc});
 assert.match(html,/underlying input freshness unverified/);assert.match(html,/Data sources: 0\/0/);
 assert.match(html,/no paid AI/);assert.doesNotMatch(html,/\[fresh\]|Claude synthesis|<script>/);
});
