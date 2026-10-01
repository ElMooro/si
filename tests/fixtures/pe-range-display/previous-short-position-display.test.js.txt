const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),test=require('node:test'),assert=require('node:assert/strict');
const parserModule={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parserModule.exports,parserModule);const acorn=parserModule.exports;
const W=path.join(__dirname,'..'),widget=fs.readFileSync(path.join(W,'jh-short-position-context.js'),'utf8');
const flags={calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function fixture(){return {row:{...flags,observation_freshness_verified:false,identity_verified:false,ticker:'TEST',reported_ticker_key:'TEST',source_row:'/by_ticker/TEST',short_interest_shares:100,settlement_date:'2026-09-15',latest_reported:true,days_to_cover:2,reported_days_to_cover:999,reported_reconstructed_ratio:2,days_to_cover_basis:'producer_reconstructed_ratio',reported_dtc_status:'provider_differs_from_reconstructed_ratio'},meta:{...flags,artifact:'data/short-interest-tickers.json',read_status:'parsed',context_status:'descriptive_only',source_contract:'short-interest-tickers.v1',context_contract:'short-position-consumer-context.v1',body_sha256:'a'.repeat(64),body_bytes:1000}};}
function render(name,f,extra={}){
 const html=fs.readFileSync(path.join(W,name+'.html'),'utf8');
 const ctx={window:{},RESEARCH:{TEST:{thesis:'Invented research status',short_position_context:f.row,...extra}},RMETA:{confirmation_feeds:{short_interest:f.meta}},RESEARCH_META:{short_interest:f.meta},SF:{},fetch(){throw Error('No network');}};vm.createContext(ctx);vm.runInContext(widget,ctx);
 let found=false;
 for(const script of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
   if(/\bsrc\s*=|type\s*=\s*["']application\//i.test(script[1]))continue;
   const body=script[2],tree=acorn.parse(body,{ecmaVersion:'latest',sourceType:'script'});
   if(!tree.body.some(n=>n.type==='FunctionDeclaration'&&n.id.name===(name==='alpha-scoreboard'?'drawer':'deepDrawer')))continue;
   assert.equal(found,false);found=true;
   for(const node of tree.body.filter(n=>n.type==='FunctionDeclaration'))vm.runInContext(body.slice(node.start,node.end),ctx);
   for(const node of tree.body.filter(n=>n.type==='VariableDeclaration'))for(const declaration of node.declarations)if(['ArrowFunctionExpression','FunctionExpression'].includes(declaration.init?.type))vm.runInContext(node.kind+' '+body.slice(declaration.start,declaration.end)+';',ctx);
 }
 assert.equal(found,true);return vm.runInContext(name==='alpha-scoreboard'?'drawer("TEST")':'deepDrawer("TEST")',ctx);
}
test('both actual page renderers use the typed reported settlement quantities',()=>{
 for(const name of ['alpha-scoreboard','opportunities']){const html=render(name,fixture());assert.match(html,/Reported short positions/);assert.match(html,/100 shares/);assert.match(html,/2 volume-days/);assert.match(html,/2026-09-15/);assert.match(html,/No Calls, forecast, sizing or execution authority/);}
});
test('legacy short percentage cannot reappear when the new context is withheld',()=>{
 for(const name of ['alpha-scoreboard','opportunities']){const f=fixture();f.row=null;const html=render(name,f,{short_pct:98,short_signal:'BUY'});assert.match(html,/Short-position context is unavailable/);assert.doesNotMatch(html,/Short <b>|98%|BUY/);}
});
test('conflicts and invalid provenance withhold quantities in both actual drawers',()=>{
 for(const name of ['alpha-scoreboard','opportunities'])for(const invalid of ['row','contract','pointer','authority']){const f=fixture();if(invalid==='row')f.row=null;if(invalid==='contract')f.meta.context_contract='other';if(invalid==='pointer')f.row.source_row='/by_ticker/OTHER';if(invalid==='authority')f.meta.calls_eligible=true;const html=render(name,f);assert.match(html,/Short-position context is unavailable/);assert.doesNotMatch(html,/100 shares|2 volume-days/);}
});
test('zero and historical provenance survive the real page integration',()=>{
 for(const name of ['alpha-scoreboard','opportunities']){const f=fixture();f.row.short_interest_shares=0;f.row.latest_reported=false;const html=render(name,f);assert.match(html,/0 shares/);assert.match(html,/Explicitly historical/);assert.match(html,/a{64}/);}
});

test('whole page predecessors and exactly reviewed display edits remain',()=>{
 const hash=v=>require('node:crypto').createHash('sha256').update(v).digest('hex');
 const spec=JSON.parse(fs.readFileSync(path.join(W,'tests/fixtures/short-position-display/preservation.json'),'utf8'));
 for(const [target,item] of Object.entries(spec.pages)){
  const raw=fs.readFileSync(path.join(W,item.predecessor));assert.equal(raw.length,item.bytes);assert.equal(hash(raw),item.previous_sha256);
  let expected=raw.toString('utf8');for(const [a,b] of item.edits){assert.equal(expected.split(a).length,2);expected=expected.replace(a,b);}
  const actual=fs.readFileSync(path.join(W,target));assert.equal(actual.toString('utf8'),expected);assert.equal(hash(actual),item.candidate_sha256);
 }
});
