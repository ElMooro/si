const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
test('actual LCE page abstains even when cached legacy payload includes portfolio advice',()=>{
 const html=fs.readFileSync('lce.html','utf8');
 const code=html.slice(html.indexOf('function renderInterpretation('),html.indexOf('// ── Filter bar wiring'));
 const nodes=new Map();const node=()=>({style:{},children:[],appendChild(v){this.children.push(v)},querySelector(){return this}});
 const document={getElementById(id){if(!nodes.has(id))nodes.set(id,node());return nodes.get(id)},createElement:node};
 const ctx={document};vm.createContext(ctx);vm.runInContext(code,ctx);
 ctx.renderInterpretation({confidence:'HIGH',decisive_call:'LONG SPY',target_allocation:[{ticker:'SPY',weight_pct:100}],cross_asset:{bitcoin:{signal:2}}});
 assert.match(nodes.get('decisiveText').textContent,/No trade/);
 assert.equal(nodes.get('crossAssetLadder').innerHTML,'');
 assert.match(nodes.get('allocTbl').innerHTML,/Unavailable/);
 assert.match(nodes.get('hedgesList').textContent,/no verified portfolio/);
 assert.equal(nodes.get('risksList').children.length,3);
 for(const regime of [null,'NORMAL','CRISIS'])assert.match(ctx.decisiveCall({regime}).title,/WAIT/);
 assert.ok(!html.includes('src="/jh-page-ai.js"'));assert.ok(!html.includes('data-bars="interpretation.target_allocation'));
});
