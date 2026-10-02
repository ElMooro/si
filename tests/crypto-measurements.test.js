const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'..'),F=path.join(R,'tests/fixtures/crypto-measurements');
const parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);const acorn=parser.exports;
function functions(html){
 for(const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
  if(/\bsrc\s*=|type\s*=\s*["']application\//i.test(m[1]))continue;
  const tree=acorn.parse(m[2],{ecmaVersion:'latest'});
  if(tree.body.some(n=>n.type==='FunctionDeclaration'&&n.id.name==='render'))return tree.body.filter(n=>n.type==='FunctionDeclaration').map(n=>m[2].slice(n.start,n.end)).join('\n');
 }throw Error('Actual page render function missing');
}
const candidate=functions(fs.readFileSync(path.join(R,'crypto/index.html'),'utf8')),prior=functions(fs.readFileSync(path.join(F,'predecessor.html.txt'),'utf8'));
function runtime(old=false){const nodes={main:{innerHTML:''},ts:{textContent:''}},errors=[];const ctx={D:{},window:{},document:{getElementById(id){return nodes[id]||null;}},console:{error(...x){errors.push(x.map(String).join(' '));}},fetch(){throw Error('No network');}};vm.createContext(ctx);vm.runInContext(old?prior:candidate,ctx);return {ctx,nodes,errors,render(data){ctx.D=data;ctx.render();assert.deepEqual(errors,[]);return nodes.main.innerHTML;}};}
function packet(v){return {risk_score:{score:v,regime:'Invented regime',action:'Invented action'},fear_greed:{current:v,label:'Invented label',avg_7d:v,avg_30d:v},global_market:{btc_dominance:v},technicals:{coins:{TEST:{price:v,consensus:'MIXED',timeframes:{'4h':{status:'ok',score:v,bias:'MIXED',indicators:{rsi:v,bollinger:{position:v},stochrsi:{k:v},atr_pct:v}}}}}}};}
test('predecessor replaces absent scores with 50, absent prices with zero and corrupts bar CSS',()=>{
 const r=runtime(true);assert.match(r.ctx.scoreBar(null),/>50<\/span>/);assert.equal(r.ctx.priceFmt(null),'$0');
 const html=r.render(packet(0));assert.match(html,/width:50%25/);assert.doesNotMatch(html,/crypto-score-circle/);assert.match(html,/Risk Score/);
});
test('scores require finite numeric values on the defined 0–100 scale',()=>{
 const {ctx}=runtime();for(const v of [null,undefined,false,true,'0','50',NaN,Infinity,-1,101,{},[]]){assert.match(ctx.scoreBar(v),/Unavailable/);assert.equal(ctx.arc(v),'');assert.match(ctx.cryptoScoreCircle(v),/N\/A/);}
 for(const v of [0,50,100]){assert.ok(ctx.scoreBar(v).includes('width:'+v+'%'));assert.ok(ctx.arc(v).includes('<svg'));assert.ok(ctx.cryptoScoreCircle(v).includes('>'+v+'</div>'));}
});
test('fear/greed refuses missing values and preserves zero without fabricating a gauge',()=>{
 const {ctx}=runtime();for(const v of [null,undefined,true,'25',NaN,Infinity,-1,101])assert.doesNotMatch(ctx.cryptoFearGauge(v,'Invented','var(--yellow)'),/transform:rotate|<svg/);
 assert.match(ctx.cryptoFearGauge(0,'Invented','var(--red)'),/rotate\(-90deg\)/);assert.match(ctx.cryptoFearGauge(100,'Invented','var(--green)'),/rotate\(90deg\)/);
});
test('price formatting distinguishes missing and malformed from zero and tiny prices',()=>{
 const {ctx}=runtime();for(const v of [null,undefined,'0',true,false,-1,NaN,Infinity,{},[]])assert.equal(ctx.priceFmt(v),'Unavailable');
 assert.equal(ctx.priceFmt(0),'$0');assert.equal(ctx.priceFmt(123.45),'$123.45');assert.notEqual(ctx.priceFmt(1e-10),'$0');assert.match(ctx.priceFmt(1e-10),/e-10/);
 ctx.D={prices_canonical:{prices:{TEST:{price:0}}}};assert.equal(ctx.canonPx('TEST',99),0);
 ctx.D.prices_canonical.prices.TEST.price='bad';assert.equal(ctx.canonPx('TEST',99),99);assert.equal(ctx.canonPx('TEST',null),null);
});
test('oscillators preserve zero and use valid CSS; Bollinger values outside the bands remain visible',()=>{
 const {ctx}=runtime(),zero=ctx.cryptoIndicatorBars({rsi:0,bollinger:{position:0},stochrsi:{k:0}});assert.equal((zero.match(/width:0%;/g)||[]).length,3);assert.doesNotMatch(zero,/%25|Unavailable/);
 for(const position of [-25,125]){const html=ctx.cryptoIndicatorBars({bollinger:{position}});assert.ok(html.includes('>'+position+'</span>'));assert.ok(html.includes('width:'+(position<0?0:100)+'%;'));assert.match(html,/reported value is shown alongside/);}
});
test('malformed oscillator fields never become a neutral bar',()=>{
 const {ctx}=runtime();for(const v of [null,undefined,true,false,'50',NaN,Infinity,[],{}]){const html=ctx.cryptoIndicatorBars({rsi:v,bollinger:{position:v},stochrsi:{k:v}});assert.equal((html.match(/crypto-value-unavailable/g)||[]).length,3);assert.doesNotMatch(html,/crypto-indicator-fill/);}
 for(const v of [-1,101]){const html=ctx.cryptoIndicatorBars({rsi:v,stochrsi:{k:v}});assert.equal((html.match(/crypto-value-unavailable/g)||[]).length,3);}
});
test('changed score labels and values cannot inject markup',()=>{
 const payload='<img src=x onerror=alert(1)><script>bad()</script>';const {ctx}=runtime();
 for(const html of [ctx.cryptoText(payload),ctx.cryptoFearGauge(25,payload,'var(--yellow)'),ctx.scoreBar(payload),ctx.cryptoScoreCircle(payload),ctx.cryptoIndicatorBars({rsi:payload,bollinger:{position:payload},stochrsi:{k:payload}})])assert.doesNotMatch(html,/<img|<script/);
 const p=packet(25);p.risk_score.regime=payload;p.risk_score.action=payload;p.fear_greed.label=payload;const html=runtime().render(p);assert.doesNotMatch(html,/<img|<script/);assert.match(html,/&lt;img/);
});
test('actual complete renderer keeps valid zero and marks unavailable score/oscillator inputs',()=>{
 const zero=runtime().render(packet(0));assert.match(zero,/margin-top:-8px'>0<\/div>/);assert.match(zero,/width:0%/);assert.doesNotMatch(zero,/%25/);
 for(const v of [null,undefined,true,'50']){const html=runtime().render(packet(v));assert.match(html,/margin-top:-8px'>Unavailable<\/div>/);assert.equal((html.match(/class='crypto-indicator-fill'/g)||[]).length,0);assert.match(html,/crypto-score-circle[\s\S]*?>N\/A<\/div>/);}
});
test('a missing risk score cannot borrow its old regime or action label',()=>{
 const html=runtime().render({risk_score:{regime:'STORED_STALE_REGIME',action:'STORED_STALE_ACTION'},fear_greed:{label:'STORED_STALE_FG'}});assert.doesNotMatch(html,/STORED_STALE_/);
});
test('all duplicate numeric consumers in the actual page reject hostile values',()=>{
 const payload='<img src=x onerror=alert(1)><script>bad()</script>',p=packet(payload);p.global_market.eth_dominance=payload;
 const html=runtime().render(p);assert.doesNotMatch(html,/<img|<script|NaN/);assert.match(html,/Combined breakdown unavailable/);
});
test('dominance rejects invented shares and invalid totals while retaining reported zero',()=>{
 const {ctx}=runtime();assert.match(ctx.cryptoDominance({}),/BTC: Unavailable · ETH: Unavailable/);
 const zero=ctx.cryptoDominance({btc_dominance:0,eth_dominance:0});assert.match(zero,/BTC: 0% · ETH: 0%/);assert.match(zero,/remainder \(100 − BTC − ETH\): 100%/);
 const bad=ctx.cryptoDominance({btc_dominance:80,eth_dominance:30});assert.match(bad,/BTC: 80% · ETH: 30%/);assert.match(bad,/Combined breakdown unavailable/);assert.doesNotMatch(bad,/role='img'/);
});
test('sentiment transformations name their basis and missing inputs stay unavailable',()=>{
 const html=runtime().render({}),start=html.indexOf('Sentiment Components');
 const block=html.slice(start,html.indexOf('Reported Bitcoin sentiment history',start));assert.equal((block.match(/crypto-sentiment-component/g)||[]).length,5);assert.equal((block.match(/Unavailable/g)||[]).length,5);assert.match(block,/not validated comparable scores/);
});
test('whole prior Crypto page and previous preservation manifest remain exact',()=>{
 const hash=b=>crypto.createHash('sha256').update(b).digest('hex'),plan=JSON.parse(fs.readFileSync(path.join(F,'edits.json'))),old=fs.readFileSync(path.join(F,'predecessor.html.txt'));assert.equal(hash(old),plan.predecessor_sha256);
 let expected=old.toString('utf8');for(const [a,b] of plan.edits){assert.equal(expected.split(a).length,2);expected=expected.replace(a,b);}
 assert.equal(expected,fs.readFileSync(path.join(R,'crypto/index.html'),'utf8'));assert.equal(hash(expected),plan.candidate_sha256);
 const previous=fs.readFileSync(path.join(F,'previous-commentary-edits.json'));assert.equal(hash(previous),plan.previous_manifest_sha256);const priorPlan=JSON.parse(previous);assert.equal(priorPlan.candidate_sha256,plan.predecessor_sha256);priorPlan.edits.push(...plan.edits);priorPlan.candidate_sha256=plan.candidate_sha256;
 assert.deepEqual(JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/crypto-commentary/edits.json'))),priorPlan);
});
