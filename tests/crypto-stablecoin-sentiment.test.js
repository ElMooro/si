const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),{page}=require(path.join(R,'tests/crypto-market-cap-support.cjs'));
function render(stock){
 const nodes={main:{innerHTML:''},ts:{textContent:''}},errors=[];
 const scope={D:{stablecoins:stock,fear_greed:require('./crypto-sentiment-support.cjs').fixture(),global_market:{btc_dominance:61}},window:{},document:{getElementById:id=>nodes[id]},console:{error:e=>errors.push(String(e))}};
 vm.createContext(scope);vm.runInContext(page('crypto/index.html').code,scope);scope.render();assert.deepEqual(errors,[]);
 return nodes.main.innerHTML;
}
function component(html){
 const start=html.indexOf('Stablecoin flow (transaction evidence unavailable)');assert.ok(start>=0);
 return html.slice(start,html.indexOf("<div class='crypto-sentiment-component'",start));
}
test('full Sentiment pane cannot turn any legacy stablecoin category into a score',()=>{
 for(const signal of ['INFLOW','OUTFLOW','NEUTRAL','',null,true,75,'<img src=x>']){
  const html=render({net_signal:signal}),block=component(html);
  assert.match(block,/>Unavailable<\/span>/);assert.doesNotMatch(block,/height:8px|width:(75|25|50)%|<img/);
  assert.doesNotMatch(html,/Stablecoin category \(inflow/);
 }
});
test('legacy net_signal is never accessed, even when its getter throws',()=>{
 const stock={};Object.defineProperty(stock,'net_signal',{get(){throw Error('Legacy category was consumed');}});
 assert.match(component(render(stock)),/>Unavailable<\/span>/);
});
test('stock uncertainty does not erase unrelated reported sentiment measurements',()=>{
 const html=render({net_signal:'INFLOW'}),start=html.indexOf('Sentiment Components'),end=html.indexOf('Reported Bitcoin sentiment history',start),section=html.slice(start,end);
 assert.equal((section.match(/crypto-sentiment-component/g)||[]).length,5);
 assert.match(section,/>0<\/span>/);assert.match(section,/>61<\/span>/);
 assert.match(component(html),/>Unavailable<\/span>/);
});

test('complete shipped predecessor differs by only the explicit sentiment edit',()=>{
 const hash=b=>require('node:crypto').createHash('sha256').update(b).digest('hex'),dir=path.join(__dirname,'fixtures/crypto-stablecoin-sentiment');
 const raw=fs.readFileSync(path.join(dir,'predecessor.html.txt')),plan=JSON.parse(fs.readFileSync(path.join(dir,'change.json')));assert.equal(hash(raw),plan.predecessor_sha256);
 let value=raw.toString('utf8');for(const [before,after] of plan.edits){assert.equal(value.split(before).length,2);value=value.replace(before,after);}
 assert.equal(value,fs.readFileSync(path.join(R,'crypto/index.html'),'utf8'));assert.equal(hash(value),plan.candidate_sha256);
});
