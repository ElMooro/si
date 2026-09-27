const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const M=require('../jh-inventory-research.js'),V=require('../jh-table-values.js'),O=require('../jh-materials-orders.js'),root=path.join(__dirname,'..');
test('full received population and repeated tickers remain separate',()=>{
 const p={sector_drawdown:[{sector:'Total',latest_ratio:0},null],stock_drawdown_board:[{ticker:'A',dio_latest:0},{ticker:'A',dio_latest:3},false]};
 const data=M.records(p);assert.equal(data.sectors.length,1);assert.equal(data.stocks.length,2);assert.equal(data.issues.length,2);assert.equal(data.stocks[0].dio,0);assert.match(data.stocks[0].status,/Legacy/);
});
test('new monthly measurements are distinct from unavailable legacy signal aliases',()=>{
 const p={measurement_contract:'inventory-observation-measurements.v1',sector_drawdown:[{series:'ISRATIO',chg_6m:null,ratio_changes_pct:{'6m':{value:-10}},as_of:'2026-08-01'}],stock_drawdown_board:[{ticker:'A',rev_growth_yoy:500,revenue_per_share_change_pct:0,as_of:'2026-06-30'}]};
 assert.equal(M.records(p).sectors[0].c6,-10);assert.equal(M.records(p).stocks[0].rps,0);
 assert.equal(O.records({inventory:p,backlog:{by_ticker:{}},capex:{rows:[]}}).sectors[0].chg_6m,-10);
 delete p.measurement_contract;assert.equal(M.records(p).sectors[0].c6,null);assert.equal(M.records(p).stocks[0].rps,null);
});
test('safe text and zero handling',()=>{
 for(const v of [false,true,'',' ',Infinity,NaN,{},[]])assert.equal(M.format(v,'number'),'');
 assert.equal(M.format(0,'number'),'0.00');assert.equal(M.esc('<img src=x>'),'&lt;img src=x&gt;');
});
async function render(packet,error){
 const nodes=new Map(),calls=[];const get=id=>{if(!nodes.has(id))nodes.set(id,{value:'',textContent:'',innerHTML:''});return nodes.get(id);};
 const context=vm.createContext({document:{getElementById:get,querySelectorAll:()=>[]},JHTableValues:{...V,load:async p=>{calls.push(p);if(error)throw error;return {packet,raw:JSON.stringify(packet)};}}});
 vm.runInContext(fs.readFileSync(path.join(root,'jh-inventory-research.js'),'utf8'),context);await vm.runInContext('JHInventoryResearch.start()',context);return {get,calls};
}
test('full original single stored read filter and inert payloads',async()=>{
 const p={generated_at:'date',sector_drawdown:[{sector:'<img>'}],stock_drawdown_board:Array.from({length:130},(_,i)=>({ticker:'A'+i,dio_latest:i}))};
 const s=await render(p);assert.deepEqual(s.calls,['/data/inventory-drawdown.json']);assert.equal(s.get('original').textContent,JSON.stringify(p));assert.match(s.get('rows').textContent,/130 of 130/);
 assert.ok(!s.get('sectors').innerHTML.includes('<img>'));assert.match(s.get('sectors').innerHTML,/&lt;img&gt;/);
 s.get('q').value='A129';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 of 130/);assert.match(s.get('stocks').innerHTML,/ticker.html\?symbol=A129/);
 s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/130 of 130/);
});
test('failed source keeps rejected original without a fallback acquisition',async()=>{
 const e=Error('duplicate key');e.original_text='{"a":1,"a":2}';const s=await render(null,e);
 assert.equal(s.get('original').textContent,e.original_text);assert.equal(s.calls.length,1);assert.match(s.get('status').textContent,/Source unavailable/);
});
test('whole original page and keyboard accessible regions remain',()=>{
 const raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-inventory-observation-page.html.txt'));
 assert.equal(raw.length,9355);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'50cec1d0760bfab042ce5511f3a3a184f025606a4420da4aa1e2eb4aa2d94558');
 const page=fs.readFileSync(path.join(root,'inventory-drawdown.html'),'utf8');assert.match(page,/jh-inventory-research.js/);assert.match(page,/<label for="q">/);
 assert.equal((page.match(/class="table-region" role="region"[^>]*tabindex="0"/g)||[]).length,2);assert.ok(!page.includes('jh-inventory-v2-mount.js'));assert.match(page,/first-release vintages/);
});
